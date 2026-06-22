mod runtime;

pub use runtime::{SafePointError, SymbolRuntime};

use crate::{
    bounded_handoff::BoundedHandoff,
    journal_adapter::{JournalAdapterError, JournalOutputAppender},
    output_commit_boundary::{
        run_output_batch_commit_step_report_with_identity_and_metadata_context,
        OutputBatchCommitMetadataContext, OutputBatchCommitResult, OutputBatchCommitStepReport,
        OutputBatchIdentity, OutputBatchQueryStatus, OutputJournalClient, PendingOutputBuffer,
        PendingOutputBufferError,
    },
};
use std::thread::{self, JoinHandle};

pub type SymbolRuntimeWorkerResult<O> = (
    SymbolRuntime,
    BoundedHandoff,
    O,
    Result<usize, JournalAdapterError>,
);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SymbolRuntimeOutputCommitStepReport {
    pub input_processed_count: usize,
    pub safe_point_advanced_count: usize,
    pub output_batch_identity: Option<OutputBatchIdentity>,
    pub output_batch_query_status: Option<OutputBatchQueryStatus>,
    pub output_commit_report: OutputBatchCommitStepReport,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SymbolRuntimeOutputCommitStepError {
    PendingOutputBuffer(PendingOutputBufferError),
    SafePoint(SafePointError),
}

pub fn spawn_symbol_runtime_once<O>(
    mut runtime: SymbolRuntime,
    mut queue: BoundedHandoff,
    mut output: O,
    max_entries: usize,
) -> JoinHandle<SymbolRuntimeWorkerResult<O>>
where
    O: JournalOutputAppender + Send + 'static,
{
    thread::spawn(move || {
        let result = run_symbol_runtime_step(&mut runtime, &mut queue, &mut output, max_entries);

        (runtime, queue, output, result)
    })
}

pub fn run_symbol_runtime_step(
    runtime: &mut SymbolRuntime,
    queue: &mut BoundedHandoff,
    output: &mut dyn JournalOutputAppender,
    max_entries: usize,
) -> Result<usize, JournalAdapterError> {
    let entries = queue.drain_batch(max_entries);
    let mut remaining = entries.into_iter();
    let mut processed = 0;

    while let Some(entry) = remaining.next() {
        match runtime.process_entry(entry.clone(), output) {
            Ok(()) => processed += 1,
            Err(error) => {
                let mut to_prepend = vec![entry];
                to_prepend.extend(remaining);
                queue.prepend_entries(to_prepend);
                return Err(error);
            }
        }
    }

    Ok(processed)
}

pub fn run_symbol_runtime_step_to_pending_output_buffer(
    runtime: &mut SymbolRuntime,
    handoff: &mut BoundedHandoff,
    pending_output_buffer: &mut PendingOutputBuffer,
    max_entries: usize,
) -> Result<usize, PendingOutputBufferError> {
    let entries = handoff.drain_batch(max_entries);
    let mut remaining = entries.into_iter();
    let mut processed = 0;

    while let Some(entry) = remaining.next() {
        match runtime.process_entry_into_pending_output_buffer(entry.clone(), pending_output_buffer)
        {
            Ok(()) => processed += 1,
            Err(error) => {
                let mut to_prepend = vec![entry];
                to_prepend.extend(remaining);
                handoff.prepend_entries(to_prepend);
                return Err(error);
            }
        }
    }

    Ok(processed)
}

pub fn advance_runtime_safe_point_from_output_commit(
    runtime: &mut SymbolRuntime,
    commit_result: &OutputBatchCommitResult,
) -> Result<usize, SafePointError> {
    let mut advanced = 0;

    for journal_seq in &commit_result.committed_seqs {
        runtime.mark_output_committed(*journal_seq)?;
        advanced += 1;
    }

    Ok(advanced)
}

pub fn run_symbol_runtime_step_with_output_batch_commit(
    runtime: &mut SymbolRuntime,
    handoff: &mut BoundedHandoff,
    pending_output_buffer: &mut PendingOutputBuffer,
    journal_client: &mut OutputJournalClient,
    output: &mut dyn JournalOutputAppender,
    max_input_entries: usize,
    max_output_requests: usize,
) -> Result<SymbolRuntimeOutputCommitStepReport, SymbolRuntimeOutputCommitStepError> {
    run_symbol_runtime_step_with_output_batch_commit_metadata_context(
        runtime,
        handoff,
        pending_output_buffer,
        journal_client,
        output,
        max_input_entries,
        max_output_requests,
        None,
    )
}

pub fn run_symbol_runtime_step_with_output_batch_commit_metadata_context(
    runtime: &mut SymbolRuntime,
    handoff: &mut BoundedHandoff,
    pending_output_buffer: &mut PendingOutputBuffer,
    journal_client: &mut OutputJournalClient,
    output: &mut dyn JournalOutputAppender,
    max_input_entries: usize,
    max_output_requests: usize,
    metadata_context: Option<OutputBatchCommitMetadataContext>,
) -> Result<SymbolRuntimeOutputCommitStepReport, SymbolRuntimeOutputCommitStepError> {
    let input_processed_count = run_symbol_runtime_step_to_pending_output_buffer(
        runtime,
        handoff,
        pending_output_buffer,
        max_input_entries,
    )
    .map_err(SymbolRuntimeOutputCommitStepError::PendingOutputBuffer)?;

    let output_commit_report_with_identity =
        run_output_batch_commit_step_report_with_identity_and_metadata_context(
            runtime.symbol(),
            journal_client,
            pending_output_buffer,
            output,
            max_output_requests,
            metadata_context,
        );
    let output_commit_report = output_commit_report_with_identity.commit_report;
    let safe_point_advanced_count =
        advance_runtime_safe_point_from_output_commit(runtime, &output_commit_report.commit_result)
            .map_err(SymbolRuntimeOutputCommitStepError::SafePoint)?;

    Ok(SymbolRuntimeOutputCommitStepReport {
        input_processed_count,
        safe_point_advanced_count,
        output_batch_identity: output_commit_report_with_identity.batch_identity,
        output_batch_query_status: output_commit_report_with_identity.output_batch_query_status,
        output_commit_report,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bounded_handoff::BoundedHandoff;
    use crate::journal_adapter::{
        JournalAdapterError, JournalInputEntry, JournalOutputAppender, JournalOutputEntry,
    };
    use crate::matching_engine::{
        EngineEvent, MarketEvent, OrderAck, OrderAddedEvent, PriceLevelChangedEvent,
    };
    use crate::order::{Command, Order};
    use crate::output_commit_boundary::OutputBatchCommitResult;
    use crate::output_commit_boundary::{PendingOutputBuffer, PendingOutputBufferError};
    use crate::symbol_runtime::{SafePointError, SymbolRuntime};
    use crate::types::{CommandId, JournalSeq, MarketSeq, OrderId, Price, Quantity, Side, Symbol};

    struct InMemoryJournalOutputAppender {
        entries: Vec<JournalOutputEntry>,
    }

    impl InMemoryJournalOutputAppender {
        fn new() -> Self {
            Self {
                entries: Vec::new(),
            }
        }
    }

    impl JournalOutputAppender for InMemoryJournalOutputAppender {
        fn append(
            &mut self,
            command_id: CommandId,
            journal_seq: JournalSeq,
            events: Vec<EngineEvent>,
        ) -> Result<(), JournalAdapterError> {
            self.entries.push(JournalOutputEntry {
                command_id,
                journal_seq,
                events,
                output_commit_metadata: None,
            });
            Ok(())
        }

        fn read_all(&self) -> Vec<JournalOutputEntry> {
            self.entries.clone()
        }
    }

    fn symbol() -> Symbol {
        Symbol("BTC-USDT".to_string())
    }

    fn input_entry(seq: u64, command_id: u64, order_id: u64) -> JournalInputEntry {
        JournalInputEntry {
            seq: JournalSeq(seq),
            command_id: CommandId(command_id),
            command: Command::PlaceLimit(Order {
                order_id: OrderId(order_id),
                symbol: symbol(),
                side: Side::Buy,
                price: Price(100),
                quantity: Quantity(5),
            }),
        }
    }

    #[test]
    fn symbol_runtime_step_drains_queue_and_processes_entries() {
        let mut queue = BoundedHandoff::new(4);
        let mut runtime = SymbolRuntime::new(symbol());
        let mut output = InMemoryJournalOutputAppender::new();

        assert_eq!(queue.enqueue(input_entry(1, 10, 100)), Ok(()));
        assert_eq!(queue.enqueue(input_entry(2, 11, 101)), Ok(()));

        let processed = run_symbol_runtime_step(&mut runtime, &mut queue, &mut output, 10);

        assert_eq!(processed, Ok(2));
        assert_eq!(runtime.last_input_seq(), Some(JournalSeq(2)));
        assert_eq!(queue.len(), 0);

        let output_entries = output.read_all();
        assert_eq!(output_entries.len(), 2);
        assert_eq!(
            output_entries[0].events,
            vec![
                EngineEvent::OrderAck(OrderAck::Accepted {
                    command_id: CommandId(10),
                    order_id: OrderId(100),
                    journal_seq: JournalSeq(1),
                }),
                EngineEvent::Market(MarketEvent::OrderAdded(OrderAddedEvent {
                    market_seq: MarketSeq(1),
                    command_id: CommandId(10),
                    journal_seq: JournalSeq(1),
                    order_id: OrderId(100),
                    side: Side::Buy,
                    price: Price(100),
                    quantity: Quantity(5),
                })),
                EngineEvent::Market(MarketEvent::PriceLevelChanged(PriceLevelChangedEvent {
                    market_seq: MarketSeq(2),
                    command_id: CommandId(10),
                    journal_seq: JournalSeq(1),
                    side: Side::Buy,
                    price: Price(100),
                    quantity_after: Quantity(5),
                })),
            ]
        );
    }

    struct FailOnSecondAppendJournalOutputAppender {
        entries: Vec<JournalOutputEntry>,
        append_count: usize,
    }

    impl FailOnSecondAppendJournalOutputAppender {
        fn new() -> Self {
            Self {
                entries: Vec::new(),
                append_count: 0,
            }
        }
    }

    impl JournalOutputAppender for FailOnSecondAppendJournalOutputAppender {
        fn append(
            &mut self,
            command_id: CommandId,
            journal_seq: JournalSeq,
            events: Vec<EngineEvent>,
        ) -> Result<(), JournalAdapterError> {
            self.append_count += 1;

            if self.append_count == 2 {
                return Err(JournalAdapterError::AppendFailed);
            }

            self.entries.push(JournalOutputEntry {
                command_id,
                journal_seq,
                events,
                output_commit_metadata: None,
            });

            Ok(())
        }

        fn read_all(&self) -> Vec<JournalOutputEntry> {
            self.entries.clone()
        }
    }

    #[test]
    fn symbol_runtime_step_keeps_unprocessed_entries_when_batch_fails() {
        let mut queue = BoundedHandoff::new(4);
        let mut runtime = SymbolRuntime::new(symbol());
        let mut output = FailOnSecondAppendJournalOutputAppender::new();

        assert_eq!(queue.enqueue(input_entry(1, 10, 100)), Ok(()));
        assert_eq!(queue.enqueue(input_entry(2, 11, 101)), Ok(()));
        assert_eq!(queue.enqueue(input_entry(3, 12, 102)), Ok(()));

        let result = run_symbol_runtime_step(&mut runtime, &mut queue, &mut output, 10);

        assert_eq!(result, Err(JournalAdapterError::AppendFailed));
        assert_eq!(runtime.last_input_seq(), Some(JournalSeq(1)));

        let remaining = queue.drain_batch(10);
        assert_eq!(remaining.len(), 2);
        assert_eq!(remaining[0].seq, JournalSeq(2));
        assert_eq!(remaining[1].seq, JournalSeq(3));
    }

    #[test]
    fn symbol_runtime_worker_thread_processes_one_batch_and_returns_state() {
        let mut queue = BoundedHandoff::new(4);
        let runtime = SymbolRuntime::new(symbol());
        let output = InMemoryJournalOutputAppender::new();

        assert_eq!(queue.enqueue(input_entry(1, 10, 100)), Ok(()));
        assert_eq!(queue.enqueue(input_entry(2, 11, 101)), Ok(()));

        let handle = spawn_symbol_runtime_once(runtime, queue, output, 10);

        let (runtime, queue, output, result) = handle
            .join()
            .expect("worker thread must finish successfully");

        assert_eq!(result, Ok(2));
        assert_eq!(runtime.last_input_seq(), Some(JournalSeq(2)));
        assert_eq!(queue.len(), 0);
        assert_eq!(output.read_all().len(), 2);
    }

    #[test]
    fn symbol_runtime_step_can_enqueue_output_requests_without_advancing_safe_point() {
        let mut handoff = BoundedHandoff::new(4);
        let mut pending_output_buffer = PendingOutputBuffer::new(4);
        let mut runtime = SymbolRuntime::new(symbol());

        assert_eq!(handoff.enqueue(input_entry(1, 10, 100)), Ok(()));
        assert_eq!(handoff.enqueue(input_entry(2, 11, 101)), Ok(()));

        let processed = run_symbol_runtime_step_to_pending_output_buffer(
            &mut runtime,
            &mut handoff,
            &mut pending_output_buffer,
            10,
        );

        assert_eq!(processed, Ok(2));
        assert_eq!(handoff.len(), 0);
        assert_eq!(runtime.last_input_seq(), None);

        let requests = pending_output_buffer.drain_batch(10);
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].journal_seq, JournalSeq(1));
        assert_eq!(requests[1].journal_seq, JournalSeq(2));
    }

    #[test]
    fn symbol_runtime_step_requeues_unprocessed_input_when_pending_output_buffer_is_full() {
        let mut handoff = BoundedHandoff::new(4);
        let mut pending_output_buffer = PendingOutputBuffer::new(1);
        let mut runtime = SymbolRuntime::new(symbol());

        assert_eq!(handoff.enqueue(input_entry(1, 10, 100)), Ok(()));
        assert_eq!(handoff.enqueue(input_entry(2, 11, 101)), Ok(()));
        assert_eq!(handoff.enqueue(input_entry(3, 12, 102)), Ok(()));

        let result = run_symbol_runtime_step_to_pending_output_buffer(
            &mut runtime,
            &mut handoff,
            &mut pending_output_buffer,
            10,
        );

        assert_eq!(result, Err(PendingOutputBufferError::BufferFull));
        assert_eq!(runtime.last_input_seq(), None);

        let output_requests = pending_output_buffer.drain_batch(10);
        assert_eq!(output_requests.len(), 1);
        assert_eq!(output_requests[0].journal_seq, JournalSeq(1));

        let remaining_inputs = handoff.drain_batch(10);
        assert_eq!(remaining_inputs.len(), 2);
        assert_eq!(remaining_inputs[0].seq, JournalSeq(2));
        assert_eq!(remaining_inputs[1].seq, JournalSeq(3));
    }

    #[test]
    fn safe_point_controller_advances_runtime_from_confirmed_output_sequences() {
        let mut runtime = SymbolRuntime::new(symbol());
        let commit_result = OutputBatchCommitResult {
            committed_count: 2,
            last_committed_seq: Some(JournalSeq(2)),
            committed_seqs: vec![JournalSeq(1), JournalSeq(2)],
        };

        assert_eq!(
            advance_runtime_safe_point_from_output_commit(&mut runtime, &commit_result),
            Ok(2)
        );
        assert_eq!(runtime.last_input_seq(), Some(JournalSeq(2)));
    }

    #[test]
    fn safe_point_controller_allows_per_symbol_global_sequence_gaps() {
        let mut runtime = SymbolRuntime::new(symbol());
        let commit_result = OutputBatchCommitResult {
            committed_count: 2,
            last_committed_seq: Some(JournalSeq(3)),
            committed_seqs: vec![JournalSeq(1), JournalSeq(3)],
        };

        assert_eq!(
            advance_runtime_safe_point_from_output_commit(&mut runtime, &commit_result),
            Ok(2)
        );
        assert_eq!(runtime.last_input_seq(), Some(JournalSeq(3)));
    }

    #[test]
    fn safe_point_controller_rejects_non_monotonic_output_confirmations() {
        let mut runtime = SymbolRuntime::new(symbol());
        let commit_result = OutputBatchCommitResult {
            committed_count: 2,
            last_committed_seq: Some(JournalSeq(2)),
            committed_seqs: vec![JournalSeq(3), JournalSeq(2)],
        };

        assert_eq!(
            advance_runtime_safe_point_from_output_commit(&mut runtime, &commit_result),
            Err(SafePointError::NonMonotonicCommit {
                last_committed: Some(JournalSeq(3)),
                actual: JournalSeq(2),
            })
        );
        assert_eq!(runtime.last_input_seq(), Some(JournalSeq(3)));
    }
}
