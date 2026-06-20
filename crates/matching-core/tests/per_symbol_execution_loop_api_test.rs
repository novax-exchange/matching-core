use matching_core::bounded_handoff::BoundedHandoff;
use matching_core::journal_adapter::{
    JournalAdapterError, JournalInputEntry, JournalOutputAppender, JournalOutputEntry,
};
use matching_core::matching_engine::EngineEvent;
use matching_core::order::{Command, Order};
use matching_core::output_commit_boundary::{
    run_output_batch_commit_step_report, OutputJournalClient, PendingOutputBuffer,
};
use matching_core::per_symbol_execution_loop::SymbolRuntime;
use matching_core::per_symbol_execution_loop::{
    advance_runtime_safe_point_from_output_commit, run_per_symbol_execution_loop_step,
    run_per_symbol_execution_loop_step_to_pending_output_buffer,
    run_per_symbol_execution_loop_step_with_output_batch_commit,
    spawn_per_symbol_execution_loop_once,
};
use matching_core::types::{CommandId, JournalSeq, OrderId, Price, Quantity, Side, Symbol};

struct TestJournalOutputAppender {
    entries: Vec<JournalOutputEntry>,
}

impl TestJournalOutputAppender {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
}

impl JournalOutputAppender for TestJournalOutputAppender {
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

fn symbol() -> Symbol {
    Symbol("BTC-USDT".to_string())
}

fn command_entry(seq: u64) -> JournalInputEntry {
    JournalInputEntry {
        seq: JournalSeq(seq),
        command_id: CommandId(seq),
        command: Command::PlaceLimit(Order {
            order_id: OrderId(seq),
            symbol: symbol(),
            side: Side::Buy,
            price: Price(100),
            quantity: Quantity(1),
        }),
    }
}

#[test]
fn per_symbol_execution_loop_step_is_available_from_public_api() {
    let mut queue = BoundedHandoff::new(4);
    let mut runtime = SymbolRuntime::new(symbol());
    let mut output = TestJournalOutputAppender::new();

    assert_eq!(queue.enqueue(command_entry(1)), Ok(()));
    assert_eq!(queue.enqueue(command_entry(2)), Ok(()));

    assert_eq!(
        run_per_symbol_execution_loop_step(&mut runtime, &mut queue, &mut output, 10),
        Ok(2)
    );

    assert_eq!(runtime.last_input_seq(), Some(JournalSeq(2)));
    assert_eq!(queue.len(), 0);
    assert_eq!(output.read_all().len(), 2);
}

#[test]
fn per_symbol_execution_loop_step_to_pending_output_buffer_is_available_from_public_api() {
    let mut handoff = BoundedHandoff::new(4);
    let mut pending_output_buffer = PendingOutputBuffer::new(4);
    let mut runtime = SymbolRuntime::new(symbol());

    assert_eq!(handoff.enqueue(command_entry(1)), Ok(()));
    assert_eq!(handoff.enqueue(command_entry(2)), Ok(()));

    assert_eq!(
        run_per_symbol_execution_loop_step_to_pending_output_buffer(
            &mut runtime,
            &mut handoff,
            &mut pending_output_buffer,
            10,
        ),
        Ok(2)
    );

    assert_eq!(runtime.last_input_seq(), None);
    assert_eq!(handoff.len(), 0);

    let requests = pending_output_buffer.drain_batch(10);
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].journal_seq, JournalSeq(1));
    assert_eq!(requests[1].journal_seq, JournalSeq(2));
}

#[test]
fn output_commit_report_advances_safe_point_only_for_confirmed_prefix() {
    let mut handoff = BoundedHandoff::new(4);
    let mut pending_output_buffer = PendingOutputBuffer::new(4);
    let mut runtime = SymbolRuntime::new(symbol());

    assert_eq!(handoff.enqueue(command_entry(1)), Ok(()));
    assert_eq!(handoff.enqueue(command_entry(2)), Ok(()));
    assert_eq!(handoff.enqueue(command_entry(3)), Ok(()));

    assert_eq!(
        run_per_symbol_execution_loop_step_to_pending_output_buffer(
            &mut runtime,
            &mut handoff,
            &mut pending_output_buffer,
            10,
        ),
        Ok(3)
    );

    let mut journal_client = OutputJournalClient::new();
    let mut output = FailOnSecondAppendJournalOutputAppender::new();
    let report = run_output_batch_commit_step_report(
        &mut journal_client,
        &mut pending_output_buffer,
        &mut output,
        10,
    );

    assert_eq!(
        advance_runtime_safe_point_from_output_commit(&mut runtime, &report.commit_result),
        Ok(1)
    );
    assert_eq!(runtime.last_input_seq(), Some(JournalSeq(1)));

    let remaining = pending_output_buffer.drain_batch(10);
    assert_eq!(remaining.len(), 2);
    assert_eq!(remaining[0].journal_seq, JournalSeq(2));
    assert_eq!(remaining[1].journal_seq, JournalSeq(3));
}

#[test]
fn per_symbol_execution_loop_step_with_output_batch_commit_is_available_from_public_api() {
    let mut handoff = BoundedHandoff::new(4);
    let mut pending_output_buffer = PendingOutputBuffer::new(4);
    let mut runtime = SymbolRuntime::new(symbol());
    let mut journal_client = OutputJournalClient::new();
    let mut output = TestJournalOutputAppender::new();

    assert_eq!(handoff.enqueue(command_entry(1)), Ok(()));
    assert_eq!(handoff.enqueue(command_entry(2)), Ok(()));

    let result = run_per_symbol_execution_loop_step_with_output_batch_commit(
        &mut runtime,
        &mut handoff,
        &mut pending_output_buffer,
        &mut journal_client,
        &mut output,
        10,
        10,
    );

    let report = result.expect("integrated execution and output commit step should succeed");

    assert_eq!(report.input_processed_count, 2);
    assert_eq!(report.safe_point_advanced_count, 2);
    assert_eq!(report.output_commit_report.blocking_seq, None);
    assert_eq!(runtime.last_input_seq(), Some(JournalSeq(2)));
    assert_eq!(handoff.len(), 0);
    assert!(pending_output_buffer.is_empty());
    assert_eq!(output.read_all().len(), 2);
}

#[test]
fn per_symbol_execution_loop_step_with_output_batch_commit_preserves_blocked_tail() {
    let mut handoff = BoundedHandoff::new(4);
    let mut pending_output_buffer = PendingOutputBuffer::new(4);
    let mut runtime = SymbolRuntime::new(symbol());
    let mut journal_client = OutputJournalClient::new();
    let mut output = FailOnSecondAppendJournalOutputAppender::new();

    assert_eq!(handoff.enqueue(command_entry(1)), Ok(()));
    assert_eq!(handoff.enqueue(command_entry(2)), Ok(()));
    assert_eq!(handoff.enqueue(command_entry(3)), Ok(()));

    let report = run_per_symbol_execution_loop_step_with_output_batch_commit(
        &mut runtime,
        &mut handoff,
        &mut pending_output_buffer,
        &mut journal_client,
        &mut output,
        10,
        10,
    )
    .expect("blocked output commit still reports confirmed prefix");

    assert_eq!(report.input_processed_count, 3);
    assert_eq!(report.safe_point_advanced_count, 1);
    assert_eq!(
        report.output_commit_report.blocking_seq,
        Some(JournalSeq(2))
    );
    assert_eq!(runtime.last_input_seq(), Some(JournalSeq(1)));
    assert_eq!(handoff.len(), 0);

    let remaining = pending_output_buffer.drain_batch(10);
    assert_eq!(remaining.len(), 2);
    assert_eq!(remaining[0].journal_seq, JournalSeq(2));
    assert_eq!(remaining[1].journal_seq, JournalSeq(3));
}

#[test]
fn one_shot_per_symbol_execution_loop_worker_is_available_from_public_api() {
    let mut queue = BoundedHandoff::new(4);
    let runtime = SymbolRuntime::new(symbol());
    let output = TestJournalOutputAppender::new();

    assert_eq!(queue.enqueue(command_entry(1)), Ok(()));
    assert_eq!(queue.enqueue(command_entry(2)), Ok(()));

    let handle = spawn_per_symbol_execution_loop_once(runtime, queue, output, 10);

    let (runtime, queue, output, result) = handle
        .join()
        .expect("worker thread must finish successfully");

    assert_eq!(result, Ok(2));
    assert_eq!(runtime.last_input_seq(), Some(JournalSeq(2)));
    assert_eq!(queue.len(), 0);
    assert_eq!(output.read_all().len(), 2);
}
