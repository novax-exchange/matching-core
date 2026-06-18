use crate::{
    bounded_handoff::BoundedHandoff,
    journal::{OutputJournal, OutputJournalError},
    output_queue::{OutputQueue, OutputQueueError},
    symbol_runtime::SymbolRuntime,
};
use std::thread::{self, JoinHandle};

pub type SymbolRuntimeWorkerResult<O> = (
    SymbolRuntime,
    BoundedHandoff,
    O,
    Result<usize, OutputJournalError>,
);

pub fn spawn_symbol_runtime_once<O>(
    mut runtime: SymbolRuntime,
    mut queue: BoundedHandoff,
    mut output: O,
    max_entries: usize,
) -> JoinHandle<SymbolRuntimeWorkerResult<O>>
where
    O: OutputJournal + Send + 'static,
{
    thread::spawn(move || {
        let result = run_symbol_runtime_step(
            &mut runtime, 
            &mut queue,
            &mut output, 
            max_entries,
        );

        (runtime, queue, output, result)
    })
}

pub fn run_symbol_runtime_step(
    runtime: &mut SymbolRuntime,
    queue: &mut BoundedHandoff,
    output: &mut dyn OutputJournal,
    max_entries: usize,
) -> Result<usize, OutputJournalError> {
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

pub fn run_symbol_runtime_step_to_output_queue(
    runtime: &mut SymbolRuntime,
    handoff: &mut BoundedHandoff,
    output_queue: &mut OutputQueue,
    max_entries: usize,
) -> Result<usize, OutputQueueError> {
    let entries = handoff.drain_batch(max_entries);
    let mut remaining = entries.into_iter();
    let mut processed = 0;

    while let Some(entry) = remaining.next() {
        match runtime.process_entry_into_output_queue(entry.clone(), output_queue) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{EngineEvent, OrderAck};
    use crate::bounded_handoff::BoundedHandoff;
    use crate::journal::{InputJournalEntry, OutputJournal, OutputJournalEntry, OutputJournalError};
    use crate::order::{Command, Order};
    use crate::output_queue::{OutputQueue, OutputQueueError};
    use crate::symbol_runtime::SymbolRuntime;
    use crate::types::{CommandId, JournalSeq, OrderId, Price, Quantity, Side, Symbol};

    struct InMemoryOutputJournal {
        entries: Vec<OutputJournalEntry>,
    }

    impl InMemoryOutputJournal {
        fn new() -> Self {
            Self { entries: Vec::new() }
        }
    }

    impl OutputJournal for InMemoryOutputJournal {
        fn append(
            &mut self,
            command_id: CommandId,
            journal_seq: JournalSeq,
            events: Vec<EngineEvent>,
        ) -> Result<(), OutputJournalError> {
            self.entries.push(OutputJournalEntry {
                command_id,
                journal_seq,
                events,
            });
            Ok(())
        }

        fn read_all(&self) -> Vec<OutputJournalEntry> {
            self.entries.clone()
        }
    }

    fn symbol() -> Symbol {
        Symbol("BTC-USDT".to_string())
    }

    fn input_entry(seq: u64, command_id: u64, order_id: u64) -> InputJournalEntry {
        InputJournalEntry {
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
    fn runtime_loop_step_drains_queue_and_processes_entries() {
        let mut queue = BoundedHandoff::new(4);
        let mut runtime = SymbolRuntime::new(symbol());
        let mut output = InMemoryOutputJournal::new();

        assert_eq!(queue.enqueue(input_entry(1, 10, 100)), Ok(()));
        assert_eq!(queue.enqueue(input_entry(2, 11, 101)), Ok(()));

        let processed = run_symbol_runtime_step(
            &mut runtime,
            &mut queue,
            &mut output,
            10,
        );

        assert_eq!(processed, Ok(2));
        assert_eq!(runtime.last_input_seq(), Some(JournalSeq(2)));
        assert_eq!(queue.len(), 0);

        let output_entries = output.read_all();
        assert_eq!(output_entries.len(), 2);
        assert_eq!(
            output_entries[0].events,
            vec![EngineEvent::OrderAck(OrderAck::Accepted {
                command_id: CommandId(10),
                order_id: OrderId(100),
                journal_seq: JournalSeq(1),
            })]
        );
    }

    struct FailOnSecondAppendOutputJournal {
        entries: Vec<OutputJournalEntry>,
        append_count: usize,
    }
    
    impl FailOnSecondAppendOutputJournal {
        fn new() -> Self {
            Self {
                entries: Vec::new(),
                append_count: 0,
            }
        }
    }
    
    impl OutputJournal for FailOnSecondAppendOutputJournal {
        fn append(
            &mut self,
            command_id: CommandId,
            journal_seq: JournalSeq,
            events: Vec<EngineEvent>,
        ) -> Result<(), OutputJournalError> {
            self.append_count += 1;
    
            if self.append_count == 2 {
                return Err(OutputJournalError::AppendFailed);
            }
    
            self.entries.push(OutputJournalEntry {
                command_id,
                journal_seq,
                events,
            });
    
            Ok(())
        }
    
        fn read_all(&self) -> Vec<OutputJournalEntry> {
            self.entries.clone()
        }
    }

    #[test]
    fn runtime_loop_step_keeps_unprocessed_entries_when_batch_fails() {
        let mut queue = BoundedHandoff::new(4);
        let mut runtime = SymbolRuntime::new(symbol());
        let mut output = FailOnSecondAppendOutputJournal::new();

        assert_eq!(queue.enqueue(input_entry(1, 10, 100)), Ok(()));
        assert_eq!(queue.enqueue(input_entry(2, 11, 101)), Ok(()));
        assert_eq!(queue.enqueue(input_entry(3, 12, 102)), Ok(()));

        let result = run_symbol_runtime_step(
            &mut runtime,
            &mut queue,
            &mut output,
            10,
        );

        assert_eq!(result, Err(OutputJournalError::AppendFailed));
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
        let output = InMemoryOutputJournal::new();

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
    fn runtime_loop_step_can_enqueue_output_requests_without_advancing_safe_point() {
        let mut handoff = BoundedHandoff::new(4);
        let mut output_queue = OutputQueue::new(4);
        let mut runtime = SymbolRuntime::new(symbol());

        assert_eq!(handoff.enqueue(input_entry(1, 10, 100)), Ok(()));
        assert_eq!(handoff.enqueue(input_entry(2, 11, 101)), Ok(()));

        let processed = run_symbol_runtime_step_to_output_queue(
            &mut runtime,
            &mut handoff,
            &mut output_queue,
            10,
        );

        assert_eq!(processed, Ok(2));
        assert_eq!(handoff.len(), 0);
        assert_eq!(runtime.last_input_seq(), None);

        let requests = output_queue.drain_batch(10);
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].journal_seq, JournalSeq(1));
        assert_eq!(requests[1].journal_seq, JournalSeq(2));
    }

    #[test]
    fn runtime_loop_step_requeues_unprocessed_input_when_output_queue_is_full() {
        let mut handoff = BoundedHandoff::new(4);
        let mut output_queue = OutputQueue::new(1);
        let mut runtime = SymbolRuntime::new(symbol());

        assert_eq!(handoff.enqueue(input_entry(1, 10, 100)), Ok(()));
        assert_eq!(handoff.enqueue(input_entry(2, 11, 101)), Ok(()));
        assert_eq!(handoff.enqueue(input_entry(3, 12, 102)), Ok(()));

        let result = run_symbol_runtime_step_to_output_queue(
            &mut runtime,
            &mut handoff,
            &mut output_queue,
            10,
        );

        assert_eq!(result, Err(OutputQueueError::QueueFull));
        assert_eq!(runtime.last_input_seq(), None);

        let output_requests = output_queue.drain_batch(10);
        assert_eq!(output_requests.len(), 1);
        assert_eq!(output_requests[0].journal_seq, JournalSeq(1));

        let remaining_inputs = handoff.drain_batch(10);
        assert_eq!(remaining_inputs.len(), 2);
        assert_eq!(remaining_inputs[0].seq, JournalSeq(2));
        assert_eq!(remaining_inputs[1].seq, JournalSeq(3));
    }
}
