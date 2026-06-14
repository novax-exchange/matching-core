use matching_core::engine::EngineEvent;
use matching_core::input_queue::PerSymbolInputQueue;
use matching_core::journal::{
    InputJournalEntry, OutputJournal, OutputJournalEntry, OutputJournalError,
};
use matching_core::order::{Command, Order};
use matching_core::runtime_loop::{run_symbol_runtime_step, spawn_symbol_runtime_once};
use matching_core::symbol_runtime::SymbolRuntime;
use matching_core::types::{
    CommandId, JournalSeq, OrderId, Price, Quantity, Side, Symbol,
};

struct TestOutputJournal {
    entries: Vec<OutputJournalEntry>,
}

impl TestOutputJournal {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
}

impl OutputJournal for TestOutputJournal {
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

fn command_entry(seq: u64) -> InputJournalEntry {
    InputJournalEntry {
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
fn runtime_loop_step_is_available_from_public_api() {
    let mut queue = PerSymbolInputQueue::new(4);
    let mut runtime = SymbolRuntime::new(symbol());
    let mut output = TestOutputJournal::new();

    assert_eq!(queue.enqueue(command_entry(1)), Ok(()));
    assert_eq!(queue.enqueue(command_entry(2)), Ok(()));

    assert_eq!(
        run_symbol_runtime_step(&mut runtime, &mut queue, &mut output, 10),
        Ok(2)
    );

    assert_eq!(runtime.last_input_seq(), Some(JournalSeq(2)));
    assert_eq!(queue.len(), 0);
    assert_eq!(output.read_all().len(), 2);
}

#[test]
fn one_shot_symbol_runtime_worker_is_available_from_public_api() {
    let mut queue = PerSymbolInputQueue::new(4);
    let runtime = SymbolRuntime::new(symbol());
    let output = TestOutputJournal::new();

    assert_eq!(queue.enqueue(command_entry(1)), Ok(()));
    assert_eq!(queue.enqueue(command_entry(2)), Ok(()));

    let handle = spawn_symbol_runtime_once(runtime, queue, output, 10);

    let (runtime, queue, output, result) = handle
        .join()
        .expect("worker thread must finish successfully");

    assert_eq!(result, Ok(2));
    assert_eq!(runtime.last_input_seq(), Some(JournalSeq(2)));
    assert_eq!(queue.len(), 0);
    assert_eq!(output.read_all().len(), 2);
}