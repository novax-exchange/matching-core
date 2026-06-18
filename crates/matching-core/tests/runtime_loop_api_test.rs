use matching_core::bounded_handoff::BoundedHandoff;
use matching_core::engine::EngineEvent;
use matching_core::journal_adapter::{
    JournalAdapterError, JournalInputEntry, JournalOutputAppender, JournalOutputEntry,
};
use matching_core::order::{Command, Order};
use matching_core::output_queue::OutputQueue;
use matching_core::runtime_loop::{
    run_symbol_runtime_step, run_symbol_runtime_step_to_output_queue, spawn_symbol_runtime_once,
};
use matching_core::symbol_runtime::SymbolRuntime;
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
fn runtime_loop_step_is_available_from_public_api() {
    let mut queue = BoundedHandoff::new(4);
    let mut runtime = SymbolRuntime::new(symbol());
    let mut output = TestJournalOutputAppender::new();

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
fn runtime_loop_step_to_output_queue_is_available_from_public_api() {
    let mut handoff = BoundedHandoff::new(4);
    let mut output_queue = OutputQueue::new(4);
    let mut runtime = SymbolRuntime::new(symbol());

    assert_eq!(handoff.enqueue(command_entry(1)), Ok(()));
    assert_eq!(handoff.enqueue(command_entry(2)), Ok(()));

    assert_eq!(
        run_symbol_runtime_step_to_output_queue(&mut runtime, &mut handoff, &mut output_queue, 10,),
        Ok(2)
    );

    assert_eq!(runtime.last_input_seq(), None);
    assert_eq!(handoff.len(), 0);

    let requests = output_queue.drain_batch(10);
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].journal_seq, JournalSeq(1));
    assert_eq!(requests[1].journal_seq, JournalSeq(2));
}

#[test]
fn one_shot_symbol_runtime_worker_is_available_from_public_api() {
    let mut queue = BoundedHandoff::new(4);
    let runtime = SymbolRuntime::new(symbol());
    let output = TestJournalOutputAppender::new();

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
