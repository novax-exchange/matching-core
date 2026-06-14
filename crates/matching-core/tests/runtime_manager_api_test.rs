use matching_core::engine::EngineEvent;
use matching_core::journal::{
    InputJournalEntry, OutputJournal, OutputJournalEntry, OutputJournalError,
};
use matching_core::order::{Command, Order};
use matching_core::runtime_manager::{RuntimeManager, RuntimeManagerError};
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

fn command_entry(seq: u64, symbol: Symbol) -> InputJournalEntry {
    InputJournalEntry {
        seq: JournalSeq(seq),
        command_id: CommandId(seq),
        command: Command::PlaceLimit(Order {
            order_id: OrderId(seq),
            symbol,
            side: Side::Buy,
            price: Price(100),
            quantity: Quantity(1),
        }),
    }
}

#[test]
fn runtime_manager_is_available_from_public_api() {
    let btc = Symbol("BTC-USDT".to_string());
    let mut manager = RuntimeManager::new();
    let mut output = TestOutputJournal::new();

    manager.add_symbol(btc.clone());

    assert_eq!(
        manager.process_entry(command_entry(1, btc.clone()), &mut output),
        Ok(())
    );
    assert_eq!(manager.last_input_seq(&btc), Some(Some(JournalSeq(1))));
}

#[test]
fn runtime_manager_error_is_available_from_public_api() {
    let btc = Symbol("BTC-USDT".to_string());
    let eth = Symbol("ETH-USDT".to_string());
    let mut manager = RuntimeManager::new();
    let mut output = TestOutputJournal::new();

    manager.add_symbol(btc);

    assert_eq!(
        manager.process_entry(command_entry(1, eth), &mut output),
        Err(RuntimeManagerError::UnknownSymbol)
    );
}