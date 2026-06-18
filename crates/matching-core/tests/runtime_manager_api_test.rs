use matching_core::engine::EngineEvent;
use matching_core::journal_adapter::{
    JournalAdapterError, JournalInputEntry, JournalOutputAppender, JournalOutputEntry,
};
use matching_core::order::{Command, Order};
use matching_core::runtime_manager::{RuntimeManager, RuntimeManagerError};
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

fn command_entry(seq: u64, symbol: Symbol) -> JournalInputEntry {
    JournalInputEntry {
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
    let mut output = TestJournalOutputAppender::new();

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
    let mut output = TestJournalOutputAppender::new();

    manager.add_symbol(btc);

    assert_eq!(
        manager.process_entry(command_entry(1, eth), &mut output),
        Err(RuntimeManagerError::UnknownSymbol)
    );
}
