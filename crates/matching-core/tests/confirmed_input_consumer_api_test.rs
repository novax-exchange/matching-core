use matching_core::bounded_handoff::BoundedHandoff;
use matching_core::confirmed_input_consumer::ConfirmedInputConsumer;
use matching_core::journal_adapter::{JournalInputEntry, JournalInputReader};
use matching_core::order::{Command, Order};
use matching_core::symbol_routing::SymbolRouting;
use matching_core::types::{CommandId, JournalSeq, OrderId, Price, Quantity, Side, Symbol};
use std::collections::HashMap;

struct TestJournalInputReader {
    entries: Vec<JournalInputEntry>,
}

impl TestJournalInputReader {
    fn new(entries: Vec<JournalInputEntry>) -> Self {
        Self { entries }
    }
}

impl JournalInputReader for TestJournalInputReader {
    fn append(&mut self, command_id: CommandId, command: Command) -> JournalSeq {
        let seq = JournalSeq(self.entries.len() as u64 + 1);
        self.entries.push(JournalInputEntry {
            seq,
            command_id,
            command,
        });
        seq
    }

    fn read_from(&self, from: JournalSeq) -> Vec<JournalInputEntry> {
        self.entries
            .iter()
            .filter(|entry| entry.seq >= from)
            .cloned()
            .collect()
    }

    fn latest_seq(&self) -> Option<JournalSeq> {
        self.entries.last().map(|entry| entry.seq)
    }
}

fn btc() -> Symbol {
    Symbol("BTC-USDT".to_string())
}

fn command_entry(seq: u64) -> JournalInputEntry {
    JournalInputEntry {
        seq: JournalSeq(seq),
        command_id: CommandId(seq),
        command: Command::PlaceLimit(Order {
            order_id: OrderId(seq),
            symbol: btc(),
            side: Side::Buy,
            price: Price(100),
            quantity: Quantity(1),
        }),
    }
}

#[test]
fn confirmed_input_consumer_is_available_from_public_api() {
    let journal = TestJournalInputReader::new(vec![command_entry(1)]);

    let mut routing = SymbolRouting::new();
    routing.add_symbol(btc());

    let mut handoffs = HashMap::new();
    handoffs.insert(btc(), BoundedHandoff::new(2));

    let mut consumer = ConfirmedInputConsumer::new(JournalSeq(1), 10);

    assert_eq!(consumer.poll_once(&journal, &routing, &mut handoffs), Ok(1));
    assert_eq!(consumer.next_seq(), JournalSeq(2));
}
