use matching_core::journal::{InputJournal, InputJournalEntry};
use matching_core::order::{Command, Order};
use matching_core::order_book::OrderBook;
use matching_core::replay::ReplayRunner;
use matching_core::snapshot::OrderBookSnapshot;
use matching_core::types::*;

#[test]
fn snapshot_can_be_created_and_restored_from_public_api() {
    let symbol = Symbol("BTC-USDT".to_string());
    let mut book = OrderBook::new(symbol.clone());

    book.insert(Order {
        order_id: OrderId(1),
        symbol: symbol.clone(),
        side: Side::Buy,
        price: Price(100),
        quantity: Quantity(5),
    });

    let snapshot = OrderBookSnapshot::from_order_book(&book, JournalSeq(10));
    let restored = snapshot.restore_order_book();

    assert_eq!(snapshot.last_input_seq, JournalSeq(10));
    assert_eq!(restored.symbol(), &symbol);
    assert_eq!(restored.checksum(), book.checksum());
    assert_eq!(restored.resting_orders(), snapshot.resting_orders);
}

struct TestInputJournal {
    entries: Vec<InputJournalEntry>,
}

impl TestInputJournal {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
}

impl InputJournal for TestInputJournal {
    fn append(&mut self, command_id: CommandId, command: Command) -> JournalSeq {
        let seq = JournalSeq(self.entries.len() as u64 + 1);

        self.entries.push(InputJournalEntry {
            seq,
            command_id,
            command,
        });

        seq
    }

    fn read_from(&self, from: JournalSeq) -> Vec<InputJournalEntry> {
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

#[test]
fn restored_snapshot_can_continue_replay_from_public_api() {
    let symbol = Symbol("BTC-USDT".to_string());

    let mut snapshot_book = OrderBook::new(symbol.clone());
    snapshot_book.insert(Order {
        order_id: OrderId(1),
        symbol: symbol.clone(),
        side: Side::Buy,
        price: Price(100),
        quantity: Quantity(5),
    });

    let snapshot = OrderBookSnapshot::from_order_book(&snapshot_book, JournalSeq(1));
    let restored = snapshot.restore_order_book();

    let mut journal = TestInputJournal::new();
    journal.append(
        CommandId(1),
        Command::PlaceLimit(Order {
            order_id: OrderId(1),
            symbol: symbol.clone(),
            side: Side::Buy,
            price: Price(100),
            quantity: Quantity(5),
        }),
    );
    journal.append(
        CommandId(2),
        Command::PlaceLimit(Order {
            order_id: OrderId(2),
            symbol: symbol.clone(),
            side: Side::Sell,
            price: Price(105),
            quantity: Quantity(3),
        }),
    );

    let full_checksum = ReplayRunner::new(symbol.clone()).replay(&journal);
    let resumed_checksum = ReplayRunner::new(symbol).replay_from_order_book(
        restored,
        &journal,
        JournalSeq(snapshot.last_input_seq.0 + 1),
    );

    assert_ne!(full_checksum, Checksum(0));
    assert_eq!(resumed_checksum, full_checksum);
}
