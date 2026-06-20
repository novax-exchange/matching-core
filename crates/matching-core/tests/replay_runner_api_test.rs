use matching_core::journal_adapter::{JournalInputEntry, JournalInputReader};
use matching_core::order::{Command, Order};
use matching_core::replay_runner::ReplayRunner;
use matching_core::types::{
    Checksum, CommandId, JournalSeq, OrderId, Price, Quantity, Side, Symbol,
};

struct TestJournalInputReader {
    entries: Vec<JournalInputEntry>,
}

impl TestJournalInputReader {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
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

fn symbol() -> Symbol {
    Symbol("BTC-USDT".to_string())
}

fn limit_order(order_id: u64, side: Side, price: u64, quantity: u64) -> Command {
    Command::PlaceLimit(Order {
        order_id: OrderId(order_id),
        symbol: symbol(),
        side,
        price: Price(price),
        quantity: Quantity(quantity),
    })
}

#[test]
fn replay_runner_is_available_from_public_api() {
    let mut journal = TestJournalInputReader::new();

    journal.append(CommandId(1), limit_order(1, Side::Buy, 100, 5));

    let checksum = ReplayRunner::new(symbol()).replay(&journal);

    assert_ne!(checksum, Checksum(0));
}

#[test]
fn replay_result_is_available_from_public_api() {
    let mut journal = TestJournalInputReader::new();

    journal.append(CommandId(1), limit_order(1, Side::Buy, 100, 5));

    let result = ReplayRunner::new(symbol()).replay_result(&journal);

    assert_ne!(result.checksum, Checksum(0));
    assert_eq!(result.last_replayed_seq, Some(JournalSeq(1)));
}
