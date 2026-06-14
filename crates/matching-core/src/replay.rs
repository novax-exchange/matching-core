use crate::journal::InputJournal;
use crate::order::Command;
use crate::order_book::OrderBook;
use crate::types::{Checksum, JournalSeq, Symbol};

pub struct ReplayRunner {
    symbol: Symbol,
}

pub struct ReplayResult {
    pub checksum: Checksum,
    pub last_replayed_seq: Option<JournalSeq>,
}

impl ReplayRunner {
    pub fn new(symbol: Symbol) -> Self {
        ReplayRunner { symbol }
    }

    pub fn replay(&self, journal: &dyn InputJournal) -> Checksum {
        self.replay_from(journal, JournalSeq(1))
    }

    pub fn replay_from(&self, journal: &dyn InputJournal, from: JournalSeq) -> Checksum {
        let book = OrderBook::new(self.symbol.clone());
        self.replay_from_order_book(book, journal, from)
    }

    pub fn replay_from_order_book(
        &self,
        mut book: OrderBook,
        journal: &dyn InputJournal,
        from: JournalSeq,
    ) -> Checksum {
        for entry in journal.read_from(from) {
            match entry.command {
                Command::PlaceLimit(order) => {
                    book.place_limit(order);
                }
                Command::Cancel { order_id, .. } => {
                    let _ = book.cancel(order_id);
                }
            }
        }

        book.checksum()
    }

    pub fn replay_result(&self, journal: &dyn InputJournal) -> ReplayResult {
        ReplayResult {
            checksum: self.replay_from(journal, JournalSeq(1)),
            last_replayed_seq: journal.latest_seq(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::{InputJournal, InputJournalEntry};
    use crate::order::{Command, Order};
    use crate::types::{Checksum, CommandId, JournalSeq, OrderId, Price, Quantity, Side, Symbol};

    struct InMemoryInputJournal {
        entries: Vec<InputJournalEntry>,
    }

    impl InMemoryInputJournal {
        fn new() -> Self {
            Self {
                entries: Vec::new(),
            }
        }
    }

    impl InputJournal for InMemoryInputJournal {
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
    fn replaying_same_input_journal_produces_same_checksum() {
        let mut journal = InMemoryInputJournal::new();

        journal.append(CommandId(1), limit_order(1, Side::Buy, 100, 5));
        journal.append(CommandId(2), limit_order(2, Side::Sell, 101, 3));
        journal.append(CommandId(3), limit_order(3, Side::Buy, 99, 2));

        let first = ReplayRunner::new(symbol()).replay(&journal);
        let second = ReplayRunner::new(symbol()).replay(&journal);

        assert_eq!(first, second);
        assert_ne!(first, Checksum(0));
    }

    #[test]
    fn replay_applies_cancel_commands_before_calculating_checksum() {
        let mut with_cancel = InMemoryInputJournal::new();

        with_cancel.append(CommandId(1), limit_order(1, Side::Buy, 100, 5));
        with_cancel.append(
            CommandId(2),
            Command::Cancel {
                order_id: OrderId(1),
                symbol: symbol(),
            },
        );

        let empty = InMemoryInputJournal::new();

        let cancelled_checksum = ReplayRunner::new(symbol()).replay(&with_cancel);
        let empty_checksum = ReplayRunner::new(symbol()).replay(&empty);

        assert_eq!(cancelled_checksum, empty_checksum);
    }

    #[test]
    fn replay_from_starts_at_requested_sequence() {
        let mut journal = InMemoryInputJournal::new();

        journal.append(CommandId(1), limit_order(1, Side::Buy, 100, 5));
        journal.append(CommandId(2), limit_order(2, Side::Buy, 101, 3));

        let replay_from_second = ReplayRunner::new(symbol()).replay_from(&journal, JournalSeq(2));

        let mut expected_journal = InMemoryInputJournal::new();
        expected_journal.append(CommandId(2), limit_order(2, Side::Buy, 101, 3));

        let expected = ReplayRunner::new(symbol()).replay(&expected_journal);

        assert_eq!(replay_from_second, expected);
    }

    #[test]
    fn replay_result_exposes_final_checksum() {
        let mut journal = InMemoryInputJournal::new();

        journal.append(CommandId(1), limit_order(1, Side::Buy, 100, 5));

        let result = ReplayRunner::new(symbol()).replay_result(&journal);

        assert_ne!(result.checksum, Checksum(0));
    }

    #[test]
    fn replay_result_exposes_last_replayed_sequence() {
        let mut journal = InMemoryInputJournal::new();

        journal.append(CommandId(1), limit_order(1, Side::Buy, 100, 5));
        journal.append(CommandId(2), limit_order(2, Side::Sell, 101, 3));

        let result = ReplayRunner::new(symbol()).replay_result(&journal);

        assert_eq!(result.last_replayed_seq, Some(JournalSeq(2)));
    }

    #[test]
    fn replay_result_has_no_last_replayed_sequence_for_empty_journal() {
        let journal = InMemoryInputJournal::new();

        let result = ReplayRunner::new(symbol()).replay_result(&journal);

        assert_eq!(result.last_replayed_seq, None);
    }
}
