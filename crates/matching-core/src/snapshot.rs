use crate::order::Order;
use crate::order_book::OrderBook;
use crate::types::*;

pub struct OrderBookSnapshot {
    pub symbol: Symbol,
    pub last_input_seq: JournalSeq,
    pub checksum: Checksum,
    pub resting_orders: Vec<Order>,
}

impl OrderBookSnapshot {
    pub fn from_order_book(book: &OrderBook, last_input_seq: JournalSeq) -> Self {
        Self {
            symbol: book.symbol().clone(),
            last_input_seq,
            checksum: book.checksum(),
            resting_orders: book.resting_orders(),
        }
    }

    pub fn restore_order_book(&self) -> OrderBook {
        let mut book = OrderBook::new(self.symbol.clone());

        for order in self.resting_orders.clone() {
            book.insert(order);
        }

        book
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal_adapter::{JournalInputEntry, JournalInputReader};
    use crate::order::Command;
    use crate::order::Order;
    use crate::order_book::OrderBook;
    use crate::replay::ReplayRunner;
    use crate::types::CommandId;

    #[test]
    fn order_book_snapshot_captures_symbol_sequence_and_checksum() {
        let snapshot = OrderBookSnapshot {
            symbol: Symbol("BTC-USDT".to_string()),
            last_input_seq: JournalSeq(10),
            checksum: Checksum(123),
            resting_orders: Vec::new(),
        };

        assert_eq!(snapshot.symbol, Symbol("BTC-USDT".to_string()));
        assert_eq!(snapshot.last_input_seq, JournalSeq(10));
        assert_eq!(snapshot.checksum, Checksum(123));
    }

    #[test]
    fn order_book_snapshot_captures_resting_orders() {
        let order = Order {
            order_id: OrderId(1),
            symbol: Symbol("BTC-USDT".to_string()),
            side: Side::Buy,
            price: Price(100),
            quantity: Quantity(5),
        };

        let snapshot = OrderBookSnapshot {
            symbol: Symbol("BTC-USDT".to_string()),
            last_input_seq: JournalSeq(10),
            checksum: Checksum(123),
            resting_orders: vec![order.clone()],
        };

        assert_eq!(snapshot.resting_orders, vec![order]);
    }

    #[test]
    fn snapshot_can_be_created_from_order_book() {
        let symbol = Symbol("BTC-USDT".to_string());
        let mut book = OrderBook::new(symbol.clone());

        let order = Order {
            order_id: OrderId(1),
            symbol: symbol.clone(),
            side: Side::Buy,
            price: Price(100),
            quantity: Quantity(5),
        };

        book.insert(order.clone());

        let snapshot = OrderBookSnapshot::from_order_book(&book, JournalSeq(10));

        assert_eq!(snapshot.symbol, symbol);
        assert_eq!(snapshot.last_input_seq, JournalSeq(10));
        assert_eq!(snapshot.checksum, book.checksum());
        assert_eq!(snapshot.resting_orders, vec![order]);
    }

    #[test]
    fn snapshot_can_restore_order_book_with_same_checksum() {
        let symbol = Symbol("BTC-USDT".to_string());
        let mut original = OrderBook::new(symbol.clone());

        original.insert(Order {
            order_id: OrderId(1),
            symbol: symbol.clone(),
            side: Side::Buy,
            price: Price(100),
            quantity: Quantity(5),
        });

        original.insert(Order {
            order_id: OrderId(2),
            symbol: symbol.clone(),
            side: Side::Sell,
            price: Price(105),
            quantity: Quantity(3),
        });

        let snapshot = OrderBookSnapshot::from_order_book(&original, JournalSeq(10));

        let restored = snapshot.restore_order_book();

        assert_eq!(restored.symbol(), &symbol);
        assert_eq!(restored.checksum(), snapshot.checksum);
        assert_eq!(restored.checksum(), original.checksum());
        assert_eq!(restored.resting_orders(), snapshot.resting_orders);
    }

    struct InMemoryJournalInputReader {
        entries: Vec<JournalInputEntry>,
    }

    impl InMemoryJournalInputReader {
        fn new() -> Self {
            Self {
                entries: Vec::new(),
            }
        }
    }

    impl JournalInputReader for InMemoryJournalInputReader {
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

    #[test]
    fn snapshot_restore_then_replay_from_next_sequence_matches_full_replay() {
        let symbol = Symbol("BTC-USDT".to_string());
        let mut journal = InMemoryJournalInputReader::new();

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

        let mut partial_book = OrderBook::new(symbol.clone());
        partial_book.insert(Order {
            order_id: OrderId(1),
            symbol: symbol.clone(),
            side: Side::Buy,
            price: Price(100),
            quantity: Quantity(5),
        });

        let snapshot = OrderBookSnapshot::from_order_book(&partial_book, JournalSeq(1));
        let restored = snapshot.restore_order_book();

        let resumed_checksum = ReplayRunner::new(symbol.clone()).replay_from_order_book(
            restored,
            &journal,
            JournalSeq(snapshot.last_input_seq.0 + 1),
        );

        assert_eq!(resumed_checksum, full_checksum);
    }
}
