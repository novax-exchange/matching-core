use crate::bounded_handoff::BoundedHandoff;
use crate::journal_adapter::{JournalInputEntry, JournalInputReader};
use crate::symbol_routing::{SymbolRouting, SymbolRoutingError};
use crate::types::{JournalSeq, Symbol};
use std::collections::HashMap;

pub struct ConfirmedInputConsumer {
    next_seq: JournalSeq,
    max_batch_entries: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfirmedInputConsumerError {
    Routing(SymbolRoutingError),
    SequenceGap {
        expected: JournalSeq,
        actual: JournalSeq,
    },
}

impl From<SymbolRoutingError> for ConfirmedInputConsumerError {
    fn from(error: SymbolRoutingError) -> Self {
        Self::Routing(error)
    }
}

impl ConfirmedInputConsumer {
    pub fn new(next_seq: JournalSeq, max_batch_entries: usize) -> Self {
        Self {
            next_seq,
            max_batch_entries,
        }
    }

    pub fn next_seq(&self) -> JournalSeq {
        self.next_seq
    }

    pub fn poll_once(
        &mut self,
        journal: &dyn JournalInputReader,
        routing: &SymbolRouting,
        handoffs: &mut HashMap<Symbol, BoundedHandoff>,
    ) -> Result<usize, ConfirmedInputConsumerError> {
        let entries: Vec<JournalInputEntry> = journal
            .read_from(self.next_seq)
            .into_iter()
            .take(self.max_batch_entries)
            .collect();

        let mut expected_seq = self.next_seq;
        for entry in &entries {
            if entry.seq != expected_seq {
                return Err(ConfirmedInputConsumerError::SequenceGap {
                    expected: expected_seq,
                    actual: entry.seq,
                });
            }

            expected_seq = JournalSeq(expected_seq.0 + 1);
        }

        let routed = routing.route_batch(entries)?;
        let consumed = routed.values().map(Vec::len).sum::<usize>();

        for (symbol, entries) in &routed {
            let handoff = handoffs
                .get(symbol)
                .ok_or(SymbolRoutingError::UnknownSymbol)?;

            if handoff.available_capacity() < entries.len() {
                return Err(SymbolRoutingError::QueueFull.into());
            }
        }

        for (symbol, entries) in routed {
            let handoff = handoffs
                .get_mut(&symbol)
                .ok_or(SymbolRoutingError::UnknownSymbol)?;

            for entry in entries {
                handoff
                    .enqueue(entry)
                    .map_err(|_| SymbolRoutingError::QueueFull)?;
            }
        }

        self.next_seq = JournalSeq(self.next_seq.0 + consumed as u64);

        Ok(consumed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bounded_handoff::BoundedHandoff;
    use crate::journal_adapter::{JournalInputEntry, JournalInputReader};
    use crate::order::{Command, Order};
    use crate::symbol_routing::SymbolRouting;
    use crate::types::{CommandId, JournalSeq, OrderId, Price, Quantity, Side, Symbol};
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

    fn eth() -> Symbol {
        Symbol("ETH-USDT".to_string())
    }

    fn input_entry(seq: u64, command_id: u64, order_id: u64, symbol: Symbol) -> JournalInputEntry {
        JournalInputEntry {
            seq: JournalSeq(seq),
            command_id: CommandId(command_id),
            command: Command::PlaceLimit(Order {
                order_id: OrderId(order_id),
                symbol,
                side: Side::Buy,
                price: Price(100),
                quantity: Quantity(5),
            }),
        }
    }

    #[test]
    fn consumer_routes_confirmed_entries_to_symbol_handoffs() {
        let journal = TestJournalInputReader::new(vec![
            input_entry(1, 10, 100, btc()),
            input_entry(2, 11, 200, eth()),
        ]);

        let mut routing = SymbolRouting::new();
        routing.add_symbol(btc());
        routing.add_symbol(eth());

        let mut handoffs = HashMap::new();
        handoffs.insert(btc(), BoundedHandoff::new(4));
        handoffs.insert(eth(), BoundedHandoff::new(4));

        let mut consumer = ConfirmedInputConsumer::new(JournalSeq(1), 10);

        let consumed = consumer.poll_once(&journal, &routing, &mut handoffs);

        assert_eq!(consumed, Ok(2));
        assert_eq!(consumer.next_seq(), JournalSeq(3));

        let btc_entries = handoffs.get_mut(&btc()).unwrap().drain_batch(10);
        let eth_entries = handoffs.get_mut(&eth()).unwrap().drain_batch(10);

        assert_eq!(btc_entries.len(), 1);
        assert_eq!(btc_entries[0].seq, JournalSeq(1));

        assert_eq!(eth_entries.len(), 1);
        assert_eq!(eth_entries[0].seq, JournalSeq(2));
    }

    #[test]
    fn consumer_rejects_unknown_symbol_without_advancing_or_enqueuing_partial_batch() {
        let journal = TestJournalInputReader::new(vec![
            input_entry(1, 10, 100, btc()),
            input_entry(2, 11, 200, eth()),
        ]);

        let mut routing = SymbolRouting::new();
        routing.add_symbol(btc());

        let mut handoffs = HashMap::new();
        handoffs.insert(btc(), BoundedHandoff::new(4));

        let mut consumer = ConfirmedInputConsumer::new(JournalSeq(1), 10);

        let consumed = consumer.poll_once(&journal, &routing, &mut handoffs);

        assert_eq!(
            consumed,
            Err(ConfirmedInputConsumerError::Routing(
                SymbolRoutingError::UnknownSymbol
            ))
        );
        assert_eq!(consumer.next_seq(), JournalSeq(1));
        assert_eq!(handoffs.get_mut(&btc()).unwrap().drain_batch(10).len(), 0);
    }

    #[test]
    fn consumer_rejects_full_handoff_without_advancing_or_enqueuing_partial_batch() {
        let journal = TestJournalInputReader::new(vec![
            input_entry(1, 10, 100, btc()),
            input_entry(2, 11, 101, btc()),
        ]);

        let mut routing = SymbolRouting::new();
        routing.add_symbol(btc());

        let mut handoffs = HashMap::new();
        handoffs.insert(btc(), BoundedHandoff::new(1));

        let mut consumer = ConfirmedInputConsumer::new(JournalSeq(1), 10);

        let consumed = consumer.poll_once(&journal, &routing, &mut handoffs);

        assert_eq!(
            consumed,
            Err(ConfirmedInputConsumerError::Routing(
                SymbolRoutingError::QueueFull
            ))
        );
        assert_eq!(consumer.next_seq(), JournalSeq(1));
        assert_eq!(handoffs.get_mut(&btc()).unwrap().drain_batch(10).len(), 0);
    }

    #[test]
    fn consumer_reads_at_most_configured_batch_size() {
        let journal = TestJournalInputReader::new(vec![
            input_entry(1, 10, 100, btc()),
            input_entry(2, 11, 101, btc()),
            input_entry(3, 12, 102, btc()),
        ]);

        let mut routing = SymbolRouting::new();
        routing.add_symbol(btc());

        let mut handoffs = HashMap::new();
        handoffs.insert(btc(), BoundedHandoff::new(4));

        let mut consumer = ConfirmedInputConsumer::new(JournalSeq(1), 2);

        let consumed = consumer.poll_once(&journal, &routing, &mut handoffs);

        assert_eq!(consumed, Ok(2));
        assert_eq!(consumer.next_seq(), JournalSeq(3));

        let btc_entries = handoffs.get_mut(&btc()).unwrap().drain_batch(10);

        assert_eq!(btc_entries.len(), 2);
        assert_eq!(btc_entries[0].seq, JournalSeq(1));
        assert_eq!(btc_entries[1].seq, JournalSeq(2));
    }

    #[test]
    fn consumer_rejects_sequence_gap_without_advancing_or_enqueuing() {
        let journal = TestJournalInputReader::new(vec![
            input_entry(1, 10, 100, btc()),
            input_entry(3, 12, 102, btc()),
        ]);

        let mut routing = SymbolRouting::new();
        routing.add_symbol(btc());

        let mut handoffs = HashMap::new();
        handoffs.insert(btc(), BoundedHandoff::new(4));

        let mut consumer = ConfirmedInputConsumer::new(JournalSeq(1), 10);

        let consumed = consumer.poll_once(&journal, &routing, &mut handoffs);

        assert_eq!(
            consumed,
            Err(ConfirmedInputConsumerError::SequenceGap {
                expected: JournalSeq(2),
                actual: JournalSeq(3),
            })
        );
        assert_eq!(consumer.next_seq(), JournalSeq(1));
        assert_eq!(handoffs.get_mut(&btc()).unwrap().drain_batch(10).len(), 0);
    }
}
