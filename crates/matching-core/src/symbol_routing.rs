use crate::bounded_handoff::BoundedHandoff;
use crate::journal_adapter::JournalInputEntry;
use crate::types::Symbol;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SymbolRoutingError {
    UnknownSymbol,
    QueueFull,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutedInput {
    pub symbol: Symbol,
    pub entry: JournalInputEntry,
}

pub struct SymbolRouting {
    symbols: HashSet<Symbol>,
}

impl SymbolRouting {
    pub fn new() -> Self {
        Self {
            symbols: HashSet::new(),
        }
    }

    pub fn add_symbol(&mut self, symbol: Symbol) {
        self.symbols.insert(symbol);
    }

    pub fn route_batch(
        &self,
        entries: Vec<JournalInputEntry>,
    ) -> Result<HashMap<Symbol, Vec<JournalInputEntry>>, SymbolRoutingError> {
        let mut routed = HashMap::new();

        for entry in entries {
            let routed_input = self.route_entry(entry)?;

            routed
                .entry(routed_input.symbol)
                .or_insert_with(Vec::new)
                .push(routed_input.entry);
        }

        Ok(routed)
    }

    pub fn route_entry(&self, entry: JournalInputEntry) -> Result<RoutedInput, SymbolRoutingError> {
        let symbol = entry.command.symbol().clone();

        if !self.symbols.contains(&symbol) {
            return Err(SymbolRoutingError::UnknownSymbol);
        }

        Ok(RoutedInput { symbol, entry })
    }

    pub fn route_batch_to_queues(
        &self,
        entries: Vec<JournalInputEntry>,
        queues: &mut HashMap<Symbol, BoundedHandoff>,
    ) -> Result<usize, SymbolRoutingError> {
        let mut routed = 0;

        for entry in entries {
            self.route_entry_to_queue(entry, queues)?;
            routed += 1;
        }

        Ok(routed)
    }

    pub fn route_entry_to_queue(
        &self,
        entry: JournalInputEntry,
        queues: &mut HashMap<Symbol, BoundedHandoff>,
    ) -> Result<(), SymbolRoutingError> {
        let routed = self.route_entry(entry)?;

        let queue = queues
            .get_mut(&routed.symbol)
            .ok_or(SymbolRoutingError::UnknownSymbol)?;

        queue
            .enqueue(routed.entry)
            .map_err(|_| SymbolRoutingError::QueueFull)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal_adapter::JournalInputEntry;
    use crate::order::{Command, Order};
    use crate::types::{CommandId, JournalSeq, OrderId, Price, Quantity, Side, Symbol};

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
    fn router_routes_entry_for_registered_symbol() {
        let mut router = SymbolRouting::new();
        router.add_symbol(btc());

        let entry = input_entry(1, 10, 100, btc());

        let routed = router.route_entry(entry).unwrap();

        assert_eq!(routed.symbol, btc());
        assert_eq!(routed.entry.seq, JournalSeq(1));
        assert_eq!(routed.entry.command_id, CommandId(10));
    }

    #[test]
    fn router_rejects_entry_for_unknown_symbol() {
        let mut router = SymbolRouting::new();
        router.add_symbol(btc());

        let entry = input_entry(1, 10, 100, eth());

        assert_eq!(
            router.route_entry(entry),
            Err(SymbolRoutingError::UnknownSymbol)
        );
    }

    #[test]
    fn router_groups_batch_by_symbol_and_preserves_per_symbol_order() {
        let mut router = SymbolRouting::new();
        router.add_symbol(btc());
        router.add_symbol(eth());

        let entries = vec![
            input_entry(1, 10, 100, btc()),
            input_entry(2, 11, 200, eth()),
            input_entry(3, 12, 101, btc()),
            input_entry(4, 13, 201, eth()),
        ];

        let routed = router.route_batch(entries).unwrap();

        let btc_entries = routed.get(&btc()).unwrap();
        let eth_entries = routed.get(&eth()).unwrap();

        assert_eq!(btc_entries.len(), 2);
        assert_eq!(btc_entries[0].seq, JournalSeq(1));
        assert_eq!(btc_entries[1].seq, JournalSeq(3));

        assert_eq!(eth_entries.len(), 2);
        assert_eq!(eth_entries[0].seq, JournalSeq(2));
        assert_eq!(eth_entries[1].seq, JournalSeq(4));
    }

    #[test]
    fn router_batch_returns_error_when_any_entry_has_unknown_symbol() {
        let mut router = SymbolRouting::new();
        router.add_symbol(btc());

        let entries = vec![
            input_entry(1, 10, 100, btc()),
            input_entry(2, 11, 200, eth()),
            input_entry(3, 12, 101, btc()),
        ];

        assert_eq!(
            router.route_batch(entries),
            Err(SymbolRoutingError::UnknownSymbol)
        );
    }

    #[test]
    fn router_enqueues_entry_to_matching_symbol_queue() {
        let mut router = SymbolRouting::new();
        router.add_symbol(btc());
        router.add_symbol(eth());

        let mut queues = HashMap::new();
        queues.insert(btc(), BoundedHandoff::new(2));
        queues.insert(eth(), BoundedHandoff::new(2));

        assert_eq!(
            router.route_entry_to_queue(input_entry(1, 10, 100, btc()), &mut queues,),
            Ok(())
        );

        let btc_entries = queues.get_mut(&btc()).unwrap().drain_batch(10);
        let eth_entries = queues.get_mut(&eth()).unwrap().drain_batch(10);

        assert_eq!(btc_entries.len(), 1);
        assert_eq!(btc_entries[0].seq, JournalSeq(1));
        assert_eq!(eth_entries.len(), 0);
    }

    #[test]
    fn router_returns_queue_full_when_matching_symbol_queue_is_full() {
        let mut router = SymbolRouting::new();
        router.add_symbol(btc());

        let mut queues = HashMap::new();
        queues.insert(btc(), BoundedHandoff::new(1));

        assert_eq!(
            router.route_entry_to_queue(input_entry(1, 10, 100, btc()), &mut queues,),
            Ok(())
        );

        assert_eq!(
            router.route_entry_to_queue(input_entry(2, 11, 101, btc()), &mut queues,),
            Err(SymbolRoutingError::QueueFull)
        );

        let btc_entries = queues.get_mut(&btc()).unwrap().drain_batch(10);

        assert_eq!(btc_entries.len(), 1);
        assert_eq!(btc_entries[0].seq, JournalSeq(1));
    }

    #[test]
    fn router_enqueues_batch_to_matching_symbol_queues_in_input_order() {
        let mut router = SymbolRouting::new();
        router.add_symbol(btc());
        router.add_symbol(eth());

        let mut queues = HashMap::new();
        queues.insert(btc(), BoundedHandoff::new(4));
        queues.insert(eth(), BoundedHandoff::new(4));

        let entries = vec![
            input_entry(1, 10, 100, btc()),
            input_entry(2, 11, 200, eth()),
            input_entry(3, 12, 101, btc()),
            input_entry(4, 13, 201, eth()),
        ];

        assert_eq!(router.route_batch_to_queues(entries, &mut queues), Ok(4));

        let btc_entries = queues.get_mut(&btc()).unwrap().drain_batch(10);
        let eth_entries = queues.get_mut(&eth()).unwrap().drain_batch(10);

        assert_eq!(btc_entries.len(), 2);
        assert_eq!(btc_entries[0].seq, JournalSeq(1));
        assert_eq!(btc_entries[1].seq, JournalSeq(3));

        assert_eq!(eth_entries.len(), 2);
        assert_eq!(eth_entries[0].seq, JournalSeq(2));
        assert_eq!(eth_entries[1].seq, JournalSeq(4));
    }

    #[test]
    fn router_batch_to_queues_stops_at_queue_full_and_keeps_prior_enqueues() {
        let mut router = SymbolRouting::new();
        router.add_symbol(btc());
        router.add_symbol(eth());

        let mut queues = HashMap::new();
        queues.insert(btc(), BoundedHandoff::new(1));
        queues.insert(eth(), BoundedHandoff::new(2));

        let entries = vec![
            input_entry(1, 10, 100, btc()),
            input_entry(2, 11, 101, btc()),
            input_entry(3, 12, 200, eth()),
        ];

        assert_eq!(
            router.route_batch_to_queues(entries, &mut queues),
            Err(SymbolRoutingError::QueueFull)
        );

        let btc_entries = queues.get_mut(&btc()).unwrap().drain_batch(10);
        let eth_entries = queues.get_mut(&eth()).unwrap().drain_batch(10);

        assert_eq!(btc_entries.len(), 1);
        assert_eq!(btc_entries[0].seq, JournalSeq(1));

        assert_eq!(eth_entries.len(), 0);
    }

    #[test]
    fn router_batch_to_queues_stops_at_unknown_symbol_and_keeps_prior_enqueues() {
        let mut router = SymbolRouting::new();
        router.add_symbol(btc());

        let mut queues = HashMap::new();
        queues.insert(btc(), BoundedHandoff::new(2));

        let entries = vec![
            input_entry(1, 10, 100, btc()),
            input_entry(2, 11, 200, eth()),
            input_entry(3, 12, 101, btc()),
        ];

        assert_eq!(
            router.route_batch_to_queues(entries, &mut queues),
            Err(SymbolRoutingError::UnknownSymbol)
        );

        let btc_entries = queues.get_mut(&btc()).unwrap().drain_batch(10);

        assert_eq!(btc_entries.len(), 1);
        assert_eq!(btc_entries[0].seq, JournalSeq(1));
    }
}
