use crate::journal::InputJournalEntry;
use crate::types::Symbol;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SymbolRouterError {
    UnknownSymbol,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutedInput {
    pub symbol: Symbol,
    pub entry: InputJournalEntry,
}

pub struct SymbolRouter {
    symbols: HashSet<Symbol>
}

impl SymbolRouter {
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
        entries: Vec<InputJournalEntry>,
    ) -> Result<HashMap<Symbol, Vec<InputJournalEntry>>, SymbolRouterError> {
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

    pub fn route_entry(
        &self,
        entry: InputJournalEntry,
    ) -> Result<RoutedInput, SymbolRouterError>{
        let symbol = entry.command.symbol().clone();

        if !self.symbols.contains(&symbol) {
            return Err(SymbolRouterError::UnknownSymbol);
        }

        Ok(RoutedInput { symbol, entry })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::InputJournalEntry;
    use crate::order::{Command, Order};
    use crate::types::{CommandId, JournalSeq, OrderId, Price, Quantity, Side, Symbol};

    fn btc() -> Symbol {
        Symbol("BTC-USDT".to_string())
    }

    fn eth() -> Symbol {
        Symbol("ETH-USDT".to_string())
    }

    fn input_entry(seq: u64, command_id: u64, order_id: u64, symbol: Symbol) -> InputJournalEntry {
        InputJournalEntry {
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
        let mut router = SymbolRouter::new();
        router.add_symbol(btc());

        let entry = input_entry(1, 10, 100, btc());

        let routed = router.route_entry(entry).unwrap();

        assert_eq!(routed.symbol, btc());
        assert_eq!(routed.entry.seq, JournalSeq(1));
        assert_eq!(routed.entry.command_id, CommandId(10));
    }

    #[test]
    fn router_rejects_entry_for_unknown_symbol() {
        let mut router = SymbolRouter::new();
        router.add_symbol(btc());

        let entry = input_entry(1, 10, 100, eth());

        assert_eq!(
            router.route_entry(entry),
            Err(SymbolRouterError::UnknownSymbol)
        );
    }

    #[test]
    fn router_groups_batch_by_symbol_and_preserves_per_symbol_order() {
        let mut router = SymbolRouter::new();
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
        let mut router = SymbolRouter::new();
        router.add_symbol(btc());

        let entries = vec![
            input_entry(1, 10, 100, btc()),
            input_entry(2, 11, 200, eth()),
            input_entry(3, 12, 101, btc()),
        ];

        assert_eq!(
            router.route_batch(entries),
            Err(SymbolRouterError::UnknownSymbol)
        );
    }
}
