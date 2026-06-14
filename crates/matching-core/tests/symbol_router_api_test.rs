use matching_core::journal::InputJournalEntry;
use matching_core::order::{Command, Order};
use matching_core::symbol_router::{SymbolRouter, SymbolRouterError};
use matching_core::types::{
    CommandId, JournalSeq, OrderId, Price, Quantity, Side, Symbol,
};
use matching_core::input_queue::PerSymbolInputQueue;
use std::collections::HashMap;

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
fn symbol_router_is_available_from_public_api() {
    let btc = Symbol("BTC-USDT".to_string());
    let mut router = SymbolRouter::new();

    router.add_symbol(btc.clone());

    let routed = router.route_entry(command_entry(1, btc.clone())).unwrap();

    assert_eq!(routed.symbol, btc);
    assert_eq!(routed.entry.seq, JournalSeq(1));
}

#[test]
fn symbol_router_error_is_available_from_public_api() {
    let btc = Symbol("BTC-USDT".to_string());
    let eth = Symbol("ETH-USDT".to_string());
    let mut router = SymbolRouter::new();

    router.add_symbol(btc);

    assert_eq!(
        router.route_entry(command_entry(1, eth)),
        Err(SymbolRouterError::UnknownSymbol)
    );
}

#[test]
fn symbol_router_can_enqueue_to_public_input_queue_api() {
    let btc = Symbol("BTC-USDT".to_string());
    let mut router = SymbolRouter::new();
    router.add_symbol(btc.clone());

    let mut queues = HashMap::new();
    queues.insert(btc.clone(), PerSymbolInputQueue::new(1));

    assert_eq!(
        router.route_entry_to_queue(command_entry(1, btc.clone()), &mut queues),
        Ok(())
    );

    assert_eq!(
        router.route_entry_to_queue(command_entry(2, btc.clone()), &mut queues),
        Err(SymbolRouterError::QueueFull)
    );
}