mod support;

use matching_core::engine::gateway::{CommandGateway, GatewayResult};
use matching_core::engine::matching::MatchingEngine;
use matching_core::engine::result::TradeEvent;
use matching_core::journal::traits::{InputJournal, OutputJournal};
use matching_core::types::*;
use rust_decimal_macros::dec;
use std::sync::Arc;
use support::in_memory_journal::{InMemoryInputJournal, InMemoryOutputJournal};

fn make_input() -> Arc<InMemoryInputJournal> {
    let journal = Arc::new(InMemoryInputJournal::default());
    let symbol = Symbol("BTCUSDT".into());
    let commands = vec![
        MatchingCommand::PlaceOrder(OrderCommand {
            command_id: CommandId(1),
            order_id: OrderId(1),
            symbol: symbol.clone(),
            side: Side::Bid,
            order_type: OrderType::Limit,
            price: dec!(100),
            quantity: dec!(5),
            config_version: ConfigVersion(1),
            timestamp_ns: 1,
        }),
        MatchingCommand::PlaceOrder(OrderCommand {
            command_id: CommandId(2),
            order_id: OrderId(2),
            symbol: symbol.clone(),
            side: Side::Ask,
            order_type: OrderType::Limit,
            price: dec!(100),
            quantity: dec!(3),
            config_version: ConfigVersion(1),
            timestamp_ns: 2,
        }),
        MatchingCommand::PlaceOrder(OrderCommand {
            command_id: CommandId(3),
            order_id: OrderId(3),
            symbol: symbol.clone(),
            side: Side::Bid,
            order_type: OrderType::Limit,
            price: dec!(99),
            quantity: dec!(2),
            config_version: ConfigVersion(1),
            timestamp_ns: 3,
        }),
        MatchingCommand::CancelOrder(CancelCommand {
            command_id: CommandId(4),
            order_id: OrderId(3),
            symbol: symbol.clone(),
            config_version: ConfigVersion(1),
            timestamp_ns: 4,
        }),
    ];
    for command in commands {
        journal.append(symbol.clone(), command);
    }
    journal
}

fn config() -> SymbolConfig {
    SymbolConfig {
        price_tick: dec!(0.01),
        quantity_tick: dec!(0.001),
        min_quantity: dec!(0.001),
        config_version: ConfigVersion(1),
    }
}

fn replay_once(input: &Arc<InMemoryInputJournal>) -> (Vec<TradeEvent>, u64) {
    let symbol = Symbol("BTCUSDT".into());
    let output = InMemoryOutputJournal::default();
    let mut engine = MatchingEngine::new(symbol.clone());
    let mut gateway = CommandGateway::new(symbol.clone(), config());
    let mut trades = Vec::new();

    for entry in input.read_from(&symbol, JournalSeq(1)) {
        let command = match gateway.validate(entry.command, entry.seq) {
            GatewayResult::Accept(command) => command,
            _ => continue,
        };
        let result = engine.process(command, entry.seq);
        output.append_output(
            entry.command_id,
            &result.order_ack,
            &result.trades,
            &result.market_event,
        );
        trades.extend(result.trades);
    }

    (trades, engine.order_book().checksum())
}

#[test]
fn replay_three_times_produces_identical_output() {
    let input = make_input();
    let (trades1, checksum1) = replay_once(&input);
    let (trades2, checksum2) = replay_once(&input);
    let (trades3, checksum3) = replay_once(&input);

    assert_eq!(trades1, trades2, "run 1 vs run 2 diverge");
    assert_eq!(trades2, trades3, "run 2 vs run 3 diverge");
    assert_eq!(checksum1, checksum2);
    assert_eq!(checksum2, checksum3);
}

#[test]
fn duplicate_command_does_not_produce_duplicate_trade() {
    let journal = Arc::new(InMemoryInputJournal::default());
    let symbol = Symbol("BTCUSDT".into());
    let command = MatchingCommand::PlaceOrder(OrderCommand {
        command_id: CommandId(1),
        order_id: OrderId(1),
        symbol: symbol.clone(),
        side: Side::Bid,
        order_type: OrderType::Limit,
        price: dec!(100),
        quantity: dec!(5),
        config_version: ConfigVersion(1),
        timestamp_ns: 0,
    });
    journal.append(symbol.clone(), command.clone());
    journal.append(symbol.clone(), command);

    let mut engine = MatchingEngine::new(symbol.clone());
    let mut gateway = CommandGateway::new(symbol.clone(), config());
    let mut accepted = 0;

    for entry in journal.read_from(&symbol, JournalSeq(1)) {
        match gateway.validate(entry.command, entry.seq) {
            GatewayResult::Accept(command) => {
                engine.process(command, entry.seq);
                accepted += 1;
            }
            GatewayResult::Duplicate { .. } | GatewayResult::Reject { .. } => {}
        }
    }

    assert_eq!(accepted, 1);
}
