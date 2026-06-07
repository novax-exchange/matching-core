mod support;

use matching_core::engine::gateway::{CommandGateway, GatewayResult};
use matching_core::engine::matching::MatchingEngine;
use matching_core::journal::traits::{AppendResult, InputJournal, OutputJournal};
use matching_core::types::*;
use rust_decimal_macros::dec;
use support::in_memory_journal::{InMemoryInputJournal, InMemoryOutputJournal};

#[test]
fn last_input_seq_only_advances_after_successful_output_append() {
    let input = InMemoryInputJournal::default();
    let output = InMemoryOutputJournal::default();
    let symbol = Symbol("BTCUSDT".into());

    input.append(
        symbol.clone(),
        MatchingCommand::PlaceOrder(OrderCommand {
            command_id: CommandId(1),
            order_id: OrderId(1),
            symbol: symbol.clone(),
            side: Side::Bid,
            order_type: OrderType::Limit,
            price: dec!(100),
            quantity: dec!(1),
            config_version: ConfigVersion(1),
            timestamp_ns: 0,
        }),
    );

    let config = SymbolConfig {
        price_tick: dec!(0.01),
        quantity_tick: dec!(0.001),
        min_quantity: dec!(0.001),
        config_version: ConfigVersion(1),
    };

    let mut engine = MatchingEngine::new(symbol.clone());
    let mut gateway = CommandGateway::new(symbol.clone(), config);
    let mut last_seq = JournalSeq(0);

    for entry in input.read_from(&symbol, last_seq.next()) {
        let command = match gateway.validate(entry.command, entry.seq) {
            GatewayResult::Accept(command) => command,
            _ => continue,
        };
        let result = engine.process(command, entry.seq);
        let append = output.append_output(
            entry.command_id,
            &result.order_ack,
            &result.trades,
            &result.market_event,
        );
        match append {
            AppendResult::Accepted { .. } | AppendResult::DuplicateAccepted { .. } => {
                last_seq = entry.seq;
            }
            _ => break,
        }
    }

    assert_eq!(last_seq, JournalSeq(1));
}
