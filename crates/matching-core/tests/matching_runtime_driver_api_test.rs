use matching_core::matching_runtime_driver::{
    InputHandoffWriteCommand, InputHandoffWriter, ManualMatchingRuntimeDriver,
    MatchingRuntimeDriver, MatchingRuntimeDriverError,
};
use matching_core::runtime_config::{
    MatchingRuntimeConfig, RuntimeShardId, RuntimeTopologyConfig, SymbolAssignmentPolicy,
};
use matching_core::shard_runtime::ShardRuntimeError;
use matching_core::types::{CommandId, JournalSeq, OrderId, Price, Quantity, Side, Symbol};
use matching_core::{
    journal_adapter::JournalInputEntry,
    order::{Command, Order},
};

fn symbol(value: &str) -> Symbol {
    Symbol(value.to_string())
}

fn command_entry(seq: u64, symbol: Symbol) -> JournalInputEntry {
    JournalInputEntry {
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
fn manual_matching_runtime_driver_builds_shards_from_runtime_topology_from_public_api() {
    let btc = symbol("BTC-USDT");
    let eth = symbol("ETH-USDT");
    let sol = symbol("SOL-USDT");
    let mut config = MatchingRuntimeConfig::default();
    config.topology = RuntimeTopologyConfig {
        shard_count: 2,
        assignment_policy: SymbolAssignmentPolicy::DeclarationOrder,
    };

    let driver = ManualMatchingRuntimeDriver::from_symbols_with_config(
        vec![btc.clone(), eth.clone(), sol.clone()],
        config,
    )
    .expect("manual matching runtime driver topology should resolve");

    assert_eq!(driver.shard_count(), 2);
    assert_eq!(
        driver.shard_ids(),
        vec![RuntimeShardId(0), RuntimeShardId(1)]
    );
    assert_eq!(
        driver.symbols_for_shard(RuntimeShardId(0)),
        Some(&[btc, sol][..])
    );
    assert_eq!(
        driver.symbols_for_shard(RuntimeShardId(1)),
        Some(&[eth][..])
    );
}

#[test]
fn manual_matching_runtime_driver_wraps_shard_runtime_errors_from_public_api() {
    let btc = symbol("BTC-USDT");
    let eth = symbol("ETH-USDT");
    let mut driver = ManualMatchingRuntimeDriver::from_symbols_with_config(
        vec![btc],
        MatchingRuntimeConfig::default(),
    )
    .expect("manual matching runtime driver topology should resolve");

    assert_eq!(
        driver.write_input(command_entry(1, eth.clone())),
        Err(MatchingRuntimeDriverError::ShardRuntime(
            ShardRuntimeError::UnregisteredHandoff(eth)
        ))
    );
}

#[test]
fn manual_matching_runtime_driver_plans_input_handoff_writes_by_owning_shard_from_public_api() {
    let btc = symbol("BTC-USDT");
    let eth = symbol("ETH-USDT");
    let sol = symbol("SOL-USDT");
    let mut config = MatchingRuntimeConfig::default();
    config.topology = RuntimeTopologyConfig {
        shard_count: 2,
        assignment_policy: SymbolAssignmentPolicy::DeclarationOrder,
    };
    let driver = ManualMatchingRuntimeDriver::from_symbols_with_config(
        vec![btc.clone(), eth.clone(), sol.clone()],
        config,
    )
    .expect("manual matching runtime driver topology should resolve");
    let btc_entry = command_entry(1, btc);
    let eth_entry = command_entry(2, eth);
    let sol_entry = command_entry(3, sol);

    let commands = driver
        .plan_writes(&[btc_entry.clone(), eth_entry.clone(), sol_entry.clone()])
        .expect("manual matching runtime driver should plan input handoff writes");

    assert_eq!(
        commands,
        vec![
            InputHandoffWriteCommand::WriteInputs {
                shard_id: RuntimeShardId(0),
                entries: vec![btc_entry, sol_entry],
            },
            InputHandoffWriteCommand::WriteInputs {
                shard_id: RuntimeShardId(1),
                entries: vec![eth_entry],
            },
        ]
    );
}
