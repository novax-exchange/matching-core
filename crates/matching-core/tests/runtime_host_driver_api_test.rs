use matching_core::runtime_config::{
    MatchingRuntimeConfig, RuntimeShardId, RuntimeTopologyConfig, SymbolAssignmentPolicy,
};
use matching_core::runtime_host_driver::{
    ManualRuntimeHostDriver, RuntimeHostDriver, RuntimeHostDriverError,
};
use matching_core::runtime_loop::RuntimeLoopError;
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
fn manual_runtime_host_driver_builds_shards_from_runtime_topology_from_public_api() {
    let btc = symbol("BTC-USDT");
    let eth = symbol("ETH-USDT");
    let sol = symbol("SOL-USDT");
    let mut config = MatchingRuntimeConfig::default();
    config.topology = RuntimeTopologyConfig {
        shard_count: 2,
        assignment_policy: SymbolAssignmentPolicy::DeclarationOrder,
    };

    let driver = ManualRuntimeHostDriver::from_symbols_with_config(
        vec![btc.clone(), eth.clone(), sol.clone()],
        config,
    )
    .expect("manual runtime host driver topology should resolve");

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
fn manual_runtime_host_driver_wraps_runtime_loop_errors_from_public_api() {
    let btc = symbol("BTC-USDT");
    let eth = symbol("ETH-USDT");
    let mut driver = ManualRuntimeHostDriver::from_symbols_with_config(
        vec![btc],
        MatchingRuntimeConfig::default(),
    )
    .expect("manual runtime host driver topology should resolve");

    assert_eq!(
        driver.enqueue_input(command_entry(1, eth.clone())),
        Err(RuntimeHostDriverError::RuntimeLoop(
            RuntimeLoopError::UnregisteredHandoff(eth)
        ))
    );
}
