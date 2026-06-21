use matching_core::runtime_config::{
    MatchingRuntimeConfig, RuntimeShardId, RuntimeTopologyConfig, SymbolAssignmentPolicy,
};
use matching_core::runtime_host_driver::{ManualRuntimeHostDriver, RuntimeHostDriver};
use matching_core::types::Symbol;

fn symbol(value: &str) -> Symbol {
    Symbol(value.to_string())
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
