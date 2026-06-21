use matching_core::runtime_config::{
    RuntimeShardId, RuntimeTopologyConfig, SymbolAssignmentPolicy, SymbolShardAssignment,
};
use matching_core::runtime_topology::{RuntimeTopology, RuntimeTopologyError};
use matching_core::types::Symbol;

fn symbol(value: &str) -> Symbol {
    Symbol(value.to_string())
}

#[test]
fn runtime_topology_default_assigns_all_symbols_to_single_shard_from_public_api() {
    let btc = symbol("BTC-USDT");
    let eth = symbol("ETH-USDT");

    let topology = RuntimeTopology::resolve(
        &[btc.clone(), eth.clone()],
        &RuntimeTopologyConfig::default(),
    )
    .expect("default topology should resolve");

    assert_eq!(topology.shard_count(), 1);
    assert_eq!(
        topology.symbols_for_shard(RuntimeShardId(0)),
        Some(&[btc, eth][..])
    );
}

#[test]
fn runtime_topology_declaration_order_distributes_symbols_across_shards_from_public_api() {
    let btc = symbol("BTC-USDT");
    let eth = symbol("ETH-USDT");
    let sol = symbol("SOL-USDT");
    let ada = symbol("ADA-USDT");
    let config = RuntimeTopologyConfig {
        shard_count: 2,
        assignment_policy: SymbolAssignmentPolicy::DeclarationOrder,
    };

    let topology = RuntimeTopology::resolve(
        &[btc.clone(), eth.clone(), sol.clone(), ada.clone()],
        &config,
    )
    .expect("declaration-order topology should resolve");

    assert_eq!(
        topology.symbols_for_shard(RuntimeShardId(0)),
        Some(&[btc, sol][..])
    );
    assert_eq!(
        topology.symbols_for_shard(RuntimeShardId(1)),
        Some(&[eth, ada][..])
    );
}

#[test]
fn runtime_topology_stable_hash_is_independent_of_symbol_declaration_order() {
    let btc = symbol("BTC-USDT");
    let eth = symbol("ETH-USDT");
    let sol = symbol("SOL-USDT");
    let config = RuntimeTopologyConfig {
        shard_count: 4,
        assignment_policy: SymbolAssignmentPolicy::StableHash,
    };

    let first = RuntimeTopology::resolve(&[btc.clone(), eth.clone(), sol.clone()], &config)
        .expect("stable-hash topology should resolve");
    let second = RuntimeTopology::resolve(&[sol, btc.clone(), eth], &config)
        .expect("stable-hash topology should resolve");

    assert_eq!(first.shard_for_symbol(&btc), second.shard_for_symbol(&btc));
}

#[test]
fn runtime_topology_explicit_map_assigns_symbols_from_public_api() {
    let btc = symbol("BTC-USDT");
    let eth = symbol("ETH-USDT");
    let config = RuntimeTopologyConfig {
        shard_count: 2,
        assignment_policy: SymbolAssignmentPolicy::ExplicitMap(vec![
            SymbolShardAssignment {
                symbol: btc.clone(),
                shard_id: RuntimeShardId(1),
            },
            SymbolShardAssignment {
                symbol: eth.clone(),
                shard_id: RuntimeShardId(0),
            },
        ]),
    };

    let topology = RuntimeTopology::resolve(&[btc.clone(), eth.clone()], &config)
        .expect("explicit topology should resolve");

    assert_eq!(topology.shard_for_symbol(&btc), Some(RuntimeShardId(1)));
    assert_eq!(topology.shard_for_symbol(&eth), Some(RuntimeShardId(0)));
}

#[test]
fn runtime_topology_rejects_zero_shard_count_from_public_api() {
    let config = RuntimeTopologyConfig {
        shard_count: 0,
        assignment_policy: SymbolAssignmentPolicy::DeclarationOrder,
    };

    let result = RuntimeTopology::resolve(&[symbol("BTC-USDT")], &config);

    assert_eq!(result, Err(RuntimeTopologyError::ZeroShardCount));
}

#[test]
fn runtime_topology_rejects_duplicate_symbol_assignment_from_public_api() {
    let btc = symbol("BTC-USDT");
    let config = RuntimeTopologyConfig {
        shard_count: 2,
        assignment_policy: SymbolAssignmentPolicy::ExplicitMap(vec![
            SymbolShardAssignment {
                symbol: btc.clone(),
                shard_id: RuntimeShardId(0),
            },
            SymbolShardAssignment {
                symbol: btc.clone(),
                shard_id: RuntimeShardId(1),
            },
        ]),
    };

    let result = RuntimeTopology::resolve(&[btc.clone()], &config);

    assert_eq!(
        result,
        Err(RuntimeTopologyError::DuplicateSymbolAssignment(btc))
    );
}

#[test]
fn runtime_topology_rejects_missing_symbol_assignment_from_public_api() {
    let btc = symbol("BTC-USDT");
    let eth = symbol("ETH-USDT");
    let config = RuntimeTopologyConfig {
        shard_count: 2,
        assignment_policy: SymbolAssignmentPolicy::ExplicitMap(vec![SymbolShardAssignment {
            symbol: btc,
            shard_id: RuntimeShardId(0),
        }]),
    };

    let result = RuntimeTopology::resolve(&[symbol("BTC-USDT"), eth.clone()], &config);

    assert_eq!(
        result,
        Err(RuntimeTopologyError::MissingSymbolAssignment(eth))
    );
}

#[test]
fn runtime_topology_rejects_unknown_symbol_assignment_from_public_api() {
    let xrp = symbol("XRP-USDT");
    let config = RuntimeTopologyConfig {
        shard_count: 2,
        assignment_policy: SymbolAssignmentPolicy::ExplicitMap(vec![
            SymbolShardAssignment {
                symbol: symbol("BTC-USDT"),
                shard_id: RuntimeShardId(0),
            },
            SymbolShardAssignment {
                symbol: xrp.clone(),
                shard_id: RuntimeShardId(1),
            },
        ]),
    };

    let result = RuntimeTopology::resolve(&[symbol("BTC-USDT")], &config);

    assert_eq!(
        result,
        Err(RuntimeTopologyError::UnknownSymbolAssignment(xrp))
    );
}

#[test]
fn runtime_topology_rejects_out_of_range_shard_assignment_from_public_api() {
    let config = RuntimeTopologyConfig {
        shard_count: 2,
        assignment_policy: SymbolAssignmentPolicy::ExplicitMap(vec![SymbolShardAssignment {
            symbol: symbol("BTC-USDT"),
            shard_id: RuntimeShardId(2),
        }]),
    };

    let result = RuntimeTopology::resolve(&[symbol("BTC-USDT")], &config);

    assert_eq!(
        result,
        Err(RuntimeTopologyError::ShardIdOutOfRange(RuntimeShardId(2)))
    );
}
