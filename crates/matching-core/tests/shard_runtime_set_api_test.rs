use matching_core::runtime_config::{
    MatchingRuntimeConfig, RuntimeShardId, RuntimeTopologyConfig, SymbolAssignmentPolicy,
};
use matching_core::shard_runtime::ShardRuntimeError;
use matching_core::shard_runtime_set::{
    InlineShardRuntimeSet, InputHandoffWriteCommand, InputHandoffWriter, ShardRuntimeSet,
    ShardRuntimeSetError, ThreadPerShardRuntimeSet,
};
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
fn inline_shard_runtime_set_builds_shards_from_runtime_topology_from_public_api() {
    let btc = symbol("BTC-USDT");
    let eth = symbol("ETH-USDT");
    let sol = symbol("SOL-USDT");
    let mut config = MatchingRuntimeConfig::default();
    config.topology = RuntimeTopologyConfig {
        shard_count: 2,
        assignment_policy: SymbolAssignmentPolicy::DeclarationOrder,
    };

    let runtime_set = InlineShardRuntimeSet::from_symbols_with_config(
        vec![btc.clone(), eth.clone(), sol.clone()],
        config,
    )
    .expect("inline matching runtime runtime_set topology should resolve");

    assert_eq!(runtime_set.shard_count(), 2);
    assert_eq!(
        runtime_set.shard_ids(),
        vec![RuntimeShardId(0), RuntimeShardId(1)]
    );
    assert_eq!(
        runtime_set.symbols_for_shard(RuntimeShardId(0)),
        Some(&[btc, sol][..])
    );
    assert_eq!(
        runtime_set.symbols_for_shard(RuntimeShardId(1)),
        Some(&[eth][..])
    );
}

#[test]
fn thread_per_shard_runtime_set_builds_shards_from_runtime_topology_from_public_api() {
    let btc = symbol("BTC-USDT");
    let eth = symbol("ETH-USDT");
    let sol = symbol("SOL-USDT");
    let mut config = MatchingRuntimeConfig::default();
    config.execution.mode = matching_core::runtime_config::RuntimeExecutionMode::ThreadPerShard;
    config.topology = RuntimeTopologyConfig {
        shard_count: 2,
        assignment_policy: SymbolAssignmentPolicy::DeclarationOrder,
    };

    let runtime_set = ThreadPerShardRuntimeSet::from_symbols_with_config(
        vec![btc.clone(), eth.clone(), sol.clone()],
        config,
    )
    .expect("thread-per-shard matching runtime runtime_set topology should resolve");

    assert_eq!(runtime_set.shard_count(), 2);
    assert_eq!(
        runtime_set.shard_ids(),
        vec![RuntimeShardId(0), RuntimeShardId(1)]
    );
    assert_eq!(
        runtime_set.symbols_for_shard(RuntimeShardId(0)),
        Some(&[btc.clone(), sol.clone()][..])
    );
    assert_eq!(
        runtime_set.symbols_for_shard(RuntimeShardId(1)),
        Some(&[eth.clone()][..])
    );

    assert_eq!(runtime_set.worker_count(), 2);
    assert_eq!(
        runtime_set.worker_symbols_for_shard(RuntimeShardId(0)),
        Some(&[btc.clone(), sol.clone()][..])
    );
    assert_eq!(
        runtime_set.worker_symbols_for_shard(RuntimeShardId(1)),
        Some(&[eth.clone()][..])
    );
}

#[test]
fn inline_shard_runtime_set_wraps_shard_runtime_errors_from_public_api() {
    let btc = symbol("BTC-USDT");
    let eth = symbol("ETH-USDT");
    let mut runtime_set = InlineShardRuntimeSet::from_symbols_with_config(
        vec![btc],
        MatchingRuntimeConfig::default(),
    )
    .expect("inline matching runtime runtime_set topology should resolve");

    assert_eq!(
        runtime_set.write_input(command_entry(1, eth.clone())),
        Err(ShardRuntimeSetError::ShardRuntime(
            ShardRuntimeError::UnregisteredHandoff(eth)
        ))
    );
}

#[test]
fn thread_per_shard_runtime_set_wraps_shard_runtime_errors_from_public_api() {
    let btc = symbol("BTC-USDT");
    let eth = symbol("ETH-USDT");
    let mut config = MatchingRuntimeConfig::default();
    config.execution.mode = matching_core::runtime_config::RuntimeExecutionMode::ThreadPerShard;
    let mut runtime_set = ThreadPerShardRuntimeSet::from_symbols_with_config(vec![btc], config)
        .expect("thread-per-shard matching runtime runtime_set topology should resolve");

    assert_eq!(
        runtime_set.write_input(command_entry(1, eth.clone())),
        Err(ShardRuntimeSetError::ShardRuntime(
            ShardRuntimeError::UnregisteredHandoff(eth)
        ))
    );
}

#[test]
fn inline_shard_runtime_set_plans_input_handoff_writes_by_owning_shard_from_public_api() {
    let btc = symbol("BTC-USDT");
    let eth = symbol("ETH-USDT");
    let sol = symbol("SOL-USDT");
    let mut config = MatchingRuntimeConfig::default();
    config.topology = RuntimeTopologyConfig {
        shard_count: 2,
        assignment_policy: SymbolAssignmentPolicy::DeclarationOrder,
    };
    let runtime_set = InlineShardRuntimeSet::from_symbols_with_config(
        vec![btc.clone(), eth.clone(), sol.clone()],
        config,
    )
    .expect("inline matching runtime runtime_set topology should resolve");
    let btc_entry = command_entry(1, btc);
    let eth_entry = command_entry(2, eth);
    let sol_entry = command_entry(3, sol);

    let commands = runtime_set
        .plan_writes(&[btc_entry.clone(), eth_entry.clone(), sol_entry.clone()])
        .expect("inline matching runtime runtime_set should plan input handoff writes");

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

#[test]
fn thread_per_shard_runtime_set_plans_input_handoff_writes_by_owning_shard_from_public_api() {
    let btc = symbol("BTC-USDT");
    let eth = symbol("ETH-USDT");
    let sol = symbol("SOL-USDT");
    let mut config = MatchingRuntimeConfig::default();
    config.execution.mode = matching_core::runtime_config::RuntimeExecutionMode::ThreadPerShard;
    config.topology = RuntimeTopologyConfig {
        shard_count: 2,
        assignment_policy: SymbolAssignmentPolicy::DeclarationOrder,
    };
    let runtime_set = ThreadPerShardRuntimeSet::from_symbols_with_config(
        vec![btc.clone(), eth.clone(), sol.clone()],
        config,
    )
    .expect("thread-per-shard matching runtime runtime_set should resolve");
    let btc_entry = command_entry(1, btc);
    let eth_entry = command_entry(2, eth);
    let sol_entry = command_entry(3, sol);

    let commands = runtime_set
        .plan_writes(&[btc_entry.clone(), eth_entry.clone(), sol_entry.clone()])
        .expect("thread-per-shard matching runtime runtime_set should plan input handoff writes");

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
