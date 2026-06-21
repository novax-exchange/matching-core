use matching_core::runtime_config::{
    HandoffConfig, InputConsumerConfig, MatchingRuntimeConfig, OutputCommitConfig,
    RuntimeHostConfig, RuntimeHostMode, RuntimeShardId, RuntimeTopologyConfig, SnapshotConfig,
    SnapshotVerificationConfig, SymbolAssignmentPolicy, SymbolRuntimeConfig, SymbolShardAssignment,
};
use matching_core::runtime_loop::RuntimeLoopRunLimit;
use matching_core::types::Symbol;

#[test]
fn matching_runtime_config_groups_runtime_policy_from_public_api() {
    let btc = Symbol("BTC-USDT".to_string());
    let config = MatchingRuntimeConfig {
        topology: RuntimeTopologyConfig {
            shard_count: 2,
            assignment_policy: SymbolAssignmentPolicy::ExplicitMap(vec![SymbolShardAssignment {
                symbol: btc.clone(),
                shard_id: RuntimeShardId(1),
            }]),
        },
        host: RuntimeHostConfig {
            mode: RuntimeHostMode::Manual,
            max_run_cycles_per_call: 7,
        },
        output_commit: OutputCommitConfig {
            pending_output_capacity: 512,
            max_unavailable_attempts: 2,
            max_output_requests_per_step: 64,
        },
        input_consumer: InputConsumerConfig {
            max_batch_entries: 128,
        },
        handoff: HandoffConfig { capacity: 256 },
        symbol_runtime: SymbolRuntimeConfig {
            max_input_entries_per_step: 32,
        },
        snapshot: SnapshotConfig { retention_limit: 5 },
        snapshot_verification: SnapshotVerificationConfig {
            max_mismatch_attempts: 3,
        },
    };

    assert_eq!(config.topology.shard_count, 2);
    assert_eq!(
        config.topology.assignment_policy,
        SymbolAssignmentPolicy::ExplicitMap(vec![SymbolShardAssignment {
            symbol: btc,
            shard_id: RuntimeShardId(1),
        }])
    );
    assert_eq!(config.host.mode, RuntimeHostMode::Manual);
    assert_eq!(config.host.max_run_cycles_per_call, 7);
    assert_eq!(config.output_commit.pending_output_capacity, 512);
    assert_eq!(config.output_commit.max_unavailable_attempts, 2);
    assert_eq!(config.output_commit.max_output_requests_per_step, 64);
    assert_eq!(config.input_consumer.max_batch_entries, 128);
    assert_eq!(config.handoff.capacity, 256);
    assert_eq!(config.symbol_runtime.max_input_entries_per_step, 32);
    assert_eq!(config.snapshot.retention_limit, 5);
    assert_eq!(config.snapshot_verification.max_mismatch_attempts, 3);
}

#[test]
fn matching_runtime_config_defaults_are_available_from_public_api() {
    let config = MatchingRuntimeConfig::default();

    assert_eq!(config.topology.shard_count, 1);
    assert_eq!(
        config.topology.assignment_policy,
        SymbolAssignmentPolicy::DeclarationOrder
    );
    assert_eq!(config.host.mode, RuntimeHostMode::Manual);
    assert_eq!(config.host.max_run_cycles_per_call, 1024);
    assert_eq!(config.output_commit.pending_output_capacity, 1024);
    assert_eq!(config.output_commit.max_unavailable_attempts, 3);
    assert_eq!(config.output_commit.max_output_requests_per_step, 1024);
    assert_eq!(config.input_consumer.max_batch_entries, 1024);
    assert_eq!(config.handoff.capacity, 1024);
    assert_eq!(config.symbol_runtime.max_input_entries_per_step, 1024);
    assert_eq!(config.snapshot.retention_limit, 1);
    assert_eq!(config.snapshot_verification.max_mismatch_attempts, 3);
}

#[test]
fn runtime_loop_run_limit_can_be_derived_from_runtime_config() {
    let mut config = MatchingRuntimeConfig::default();
    config.host.max_run_cycles_per_call = 13;

    let limit = RuntimeLoopRunLimit::from_config(&config);

    assert_eq!(limit.max_cycles, 13);
}
