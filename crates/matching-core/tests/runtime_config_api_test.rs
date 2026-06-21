use matching_core::runtime_config::{
    MatchingRuntimeConfig, OutputCommitConfig, SnapshotConfig, SnapshotVerificationConfig,
};

#[test]
fn matching_runtime_config_groups_runtime_policy_from_public_api() {
    let config = MatchingRuntimeConfig {
        output_commit: OutputCommitConfig {
            pending_output_capacity: 512,
            max_unavailable_attempts: 2,
        },
        snapshot: SnapshotConfig { retention_limit: 5 },
        snapshot_verification: SnapshotVerificationConfig {
            max_mismatch_attempts: 3,
        },
    };

    assert_eq!(config.output_commit.pending_output_capacity, 512);
    assert_eq!(config.output_commit.max_unavailable_attempts, 2);
    assert_eq!(config.snapshot.retention_limit, 5);
    assert_eq!(config.snapshot_verification.max_mismatch_attempts, 3);
}

#[test]
fn matching_runtime_config_defaults_are_available_from_public_api() {
    let config = MatchingRuntimeConfig::default();

    assert_eq!(config.output_commit.pending_output_capacity, 1024);
    assert_eq!(config.output_commit.max_unavailable_attempts, 3);
    assert_eq!(config.snapshot.retention_limit, 1);
    assert_eq!(config.snapshot_verification.max_mismatch_attempts, 3);
}
