#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchingRuntimeConfig {
    pub output_commit: OutputCommitConfig,
    pub snapshot: SnapshotConfig,
    pub snapshot_verification: SnapshotVerificationConfig,
}

impl Default for MatchingRuntimeConfig {
    fn default() -> Self {
        Self {
            output_commit: OutputCommitConfig::default(),
            snapshot: SnapshotConfig::default(),
            snapshot_verification: SnapshotVerificationConfig::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputCommitConfig {
    pub pending_output_capacity: usize,
    pub max_unavailable_attempts: usize,
}

impl Default for OutputCommitConfig {
    fn default() -> Self {
        Self {
            pending_output_capacity: 1024,
            max_unavailable_attempts: 3,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotConfig {
    pub retention_limit: usize,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self { retention_limit: 1 }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotVerificationConfig {
    pub max_mismatch_attempts: usize,
}

impl Default for SnapshotVerificationConfig {
    fn default() -> Self {
        Self {
            max_mismatch_attempts: 3,
        }
    }
}
