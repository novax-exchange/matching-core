#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchingRuntimeConfig {
    pub output_commit: OutputCommitConfig,
    pub input_consumer: InputConsumerConfig,
    pub handoff: HandoffConfig,
    pub execution_loop: ExecutionLoopConfig,
    pub snapshot: SnapshotConfig,
    pub snapshot_verification: SnapshotVerificationConfig,
}

impl Default for MatchingRuntimeConfig {
    fn default() -> Self {
        Self {
            output_commit: OutputCommitConfig::default(),
            input_consumer: InputConsumerConfig::default(),
            handoff: HandoffConfig::default(),
            execution_loop: ExecutionLoopConfig::default(),
            snapshot: SnapshotConfig::default(),
            snapshot_verification: SnapshotVerificationConfig::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputCommitConfig {
    pub pending_output_capacity: usize,
    pub max_unavailable_attempts: usize,
    pub max_output_requests_per_step: usize,
}

impl Default for OutputCommitConfig {
    fn default() -> Self {
        Self {
            pending_output_capacity: 1024,
            max_unavailable_attempts: 3,
            max_output_requests_per_step: 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputConsumerConfig {
    pub max_batch_entries: usize,
}

impl Default for InputConsumerConfig {
    fn default() -> Self {
        Self {
            max_batch_entries: 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandoffConfig {
    pub capacity: usize,
}

impl Default for HandoffConfig {
    fn default() -> Self {
        Self { capacity: 1024 }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionLoopConfig {
    pub max_input_entries_per_step: usize,
}

impl Default for ExecutionLoopConfig {
    fn default() -> Self {
        Self {
            max_input_entries_per_step: 1024,
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
