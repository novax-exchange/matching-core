use crate::types::Symbol;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchingRuntimeConfig {
    pub topology: RuntimeTopologyConfig,
    pub host: RuntimeHostConfig,
    pub output_commit: OutputCommitConfig,
    pub input_consumer: InputConsumerConfig,
    pub handoff: HandoffConfig,
    pub symbol_runtime: SymbolRuntimeConfig,
    pub snapshot: SnapshotConfig,
    pub snapshot_verification: SnapshotVerificationConfig,
}

impl Default for MatchingRuntimeConfig {
    fn default() -> Self {
        Self {
            topology: RuntimeTopologyConfig::default(),
            host: RuntimeHostConfig::default(),
            output_commit: OutputCommitConfig::default(),
            input_consumer: InputConsumerConfig::default(),
            handoff: HandoffConfig::default(),
            symbol_runtime: SymbolRuntimeConfig::default(),
            snapshot: SnapshotConfig::default(),
            snapshot_verification: SnapshotVerificationConfig::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeTopologyConfig {
    pub shard_count: usize,
    pub assignment_policy: SymbolAssignmentPolicy,
}

impl Default for RuntimeTopologyConfig {
    fn default() -> Self {
        Self {
            shard_count: 1,
            assignment_policy: SymbolAssignmentPolicy::DeclarationOrder,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SymbolAssignmentPolicy {
    DeclarationOrder,
    StableHash,
    ExplicitMap(Vec<SymbolShardAssignment>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SymbolShardAssignment {
    pub symbol: Symbol,
    pub shard_id: RuntimeShardId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RuntimeShardId(pub usize);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeHostConfig {
    pub mode: RuntimeHostMode,
    pub max_run_cycles_per_call: usize,
    pub max_run_calls_per_until_idle: usize,
}

impl Default for RuntimeHostConfig {
    fn default() -> Self {
        Self {
            mode: RuntimeHostMode::Manual,
            max_run_cycles_per_call: 1024,
            max_run_calls_per_until_idle: 1024,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeHostMode {
    Manual,
    Inline,
    ThreadPerShard,
    AsyncTaskPerShard,
    ProcessPerShard,
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
pub struct SymbolRuntimeConfig {
    pub max_input_entries_per_step: usize,
}

impl Default for SymbolRuntimeConfig {
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
