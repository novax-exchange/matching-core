use crate::journal_adapter::{JournalInputEntry, JournalOutputAppender};
use crate::output_commit_boundary::OutputJournalClient;
use crate::runtime_config::{MatchingRuntimeConfig, RuntimeExecutionMode, RuntimeShardId};
use crate::runtime_topology::RuntimeTopologyError;
use crate::shard_runtime::{
    ShardRuntimeError, ShardRuntimeRunLimit, ShardRuntimeRunOnceLimits, ShardRuntimeRunOnceReport,
    ShardRuntimeRunReport, ShardRuntimeRunStopReason,
};
use crate::shard_runtime_set::{
    InlineShardRuntimeSet, ShardRuntimeOutputWriter, ShardRuntimeSet, ShardRuntimeSetError,
    ShardRuntimeSetShutdownReport, ShardWorkerRuntimeSet,
};
use crate::types::Symbol;

pub struct MatchingRuntime {
    mode: RuntimeExecutionMode,
    runtime_set: Box<dyn ShardRuntimeSet>,
    run_once_limits: ShardRuntimeRunOnceLimits,
    run_limit: ShardRuntimeRunLimit,
    run_until_idle_limit: MatchingRuntimeRunUntilIdleLimit,
    input_state: MatchingRuntimeInputState,
    lifecycle_state: MatchingRuntimeLifecycleState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MatchingRuntimeError {
    InputClosed,
    RuntimeShutdown,
    UnsupportedMode(RuntimeExecutionMode),
    ShardRuntimeSet(ShardRuntimeSetError),
    Topology(RuntimeTopologyError),
    ShardRuntime(ShardRuntimeError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchingRuntimeInputState {
    Open,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchingRuntimeLifecycleState {
    Running,
    Shutdown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchingRuntimeRunOnceReport {
    pub shard_reports: Vec<MatchingRuntimeShardRunOnceReport>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchingRuntimeRunReport {
    pub shard_reports: Vec<MatchingRuntimeShardRunReport>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchingRuntimeRunStopReason {
    Idle,
    Blocked,
    RunLimitReached,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MatchingRuntimeRunUntilIdleLimit {
    pub max_run_calls: usize,
}

impl MatchingRuntimeRunUntilIdleLimit {
    pub fn from_config(config: &MatchingRuntimeConfig) -> Self {
        Self {
            max_run_calls: config.execution.max_run_calls_per_until_idle,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchingRuntimeRunUntilIdleStopReason {
    Idle,
    Blocked,
    CallLimitReached,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchingRuntimeRunUntilIdleReport {
    pub run_reports: Vec<MatchingRuntimeRunReport>,
    pub stop_reason: MatchingRuntimeRunUntilIdleStopReason,
    pub final_status: MatchingRuntimeStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchingRuntimeDrainStopReason {
    Drained,
    Blocked,
    CallLimitReached,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchingRuntimeDrainReport {
    pub run_report: MatchingRuntimeRunUntilIdleReport,
    pub stop_reason: MatchingRuntimeDrainStopReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchingRuntimeShutdownReport {
    pub input_state: MatchingRuntimeInputState,
    pub lifecycle_state: MatchingRuntimeLifecycleState,
    pub runtime_set_report: ShardRuntimeSetShutdownReport,
    pub final_status: MatchingRuntimeStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchingRuntimeStatus {
    pub input_state: MatchingRuntimeInputState,
    pub lifecycle_state: MatchingRuntimeLifecycleState,
    pub shard_statuses: Vec<MatchingRuntimeShardStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchingRuntimeShardStatus {
    pub shard_id: RuntimeShardId,
    pub symbol_statuses: Vec<MatchingRuntimeSymbolStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchingRuntimeSymbolStatus {
    pub symbol: Symbol,
    pub pending_input_len: usize,
    pub pending_input_capacity: usize,
    pub pending_input_full: bool,
    pub pending_output_len: usize,
    pub pending_output_capacity: usize,
    pub pending_output_full: bool,
    pub output_commit_blocked: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchingRuntimeShardRunOnceReport {
    pub shard_id: RuntimeShardId,
    pub run_once_report: ShardRuntimeRunOnceReport,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchingRuntimeShardRunReport {
    pub shard_id: RuntimeShardId,
    pub run_report: ShardRuntimeRunReport,
}

impl MatchingRuntime {
    fn new_with_runtime_set(
        mode: RuntimeExecutionMode,
        config: &MatchingRuntimeConfig,
        runtime_set: Box<dyn ShardRuntimeSet>,
    ) -> Self {
        Self {
            mode,
            runtime_set,
            run_once_limits: ShardRuntimeRunOnceLimits::from_config(config),
            run_limit: ShardRuntimeRunLimit::from_config(config),
            run_until_idle_limit: MatchingRuntimeRunUntilIdleLimit::from_config(config),
            input_state: MatchingRuntimeInputState::Open,
            lifecycle_state: MatchingRuntimeLifecycleState::Running,
        }
    }

    pub fn new_for_symbols_with_config(
        symbols: Vec<Symbol>,
        config: MatchingRuntimeConfig,
    ) -> Result<Self, MatchingRuntimeError> {
        match config.execution.mode {
            RuntimeExecutionMode::Inline => {
                let mode = config.execution.mode;
                let runtime_set =
                    InlineShardRuntimeSet::from_symbols_with_config(symbols, config.clone())
                        .map_err(MatchingRuntimeError::Topology)?;

                Ok(Self::new_with_runtime_set(
                    mode,
                    &config,
                    Box::new(runtime_set),
                ))
            }
            RuntimeExecutionMode::ShardWorker => {
                let mode = config.execution.mode;
                let runtime_set =
                    ShardWorkerRuntimeSet::from_symbols_with_config(symbols, config.clone())
                        .map_err(MatchingRuntimeError::Topology)?;

                Ok(Self::new_with_runtime_set(
                    mode,
                    &config,
                    Box::new(runtime_set),
                ))
            }
        }
    }

    pub fn new_shard_worker_for_symbols_with_config_and_output_factory<F>(
        symbols: Vec<Symbol>,
        config: MatchingRuntimeConfig,
        output_factory: F,
    ) -> Result<Self, MatchingRuntimeError>
    where
        F: FnMut(RuntimeShardId) -> ShardRuntimeOutputWriter,
    {
        if config.execution.mode != RuntimeExecutionMode::ShardWorker {
            return Err(MatchingRuntimeError::UnsupportedMode(config.execution.mode));
        }

        let runtime_set = ShardWorkerRuntimeSet::from_symbols_with_config_and_output_factory(
            symbols,
            config.clone(),
            output_factory,
        )
        .map_err(MatchingRuntimeError::Topology)?;

        Ok(Self::new_with_runtime_set(
            RuntimeExecutionMode::ShardWorker,
            &config,
            Box::new(runtime_set),
        ))
    }

    pub fn new_shard_worker_with_output_factory<F>(
        symbols: Vec<Symbol>,
        config: MatchingRuntimeConfig,
        output_factory: F,
    ) -> Result<Self, MatchingRuntimeError>
    where
        F: FnMut(RuntimeShardId) -> ShardRuntimeOutputWriter,
    {
        Self::new_shard_worker_for_symbols_with_config_and_output_factory(
            symbols,
            config,
            output_factory,
        )
    }

    pub fn mode(&self) -> RuntimeExecutionMode {
        self.mode
    }

    pub fn shard_count(&self) -> usize {
        self.runtime_set.shard_count()
    }

    pub fn shard_ids(&self) -> Vec<RuntimeShardId> {
        self.runtime_set.shard_ids()
    }

    pub fn symbols_for_shard(&self, shard_id: RuntimeShardId) -> Option<&[Symbol]> {
        self.runtime_set.symbols_for_shard(shard_id)
    }

    pub fn input_state(&self) -> MatchingRuntimeInputState {
        self.input_state
    }

    pub fn lifecycle_state(&self) -> MatchingRuntimeLifecycleState {
        self.lifecycle_state
    }

    pub fn close_input(&mut self) {
        self.input_state = MatchingRuntimeInputState::Closed;
    }

    pub fn enqueue_input(&mut self, entry: JournalInputEntry) -> Result<(), MatchingRuntimeError> {
        self.ensure_runtime_running()?;
        self.ensure_input_open()?;

        self.runtime_set
            .write_input(entry)
            .map_err(MatchingRuntimeError::from_runtime_set_error)
    }

    pub fn enqueue_inputs(
        &mut self,
        entries: Vec<JournalInputEntry>,
    ) -> Result<usize, MatchingRuntimeError> {
        self.ensure_runtime_running()?;
        self.ensure_input_open()?;

        self.runtime_set
            .write_inputs(entries)
            .map_err(MatchingRuntimeError::from_runtime_set_error)
    }

    pub fn can_enqueue_inputs(
        &self,
        entries: &[JournalInputEntry],
    ) -> Result<(), MatchingRuntimeError> {
        self.ensure_runtime_running()?;
        self.ensure_input_open()?;
        self.runtime_set
            .can_write_inputs(entries)
            .map_err(MatchingRuntimeError::from_runtime_set_error)
    }

    pub fn status(&self) -> Result<MatchingRuntimeStatus, MatchingRuntimeError> {
        let shard_statuses = self
            .runtime_set
            .shard_statuses()
            .map_err(MatchingRuntimeError::from_runtime_set_error)?;

        Ok(MatchingRuntimeStatus {
            input_state: self.input_state,
            lifecycle_state: self.lifecycle_state,
            shard_statuses,
        })
    }

    fn ensure_input_open(&self) -> Result<(), MatchingRuntimeError> {
        if self.input_state == MatchingRuntimeInputState::Closed {
            return Err(MatchingRuntimeError::InputClosed);
        }

        Ok(())
    }

    fn ensure_runtime_running(&self) -> Result<(), MatchingRuntimeError> {
        if self.lifecycle_state == MatchingRuntimeLifecycleState::Shutdown {
            return Err(MatchingRuntimeError::RuntimeShutdown);
        }

        Ok(())
    }

    pub fn run_once_all(
        &mut self,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
        limits: ShardRuntimeRunOnceLimits,
    ) -> Result<MatchingRuntimeRunOnceReport, MatchingRuntimeError> {
        self.ensure_runtime_running()?;

        self.runtime_set
            .run_once_all(journal_client, output, limits)
            .map_err(MatchingRuntimeError::from_runtime_set_error)
    }

    pub fn run_limited_all(
        &mut self,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
        limits: ShardRuntimeRunOnceLimits,
        limit: ShardRuntimeRunLimit,
    ) -> Result<MatchingRuntimeRunReport, MatchingRuntimeError> {
        self.ensure_runtime_running()?;

        self.runtime_set
            .run_limited_all(journal_client, output, limits, limit)
            .map_err(MatchingRuntimeError::from_runtime_set_error)
    }

    pub fn run_configured_all(
        &mut self,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
    ) -> Result<MatchingRuntimeRunReport, MatchingRuntimeError> {
        self.run_limited_all(journal_client, output, self.run_once_limits, self.run_limit)
    }

    pub fn run_until_idle_configured(
        &mut self,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
    ) -> Result<MatchingRuntimeRunUntilIdleReport, MatchingRuntimeError> {
        self.run_until_idle(journal_client, output, self.run_until_idle_limit)
    }

    pub fn drain_configured(
        &mut self,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
    ) -> Result<MatchingRuntimeDrainReport, MatchingRuntimeError> {
        self.ensure_runtime_running()?;
        self.close_input();

        let run_report = self.run_until_idle_configured(journal_client, output)?;
        let stop_reason = match run_report.stop_reason {
            MatchingRuntimeRunUntilIdleStopReason::Idle => MatchingRuntimeDrainStopReason::Drained,
            MatchingRuntimeRunUntilIdleStopReason::Blocked => {
                MatchingRuntimeDrainStopReason::Blocked
            }
            MatchingRuntimeRunUntilIdleStopReason::CallLimitReached => {
                MatchingRuntimeDrainStopReason::CallLimitReached
            }
        };

        Ok(MatchingRuntimeDrainReport {
            run_report,
            stop_reason,
        })
    }

    pub fn shutdown(&mut self) -> Result<MatchingRuntimeShutdownReport, MatchingRuntimeError> {
        self.ensure_runtime_running()?;
        self.close_input();
        let final_shard_statuses = self
            .runtime_set
            .shard_statuses()
            .map_err(MatchingRuntimeError::from_runtime_set_error)?;

        let runtime_set_report = self
            .runtime_set
            .shutdown()
            .map_err(MatchingRuntimeError::from_runtime_set_error)?;
        self.lifecycle_state = MatchingRuntimeLifecycleState::Shutdown;
        let final_status = MatchingRuntimeStatus {
            input_state: self.input_state,
            lifecycle_state: self.lifecycle_state,
            shard_statuses: final_shard_statuses,
        };

        Ok(MatchingRuntimeShutdownReport {
            input_state: self.input_state,
            lifecycle_state: self.lifecycle_state,
            runtime_set_report,
            final_status,
        })
    }

    pub fn run_until_idle(
        &mut self,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
        limit: MatchingRuntimeRunUntilIdleLimit,
    ) -> Result<MatchingRuntimeRunUntilIdleReport, MatchingRuntimeError> {
        self.ensure_runtime_running()?;

        let mut run_reports = Vec::new();

        for _ in 0..limit.max_run_calls {
            let run_report = self.run_configured_all(journal_client, output)?;

            let stop_reason = match run_report.stop_reason() {
                MatchingRuntimeRunStopReason::Idle => {
                    Some(MatchingRuntimeRunUntilIdleStopReason::Idle)
                }
                MatchingRuntimeRunStopReason::Blocked => {
                    Some(MatchingRuntimeRunUntilIdleStopReason::Blocked)
                }
                MatchingRuntimeRunStopReason::RunLimitReached => None,
            };

            run_reports.push(run_report);

            if let Some(stop_reason) = stop_reason {
                let final_status = self.status()?;

                return Ok(MatchingRuntimeRunUntilIdleReport {
                    run_reports,
                    stop_reason,
                    final_status,
                });
            }
        }

        let final_status = self.status()?;

        Ok(MatchingRuntimeRunUntilIdleReport {
            run_reports,
            stop_reason: MatchingRuntimeRunUntilIdleStopReason::CallLimitReached,
            final_status,
        })
    }
}

impl MatchingRuntimeError {
    fn from_runtime_set_error(error: ShardRuntimeSetError) -> Self {
        match error {
            ShardRuntimeSetError::ShardRuntime(error) => Self::ShardRuntime(error),
            error => Self::ShardRuntimeSet(error),
        }
    }
}

impl MatchingRuntimeRunOnceReport {
    pub fn shard_report(
        &self,
        shard_id: RuntimeShardId,
    ) -> Option<&MatchingRuntimeShardRunOnceReport> {
        self.shard_reports
            .iter()
            .find(|report| report.shard_id == shard_id)
    }
}

impl MatchingRuntimeStatus {
    pub fn shard_status(&self, shard_id: RuntimeShardId) -> Option<&MatchingRuntimeShardStatus> {
        self.shard_statuses
            .iter()
            .find(|status| status.shard_id == shard_id)
    }

    pub fn is_idle(&self) -> bool {
        !self.has_work_remaining() && !self.has_blocked_symbols()
    }

    pub fn has_work_remaining(&self) -> bool {
        self.shard_statuses
            .iter()
            .any(MatchingRuntimeShardStatus::has_work_remaining)
    }

    pub fn has_blocked_symbols(&self) -> bool {
        self.shard_statuses
            .iter()
            .any(MatchingRuntimeShardStatus::has_blocked_symbols)
    }

    pub fn shards_with_remaining_work(&self) -> Vec<RuntimeShardId> {
        self.shard_statuses
            .iter()
            .filter(|status| status.has_work_remaining())
            .map(|status| status.shard_id)
            .collect()
    }

    pub fn blocked_shards(&self) -> Vec<RuntimeShardId> {
        self.shard_statuses
            .iter()
            .filter(|status| status.has_blocked_symbols())
            .map(|status| status.shard_id)
            .collect()
    }

    pub fn symbols_with_remaining_work(&self) -> Vec<Symbol> {
        self.shard_statuses
            .iter()
            .flat_map(MatchingRuntimeShardStatus::symbols_with_remaining_work)
            .collect()
    }

    pub fn blocked_symbols(&self) -> Vec<Symbol> {
        self.shard_statuses
            .iter()
            .flat_map(MatchingRuntimeShardStatus::blocked_symbols)
            .collect()
    }

    pub fn shards_with_full_input(&self) -> Vec<RuntimeShardId> {
        self.shard_statuses
            .iter()
            .filter(|status| status.has_full_input())
            .map(|status| status.shard_id)
            .collect()
    }

    pub fn shards_with_full_output(&self) -> Vec<RuntimeShardId> {
        self.shard_statuses
            .iter()
            .filter(|status| status.has_full_output())
            .map(|status| status.shard_id)
            .collect()
    }

    pub fn symbols_with_full_input(&self) -> Vec<Symbol> {
        self.shard_statuses
            .iter()
            .flat_map(MatchingRuntimeShardStatus::symbols_with_full_input)
            .collect()
    }

    pub fn symbols_with_full_output(&self) -> Vec<Symbol> {
        self.shard_statuses
            .iter()
            .flat_map(MatchingRuntimeShardStatus::symbols_with_full_output)
            .collect()
    }
}

impl MatchingRuntimeShardStatus {
    pub fn symbol_status(&self, symbol: &Symbol) -> Option<&MatchingRuntimeSymbolStatus> {
        self.symbol_statuses
            .iter()
            .find(|status| status.symbol == *symbol)
    }

    pub fn has_work_remaining(&self) -> bool {
        self.symbol_statuses
            .iter()
            .any(MatchingRuntimeSymbolStatus::has_work_remaining)
    }

    pub fn has_blocked_symbols(&self) -> bool {
        self.symbol_statuses
            .iter()
            .any(|status| status.output_commit_blocked)
    }

    pub fn has_full_input(&self) -> bool {
        self.symbol_statuses
            .iter()
            .any(|status| status.pending_input_full)
    }

    pub fn has_full_output(&self) -> bool {
        self.symbol_statuses
            .iter()
            .any(|status| status.pending_output_full)
    }

    pub fn symbols_with_remaining_work(&self) -> Vec<Symbol> {
        self.symbol_statuses
            .iter()
            .filter(|status| status.has_work_remaining())
            .map(|status| status.symbol.clone())
            .collect()
    }

    pub fn blocked_symbols(&self) -> Vec<Symbol> {
        self.symbol_statuses
            .iter()
            .filter(|status| status.output_commit_blocked)
            .map(|status| status.symbol.clone())
            .collect()
    }

    pub fn symbols_with_full_input(&self) -> Vec<Symbol> {
        self.symbol_statuses
            .iter()
            .filter(|status| status.pending_input_full)
            .map(|status| status.symbol.clone())
            .collect()
    }

    pub fn symbols_with_full_output(&self) -> Vec<Symbol> {
        self.symbol_statuses
            .iter()
            .filter(|status| status.pending_output_full)
            .map(|status| status.symbol.clone())
            .collect()
    }
}

impl MatchingRuntimeSymbolStatus {
    pub fn has_work_remaining(&self) -> bool {
        self.pending_input_len > 0 || self.pending_output_len > 0 || self.output_commit_blocked
    }
}

impl MatchingRuntimeRunReport {
    pub fn shard_report(&self, shard_id: RuntimeShardId) -> Option<&MatchingRuntimeShardRunReport> {
        self.shard_reports
            .iter()
            .find(|report| report.shard_id == shard_id)
    }

    pub fn stop_reason(&self) -> MatchingRuntimeRunStopReason {
        if self.is_idle() {
            return MatchingRuntimeRunStopReason::Idle;
        }

        if self.needs_another_run() {
            return MatchingRuntimeRunStopReason::RunLimitReached;
        }

        if self.has_blocked_symbols() {
            return MatchingRuntimeRunStopReason::Blocked;
        }

        MatchingRuntimeRunStopReason::RunLimitReached
    }

    pub fn made_progress(&self) -> bool {
        self.shard_reports
            .iter()
            .any(|report| report.run_report.made_progress)
    }

    pub fn has_work_remaining(&self) -> bool {
        self.shard_reports
            .iter()
            .any(|report| report.run_report.has_work_remaining)
    }

    pub fn has_blocked_symbols(&self) -> bool {
        self.shard_reports
            .iter()
            .any(|report| report.run_report.has_blocked_symbols)
    }

    pub fn is_idle(&self) -> bool {
        self.shard_reports
            .iter()
            .all(|report| report.run_report.is_idle())
    }

    pub fn idle_shards(&self) -> Vec<RuntimeShardId> {
        self.shard_reports
            .iter()
            .filter(|report| report.run_report.is_idle())
            .map(|report| report.shard_id)
            .collect()
    }

    pub fn shards_with_remaining_work(&self) -> Vec<RuntimeShardId> {
        self.shard_reports
            .iter()
            .filter(|report| report.run_report.has_work_remaining)
            .map(|report| report.shard_id)
            .collect()
    }

    pub fn blocked_shards(&self) -> Vec<RuntimeShardId> {
        self.shard_reports
            .iter()
            .filter(|report| report.run_report.has_blocked_symbols)
            .map(|report| report.shard_id)
            .collect()
    }

    pub fn symbols_with_remaining_work(&self) -> Vec<Symbol> {
        self.shard_reports
            .iter()
            .flat_map(|report| report.run_report.symbols_with_remaining_work())
            .collect()
    }

    pub fn blocked_symbols(&self) -> Vec<Symbol> {
        self.shard_reports
            .iter()
            .flat_map(|report| report.run_report.blocked_symbols())
            .collect()
    }

    pub fn shards_reaching_run_limit(&self) -> Vec<RuntimeShardId> {
        self.shard_reports
            .iter()
            .filter(|report| {
                report.run_report.stop_reason == ShardRuntimeRunStopReason::RunLimitReached
            })
            .map(|report| report.shard_id)
            .collect()
    }

    pub fn needs_another_run(&self) -> bool {
        self.shard_reports.iter().any(|report| {
            report.run_report.stop_reason == ShardRuntimeRunStopReason::RunLimitReached
                && report.run_report.has_work_remaining
        })
    }
}

impl MatchingRuntimeRunUntilIdleReport {
    pub fn configured_run_count(&self) -> usize {
        self.run_reports.len()
    }

    pub fn last_run_report(&self) -> Option<&MatchingRuntimeRunReport> {
        self.run_reports.last()
    }

    pub fn is_idle(&self) -> bool {
        self.stop_reason == MatchingRuntimeRunUntilIdleStopReason::Idle
    }

    pub fn has_work_remaining(&self) -> bool {
        self.final_status.has_work_remaining()
    }

    pub fn has_blocked_symbols(&self) -> bool {
        self.final_status.has_blocked_symbols()
    }

    pub fn blocked_shards(&self) -> Vec<RuntimeShardId> {
        self.final_status.blocked_shards()
    }

    pub fn shards_with_remaining_work(&self) -> Vec<RuntimeShardId> {
        self.final_status.shards_with_remaining_work()
    }

    pub fn symbols_with_remaining_work(&self) -> Vec<Symbol> {
        self.final_status.symbols_with_remaining_work()
    }

    pub fn blocked_symbols(&self) -> Vec<Symbol> {
        self.final_status.blocked_symbols()
    }
}

impl MatchingRuntimeDrainReport {
    pub fn configured_run_count(&self) -> usize {
        self.run_report.configured_run_count()
    }

    pub fn is_drained(&self) -> bool {
        self.stop_reason == MatchingRuntimeDrainStopReason::Drained
    }

    pub fn has_work_remaining(&self) -> bool {
        self.run_report.has_work_remaining()
    }

    pub fn shards_with_remaining_work(&self) -> Vec<RuntimeShardId> {
        self.run_report.shards_with_remaining_work()
    }

    pub fn has_blocked_symbols(&self) -> bool {
        self.run_report.has_blocked_symbols()
    }

    pub fn blocked_shards(&self) -> Vec<RuntimeShardId> {
        self.run_report.blocked_shards()
    }

    pub fn symbols_with_remaining_work(&self) -> Vec<Symbol> {
        self.run_report.symbols_with_remaining_work()
    }

    pub fn blocked_symbols(&self) -> Vec<Symbol> {
        self.run_report.blocked_symbols()
    }
}

impl MatchingRuntimeShutdownReport {
    pub fn has_work_remaining(&self) -> bool {
        self.final_status.has_work_remaining()
    }

    pub fn shards_with_remaining_work(&self) -> Vec<RuntimeShardId> {
        self.final_status.shards_with_remaining_work()
    }

    pub fn has_blocked_symbols(&self) -> bool {
        self.final_status.has_blocked_symbols()
    }

    pub fn blocked_shards(&self) -> Vec<RuntimeShardId> {
        self.final_status.blocked_shards()
    }

    pub fn symbols_with_remaining_work(&self) -> Vec<Symbol> {
        self.final_status.symbols_with_remaining_work()
    }

    pub fn blocked_symbols(&self) -> Vec<Symbol> {
        self.final_status.blocked_symbols()
    }
}
