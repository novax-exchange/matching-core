use crate::journal_adapter::{JournalInputEntry, JournalOutputAppender};
use crate::matching_runtime_driver::{
    ManualMatchingRuntimeDriver, MatchingRuntimeDriver, MatchingRuntimeDriverError,
    MatchingRuntimeDriverShutdownReport,
};
use crate::output_commit_boundary::OutputJournalClient;
use crate::runtime_config::{MatchingRuntimeConfig, RuntimeExecutionMode, RuntimeShardId};
use crate::runtime_topology::RuntimeTopologyError;
use crate::shard_runtime::{
    ShardRuntimeError, ShardRuntimeRunLimit, ShardRuntimeRunOnceLimits, ShardRuntimeRunOnceReport,
    ShardRuntimeRunReport, ShardRuntimeRunStopReason,
};
use crate::types::Symbol;

pub struct MatchingRuntime {
    mode: RuntimeExecutionMode,
    driver: Box<dyn MatchingRuntimeDriver>,
    run_once_limits: ShardRuntimeRunOnceLimits,
    run_limit: ShardRuntimeRunLimit,
    run_until_idle_limit: MatchingRuntimeRunUntilIdleLimit,
    input_state: MatchingRuntimeInputState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MatchingRuntimeError {
    InputClosed,
    UnsupportedMode(RuntimeExecutionMode),
    RuntimeDriverRequired(RuntimeExecutionMode),
    RuntimeDriver(MatchingRuntimeDriverError),
    Topology(RuntimeTopologyError),
    ShardRuntime(ShardRuntimeError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchingRuntimeInputState {
    Open,
    Closed,
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
    pub driver_report: MatchingRuntimeDriverShutdownReport,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchingRuntimeStatus {
    pub input_state: MatchingRuntimeInputState,
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
    pub fn new_for_symbols_with_config(
        symbols: Vec<Symbol>,
        config: MatchingRuntimeConfig,
    ) -> Result<Self, MatchingRuntimeError> {
        match config.execution.mode {
            RuntimeExecutionMode::Manual => {
                let mode = config.execution.mode;
                let run_once_limits = ShardRuntimeRunOnceLimits::from_config(&config);
                let run_limit = ShardRuntimeRunLimit::from_config(&config);
                let run_until_idle_limit = MatchingRuntimeRunUntilIdleLimit::from_config(&config);
                let driver = ManualMatchingRuntimeDriver::from_symbols_with_config(symbols, config)
                    .map_err(MatchingRuntimeError::Topology)?;

                Ok(Self {
                    mode,
                    driver: Box::new(driver),
                    run_once_limits,
                    run_limit,
                    run_until_idle_limit,
                    input_state: MatchingRuntimeInputState::Open,
                })
            }
            RuntimeExecutionMode::ThreadPerShard | RuntimeExecutionMode::AsyncTaskPerShard => Err(
                MatchingRuntimeError::RuntimeDriverRequired(config.execution.mode),
            ),
        }
    }

    pub fn mode(&self) -> RuntimeExecutionMode {
        self.mode
    }

    pub fn shard_count(&self) -> usize {
        self.driver.shard_count()
    }

    pub fn shard_ids(&self) -> Vec<RuntimeShardId> {
        self.driver.shard_ids()
    }

    pub fn symbols_for_shard(&self, shard_id: RuntimeShardId) -> Option<&[Symbol]> {
        self.driver.symbols_for_shard(shard_id)
    }

    pub fn input_state(&self) -> MatchingRuntimeInputState {
        self.input_state
    }

    pub fn close_input(&mut self) {
        self.input_state = MatchingRuntimeInputState::Closed;
    }

    pub fn enqueue_input(&mut self, entry: JournalInputEntry) -> Result<(), MatchingRuntimeError> {
        self.ensure_input_open()?;

        self.driver
            .write_input(entry)
            .map_err(MatchingRuntimeError::from_driver_error)
    }

    pub fn enqueue_inputs(
        &mut self,
        entries: Vec<JournalInputEntry>,
    ) -> Result<usize, MatchingRuntimeError> {
        self.ensure_input_open()?;

        self.driver
            .write_inputs(entries)
            .map_err(MatchingRuntimeError::from_driver_error)
    }

    pub fn can_enqueue_inputs(
        &self,
        entries: &[JournalInputEntry],
    ) -> Result<(), MatchingRuntimeError> {
        self.ensure_input_open()?;
        self.driver
            .can_write_inputs(entries)
            .map_err(MatchingRuntimeError::from_driver_error)
    }

    pub fn status(&self) -> Result<MatchingRuntimeStatus, MatchingRuntimeError> {
        let shard_statuses = self
            .driver
            .shard_statuses()
            .map_err(MatchingRuntimeError::from_driver_error)?;

        Ok(MatchingRuntimeStatus {
            input_state: self.input_state,
            shard_statuses,
        })
    }

    fn ensure_input_open(&self) -> Result<(), MatchingRuntimeError> {
        if self.input_state == MatchingRuntimeInputState::Closed {
            return Err(MatchingRuntimeError::InputClosed);
        }

        Ok(())
    }

    pub fn run_once_all(
        &mut self,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
        limits: ShardRuntimeRunOnceLimits,
    ) -> Result<MatchingRuntimeRunOnceReport, MatchingRuntimeError> {
        self.driver
            .run_once_all(journal_client, output, limits)
            .map_err(MatchingRuntimeError::from_driver_error)
    }

    pub fn run_limited_all(
        &mut self,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
        limits: ShardRuntimeRunOnceLimits,
        limit: ShardRuntimeRunLimit,
    ) -> Result<MatchingRuntimeRunReport, MatchingRuntimeError> {
        self.driver
            .run_limited_all(journal_client, output, limits, limit)
            .map_err(MatchingRuntimeError::from_driver_error)
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
        self.close_input();

        let driver_report = self
            .driver
            .shutdown()
            .map_err(MatchingRuntimeError::from_driver_error)?;

        Ok(MatchingRuntimeShutdownReport {
            input_state: self.input_state,
            driver_report,
        })
    }

    pub fn run_until_idle(
        &mut self,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
        limit: MatchingRuntimeRunUntilIdleLimit,
    ) -> Result<MatchingRuntimeRunUntilIdleReport, MatchingRuntimeError> {
        let mut run_reports = Vec::new();

        for _ in 0..limit.max_run_calls {
            let run_report = self.run_configured_all(journal_client, output)?;

            let stop_reason = if run_report.is_idle() {
                Some(MatchingRuntimeRunUntilIdleStopReason::Idle)
            } else if run_report.has_blocked_symbols() && !run_report.needs_another_run() {
                Some(MatchingRuntimeRunUntilIdleStopReason::Blocked)
            } else {
                None
            };

            run_reports.push(run_report);

            if let Some(stop_reason) = stop_reason {
                return Ok(MatchingRuntimeRunUntilIdleReport {
                    run_reports,
                    stop_reason,
                });
            }
        }

        Ok(MatchingRuntimeRunUntilIdleReport {
            run_reports,
            stop_reason: MatchingRuntimeRunUntilIdleStopReason::CallLimitReached,
        })
    }
}

impl MatchingRuntimeError {
    fn from_driver_error(error: MatchingRuntimeDriverError) -> Self {
        match error {
            MatchingRuntimeDriverError::ShardRuntime(error) => Self::ShardRuntime(error),
            error => Self::RuntimeDriver(error),
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
        self.last_run_report()
            .map(MatchingRuntimeRunReport::has_work_remaining)
            .unwrap_or(false)
    }

    pub fn has_blocked_symbols(&self) -> bool {
        self.last_run_report()
            .map(MatchingRuntimeRunReport::has_blocked_symbols)
            .unwrap_or(false)
    }

    pub fn blocked_shards(&self) -> Vec<RuntimeShardId> {
        self.last_run_report()
            .map(MatchingRuntimeRunReport::blocked_shards)
            .unwrap_or_default()
    }
}

impl MatchingRuntimeDrainReport {
    pub fn configured_run_count(&self) -> usize {
        self.run_report.configured_run_count()
    }

    pub fn is_drained(&self) -> bool {
        self.stop_reason == MatchingRuntimeDrainStopReason::Drained
    }

    pub fn has_blocked_symbols(&self) -> bool {
        self.run_report.has_blocked_symbols()
    }

    pub fn blocked_shards(&self) -> Vec<RuntimeShardId> {
        self.run_report.blocked_shards()
    }
}
