use crate::journal_adapter::{JournalInputEntry, JournalOutputAppender};
use crate::output_commit_boundary::OutputJournalClient;
use crate::runtime_config::{MatchingRuntimeConfig, RuntimeHostMode, RuntimeShardId};
use crate::runtime_host_driver::{
    ManualRuntimeHostDriver, RuntimeHostDriver, RuntimeHostDriverError,
};
use crate::runtime_loop::{
    RuntimeLoopError, RuntimeLoopRunLimit, RuntimeLoopRunOnceLimits, RuntimeLoopRunOnceReport,
    RuntimeLoopRunReport, RuntimeLoopRunStopReason,
};
use crate::runtime_topology::RuntimeTopologyError;
use crate::types::Symbol;

pub struct RuntimeHost {
    mode: RuntimeHostMode,
    driver: Box<dyn RuntimeHostDriver>,
    run_once_limits: RuntimeLoopRunOnceLimits,
    run_limit: RuntimeLoopRunLimit,
    run_until_idle_limit: RuntimeHostRunUntilIdleLimit,
    input_state: RuntimeHostInputState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeHostError {
    InputClosed,
    UnsupportedMode(RuntimeHostMode),
    RuntimeDriverRequired(RuntimeHostMode),
    RuntimeDriver(RuntimeHostDriverError),
    Topology(RuntimeTopologyError),
    RuntimeLoop(RuntimeLoopError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeHostInputState {
    Open,
    Closed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeHostRunOnceReport {
    pub shard_reports: Vec<RuntimeHostShardRunOnceReport>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeHostRunReport {
    pub shard_reports: Vec<RuntimeHostShardRunReport>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeHostRunUntilIdleLimit {
    pub max_run_calls: usize,
}

impl RuntimeHostRunUntilIdleLimit {
    pub fn from_config(config: &MatchingRuntimeConfig) -> Self {
        Self {
            max_run_calls: config.host.max_run_calls_per_until_idle,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeHostRunUntilIdleStopReason {
    Idle,
    Blocked,
    CallLimitReached,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeHostRunUntilIdleReport {
    pub run_reports: Vec<RuntimeHostRunReport>,
    pub stop_reason: RuntimeHostRunUntilIdleStopReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeHostDrainStopReason {
    Drained,
    Blocked,
    CallLimitReached,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeHostDrainReport {
    pub run_report: RuntimeHostRunUntilIdleReport,
    pub stop_reason: RuntimeHostDrainStopReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeHostStatus {
    pub input_state: RuntimeHostInputState,
    pub shard_statuses: Vec<RuntimeHostShardStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeHostShardStatus {
    pub shard_id: RuntimeShardId,
    pub symbol_statuses: Vec<RuntimeHostSymbolStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeHostSymbolStatus {
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
pub struct RuntimeHostShardRunOnceReport {
    pub shard_id: RuntimeShardId,
    pub run_once_report: RuntimeLoopRunOnceReport,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeHostShardRunReport {
    pub shard_id: RuntimeShardId,
    pub run_report: RuntimeLoopRunReport,
}

impl RuntimeHost {
    pub fn new_for_symbols_with_config(
        symbols: Vec<Symbol>,
        config: MatchingRuntimeConfig,
    ) -> Result<Self, RuntimeHostError> {
        match config.host.mode {
            RuntimeHostMode::Manual => {
                let mode = config.host.mode;
                let run_once_limits = RuntimeLoopRunOnceLimits::from_config(&config);
                let run_limit = RuntimeLoopRunLimit::from_config(&config);
                let run_until_idle_limit = RuntimeHostRunUntilIdleLimit::from_config(&config);
                let driver = ManualRuntimeHostDriver::from_symbols_with_config(symbols, config)
                    .map_err(RuntimeHostError::Topology)?;

                Ok(Self {
                    mode,
                    driver: Box::new(driver),
                    run_once_limits,
                    run_limit,
                    run_until_idle_limit,
                    input_state: RuntimeHostInputState::Open,
                })
            }
            RuntimeHostMode::ThreadPerShard
            | RuntimeHostMode::AsyncTaskPerShard
            | RuntimeHostMode::ProcessPerShard => {
                Err(RuntimeHostError::RuntimeDriverRequired(config.host.mode))
            }
            unsupported => Err(RuntimeHostError::UnsupportedMode(unsupported)),
        }
    }

    pub fn mode(&self) -> RuntimeHostMode {
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

    pub fn input_state(&self) -> RuntimeHostInputState {
        self.input_state
    }

    pub fn close_input(&mut self) {
        self.input_state = RuntimeHostInputState::Closed;
    }

    pub fn enqueue_input(&mut self, entry: JournalInputEntry) -> Result<(), RuntimeHostError> {
        self.ensure_input_open()?;

        self.driver
            .enqueue_input(entry)
            .map_err(RuntimeHostError::from_driver_error)
    }

    pub fn enqueue_inputs(
        &mut self,
        entries: Vec<JournalInputEntry>,
    ) -> Result<usize, RuntimeHostError> {
        self.ensure_input_open()?;

        self.driver
            .enqueue_inputs(entries)
            .map_err(RuntimeHostError::from_driver_error)
    }

    pub fn can_enqueue_inputs(
        &self,
        entries: &[JournalInputEntry],
    ) -> Result<(), RuntimeHostError> {
        self.ensure_input_open()?;
        self.driver
            .can_enqueue_inputs(entries)
            .map_err(RuntimeHostError::from_driver_error)
    }

    pub fn status(&self) -> Result<RuntimeHostStatus, RuntimeHostError> {
        let shard_statuses = self
            .driver
            .shard_statuses()
            .map_err(RuntimeHostError::from_driver_error)?;

        Ok(RuntimeHostStatus {
            input_state: self.input_state,
            shard_statuses,
        })
    }

    fn ensure_input_open(&self) -> Result<(), RuntimeHostError> {
        if self.input_state == RuntimeHostInputState::Closed {
            return Err(RuntimeHostError::InputClosed);
        }

        Ok(())
    }

    pub fn run_once_all(
        &mut self,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
        limits: RuntimeLoopRunOnceLimits,
    ) -> Result<RuntimeHostRunOnceReport, RuntimeHostError> {
        self.driver
            .run_once_all(journal_client, output, limits)
            .map_err(RuntimeHostError::from_driver_error)
    }

    pub fn run_limited_all(
        &mut self,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
        limits: RuntimeLoopRunOnceLimits,
        limit: RuntimeLoopRunLimit,
    ) -> Result<RuntimeHostRunReport, RuntimeHostError> {
        self.driver
            .run_limited_all(journal_client, output, limits, limit)
            .map_err(RuntimeHostError::from_driver_error)
    }

    pub fn run_configured_all(
        &mut self,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
    ) -> Result<RuntimeHostRunReport, RuntimeHostError> {
        self.run_limited_all(journal_client, output, self.run_once_limits, self.run_limit)
    }

    pub fn run_until_idle_configured(
        &mut self,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
    ) -> Result<RuntimeHostRunUntilIdleReport, RuntimeHostError> {
        self.run_until_idle(journal_client, output, self.run_until_idle_limit)
    }

    pub fn drain_configured(
        &mut self,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
    ) -> Result<RuntimeHostDrainReport, RuntimeHostError> {
        self.close_input();

        let run_report = self.run_until_idle_configured(journal_client, output)?;
        let stop_reason = match run_report.stop_reason {
            RuntimeHostRunUntilIdleStopReason::Idle => RuntimeHostDrainStopReason::Drained,
            RuntimeHostRunUntilIdleStopReason::Blocked => RuntimeHostDrainStopReason::Blocked,
            RuntimeHostRunUntilIdleStopReason::CallLimitReached => {
                RuntimeHostDrainStopReason::CallLimitReached
            }
        };

        Ok(RuntimeHostDrainReport {
            run_report,
            stop_reason,
        })
    }

    pub fn run_until_idle(
        &mut self,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
        limit: RuntimeHostRunUntilIdleLimit,
    ) -> Result<RuntimeHostRunUntilIdleReport, RuntimeHostError> {
        let mut run_reports = Vec::new();

        for _ in 0..limit.max_run_calls {
            let run_report = self.run_configured_all(journal_client, output)?;

            let stop_reason = if run_report.is_idle() {
                Some(RuntimeHostRunUntilIdleStopReason::Idle)
            } else if run_report.has_blocked_symbols() && !run_report.needs_another_run() {
                Some(RuntimeHostRunUntilIdleStopReason::Blocked)
            } else {
                None
            };

            run_reports.push(run_report);

            if let Some(stop_reason) = stop_reason {
                return Ok(RuntimeHostRunUntilIdleReport {
                    run_reports,
                    stop_reason,
                });
            }
        }

        Ok(RuntimeHostRunUntilIdleReport {
            run_reports,
            stop_reason: RuntimeHostRunUntilIdleStopReason::CallLimitReached,
        })
    }
}

impl RuntimeHostError {
    fn from_driver_error(error: RuntimeHostDriverError) -> Self {
        match error {
            RuntimeHostDriverError::RuntimeLoop(error) => Self::RuntimeLoop(error),
            error => Self::RuntimeDriver(error),
        }
    }
}

impl RuntimeHostRunOnceReport {
    pub fn shard_report(&self, shard_id: RuntimeShardId) -> Option<&RuntimeHostShardRunOnceReport> {
        self.shard_reports
            .iter()
            .find(|report| report.shard_id == shard_id)
    }
}

impl RuntimeHostStatus {
    pub fn shard_status(&self, shard_id: RuntimeShardId) -> Option<&RuntimeHostShardStatus> {
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
            .any(RuntimeHostShardStatus::has_work_remaining)
    }

    pub fn has_blocked_symbols(&self) -> bool {
        self.shard_statuses
            .iter()
            .any(RuntimeHostShardStatus::has_blocked_symbols)
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
            .flat_map(RuntimeHostShardStatus::symbols_with_full_input)
            .collect()
    }

    pub fn symbols_with_full_output(&self) -> Vec<Symbol> {
        self.shard_statuses
            .iter()
            .flat_map(RuntimeHostShardStatus::symbols_with_full_output)
            .collect()
    }
}

impl RuntimeHostShardStatus {
    pub fn symbol_status(&self, symbol: &Symbol) -> Option<&RuntimeHostSymbolStatus> {
        self.symbol_statuses
            .iter()
            .find(|status| status.symbol == *symbol)
    }

    pub fn has_work_remaining(&self) -> bool {
        self.symbol_statuses
            .iter()
            .any(RuntimeHostSymbolStatus::has_work_remaining)
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

impl RuntimeHostSymbolStatus {
    pub fn has_work_remaining(&self) -> bool {
        self.pending_input_len > 0 || self.pending_output_len > 0 || self.output_commit_blocked
    }
}

impl RuntimeHostRunReport {
    pub fn shard_report(&self, shard_id: RuntimeShardId) -> Option<&RuntimeHostShardRunReport> {
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
                report.run_report.stop_reason == RuntimeLoopRunStopReason::RunLimitReached
            })
            .map(|report| report.shard_id)
            .collect()
    }

    pub fn needs_another_run(&self) -> bool {
        self.shard_reports.iter().any(|report| {
            report.run_report.stop_reason == RuntimeLoopRunStopReason::RunLimitReached
                && report.run_report.has_work_remaining
        })
    }
}

impl RuntimeHostRunUntilIdleReport {
    pub fn configured_run_count(&self) -> usize {
        self.run_reports.len()
    }

    pub fn last_run_report(&self) -> Option<&RuntimeHostRunReport> {
        self.run_reports.last()
    }

    pub fn is_idle(&self) -> bool {
        self.stop_reason == RuntimeHostRunUntilIdleStopReason::Idle
    }

    pub fn has_work_remaining(&self) -> bool {
        self.last_run_report()
            .map(RuntimeHostRunReport::has_work_remaining)
            .unwrap_or(false)
    }

    pub fn has_blocked_symbols(&self) -> bool {
        self.last_run_report()
            .map(RuntimeHostRunReport::has_blocked_symbols)
            .unwrap_or(false)
    }

    pub fn blocked_shards(&self) -> Vec<RuntimeShardId> {
        self.last_run_report()
            .map(RuntimeHostRunReport::blocked_shards)
            .unwrap_or_default()
    }
}

impl RuntimeHostDrainReport {
    pub fn configured_run_count(&self) -> usize {
        self.run_report.configured_run_count()
    }

    pub fn is_drained(&self) -> bool {
        self.stop_reason == RuntimeHostDrainStopReason::Drained
    }

    pub fn has_blocked_symbols(&self) -> bool {
        self.run_report.has_blocked_symbols()
    }

    pub fn blocked_shards(&self) -> Vec<RuntimeShardId> {
        self.run_report.blocked_shards()
    }
}
