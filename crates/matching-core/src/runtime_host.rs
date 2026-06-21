use crate::journal_adapter::{JournalInputEntry, JournalOutputAppender};
use crate::output_commit_boundary::OutputJournalClient;
use crate::runtime_config::{MatchingRuntimeConfig, RuntimeHostMode, RuntimeShardId};
use crate::runtime_loop::{
    RuntimeLoopError, RuntimeLoopRunLimit, RuntimeLoopRunOnceLimits, RuntimeLoopRunOnceReport,
    RuntimeLoopRunReport, RuntimeLoopRunStopReason,
};
use crate::runtime_manager::RuntimeManagerError;
use crate::runtime_shard_runner::RuntimeShardRunner;
use crate::runtime_topology::RuntimeTopologyError;
use crate::types::Symbol;

pub struct RuntimeHost {
    mode: RuntimeHostMode,
    runners: Vec<RuntimeShardRunner>,
    run_once_limits: RuntimeLoopRunOnceLimits,
    run_limit: RuntimeLoopRunLimit,
    run_until_idle_limit: RuntimeHostRunUntilIdleLimit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeHostError {
    UnsupportedMode(RuntimeHostMode),
    Topology(RuntimeTopologyError),
    RuntimeLoop(RuntimeLoopError),
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeHostStatus {
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
                let runners = RuntimeShardRunner::from_symbols_with_config(symbols, config)
                    .map_err(RuntimeHostError::Topology)?;

                Ok(Self {
                    mode,
                    runners,
                    run_once_limits,
                    run_limit,
                    run_until_idle_limit,
                })
            }
            unsupported => Err(RuntimeHostError::UnsupportedMode(unsupported)),
        }
    }

    pub fn mode(&self) -> RuntimeHostMode {
        self.mode
    }

    pub fn shard_count(&self) -> usize {
        self.runners.len()
    }

    pub fn shard_ids(&self) -> Vec<RuntimeShardId> {
        self.runners
            .iter()
            .map(RuntimeShardRunner::shard_id)
            .collect()
    }

    pub fn symbols_for_shard(&self, shard_id: RuntimeShardId) -> Option<&[Symbol]> {
        self.runners
            .iter()
            .find(|runner| runner.shard_id() == shard_id)
            .map(RuntimeShardRunner::symbols)
    }

    pub fn enqueue_input(&mut self, entry: JournalInputEntry) -> Result<(), RuntimeHostError> {
        let symbol = entry.command.symbol().clone();
        let runner = self
            .runners
            .iter_mut()
            .find(|runner| runner.symbols().contains(&symbol))
            .ok_or_else(|| {
                RuntimeHostError::RuntimeLoop(RuntimeLoopError::UnregisteredHandoff(symbol))
            })?;

        runner
            .enqueue_input(entry)
            .map_err(RuntimeHostError::RuntimeLoop)
    }

    pub fn status(&self) -> Result<RuntimeHostStatus, RuntimeHostError> {
        let mut shard_statuses = Vec::new();

        for runner in &self.runners {
            let mut symbol_statuses = Vec::new();

            for symbol in runner.symbols() {
                let pending_input_status =
                    runner.pending_input_status(symbol).ok_or_else(|| {
                        RuntimeHostError::RuntimeLoop(RuntimeLoopError::MissingHandoff(
                            symbol.clone(),
                        ))
                    })?;
                let runtime_status = runner.symbol_status(symbol).ok_or_else(|| {
                    RuntimeHostError::RuntimeLoop(RuntimeLoopError::RuntimeManager(
                        RuntimeManagerError::UnknownSymbol,
                    ))
                })?;

                symbol_statuses.push(RuntimeHostSymbolStatus {
                    symbol: symbol.clone(),
                    pending_input_len: pending_input_status.len,
                    pending_input_capacity: pending_input_status.capacity,
                    pending_input_full: pending_input_status.full,
                    pending_output_len: runtime_status.pending_output_len,
                    pending_output_capacity: runtime_status.pending_output_capacity,
                    pending_output_full: runtime_status.pending_output_full,
                    output_commit_blocked: runtime_status.output_commit_blockage.is_some(),
                });
            }

            shard_statuses.push(RuntimeHostShardStatus {
                shard_id: runner.shard_id(),
                symbol_statuses,
            });
        }

        Ok(RuntimeHostStatus { shard_statuses })
    }

    pub fn run_once_all(
        &mut self,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
        limits: RuntimeLoopRunOnceLimits,
    ) -> Result<RuntimeHostRunOnceReport, RuntimeHostError> {
        let mut shard_reports = Vec::new();

        for runner in &mut self.runners {
            let run_once_report = runner
                .run_once(journal_client, output, limits)
                .map_err(RuntimeHostError::RuntimeLoop)?;
            shard_reports.push(RuntimeHostShardRunOnceReport {
                shard_id: runner.shard_id(),
                run_once_report,
            });
        }

        Ok(RuntimeHostRunOnceReport { shard_reports })
    }

    pub fn run_limited_all(
        &mut self,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
        limits: RuntimeLoopRunOnceLimits,
        limit: RuntimeLoopRunLimit,
    ) -> Result<RuntimeHostRunReport, RuntimeHostError> {
        let mut shard_reports = Vec::new();

        for runner in &mut self.runners {
            let run_report = runner
                .run_limited(journal_client, output, limits, limit)
                .map_err(RuntimeHostError::RuntimeLoop)?;
            shard_reports.push(RuntimeHostShardRunReport {
                shard_id: runner.shard_id(),
                run_report,
            });
        }

        Ok(RuntimeHostRunReport { shard_reports })
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
