use crate::journal_adapter::{JournalInputEntry, JournalOutputAppender};
use crate::output_commit_boundary::OutputJournalClient;
use crate::runtime_config::{MatchingRuntimeConfig, RuntimeHostMode, RuntimeShardId};
use crate::runtime_loop::{
    RuntimeLoopError, RuntimeLoopRunLimit, RuntimeLoopRunOnceLimits, RuntimeLoopRunOnceReport,
    RuntimeLoopRunReport, RuntimeLoopRunStopReason,
};
use crate::runtime_shard_runner::RuntimeShardRunner;
use crate::runtime_topology::RuntimeTopologyError;
use crate::types::Symbol;

pub struct RuntimeHost {
    mode: RuntimeHostMode,
    runners: Vec<RuntimeShardRunner>,
    run_once_limits: RuntimeLoopRunOnceLimits,
    run_limit: RuntimeLoopRunLimit,
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
                let runners = RuntimeShardRunner::from_symbols_with_config(symbols, config)
                    .map_err(RuntimeHostError::Topology)?;

                Ok(Self {
                    mode,
                    runners,
                    run_once_limits,
                    run_limit,
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
}

impl RuntimeHostRunOnceReport {
    pub fn shard_report(&self, shard_id: RuntimeShardId) -> Option<&RuntimeHostShardRunOnceReport> {
        self.shard_reports
            .iter()
            .find(|report| report.shard_id == shard_id)
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
