use crate::journal_adapter::{JournalInputEntry, JournalOutputAppender};
use crate::output_commit_boundary::OutputJournalClient;
use crate::runtime_config::{MatchingRuntimeConfig, RuntimeHostMode, RuntimeShardId};
use crate::runtime_loop::{RuntimeLoopError, RuntimeLoopTickLimits, RuntimeLoopTickReport};
use crate::runtime_shard_runner::RuntimeShardRunner;
use crate::runtime_topology::RuntimeTopologyError;
use crate::types::Symbol;

pub struct RuntimeHost {
    mode: RuntimeHostMode,
    runners: Vec<RuntimeShardRunner>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeHostError {
    UnsupportedMode(RuntimeHostMode),
    Topology(RuntimeTopologyError),
    RuntimeLoop(RuntimeLoopError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeHostTickReport {
    pub shard_reports: Vec<RuntimeHostShardTickReport>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeHostShardTickReport {
    pub shard_id: RuntimeShardId,
    pub tick_report: RuntimeLoopTickReport,
}

impl RuntimeHost {
    pub fn new_for_symbols_with_config(
        symbols: Vec<Symbol>,
        config: MatchingRuntimeConfig,
    ) -> Result<Self, RuntimeHostError> {
        match config.host.mode {
            RuntimeHostMode::Manual => {
                let mode = config.host.mode;
                let runners = RuntimeShardRunner::from_symbols_with_config(symbols, config)
                    .map_err(RuntimeHostError::Topology)?;

                Ok(Self { mode, runners })
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

    pub fn run_tick_all(
        &mut self,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
        limits: RuntimeLoopTickLimits,
    ) -> Result<RuntimeHostTickReport, RuntimeHostError> {
        let mut shard_reports = Vec::new();

        for runner in &mut self.runners {
            let tick_report = runner
                .run_tick(journal_client, output, limits)
                .map_err(RuntimeHostError::RuntimeLoop)?;
            shard_reports.push(RuntimeHostShardTickReport {
                shard_id: runner.shard_id(),
                tick_report,
            });
        }

        Ok(RuntimeHostTickReport { shard_reports })
    }
}

impl RuntimeHostTickReport {
    pub fn shard_report(&self, shard_id: RuntimeShardId) -> Option<&RuntimeHostShardTickReport> {
        self.shard_reports
            .iter()
            .find(|report| report.shard_id == shard_id)
    }
}
