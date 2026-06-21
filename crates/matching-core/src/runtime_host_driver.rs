use crate::journal_adapter::{JournalInputEntry, JournalOutputAppender};
use crate::output_commit_boundary::OutputJournalClient;
use crate::runtime_config::{MatchingRuntimeConfig, RuntimeShardId};
use crate::runtime_host::{
    RuntimeHostRunOnceReport, RuntimeHostRunReport, RuntimeHostShardRunOnceReport,
    RuntimeHostShardRunReport, RuntimeHostShardStatus, RuntimeHostSymbolStatus,
};
use crate::runtime_loop::{
    RuntimeLoopError, RuntimeLoopRunLimit, RuntimeLoopRunOnceLimits, RuntimeLoopRunOnceReport,
    RuntimeLoopRunReport,
};
use crate::runtime_manager::{RuntimeManagerError, SymbolRuntimeStatus};
use crate::runtime_shard_runner::RuntimeShardRunner;
use crate::runtime_topology::RuntimeTopologyError;
use crate::types::Symbol;
use std::collections::HashMap;

pub trait RuntimeHostDriver {
    fn shard_count(&self) -> usize;
    fn shard_ids(&self) -> Vec<RuntimeShardId>;
    fn symbols_for_shard(&self, shard_id: RuntimeShardId) -> Option<&[Symbol]>;
    fn enqueue_input(&mut self, entry: JournalInputEntry) -> Result<(), RuntimeLoopError>;
    fn enqueue_inputs(
        &mut self,
        entries: Vec<JournalInputEntry>,
    ) -> Result<usize, RuntimeLoopError>;
    fn can_enqueue_inputs(&self, entries: &[JournalInputEntry]) -> Result<(), RuntimeLoopError>;
    fn shard_statuses(&self) -> Result<Vec<RuntimeHostShardStatus>, RuntimeLoopError>;
    fn run_once_all(
        &mut self,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
        limits: RuntimeLoopRunOnceLimits,
    ) -> Result<RuntimeHostRunOnceReport, RuntimeLoopError>;
    fn run_limited_all(
        &mut self,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
        limits: RuntimeLoopRunOnceLimits,
        limit: RuntimeLoopRunLimit,
    ) -> Result<RuntimeHostRunReport, RuntimeLoopError>;
}

pub struct ManualRuntimeHostDriver {
    runners: Vec<RuntimeShardRunner>,
}

impl ManualRuntimeHostDriver {
    pub fn from_symbols_with_config(
        symbols: Vec<Symbol>,
        config: MatchingRuntimeConfig,
    ) -> Result<Self, RuntimeTopologyError> {
        Ok(Self {
            runners: RuntimeShardRunner::from_symbols_with_config(symbols, config)?,
        })
    }

    fn runner_index_for_symbol(&self, symbol: &Symbol) -> Option<usize> {
        self.runners
            .iter()
            .position(|runner| runner.symbols().contains(symbol))
    }

    fn validate_enqueue_inputs(
        &self,
        entries: &[JournalInputEntry],
    ) -> Result<HashMap<Symbol, usize>, RuntimeLoopError> {
        let mut requested_by_symbol: HashMap<Symbol, usize> = HashMap::new();
        let mut owner_by_symbol: HashMap<Symbol, usize> = HashMap::new();

        for entry in entries {
            let symbol = entry.command.symbol().clone();
            let runner_index = self
                .runner_index_for_symbol(&symbol)
                .ok_or_else(|| RuntimeLoopError::UnregisteredHandoff(symbol.clone()))?;

            *requested_by_symbol.entry(symbol.clone()).or_insert(0) += 1;
            owner_by_symbol.insert(symbol, runner_index);
        }

        let mut requested_symbols: Vec<Symbol> = requested_by_symbol.keys().cloned().collect();
        requested_symbols.sort_by(|left, right| left.0.cmp(&right.0));

        for symbol in requested_symbols {
            let requested_count = requested_by_symbol
                .get(&symbol)
                .expect("requested symbol should have a requested count");
            let runner_index = owner_by_symbol
                .get(&symbol)
                .expect("requested symbol should have an owning runner");
            let pending_input_status = self.runners[*runner_index]
                .pending_input_status(&symbol)
                .ok_or_else(|| RuntimeLoopError::MissingHandoff(symbol.clone()))?;
            let available_capacity = pending_input_status
                .capacity
                .saturating_sub(pending_input_status.len);

            if available_capacity < *requested_count {
                return Err(RuntimeLoopError::InputHandoffFull(symbol));
            }
        }

        Ok(owner_by_symbol)
    }
}

impl RuntimeHostDriver for ManualRuntimeHostDriver {
    fn shard_count(&self) -> usize {
        self.runners.len()
    }

    fn shard_ids(&self) -> Vec<RuntimeShardId> {
        self.runners
            .iter()
            .map(RuntimeShardRunner::shard_id)
            .collect()
    }

    fn symbols_for_shard(&self, shard_id: RuntimeShardId) -> Option<&[Symbol]> {
        self.runners
            .iter()
            .find(|runner| runner.shard_id() == shard_id)
            .map(RuntimeShardRunner::symbols)
    }

    fn enqueue_input(&mut self, entry: JournalInputEntry) -> Result<(), RuntimeLoopError> {
        let symbol = entry.command.symbol().clone();
        let runner_index = self
            .runner_index_for_symbol(&symbol)
            .ok_or(RuntimeLoopError::UnregisteredHandoff(symbol))?;

        self.runners[runner_index].enqueue_input(entry)
    }

    fn enqueue_inputs(
        &mut self,
        entries: Vec<JournalInputEntry>,
    ) -> Result<usize, RuntimeLoopError> {
        let owner_by_symbol = self.validate_enqueue_inputs(&entries)?;
        let enqueued_count = entries.len();
        let mut entries_by_runner: Vec<Vec<JournalInputEntry>> =
            (0..self.runners.len()).map(|_| Vec::new()).collect();

        for entry in entries {
            let symbol = entry.command.symbol().clone();
            let runner_index = owner_by_symbol
                .get(&symbol)
                .expect("entry symbol should have an owning runner after validation");
            entries_by_runner[*runner_index].push(entry);
        }

        for (runner, runner_entries) in self.runners.iter_mut().zip(entries_by_runner) {
            if !runner_entries.is_empty() {
                runner.enqueue_inputs(runner_entries)?;
            }
        }

        Ok(enqueued_count)
    }

    fn can_enqueue_inputs(&self, entries: &[JournalInputEntry]) -> Result<(), RuntimeLoopError> {
        self.validate_enqueue_inputs(entries).map(|_| ())
    }

    fn shard_statuses(&self) -> Result<Vec<RuntimeHostShardStatus>, RuntimeLoopError> {
        let mut shard_statuses = Vec::new();

        for runner in &self.runners {
            let mut symbol_statuses = Vec::new();

            for symbol in runner.symbols() {
                let pending_input_status = runner
                    .pending_input_status(symbol)
                    .ok_or_else(|| RuntimeLoopError::MissingHandoff(symbol.clone()))?;
                let runtime_status =
                    runner
                        .symbol_status(symbol)
                        .ok_or(RuntimeLoopError::RuntimeManager(
                            RuntimeManagerError::UnknownSymbol,
                        ))?;

                symbol_statuses.push(symbol_status_from_runtime_status(
                    symbol.clone(),
                    pending_input_status.len,
                    pending_input_status.capacity,
                    pending_input_status.full,
                    runtime_status,
                ));
            }

            shard_statuses.push(RuntimeHostShardStatus {
                shard_id: runner.shard_id(),
                symbol_statuses,
            });
        }

        Ok(shard_statuses)
    }

    fn run_once_all(
        &mut self,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
        limits: RuntimeLoopRunOnceLimits,
    ) -> Result<RuntimeHostRunOnceReport, RuntimeLoopError> {
        let mut shard_reports = Vec::new();

        for runner in &mut self.runners {
            let run_once_report: RuntimeLoopRunOnceReport =
                runner.run_once(journal_client, output, limits)?;
            shard_reports.push(RuntimeHostShardRunOnceReport {
                shard_id: runner.shard_id(),
                run_once_report,
            });
        }

        Ok(RuntimeHostRunOnceReport { shard_reports })
    }

    fn run_limited_all(
        &mut self,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
        limits: RuntimeLoopRunOnceLimits,
        limit: RuntimeLoopRunLimit,
    ) -> Result<RuntimeHostRunReport, RuntimeLoopError> {
        let mut shard_reports = Vec::new();

        for runner in &mut self.runners {
            let run_report: RuntimeLoopRunReport =
                runner.run_limited(journal_client, output, limits, limit)?;
            shard_reports.push(RuntimeHostShardRunReport {
                shard_id: runner.shard_id(),
                run_report,
            });
        }

        Ok(RuntimeHostRunReport { shard_reports })
    }
}

fn symbol_status_from_runtime_status(
    symbol: Symbol,
    pending_input_len: usize,
    pending_input_capacity: usize,
    pending_input_full: bool,
    runtime_status: SymbolRuntimeStatus,
) -> RuntimeHostSymbolStatus {
    RuntimeHostSymbolStatus {
        symbol,
        pending_input_len,
        pending_input_capacity,
        pending_input_full,
        pending_output_len: runtime_status.pending_output_len,
        pending_output_capacity: runtime_status.pending_output_capacity,
        pending_output_full: runtime_status.pending_output_full,
        output_commit_blocked: runtime_status.output_commit_blockage.is_some(),
    }
}
