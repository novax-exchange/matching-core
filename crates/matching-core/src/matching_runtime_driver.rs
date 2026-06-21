use crate::journal_adapter::{JournalInputEntry, JournalOutputAppender};
use crate::matching_runtime::{
    MatchingRuntimeRunOnceReport, MatchingRuntimeRunReport, MatchingRuntimeShardRunOnceReport,
    MatchingRuntimeShardRunReport, MatchingRuntimeShardStatus, MatchingRuntimeSymbolStatus,
};
use crate::output_commit_boundary::OutputJournalClient;
use crate::runtime_config::{MatchingRuntimeConfig, RuntimeShardId};
use crate::runtime_topology::RuntimeTopologyError;
use crate::shard_execution_core::{ShardExecutionCoreError, SymbolRuntimeStatus};
use crate::shard_runtime::{
    ShardRuntime, ShardRuntimeError, ShardRuntimeRunLimit, ShardRuntimeRunOnceLimits,
    ShardRuntimeRunOnceReport, ShardRuntimeRunReport,
};
use crate::types::Symbol;
use std::collections::HashMap;

pub trait InputHandoffWriter {
    fn plan_writes(
        &self,
        entries: &[JournalInputEntry],
    ) -> Result<Vec<InputHandoffWriteCommand>, MatchingRuntimeDriverError>;
    fn write_input(&mut self, entry: JournalInputEntry) -> Result<(), MatchingRuntimeDriverError>;
    fn write_inputs(
        &mut self,
        entries: Vec<JournalInputEntry>,
    ) -> Result<usize, MatchingRuntimeDriverError>;
    fn can_write_inputs(
        &self,
        entries: &[JournalInputEntry],
    ) -> Result<(), MatchingRuntimeDriverError>;
}

pub trait MatchingRuntimeDriver: InputHandoffWriter {
    fn shard_count(&self) -> usize;
    fn shard_ids(&self) -> Vec<RuntimeShardId>;
    fn symbols_for_shard(&self, shard_id: RuntimeShardId) -> Option<&[Symbol]>;
    fn shard_statuses(&self)
        -> Result<Vec<MatchingRuntimeShardStatus>, MatchingRuntimeDriverError>;
    fn run_once_all(
        &mut self,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
        limits: ShardRuntimeRunOnceLimits,
    ) -> Result<MatchingRuntimeRunOnceReport, MatchingRuntimeDriverError>;
    fn run_limited_all(
        &mut self,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
        limits: ShardRuntimeRunOnceLimits,
        limit: ShardRuntimeRunLimit,
    ) -> Result<MatchingRuntimeRunReport, MatchingRuntimeDriverError>;
    fn shutdown(
        &mut self,
    ) -> Result<MatchingRuntimeDriverShutdownReport, MatchingRuntimeDriverError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MatchingRuntimeDriverError {
    ShardRuntime(ShardRuntimeError),
    DriverUnavailable(RuntimeShardId),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InputHandoffWriteCommand {
    WriteInputs {
        shard_id: RuntimeShardId,
        entries: Vec<JournalInputEntry>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchingRuntimeDriverShutdownReport {
    pub shard_ids: Vec<RuntimeShardId>,
}

impl From<ShardRuntimeError> for MatchingRuntimeDriverError {
    fn from(error: ShardRuntimeError) -> Self {
        Self::ShardRuntime(error)
    }
}

pub struct ManualMatchingRuntimeDriver {
    runtimes: Vec<ShardRuntime>,
}

impl ManualMatchingRuntimeDriver {
    pub fn from_symbols_with_config(
        symbols: Vec<Symbol>,
        config: MatchingRuntimeConfig,
    ) -> Result<Self, RuntimeTopologyError> {
        Ok(Self {
            runtimes: ShardRuntime::from_symbols_with_config(symbols, config)?,
        })
    }

    fn runtime_index_for_symbol(&self, symbol: &Symbol) -> Option<usize> {
        self.runtimes
            .iter()
            .position(|runtime| runtime.symbols().contains(symbol))
    }

    fn validate_enqueue_inputs(
        &self,
        entries: &[JournalInputEntry],
    ) -> Result<HashMap<Symbol, usize>, MatchingRuntimeDriverError> {
        let mut requested_by_symbol: HashMap<Symbol, usize> = HashMap::new();
        let mut owner_by_symbol: HashMap<Symbol, usize> = HashMap::new();

        for entry in entries {
            let symbol = entry.command.symbol().clone();
            let runtime_index = self.runtime_index_for_symbol(&symbol).ok_or_else(|| {
                MatchingRuntimeDriverError::ShardRuntime(ShardRuntimeError::UnregisteredHandoff(
                    symbol.clone(),
                ))
            })?;

            *requested_by_symbol.entry(symbol.clone()).or_insert(0) += 1;
            owner_by_symbol.insert(symbol, runtime_index);
        }

        let mut requested_symbols: Vec<Symbol> = requested_by_symbol.keys().cloned().collect();
        requested_symbols.sort_by(|left, right| left.0.cmp(&right.0));

        for symbol in requested_symbols {
            let requested_count = requested_by_symbol
                .get(&symbol)
                .expect("requested symbol should have a requested count");
            let runtime_index = owner_by_symbol
                .get(&symbol)
                .expect("requested symbol should have an owning runtime");
            let pending_input_status = self.runtimes[*runtime_index]
                .pending_input_status(&symbol)
                .ok_or_else(|| {
                    MatchingRuntimeDriverError::ShardRuntime(ShardRuntimeError::MissingHandoff(
                        symbol.clone(),
                    ))
                })?;
            let available_capacity = pending_input_status
                .capacity
                .saturating_sub(pending_input_status.len);

            if available_capacity < *requested_count {
                return Err(MatchingRuntimeDriverError::ShardRuntime(
                    ShardRuntimeError::InputHandoffFull(symbol),
                ));
            }
        }

        Ok(owner_by_symbol)
    }
}

impl InputHandoffWriter for ManualMatchingRuntimeDriver {
    fn plan_writes(
        &self,
        entries: &[JournalInputEntry],
    ) -> Result<Vec<InputHandoffWriteCommand>, MatchingRuntimeDriverError> {
        let owner_by_symbol = self.validate_enqueue_inputs(entries)?;
        let mut entries_by_runtime: Vec<Vec<JournalInputEntry>> =
            (0..self.runtimes.len()).map(|_| Vec::new()).collect();

        for entry in entries {
            let symbol = entry.command.symbol().clone();
            let runtime_index = owner_by_symbol
                .get(&symbol)
                .expect("entry symbol should have an owning runtime after validation");
            entries_by_runtime[*runtime_index].push(entry.clone());
        }

        Ok(self
            .runtimes
            .iter()
            .zip(entries_by_runtime)
            .filter(|(_, entries)| !entries.is_empty())
            .map(|(runtime, entries)| InputHandoffWriteCommand::WriteInputs {
                shard_id: runtime.shard_id(),
                entries,
            })
            .collect())
    }

    fn write_input(&mut self, entry: JournalInputEntry) -> Result<(), MatchingRuntimeDriverError> {
        let symbol = entry.command.symbol().clone();
        let runtime_index = self.runtime_index_for_symbol(&symbol).ok_or(
            MatchingRuntimeDriverError::ShardRuntime(ShardRuntimeError::UnregisteredHandoff(
                symbol,
            )),
        )?;

        self.runtimes[runtime_index]
            .enqueue_input(entry)
            .map_err(MatchingRuntimeDriverError::from)
    }

    fn write_inputs(
        &mut self,
        entries: Vec<JournalInputEntry>,
    ) -> Result<usize, MatchingRuntimeDriverError> {
        let written_count = entries.len();
        let commands = self.plan_writes(&entries)?;

        for command in commands {
            match command {
                InputHandoffWriteCommand::WriteInputs { shard_id, entries } => {
                    let runtime = self
                        .runtimes
                        .iter_mut()
                        .find(|runtime| runtime.shard_id() == shard_id)
                        .ok_or(MatchingRuntimeDriverError::DriverUnavailable(shard_id))?;
                    runtime
                        .enqueue_inputs(entries)
                        .map_err(MatchingRuntimeDriverError::from)?;
                }
            }
        }

        Ok(written_count)
    }

    fn can_write_inputs(
        &self,
        entries: &[JournalInputEntry],
    ) -> Result<(), MatchingRuntimeDriverError> {
        self.validate_enqueue_inputs(entries).map(|_| ())
    }
}

impl MatchingRuntimeDriver for ManualMatchingRuntimeDriver {
    fn shard_count(&self) -> usize {
        self.runtimes.len()
    }

    fn shard_ids(&self) -> Vec<RuntimeShardId> {
        self.runtimes.iter().map(ShardRuntime::shard_id).collect()
    }

    fn symbols_for_shard(&self, shard_id: RuntimeShardId) -> Option<&[Symbol]> {
        self.runtimes
            .iter()
            .find(|runtime| runtime.shard_id() == shard_id)
            .map(ShardRuntime::symbols)
    }

    fn shard_statuses(
        &self,
    ) -> Result<Vec<MatchingRuntimeShardStatus>, MatchingRuntimeDriverError> {
        let mut shard_statuses = Vec::new();

        for runtime in &self.runtimes {
            let mut symbol_statuses = Vec::new();

            for symbol in runtime.symbols() {
                let pending_input_status =
                    runtime.pending_input_status(symbol).ok_or_else(|| {
                        MatchingRuntimeDriverError::ShardRuntime(ShardRuntimeError::MissingHandoff(
                            symbol.clone(),
                        ))
                    })?;
                let runtime_status = runtime.symbol_status(symbol).ok_or(
                    MatchingRuntimeDriverError::ShardRuntime(
                        ShardRuntimeError::ShardExecutionCore(
                            ShardExecutionCoreError::UnknownSymbol,
                        ),
                    ),
                )?;

                symbol_statuses.push(symbol_status_from_runtime_status(
                    symbol.clone(),
                    pending_input_status.len,
                    pending_input_status.capacity,
                    pending_input_status.full,
                    runtime_status,
                ));
            }

            shard_statuses.push(MatchingRuntimeShardStatus {
                shard_id: runtime.shard_id(),
                symbol_statuses,
            });
        }

        Ok(shard_statuses)
    }

    fn run_once_all(
        &mut self,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
        limits: ShardRuntimeRunOnceLimits,
    ) -> Result<MatchingRuntimeRunOnceReport, MatchingRuntimeDriverError> {
        let mut shard_reports = Vec::new();

        for runtime in &mut self.runtimes {
            let run_once_report: ShardRuntimeRunOnceReport = runtime
                .run_once(journal_client, output, limits)
                .map_err(MatchingRuntimeDriverError::from)?;
            shard_reports.push(MatchingRuntimeShardRunOnceReport {
                shard_id: runtime.shard_id(),
                run_once_report,
            });
        }

        Ok(MatchingRuntimeRunOnceReport { shard_reports })
    }

    fn run_limited_all(
        &mut self,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
        limits: ShardRuntimeRunOnceLimits,
        limit: ShardRuntimeRunLimit,
    ) -> Result<MatchingRuntimeRunReport, MatchingRuntimeDriverError> {
        let mut shard_reports = Vec::new();

        for runtime in &mut self.runtimes {
            let run_report: ShardRuntimeRunReport = runtime
                .run_limited(journal_client, output, limits, limit)
                .map_err(MatchingRuntimeDriverError::from)?;
            shard_reports.push(MatchingRuntimeShardRunReport {
                shard_id: runtime.shard_id(),
                run_report,
            });
        }

        Ok(MatchingRuntimeRunReport { shard_reports })
    }

    fn shutdown(
        &mut self,
    ) -> Result<MatchingRuntimeDriverShutdownReport, MatchingRuntimeDriverError> {
        Ok(MatchingRuntimeDriverShutdownReport {
            shard_ids: self.shard_ids(),
        })
    }
}

fn symbol_status_from_runtime_status(
    symbol: Symbol,
    pending_input_len: usize,
    pending_input_capacity: usize,
    pending_input_full: bool,
    runtime_status: SymbolRuntimeStatus,
) -> MatchingRuntimeSymbolStatus {
    MatchingRuntimeSymbolStatus {
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
