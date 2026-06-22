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
    ) -> Result<Vec<InputHandoffWriteCommand>, ShardRuntimeSetError>;
    fn write_input(&mut self, entry: JournalInputEntry) -> Result<(), ShardRuntimeSetError>;
    fn write_inputs(
        &mut self,
        entries: Vec<JournalInputEntry>,
    ) -> Result<usize, ShardRuntimeSetError>;
    fn can_write_inputs(&self, entries: &[JournalInputEntry]) -> Result<(), ShardRuntimeSetError>;
}

pub trait ShardRuntimeSet: InputHandoffWriter {
    fn shard_count(&self) -> usize;
    fn shard_ids(&self) -> Vec<RuntimeShardId>;
    fn symbols_for_shard(&self, shard_id: RuntimeShardId) -> Option<&[Symbol]>;
    fn shard_statuses(&self) -> Result<Vec<MatchingRuntimeShardStatus>, ShardRuntimeSetError>;
    fn run_once_all(
        &mut self,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
        limits: ShardRuntimeRunOnceLimits,
    ) -> Result<MatchingRuntimeRunOnceReport, ShardRuntimeSetError>;
    fn run_limited_all(
        &mut self,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
        limits: ShardRuntimeRunOnceLimits,
        limit: ShardRuntimeRunLimit,
    ) -> Result<MatchingRuntimeRunReport, ShardRuntimeSetError>;
    fn shutdown(&mut self) -> Result<ShardRuntimeSetShutdownReport, ShardRuntimeSetError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShardRuntimeSetError {
    ShardRuntime(ShardRuntimeError),
    ShardRuntimeUnavailable(RuntimeShardId),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InputHandoffWriteCommand {
    WriteInputs {
        shard_id: RuntimeShardId,
        entries: Vec<JournalInputEntry>,
    },
}

pub struct ThreadPerShardRuntimeSet {
    workers: Vec<ShardRuntimeWorker>,
}

struct ShardRuntimeWorker {
    runtime: ShardRuntime,
}

enum ShardRuntimeWorkerCommand {
    WriteInputs(Vec<JournalInputEntry>),
    RunOnce(ShardRuntimeRunOnceLimits),
    RunLimited {
        limits: ShardRuntimeRunOnceLimits,
        limit: ShardRuntimeRunLimit,
    },
    Status,
    Shutdown,
}

impl ShardRuntimeWorker {
    fn new(runtime: ShardRuntime) -> Self {
        Self { runtime }
    }

    fn shard_id(&self) -> RuntimeShardId {
        self.runtime.shard_id()
    }

    fn symbols(&self) -> &[Symbol] {
        self.runtime.symbols()
    }

    fn has_symbol(&self, symbol: &Symbol) -> bool {
        self.runtime.symbols().contains(symbol)
    }

    fn available_input_capacity(&self, symbol: &Symbol) -> Result<usize, ShardRuntimeSetError> {
        let pending_input_status = self.runtime.pending_input_status(symbol).ok_or_else(|| {
            ShardRuntimeSetError::ShardRuntime(ShardRuntimeError::MissingHandoff(symbol.clone()))
        })?;

        Ok(pending_input_status
            .capacity
            .saturating_sub(pending_input_status.len))
    }

    fn write_inputs(
        &mut self,
        entries: Vec<JournalInputEntry>,
    ) -> Result<usize, ShardRuntimeSetError> {
        self.runtime
            .enqueue_inputs(entries)
            .map_err(ShardRuntimeSetError::from)
    }

    fn symbol_statuses(&self) -> Result<Vec<MatchingRuntimeSymbolStatus>, ShardRuntimeSetError> {
        let mut symbol_statuses = Vec::new();

        for symbol in self.runtime.symbols() {
            let pending_input_status =
                self.runtime.pending_input_status(symbol).ok_or_else(|| {
                    ShardRuntimeSetError::ShardRuntime(ShardRuntimeError::MissingHandoff(
                        symbol.clone(),
                    ))
                })?;

            let runtime_status =
                self.runtime
                    .symbol_status(symbol)
                    .ok_or(ShardRuntimeSetError::ShardRuntime(
                        ShardRuntimeError::ShardExecutionCore(
                            ShardExecutionCoreError::UnknownSymbol,
                        ),
                    ))?;

            symbol_statuses.push(symbol_status_from_runtime_status(
                symbol.clone(),
                pending_input_status.len,
                pending_input_status.capacity,
                pending_input_status.full,
                runtime_status,
            ));
        }

        Ok(symbol_statuses)
    }

    fn run_once(
        &mut self,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
        limits: ShardRuntimeRunOnceLimits,
    ) -> Result<ShardRuntimeRunOnceReport, ShardRuntimeSetError> {
        self.runtime
            .run_once(journal_client, output, limits)
            .map_err(ShardRuntimeSetError::from)
    }

    fn run_limited(
        &mut self,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
        limits: ShardRuntimeRunOnceLimits,
        limit: ShardRuntimeRunLimit,
    ) -> Result<ShardRuntimeRunReport, ShardRuntimeSetError> {
        self.runtime
            .run_limited(journal_client, output, limits, limit)
            .map_err(ShardRuntimeSetError::from)
    }

    fn shutdown(&mut self) -> RuntimeShardId {
        self.shard_id()
    }
}

impl ThreadPerShardRuntimeSet {
    pub fn from_symbols_with_config(
        symbols: Vec<Symbol>,
        config: MatchingRuntimeConfig,
    ) -> Result<Self, RuntimeTopologyError> {
        let worker_runtimes = ShardRuntime::from_symbols_with_config(symbols, config)?;
        let workers = worker_runtimes
            .into_iter()
            .map(ShardRuntimeWorker::new)
            .collect();

        Ok(Self { workers })
    }

    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    pub fn worker_symbols_for_shard(&self, shard_id: RuntimeShardId) -> Option<&[Symbol]> {
        self.workers
            .iter()
            .find(|worker| worker.shard_id() == shard_id)
            .map(|worker| worker.symbols())
    }

    fn worker_index_for_symbol(&self, symbol: &Symbol) -> Option<usize> {
        self.workers
            .iter()
            .position(|worker| worker.has_symbol(symbol))
    }

    fn validate_enqueue_inputs(
        &self,
        entries: &[JournalInputEntry],
    ) -> Result<HashMap<Symbol, usize>, ShardRuntimeSetError> {
        let mut requested_by_symbol: HashMap<Symbol, usize> = HashMap::new();
        let mut owner_by_symbol: HashMap<Symbol, usize> = HashMap::new();

        for entry in entries {
            let symbol = entry.command.symbol().clone();
            let worker_index = self.worker_index_for_symbol(&symbol).ok_or_else(|| {
                ShardRuntimeSetError::ShardRuntime(ShardRuntimeError::UnregisteredHandoff(
                    symbol.clone(),
                ))
            })?;

            *requested_by_symbol.entry(symbol.clone()).or_insert(0) += 1;
            owner_by_symbol.insert(symbol, worker_index);
        }

        let mut requested_symbols: Vec<Symbol> = requested_by_symbol.keys().cloned().collect();
        requested_symbols.sort_by(|left, right| left.0.cmp(&right.0));

        for symbol in requested_symbols {
            let requested_count = requested_by_symbol
                .get(&symbol)
                .expect("requested symbol should have a requested count");
            let worker_index = owner_by_symbol
                .get(&symbol)
                .expect("requested symbol should have an owning worker");
            let available_capacity =
                self.workers[*worker_index].available_input_capacity(&symbol)?;

            if available_capacity < *requested_count {
                return Err(ShardRuntimeSetError::ShardRuntime(
                    ShardRuntimeError::InputHandoffFull(symbol),
                ));
            }
        }

        Ok(owner_by_symbol)
    }

    fn plan_worker_write_commands(
        &self,
        entries: &[JournalInputEntry],
    ) -> Result<Vec<(RuntimeShardId, ShardRuntimeWorkerCommand)>, ShardRuntimeSetError> {
        let write_commands = self.plan_writes(entries)?;

        Ok(write_commands
            .into_iter()
            .map(|command| match command {
                InputHandoffWriteCommand::WriteInputs { shard_id, entries } => {
                    (shard_id, ShardRuntimeWorkerCommand::WriteInputs(entries))
                }
            })
            .collect())
    }

    fn plan_worker_run_once_commands(
        &self,
        limits: ShardRuntimeRunOnceLimits,
    ) -> Vec<(RuntimeShardId, ShardRuntimeWorkerCommand)> {
        self.workers
            .iter()
            .map(|worker| {
                (
                    worker.shard_id(),
                    ShardRuntimeWorkerCommand::RunOnce(limits),
                )
            })
            .collect()
    }

    fn plan_worker_run_limited_commands(
        &self,
        limits: ShardRuntimeRunOnceLimits,
        limit: ShardRuntimeRunLimit,
    ) -> Vec<(RuntimeShardId, ShardRuntimeWorkerCommand)> {
        self.workers
            .iter()
            .map(|worker| {
                (
                    worker.shard_id(),
                    ShardRuntimeWorkerCommand::RunLimited { limits, limit },
                )
            })
            .collect()
    }

    fn plan_worker_status_commands(&self) -> Vec<(RuntimeShardId, ShardRuntimeWorkerCommand)> {
        self.workers
            .iter()
            .map(|worker| (worker.shard_id(), ShardRuntimeWorkerCommand::Status))
            .collect()
    }

    fn plan_worker_shutdown_commands(&self) -> Vec<(RuntimeShardId, ShardRuntimeWorkerCommand)> {
        self.workers
            .iter()
            .map(|worker| (worker.shard_id(), ShardRuntimeWorkerCommand::Shutdown))
            .collect()
    }

    fn worker_mut_for_shard(
        &mut self,
        shard_id: RuntimeShardId,
    ) -> Result<&mut ShardRuntimeWorker, ShardRuntimeSetError> {
        self.workers
            .iter_mut()
            .find(|worker| worker.shard_id() == shard_id)
            .ok_or(ShardRuntimeSetError::ShardRuntimeUnavailable(shard_id))
    }
}

impl InputHandoffWriter for ThreadPerShardRuntimeSet {
    fn plan_writes(
        &self,
        entries: &[JournalInputEntry],
    ) -> Result<Vec<InputHandoffWriteCommand>, ShardRuntimeSetError> {
        let owner_by_symbol = self.validate_enqueue_inputs(entries)?;
        let mut entries_by_worker: Vec<Vec<JournalInputEntry>> =
            (0..self.workers.len()).map(|_| Vec::new()).collect();

        for entry in entries {
            let symbol = entry.command.symbol().clone();
            let worker_index = owner_by_symbol
                .get(&symbol)
                .expect("entry symbol should have an owning worker after validation");
            entries_by_worker[*worker_index].push(entry.clone());
        }

        Ok(self
            .workers
            .iter()
            .zip(entries_by_worker)
            .filter(|(_, entries)| !entries.is_empty())
            .map(|(worker, entries)| InputHandoffWriteCommand::WriteInputs {
                shard_id: worker.shard_id(),
                entries,
            })
            .collect())
    }

    fn write_input(&mut self, entry: JournalInputEntry) -> Result<(), ShardRuntimeSetError> {
        self.write_inputs(vec![entry]).map(|_| ())
    }

    fn write_inputs(
        &mut self,
        entries: Vec<JournalInputEntry>,
    ) -> Result<usize, ShardRuntimeSetError> {
        let written_count = entries.len();
        let worker_commands = self.plan_worker_write_commands(&entries)?;

        for (shard_id, command) in worker_commands {
            match command {
                ShardRuntimeWorkerCommand::WriteInputs(entries) => {
                    self.worker_mut_for_shard(shard_id)?.write_inputs(entries)?;
                }
                _ => unreachable!("write_inputs should only plan write commands"),
            }
        }

        Ok(written_count)
    }

    fn can_write_inputs(&self, entries: &[JournalInputEntry]) -> Result<(), ShardRuntimeSetError> {
        self.validate_enqueue_inputs(entries).map(|_| ())
    }
}

impl ShardRuntimeSet for ThreadPerShardRuntimeSet {
    fn shard_count(&self) -> usize {
        self.workers.len()
    }

    fn shard_ids(&self) -> Vec<RuntimeShardId> {
        self.workers
            .iter()
            .map(ShardRuntimeWorker::shard_id)
            .collect()
    }

    fn symbols_for_shard(&self, shard_id: RuntimeShardId) -> Option<&[Symbol]> {
        self.worker_symbols_for_shard(shard_id)
    }

    fn shard_statuses(&self) -> Result<Vec<MatchingRuntimeShardStatus>, ShardRuntimeSetError> {
        let worker_commands = self.plan_worker_status_commands();
        let mut shard_statuses = Vec::new();

        for (shard_id, command) in worker_commands {
            match command {
                ShardRuntimeWorkerCommand::Status => {
                    let worker = self
                        .workers
                        .iter()
                        .find(|worker| worker.shard_id() == shard_id)
                        .ok_or(ShardRuntimeSetError::ShardRuntimeUnavailable(shard_id))?;

                    shard_statuses.push(MatchingRuntimeShardStatus {
                        shard_id,
                        symbol_statuses: worker.symbol_statuses()?,
                    });
                }
                _ => unreachable!("shard_statuses should only plan status commands"),
            }
        }

        Ok(shard_statuses)
    }

    fn run_once_all(
        &mut self,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
        limits: ShardRuntimeRunOnceLimits,
    ) -> Result<MatchingRuntimeRunOnceReport, ShardRuntimeSetError> {
        let worker_commands = self.plan_worker_run_once_commands(limits);

        let mut shard_reports = Vec::new();

        for (shard_id, command) in worker_commands {
            match command {
                ShardRuntimeWorkerCommand::RunOnce(planned_limits) => {
                    let run_once_report = self.worker_mut_for_shard(shard_id)?.run_once(
                        journal_client,
                        output,
                        planned_limits,
                    )?;

                    shard_reports.push(MatchingRuntimeShardRunOnceReport {
                        shard_id,
                        run_once_report,
                    });
                }
                _ => unreachable!("run_once_all should only plan run-once commands"),
            }
        }

        Ok(MatchingRuntimeRunOnceReport { shard_reports })
    }

    fn run_limited_all(
        &mut self,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
        limits: ShardRuntimeRunOnceLimits,
        limit: ShardRuntimeRunLimit,
    ) -> Result<MatchingRuntimeRunReport, ShardRuntimeSetError> {
        let worker_commands = self.plan_worker_run_limited_commands(limits, limit);

        let mut shard_reports = Vec::new();

        for (shard_id, command) in worker_commands {
            match command {
                ShardRuntimeWorkerCommand::RunLimited {
                    limits: planned_limits,
                    limit: planned_limit,
                } => {
                    let run_report = self.worker_mut_for_shard(shard_id)?.run_limited(
                        journal_client,
                        output,
                        planned_limits,
                        planned_limit,
                    )?;

                    shard_reports.push(MatchingRuntimeShardRunReport {
                        shard_id,
                        run_report,
                    });
                }
                _ => unreachable!("run_limited_all should only plan run-limited commands"),
            }
        }

        Ok(MatchingRuntimeRunReport { shard_reports })
    }

    fn shutdown(&mut self) -> Result<ShardRuntimeSetShutdownReport, ShardRuntimeSetError> {
        let worker_commands = self.plan_worker_shutdown_commands();
        let mut shard_ids = Vec::new();

        for (shard_id, command) in worker_commands {
            match command {
                ShardRuntimeWorkerCommand::Shutdown => {
                    let shutdown_shard_id = self.worker_mut_for_shard(shard_id)?.shutdown();
                    shard_ids.push(shutdown_shard_id);
                }
                _ => unreachable!("shutdown should only plan shutdown commands"),
            }
        }

        Ok(ShardRuntimeSetShutdownReport { shard_ids })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardRuntimeSetShutdownReport {
    pub shard_ids: Vec<RuntimeShardId>,
}

impl From<ShardRuntimeError> for ShardRuntimeSetError {
    fn from(error: ShardRuntimeError) -> Self {
        Self::ShardRuntime(error)
    }
}

pub struct InlineShardRuntimeSet {
    runtimes: Vec<ShardRuntime>,
}

impl InlineShardRuntimeSet {
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
    ) -> Result<HashMap<Symbol, usize>, ShardRuntimeSetError> {
        let mut requested_by_symbol: HashMap<Symbol, usize> = HashMap::new();
        let mut owner_by_symbol: HashMap<Symbol, usize> = HashMap::new();

        for entry in entries {
            let symbol = entry.command.symbol().clone();
            let runtime_index = self.runtime_index_for_symbol(&symbol).ok_or_else(|| {
                ShardRuntimeSetError::ShardRuntime(ShardRuntimeError::UnregisteredHandoff(
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
                    ShardRuntimeSetError::ShardRuntime(ShardRuntimeError::MissingHandoff(
                        symbol.clone(),
                    ))
                })?;
            let available_capacity = pending_input_status
                .capacity
                .saturating_sub(pending_input_status.len);

            if available_capacity < *requested_count {
                return Err(ShardRuntimeSetError::ShardRuntime(
                    ShardRuntimeError::InputHandoffFull(symbol),
                ));
            }
        }

        Ok(owner_by_symbol)
    }
}

impl InputHandoffWriter for InlineShardRuntimeSet {
    fn plan_writes(
        &self,
        entries: &[JournalInputEntry],
    ) -> Result<Vec<InputHandoffWriteCommand>, ShardRuntimeSetError> {
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

    fn write_input(&mut self, entry: JournalInputEntry) -> Result<(), ShardRuntimeSetError> {
        let symbol = entry.command.symbol().clone();
        let runtime_index =
            self.runtime_index_for_symbol(&symbol)
                .ok_or(ShardRuntimeSetError::ShardRuntime(
                    ShardRuntimeError::UnregisteredHandoff(symbol),
                ))?;

        self.runtimes[runtime_index]
            .enqueue_input(entry)
            .map_err(ShardRuntimeSetError::from)
    }

    fn write_inputs(
        &mut self,
        entries: Vec<JournalInputEntry>,
    ) -> Result<usize, ShardRuntimeSetError> {
        let written_count = entries.len();
        let commands = self.plan_writes(&entries)?;

        for command in commands {
            match command {
                InputHandoffWriteCommand::WriteInputs { shard_id, entries } => {
                    let runtime = self
                        .runtimes
                        .iter_mut()
                        .find(|runtime| runtime.shard_id() == shard_id)
                        .ok_or(ShardRuntimeSetError::ShardRuntimeUnavailable(shard_id))?;
                    runtime
                        .enqueue_inputs(entries)
                        .map_err(ShardRuntimeSetError::from)?;
                }
            }
        }

        Ok(written_count)
    }

    fn can_write_inputs(&self, entries: &[JournalInputEntry]) -> Result<(), ShardRuntimeSetError> {
        self.validate_enqueue_inputs(entries).map(|_| ())
    }
}

impl ShardRuntimeSet for InlineShardRuntimeSet {
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

    fn shard_statuses(&self) -> Result<Vec<MatchingRuntimeShardStatus>, ShardRuntimeSetError> {
        let mut shard_statuses = Vec::new();

        for runtime in &self.runtimes {
            let mut symbol_statuses = Vec::new();

            for symbol in runtime.symbols() {
                let pending_input_status =
                    runtime.pending_input_status(symbol).ok_or_else(|| {
                        ShardRuntimeSetError::ShardRuntime(ShardRuntimeError::MissingHandoff(
                            symbol.clone(),
                        ))
                    })?;
                let runtime_status =
                    runtime
                        .symbol_status(symbol)
                        .ok_or(ShardRuntimeSetError::ShardRuntime(
                            ShardRuntimeError::ShardExecutionCore(
                                ShardExecutionCoreError::UnknownSymbol,
                            ),
                        ))?;

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
    ) -> Result<MatchingRuntimeRunOnceReport, ShardRuntimeSetError> {
        let mut shard_reports = Vec::new();

        for runtime in &mut self.runtimes {
            let run_once_report: ShardRuntimeRunOnceReport = runtime
                .run_once(journal_client, output, limits)
                .map_err(ShardRuntimeSetError::from)?;
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
    ) -> Result<MatchingRuntimeRunReport, ShardRuntimeSetError> {
        let mut shard_reports = Vec::new();

        for runtime in &mut self.runtimes {
            let run_report: ShardRuntimeRunReport = runtime
                .run_limited(journal_client, output, limits, limit)
                .map_err(ShardRuntimeSetError::from)?;
            shard_reports.push(MatchingRuntimeShardRunReport {
                shard_id: runtime.shard_id(),
                run_report,
            });
        }

        Ok(MatchingRuntimeRunReport { shard_reports })
    }

    fn shutdown(&mut self) -> Result<ShardRuntimeSetShutdownReport, ShardRuntimeSetError> {
        Ok(ShardRuntimeSetShutdownReport {
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
