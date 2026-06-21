use crate::bounded_handoff::BoundedHandoff;
use crate::journal_adapter::{JournalInputEntry, JournalOutputAppender};
use crate::output_commit_boundary::{
    OutputBatchIdentity, OutputBatchQueryStatus, OutputCommitBlockDecision, OutputCommitOutcome,
    OutputJournalClient,
};
use crate::runtime_config::{MatchingRuntimeConfig, RuntimeShardId, RuntimeTopologyConfig};
use crate::runtime_topology::{RuntimeTopology, RuntimeTopologyError};
use crate::shard_execution_core::{
    ShardExecutionCore, ShardExecutionCoreError, ShardExecutionCoreRetryAwareStepReport,
    SymbolRuntimeStatus,
};
use crate::snapshot_restore::SymbolRuntimeSnapshot;
use crate::snapshot_store::{SnapshotRecord, SnapshotStore, SnapshotStoreError};
use crate::types::{Checksum, JournalSeq, Symbol};
use std::collections::HashMap;

pub struct ShardRuntime {
    shard_id: RuntimeShardId,
    symbols: Vec<Symbol>,
    execution_core: ShardExecutionCore,
    handoffs: HashMap<Symbol, BoundedHandoff>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardRuntimeRunOnceLimits {
    pub max_input_entries_per_symbol: usize,
    pub max_output_requests_per_symbol: usize,
}

impl ShardRuntimeRunOnceLimits {
    pub fn from_config(config: &MatchingRuntimeConfig) -> Self {
        Self {
            max_input_entries_per_symbol: config.symbol_runtime.max_input_entries_per_step,
            max_output_requests_per_symbol: config.output_commit.max_output_requests_per_step,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShardRuntimeError {
    MissingHandoff(Symbol),
    UnregisteredHandoff(Symbol),
    InputHandoffFull(Symbol),
    ShardExecutionCore(ShardExecutionCoreError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardRuntimeRunOnceReport {
    pub symbol_reports: Vec<ShardRuntimeSymbolRunOnceReport>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardRuntimeRunLimit {
    pub max_cycles: usize,
}

impl ShardRuntimeRunLimit {
    pub fn from_config(config: &MatchingRuntimeConfig) -> Self {
        Self {
            max_cycles: config.execution.max_run_cycles_per_call,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardRuntimeRunStopReason {
    Idle,
    Blocked,
    RunLimitReached,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardRuntimeRunReport {
    pub cycle_reports: Vec<ShardRuntimeRunOnceReport>,
    pub stop_reason: ShardRuntimeRunStopReason,
    pub made_progress: bool,
    pub has_work_remaining: bool,
    pub has_blocked_symbols: bool,
    pub work_status_after_run: Vec<ShardRuntimeSymbolWorkStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardRuntimeSymbolWorkStatus {
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
struct ShardRuntimeWorkState {
    symbol_statuses: Vec<ShardRuntimeSymbolWorkStatus>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardRuntimeInputStatus {
    pub len: usize,
    pub capacity: usize,
    pub full: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardRuntimeSymbolRunOnceReport {
    pub symbol: Symbol,
    pub input_processed_count: usize,
    pub safe_point_advanced_count: usize,
    pub output_batch_identity: Option<OutputBatchIdentity>,
    pub output_batch_query_status: Option<OutputBatchQueryStatus>,
    pub blocking_seq: Option<JournalSeq>,
    pub blocking_outcome: Option<OutputCommitOutcome>,
    pub block_decision: Option<OutputCommitBlockDecision>,
    pub runtime_status_after_run: SymbolRuntimeStatus,
    pub pending_input_len_after_run: usize,
    pub pending_input_capacity: usize,
    pub pending_input_full: bool,
}

impl ShardRuntime {
    pub fn new(
        execution_core: ShardExecutionCore,
        handoffs: HashMap<Symbol, BoundedHandoff>,
    ) -> Self {
        let mut symbols = execution_core.symbols();
        symbols.sort_by(|left, right| left.0.cmp(&right.0));

        Self {
            shard_id: RuntimeShardId(0),
            symbols,
            execution_core,
            handoffs,
        }
    }

    pub fn new_for_symbols(
        symbols: Vec<Symbol>,
        pending_input_capacity: usize,
        pending_output_capacity: usize,
    ) -> Self {
        Self::new_for_shard_symbols(
            RuntimeShardId(0),
            symbols,
            pending_input_capacity,
            pending_output_capacity,
        )
    }

    fn new_for_shard_symbols(
        shard_id: RuntimeShardId,
        symbols: Vec<Symbol>,
        pending_input_capacity: usize,
        pending_output_capacity: usize,
    ) -> Self {
        let mut execution_core =
            ShardExecutionCore::new_with_pending_output_capacity(pending_output_capacity);
        let mut handoffs = HashMap::new();

        for symbol in &symbols {
            execution_core.add_symbol(symbol.clone());
            handoffs.insert(symbol.clone(), BoundedHandoff::new(pending_input_capacity));
        }

        Self {
            shard_id,
            symbols,
            execution_core,
            handoffs,
        }
    }

    pub fn new_for_symbols_with_config(
        symbols: Vec<Symbol>,
        config: MatchingRuntimeConfig,
    ) -> Self {
        Self::new_for_shard_symbols_with_config(RuntimeShardId(0), symbols, config)
    }

    fn new_for_shard_symbols_with_config(
        shard_id: RuntimeShardId,
        symbols: Vec<Symbol>,
        config: MatchingRuntimeConfig,
    ) -> Self {
        let mut execution_core = ShardExecutionCore::new_with_config(config.clone());
        let mut handoffs = HashMap::new();

        for symbol in &symbols {
            execution_core.add_symbol(symbol.clone());
            handoffs.insert(symbol.clone(), BoundedHandoff::new(config.handoff.capacity));
        }

        Self {
            shard_id,
            symbols,
            execution_core,
            handoffs,
        }
    }

    pub fn from_symbols_with_config(
        symbols: Vec<Symbol>,
        config: MatchingRuntimeConfig,
    ) -> Result<Vec<Self>, RuntimeTopologyError> {
        let topology = RuntimeTopology::resolve(&symbols, &config.topology)?;

        Ok(topology
            .shards()
            .iter()
            .map(|shard| {
                let symbols = shard.symbols.clone();
                let mut shard_config = config.clone();
                shard_config.topology = RuntimeTopologyConfig::default();

                Self::new_for_shard_symbols_with_config(shard.id, symbols, shard_config)
            })
            .collect())
    }

    pub fn new_from_symbol_snapshots(
        snapshots: Vec<SymbolRuntimeSnapshot>,
        pending_input_capacity: usize,
        pending_output_capacity: usize,
    ) -> Self {
        let mut execution_core =
            ShardExecutionCore::new_with_pending_output_capacity(pending_output_capacity);
        let mut handoffs = HashMap::new();
        let mut symbols = Vec::new();

        for snapshot in snapshots {
            let symbol = snapshot.order_book_snapshot.symbol.clone();

            execution_core.restore_symbol_from_snapshot(snapshot);
            handoffs.insert(symbol.clone(), BoundedHandoff::new(pending_input_capacity));
            symbols.push(symbol);
        }

        Self {
            shard_id: RuntimeShardId(0),
            symbols,
            execution_core,
            handoffs,
        }
    }

    pub fn new_from_symbol_snapshots_with_config(
        snapshots: Vec<SymbolRuntimeSnapshot>,
        config: MatchingRuntimeConfig,
    ) -> Self {
        let mut execution_core = ShardExecutionCore::new_with_config(config.clone());
        let mut handoffs = HashMap::new();
        let mut symbols = Vec::new();

        for snapshot in snapshots {
            let symbol = snapshot.order_book_snapshot.symbol.clone();

            execution_core.restore_symbol_from_snapshot(snapshot);
            handoffs.insert(symbol.clone(), BoundedHandoff::new(config.handoff.capacity));
            symbols.push(symbol);
        }

        Self {
            shard_id: RuntimeShardId(0),
            symbols,
            execution_core,
            handoffs,
        }
    }

    pub fn shard_id(&self) -> RuntimeShardId {
        self.shard_id
    }

    pub fn symbols(&self) -> &[Symbol] {
        &self.symbols
    }

    pub fn run_once(
        &mut self,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
        limits: ShardRuntimeRunOnceLimits,
    ) -> Result<ShardRuntimeRunOnceReport, ShardRuntimeError> {
        self.validate_configuration()?;

        let mut symbols = self.execution_core.symbols();
        symbols.sort_by(|left, right| left.0.cmp(&right.0));

        let mut symbol_reports = Vec::new();

        for symbol in symbols {
            let handoff = self
                .handoffs
                .get_mut(&symbol)
                .ok_or_else(|| ShardRuntimeError::MissingHandoff(symbol.clone()))?;
            let report = self
                .execution_core
                .run_symbol_retry_aware_step(
                    &symbol,
                    handoff,
                    journal_client,
                    output,
                    limits.max_input_entries_per_symbol,
                    limits.max_output_requests_per_symbol,
                )
                .map_err(ShardRuntimeError::ShardExecutionCore)?;
            let pending_input_len_after_run = handoff.len();
            let pending_input_capacity = handoff.capacity();
            let pending_input_full = handoff.is_full();
            let runtime_status_after_run = self.execution_core.symbol_status(&symbol).ok_or(
                ShardRuntimeError::ShardExecutionCore(ShardExecutionCoreError::UnknownSymbol),
            )?;

            symbol_reports.push(ShardRuntimeSymbolRunOnceReport::from_retry_aware_report(
                symbol,
                report,
                runtime_status_after_run,
                pending_input_len_after_run,
                pending_input_capacity,
                pending_input_full,
            ));
        }

        Ok(ShardRuntimeRunOnceReport { symbol_reports })
    }

    pub fn run_limited(
        &mut self,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
        limits: ShardRuntimeRunOnceLimits,
        limit: ShardRuntimeRunLimit,
    ) -> Result<ShardRuntimeRunReport, ShardRuntimeError> {
        let initial_state = self.current_work_state()?;

        if !initial_state.has_work_remaining() && !initial_state.has_blocked_symbols() {
            return Ok(ShardRuntimeRunReport {
                cycle_reports: Vec::new(),
                stop_reason: ShardRuntimeRunStopReason::Idle,
                made_progress: false,
                has_work_remaining: false,
                has_blocked_symbols: false,
                work_status_after_run: initial_state.symbol_statuses,
            });
        }

        if limit.max_cycles == 0 {
            return Ok(ShardRuntimeRunReport {
                cycle_reports: Vec::new(),
                stop_reason: ShardRuntimeRunStopReason::RunLimitReached,
                made_progress: false,
                has_work_remaining: initial_state.has_work_remaining(),
                has_blocked_symbols: initial_state.has_blocked_symbols(),
                work_status_after_run: initial_state.symbol_statuses,
            });
        }

        let mut cycle_reports = Vec::new();
        let mut made_progress = false;

        for _ in 0..limit.max_cycles {
            let run_once_report = self.run_once(journal_client, output, limits)?;
            let cycle_made_progress = run_once_report.made_progress();

            made_progress |= cycle_made_progress;
            let has_work_remaining = run_once_report.has_work_remaining();
            let has_blocked_symbols = run_once_report.has_blocked_symbols();

            let stop_reason = if !has_work_remaining && !has_blocked_symbols {
                Some(ShardRuntimeRunStopReason::Idle)
            } else if has_blocked_symbols && !cycle_made_progress {
                Some(ShardRuntimeRunStopReason::Blocked)
            } else {
                None
            };

            cycle_reports.push(run_once_report);

            if let Some(stop_reason) = stop_reason {
                let final_state = self.current_work_state()?;

                return Ok(ShardRuntimeRunReport {
                    cycle_reports,
                    stop_reason,
                    made_progress,
                    has_work_remaining: final_state.has_work_remaining(),
                    has_blocked_symbols: final_state.has_blocked_symbols(),
                    work_status_after_run: final_state.symbol_statuses,
                });
            }
        }

        let final_state = self.current_work_state()?;

        Ok(ShardRuntimeRunReport {
            cycle_reports,
            stop_reason: ShardRuntimeRunStopReason::RunLimitReached,
            made_progress,
            has_work_remaining: final_state.has_work_remaining(),
            has_blocked_symbols: final_state.has_blocked_symbols(),
            work_status_after_run: final_state.symbol_statuses,
        })
    }

    pub fn last_input_seq(&self, symbol: &Symbol) -> Option<Option<JournalSeq>> {
        self.execution_core.last_input_seq(symbol)
    }

    pub fn checksum(&self, symbol: &Symbol) -> Option<Checksum> {
        self.execution_core.checksum(symbol)
    }

    pub fn save_symbol_snapshot(
        &self,
        symbol: &Symbol,
        snapshot_store: &mut dyn SnapshotStore,
    ) -> Result<Option<SnapshotRecord>, SnapshotStoreError> {
        let Some(snapshot) = self.execution_core.symbol_snapshot(symbol).flatten() else {
            return Ok(None);
        };

        snapshot_store.save_symbol_snapshot(&snapshot).map(Some)
    }

    pub fn symbol_status(&self, symbol: &Symbol) -> Option<SymbolRuntimeStatus> {
        self.execution_core.symbol_status(symbol)
    }

    pub fn pending_input_len(&self, symbol: &Symbol) -> Option<usize> {
        self.handoffs.get(symbol).map(BoundedHandoff::len)
    }

    pub fn pending_input_status(&self, symbol: &Symbol) -> Option<ShardRuntimeInputStatus> {
        self.handoffs
            .get(symbol)
            .map(|handoff| ShardRuntimeInputStatus {
                len: handoff.len(),
                capacity: handoff.capacity(),
                full: handoff.is_full(),
            })
    }

    pub fn validate_configuration(&self) -> Result<(), ShardRuntimeError> {
        let mut symbols = self.execution_core.symbols();
        symbols.sort_by(|left, right| left.0.cmp(&right.0));

        for symbol in &symbols {
            if !self.handoffs.contains_key(symbol) {
                return Err(ShardRuntimeError::MissingHandoff(symbol.clone()));
            }
        }

        let mut handoff_symbols: Vec<Symbol> = self.handoffs.keys().cloned().collect();
        handoff_symbols.sort_by(|left, right| left.0.cmp(&right.0));

        for symbol in handoff_symbols {
            if !symbols.contains(&symbol) {
                return Err(ShardRuntimeError::UnregisteredHandoff(symbol));
            }
        }

        Ok(())
    }

    fn current_work_state(&self) -> Result<ShardRuntimeWorkState, ShardRuntimeError> {
        self.validate_configuration()?;

        let mut symbol_statuses = Vec::new();
        let mut symbols = self.execution_core.symbols();
        symbols.sort_by(|left, right| left.0.cmp(&right.0));

        for symbol in symbols {
            let handoff = self
                .handoffs
                .get(&symbol)
                .ok_or_else(|| ShardRuntimeError::MissingHandoff(symbol.clone()))?;
            let runtime_status = self.execution_core.symbol_status(&symbol).ok_or(
                ShardRuntimeError::ShardExecutionCore(ShardExecutionCoreError::UnknownSymbol),
            )?;

            symbol_statuses.push(ShardRuntimeSymbolWorkStatus {
                symbol,
                pending_input_len: handoff.len(),
                pending_input_capacity: handoff.capacity(),
                pending_input_full: handoff.is_full(),
                pending_output_len: runtime_status.pending_output_len,
                pending_output_capacity: runtime_status.pending_output_capacity,
                pending_output_full: runtime_status.pending_output_full,
                output_commit_blocked: runtime_status.output_commit_blockage.is_some(),
            });
        }

        Ok(ShardRuntimeWorkState { symbol_statuses })
    }

    pub fn enqueue_input(&mut self, entry: JournalInputEntry) -> Result<(), ShardRuntimeError> {
        let symbol = entry.command.symbol().clone();
        if self.execution_core.symbol_status(&symbol).is_none() {
            return Err(ShardRuntimeError::UnregisteredHandoff(symbol));
        }

        let handoff = self
            .handoffs
            .get_mut(&symbol)
            .ok_or_else(|| ShardRuntimeError::MissingHandoff(symbol.clone()))?;

        handoff
            .enqueue(entry)
            .map_err(|_| ShardRuntimeError::InputHandoffFull(symbol))
    }

    pub fn enqueue_inputs(
        &mut self,
        entries: Vec<JournalInputEntry>,
    ) -> Result<usize, ShardRuntimeError> {
        let mut requested_by_symbol: HashMap<Symbol, usize> = HashMap::new();

        for entry in &entries {
            let symbol = entry.command.symbol().clone();
            if self.execution_core.symbol_status(&symbol).is_none() {
                return Err(ShardRuntimeError::UnregisteredHandoff(symbol));
            }
            if !self.handoffs.contains_key(&symbol) {
                return Err(ShardRuntimeError::MissingHandoff(symbol));
            }

            *requested_by_symbol.entry(symbol).or_insert(0) += 1;
        }

        let mut requested_symbols: Vec<Symbol> = requested_by_symbol.keys().cloned().collect();
        requested_symbols.sort_by(|left, right| left.0.cmp(&right.0));

        for symbol in requested_symbols {
            let requested_count = requested_by_symbol
                .get(&symbol)
                .expect("requested symbol should have a requested count");
            let handoff = self
                .handoffs
                .get(&symbol)
                .ok_or_else(|| ShardRuntimeError::MissingHandoff(symbol.clone()))?;
            if handoff.available_capacity() < *requested_count {
                return Err(ShardRuntimeError::InputHandoffFull(symbol));
            }
        }

        let enqueued_count = entries.len();

        for entry in entries {
            let symbol = entry.command.symbol().clone();
            let handoff = self
                .handoffs
                .get_mut(&symbol)
                .ok_or_else(|| ShardRuntimeError::MissingHandoff(symbol.clone()))?;
            handoff
                .enqueue(entry)
                .map_err(|_| ShardRuntimeError::InputHandoffFull(symbol))?;
        }

        Ok(enqueued_count)
    }

    pub fn quarantine_symbol_output_commit_escalation(
        &mut self,
        symbol: &Symbol,
    ) -> Result<Option<OutputCommitBlockDecision>, ShardExecutionCoreError> {
        self.execution_core
            .quarantine_symbol_output_commit_escalation(symbol)
    }

    pub fn clear_symbol_output_commit_quarantine(
        &mut self,
        symbol: &Symbol,
    ) -> Result<Option<OutputCommitBlockDecision>, ShardExecutionCoreError> {
        self.execution_core
            .clear_symbol_output_commit_quarantine(symbol)
    }
}

impl ShardRuntimeRunOnceReport {
    pub fn symbol_report(&self, symbol: &Symbol) -> Option<&ShardRuntimeSymbolRunOnceReport> {
        self.symbol_reports
            .iter()
            .find(|report| report.symbol == *symbol)
    }

    pub fn made_progress(&self) -> bool {
        self.symbol_reports
            .iter()
            .any(|report| report.input_processed_count > 0 || report.safe_point_advanced_count > 0)
    }

    pub fn has_work_remaining(&self) -> bool {
        self.symbol_reports.iter().any(|report| {
            report.pending_input_len_after_run > 0
                || report.runtime_status_after_run.pending_output_len > 0
                || report
                    .runtime_status_after_run
                    .output_commit_blockage
                    .is_some()
        })
    }

    pub fn has_blocked_symbols(&self) -> bool {
        self.symbol_reports.iter().any(|report| {
            report.block_decision.is_some()
                || report
                    .runtime_status_after_run
                    .output_commit_blockage
                    .is_some()
        })
    }

    pub fn is_idle(&self) -> bool {
        !self.made_progress() && !self.has_work_remaining() && !self.has_blocked_symbols()
    }
}

impl ShardRuntimeRunReport {
    pub fn cycle_count(&self) -> usize {
        self.cycle_reports.len()
    }

    pub fn last_run_once_report(&self) -> Option<&ShardRuntimeRunOnceReport> {
        self.cycle_reports.last()
    }

    pub fn is_idle(&self) -> bool {
        self.stop_reason == ShardRuntimeRunStopReason::Idle
            && !self.has_work_remaining
            && !self.has_blocked_symbols
    }

    pub fn symbols_with_remaining_work(&self) -> Vec<Symbol> {
        self.work_status_after_run
            .iter()
            .filter(|status| status.has_work_remaining())
            .map(|status| status.symbol.clone())
            .collect()
    }

    pub fn blocked_symbols(&self) -> Vec<Symbol> {
        self.work_status_after_run
            .iter()
            .filter(|status| status.output_commit_blocked)
            .map(|status| status.symbol.clone())
            .collect()
    }
}

impl ShardRuntimeSymbolWorkStatus {
    pub fn has_work_remaining(&self) -> bool {
        self.pending_input_len > 0 || self.pending_output_len > 0 || self.output_commit_blocked
    }
}

impl ShardRuntimeWorkState {
    fn has_work_remaining(&self) -> bool {
        self.symbol_statuses
            .iter()
            .any(ShardRuntimeSymbolWorkStatus::has_work_remaining)
    }

    fn has_blocked_symbols(&self) -> bool {
        self.symbol_statuses
            .iter()
            .any(|status| status.output_commit_blocked)
    }
}

impl ShardRuntimeSymbolRunOnceReport {
    fn from_retry_aware_report(
        symbol: Symbol,
        report: ShardExecutionCoreRetryAwareStepReport,
        runtime_status_after_run: SymbolRuntimeStatus,
        pending_input_len_after_run: usize,
        pending_input_capacity: usize,
        pending_input_full: bool,
    ) -> Self {
        Self {
            symbol,
            input_processed_count: report.input_processed_count,
            safe_point_advanced_count: report.safe_point_advanced_count,
            output_batch_identity: report.output_batch_identity,
            output_batch_query_status: report.output_batch_query_status,
            blocking_seq: report.output_commit_report.blocking_seq,
            blocking_outcome: report.output_commit_report.blocking_outcome,
            block_decision: report.block_decision,
            runtime_status_after_run,
            pending_input_len_after_run,
            pending_input_capacity,
            pending_input_full,
        }
    }
}
