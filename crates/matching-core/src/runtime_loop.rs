use crate::bounded_handoff::BoundedHandoff;
use crate::journal_adapter::{JournalInputEntry, JournalOutputAppender};
use crate::output_commit_boundary::{
    OutputBatchIdentity, OutputBatchQueryStatus, OutputCommitBlockDecision, OutputCommitOutcome,
    OutputJournalClient,
};
use crate::runtime_config::MatchingRuntimeConfig;
use crate::runtime_manager::{
    RuntimeManager, RuntimeManagerError, RuntimeManagerRetryAwareStepReport, SymbolRuntimeStatus,
};
use crate::snapshot_restore::SymbolRuntimeSnapshot;
use crate::snapshot_store::{SnapshotRecord, SnapshotStore, SnapshotStoreError};
use crate::types::{Checksum, JournalSeq, Symbol};
use std::collections::HashMap;

pub struct RuntimeLoop {
    manager: RuntimeManager,
    handoffs: HashMap<Symbol, BoundedHandoff>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeLoopTickLimits {
    pub max_input_entries_per_symbol: usize,
    pub max_output_requests_per_symbol: usize,
}

impl RuntimeLoopTickLimits {
    pub fn from_config(config: &MatchingRuntimeConfig) -> Self {
        Self {
            max_input_entries_per_symbol: config.execution_loop.max_input_entries_per_step,
            max_output_requests_per_symbol: config.output_commit.max_output_requests_per_step,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeLoopError {
    MissingHandoff(Symbol),
    UnregisteredHandoff(Symbol),
    InputHandoffFull(Symbol),
    RuntimeManager(RuntimeManagerError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeLoopTickReport {
    pub symbol_reports: Vec<RuntimeLoopSymbolTickReport>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeLoopRunBudget {
    pub max_ticks: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeLoopRunStopReason {
    Idle,
    Blocked,
    TickBudgetExhausted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeLoopRunReport {
    pub tick_reports: Vec<RuntimeLoopTickReport>,
    pub stop_reason: RuntimeLoopRunStopReason,
    pub made_progress: bool,
    pub has_work_remaining: bool,
    pub has_blocked_symbols: bool,
    pub work_status_after_run: Vec<RuntimeLoopSymbolWorkStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeLoopSymbolWorkStatus {
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
struct RuntimeLoopWorkState {
    symbol_statuses: Vec<RuntimeLoopSymbolWorkStatus>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeLoopInputStatus {
    pub len: usize,
    pub capacity: usize,
    pub full: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeLoopSymbolTickReport {
    pub symbol: Symbol,
    pub input_processed_count: usize,
    pub safe_point_advanced_count: usize,
    pub output_batch_identity: Option<OutputBatchIdentity>,
    pub output_batch_query_status: Option<OutputBatchQueryStatus>,
    pub blocking_seq: Option<JournalSeq>,
    pub blocking_outcome: Option<OutputCommitOutcome>,
    pub block_decision: Option<OutputCommitBlockDecision>,
    pub runtime_status_after_tick: SymbolRuntimeStatus,
    pub pending_input_len_after_tick: usize,
    pub pending_input_capacity: usize,
    pub pending_input_full: bool,
}

impl RuntimeLoop {
    pub fn new(manager: RuntimeManager, handoffs: HashMap<Symbol, BoundedHandoff>) -> Self {
        Self { manager, handoffs }
    }

    pub fn new_for_symbols(
        symbols: Vec<Symbol>,
        pending_input_capacity: usize,
        pending_output_capacity: usize,
    ) -> Self {
        let mut manager = RuntimeManager::new_with_pending_output_capacity(pending_output_capacity);
        let mut handoffs = HashMap::new();

        for symbol in symbols {
            manager.add_symbol(symbol.clone());
            handoffs.insert(symbol, BoundedHandoff::new(pending_input_capacity));
        }

        Self { manager, handoffs }
    }

    pub fn new_for_symbols_with_config(
        symbols: Vec<Symbol>,
        config: MatchingRuntimeConfig,
    ) -> Self {
        let mut manager = RuntimeManager::new_with_config(config.clone());
        let mut handoffs = HashMap::new();

        for symbol in symbols {
            manager.add_symbol(symbol.clone());
            handoffs.insert(symbol, BoundedHandoff::new(config.handoff.capacity));
        }

        Self { manager, handoffs }
    }

    pub fn new_from_symbol_snapshots(
        snapshots: Vec<SymbolRuntimeSnapshot>,
        pending_input_capacity: usize,
        pending_output_capacity: usize,
    ) -> Self {
        let mut manager = RuntimeManager::new_with_pending_output_capacity(pending_output_capacity);
        let mut handoffs = HashMap::new();

        for snapshot in snapshots {
            let symbol = snapshot.order_book_snapshot.symbol.clone();

            manager.restore_symbol_from_snapshot(snapshot);
            handoffs.insert(symbol, BoundedHandoff::new(pending_input_capacity));
        }

        Self { manager, handoffs }
    }

    pub fn new_from_symbol_snapshots_with_config(
        snapshots: Vec<SymbolRuntimeSnapshot>,
        config: MatchingRuntimeConfig,
    ) -> Self {
        let mut manager = RuntimeManager::new_with_config(config.clone());
        let mut handoffs = HashMap::new();

        for snapshot in snapshots {
            let symbol = snapshot.order_book_snapshot.symbol.clone();

            manager.restore_symbol_from_snapshot(snapshot);
            handoffs.insert(symbol, BoundedHandoff::new(config.handoff.capacity));
        }

        Self { manager, handoffs }
    }

    pub fn run_tick(
        &mut self,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
        limits: RuntimeLoopTickLimits,
    ) -> Result<RuntimeLoopTickReport, RuntimeLoopError> {
        self.validate_configuration()?;

        let mut symbols = self.manager.symbols();
        symbols.sort_by(|left, right| left.0.cmp(&right.0));

        let mut symbol_reports = Vec::new();

        for symbol in symbols {
            let handoff = self
                .handoffs
                .get_mut(&symbol)
                .ok_or_else(|| RuntimeLoopError::MissingHandoff(symbol.clone()))?;
            let report = self
                .manager
                .run_symbol_retry_aware_step(
                    &symbol,
                    handoff,
                    journal_client,
                    output,
                    limits.max_input_entries_per_symbol,
                    limits.max_output_requests_per_symbol,
                )
                .map_err(RuntimeLoopError::RuntimeManager)?;
            let pending_input_len_after_tick = handoff.len();
            let pending_input_capacity = handoff.capacity();
            let pending_input_full = handoff.is_full();
            let runtime_status_after_tick =
                self.manager
                    .symbol_status(&symbol)
                    .ok_or(RuntimeLoopError::RuntimeManager(
                        RuntimeManagerError::UnknownSymbol,
                    ))?;

            symbol_reports.push(RuntimeLoopSymbolTickReport::from_retry_aware_report(
                symbol,
                report,
                runtime_status_after_tick,
                pending_input_len_after_tick,
                pending_input_capacity,
                pending_input_full,
            ));
        }

        Ok(RuntimeLoopTickReport { symbol_reports })
    }

    pub fn run_budgeted(
        &mut self,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
        limits: RuntimeLoopTickLimits,
        budget: RuntimeLoopRunBudget,
    ) -> Result<RuntimeLoopRunReport, RuntimeLoopError> {
        let initial_state = self.current_work_state()?;

        if !initial_state.has_work_remaining() && !initial_state.has_blocked_symbols() {
            return Ok(RuntimeLoopRunReport {
                tick_reports: Vec::new(),
                stop_reason: RuntimeLoopRunStopReason::Idle,
                made_progress: false,
                has_work_remaining: false,
                has_blocked_symbols: false,
                work_status_after_run: initial_state.symbol_statuses,
            });
        }

        if budget.max_ticks == 0 {
            return Ok(RuntimeLoopRunReport {
                tick_reports: Vec::new(),
                stop_reason: RuntimeLoopRunStopReason::TickBudgetExhausted,
                made_progress: false,
                has_work_remaining: initial_state.has_work_remaining(),
                has_blocked_symbols: initial_state.has_blocked_symbols(),
                work_status_after_run: initial_state.symbol_statuses,
            });
        }

        let mut tick_reports = Vec::new();
        let mut made_progress = false;

        for _ in 0..budget.max_ticks {
            let tick_report = self.run_tick(journal_client, output, limits)?;
            let tick_made_progress = tick_report.made_progress();

            made_progress |= tick_made_progress;
            let has_work_remaining = tick_report.has_work_remaining();
            let has_blocked_symbols = tick_report.has_blocked_symbols();

            let stop_reason = if !has_work_remaining && !has_blocked_symbols {
                Some(RuntimeLoopRunStopReason::Idle)
            } else if has_blocked_symbols && !tick_made_progress {
                Some(RuntimeLoopRunStopReason::Blocked)
            } else {
                None
            };

            tick_reports.push(tick_report);

            if let Some(stop_reason) = stop_reason {
                let final_state = self.current_work_state()?;

                return Ok(RuntimeLoopRunReport {
                    tick_reports,
                    stop_reason,
                    made_progress,
                    has_work_remaining: final_state.has_work_remaining(),
                    has_blocked_symbols: final_state.has_blocked_symbols(),
                    work_status_after_run: final_state.symbol_statuses,
                });
            }
        }

        let final_state = self.current_work_state()?;

        Ok(RuntimeLoopRunReport {
            tick_reports,
            stop_reason: RuntimeLoopRunStopReason::TickBudgetExhausted,
            made_progress,
            has_work_remaining: final_state.has_work_remaining(),
            has_blocked_symbols: final_state.has_blocked_symbols(),
            work_status_after_run: final_state.symbol_statuses,
        })
    }

    pub fn last_input_seq(&self, symbol: &Symbol) -> Option<Option<JournalSeq>> {
        self.manager.last_input_seq(symbol)
    }

    pub fn checksum(&self, symbol: &Symbol) -> Option<Checksum> {
        self.manager.checksum(symbol)
    }

    pub fn save_symbol_snapshot(
        &self,
        symbol: &Symbol,
        snapshot_store: &mut dyn SnapshotStore,
    ) -> Result<Option<SnapshotRecord>, SnapshotStoreError> {
        let Some(snapshot) = self.manager.symbol_snapshot(symbol).flatten() else {
            return Ok(None);
        };

        snapshot_store.save_symbol_snapshot(&snapshot).map(Some)
    }

    pub fn symbol_status(&self, symbol: &Symbol) -> Option<SymbolRuntimeStatus> {
        self.manager.symbol_status(symbol)
    }

    pub fn pending_input_len(&self, symbol: &Symbol) -> Option<usize> {
        self.handoffs.get(symbol).map(BoundedHandoff::len)
    }

    pub fn pending_input_status(&self, symbol: &Symbol) -> Option<RuntimeLoopInputStatus> {
        self.handoffs
            .get(symbol)
            .map(|handoff| RuntimeLoopInputStatus {
                len: handoff.len(),
                capacity: handoff.capacity(),
                full: handoff.is_full(),
            })
    }

    pub fn validate_configuration(&self) -> Result<(), RuntimeLoopError> {
        let mut symbols = self.manager.symbols();
        symbols.sort_by(|left, right| left.0.cmp(&right.0));

        for symbol in &symbols {
            if !self.handoffs.contains_key(symbol) {
                return Err(RuntimeLoopError::MissingHandoff(symbol.clone()));
            }
        }

        let mut handoff_symbols: Vec<Symbol> = self.handoffs.keys().cloned().collect();
        handoff_symbols.sort_by(|left, right| left.0.cmp(&right.0));

        for symbol in handoff_symbols {
            if !symbols.contains(&symbol) {
                return Err(RuntimeLoopError::UnregisteredHandoff(symbol));
            }
        }

        Ok(())
    }

    fn current_work_state(&self) -> Result<RuntimeLoopWorkState, RuntimeLoopError> {
        self.validate_configuration()?;

        let mut symbol_statuses = Vec::new();
        let mut symbols = self.manager.symbols();
        symbols.sort_by(|left, right| left.0.cmp(&right.0));

        for symbol in symbols {
            let handoff = self
                .handoffs
                .get(&symbol)
                .ok_or_else(|| RuntimeLoopError::MissingHandoff(symbol.clone()))?;
            let runtime_status =
                self.manager
                    .symbol_status(&symbol)
                    .ok_or(RuntimeLoopError::RuntimeManager(
                        RuntimeManagerError::UnknownSymbol,
                    ))?;

            symbol_statuses.push(RuntimeLoopSymbolWorkStatus {
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

        Ok(RuntimeLoopWorkState { symbol_statuses })
    }

    pub fn enqueue_input(&mut self, entry: JournalInputEntry) -> Result<(), RuntimeLoopError> {
        let symbol = entry.command.symbol().clone();
        if self.manager.symbol_status(&symbol).is_none() {
            return Err(RuntimeLoopError::UnregisteredHandoff(symbol));
        }

        let handoff = self
            .handoffs
            .get_mut(&symbol)
            .ok_or_else(|| RuntimeLoopError::MissingHandoff(symbol.clone()))?;

        handoff
            .enqueue(entry)
            .map_err(|_| RuntimeLoopError::InputHandoffFull(symbol))
    }

    pub fn enqueue_inputs(
        &mut self,
        entries: Vec<JournalInputEntry>,
    ) -> Result<usize, RuntimeLoopError> {
        let mut requested_by_symbol: HashMap<Symbol, usize> = HashMap::new();

        for entry in &entries {
            let symbol = entry.command.symbol().clone();
            if self.manager.symbol_status(&symbol).is_none() {
                return Err(RuntimeLoopError::UnregisteredHandoff(symbol));
            }
            if !self.handoffs.contains_key(&symbol) {
                return Err(RuntimeLoopError::MissingHandoff(symbol));
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
                .ok_or_else(|| RuntimeLoopError::MissingHandoff(symbol.clone()))?;
            if handoff.available_capacity() < *requested_count {
                return Err(RuntimeLoopError::InputHandoffFull(symbol));
            }
        }

        let enqueued_count = entries.len();

        for entry in entries {
            let symbol = entry.command.symbol().clone();
            let handoff = self
                .handoffs
                .get_mut(&symbol)
                .ok_or_else(|| RuntimeLoopError::MissingHandoff(symbol.clone()))?;
            handoff
                .enqueue(entry)
                .map_err(|_| RuntimeLoopError::InputHandoffFull(symbol))?;
        }

        Ok(enqueued_count)
    }

    pub fn quarantine_symbol_output_commit_escalation(
        &mut self,
        symbol: &Symbol,
    ) -> Result<Option<OutputCommitBlockDecision>, RuntimeManagerError> {
        self.manager
            .quarantine_symbol_output_commit_escalation(symbol)
    }

    pub fn clear_symbol_output_commit_quarantine(
        &mut self,
        symbol: &Symbol,
    ) -> Result<Option<OutputCommitBlockDecision>, RuntimeManagerError> {
        self.manager.clear_symbol_output_commit_quarantine(symbol)
    }
}

impl RuntimeLoopTickReport {
    pub fn symbol_report(&self, symbol: &Symbol) -> Option<&RuntimeLoopSymbolTickReport> {
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
            report.pending_input_len_after_tick > 0
                || report.runtime_status_after_tick.pending_output_len > 0
                || report
                    .runtime_status_after_tick
                    .output_commit_blockage
                    .is_some()
        })
    }

    pub fn has_blocked_symbols(&self) -> bool {
        self.symbol_reports.iter().any(|report| {
            report.block_decision.is_some()
                || report
                    .runtime_status_after_tick
                    .output_commit_blockage
                    .is_some()
        })
    }

    pub fn is_idle(&self) -> bool {
        !self.made_progress() && !self.has_work_remaining() && !self.has_blocked_symbols()
    }
}

impl RuntimeLoopRunReport {
    pub fn tick_count(&self) -> usize {
        self.tick_reports.len()
    }

    pub fn last_tick_report(&self) -> Option<&RuntimeLoopTickReport> {
        self.tick_reports.last()
    }

    pub fn is_idle(&self) -> bool {
        self.stop_reason == RuntimeLoopRunStopReason::Idle
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

impl RuntimeLoopSymbolWorkStatus {
    pub fn has_work_remaining(&self) -> bool {
        self.pending_input_len > 0 || self.pending_output_len > 0 || self.output_commit_blocked
    }
}

impl RuntimeLoopWorkState {
    fn has_work_remaining(&self) -> bool {
        self.symbol_statuses
            .iter()
            .any(RuntimeLoopSymbolWorkStatus::has_work_remaining)
    }

    fn has_blocked_symbols(&self) -> bool {
        self.symbol_statuses
            .iter()
            .any(|status| status.output_commit_blocked)
    }
}

impl RuntimeLoopSymbolTickReport {
    fn from_retry_aware_report(
        symbol: Symbol,
        report: RuntimeManagerRetryAwareStepReport,
        runtime_status_after_tick: SymbolRuntimeStatus,
        pending_input_len_after_tick: usize,
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
            runtime_status_after_tick,
            pending_input_len_after_tick,
            pending_input_capacity,
            pending_input_full,
        }
    }
}
