use crate::bounded_handoff::BoundedHandoff;
use crate::journal_adapter::{JournalInputEntry, JournalOutputAppender};
use crate::output_commit_boundary::{
    OutputBatchIdentity, OutputBatchQueryStatus, OutputCommitBlockDecision, OutputCommitOutcome,
    OutputJournalClient,
};
use crate::runtime_manager::{
    RuntimeManager, RuntimeManagerError, RuntimeManagerRetryAwareStepReport, SymbolRuntimeStatus,
};
use crate::types::{JournalSeq, Symbol};
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

    pub fn last_input_seq(&self, symbol: &Symbol) -> Option<Option<JournalSeq>> {
        self.manager.last_input_seq(symbol)
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
