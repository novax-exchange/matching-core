use crate::bounded_handoff::BoundedHandoff;
use crate::journal_adapter::{JournalInputEntry, JournalOutputAppender};
use crate::output_commit_boundary::{
    run_output_batch_commit_step_report_with_identity, OutputBatchCommitResult,
    OutputBatchCommitStepReport, OutputBatchIdentity, OutputBatchQueryStatus,
    OutputCommitBlockAction, OutputCommitBlockDecision, OutputCommitRetryTracker,
    OutputJournalClient, PendingOutputBuffer,
};
use crate::per_symbol_execution_loop::{
    advance_runtime_safe_point_from_output_commit,
    run_per_symbol_execution_loop_step_with_output_batch_commit,
    PerSymbolExecutionLoopOutputCommitStepError, PerSymbolExecutionLoopOutputCommitStepReport,
    SymbolRuntime,
};
use crate::runtime_config::MatchingRuntimeConfig;
use crate::snapshot_restore::SymbolRuntimeSnapshot;
use crate::types::{Checksum, JournalSeq, Symbol};
use std::collections::HashMap;

pub struct RuntimeManager {
    runtimes: HashMap<Symbol, SymbolRuntime>,
    pending_output_buffers: HashMap<Symbol, PendingOutputBuffer>,
    output_commit_retry_trackers: HashMap<Symbol, OutputCommitRetryTracker>,
    output_commit_escalations: HashMap<Symbol, OutputCommitBlockDecision>,
    output_commit_escalation_query_statuses: HashMap<Symbol, OutputBatchQueryStatus>,
    output_commit_quarantines: HashMap<Symbol, OutputCommitBlockDecision>,
    output_commit_quarantine_query_statuses: HashMap<Symbol, OutputBatchQueryStatus>,
    default_pending_output_capacity: usize,
    output_commit_max_unavailable_attempts: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeManagerError {
    UnknownSymbol,
    OutputAppendFailed,
    OutputCommitStepFailed(PerSymbolExecutionLoopOutputCommitStepError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputCommitBlockageKind {
    Escalation,
    Quarantine,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputCommitBlockageStatus {
    pub kind: OutputCommitBlockageKind,
    pub decision: OutputCommitBlockDecision,
    pub output_batch_query_status: Option<OutputBatchQueryStatus>,
    pub pending_output_len: usize,
    pub pending_output_capacity: usize,
    pub pending_output_full: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SymbolRuntimeStatus {
    pub symbol: Symbol,
    pub last_input_seq: Option<JournalSeq>,
    pub pending_output_len: usize,
    pub pending_output_capacity: usize,
    pub pending_output_full: bool,
    pub output_commit_escalation: Option<OutputCommitBlockDecision>,
    pub output_commit_quarantine: Option<OutputCommitBlockDecision>,
    pub output_commit_blockage: Option<OutputCommitBlockageStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeManagerOutputCommitStepReport {
    pub safe_point_advanced_count: usize,
    pub output_batch_identity: Option<OutputBatchIdentity>,
    pub output_batch_query_status: Option<OutputBatchQueryStatus>,
    pub output_commit_report: OutputBatchCommitStepReport,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeManagerRetryAwareStepReport {
    pub input_processed_count: usize,
    pub safe_point_advanced_count: usize,
    pub output_batch_identity: Option<OutputBatchIdentity>,
    pub output_batch_query_status: Option<OutputBatchQueryStatus>,
    pub output_commit_report: OutputBatchCommitStepReport,
    pub block_decision: Option<OutputCommitBlockDecision>,
}

impl RuntimeManager {
    pub fn new() -> Self {
        Self::new_with_config(MatchingRuntimeConfig::default())
    }

    pub fn new_with_config(config: MatchingRuntimeConfig) -> Self {
        Self::new_with_pending_output_capacity_and_output_retry_limit(
            config.output_commit.pending_output_capacity,
            config.output_commit.max_unavailable_attempts,
        )
    }

    pub fn new_with_pending_output_capacity(default_pending_output_capacity: usize) -> Self {
        let config = MatchingRuntimeConfig::default();
        Self::new_with_pending_output_capacity_and_output_retry_limit(
            default_pending_output_capacity,
            config.output_commit.max_unavailable_attempts,
        )
    }

    pub fn new_with_pending_output_capacity_and_output_retry_limit(
        default_pending_output_capacity: usize,
        output_commit_max_unavailable_attempts: usize,
    ) -> Self {
        Self {
            runtimes: HashMap::new(),
            pending_output_buffers: HashMap::new(),
            output_commit_retry_trackers: HashMap::new(),
            output_commit_escalations: HashMap::new(),
            output_commit_escalation_query_statuses: HashMap::new(),
            output_commit_quarantines: HashMap::new(),
            output_commit_quarantine_query_statuses: HashMap::new(),
            default_pending_output_capacity,
            output_commit_max_unavailable_attempts,
        }
    }

    pub fn symbols(&self) -> Vec<Symbol> {
        self.runtimes.keys().cloned().collect()
    }

    pub fn add_symbol(&mut self, symbol: Symbol) {
        self.runtimes
            .entry(symbol.clone())
            .or_insert_with(|| SymbolRuntime::new(symbol.clone()));
        self.pending_output_buffers
            .entry(symbol.clone())
            .or_insert_with(|| PendingOutputBuffer::new(self.default_pending_output_capacity));
        self.output_commit_retry_trackers
            .entry(symbol)
            .or_insert_with(|| {
                OutputCommitRetryTracker::new(self.output_commit_max_unavailable_attempts)
            });
    }

    pub fn restore_symbol_from_snapshot(&mut self, snapshot: SymbolRuntimeSnapshot) {
        let symbol = snapshot.order_book_snapshot.symbol.clone();

        self.runtimes.insert(
            symbol.clone(),
            SymbolRuntime::restore_from_snapshot(snapshot),
        );
        self.pending_output_buffers.insert(
            symbol.clone(),
            PendingOutputBuffer::new(self.default_pending_output_capacity),
        );
        self.output_commit_retry_trackers.insert(
            symbol.clone(),
            OutputCommitRetryTracker::new(self.output_commit_max_unavailable_attempts),
        );
        self.output_commit_escalations.remove(&symbol);
        self.output_commit_escalation_query_statuses.remove(&symbol);
        self.output_commit_quarantines.remove(&symbol);
        self.output_commit_quarantine_query_statuses.remove(&symbol);
    }

    pub fn symbol_status(&self, symbol: &Symbol) -> Option<SymbolRuntimeStatus> {
        let runtime = self.runtimes.get(symbol)?;
        let pending_output_buffer = self.pending_output_buffers.get(symbol)?;

        Some(SymbolRuntimeStatus {
            symbol: symbol.clone(),
            last_input_seq: runtime.last_input_seq(),
            pending_output_len: pending_output_buffer.len(),
            pending_output_capacity: pending_output_buffer.capacity(),
            pending_output_full: pending_output_buffer.is_full(),
            output_commit_escalation: self.output_commit_escalations.get(symbol).copied(),
            output_commit_quarantine: self.output_commit_quarantines.get(symbol).copied(),
            output_commit_blockage: self.output_commit_blockage_status(
                symbol,
                pending_output_buffer.len(),
                pending_output_buffer.capacity(),
                pending_output_buffer.is_full(),
            ),
        })
    }

    fn output_commit_blockage_status(
        &self,
        symbol: &Symbol,
        pending_output_len: usize,
        pending_output_capacity: usize,
        pending_output_full: bool,
    ) -> Option<OutputCommitBlockageStatus> {
        if let Some(decision) = self.output_commit_escalations.get(symbol).copied() {
            return Some(OutputCommitBlockageStatus {
                kind: OutputCommitBlockageKind::Escalation,
                decision,
                output_batch_query_status: self
                    .output_commit_escalation_query_statuses
                    .get(symbol)
                    .cloned(),
                pending_output_len,
                pending_output_capacity,
                pending_output_full,
            });
        }

        self.output_commit_quarantines
            .get(symbol)
            .copied()
            .map(|decision| OutputCommitBlockageStatus {
                kind: OutputCommitBlockageKind::Quarantine,
                decision,
                output_batch_query_status: self
                    .output_commit_quarantine_query_statuses
                    .get(symbol)
                    .cloned(),
                pending_output_len,
                pending_output_capacity,
                pending_output_full,
            })
    }

    pub fn last_input_seq(&self, symbol: &Symbol) -> Option<Option<JournalSeq>> {
        self.runtimes
            .get(symbol)
            .map(|runtime| runtime.last_input_seq())
    }

    pub fn checksum(&self, symbol: &Symbol) -> Option<Checksum> {
        self.runtimes.get(symbol).map(SymbolRuntime::checksum)
    }

    pub fn symbol_snapshot(&self, symbol: &Symbol) -> Option<Option<SymbolRuntimeSnapshot>> {
        self.runtimes.get(symbol).map(SymbolRuntime::snapshot)
    }

    pub fn pending_output_len(&self, symbol: &Symbol) -> Option<usize> {
        self.pending_output_buffers
            .get(symbol)
            .map(PendingOutputBuffer::len)
    }

    pub fn clear_symbol_output_commit_escalation(
        &mut self,
        symbol: &Symbol,
    ) -> Result<Option<OutputCommitBlockDecision>, RuntimeManagerError> {
        self.runtimes
            .get(symbol)
            .ok_or(RuntimeManagerError::UnknownSymbol)?;

        self.output_commit_retry_trackers.insert(
            symbol.clone(),
            OutputCommitRetryTracker::new(self.output_commit_max_unavailable_attempts),
        );

        self.output_commit_escalation_query_statuses.remove(symbol);
        Ok(self.output_commit_escalations.remove(symbol))
    }

    pub fn quarantine_symbol_output_commit_escalation(
        &mut self,
        symbol: &Symbol,
    ) -> Result<Option<OutputCommitBlockDecision>, RuntimeManagerError> {
        self.runtimes
            .get(symbol)
            .ok_or(RuntimeManagerError::UnknownSymbol)?;

        let escalation = self.output_commit_escalations.remove(symbol);
        let output_batch_query_status = self.output_commit_escalation_query_statuses.remove(symbol);

        if let Some(decision) = escalation {
            self.output_commit_quarantines
                .insert(symbol.clone(), decision);
            if let Some(status) = output_batch_query_status {
                self.output_commit_quarantine_query_statuses
                    .insert(symbol.clone(), status);
            }
        }

        Ok(escalation)
    }

    pub fn clear_symbol_output_commit_quarantine(
        &mut self,
        symbol: &Symbol,
    ) -> Result<Option<OutputCommitBlockDecision>, RuntimeManagerError> {
        self.runtimes
            .get(symbol)
            .ok_or(RuntimeManagerError::UnknownSymbol)?;

        self.output_commit_retry_trackers.insert(
            symbol.clone(),
            OutputCommitRetryTracker::new(self.output_commit_max_unavailable_attempts),
        );

        self.output_commit_quarantine_query_statuses.remove(symbol);
        Ok(self.output_commit_quarantines.remove(symbol))
    }

    pub fn run_symbol_step_with_output_batch_commit(
        &mut self,
        symbol: &Symbol,
        handoff: &mut BoundedHandoff,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
        max_input_entries: usize,
        max_output_requests: usize,
    ) -> Result<PerSymbolExecutionLoopOutputCommitStepReport, RuntimeManagerError> {
        let runtime = self
            .runtimes
            .get_mut(symbol)
            .ok_or(RuntimeManagerError::UnknownSymbol)?;
        let pending_output_buffer = self
            .pending_output_buffers
            .get_mut(symbol)
            .ok_or(RuntimeManagerError::UnknownSymbol)?;

        run_per_symbol_execution_loop_step_with_output_batch_commit(
            runtime,
            handoff,
            pending_output_buffer,
            journal_client,
            output,
            max_input_entries,
            max_output_requests,
        )
        .map_err(RuntimeManagerError::OutputCommitStepFailed)
    }

    pub fn run_symbol_output_batch_commit_step(
        &mut self,
        symbol: &Symbol,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
        max_output_requests: usize,
    ) -> Result<RuntimeManagerOutputCommitStepReport, RuntimeManagerError> {
        let runtime = self
            .runtimes
            .get_mut(symbol)
            .ok_or(RuntimeManagerError::UnknownSymbol)?;
        let pending_output_buffer = self
            .pending_output_buffers
            .get_mut(symbol)
            .ok_or(RuntimeManagerError::UnknownSymbol)?;

        let output_commit_report_with_identity = run_output_batch_commit_step_report_with_identity(
            symbol,
            journal_client,
            pending_output_buffer,
            output,
            max_output_requests,
        );
        let output_commit_report = output_commit_report_with_identity.commit_report;
        let safe_point_advanced_count = advance_runtime_safe_point_from_output_commit(
            runtime,
            &output_commit_report.commit_result,
        )
        .map_err(|error| {
            RuntimeManagerError::OutputCommitStepFailed(
                PerSymbolExecutionLoopOutputCommitStepError::SafePoint(error),
            )
        })?;

        Ok(RuntimeManagerOutputCommitStepReport {
            safe_point_advanced_count,
            output_batch_identity: output_commit_report_with_identity.batch_identity,
            output_batch_query_status: output_commit_report_with_identity.output_batch_query_status,
            output_commit_report,
        })
    }

    pub fn run_symbol_pressure_aware_step(
        &mut self,
        symbol: &Symbol,
        handoff: &mut BoundedHandoff,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
        max_input_entries: usize,
        max_output_requests: usize,
    ) -> Result<PerSymbolExecutionLoopOutputCommitStepReport, RuntimeManagerError> {
        let pending_output_full = self
            .pending_output_buffers
            .get(symbol)
            .ok_or(RuntimeManagerError::UnknownSymbol)?
            .is_full();

        if pending_output_full {
            let output_only_report = self.run_symbol_output_batch_commit_step(
                symbol,
                journal_client,
                output,
                max_output_requests,
            )?;

            return Ok(PerSymbolExecutionLoopOutputCommitStepReport {
                input_processed_count: 0,
                safe_point_advanced_count: output_only_report.safe_point_advanced_count,
                output_batch_identity: output_only_report.output_batch_identity,
                output_batch_query_status: output_only_report.output_batch_query_status,
                output_commit_report: output_only_report.output_commit_report,
            });
        }

        self.run_symbol_step_with_output_batch_commit(
            symbol,
            handoff,
            journal_client,
            output,
            max_input_entries,
            max_output_requests,
        )
    }

    pub fn run_symbol_retry_aware_step(
        &mut self,
        symbol: &Symbol,
        handoff: &mut BoundedHandoff,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
        max_input_entries: usize,
        max_output_requests: usize,
    ) -> Result<RuntimeManagerRetryAwareStepReport, RuntimeManagerError> {
        self.runtimes
            .get(symbol)
            .ok_or(RuntimeManagerError::UnknownSymbol)?;

        if let Some(decision) = self.output_commit_escalations.get(symbol).copied() {
            return Ok(RuntimeManagerRetryAwareStepReport {
                input_processed_count: 0,
                safe_point_advanced_count: 0,
                output_batch_identity: None,
                output_batch_query_status: self
                    .output_commit_escalation_query_statuses
                    .get(symbol)
                    .cloned(),
                output_commit_report: OutputBatchCommitStepReport {
                    commit_result: OutputBatchCommitResult {
                        committed_count: 0,
                        last_committed_seq: None,
                        committed_seqs: Vec::new(),
                    },
                    blocking_seq: Some(decision.blocked_seq),
                    blocking_outcome: Some(decision.outcome),
                },
                block_decision: Some(decision),
            });
        }

        if let Some(decision) = self.output_commit_quarantines.get(symbol).copied() {
            return Ok(RuntimeManagerRetryAwareStepReport {
                input_processed_count: 0,
                safe_point_advanced_count: 0,
                output_batch_identity: None,
                output_batch_query_status: self
                    .output_commit_quarantine_query_statuses
                    .get(symbol)
                    .cloned(),
                output_commit_report: OutputBatchCommitStepReport {
                    commit_result: OutputBatchCommitResult {
                        committed_count: 0,
                        last_committed_seq: None,
                        committed_seqs: Vec::new(),
                    },
                    blocking_seq: Some(decision.blocked_seq),
                    blocking_outcome: Some(decision.outcome),
                },
                block_decision: Some(decision),
            });
        }

        let step_report = self.run_symbol_pressure_aware_step(
            symbol,
            handoff,
            journal_client,
            output,
            max_input_entries,
            max_output_requests,
        )?;
        let retry_tracker = self
            .output_commit_retry_trackers
            .get_mut(symbol)
            .ok_or(RuntimeManagerError::UnknownSymbol)?;
        let block_decision = retry_tracker.record_blocked_report(&step_report.output_commit_report);

        if let Some(decision) = block_decision {
            if decision.action == OutputCommitBlockAction::StopAndEscalate {
                self.output_commit_escalations
                    .insert(symbol.clone(), decision);
                if let Some(status) = step_report.output_batch_query_status.clone() {
                    self.output_commit_escalation_query_statuses
                        .insert(symbol.clone(), status);
                } else {
                    self.output_commit_escalation_query_statuses.remove(symbol);
                }
            }
        }

        Ok(RuntimeManagerRetryAwareStepReport {
            input_processed_count: step_report.input_processed_count,
            safe_point_advanced_count: step_report.safe_point_advanced_count,
            output_batch_identity: step_report.output_batch_identity,
            output_batch_query_status: step_report.output_batch_query_status,
            output_commit_report: step_report.output_commit_report,
            block_decision,
        })
    }

    pub fn process_batch(
        &mut self,
        entries: Vec<JournalInputEntry>,
        output: &mut dyn JournalOutputAppender,
    ) -> Result<usize, RuntimeManagerError> {
        let mut processed = 0;

        for entry in entries {
            self.process_entry(entry, output)?;
            processed += 1;
        }

        Ok(processed)
    }

    pub fn process_entry(
        &mut self,
        entry: JournalInputEntry,
        output: &mut dyn JournalOutputAppender,
    ) -> Result<(), RuntimeManagerError> {
        let symbol = entry.command.symbol().clone();
        let runtime = self
            .runtimes
            .get_mut(&symbol)
            .ok_or(RuntimeManagerError::UnknownSymbol)?;

        runtime
            .process_entry(entry, output)
            .map_err(|_| RuntimeManagerError::OutputAppendFailed)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bounded_handoff::BoundedHandoff;
    use crate::journal_adapter::{
        JournalAdapterError, JournalInputEntry, JournalOutputAppender, JournalOutputEntry,
    };
    use crate::matching_engine::{
        EngineEvent, MarketEvent, OrderAck, OrderAddedEvent, PriceLevelChangedEvent,
    };
    use crate::order::{Command, Order};
    use crate::output_commit_boundary::{
        OutputCommitBlockAction, OutputCommitOutcome, OutputJournalClient,
    };
    use crate::types::{CommandId, JournalSeq, MarketSeq, OrderId, Price, Quantity, Side, Symbol};

    fn btc() -> Symbol {
        Symbol("BTC-USDT".to_string())
    }

    fn eth() -> Symbol {
        Symbol("ETH-USDT".to_string())
    }

    fn default_pending_output_capacity() -> usize {
        MatchingRuntimeConfig::default()
            .output_commit
            .pending_output_capacity
    }

    #[test]
    fn manager_can_register_multiple_symbol_runtimes() {
        let mut manager = RuntimeManager::new();

        manager.add_symbol(btc());
        manager.add_symbol(eth());

        assert_eq!(manager.last_input_seq(&btc()), Some(None));
        assert_eq!(manager.last_input_seq(&eth()), Some(None));
    }

    #[test]
    fn manager_returns_none_for_unknown_symbol() {
        let manager = RuntimeManager::new();

        assert_eq!(manager.last_input_seq(&btc()), None);
    }

    struct InMemoryJournalOutputAppender {
        entries: Vec<JournalOutputEntry>,
    }

    impl InMemoryJournalOutputAppender {
        fn new() -> Self {
            Self {
                entries: Vec::new(),
            }
        }
    }

    impl JournalOutputAppender for InMemoryJournalOutputAppender {
        fn append(
            &mut self,
            command_id: CommandId,
            journal_seq: JournalSeq,
            events: Vec<EngineEvent>,
        ) -> Result<(), JournalAdapterError> {
            self.entries.push(JournalOutputEntry {
                command_id,
                journal_seq,
                events,
                output_commit_metadata: None,
            });

            Ok(())
        }

        fn read_all(&self) -> Vec<JournalOutputEntry> {
            self.entries.clone()
        }
    }

    fn input_entry(seq: u64, command_id: u64, order_id: u64, symbol: Symbol) -> JournalInputEntry {
        JournalInputEntry {
            seq: JournalSeq(seq),
            command_id: CommandId(command_id),
            command: Command::PlaceLimit(Order {
                order_id: OrderId(order_id),
                symbol,
                side: Side::Buy,
                price: Price(100),
                quantity: Quantity(5),
            }),
        }
    }

    #[test]
    fn manager_routes_entry_to_matching_symbol_runtime() {
        let mut manager = RuntimeManager::new();
        manager.add_symbol(btc());
        manager.add_symbol(eth());

        let mut output = InMemoryJournalOutputAppender::new();

        assert_eq!(
            manager.process_entry(input_entry(1, 10, 100, btc()), &mut output),
            Ok(())
        );

        assert_eq!(manager.last_input_seq(&btc()), Some(Some(JournalSeq(1))));
        assert_eq!(manager.last_input_seq(&eth()), Some(None));

        let entries = output.read_all();
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].events,
            vec![
                EngineEvent::OrderAck(OrderAck::Accepted {
                    command_id: CommandId(10),
                    order_id: OrderId(100),
                    journal_seq: JournalSeq(1),
                }),
                EngineEvent::Market(MarketEvent::OrderAdded(OrderAddedEvent {
                    market_seq: MarketSeq(1),
                    command_id: CommandId(10),
                    journal_seq: JournalSeq(1),
                    order_id: OrderId(100),
                    side: Side::Buy,
                    price: Price(100),
                    quantity: Quantity(5),
                })),
                EngineEvent::Market(MarketEvent::PriceLevelChanged(PriceLevelChangedEvent {
                    market_seq: MarketSeq(2),
                    command_id: CommandId(10),
                    journal_seq: JournalSeq(1),
                    side: Side::Buy,
                    price: Price(100),
                    quantity_after: Quantity(5),
                })),
            ]
        );
    }

    #[test]
    fn manager_returns_error_for_unknown_symbol_entry() {
        let mut manager = RuntimeManager::new();
        manager.add_symbol(btc());

        let mut output = InMemoryJournalOutputAppender::new();

        let result = manager.process_entry(input_entry(1, 10, 100, eth()), &mut output);

        assert_eq!(result, Err(RuntimeManagerError::UnknownSymbol));
        assert_eq!(manager.last_input_seq(&btc()), Some(None));
        assert_eq!(output.read_all(), Vec::new());
    }

    struct FailingJournalOutputAppender;

    impl JournalOutputAppender for FailingJournalOutputAppender {
        fn append(
            &mut self,
            _command_id: CommandId,
            _journal_seq: JournalSeq,
            _events: Vec<EngineEvent>,
        ) -> Result<(), JournalAdapterError> {
            Err(JournalAdapterError::AppendFailed)
        }

        fn read_all(&self) -> Vec<JournalOutputEntry> {
            Vec::new()
        }
    }

    #[test]
    fn manager_maps_output_append_failure_and_does_not_advance_runtime() {
        let mut manager = RuntimeManager::new();
        manager.add_symbol(btc());

        let mut output = FailingJournalOutputAppender;

        let result = manager.process_entry(input_entry(1, 10, 100, btc()), &mut output);

        assert_eq!(result, Err(RuntimeManagerError::OutputAppendFailed));
        assert_eq!(manager.last_input_seq(&btc()), Some(None));
    }

    #[test]
    fn manager_processes_batch_across_multiple_symbols() {
        let mut manager = RuntimeManager::new();
        manager.add_symbol(btc());
        manager.add_symbol(eth());

        let mut output = InMemoryJournalOutputAppender::new();

        let entries = vec![
            input_entry(1, 10, 100, btc()),
            input_entry(2, 11, 200, eth()),
            input_entry(3, 12, 101, btc()),
        ];

        assert_eq!(manager.process_batch(entries, &mut output), Ok(3));

        assert_eq!(manager.last_input_seq(&btc()), Some(Some(JournalSeq(3))));
        assert_eq!(manager.last_input_seq(&eth()), Some(Some(JournalSeq(2))));

        let output_entries = output.read_all();
        assert_eq!(output_entries.len(), 3);
        assert_eq!(output_entries[0].journal_seq, JournalSeq(1));
        assert_eq!(output_entries[1].journal_seq, JournalSeq(2));
        assert_eq!(output_entries[2].journal_seq, JournalSeq(3));
    }

    #[test]
    fn manager_batch_stops_at_unknown_symbol_and_does_not_process_later_entries() {
        let mut manager = RuntimeManager::new();
        manager.add_symbol(btc());

        let mut output = InMemoryJournalOutputAppender::new();

        let entries = vec![
            input_entry(1, 10, 100, btc()),
            input_entry(2, 11, 200, eth()),
            input_entry(3, 12, 101, btc()),
        ];

        assert_eq!(
            manager.process_batch(entries, &mut output),
            Err(RuntimeManagerError::UnknownSymbol)
        );

        assert_eq!(manager.last_input_seq(&btc()), Some(Some(JournalSeq(1))));

        let output_entries = output.read_all();
        assert_eq!(output_entries.len(), 1);
        assert_eq!(output_entries[0].journal_seq, JournalSeq(1));
    }

    struct FailOnSecondAppendJournalOutputAppender {
        entries: Vec<JournalOutputEntry>,
        append_count: usize,
    }

    impl FailOnSecondAppendJournalOutputAppender {
        fn new() -> Self {
            Self {
                entries: Vec::new(),
                append_count: 0,
            }
        }
    }

    impl JournalOutputAppender for FailOnSecondAppendJournalOutputAppender {
        fn append(
            &mut self,
            command_id: CommandId,
            journal_seq: JournalSeq,
            events: Vec<EngineEvent>,
        ) -> Result<(), JournalAdapterError> {
            self.append_count += 1;

            if self.append_count == 2 {
                return Err(JournalAdapterError::AppendFailed);
            }

            self.entries.push(JournalOutputEntry {
                command_id,
                journal_seq,
                events,
                output_commit_metadata: None,
            });

            Ok(())
        }

        fn read_all(&self) -> Vec<JournalOutputEntry> {
            self.entries.clone()
        }
    }

    #[test]
    fn manager_batch_stops_at_output_append_failure_and_does_not_process_later_entries() {
        let mut manager = RuntimeManager::new();
        manager.add_symbol(btc());
        manager.add_symbol(eth());

        let mut output = FailOnSecondAppendJournalOutputAppender::new();

        let entries = vec![
            input_entry(1, 10, 100, btc()),
            input_entry(2, 11, 200, eth()),
            input_entry(3, 12, 101, btc()),
        ];

        assert_eq!(
            manager.process_batch(entries, &mut output),
            Err(RuntimeManagerError::OutputAppendFailed)
        );

        assert_eq!(manager.last_input_seq(&btc()), Some(Some(JournalSeq(1))));
        assert_eq!(manager.last_input_seq(&eth()), Some(None));

        let output_entries = output.read_all();
        assert_eq!(output_entries.len(), 1);
        assert_eq!(output_entries[0].journal_seq, JournalSeq(1));
    }

    #[test]
    fn manager_output_batch_commit_step_advances_confirmed_prefix_and_preserves_blocked_tail() {
        let mut manager = RuntimeManager::new();
        let mut handoff = BoundedHandoff::new(4);
        let mut journal_client = OutputJournalClient::new();
        let mut output = FailOnSecondAppendJournalOutputAppender::new();

        manager.add_symbol(btc());
        assert_eq!(handoff.enqueue(input_entry(1, 10, 100, btc())), Ok(()));
        assert_eq!(handoff.enqueue(input_entry(2, 11, 101, btc())), Ok(()));
        assert_eq!(handoff.enqueue(input_entry(3, 12, 102, btc())), Ok(()));

        let report = manager
            .run_symbol_step_with_output_batch_commit(
                &btc(),
                &mut handoff,
                &mut journal_client,
                &mut output,
                10,
                10,
            )
            .expect("unavailable journal output should block the tail without failing the step");

        assert_eq!(report.input_processed_count, 3);
        assert_eq!(report.safe_point_advanced_count, 1);
        assert_eq!(
            report.output_commit_report.blocking_seq,
            Some(JournalSeq(2))
        );
        assert_eq!(
            report.output_commit_report.blocking_outcome,
            Some(OutputCommitOutcome::Unavailable)
        );
        assert_eq!(manager.last_input_seq(&btc()), Some(Some(JournalSeq(1))));
        assert_eq!(manager.pending_output_len(&btc()), Some(2));

        let output_entries = output.read_all();
        assert_eq!(output_entries.len(), 1);
        assert_eq!(output_entries[0].journal_seq, JournalSeq(1));
    }

    #[test]
    fn manager_output_batch_commit_step_retries_blocked_tail_on_next_iteration() {
        let mut manager = RuntimeManager::new();
        let mut handoff = BoundedHandoff::new(4);
        let mut journal_client = OutputJournalClient::new();
        let mut output = FailOnSecondAppendJournalOutputAppender::new();

        manager.add_symbol(btc());
        assert_eq!(handoff.enqueue(input_entry(1, 10, 100, btc())), Ok(()));
        assert_eq!(handoff.enqueue(input_entry(2, 11, 101, btc())), Ok(()));
        assert_eq!(handoff.enqueue(input_entry(3, 12, 102, btc())), Ok(()));

        let first_report = manager
            .run_symbol_step_with_output_batch_commit(
                &btc(),
                &mut handoff,
                &mut journal_client,
                &mut output,
                10,
                10,
            )
            .expect("first iteration should preserve the blocked tail");

        assert_eq!(first_report.safe_point_advanced_count, 1);
        assert_eq!(
            first_report.output_commit_report.blocking_seq,
            Some(JournalSeq(2))
        );
        assert_eq!(manager.last_input_seq(&btc()), Some(Some(JournalSeq(1))));
        assert_eq!(manager.pending_output_len(&btc()), Some(2));

        let second_report = manager
            .run_symbol_step_with_output_batch_commit(
                &btc(),
                &mut handoff,
                &mut journal_client,
                &mut output,
                10,
                10,
            )
            .expect("second iteration should retry and commit the blocked tail");

        assert_eq!(second_report.input_processed_count, 0);
        assert_eq!(second_report.safe_point_advanced_count, 2);
        assert_eq!(second_report.output_commit_report.blocking_seq, None);
        assert_eq!(manager.last_input_seq(&btc()), Some(Some(JournalSeq(3))));
        assert_eq!(manager.pending_output_len(&btc()), Some(0));

        let output_entries = output.read_all();
        assert_eq!(output_entries.len(), 3);
        assert_eq!(output_entries[0].journal_seq, JournalSeq(1));
        assert_eq!(output_entries[1].journal_seq, JournalSeq(2));
        assert_eq!(output_entries[2].journal_seq, JournalSeq(3));
    }

    #[test]
    fn manager_retry_aware_step_tracks_unavailable_attempts_per_symbol() {
        let mut manager =
            RuntimeManager::new_with_pending_output_capacity_and_output_retry_limit(4, 2);
        let mut btc_handoff = BoundedHandoff::new(4);
        let mut eth_handoff = BoundedHandoff::new(4);
        let mut journal_client = OutputJournalClient::new();
        let mut output = FailingJournalOutputAppender;

        manager.add_symbol(btc());
        manager.add_symbol(eth());
        assert_eq!(btc_handoff.enqueue(input_entry(1, 10, 100, btc())), Ok(()));
        assert_eq!(eth_handoff.enqueue(input_entry(2, 20, 200, eth())), Ok(()));

        let btc_report = manager
            .run_symbol_retry_aware_step(
                &btc(),
                &mut btc_handoff,
                &mut journal_client,
                &mut output,
                1,
                10,
            )
            .expect("btc unavailable output should produce retry decision");
        let btc_decision = btc_report
            .block_decision
            .expect("btc unavailable output should block");

        assert_eq!(btc_decision.action, OutputCommitBlockAction::RetryLater);
        assert_eq!(btc_decision.blocked_seq, JournalSeq(1));
        assert_eq!(btc_decision.attempt_count, 1);

        let eth_report = manager
            .run_symbol_retry_aware_step(
                &eth(),
                &mut eth_handoff,
                &mut journal_client,
                &mut output,
                1,
                10,
            )
            .expect("eth unavailable output should produce independent retry decision");
        let eth_decision = eth_report
            .block_decision
            .expect("eth unavailable output should block");

        assert_eq!(eth_decision.action, OutputCommitBlockAction::RetryLater);
        assert_eq!(eth_decision.blocked_seq, JournalSeq(2));
        assert_eq!(eth_decision.attempt_count, 1);
    }

    #[test]
    fn manager_output_escalation_pauses_only_the_escalated_symbol() {
        let mut manager =
            RuntimeManager::new_with_pending_output_capacity_and_output_retry_limit(4, 1);
        let mut btc_handoff = BoundedHandoff::new(4);
        let mut eth_handoff = BoundedHandoff::new(4);
        let mut journal_client = OutputJournalClient::new();
        let mut failing_output = FailingJournalOutputAppender;

        manager.add_symbol(btc());
        manager.add_symbol(eth());
        assert_eq!(btc_handoff.enqueue(input_entry(1, 10, 100, btc())), Ok(()));

        let btc_report = manager
            .run_symbol_retry_aware_step(
                &btc(),
                &mut btc_handoff,
                &mut journal_client,
                &mut failing_output,
                1,
                10,
            )
            .expect("btc unavailable output should escalate at threshold one");
        let btc_decision = btc_report
            .block_decision
            .expect("btc unavailable output should block");

        assert_eq!(
            btc_decision.action,
            OutputCommitBlockAction::StopAndEscalate
        );
        assert_eq!(manager.last_input_seq(&btc()), Some(None));
        assert_eq!(manager.pending_output_len(&btc()), Some(1));

        let mut successful_output = InMemoryJournalOutputAppender::new();
        assert_eq!(eth_handoff.enqueue(input_entry(1, 20, 200, eth())), Ok(()));

        let eth_report = manager
            .run_symbol_retry_aware_step(
                &eth(),
                &mut eth_handoff,
                &mut journal_client,
                &mut successful_output,
                1,
                10,
            )
            .expect("eth should continue while btc is escalated");

        assert_eq!(eth_report.input_processed_count, 1);
        assert_eq!(eth_report.safe_point_advanced_count, 1);
        assert_eq!(eth_report.block_decision, None);
        assert_eq!(manager.last_input_seq(&eth()), Some(Some(JournalSeq(1))));
        assert_eq!(manager.pending_output_len(&eth()), Some(0));
    }

    #[test]
    fn manager_exposes_registered_symbols() {
        let mut manager = RuntimeManager::new();

        manager.add_symbol(btc());
        manager.add_symbol(eth());

        let symbols = manager.symbols();

        assert_eq!(symbols.len(), 2);
        assert!(symbols.contains(&btc()));
        assert!(symbols.contains(&eth()));
    }

    #[test]
    fn manager_exposes_symbol_status_for_registered_symbol() {
        let mut manager = RuntimeManager::new();
        manager.add_symbol(btc());

        let status = manager.symbol_status(&btc());

        assert_eq!(
            status,
            Some(SymbolRuntimeStatus {
                symbol: btc(),
                last_input_seq: None,
                pending_output_len: 0,
                pending_output_capacity: default_pending_output_capacity(),
                pending_output_full: false,
                output_commit_escalation: None,
                output_commit_quarantine: None,
                output_commit_blockage: None,
            })
        );
    }

    #[test]
    fn manager_symbol_status_reflects_processed_input_sequence() {
        let mut manager = RuntimeManager::new();
        manager.add_symbol(btc());

        let mut output = InMemoryJournalOutputAppender::new();

        assert_eq!(
            manager.process_entry(input_entry(7, 10, 100, btc()), &mut output),
            Ok(())
        );

        let status = manager.symbol_status(&btc());

        assert_eq!(
            status,
            Some(SymbolRuntimeStatus {
                symbol: btc(),
                last_input_seq: Some(JournalSeq(7)),
                pending_output_len: 0,
                pending_output_capacity: default_pending_output_capacity(),
                pending_output_full: false,
                output_commit_escalation: None,
                output_commit_quarantine: None,
                output_commit_blockage: None,
            })
        );
    }
}
