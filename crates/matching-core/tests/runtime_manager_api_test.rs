use matching_core::bounded_handoff::BoundedHandoff;
use matching_core::journal_adapter::{
    JournalAdapterError, JournalInputEntry, JournalOutputAppender, JournalOutputCommitMetadata,
    JournalOutputEntry,
};
use matching_core::matching_engine::{
    EngineEvent, MarketEvent, OrderAck, OrderAddedEvent, PriceLevelChangedEvent,
};
use matching_core::order::{Command, Order};
use matching_core::output_commit_boundary::{
    build_output_batch_identity, OutputBatchIdentity, OutputBatchQueryStatus,
    OutputCommitBlockAction, OutputCommitMetadataIndexError, OutputCommitOutcome,
    OutputCommitRequest, OutputJournalClient, PendingOutputBufferError, MATCHING_OUTPUT_VERSION,
};
use matching_core::runtime_config::{
    HandoffConfig, InputConsumerConfig, MatchingRuntimeConfig, OutputCommitConfig,
    RuntimeHostConfig, RuntimeHostMode, RuntimeTopologyConfig, SnapshotConfig,
    SnapshotVerificationConfig, SymbolAssignmentPolicy, SymbolRuntimeConfig,
};
use matching_core::runtime_manager::{
    OutputCommitBlockageKind, OutputCommitBlockageStatus, RuntimeManager, RuntimeManagerError,
    SymbolRuntimeStatus,
};
use matching_core::symbol_runtime::SymbolRuntimeOutputCommitStepError;
use matching_core::types::{
    CommandId, JournalSeq, MarketSeq, OrderId, Price, Quantity, Side, Symbol,
};

struct TestJournalOutputAppender {
    entries: Vec<JournalOutputEntry>,
}

impl TestJournalOutputAppender {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
}

impl JournalOutputAppender for TestJournalOutputAppender {
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

struct AlwaysFailingJournalOutputAppender;

impl JournalOutputAppender for AlwaysFailingJournalOutputAppender {
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

struct RejectingJournalOutputAppender;

impl JournalOutputAppender for RejectingJournalOutputAppender {
    fn append(
        &mut self,
        _command_id: CommandId,
        _journal_seq: JournalSeq,
        _events: Vec<EngineEvent>,
    ) -> Result<(), JournalAdapterError> {
        Err(JournalAdapterError::AppendRejected)
    }

    fn read_all(&self) -> Vec<JournalOutputEntry> {
        Vec::new()
    }
}

struct UnknownJournalOutputAppender;

impl JournalOutputAppender for UnknownJournalOutputAppender {
    fn append(
        &mut self,
        _command_id: CommandId,
        _journal_seq: JournalSeq,
        _events: Vec<EngineEvent>,
    ) -> Result<(), JournalAdapterError> {
        Err(JournalAdapterError::CommitOutcomeUnknown)
    }

    fn append_with_output_commit_metadata(
        &mut self,
        _command_id: CommandId,
        _journal_seq: JournalSeq,
        _events: Vec<EngineEvent>,
        _metadata: JournalOutputCommitMetadata,
    ) -> Result<(), JournalAdapterError> {
        Err(JournalAdapterError::CommitOutcomeUnknown)
    }

    fn read_all(&self) -> Vec<JournalOutputEntry> {
        Vec::new()
    }
}

struct DurableUnknownJournalOutputAppender {
    entries: Vec<JournalOutputEntry>,
}

impl DurableUnknownJournalOutputAppender {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
}

impl JournalOutputAppender for DurableUnknownJournalOutputAppender {
    fn append(
        &mut self,
        _command_id: CommandId,
        _journal_seq: JournalSeq,
        _events: Vec<EngineEvent>,
    ) -> Result<(), JournalAdapterError> {
        Err(JournalAdapterError::CommitOutcomeUnknown)
    }

    fn append_with_output_commit_metadata(
        &mut self,
        command_id: CommandId,
        journal_seq: JournalSeq,
        events: Vec<EngineEvent>,
        metadata: JournalOutputCommitMetadata,
    ) -> Result<(), JournalAdapterError> {
        self.entries.push(JournalOutputEntry {
            command_id,
            journal_seq,
            events,
            output_commit_metadata: Some(metadata),
        });

        Err(JournalAdapterError::CommitOutcomeUnknown)
    }

    fn read_all(&self) -> Vec<JournalOutputEntry> {
        self.entries.clone()
    }
}

struct FirstDurableThenUnknownJournalOutputAppender {
    entries: Vec<JournalOutputEntry>,
    append_count: usize,
}

impl FirstDurableThenUnknownJournalOutputAppender {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
            append_count: 0,
        }
    }
}

impl JournalOutputAppender for FirstDurableThenUnknownJournalOutputAppender {
    fn append(
        &mut self,
        _command_id: CommandId,
        _journal_seq: JournalSeq,
        _events: Vec<EngineEvent>,
    ) -> Result<(), JournalAdapterError> {
        Err(JournalAdapterError::CommitOutcomeUnknown)
    }

    fn append_with_output_commit_metadata(
        &mut self,
        command_id: CommandId,
        journal_seq: JournalSeq,
        events: Vec<EngineEvent>,
        metadata: JournalOutputCommitMetadata,
    ) -> Result<(), JournalAdapterError> {
        self.append_count += 1;

        if self.append_count == 1 {
            self.entries.push(JournalOutputEntry {
                command_id,
                journal_seq,
                events,
                output_commit_metadata: Some(metadata),
            });
        }

        Err(JournalAdapterError::CommitOutcomeUnknown)
    }

    fn read_all(&self) -> Vec<JournalOutputEntry> {
        self.entries.clone()
    }
}

struct ConflictingJournalOutputAppender {
    entries: Vec<JournalOutputEntry>,
}

impl ConflictingJournalOutputAppender {
    fn with_entries(entries: Vec<JournalOutputEntry>) -> Self {
        Self { entries }
    }
}

impl JournalOutputAppender for ConflictingJournalOutputAppender {
    fn append(
        &mut self,
        _command_id: CommandId,
        _journal_seq: JournalSeq,
        _events: Vec<EngineEvent>,
    ) -> Result<(), JournalAdapterError> {
        Ok(())
    }

    fn append_with_output_commit_metadata(
        &mut self,
        command_id: CommandId,
        journal_seq: JournalSeq,
        events: Vec<EngineEvent>,
        metadata: JournalOutputCommitMetadata,
    ) -> Result<(), JournalAdapterError> {
        self.entries.push(JournalOutputEntry {
            command_id,
            journal_seq,
            events,
            output_commit_metadata: Some(metadata),
        });

        Ok(())
    }

    fn read_all(&self) -> Vec<JournalOutputEntry> {
        self.entries.clone()
    }
}

fn command_entry(seq: u64, symbol: Symbol) -> JournalInputEntry {
    JournalInputEntry {
        seq: JournalSeq(seq),
        command_id: CommandId(seq),
        command: Command::PlaceLimit(Order {
            order_id: OrderId(seq),
            symbol,
            side: Side::Buy,
            price: Price(100),
            quantity: Quantity(1),
        }),
    }
}

fn output_request(seq: u64, order_id: u64) -> OutputCommitRequest {
    OutputCommitRequest {
        command_id: CommandId(seq),
        journal_seq: JournalSeq(seq),
        events: vec![
            EngineEvent::OrderAck(OrderAck::Accepted {
                command_id: CommandId(seq),
                order_id: OrderId(order_id),
                journal_seq: JournalSeq(seq),
            }),
            EngineEvent::Market(MarketEvent::OrderAdded(OrderAddedEvent {
                market_seq: MarketSeq(seq),
                command_id: CommandId(seq),
                journal_seq: JournalSeq(seq),
                order_id: OrderId(order_id),
                side: Side::Buy,
                price: Price(100),
                quantity: Quantity(1),
            })),
            EngineEvent::Market(MarketEvent::PriceLevelChanged(PriceLevelChangedEvent {
                market_seq: MarketSeq(seq + 1),
                command_id: CommandId(seq),
                journal_seq: JournalSeq(seq),
                side: Side::Buy,
                price: Price(100),
                quantity_after: Quantity(1),
            })),
        ],
    }
}

fn durable_output_entry(
    request: OutputCommitRequest,
    metadata: JournalOutputCommitMetadata,
) -> JournalOutputEntry {
    JournalOutputEntry {
        command_id: request.command_id,
        journal_seq: request.journal_seq,
        events: request.events,
        output_commit_metadata: Some(metadata),
    }
}

fn assert_conflicting_query_status(
    status: &Option<OutputBatchQueryStatus>,
    current_identity: &OutputBatchIdentity,
    drifted_identity: &OutputBatchIdentity,
) {
    match status {
        Some(OutputBatchQueryStatus::Conflict(OutputCommitMetadataIndexError::Conflict {
            batch_id,
            existing,
            incoming,
        })) => {
            assert_eq!(batch_id, &current_identity.batch_id.0);
            assert_eq!(existing.batch_id, incoming.batch_id);
            assert_eq!(existing.output_digest, drifted_identity.output_digest.0);
            assert_eq!(incoming.output_digest, current_identity.output_digest.0);
        }
        other => panic!("expected conflicting output batch query status, got {other:?}"),
    }
}

#[test]
fn runtime_manager_is_available_from_public_api() {
    let btc = Symbol("BTC-USDT".to_string());
    let mut manager = RuntimeManager::new();
    let mut output = TestJournalOutputAppender::new();

    manager.add_symbol(btc.clone());

    assert_eq!(
        manager.process_entry(command_entry(1, btc.clone()), &mut output),
        Ok(())
    );
    assert_eq!(manager.last_input_seq(&btc), Some(Some(JournalSeq(1))));
}

#[test]
fn runtime_manager_error_is_available_from_public_api() {
    let btc = Symbol("BTC-USDT".to_string());
    let eth = Symbol("ETH-USDT".to_string());
    let mut manager = RuntimeManager::new();
    let mut output = TestJournalOutputAppender::new();

    manager.add_symbol(btc);

    assert_eq!(
        manager.process_entry(command_entry(1, eth), &mut output),
        Err(RuntimeManagerError::UnknownSymbol)
    );
}

#[test]
fn runtime_manager_query_api_is_available_from_public_api() {
    let btc = Symbol("BTC-USDT".to_string());
    let eth = Symbol("ETH-USDT".to_string());

    let mut manager = RuntimeManager::new();
    manager.add_symbol(btc.clone());
    manager.add_symbol(eth.clone());

    let symbols = manager.symbols();

    assert_eq!(symbols.len(), 2);
    assert!(symbols.contains(&btc));
    assert!(symbols.contains(&eth));

    assert_eq!(
        manager.symbol_status(&btc),
        Some(SymbolRuntimeStatus {
            symbol: btc.clone(),
            last_input_seq: None,
            pending_output_len: 0,
            pending_output_capacity: 1024,
            pending_output_full: false,
            output_commit_escalation: None,
            output_commit_quarantine: None,
            output_commit_blockage: None,
        })
    );
}

#[test]
fn runtime_manager_uses_runtime_config_for_output_policy_from_public_api() {
    let btc = Symbol("BTC-USDT".to_string());
    let config = MatchingRuntimeConfig {
        topology: RuntimeTopologyConfig {
            shard_count: 1,
            assignment_policy: SymbolAssignmentPolicy::DeclarationOrder,
        },
        host: RuntimeHostConfig {
            mode: RuntimeHostMode::Manual,
        },
        output_commit: OutputCommitConfig {
            pending_output_capacity: 7,
            max_unavailable_attempts: 2,
            max_output_requests_per_step: 5,
        },
        input_consumer: InputConsumerConfig {
            max_batch_entries: 11,
        },
        handoff: HandoffConfig { capacity: 13 },
        symbol_runtime: SymbolRuntimeConfig {
            max_input_entries_per_step: 17,
        },
        snapshot: SnapshotConfig { retention_limit: 3 },
        snapshot_verification: SnapshotVerificationConfig {
            max_mismatch_attempts: 4,
        },
    };
    let mut manager = RuntimeManager::new_with_config(config);

    manager.add_symbol(btc.clone());

    let status = manager
        .symbol_status(&btc)
        .expect("configured symbol should have status");
    assert_eq!(status.pending_output_capacity, 7);
}

#[test]
fn runtime_manager_status_reports_pending_output_pressure_from_public_api() {
    let btc = Symbol("BTC-USDT".to_string());
    let mut manager = RuntimeManager::new_with_pending_output_capacity(1);
    let mut handoff = BoundedHandoff::new(4);
    let mut journal_client = OutputJournalClient::new();
    let mut output = TestJournalOutputAppender::new();

    manager.add_symbol(btc.clone());
    assert_eq!(handoff.enqueue(command_entry(1, btc.clone())), Ok(()));

    let report = manager
        .run_symbol_step_with_output_batch_commit(
            &btc,
            &mut handoff,
            &mut journal_client,
            &mut output,
            1,
            0,
        )
        .expect("step should leave output pending when max output commit is zero");

    assert_eq!(report.input_processed_count, 1);
    assert_eq!(report.safe_point_advanced_count, 0);
    assert_eq!(
        manager.symbol_status(&btc),
        Some(SymbolRuntimeStatus {
            symbol: btc.clone(),
            last_input_seq: None,
            pending_output_len: 1,
            pending_output_capacity: 1,
            pending_output_full: true,
            output_commit_escalation: None,
            output_commit_quarantine: None,
            output_commit_blockage: None,
        })
    );
}

#[test]
fn runtime_manager_can_commit_pending_output_without_draining_new_input_from_public_api() {
    let btc = Symbol("BTC-USDT".to_string());
    let mut manager = RuntimeManager::new_with_pending_output_capacity(1);
    let mut handoff = BoundedHandoff::new(4);
    let mut journal_client = OutputJournalClient::new();
    let mut output = TestJournalOutputAppender::new();

    manager.add_symbol(btc.clone());
    assert_eq!(handoff.enqueue(command_entry(1, btc.clone())), Ok(()));

    let first_report = manager
        .run_symbol_step_with_output_batch_commit(
            &btc,
            &mut handoff,
            &mut journal_client,
            &mut output,
            1,
            0,
        )
        .expect("first step should create one pending output request");

    assert_eq!(first_report.input_processed_count, 1);
    assert_eq!(manager.pending_output_len(&btc), Some(1));

    assert_eq!(handoff.enqueue(command_entry(2, btc.clone())), Ok(()));
    assert_eq!(
        manager.run_symbol_step_with_output_batch_commit(
            &btc,
            &mut handoff,
            &mut journal_client,
            &mut output,
            1,
            0,
        ),
        Err(RuntimeManagerError::OutputCommitStepFailed(
            SymbolRuntimeOutputCommitStepError::PendingOutputBuffer(
                PendingOutputBufferError::BufferFull,
            )
        ))
    );
    assert_eq!(handoff.len(), 1);
    assert_eq!(manager.last_input_seq(&btc), Some(None));
    assert_eq!(manager.pending_output_len(&btc), Some(1));

    let output_only_report = manager
        .run_symbol_output_batch_commit_step(&btc, &mut journal_client, &mut output, 10)
        .expect("output-only step should commit pending output");

    assert_eq!(output_only_report.safe_point_advanced_count, 1);
    assert_eq!(output_only_report.output_commit_report.blocking_seq, None);
    let output_batch_identity = output_only_report
        .output_batch_identity
        .expect("output-only commit should report attempted batch identity");
    assert_eq!(output_batch_identity.symbol, btc);
    assert_eq!(output_batch_identity.input_seq_start, JournalSeq(1));
    assert_eq!(output_batch_identity.input_seq_end, JournalSeq(1));
    assert_eq!(output_batch_identity.entry_count, 1);
    assert_eq!(
        output_batch_identity.matching_version,
        MATCHING_OUTPUT_VERSION
    );
    assert_eq!(manager.last_input_seq(&btc), Some(Some(JournalSeq(1))));
    assert_eq!(manager.pending_output_len(&btc), Some(0));
    assert_eq!(handoff.len(), 1);
}

#[test]
fn runtime_manager_pressure_aware_step_commits_full_pending_output_before_new_input_from_public_api(
) {
    let btc = Symbol("BTC-USDT".to_string());
    let mut manager = RuntimeManager::new_with_pending_output_capacity(1);
    let mut handoff = BoundedHandoff::new(4);
    let mut journal_client = OutputJournalClient::new();
    let mut output = TestJournalOutputAppender::new();

    manager.add_symbol(btc.clone());
    assert_eq!(handoff.enqueue(command_entry(1, btc.clone())), Ok(()));

    let first_report = manager
        .run_symbol_pressure_aware_step(&btc, &mut handoff, &mut journal_client, &mut output, 1, 0)
        .expect("first step should fill pending output");

    assert_eq!(first_report.input_processed_count, 1);
    assert_eq!(first_report.safe_point_advanced_count, 0);
    assert_eq!(manager.pending_output_len(&btc), Some(1));

    assert_eq!(handoff.enqueue(command_entry(2, btc.clone())), Ok(()));

    let second_report = manager
        .run_symbol_pressure_aware_step(&btc, &mut handoff, &mut journal_client, &mut output, 1, 10)
        .expect("second step should relieve pending output before draining input");

    assert_eq!(second_report.input_processed_count, 0);
    assert_eq!(second_report.safe_point_advanced_count, 1);
    assert_eq!(manager.last_input_seq(&btc), Some(Some(JournalSeq(1))));
    assert_eq!(manager.pending_output_len(&btc), Some(0));
    assert_eq!(handoff.len(), 1);

    let third_report = manager
        .run_symbol_pressure_aware_step(&btc, &mut handoff, &mut journal_client, &mut output, 1, 10)
        .expect("third step should process the waiting input after pressure is relieved");

    assert_eq!(third_report.input_processed_count, 1);
    assert_eq!(third_report.safe_point_advanced_count, 1);
    assert_eq!(manager.last_input_seq(&btc), Some(Some(JournalSeq(2))));
    assert_eq!(manager.pending_output_len(&btc), Some(0));
    assert_eq!(handoff.len(), 0);
}

#[test]
fn runtime_manager_retry_aware_step_escalates_repeated_unavailable_output_from_public_api() {
    let btc = Symbol("BTC-USDT".to_string());
    let mut manager = RuntimeManager::new_with_pending_output_capacity_and_output_retry_limit(4, 2);
    let mut handoff = BoundedHandoff::new(4);
    let mut journal_client = OutputJournalClient::new();
    let mut output = AlwaysFailingJournalOutputAppender;

    manager.add_symbol(btc.clone());
    assert_eq!(handoff.enqueue(command_entry(1, btc.clone())), Ok(()));

    let first_report = manager
        .run_symbol_retry_aware_step(&btc, &mut handoff, &mut journal_client, &mut output, 1, 10)
        .expect("first unavailable output should be reported as retryable");

    let first_output_batch_identity = first_report
        .output_batch_identity
        .expect("retry-aware step should report attempted output batch identity");
    assert_eq!(first_output_batch_identity.symbol, btc.clone());
    assert_eq!(first_output_batch_identity.input_seq_start, JournalSeq(1));
    assert_eq!(first_output_batch_identity.input_seq_end, JournalSeq(1));
    assert_eq!(first_output_batch_identity.entry_count, 1);

    let first_decision = first_report
        .block_decision
        .expect("first unavailable output should produce a block decision");
    assert_eq!(first_decision.action, OutputCommitBlockAction::RetryLater);
    assert_eq!(first_decision.blocked_seq, JournalSeq(1));
    assert_eq!(first_decision.outcome, OutputCommitOutcome::Unavailable);
    assert_eq!(first_decision.attempt_count, 1);
    assert_eq!(manager.last_input_seq(&btc), Some(None));
    assert_eq!(manager.pending_output_len(&btc), Some(1));
    assert_eq!(handoff.len(), 0);

    let second_report = manager
        .run_symbol_retry_aware_step(&btc, &mut handoff, &mut journal_client, &mut output, 1, 10)
        .expect("second unavailable output should be reported as escalated");

    let second_decision = second_report
        .block_decision
        .expect("second unavailable output should produce a block decision");
    assert_eq!(
        second_decision.action,
        OutputCommitBlockAction::StopAndEscalate
    );
    assert_eq!(second_decision.blocked_seq, JournalSeq(1));
    assert_eq!(second_decision.outcome, OutputCommitOutcome::Unavailable);
    assert_eq!(second_decision.attempt_count, 2);
    assert_eq!(manager.last_input_seq(&btc), Some(None));
    assert_eq!(manager.pending_output_len(&btc), Some(1));
    assert_eq!(
        manager
            .symbol_status(&btc)
            .expect("symbol status should exist")
            .output_commit_escalation,
        Some(second_decision)
    );
}

#[test]
fn runtime_manager_retry_aware_step_reports_missing_output_batch_query_status_from_public_api() {
    let btc = Symbol("BTC-USDT".to_string());
    let mut manager = RuntimeManager::new();
    let mut handoff = BoundedHandoff::new(4);
    let mut journal_client = OutputJournalClient::new();
    let mut output = UnknownJournalOutputAppender;

    manager.add_symbol(btc.clone());
    assert_eq!(handoff.enqueue(command_entry(1, btc.clone())), Ok(()));

    let report = manager
        .run_symbol_retry_aware_step(&btc, &mut handoff, &mut journal_client, &mut output, 1, 10)
        .expect("unknown output commit should be reported without advancing the safe point");

    let output_batch_identity = report
        .output_batch_identity
        .expect("retry-aware step should report attempted output batch identity");
    assert_eq!(output_batch_identity.symbol, btc.clone());
    assert_eq!(output_batch_identity.input_seq_start, JournalSeq(1));
    assert_eq!(output_batch_identity.input_seq_end, JournalSeq(1));
    assert_eq!(output_batch_identity.entry_count, 1);
    assert_eq!(
        output_batch_identity.matching_version,
        MATCHING_OUTPUT_VERSION
    );
    assert_eq!(
        report.output_batch_query_status,
        Some(OutputBatchQueryStatus::Missing)
    );
    assert_eq!(report.input_processed_count, 1);
    assert_eq!(report.safe_point_advanced_count, 0);
    assert_eq!(
        report.output_commit_report.blocking_seq,
        Some(JournalSeq(1))
    );
    assert_eq!(
        report.output_commit_report.blocking_outcome,
        Some(OutputCommitOutcome::Unknown)
    );

    let decision = report
        .block_decision
        .expect("unknown output should produce a manual resolution decision");
    assert_eq!(decision.action, OutputCommitBlockAction::ResolveUnknown);
    assert_eq!(decision.blocked_seq, JournalSeq(1));
    assert_eq!(decision.outcome, OutputCommitOutcome::Unknown);
    assert_eq!(decision.attempt_count, 1);
    assert_eq!(manager.last_input_seq(&btc), Some(None));
    assert_eq!(manager.pending_output_len(&btc), Some(1));
}

#[test]
fn runtime_manager_retry_aware_step_advances_safe_point_with_durable_output_batch_query_status_from_public_api(
) {
    let btc = Symbol("BTC-USDT".to_string());
    let mut manager = RuntimeManager::new();
    let mut handoff = BoundedHandoff::new(4);
    let mut journal_client = OutputJournalClient::new();
    let mut output = DurableUnknownJournalOutputAppender::new();

    manager.add_symbol(btc.clone());
    assert_eq!(handoff.enqueue(command_entry(1, btc.clone())), Ok(()));

    let report = manager
        .run_symbol_retry_aware_step(&btc, &mut handoff, &mut journal_client, &mut output, 1, 10)
        .expect("durable unknown output commit should advance the safe point");

    assert_eq!(report.input_processed_count, 1);
    assert_eq!(report.safe_point_advanced_count, 1);
    assert_eq!(report.output_commit_report.commit_result.committed_count, 1);
    assert_eq!(report.output_commit_report.blocking_seq, None);
    assert_eq!(report.output_commit_report.blocking_outcome, None);
    assert_eq!(report.block_decision, None);
    assert_eq!(
        report.output_batch_query_status,
        Some(OutputBatchQueryStatus::Durable)
    );
    assert_eq!(manager.last_input_seq(&btc), Some(Some(JournalSeq(1))));
    assert_eq!(manager.pending_output_len(&btc), Some(0));
    assert_eq!(output.read_all().len(), 1);
}

#[test]
fn runtime_manager_retry_aware_step_reports_incomplete_output_batch_query_status_from_public_api() {
    let btc = Symbol("BTC-USDT".to_string());
    let mut manager = RuntimeManager::new();
    let mut handoff = BoundedHandoff::new(4);
    let mut journal_client = OutputJournalClient::new();
    let mut output = FirstDurableThenUnknownJournalOutputAppender::new();

    manager.add_symbol(btc.clone());
    assert_eq!(handoff.enqueue(command_entry(1, btc.clone())), Ok(()));
    assert_eq!(handoff.enqueue(command_entry(2, btc.clone())), Ok(()));

    let report = manager
        .run_symbol_retry_aware_step(&btc, &mut handoff, &mut journal_client, &mut output, 2, 10)
        .expect("partially durable unknown output batch should report incomplete status");

    let output_batch_identity = report
        .output_batch_identity
        .expect("retry-aware step should report attempted output batch identity");
    assert_eq!(output_batch_identity.symbol, btc.clone());
    assert_eq!(output_batch_identity.input_seq_start, JournalSeq(1));
    assert_eq!(output_batch_identity.input_seq_end, JournalSeq(2));
    assert_eq!(output_batch_identity.entry_count, 2);
    assert_eq!(
        report.output_batch_query_status,
        Some(OutputBatchQueryStatus::Incomplete {
            observed_entry_count: 1,
            expected_entry_count: 2,
        })
    );
    assert_eq!(report.input_processed_count, 2);
    assert_eq!(report.safe_point_advanced_count, 1);
    assert_eq!(report.output_commit_report.commit_result.committed_count, 1);
    assert_eq!(
        report.output_commit_report.commit_result.last_committed_seq,
        Some(JournalSeq(1))
    );
    assert_eq!(
        report.output_commit_report.blocking_seq,
        Some(JournalSeq(2))
    );
    assert_eq!(
        report.output_commit_report.blocking_outcome,
        Some(OutputCommitOutcome::Unknown)
    );

    let decision = report
        .block_decision
        .expect("incomplete unknown output should require manual resolution");
    assert_eq!(decision.action, OutputCommitBlockAction::ResolveUnknown);
    assert_eq!(decision.blocked_seq, JournalSeq(2));
    assert_eq!(decision.outcome, OutputCommitOutcome::Unknown);
    assert_eq!(manager.last_input_seq(&btc), Some(Some(JournalSeq(1))));
    assert_eq!(manager.pending_output_len(&btc), Some(1));
    assert_eq!(output.read_all().len(), 1);
}

#[test]
fn runtime_manager_retry_aware_step_reports_conflicting_output_batch_query_status_from_public_api()
{
    let btc = Symbol("BTC-USDT".to_string());
    let current_requests = vec![output_request(1, 1)];
    let drifted_requests = vec![output_request(1, 999)];
    let current_identity =
        build_output_batch_identity(&btc, MATCHING_OUTPUT_VERSION, &current_requests)
            .expect("current batch should have identity");
    let drifted_identity =
        build_output_batch_identity(&btc, MATCHING_OUTPUT_VERSION, &drifted_requests)
            .expect("drifted batch should have identity");
    let drifted_metadata =
        JournalOutputCommitMetadata::from_output_batch_identity(&drifted_identity);
    let durable_drifted_entry =
        durable_output_entry(drifted_requests[0].clone(), drifted_metadata.clone());
    let mut manager = RuntimeManager::new();
    let mut handoff = BoundedHandoff::new(4);
    let mut journal_client = OutputJournalClient::new();
    let mut output = ConflictingJournalOutputAppender::with_entries(vec![durable_drifted_entry]);

    assert_eq!(current_identity.batch_id, drifted_identity.batch_id);
    assert_ne!(
        current_identity.output_digest,
        drifted_identity.output_digest
    );
    manager.add_symbol(btc.clone());
    assert_eq!(handoff.enqueue(command_entry(1, btc.clone())), Ok(()));

    let report = manager
        .run_symbol_retry_aware_step(&btc, &mut handoff, &mut journal_client, &mut output, 1, 10)
        .expect("conflicting output batch should be reported without advancing the safe point");

    assert_eq!(report.input_processed_count, 1);
    assert_eq!(report.safe_point_advanced_count, 0);
    assert_eq!(
        report.output_commit_report.blocking_seq,
        Some(JournalSeq(1))
    );
    assert_eq!(
        report.output_commit_report.blocking_outcome,
        Some(OutputCommitOutcome::Rejected)
    );
    assert_conflicting_query_status(
        &report.output_batch_query_status,
        &current_identity,
        &drifted_identity,
    );

    let decision = report
        .block_decision
        .expect("conflicting output should produce an escalation decision");
    assert_eq!(decision.action, OutputCommitBlockAction::StopAndEscalate);
    assert_eq!(decision.blocked_seq, JournalSeq(1));
    assert_eq!(decision.outcome, OutputCommitOutcome::Rejected);
    assert_eq!(manager.last_input_seq(&btc), Some(None));
    assert_eq!(manager.pending_output_len(&btc), Some(1));
    assert_eq!(handoff.enqueue(command_entry(2, btc.clone())), Ok(()));
    let paused_report = manager
        .run_symbol_retry_aware_step(&btc, &mut handoff, &mut journal_client, &mut output, 1, 10)
        .expect("conflicting symbol should remain paused");
    assert_eq!(paused_report.input_processed_count, 0);
    assert_eq!(paused_report.safe_point_advanced_count, 0);
    assert_eq!(paused_report.block_decision, Some(decision));
    assert_eq!(
        paused_report.output_batch_query_status,
        report.output_batch_query_status
    );
    assert_eq!(handoff.len(), 1);
    let blockage_query_status = manager
        .symbol_status(&btc)
        .expect("symbol status should exist")
        .output_commit_blockage
        .expect("conflicting output should create blockage status")
        .output_batch_query_status;
    assert_conflicting_query_status(&blockage_query_status, &current_identity, &drifted_identity);
    assert_eq!(output.read_all().len(), 1);
}

#[test]
fn runtime_manager_quarantine_preserves_conflicting_output_batch_query_status_from_public_api() {
    let btc = Symbol("BTC-USDT".to_string());
    let current_requests = vec![output_request(1, 1)];
    let drifted_requests = vec![output_request(1, 999)];
    let current_identity =
        build_output_batch_identity(&btc, MATCHING_OUTPUT_VERSION, &current_requests)
            .expect("current batch should have identity");
    let drifted_identity =
        build_output_batch_identity(&btc, MATCHING_OUTPUT_VERSION, &drifted_requests)
            .expect("drifted batch should have identity");
    let drifted_metadata =
        JournalOutputCommitMetadata::from_output_batch_identity(&drifted_identity);
    let durable_drifted_entry = durable_output_entry(drifted_requests[0].clone(), drifted_metadata);
    let mut manager = RuntimeManager::new();
    let mut handoff = BoundedHandoff::new(4);
    let mut journal_client = OutputJournalClient::new();
    let mut output = ConflictingJournalOutputAppender::with_entries(vec![durable_drifted_entry]);

    manager.add_symbol(btc.clone());
    assert_eq!(handoff.enqueue(command_entry(1, btc.clone())), Ok(()));

    let report = manager
        .run_symbol_retry_aware_step(&btc, &mut handoff, &mut journal_client, &mut output, 1, 10)
        .expect("conflicting output batch should create an escalation");
    let decision = report
        .block_decision
        .expect("conflicting output should produce an escalation decision");

    assert_conflicting_query_status(
        &report.output_batch_query_status,
        &current_identity,
        &drifted_identity,
    );
    assert_eq!(
        manager.quarantine_symbol_output_commit_escalation(&btc),
        Ok(Some(decision))
    );

    let blockage = manager
        .symbol_status(&btc)
        .expect("symbol status should exist")
        .output_commit_blockage
        .expect("quarantine should create blockage status");
    assert_eq!(blockage.kind, OutputCommitBlockageKind::Quarantine);
    assert_eq!(blockage.decision, decision);
    assert_conflicting_query_status(
        &blockage.output_batch_query_status,
        &current_identity,
        &drifted_identity,
    );

    let paused_report = manager
        .run_symbol_retry_aware_step(&btc, &mut handoff, &mut journal_client, &mut output, 1, 10)
        .expect("quarantined symbol should remain paused");
    assert_eq!(paused_report.input_processed_count, 0);
    assert_eq!(paused_report.safe_point_advanced_count, 0);
    assert_eq!(paused_report.block_decision, Some(decision));
    assert_conflicting_query_status(
        &paused_report.output_batch_query_status,
        &current_identity,
        &drifted_identity,
    );
}

#[test]
fn runtime_manager_status_records_rejected_output_escalation_from_public_api() {
    let btc = Symbol("BTC-USDT".to_string());
    let mut manager = RuntimeManager::new();
    let mut handoff = BoundedHandoff::new(4);
    let mut journal_client = OutputJournalClient::new();
    let mut output = RejectingJournalOutputAppender;

    manager.add_symbol(btc.clone());
    assert_eq!(handoff.enqueue(command_entry(1, btc.clone())), Ok(()));

    let report = manager
        .run_symbol_retry_aware_step(&btc, &mut handoff, &mut journal_client, &mut output, 1, 10)
        .expect("rejected output should be reported as an escalation decision");
    let decision = report
        .block_decision
        .expect("rejected output should produce a block decision");

    assert_eq!(decision.action, OutputCommitBlockAction::StopAndEscalate);
    assert_eq!(decision.blocked_seq, JournalSeq(1));
    assert_eq!(decision.outcome, OutputCommitOutcome::Rejected);
    assert_eq!(decision.attempt_count, 1);
    assert_eq!(
        manager.symbol_status(&btc),
        Some(SymbolRuntimeStatus {
            symbol: btc.clone(),
            last_input_seq: None,
            pending_output_len: 1,
            pending_output_capacity: 1024,
            pending_output_full: false,
            output_commit_escalation: Some(decision),
            output_commit_quarantine: None,
            output_commit_blockage: Some(OutputCommitBlockageStatus {
                kind: OutputCommitBlockageKind::Escalation,
                decision,
                output_batch_query_status: None,
                pending_output_len: 1,
                pending_output_capacity: 1024,
                pending_output_full: false,
            }),
        })
    );
}

#[test]
fn runtime_manager_retry_aware_step_pauses_symbol_after_output_escalation_from_public_api() {
    let btc = Symbol("BTC-USDT".to_string());
    let mut manager = RuntimeManager::new();
    let mut handoff = BoundedHandoff::new(4);
    let mut journal_client = OutputJournalClient::new();
    let mut rejecting_output = RejectingJournalOutputAppender;

    manager.add_symbol(btc.clone());
    assert_eq!(handoff.enqueue(command_entry(1, btc.clone())), Ok(()));

    let escalation_report = manager
        .run_symbol_retry_aware_step(
            &btc,
            &mut handoff,
            &mut journal_client,
            &mut rejecting_output,
            1,
            10,
        )
        .expect("rejected output should create an escalation");
    let escalation = escalation_report
        .block_decision
        .expect("rejected output should produce a block decision");

    assert_eq!(escalation.action, OutputCommitBlockAction::StopAndEscalate);
    assert_eq!(manager.pending_output_len(&btc), Some(1));
    assert_eq!(handoff.len(), 0);

    let mut successful_output = TestJournalOutputAppender::new();
    assert_eq!(handoff.enqueue(command_entry(2, btc.clone())), Ok(()));

    let paused_report = manager
        .run_symbol_retry_aware_step(
            &btc,
            &mut handoff,
            &mut journal_client,
            &mut successful_output,
            1,
            10,
        )
        .expect("escalated symbol should report its existing escalation without processing");

    assert_eq!(paused_report.input_processed_count, 0);
    assert_eq!(paused_report.safe_point_advanced_count, 0);
    assert_eq!(paused_report.block_decision, Some(escalation));
    assert_eq!(manager.last_input_seq(&btc), Some(None));
    assert_eq!(manager.pending_output_len(&btc), Some(1));
    assert_eq!(handoff.len(), 1);
    assert_eq!(successful_output.read_all(), Vec::new());
}

#[test]
fn runtime_manager_can_clear_output_escalation_and_retry_pending_output_from_public_api() {
    let btc = Symbol("BTC-USDT".to_string());
    let mut manager = RuntimeManager::new();
    let mut handoff = BoundedHandoff::new(4);
    let mut journal_client = OutputJournalClient::new();
    let mut rejecting_output = RejectingJournalOutputAppender;

    manager.add_symbol(btc.clone());
    assert_eq!(handoff.enqueue(command_entry(1, btc.clone())), Ok(()));

    let escalation_report = manager
        .run_symbol_retry_aware_step(
            &btc,
            &mut handoff,
            &mut journal_client,
            &mut rejecting_output,
            1,
            10,
        )
        .expect("rejected output should create an escalation");
    let escalation = escalation_report
        .block_decision
        .expect("rejected output should produce a block decision");

    assert_eq!(manager.pending_output_len(&btc), Some(1));
    assert_eq!(
        manager
            .symbol_status(&btc)
            .expect("symbol status should exist")
            .output_commit_escalation,
        Some(escalation)
    );

    assert_eq!(
        manager.clear_symbol_output_commit_escalation(&btc),
        Ok(Some(escalation))
    );
    assert_eq!(
        manager
            .symbol_status(&btc)
            .expect("symbol status should exist")
            .output_commit_escalation,
        None
    );

    let mut successful_output = TestJournalOutputAppender::new();
    let retry_report = manager
        .run_symbol_retry_aware_step(
            &btc,
            &mut handoff,
            &mut journal_client,
            &mut successful_output,
            1,
            10,
        )
        .expect("cleared escalation should allow pending output retry");

    assert_eq!(retry_report.input_processed_count, 0);
    assert_eq!(retry_report.safe_point_advanced_count, 1);
    assert_eq!(retry_report.block_decision, None);
    assert_eq!(manager.last_input_seq(&btc), Some(Some(JournalSeq(1))));
    assert_eq!(manager.pending_output_len(&btc), Some(0));
    assert_eq!(successful_output.read_all().len(), 1);
}

#[test]
fn runtime_manager_clear_output_escalation_rejects_unknown_symbol_from_public_api() {
    let eth = Symbol("ETH-USDT".to_string());
    let mut manager = RuntimeManager::new();

    assert_eq!(
        manager.clear_symbol_output_commit_escalation(&eth),
        Err(RuntimeManagerError::UnknownSymbol)
    );
}

#[test]
fn runtime_manager_can_quarantine_output_escalation_without_advancing_safe_point_from_public_api() {
    let btc = Symbol("BTC-USDT".to_string());
    let mut manager = RuntimeManager::new();
    let mut handoff = BoundedHandoff::new(4);
    let mut journal_client = OutputJournalClient::new();
    let mut rejecting_output = RejectingJournalOutputAppender;

    manager.add_symbol(btc.clone());
    assert_eq!(handoff.enqueue(command_entry(1, btc.clone())), Ok(()));

    let escalation_report = manager
        .run_symbol_retry_aware_step(
            &btc,
            &mut handoff,
            &mut journal_client,
            &mut rejecting_output,
            1,
            10,
        )
        .expect("rejected output should create an escalation");
    let escalation = escalation_report
        .block_decision
        .expect("rejected output should produce a block decision");

    assert_eq!(
        manager.quarantine_symbol_output_commit_escalation(&btc),
        Ok(Some(escalation))
    );
    assert_eq!(
        manager.symbol_status(&btc),
        Some(SymbolRuntimeStatus {
            symbol: btc.clone(),
            last_input_seq: None,
            pending_output_len: 1,
            pending_output_capacity: 1024,
            pending_output_full: false,
            output_commit_escalation: None,
            output_commit_quarantine: Some(escalation),
            output_commit_blockage: Some(OutputCommitBlockageStatus {
                kind: OutputCommitBlockageKind::Quarantine,
                decision: escalation,
                output_batch_query_status: None,
                pending_output_len: 1,
                pending_output_capacity: 1024,
                pending_output_full: false,
            }),
        })
    );

    let mut successful_output = TestJournalOutputAppender::new();
    let quarantined_report = manager
        .run_symbol_retry_aware_step(
            &btc,
            &mut handoff,
            &mut journal_client,
            &mut successful_output,
            1,
            10,
        )
        .expect("quarantined symbol should remain paused");

    assert_eq!(quarantined_report.input_processed_count, 0);
    assert_eq!(quarantined_report.safe_point_advanced_count, 0);
    assert_eq!(quarantined_report.block_decision, Some(escalation));
    assert_eq!(manager.last_input_seq(&btc), Some(None));
    assert_eq!(manager.pending_output_len(&btc), Some(1));
    assert_eq!(successful_output.read_all(), Vec::new());
}

#[test]
fn runtime_manager_can_clear_output_quarantine_and_retry_pending_output_from_public_api() {
    let btc = Symbol("BTC-USDT".to_string());
    let mut manager = RuntimeManager::new();
    let mut handoff = BoundedHandoff::new(4);
    let mut journal_client = OutputJournalClient::new();
    let mut rejecting_output = RejectingJournalOutputAppender;

    manager.add_symbol(btc.clone());
    assert_eq!(handoff.enqueue(command_entry(1, btc.clone())), Ok(()));

    let escalation_report = manager
        .run_symbol_retry_aware_step(
            &btc,
            &mut handoff,
            &mut journal_client,
            &mut rejecting_output,
            1,
            10,
        )
        .expect("rejected output should create an escalation");
    let escalation = escalation_report
        .block_decision
        .expect("rejected output should produce a block decision");

    assert_eq!(
        manager.quarantine_symbol_output_commit_escalation(&btc),
        Ok(Some(escalation))
    );
    assert_eq!(
        manager.clear_symbol_output_commit_quarantine(&btc),
        Ok(Some(escalation))
    );
    assert_eq!(
        manager
            .symbol_status(&btc)
            .expect("symbol status should exist")
            .output_commit_blockage,
        None
    );
    assert_eq!(manager.last_input_seq(&btc), Some(None));
    assert_eq!(manager.pending_output_len(&btc), Some(1));

    let mut successful_output = TestJournalOutputAppender::new();
    let retry_report = manager
        .run_symbol_retry_aware_step(
            &btc,
            &mut handoff,
            &mut journal_client,
            &mut successful_output,
            1,
            10,
        )
        .expect("cleared quarantine should allow pending output retry");

    assert_eq!(retry_report.input_processed_count, 0);
    assert_eq!(retry_report.safe_point_advanced_count, 1);
    assert_eq!(retry_report.block_decision, None);
    assert_eq!(manager.last_input_seq(&btc), Some(Some(JournalSeq(1))));
    assert_eq!(manager.pending_output_len(&btc), Some(0));
    assert_eq!(successful_output.read_all().len(), 1);
}

#[test]
fn runtime_manager_quarantine_output_escalation_handles_empty_and_unknown_symbols_from_public_api()
{
    let btc = Symbol("BTC-USDT".to_string());
    let eth = Symbol("ETH-USDT".to_string());
    let mut manager = RuntimeManager::new();

    manager.add_symbol(btc.clone());

    assert_eq!(
        manager.quarantine_symbol_output_commit_escalation(&btc),
        Ok(None)
    );
    assert_eq!(
        manager.quarantine_symbol_output_commit_escalation(&eth),
        Err(RuntimeManagerError::UnknownSymbol)
    );
}

#[test]
fn runtime_manager_clear_output_quarantine_handles_empty_and_unknown_symbols_from_public_api() {
    let btc = Symbol("BTC-USDT".to_string());
    let eth = Symbol("ETH-USDT".to_string());
    let mut manager = RuntimeManager::new();

    manager.add_symbol(btc.clone());

    assert_eq!(
        manager.clear_symbol_output_commit_quarantine(&btc),
        Ok(None)
    );
    assert_eq!(
        manager.clear_symbol_output_commit_quarantine(&eth),
        Err(RuntimeManagerError::UnknownSymbol)
    );
}

#[test]
fn runtime_manager_output_batch_commit_step_is_available_from_public_api() {
    let btc = Symbol("BTC-USDT".to_string());
    let mut manager = RuntimeManager::new();
    let mut handoff = BoundedHandoff::new(4);
    let mut journal_client = OutputJournalClient::new();
    let mut output = TestJournalOutputAppender::new();

    manager.add_symbol(btc.clone());
    assert_eq!(handoff.enqueue(command_entry(1, btc.clone())), Ok(()));
    assert_eq!(handoff.enqueue(command_entry(2, btc.clone())), Ok(()));

    let report = manager
        .run_symbol_step_with_output_batch_commit(
            &btc,
            &mut handoff,
            &mut journal_client,
            &mut output,
            10,
            10,
        )
        .expect("runtime manager step should commit output");

    assert_eq!(report.input_processed_count, 2);
    assert_eq!(report.safe_point_advanced_count, 2);
    assert_eq!(report.output_commit_report.blocking_seq, None);
    let output_batch_identity = report
        .output_batch_identity
        .expect("integrated step should report attempted output batch identity");
    assert_eq!(output_batch_identity.symbol, btc.clone());
    assert_eq!(output_batch_identity.input_seq_start, JournalSeq(1));
    assert_eq!(output_batch_identity.input_seq_end, JournalSeq(2));
    assert_eq!(output_batch_identity.entry_count, 2);
    assert_eq!(
        output_batch_identity.matching_version,
        MATCHING_OUTPUT_VERSION
    );
    assert_eq!(manager.last_input_seq(&btc), Some(Some(JournalSeq(2))));
    assert_eq!(manager.pending_output_len(&btc), Some(0));
    assert_eq!(output.read_all().len(), 2);
}
