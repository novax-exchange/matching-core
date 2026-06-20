use matching_core::journal_adapter::{
    JournalAdapterError, JournalOutputAppender, JournalOutputCommitMetadata, JournalOutputEntry,
};
use matching_core::matching_engine::{EngineEvent, OrderAck};
use matching_core::output_commit_boundary::PendingOutputBuffer;
use matching_core::output_commit_boundary::{
    build_output_batch_identity, run_output_batch_commit_step_report_with_identity,
    OutputBatchQueryStatus, OutputCommitMetadataIndexError, OutputCommitOutcome,
    OutputCommitRequest, OutputJournalClient, MATCHING_OUTPUT_VERSION,
};
use matching_core::output_commit_boundary::{
    run_output_batch_commit_step, run_output_batch_commit_step_report, OutputBatchCommitResult,
    OutputBatchCommitStepReport, OutputCommitBlockAction, OutputCommitBlockDecision,
    OutputCommitRetryTracker,
};
use matching_core::types::{CommandId, JournalSeq, OrderId, Symbol};

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

struct FailOnSecondAppendJournalOutputAppender {
    entries: Vec<JournalOutputEntry>,
    append_count: usize,
}

struct UnknownMetadataJournalOutputAppender;

impl JournalOutputAppender for UnknownMetadataJournalOutputAppender {
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

struct DurableUnknownMetadataJournalOutputAppender {
    entries: Vec<JournalOutputEntry>,
}

impl DurableUnknownMetadataJournalOutputAppender {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
}

impl JournalOutputAppender for DurableUnknownMetadataJournalOutputAppender {
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

    fn append_with_output_commit_metadata(
        &mut self,
        command_id: CommandId,
        journal_seq: JournalSeq,
        events: Vec<EngineEvent>,
        metadata: JournalOutputCommitMetadata,
    ) -> Result<(), JournalAdapterError> {
        self.append_count += 1;

        if self.append_count == 2 {
            return Err(JournalAdapterError::AppendFailed);
        }

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

struct UnknownOnSecondAppendJournalOutputAppender {
    entries: Vec<JournalOutputEntry>,
    append_count: usize,
}

impl UnknownOnSecondAppendJournalOutputAppender {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
            append_count: 0,
        }
    }
}

impl JournalOutputAppender for UnknownOnSecondAppendJournalOutputAppender {
    fn append(
        &mut self,
        command_id: CommandId,
        journal_seq: JournalSeq,
        events: Vec<EngineEvent>,
    ) -> Result<(), JournalAdapterError> {
        self.append_count += 1;

        if self.append_count == 2 {
            return Err(JournalAdapterError::CommitOutcomeUnknown);
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

struct DurableUnknownOnSecondAppendJournalOutputAppender {
    entries: Vec<JournalOutputEntry>,
    append_count: usize,
}

impl DurableUnknownOnSecondAppendJournalOutputAppender {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
            append_count: 0,
        }
    }
}

impl JournalOutputAppender for DurableUnknownOnSecondAppendJournalOutputAppender {
    fn append(
        &mut self,
        command_id: CommandId,
        journal_seq: JournalSeq,
        events: Vec<EngineEvent>,
    ) -> Result<(), JournalAdapterError> {
        self.append_count += 1;

        self.entries.push(JournalOutputEntry {
            command_id,
            journal_seq,
            events,
            output_commit_metadata: None,
        });

        if self.append_count == 2 {
            return Err(JournalAdapterError::CommitOutcomeUnknown);
        }

        Ok(())
    }

    fn read_all(&self) -> Vec<JournalOutputEntry> {
        self.entries.clone()
    }
}

struct RejectOnSecondAppendJournalOutputAppender {
    entries: Vec<JournalOutputEntry>,
    append_count: usize,
}

impl RejectOnSecondAppendJournalOutputAppender {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
            append_count: 0,
        }
    }
}

impl JournalOutputAppender for RejectOnSecondAppendJournalOutputAppender {
    fn append(
        &mut self,
        command_id: CommandId,
        journal_seq: JournalSeq,
        events: Vec<EngineEvent>,
    ) -> Result<(), JournalAdapterError> {
        self.append_count += 1;

        if self.append_count == 2 {
            return Err(JournalAdapterError::AppendRejected);
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

fn request(seq: u64) -> OutputCommitRequest {
    OutputCommitRequest {
        command_id: CommandId(seq),
        journal_seq: JournalSeq(seq),
        events: vec![EngineEvent::OrderAck(OrderAck::Accepted {
            command_id: CommandId(seq),
            order_id: OrderId(seq),
            journal_seq: JournalSeq(seq),
        })],
    }
}

fn request_with_order(seq: u64, order_id: u64) -> OutputCommitRequest {
    OutputCommitRequest {
        command_id: CommandId(seq),
        journal_seq: JournalSeq(seq),
        events: vec![EngineEvent::OrderAck(OrderAck::Accepted {
            command_id: CommandId(seq),
            order_id: OrderId(order_id),
            journal_seq: JournalSeq(seq),
        })],
    }
}

#[test]
fn output_batch_identity_covers_symbol_sequence_range_count_version_and_digest() {
    let symbol = Symbol("BTC-USDT".to_string());
    let requests = vec![request(7), request(8), request(9)];

    let identity = build_output_batch_identity(&symbol, MATCHING_OUTPUT_VERSION, &requests)
        .expect("non-empty output batch should have an identity");
    let same_identity = build_output_batch_identity(&symbol, MATCHING_OUTPUT_VERSION, &requests)
        .expect("same output batch should have an identity");

    assert_eq!(identity, same_identity);
    assert_eq!(identity.symbol, symbol);
    assert_eq!(identity.input_seq_start, JournalSeq(7));
    assert_eq!(identity.input_seq_end, JournalSeq(9));
    assert_eq!(identity.entry_count, 3);
    assert_eq!(identity.matching_version, MATCHING_OUTPUT_VERSION);

    let drifted_requests = vec![request(7), request_with_order(8, 800), request(9)];
    let drifted_identity =
        build_output_batch_identity(&symbol, MATCHING_OUTPUT_VERSION, &drifted_requests)
            .expect("drifted output batch should still have an identity");

    assert_eq!(drifted_identity.input_seq_start, JournalSeq(7));
    assert_eq!(drifted_identity.input_seq_end, JournalSeq(9));
    assert_eq!(drifted_identity.entry_count, 3);
    assert_ne!(drifted_identity.output_digest, identity.output_digest);
    assert_eq!(drifted_identity.batch_id, identity.batch_id);
}

#[test]
fn output_batch_commit_report_with_identity_reports_the_attempted_batch_identity() {
    let symbol = Symbol("BTC-USDT".to_string());
    let mut pending_buffer = PendingOutputBuffer::new(4);
    let mut journal_client = OutputJournalClient::new();
    let mut journal = FailOnSecondAppendJournalOutputAppender::new();

    assert_eq!(pending_buffer.enqueue(request(1)), Ok(()));
    assert_eq!(pending_buffer.enqueue(request(2)), Ok(()));
    assert_eq!(pending_buffer.enqueue(request(3)), Ok(()));

    let report = run_output_batch_commit_step_report_with_identity(
        &symbol,
        &mut journal_client,
        &mut pending_buffer,
        &mut journal,
        10,
    );

    assert_eq!(
        report.batch_identity,
        build_output_batch_identity(
            &symbol,
            MATCHING_OUTPUT_VERSION,
            &[request(1), request(2), request(3)]
        )
    );
    assert_eq!(
        report.commit_report,
        OutputBatchCommitStepReport {
            commit_result: OutputBatchCommitResult {
                committed_count: 1,
                last_committed_seq: Some(JournalSeq(1)),
                committed_seqs: vec![JournalSeq(1)],
            },
            blocking_seq: Some(JournalSeq(2)),
            blocking_outcome: Some(OutputCommitOutcome::Unavailable),
        }
    );

    let entries = journal.read_all();
    let metadata = JournalOutputCommitMetadata::from_output_batch_identity(
        report
            .batch_identity
            .as_ref()
            .expect("attempted batch should have identity"),
    );
    assert_eq!(entries[0].output_commit_metadata, Some(metadata));
}

#[test]
fn output_batch_commit_report_with_identity_rejects_same_batch_id_with_different_digest() {
    let symbol = Symbol("BTC-USDT".to_string());
    let first_requests = vec![request(1), request(2), request(3)];
    let drifted_requests = vec![request(1), request_with_order(2, 800), request(3)];
    let first_identity =
        build_output_batch_identity(&symbol, MATCHING_OUTPUT_VERSION, &first_requests)
            .expect("first batch should have identity");
    let first_metadata = JournalOutputCommitMetadata::from_output_batch_identity(&first_identity);
    let mut pending_buffer = PendingOutputBuffer::new(4);
    let mut journal_client = OutputJournalClient::new();
    let mut journal = InMemoryJournalOutputAppender::new();

    assert_eq!(
        journal_client.commit_one_with_metadata(
            first_requests[0].clone(),
            first_metadata,
            &mut journal
        ),
        OutputCommitOutcome::Accepted
    );
    for request in drifted_requests {
        assert_eq!(pending_buffer.enqueue(request), Ok(()));
    }

    let report = run_output_batch_commit_step_report_with_identity(
        &symbol,
        &mut journal_client,
        &mut pending_buffer,
        &mut journal,
        10,
    );

    assert_eq!(report.commit_report.commit_result.committed_count, 0);
    assert_eq!(report.commit_report.blocking_seq, Some(JournalSeq(1)));
    assert_eq!(
        report.commit_report.blocking_outcome,
        Some(OutputCommitOutcome::Rejected)
    );
    match &report.output_batch_query_status {
        Some(OutputBatchQueryStatus::Conflict(OutputCommitMetadataIndexError::Conflict {
            batch_id,
            existing,
            incoming,
        })) => {
            assert_eq!(batch_id, &existing.batch_id);
            assert_eq!(existing.batch_id, incoming.batch_id);
            assert_ne!(existing.output_digest, incoming.output_digest);
        }
        other => panic!("expected conflicting output batch query status, got {other:?}"),
    }
    assert_eq!(pending_buffer.len(), 3);
    assert_eq!(journal.read_all().len(), 1);
}

#[test]
fn output_batch_commit_report_with_identity_reports_unknown_query_status() {
    let symbol = Symbol("BTC-USDT".to_string());
    let mut pending_buffer = PendingOutputBuffer::new(4);
    let mut journal_client = OutputJournalClient::new();
    let mut journal = UnknownMetadataJournalOutputAppender;

    assert_eq!(pending_buffer.enqueue(request(1)), Ok(()));
    assert_eq!(pending_buffer.enqueue(request(2)), Ok(()));

    let report = run_output_batch_commit_step_report_with_identity(
        &symbol,
        &mut journal_client,
        &mut pending_buffer,
        &mut journal,
        10,
    );

    assert_eq!(report.commit_report.blocking_seq, Some(JournalSeq(1)));
    assert_eq!(
        report.commit_report.blocking_outcome,
        Some(OutputCommitOutcome::Unknown)
    );
    assert_eq!(
        report.output_batch_query_status,
        Some(OutputBatchQueryStatus::Missing)
    );
}

#[test]
fn output_batch_commit_report_with_identity_reports_durable_query_status_after_unknown_commit() {
    let symbol = Symbol("BTC-USDT".to_string());
    let mut pending_buffer = PendingOutputBuffer::new(4);
    let mut journal_client = OutputJournalClient::new();
    let mut journal = DurableUnknownMetadataJournalOutputAppender::new();

    assert_eq!(pending_buffer.enqueue(request(1)), Ok(()));

    let report = run_output_batch_commit_step_report_with_identity(
        &symbol,
        &mut journal_client,
        &mut pending_buffer,
        &mut journal,
        10,
    );

    assert_eq!(report.commit_report.commit_result.committed_count, 1);
    assert_eq!(
        report.commit_report.commit_result.last_committed_seq,
        Some(JournalSeq(1))
    );
    assert_eq!(report.commit_report.blocking_seq, None);
    assert_eq!(report.commit_report.blocking_outcome, None);
    assert_eq!(
        report.output_batch_query_status,
        Some(OutputBatchQueryStatus::Durable)
    );
    assert_eq!(pending_buffer.len(), 0);
    assert_eq!(journal.read_all().len(), 1);
}

#[test]
fn output_journal_client_classifies_commit_outcomes() {
    let mut journal_client = OutputJournalClient::new();

    let mut accepted_journal = InMemoryJournalOutputAppender::new();
    assert_eq!(
        journal_client.commit_one(request(1), &mut accepted_journal),
        OutputCommitOutcome::Accepted
    );

    let mut duplicate_accepted_journal = DurableUnknownOnSecondAppendJournalOutputAppender::new();
    assert_eq!(
        journal_client.commit_one(request(1), &mut duplicate_accepted_journal),
        OutputCommitOutcome::Accepted
    );
    assert_eq!(
        journal_client.commit_one(request(2), &mut duplicate_accepted_journal),
        OutputCommitOutcome::DuplicateAccepted
    );

    let mut unknown_journal = UnknownOnSecondAppendJournalOutputAppender::new();
    assert_eq!(
        journal_client.commit_one(request(1), &mut unknown_journal),
        OutputCommitOutcome::Accepted
    );
    assert_eq!(
        journal_client.commit_one(request(2), &mut unknown_journal),
        OutputCommitOutcome::Unknown
    );

    let mut unavailable_journal = FailOnSecondAppendJournalOutputAppender::new();
    assert_eq!(
        journal_client.commit_one(request(1), &mut unavailable_journal),
        OutputCommitOutcome::Accepted
    );
    assert_eq!(
        journal_client.commit_one(request(2), &mut unavailable_journal),
        OutputCommitOutcome::Unavailable
    );

    let mut rejected_journal = RejectOnSecondAppendJournalOutputAppender::new();
    assert_eq!(
        journal_client.commit_one(request(1), &mut rejected_journal),
        OutputCommitOutcome::Accepted
    );
    assert_eq!(
        journal_client.commit_one(request(2), &mut rejected_journal),
        OutputCommitOutcome::Rejected
    );
}

#[test]
fn output_batch_coordinator_requeues_failed_and_uncommitted_requests() {
    let mut pending_buffer = PendingOutputBuffer::new(4);
    let mut journal_client = OutputJournalClient::new();
    let mut journal = FailOnSecondAppendJournalOutputAppender::new();

    assert_eq!(pending_buffer.enqueue(request(1)), Ok(()));
    assert_eq!(pending_buffer.enqueue(request(2)), Ok(()));
    assert_eq!(pending_buffer.enqueue(request(3)), Ok(()));

    let result =
        run_output_batch_commit_step(&mut journal_client, &mut pending_buffer, &mut journal, 10);

    assert_eq!(result, Err(JournalAdapterError::AppendFailed));
    assert_eq!(journal.read_all().len(), 1);

    let remaining = pending_buffer.drain_batch(10);
    assert_eq!(remaining.len(), 2);
    assert_eq!(remaining[0], request(2));
    assert_eq!(remaining[1], request(3));
    assert_eq!(remaining[0].journal_seq, JournalSeq(2));
    assert_eq!(remaining[1].journal_seq, JournalSeq(3));
}

#[test]
fn output_batch_commit_report_preserves_committed_prefix_when_batch_blocks() {
    let mut pending_buffer = PendingOutputBuffer::new(4);
    let mut journal_client = OutputJournalClient::new();
    let mut journal = FailOnSecondAppendJournalOutputAppender::new();

    assert_eq!(pending_buffer.enqueue(request(1)), Ok(()));
    assert_eq!(pending_buffer.enqueue(request(2)), Ok(()));
    assert_eq!(pending_buffer.enqueue(request(3)), Ok(()));

    let report = run_output_batch_commit_step_report(
        &mut journal_client,
        &mut pending_buffer,
        &mut journal,
        10,
    );

    assert_eq!(
        report,
        OutputBatchCommitStepReport {
            commit_result: OutputBatchCommitResult {
                committed_count: 1,
                last_committed_seq: Some(JournalSeq(1)),
                committed_seqs: vec![JournalSeq(1)],
            },
            blocking_seq: Some(JournalSeq(2)),
            blocking_outcome: Some(OutputCommitOutcome::Unavailable),
        }
    );

    let remaining = pending_buffer.drain_batch(10);
    assert_eq!(remaining.len(), 2);
    assert_eq!(remaining[0], request(2));
    assert_eq!(remaining[1], request(3));
}

#[test]
fn output_commit_retry_tracker_escalates_repeated_unavailable_outcome() {
    let mut retry_tracker = OutputCommitRetryTracker::new(2);
    let report = OutputBatchCommitStepReport {
        commit_result: OutputBatchCommitResult {
            committed_count: 1,
            last_committed_seq: Some(JournalSeq(1)),
            committed_seqs: vec![JournalSeq(1)],
        },
        blocking_seq: Some(JournalSeq(2)),
        blocking_outcome: Some(OutputCommitOutcome::Unavailable),
    };

    assert_eq!(
        retry_tracker.record_blocked_report(&report),
        Some(OutputCommitBlockDecision {
            action: OutputCommitBlockAction::RetryLater,
            blocked_seq: JournalSeq(2),
            outcome: OutputCommitOutcome::Unavailable,
            attempt_count: 1,
        })
    );
    assert_eq!(
        retry_tracker.record_blocked_report(&report),
        Some(OutputCommitBlockDecision {
            action: OutputCommitBlockAction::StopAndEscalate,
            blocked_seq: JournalSeq(2),
            outcome: OutputCommitOutcome::Unavailable,
            attempt_count: 2,
        })
    );
}

#[test]
fn output_commit_retry_tracker_routes_unknown_and_rejected_to_distinct_actions() {
    let mut retry_tracker = OutputCommitRetryTracker::new(3);
    let unknown_report = OutputBatchCommitStepReport {
        commit_result: OutputBatchCommitResult {
            committed_count: 0,
            last_committed_seq: None,
            committed_seqs: vec![],
        },
        blocking_seq: Some(JournalSeq(7)),
        blocking_outcome: Some(OutputCommitOutcome::Unknown),
    };
    let rejected_report = OutputBatchCommitStepReport {
        commit_result: OutputBatchCommitResult {
            committed_count: 0,
            last_committed_seq: None,
            committed_seqs: vec![],
        },
        blocking_seq: Some(JournalSeq(8)),
        blocking_outcome: Some(OutputCommitOutcome::Rejected),
    };

    assert_eq!(
        retry_tracker.record_blocked_report(&unknown_report),
        Some(OutputCommitBlockDecision {
            action: OutputCommitBlockAction::ResolveUnknown,
            blocked_seq: JournalSeq(7),
            outcome: OutputCommitOutcome::Unknown,
            attempt_count: 1,
        })
    );
    assert_eq!(
        retry_tracker.record_blocked_report(&rejected_report),
        Some(OutputCommitBlockDecision {
            action: OutputCommitBlockAction::StopAndEscalate,
            blocked_seq: JournalSeq(8),
            outcome: OutputCommitOutcome::Rejected,
            attempt_count: 1,
        })
    );
}

#[test]
fn output_batch_coordinator_requeues_unknown_outcome_and_uncommitted_requests() {
    let mut pending_buffer = PendingOutputBuffer::new(4);
    let mut journal_client = OutputJournalClient::new();
    let mut journal = UnknownOnSecondAppendJournalOutputAppender::new();

    assert_eq!(pending_buffer.enqueue(request(1)), Ok(()));
    assert_eq!(pending_buffer.enqueue(request(2)), Ok(()));
    assert_eq!(pending_buffer.enqueue(request(3)), Ok(()));

    let result =
        run_output_batch_commit_step(&mut journal_client, &mut pending_buffer, &mut journal, 10);

    assert_eq!(result, Err(JournalAdapterError::CommitOutcomeUnknown));
    assert_eq!(journal.read_all().len(), 1);

    let remaining = pending_buffer.drain_batch(10);
    assert_eq!(remaining.len(), 2);
    assert_eq!(remaining[0], request(2));
    assert_eq!(remaining[1], request(3));
    assert_eq!(remaining[0].journal_seq, JournalSeq(2));
    assert_eq!(remaining[1].journal_seq, JournalSeq(3));
}

#[test]
fn output_batch_coordinator_resolves_unknown_outcome_when_exact_output_is_durable() {
    let mut pending_buffer = PendingOutputBuffer::new(4);
    let mut journal_client = OutputJournalClient::new();
    let mut journal = DurableUnknownOnSecondAppendJournalOutputAppender::new();

    assert_eq!(pending_buffer.enqueue(request(1)), Ok(()));
    assert_eq!(pending_buffer.enqueue(request(2)), Ok(()));
    assert_eq!(pending_buffer.enqueue(request(3)), Ok(()));

    assert_eq!(
        run_output_batch_commit_step(&mut journal_client, &mut pending_buffer, &mut journal, 10),
        Ok(OutputBatchCommitResult {
            committed_count: 3,
            last_committed_seq: Some(JournalSeq(3)),
            committed_seqs: vec![JournalSeq(1), JournalSeq(2), JournalSeq(3)],
        })
    );

    assert!(pending_buffer.is_empty());

    let entries = journal.read_all();
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].journal_seq, JournalSeq(1));
    assert_eq!(entries[1].journal_seq, JournalSeq(2));
    assert_eq!(entries[2].journal_seq, JournalSeq(3));
}

#[test]
fn output_batch_coordinator_requeues_rejected_and_uncommitted_requests() {
    let mut pending_buffer = PendingOutputBuffer::new(4);
    let mut journal_client = OutputJournalClient::new();
    let mut journal = RejectOnSecondAppendJournalOutputAppender::new();

    assert_eq!(pending_buffer.enqueue(request(1)), Ok(()));
    assert_eq!(pending_buffer.enqueue(request(2)), Ok(()));
    assert_eq!(pending_buffer.enqueue(request(3)), Ok(()));

    let result =
        run_output_batch_commit_step(&mut journal_client, &mut pending_buffer, &mut journal, 10);

    assert_eq!(result, Err(JournalAdapterError::AppendRejected));
    assert_eq!(journal.read_all().len(), 1);

    let remaining = pending_buffer.drain_batch(10);
    assert_eq!(remaining.len(), 2);
    assert_eq!(remaining[0], request(2));
    assert_eq!(remaining[1], request(3));
}

#[test]
fn output_batch_coordinator_retry_continues_from_failed_request_in_order() {
    let mut pending_buffer = PendingOutputBuffer::new(4);
    let mut journal_client = OutputJournalClient::new();
    let mut journal = FailOnSecondAppendJournalOutputAppender::new();

    assert_eq!(pending_buffer.enqueue(request(1)), Ok(()));
    assert_eq!(pending_buffer.enqueue(request(2)), Ok(()));
    assert_eq!(pending_buffer.enqueue(request(3)), Ok(()));

    assert_eq!(
        run_output_batch_commit_step(&mut journal_client, &mut pending_buffer, &mut journal, 10),
        Err(JournalAdapterError::AppendFailed)
    );
    assert_eq!(
        run_output_batch_commit_step(&mut journal_client, &mut pending_buffer, &mut journal, 10),
        Ok(OutputBatchCommitResult {
            committed_count: 2,
            last_committed_seq: Some(JournalSeq(3)),
            committed_seqs: vec![JournalSeq(2), JournalSeq(3)],
        })
    );

    let entries = journal.read_all();
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].journal_seq, JournalSeq(1));
    assert_eq!(entries[1].journal_seq, JournalSeq(2));
    assert_eq!(entries[2].journal_seq, JournalSeq(3));
    assert!(pending_buffer.is_empty());
}

#[test]
fn output_batch_coordinator_reports_last_committed_sequence_on_success() {
    let mut pending_buffer = PendingOutputBuffer::new(4);
    let mut journal_client = OutputJournalClient::new();
    let mut journal = InMemoryJournalOutputAppender::new();

    assert_eq!(pending_buffer.enqueue(request(7)), Ok(()));
    assert_eq!(pending_buffer.enqueue(request(8)), Ok(()));

    assert_eq!(
        run_output_batch_commit_step(&mut journal_client, &mut pending_buffer, &mut journal, 10),
        Ok(OutputBatchCommitResult {
            committed_count: 2,
            last_committed_seq: Some(JournalSeq(8)),
            committed_seqs: vec![JournalSeq(7), JournalSeq(8)],
        })
    );
    assert!(pending_buffer.is_empty());
}
