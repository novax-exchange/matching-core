use matching_core::journal_adapter::{
    JournalAdapterError, JournalOutputAppender, JournalOutputCommitMetadata, JournalOutputEntry,
};
use matching_core::matching_engine::{EngineEvent, OrderAck};
use matching_core::output_commit_boundary::{
    build_output_batch_identity, OutputBatchQueryStatus, OutputCommitMetadataIndexError,
    OutputCommitOutcome, OutputCommitRequest, OutputJournalClient, MATCHING_OUTPUT_VERSION,
};
use matching_core::types::{CommandId, JournalSeq, OrderId, Symbol};

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

struct UnknownJournalOutputAppender {
    entries: Vec<JournalOutputEntry>,
}

impl UnknownJournalOutputAppender {
    fn with_entries(entries: Vec<JournalOutputEntry>) -> Self {
        Self { entries }
    }
}

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
        self.entries.clone()
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

fn request(seq: u64, command_id: u64, order_id: u64) -> OutputCommitRequest {
    OutputCommitRequest {
        command_id: CommandId(command_id),
        journal_seq: JournalSeq(seq),
        events: vec![EngineEvent::OrderAck(OrderAck::Accepted {
            command_id: CommandId(command_id),
            order_id: OrderId(order_id),
            journal_seq: JournalSeq(seq),
        })],
    }
}

fn request_with_order(seq: u64, command_id: u64, order_id: u64) -> OutputCommitRequest {
    request(seq, command_id, order_id)
}

fn durable_entry(
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

#[test]
fn output_journal_client_is_available_from_public_api() {
    let mut journal = TestJournalOutputAppender::new();
    let mut journal_client = OutputJournalClient::new();

    assert_eq!(
        journal_client.append_batch(vec![request(1, 10, 100), request(2, 11, 101)], &mut journal),
        Ok(2)
    );

    let entries = journal.read_all();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].journal_seq, JournalSeq(1));
    assert_eq!(entries[1].journal_seq, JournalSeq(2));
}

#[test]
fn output_journal_client_can_append_output_with_batch_identity_metadata_from_public_api() {
    let symbol = Symbol("BTC-USDT".to_string());
    let requests = vec![request(1, 10, 100), request(2, 11, 101)];
    let identity = build_output_batch_identity(&symbol, MATCHING_OUTPUT_VERSION, &requests)
        .expect("non-empty request batch should have identity");
    let metadata = JournalOutputCommitMetadata::from_output_batch_identity(&identity);
    let mut journal = TestJournalOutputAppender::new();
    let mut journal_client = OutputJournalClient::new();

    assert_eq!(
        journal_client.append_one_with_metadata(
            requests[0].clone(),
            metadata.clone(),
            &mut journal
        ),
        Ok(())
    );

    let entries = journal.read_all();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].output_commit_metadata, Some(metadata));
}

#[test]
fn output_journal_client_rejects_same_batch_id_with_different_digest_from_public_api() {
    let symbol = Symbol("BTC-USDT".to_string());
    let first_requests = vec![request(1, 10, 100), request(2, 11, 101)];
    let drifted_requests = vec![request(1, 10, 100), request_with_order(2, 11, 999)];
    let first_identity =
        build_output_batch_identity(&symbol, MATCHING_OUTPUT_VERSION, &first_requests)
            .expect("first batch should have identity");
    let drifted_identity =
        build_output_batch_identity(&symbol, MATCHING_OUTPUT_VERSION, &drifted_requests)
            .expect("drifted batch should have identity");
    let first_metadata = JournalOutputCommitMetadata::from_output_batch_identity(&first_identity);
    let drifted_metadata =
        JournalOutputCommitMetadata::from_output_batch_identity(&drifted_identity);
    let mut journal = TestJournalOutputAppender::new();
    let mut journal_client = OutputJournalClient::new();

    assert_eq!(first_metadata.batch_id, drifted_metadata.batch_id);
    assert_ne!(first_metadata.output_digest, drifted_metadata.output_digest);
    assert_eq!(
        journal_client.commit_one_with_metadata(
            first_requests[0].clone(),
            first_metadata,
            &mut journal
        ),
        OutputCommitOutcome::Accepted
    );
    assert_eq!(
        journal_client.commit_one_with_metadata(
            drifted_requests[0].clone(),
            drifted_metadata,
            &mut journal
        ),
        OutputCommitOutcome::Rejected
    );

    assert_eq!(journal.read_all().len(), 1);
}

#[test]
fn output_journal_client_resolves_unknown_when_complete_batch_metadata_is_durable_from_public_api()
{
    let symbol = Symbol("BTC-USDT".to_string());
    let requests = vec![request(1, 10, 100), request(2, 11, 101)];
    let identity = build_output_batch_identity(&symbol, MATCHING_OUTPUT_VERSION, &requests)
        .expect("batch should have identity");
    let metadata = JournalOutputCommitMetadata::from_output_batch_identity(&identity);
    let durable_entries = vec![
        durable_entry(requests[0].clone(), metadata.clone()),
        durable_entry(requests[1].clone(), metadata.clone()),
    ];
    let mut journal = UnknownJournalOutputAppender::with_entries(durable_entries);
    let mut journal_client = OutputJournalClient::new();

    assert_eq!(
        journal_client.commit_one_with_metadata(requests[0].clone(), metadata, &mut journal),
        OutputCommitOutcome::DuplicateAccepted
    );
}

#[test]
fn output_journal_client_checks_complete_batch_metadata_durability_from_public_api() {
    let symbol = Symbol("BTC-USDT".to_string());
    let requests = vec![request(1, 10, 100), request(2, 11, 101)];
    let identity = build_output_batch_identity(&symbol, MATCHING_OUTPUT_VERSION, &requests)
        .expect("batch should have identity");
    let metadata = JournalOutputCommitMetadata::from_output_batch_identity(&identity);
    let incomplete_entries = vec![durable_entry(requests[0].clone(), metadata.clone())];
    let complete_entries = vec![
        durable_entry(requests[0].clone(), metadata.clone()),
        durable_entry(requests[1].clone(), metadata.clone()),
    ];
    let incomplete_journal = UnknownJournalOutputAppender::with_entries(incomplete_entries);
    let complete_journal = UnknownJournalOutputAppender::with_entries(complete_entries);
    let journal_client = OutputJournalClient::new();

    assert!(!journal_client.is_output_batch_durable(&metadata, &incomplete_journal));
    assert!(journal_client.is_output_batch_durable(&metadata, &complete_journal));
    assert_eq!(
        journal_client.query_output_batch(&metadata, &incomplete_journal),
        OutputBatchQueryStatus::Incomplete {
            observed_entry_count: 1,
            expected_entry_count: 2,
        }
    );
    assert_eq!(
        journal_client.query_output_batch(&metadata, &complete_journal),
        OutputBatchQueryStatus::Durable
    );
}

#[test]
fn output_journal_client_reports_missing_batch_metadata_from_public_api() {
    let symbol = Symbol("BTC-USDT".to_string());
    let requests = vec![request(1, 10, 100), request(2, 11, 101)];
    let identity = build_output_batch_identity(&symbol, MATCHING_OUTPUT_VERSION, &requests)
        .expect("batch should have identity");
    let metadata = JournalOutputCommitMetadata::from_output_batch_identity(&identity);
    let journal = UnknownJournalOutputAppender::with_entries(Vec::new());
    let journal_client = OutputJournalClient::new();

    assert_eq!(
        journal_client.query_output_batch(&metadata, &journal),
        OutputBatchQueryStatus::Missing
    );
}

#[test]
fn output_journal_client_can_rebuild_and_query_cached_batch_metadata_from_public_api() {
    let symbol = Symbol("BTC-USDT".to_string());
    let requests = vec![request(1, 10, 100), request(2, 11, 101)];
    let identity = build_output_batch_identity(&symbol, MATCHING_OUTPUT_VERSION, &requests)
        .expect("batch should have identity");
    let metadata = JournalOutputCommitMetadata::from_output_batch_identity(&identity);
    let complete_entries = vec![
        durable_entry(requests[0].clone(), metadata.clone()),
        durable_entry(requests[1].clone(), metadata.clone()),
    ];
    let journal = UnknownJournalOutputAppender::with_entries(complete_entries);
    let mut journal_client = OutputJournalClient::new();

    assert_eq!(
        journal_client.query_cached_output_batch(&metadata),
        OutputBatchQueryStatus::Missing
    );
    assert_eq!(
        journal_client.rebuild_output_metadata_cache_from_journal(&journal),
        Ok(())
    );
    assert_eq!(
        journal_client.query_cached_output_batch(&metadata),
        OutputBatchQueryStatus::Durable
    );
}

#[test]
fn output_journal_client_records_successful_output_metadata_append_in_cache_from_public_api() {
    let symbol = Symbol("BTC-USDT".to_string());
    let requests = vec![request(1, 10, 100)];
    let identity = build_output_batch_identity(&symbol, MATCHING_OUTPUT_VERSION, &requests)
        .expect("batch should have identity");
    let metadata = JournalOutputCommitMetadata::from_output_batch_identity(&identity);
    let mut journal = TestJournalOutputAppender::new();
    let mut journal_client = OutputJournalClient::new();

    assert_eq!(
        journal_client.commit_one_with_metadata(
            requests[0].clone(),
            metadata.clone(),
            &mut journal
        ),
        OutputCommitOutcome::Accepted
    );
    assert_eq!(
        journal_client.query_cached_output_batch(&metadata),
        OutputBatchQueryStatus::Durable
    );
}

#[test]
fn output_journal_client_keeps_existing_cache_when_metadata_rebuild_conflicts_from_public_api() {
    let symbol = Symbol("BTC-USDT".to_string());
    let requests = vec![request(1, 10, 100)];
    let drifted_requests = vec![request_with_order(1, 10, 999)];
    let identity = build_output_batch_identity(&symbol, MATCHING_OUTPUT_VERSION, &requests)
        .expect("batch should have identity");
    let drifted_identity =
        build_output_batch_identity(&symbol, MATCHING_OUTPUT_VERSION, &drifted_requests)
            .expect("drifted batch should have identity");
    let metadata = JournalOutputCommitMetadata::from_output_batch_identity(&identity);
    let drifted_metadata =
        JournalOutputCommitMetadata::from_output_batch_identity(&drifted_identity);
    let good_journal = UnknownJournalOutputAppender::with_entries(vec![durable_entry(
        requests[0].clone(),
        metadata.clone(),
    )]);
    let conflicting_journal = UnknownJournalOutputAppender::with_entries(vec![
        durable_entry(requests[0].clone(), metadata.clone()),
        durable_entry(drifted_requests[0].clone(), drifted_metadata),
    ]);
    let mut journal_client = OutputJournalClient::new();

    assert_eq!(
        journal_client.rebuild_output_metadata_cache_from_journal(&good_journal),
        Ok(())
    );
    assert_eq!(
        journal_client.query_cached_output_batch(&metadata),
        OutputBatchQueryStatus::Durable
    );
    assert!(matches!(
        journal_client.rebuild_output_metadata_cache_from_journal(&conflicting_journal),
        Err(OutputCommitMetadataIndexError::Conflict { .. })
    ));
    assert_eq!(
        journal_client.query_cached_output_batch(&metadata),
        OutputBatchQueryStatus::Durable
    );
}
