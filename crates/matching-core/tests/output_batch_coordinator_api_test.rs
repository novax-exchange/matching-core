use matching_core::journal_adapter::{
    JournalAdapterError, JournalOutputAppender, JournalOutputEntry,
};
use matching_core::matching_engine::{EngineEvent, OrderAck};
use matching_core::output_commit_boundary::PendingOutputBuffer;
use matching_core::output_commit_boundary::{
    run_output_batch_commit_step, OutputBatchCommitResult,
};
use matching_core::output_commit_boundary::{OutputCommitRequest, OutputJournalClient};
use matching_core::types::{CommandId, JournalSeq, OrderId};

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
    assert_eq!(remaining[0].journal_seq, JournalSeq(2));
    assert_eq!(remaining[1].journal_seq, JournalSeq(3));
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
