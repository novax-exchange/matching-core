use matching_core::engine::{EngineEvent, OrderAck};
use matching_core::journal_adapter::{
    JournalAdapterError, JournalOutputAppender, JournalOutputEntry,
};
use matching_core::output_commit_loop::run_output_commit_step;
use matching_core::output_committer::{OutputCommitRequest, OutputCommitter};
use matching_core::output_queue::OutputQueue;
use matching_core::types::{CommandId, JournalSeq, OrderId};

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
fn output_commit_loop_requeues_failed_and_uncommitted_requests() {
    let mut queue = OutputQueue::new(4);
    let mut committer = OutputCommitter::new();
    let mut journal = FailOnSecondAppendJournalOutputAppender::new();

    assert_eq!(queue.enqueue(request(1)), Ok(()));
    assert_eq!(queue.enqueue(request(2)), Ok(()));
    assert_eq!(queue.enqueue(request(3)), Ok(()));

    let result = run_output_commit_step(&mut committer, &mut queue, &mut journal, 10);

    assert_eq!(result, Err(JournalAdapterError::AppendFailed));
    assert_eq!(journal.read_all().len(), 1);

    let remaining = queue.drain_batch(10);
    assert_eq!(remaining.len(), 2);
    assert_eq!(remaining[0].journal_seq, JournalSeq(2));
    assert_eq!(remaining[1].journal_seq, JournalSeq(3));
}
