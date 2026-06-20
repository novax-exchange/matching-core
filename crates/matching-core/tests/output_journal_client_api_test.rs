use matching_core::journal_adapter::{
    JournalAdapterError, JournalOutputAppender, JournalOutputEntry,
};
use matching_core::matching_engine::{EngineEvent, OrderAck};
use matching_core::output_commit_boundary::{OutputCommitRequest, OutputJournalClient};
use matching_core::types::{CommandId, JournalSeq, OrderId};

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
