use matching_core::engine::{EngineEvent, OrderAck};
use matching_core::journal::{OutputJournal, OutputJournalEntry, OutputJournalError};
use matching_core::output_committer::{OutputCommitRequest, OutputCommitter};
use matching_core::types::{CommandId, JournalSeq, OrderId};

struct TestOutputJournal {
    entries: Vec<OutputJournalEntry>,
}

impl TestOutputJournal {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
}

impl OutputJournal for TestOutputJournal {
    fn append(
        &mut self,
        command_id: CommandId,
        journal_seq: JournalSeq,
        events: Vec<EngineEvent>,
    ) -> Result<(), OutputJournalError> {
        self.entries.push(OutputJournalEntry {
            command_id,
            journal_seq,
            events,
        });

        Ok(())
    }

    fn read_all(&self) -> Vec<OutputJournalEntry> {
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
fn output_committer_is_available_from_public_api() {
    let mut journal = TestOutputJournal::new();
    let mut committer = OutputCommitter::new();

    assert_eq!(
        committer.commit_batch(vec![request(1, 10, 100), request(2, 11, 101)], &mut journal),
        Ok(2)
    );

    let entries = journal.read_all();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].journal_seq, JournalSeq(1));
    assert_eq!(entries[1].journal_seq, JournalSeq(2));
}
