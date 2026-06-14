use crate::engine::EngineEvent;
use crate::journal::{OutputJournal, OutputJournalError};
use crate::types::{CommandId, JournalSeq};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputCommitRequest {
    pub command_id: CommandId,
    pub journal_seq: JournalSeq,
    pub events: Vec<EngineEvent>,
}

pub struct OutputCommitter;

impl OutputCommitter {
    pub fn new() -> Self {
        Self
    }

    pub fn commit_one(
        &mut self,
        request: OutputCommitRequest,
        journal: &mut dyn OutputJournal,
    ) -> Result<(), OutputJournalError> {
        journal.append(request.command_id, request.journal_seq, request.events)
    }

    pub fn commit_batch(
        &mut self,
        requests: Vec<OutputCommitRequest>,
        journal: &mut dyn OutputJournal,
    ) -> Result<usize, OutputJournalError> {
        let mut committed = 0;

        for request in requests {
            self.commit_one(request, journal)?;
            committed += 1;
        }

        Ok(committed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{EngineEvent, OrderAck};
    use crate::journal::{OutputJournal, OutputJournalEntry, OutputJournalError};
    use crate::types::{CommandId, JournalSeq, OrderId};

    struct InMemoryOutputJournal {
        entries: Vec<OutputJournalEntry>,
    }

    impl InMemoryOutputJournal {
        fn new() -> Self {
            Self {
                entries: Vec::new(),
            }
        }
    }

    impl OutputJournal for InMemoryOutputJournal {
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
    fn committer_appends_output_requests_in_order() {
        let mut journal = InMemoryOutputJournal::new();
        let mut committer = OutputCommitter::new();

        let requests = vec![request(1, 10, 100), request(2, 11, 101)];

        assert_eq!(committer.commit_batch(requests, &mut journal), Ok(2));

        let entries = journal.read_all();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].journal_seq, JournalSeq(1));
        assert_eq!(entries[1].journal_seq, JournalSeq(2));
    }

    struct FailOnSecondAppendOutputJournal {
        entries: Vec<OutputJournalEntry>,
        append_count: usize,
    }

    impl FailOnSecondAppendOutputJournal {
        fn new() -> Self {
            Self {
                entries: Vec::new(),
                append_count: 0,
            }
        }
    }

    impl OutputJournal for FailOnSecondAppendOutputJournal {
        fn append(
            &mut self,
            command_id: CommandId,
            journal_seq: JournalSeq,
            events: Vec<EngineEvent>,
        ) -> Result<(), OutputJournalError> {
            self.append_count += 1;

            if self.append_count == 2 {
                return Err(OutputJournalError::AppendFailed);
            }

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

    #[test]
    fn committer_stops_at_first_append_failure() {
        let mut journal = FailOnSecondAppendOutputJournal::new();
        let mut committer = OutputCommitter::new();

        let requests = vec![
            request(1, 10, 100),
            request(2, 11, 101),
            request(3, 12, 102),
        ];

        assert_eq!(
            committer.commit_batch(requests, &mut journal),
            Err(OutputJournalError::AppendFailed)
        );

        let entries = journal.read_all();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].journal_seq, JournalSeq(1));
    }
}
