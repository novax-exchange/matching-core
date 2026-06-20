//! Output Commit Boundary: Output Journal Client / append executor.
//!
//! Current scope: append deterministic output requests through the Journal
//! output adapter and classify append results into commit outcomes.

use crate::journal_adapter::{
    JournalAdapterError, JournalOutputAppender, JournalOutputCommitMetadata,
};
use crate::matching_engine::EngineEvent;
use crate::output_commit_boundary::{
    OutputCommitMetadataIndex, OutputCommitMetadataIndexError, OutputCommitMetadataLookup,
};
use crate::types::{CommandId, JournalSeq};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputCommitRequest {
    pub command_id: CommandId,
    pub journal_seq: JournalSeq,
    pub events: Vec<EngineEvent>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputCommitOutcome {
    Accepted,
    DuplicateAccepted,
    Unknown,
    Unavailable,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutputBatchQueryStatus {
    Missing,
    Incomplete {
        observed_entry_count: usize,
        expected_entry_count: usize,
    },
    Durable,
    Conflict(OutputCommitMetadataIndexError),
}

impl OutputCommitOutcome {
    pub fn is_committed(self) -> bool {
        matches!(
            self,
            OutputCommitOutcome::Accepted | OutputCommitOutcome::DuplicateAccepted
        )
    }
}

pub struct OutputJournalClient {
    output_metadata_cache: OutputCommitMetadataIndex,
}

impl OutputJournalClient {
    pub fn new() -> Self {
        Self {
            output_metadata_cache: OutputCommitMetadataIndex::new(),
        }
    }

    pub fn append_one(
        &mut self,
        request: OutputCommitRequest,
        journal: &mut dyn JournalOutputAppender,
    ) -> Result<(), JournalAdapterError> {
        journal.append(request.command_id, request.journal_seq, request.events)
    }

    pub fn append_one_with_metadata(
        &mut self,
        request: OutputCommitRequest,
        metadata: JournalOutputCommitMetadata,
        journal: &mut dyn JournalOutputAppender,
    ) -> Result<(), JournalAdapterError> {
        let result = journal.append_with_output_commit_metadata(
            request.command_id,
            request.journal_seq,
            request.events,
            metadata.clone(),
        );

        if result.is_ok() {
            let _ = self.output_metadata_cache.record(metadata);
        }

        result
    }

    pub fn commit_one(
        &mut self,
        request: OutputCommitRequest,
        journal: &mut dyn JournalOutputAppender,
    ) -> OutputCommitOutcome {
        match self.append_one(request.clone(), journal) {
            Ok(()) => OutputCommitOutcome::Accepted,
            Err(JournalAdapterError::CommitOutcomeUnknown)
                if self.is_request_durable(&request, journal) =>
            {
                OutputCommitOutcome::DuplicateAccepted
            }
            Err(JournalAdapterError::CommitOutcomeUnknown) => OutputCommitOutcome::Unknown,
            Err(JournalAdapterError::AppendFailed) => OutputCommitOutcome::Unavailable,
            Err(JournalAdapterError::AppendRejected) => OutputCommitOutcome::Rejected,
        }
    }

    pub fn commit_one_with_metadata(
        &mut self,
        request: OutputCommitRequest,
        metadata: JournalOutputCommitMetadata,
        journal: &mut dyn JournalOutputAppender,
    ) -> OutputCommitOutcome {
        if self.has_output_batch_metadata_conflict(&metadata, journal) {
            return OutputCommitOutcome::Rejected;
        }

        match self.append_one_with_metadata(request.clone(), metadata.clone(), journal) {
            Ok(()) => OutputCommitOutcome::Accepted,
            Err(JournalAdapterError::CommitOutcomeUnknown)
                if self.is_output_batch_durable(&metadata, journal)
                    || self.is_request_durable(&request, journal) =>
            {
                OutputCommitOutcome::DuplicateAccepted
            }
            Err(JournalAdapterError::CommitOutcomeUnknown) => OutputCommitOutcome::Unknown,
            Err(JournalAdapterError::AppendFailed) => OutputCommitOutcome::Unavailable,
            Err(JournalAdapterError::AppendRejected) => OutputCommitOutcome::Rejected,
        }
    }

    pub fn append_batch(
        &mut self,
        requests: Vec<OutputCommitRequest>,
        journal: &mut dyn JournalOutputAppender,
    ) -> Result<usize, JournalAdapterError> {
        let mut committed = 0;

        for request in requests {
            self.append_one(request, journal)?;
            committed += 1;
        }

        Ok(committed)
    }

    pub fn is_request_durable(
        &self,
        request: &OutputCommitRequest,
        journal: &dyn JournalOutputAppender,
    ) -> bool {
        journal.read_all().into_iter().any(|entry| {
            entry.command_id == request.command_id
                && entry.journal_seq == request.journal_seq
                && entry.events == request.events
        })
    }

    pub fn has_output_batch_metadata_conflict(
        &self,
        metadata: &JournalOutputCommitMetadata,
        journal: &dyn JournalOutputAppender,
    ) -> bool {
        let entries = journal.read_all();
        let Ok(index) = OutputCommitMetadataIndex::rebuild_from_entries(&entries) else {
            return true;
        };
        index
            .get(&metadata.batch_id)
            .is_some_and(|existing| existing != metadata)
    }

    pub fn is_output_batch_durable(
        &self,
        metadata: &JournalOutputCommitMetadata,
        journal: &dyn JournalOutputAppender,
    ) -> bool {
        self.query_output_batch(metadata, journal) == OutputBatchQueryStatus::Durable
    }

    pub fn query_output_batch(
        &self,
        metadata: &JournalOutputCommitMetadata,
        journal: &dyn JournalOutputAppender,
    ) -> OutputBatchQueryStatus {
        let entries = journal.read_all();
        let index = match OutputCommitMetadataIndex::rebuild_from_entries(&entries) {
            Ok(index) => index,
            Err(error) => return OutputBatchQueryStatus::Conflict(error),
        };

        Self::query_output_batch_from_index(metadata, &index)
    }

    pub fn rebuild_output_metadata_cache_from_journal(
        &mut self,
        journal: &dyn JournalOutputAppender,
    ) -> Result<(), OutputCommitMetadataIndexError> {
        let entries = journal.read_all();
        self.output_metadata_cache = OutputCommitMetadataIndex::rebuild_from_entries(&entries)?;
        Ok(())
    }

    pub fn query_cached_output_batch(
        &self,
        metadata: &JournalOutputCommitMetadata,
    ) -> OutputBatchQueryStatus {
        Self::query_output_batch_from_index(metadata, &self.output_metadata_cache)
    }

    fn query_output_batch_from_index(
        metadata: &JournalOutputCommitMetadata,
        index: &OutputCommitMetadataIndex,
    ) -> OutputBatchQueryStatus {
        match index.lookup(&metadata.batch_id) {
            OutputCommitMetadataLookup::Missing => OutputBatchQueryStatus::Missing,
            OutputCommitMetadataLookup::Incomplete {
                metadata: existing,
                observed_entry_count,
            } if existing == *metadata => OutputBatchQueryStatus::Incomplete {
                observed_entry_count,
                expected_entry_count: metadata.entry_count,
            },
            OutputCommitMetadataLookup::Complete { metadata: existing }
                if existing == *metadata =>
            {
                OutputBatchQueryStatus::Durable
            }
            OutputCommitMetadataLookup::Incomplete {
                metadata: existing, ..
            }
            | OutputCommitMetadataLookup::Complete { metadata: existing } => {
                OutputBatchQueryStatus::Conflict(OutputCommitMetadataIndexError::Conflict {
                    batch_id: metadata.batch_id.clone(),
                    existing,
                    incoming: metadata.clone(),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal_adapter::{JournalAdapterError, JournalOutputAppender, JournalOutputEntry};
    use crate::matching_engine::{EngineEvent, OrderAck};
    use crate::types::{CommandId, JournalSeq, OrderId};

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
    fn output_journal_client_appends_output_requests_in_order() {
        let mut journal = InMemoryJournalOutputAppender::new();
        let mut journal_client = OutputJournalClient::new();

        let requests = vec![request(1, 10, 100), request(2, 11, 101)];

        assert_eq!(journal_client.append_batch(requests, &mut journal), Ok(2));

        let entries = journal.read_all();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].journal_seq, JournalSeq(1));
        assert_eq!(entries[1].journal_seq, JournalSeq(2));
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
    fn output_journal_client_stops_at_first_append_failure() {
        let mut journal = FailOnSecondAppendJournalOutputAppender::new();
        let mut journal_client = OutputJournalClient::new();

        let requests = vec![
            request(1, 10, 100),
            request(2, 11, 101),
            request(3, 12, 102),
        ];

        assert_eq!(
            journal_client.append_batch(requests, &mut journal),
            Err(JournalAdapterError::AppendFailed)
        );

        let entries = journal.read_all();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].journal_seq, JournalSeq(1));
    }
}
