//! Output Commit Boundary: Output Batch Coordinator.
//!
//! Current scope: drain pending output requests, commit them in order, and
//! requeue the failed request plus uncommitted tail for a later retry step.
//! TODO: split explicit Retry / Query Resolver behavior when Journal append
//! can return Unknown, Unavailable, DuplicateAccepted, or Rejected outcomes.

use super::output_journal_client::OutputJournalClient;
use super::pending_output_buffer::PendingOutputBuffer;
use crate::journal_adapter::{JournalAdapterError, JournalOutputAppender};
use crate::types::JournalSeq;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputBatchCommitResult {
    pub committed_count: usize,
    pub last_committed_seq: Option<JournalSeq>,
    pub committed_seqs: Vec<JournalSeq>,
}

pub fn run_output_batch_commit_step(
    journal_client: &mut OutputJournalClient,
    pending_buffer: &mut PendingOutputBuffer,
    journal: &mut dyn JournalOutputAppender,
    max_requests: usize,
) -> Result<OutputBatchCommitResult, JournalAdapterError> {
    let requests = pending_buffer.drain_batch(max_requests);
    let mut remaining = requests.into_iter();
    let mut committed_count = 0;
    let mut last_committed_seq = None;
    let mut committed_seqs = Vec::new();

    while let Some(request) = remaining.next() {
        let journal_seq = request.journal_seq;
        match journal_client.append_one(request.clone(), journal) {
            Ok(()) => {
                committed_count += 1;
                last_committed_seq = Some(journal_seq);
                committed_seqs.push(journal_seq);
            }
            Err(error) => {
                let mut to_prepend = vec![request];
                to_prepend.extend(remaining);
                pending_buffer.prepend_requests(to_prepend);
                return Err(error);
            }
        }
    }

    Ok(OutputBatchCommitResult {
        committed_count,
        last_committed_seq,
        committed_seqs,
    })
}
