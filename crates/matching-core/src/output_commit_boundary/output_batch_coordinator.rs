//! Output Commit Boundary: Output Batch Coordinator.
//!
//! Current scope: drain pending output requests, commit them in order, build
//! stable batch identity for commit attempts, and requeue unresolved,
//! unavailable, or rejected requests plus the uncommitted tail for a later
//! retry or resolution step.

use super::output_batch_identity::{
    build_output_batch_identity, OutputBatchIdentity, MATCHING_OUTPUT_VERSION,
};
use super::output_journal_client::{
    OutputBatchQueryStatus, OutputCommitOutcome, OutputJournalClient,
};
use super::pending_output_buffer::PendingOutputBuffer;
use crate::journal_adapter::{
    JournalAdapterError, JournalOutputAppender, JournalOutputCommitMetadata,
};
use crate::runtime_config::RuntimeShardId;
use crate::types::{JournalSeq, Symbol};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputBatchCommitResult {
    pub committed_count: usize,
    pub last_committed_seq: Option<JournalSeq>,
    pub committed_seqs: Vec<JournalSeq>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputBatchCommitStepReport {
    pub commit_result: OutputBatchCommitResult,
    pub blocking_seq: Option<JournalSeq>,
    pub blocking_outcome: Option<OutputCommitOutcome>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputBatchCommitStepReportWithIdentity {
    pub batch_identity: Option<OutputBatchIdentity>,
    pub output_batch_query_status: Option<OutputBatchQueryStatus>,
    pub commit_report: OutputBatchCommitStepReport,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutputBatchCommitMetadataContext {
    pub shard_id: RuntimeShardId,
    pub shard_sequence: u64,
}

struct OutputBatchCommitStepExecutionReport {
    commit_report: OutputBatchCommitStepReport,
    duplicate_accepted_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputCommitBlockAction {
    RetryLater,
    ResolveUnknown,
    StopAndEscalate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutputCommitBlockDecision {
    pub action: OutputCommitBlockAction,
    pub blocked_seq: JournalSeq,
    pub outcome: OutputCommitOutcome,
    pub attempt_count: usize,
}

pub struct OutputCommitRetryTracker {
    max_unavailable_attempts: usize,
    unavailable_attempts: HashMap<JournalSeq, usize>,
}

impl OutputCommitRetryTracker {
    pub fn new(max_unavailable_attempts: usize) -> Self {
        Self {
            max_unavailable_attempts,
            unavailable_attempts: HashMap::new(),
        }
    }

    pub fn record_blocked_report(
        &mut self,
        report: &OutputBatchCommitStepReport,
    ) -> Option<OutputCommitBlockDecision> {
        for committed_seq in &report.commit_result.committed_seqs {
            self.unavailable_attempts.remove(committed_seq);
        }

        let blocked_seq = report.blocking_seq?;
        let outcome = report.blocking_outcome?;

        match outcome {
            OutputCommitOutcome::Unavailable => {
                let attempt_count = self.record_unavailable_attempt(blocked_seq);
                let action = if attempt_count >= self.max_unavailable_attempts {
                    OutputCommitBlockAction::StopAndEscalate
                } else {
                    OutputCommitBlockAction::RetryLater
                };

                Some(OutputCommitBlockDecision {
                    action,
                    blocked_seq,
                    outcome,
                    attempt_count,
                })
            }
            OutputCommitOutcome::Unknown => Some(OutputCommitBlockDecision {
                action: OutputCommitBlockAction::ResolveUnknown,
                blocked_seq,
                outcome,
                attempt_count: 1,
            }),
            OutputCommitOutcome::Rejected => Some(OutputCommitBlockDecision {
                action: OutputCommitBlockAction::StopAndEscalate,
                blocked_seq,
                outcome,
                attempt_count: 1,
            }),
            OutputCommitOutcome::Accepted | OutputCommitOutcome::DuplicateAccepted => None,
        }
    }

    fn record_unavailable_attempt(&mut self, blocked_seq: JournalSeq) -> usize {
        let attempt_count = self.unavailable_attempts.entry(blocked_seq).or_insert(0);
        *attempt_count += 1;
        *attempt_count
    }
}

pub fn run_output_batch_commit_step(
    journal_client: &mut OutputJournalClient,
    pending_buffer: &mut PendingOutputBuffer,
    journal: &mut dyn JournalOutputAppender,
    max_requests: usize,
) -> Result<OutputBatchCommitResult, JournalAdapterError> {
    let report =
        run_output_batch_commit_step_report(journal_client, pending_buffer, journal, max_requests);

    match report.blocking_outcome {
        Some(outcome) => Err(error_for_outcome(outcome)),
        None => Ok(report.commit_result),
    }
}

pub fn run_output_batch_commit_step_report(
    journal_client: &mut OutputJournalClient,
    pending_buffer: &mut PendingOutputBuffer,
    journal: &mut dyn JournalOutputAppender,
    max_requests: usize,
) -> OutputBatchCommitStepReport {
    let requests = pending_buffer.drain_batch(max_requests);
    run_output_batch_commit_step_report_for_requests(
        journal_client,
        pending_buffer,
        journal,
        requests,
        None,
    )
    .commit_report
}

pub fn run_output_batch_commit_step_report_with_identity(
    symbol: &Symbol,
    journal_client: &mut OutputJournalClient,
    pending_buffer: &mut PendingOutputBuffer,
    journal: &mut dyn JournalOutputAppender,
    max_requests: usize,
) -> OutputBatchCommitStepReportWithIdentity {
    run_output_batch_commit_step_report_with_identity_and_metadata_context(
        symbol,
        journal_client,
        pending_buffer,
        journal,
        max_requests,
        None,
    )
}

pub fn run_output_batch_commit_step_report_with_identity_and_metadata_context(
    symbol: &Symbol,
    journal_client: &mut OutputJournalClient,
    pending_buffer: &mut PendingOutputBuffer,
    journal: &mut dyn JournalOutputAppender,
    max_requests: usize,
    metadata_context: Option<OutputBatchCommitMetadataContext>,
) -> OutputBatchCommitStepReportWithIdentity {
    let requests = pending_buffer.drain_batch(max_requests);
    let batch_identity = build_output_batch_identity(symbol, MATCHING_OUTPUT_VERSION, &requests);
    let output_commit_metadata = batch_identity
        .as_ref()
        .map(|identity| output_commit_metadata_from_identity(identity, metadata_context));
    let execution_report = run_output_batch_commit_step_report_for_requests(
        journal_client,
        pending_buffer,
        journal,
        requests,
        output_commit_metadata,
    );
    let output_batch_query_status = match (
        &batch_identity,
        &execution_report.commit_report.blocking_outcome,
        execution_report.duplicate_accepted_count,
    ) {
        (Some(identity), Some(OutputCommitOutcome::Unknown), _) => {
            let metadata = output_commit_metadata_from_identity(identity, metadata_context);
            Some(journal_client.query_output_batch(&metadata, journal))
        }
        (Some(identity), Some(OutputCommitOutcome::Rejected), _) => {
            let metadata = output_commit_metadata_from_identity(identity, metadata_context);
            match journal_client.query_output_batch(&metadata, journal) {
                OutputBatchQueryStatus::Conflict(error) => {
                    Some(OutputBatchQueryStatus::Conflict(error))
                }
                _ => None,
            }
        }
        (Some(identity), None, duplicate_accepted_count) if duplicate_accepted_count > 0 => {
            let metadata = output_commit_metadata_from_identity(identity, metadata_context);
            Some(journal_client.query_output_batch(&metadata, journal))
        }
        _ => None,
    };

    OutputBatchCommitStepReportWithIdentity {
        batch_identity,
        output_batch_query_status,
        commit_report: execution_report.commit_report,
    }
}

fn output_commit_metadata_from_identity(
    identity: &OutputBatchIdentity,
    metadata_context: Option<OutputBatchCommitMetadataContext>,
) -> JournalOutputCommitMetadata {
    let mut metadata = JournalOutputCommitMetadata::from_output_batch_identity(identity);

    if let Some(metadata_context) = metadata_context {
        metadata.shard_id = Some(metadata_context.shard_id);
        metadata.shard_sequence = Some(metadata_context.shard_sequence);
    }

    metadata
}

fn run_output_batch_commit_step_report_for_requests(
    journal_client: &mut OutputJournalClient,
    pending_buffer: &mut PendingOutputBuffer,
    journal: &mut dyn JournalOutputAppender,
    requests: Vec<super::output_journal_client::OutputCommitRequest>,
    output_commit_metadata: Option<JournalOutputCommitMetadata>,
) -> OutputBatchCommitStepExecutionReport {
    let mut remaining = requests.into_iter();
    let mut committed_count = 0;
    let mut last_committed_seq = None;
    let mut committed_seqs = Vec::new();
    let mut duplicate_accepted_count = 0;

    while let Some(request) = remaining.next() {
        let journal_seq = request.journal_seq;
        let outcome = match &output_commit_metadata {
            Some(metadata) => {
                journal_client.commit_one_with_metadata(request.clone(), metadata.clone(), journal)
            }
            None => journal_client.commit_one(request.clone(), journal),
        };

        match outcome {
            outcome if outcome.is_committed() => {
                if outcome == OutputCommitOutcome::DuplicateAccepted {
                    duplicate_accepted_count += 1;
                }

                committed_count += 1;
                last_committed_seq = Some(journal_seq);
                committed_seqs.push(journal_seq);
            }
            outcome => {
                let mut to_prepend = vec![request];
                to_prepend.extend(remaining);
                pending_buffer.prepend_requests(to_prepend);
                return OutputBatchCommitStepExecutionReport {
                    commit_report: OutputBatchCommitStepReport {
                        commit_result: OutputBatchCommitResult {
                            committed_count,
                            last_committed_seq,
                            committed_seqs,
                        },
                        blocking_seq: Some(journal_seq),
                        blocking_outcome: Some(outcome),
                    },
                    duplicate_accepted_count,
                };
            }
        }
    }

    OutputBatchCommitStepExecutionReport {
        commit_report: OutputBatchCommitStepReport {
            commit_result: OutputBatchCommitResult {
                committed_count,
                last_committed_seq,
                committed_seqs,
            },
            blocking_seq: None,
            blocking_outcome: None,
        },
        duplicate_accepted_count,
    }
}

fn error_for_outcome(outcome: OutputCommitOutcome) -> JournalAdapterError {
    match outcome {
        OutputCommitOutcome::Accepted | OutputCommitOutcome::DuplicateAccepted => {
            unreachable!("committed output outcomes are handled before error conversion")
        }
        OutputCommitOutcome::Unknown => JournalAdapterError::CommitOutcomeUnknown,
        OutputCommitOutcome::Unavailable => JournalAdapterError::AppendFailed,
        OutputCommitOutcome::Rejected => JournalAdapterError::AppendRejected,
    }
}
