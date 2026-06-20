//! Output Commit Boundary component.
//!
//! This component owns the Matching Service side of output append and
//! safe-point confirmation. The submodules here are internal implementation
//! responsibilities from the component design, not separate top-level
//! architecture components.

pub mod output_batch_coordinator;
pub mod output_batch_identity;
pub mod output_commit_metadata_index;
pub mod output_journal_client;
pub mod pending_output_buffer;

pub use output_batch_coordinator::{
    run_output_batch_commit_step, run_output_batch_commit_step_report,
    run_output_batch_commit_step_report_with_identity, OutputBatchCommitResult,
    OutputBatchCommitStepReport, OutputBatchCommitStepReportWithIdentity, OutputCommitBlockAction,
    OutputCommitBlockDecision, OutputCommitRetryTracker,
};
pub use output_batch_identity::{
    build_output_batch_identity, digest_journal_output_entries, OutputBatchId, OutputBatchIdentity,
    OutputDigest, MATCHING_OUTPUT_VERSION,
};
pub use output_commit_metadata_index::{
    OutputCommitMetadataIndex, OutputCommitMetadataIndexError, OutputCommitMetadataLookup,
};
pub use output_journal_client::{
    OutputBatchQueryStatus, OutputCommitOutcome, OutputCommitRequest, OutputJournalClient,
};
pub use pending_output_buffer::{PendingOutputBuffer, PendingOutputBufferError};
