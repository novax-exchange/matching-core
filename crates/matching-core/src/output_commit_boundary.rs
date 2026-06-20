//! Output Commit Boundary component.
//!
//! This component owns the Matching Service side of output append and
//! safe-point confirmation. The submodules here are internal implementation
//! responsibilities from the component design, not separate top-level
//! architecture components.

pub mod output_batch_coordinator;
pub mod output_journal_client;
pub mod pending_output_buffer;

pub use output_batch_coordinator::{run_output_batch_commit_step, OutputBatchCommitResult};
pub use output_journal_client::{OutputCommitRequest, OutputJournalClient};
pub use pending_output_buffer::{PendingOutputBuffer, PendingOutputBufferError};
