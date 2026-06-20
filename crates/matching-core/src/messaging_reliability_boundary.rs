//! Messaging Reliability Boundary component.
//!
//! Architecture status: the current implementation has pieces of this boundary
//! in `ConfirmedInputConsumer`, `BoundedHandoff`, and the Output Commit
//! Boundary, but not a complete shared reliability boundary yet.
//!
//! TODO: make envelope validation, offset tracking, deduplication, replay /
//! backfill, reliable publication state, retry, and dead-letter behavior
//! explicit under this boundary as the implementation grows.
