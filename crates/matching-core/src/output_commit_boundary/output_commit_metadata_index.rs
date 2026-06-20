//! Output Commit Boundary: rebuildable output commit metadata index.
//!
//! The Journal output log remains the source of truth. This index is a
//! disposable lookup layer for recent or rebuilt output commit metadata.

use crate::journal_adapter::{JournalOutputCommitMetadata, JournalOutputEntry};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutputCommitMetadataIndexError {
    Conflict {
        batch_id: String,
        existing: JournalOutputCommitMetadata,
        incoming: JournalOutputCommitMetadata,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutputCommitMetadataLookup {
    Missing,
    Incomplete {
        metadata: JournalOutputCommitMetadata,
        observed_entry_count: usize,
    },
    Complete {
        metadata: JournalOutputCommitMetadata,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OutputCommitMetadataIndex {
    by_batch_id: HashMap<String, JournalOutputCommitMetadata>,
    observed_entry_counts: HashMap<String, usize>,
}

impl OutputCommitMetadataIndex {
    pub fn new() -> Self {
        Self {
            by_batch_id: HashMap::new(),
            observed_entry_counts: HashMap::new(),
        }
    }

    pub fn rebuild_from_entries(
        entries: &[JournalOutputEntry],
    ) -> Result<Self, OutputCommitMetadataIndexError> {
        let mut index = Self::new();

        for entry in entries {
            if let Some(metadata) = &entry.output_commit_metadata {
                index.record(metadata.clone())?;
            }
        }

        Ok(index)
    }

    pub fn record(
        &mut self,
        metadata: JournalOutputCommitMetadata,
    ) -> Result<(), OutputCommitMetadataIndexError> {
        let batch_id = metadata.batch_id.clone();

        if let Some(existing) = self.by_batch_id.get(&batch_id) {
            if existing != &metadata {
                return Err(OutputCommitMetadataIndexError::Conflict {
                    batch_id,
                    existing: existing.clone(),
                    incoming: metadata,
                });
            }
        } else {
            self.by_batch_id.insert(batch_id.clone(), metadata);
        }

        *self.observed_entry_counts.entry(batch_id).or_insert(0) += 1;
        Ok(())
    }

    pub fn get(&self, batch_id: &str) -> Option<&JournalOutputCommitMetadata> {
        self.by_batch_id.get(batch_id)
    }

    pub fn observed_entry_count(&self, batch_id: &str) -> usize {
        self.observed_entry_counts
            .get(batch_id)
            .copied()
            .unwrap_or(0)
    }

    pub fn is_complete(&self, batch_id: &str) -> bool {
        self.by_batch_id
            .get(batch_id)
            .is_some_and(|metadata| self.observed_entry_count(batch_id) >= metadata.entry_count)
    }

    pub fn lookup(&self, batch_id: &str) -> OutputCommitMetadataLookup {
        let Some(metadata) = self.by_batch_id.get(batch_id) else {
            return OutputCommitMetadataLookup::Missing;
        };
        let observed_entry_count = self.observed_entry_count(batch_id);

        if observed_entry_count >= metadata.entry_count {
            OutputCommitMetadataLookup::Complete {
                metadata: metadata.clone(),
            }
        } else {
            OutputCommitMetadataLookup::Incomplete {
                metadata: metadata.clone(),
                observed_entry_count,
            }
        }
    }
}
