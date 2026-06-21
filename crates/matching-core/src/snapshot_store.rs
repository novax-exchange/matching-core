use crate::snapshot_restore::{SnapshotSerializationError, SymbolRuntimeSnapshot};
use crate::types::{JournalSeq, Symbol};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotRecord {
    pub symbol: Symbol,
    pub safe_point: JournalSeq,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotStoreError {
    SnapshotSerialization(SnapshotSerializationError),
}

pub trait SnapshotStore {
    fn save_symbol_snapshot(
        &mut self,
        snapshot: &SymbolRuntimeSnapshot,
    ) -> Result<SnapshotRecord, SnapshotStoreError>;

    fn load_latest_symbol_snapshot(
        &self,
        symbol: &Symbol,
    ) -> Result<Option<SymbolRuntimeSnapshot>, SnapshotStoreError>;
}

#[derive(Debug, Clone, Default)]
pub struct InMemorySnapshotStore {
    records_by_symbol: HashMap<Symbol, Vec<SnapshotRecord>>,
    retention_limit: usize,
}

impl InMemorySnapshotStore {
    pub fn new() -> Self {
        Self::new_with_retention_limit(1)
    }

    pub fn new_with_retention_limit(retention_limit: usize) -> Self {
        Self {
            records_by_symbol: HashMap::new(),
            retention_limit: retention_limit.max(1),
        }
    }

    pub fn write_raw_symbol_snapshot_bytes(&mut self, symbol: Symbol, bytes: Vec<u8>) {
        self.records_by_symbol.insert(
            symbol.clone(),
            vec![SnapshotRecord {
                symbol,
                safe_point: JournalSeq(0),
                bytes,
            }],
        );
    }

    pub fn symbol_snapshot_records(&self, symbol: &Symbol) -> Vec<SnapshotRecord> {
        self.records_by_symbol
            .get(symbol)
            .cloned()
            .unwrap_or_default()
    }
}

impl SnapshotStore for InMemorySnapshotStore {
    fn save_symbol_snapshot(
        &mut self,
        snapshot: &SymbolRuntimeSnapshot,
    ) -> Result<SnapshotRecord, SnapshotStoreError> {
        let record = SnapshotRecord {
            symbol: snapshot.order_book_snapshot.symbol.clone(),
            safe_point: snapshot.order_book_snapshot.last_input_seq,
            bytes: snapshot.to_canonical_bytes(),
        };

        let records = self
            .records_by_symbol
            .entry(record.symbol.clone())
            .or_default();
        records.push(record.clone());

        if records.len() > self.retention_limit {
            let remove_count = records.len() - self.retention_limit;
            records.drain(0..remove_count);
        }

        Ok(record)
    }

    fn load_latest_symbol_snapshot(
        &self,
        symbol: &Symbol,
    ) -> Result<Option<SymbolRuntimeSnapshot>, SnapshotStoreError> {
        let Some(record) = self
            .records_by_symbol
            .get(symbol)
            .and_then(|records| records.last())
        else {
            return Ok(None);
        };

        SymbolRuntimeSnapshot::from_canonical_bytes(&record.bytes)
            .map(Some)
            .map_err(SnapshotStoreError::SnapshotSerialization)
    }
}
