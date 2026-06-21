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
    latest_by_symbol: HashMap<Symbol, SnapshotRecord>,
}

impl InMemorySnapshotStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn write_raw_symbol_snapshot_bytes(&mut self, symbol: Symbol, bytes: Vec<u8>) {
        self.latest_by_symbol.insert(
            symbol.clone(),
            SnapshotRecord {
                symbol,
                safe_point: JournalSeq(0),
                bytes,
            },
        );
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

        self.latest_by_symbol
            .insert(record.symbol.clone(), record.clone());

        Ok(record)
    }

    fn load_latest_symbol_snapshot(
        &self,
        symbol: &Symbol,
    ) -> Result<Option<SymbolRuntimeSnapshot>, SnapshotStoreError> {
        let Some(record) = self.latest_by_symbol.get(symbol) else {
            return Ok(None);
        };

        SymbolRuntimeSnapshot::from_canonical_bytes(&record.bytes)
            .map(Some)
            .map_err(SnapshotStoreError::SnapshotSerialization)
    }
}
