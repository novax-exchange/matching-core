use crate::snapshot_restore::{SnapshotSerializationError, SymbolRuntimeSnapshot};
use crate::types::{JournalSeq, Symbol};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotRecord {
    pub symbol: Symbol,
    pub safe_point: JournalSeq,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotStoreError {
    Io(String),
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

#[derive(Debug, Clone)]
pub struct FileSnapshotStore {
    root: PathBuf,
    retention_limit: usize,
}

impl FileSnapshotStore {
    pub fn new(root: PathBuf) -> Self {
        Self::new_with_retention_limit(root, 1)
    }

    pub fn new_with_retention_limit(root: PathBuf, retention_limit: usize) -> Self {
        Self {
            root,
            retention_limit: retention_limit.max(1),
        }
    }

    pub fn symbol_snapshot_records(
        &self,
        symbol: &Symbol,
    ) -> Result<Vec<SnapshotRecord>, SnapshotStoreError> {
        let mut records = self.read_symbol_records(symbol)?;
        records.sort_by_key(|record| record.safe_point);
        Ok(records)
    }

    fn read_symbol_records(
        &self,
        symbol: &Symbol,
    ) -> Result<Vec<SnapshotRecord>, SnapshotStoreError> {
        let symbol_dir = self.symbol_dir(symbol);

        if !symbol_dir.exists() {
            return Ok(Vec::new());
        }

        let mut records = Vec::new();

        for entry in fs::read_dir(&symbol_dir).map_err(io_error)? {
            let entry = entry.map_err(io_error)?;
            let path = entry.path();

            if !path.is_file() {
                continue;
            }

            let Some(safe_point) = safe_point_from_snapshot_path(&path) else {
                continue;
            };
            let bytes = fs::read(&path).map_err(io_error)?;

            records.push(SnapshotRecord {
                symbol: symbol.clone(),
                safe_point,
                bytes,
            });
        }

        records.sort_by_key(|record| record.safe_point);
        Ok(records)
    }

    fn symbol_dir(&self, symbol: &Symbol) -> PathBuf {
        self.root.join(encode_symbol_for_path(symbol))
    }

    fn snapshot_path(&self, symbol: &Symbol, safe_point: JournalSeq) -> PathBuf {
        self.symbol_dir(symbol)
            .join(format!("{:020}.snap", safe_point.0))
    }

    fn retain_latest_symbol_snapshots(&self, symbol: &Symbol) -> Result<(), SnapshotStoreError> {
        let records = self.read_symbol_records(symbol)?;

        if records.len() <= self.retention_limit {
            return Ok(());
        }

        let remove_count = records.len() - self.retention_limit;
        for record in records.into_iter().take(remove_count) {
            let path = self.snapshot_path(symbol, record.safe_point);
            if path.exists() {
                fs::remove_file(path).map_err(io_error)?;
            }
        }

        Ok(())
    }
}

impl SnapshotStore for FileSnapshotStore {
    fn save_symbol_snapshot(
        &mut self,
        snapshot: &SymbolRuntimeSnapshot,
    ) -> Result<SnapshotRecord, SnapshotStoreError> {
        let symbol = snapshot.order_book_snapshot.symbol.clone();
        let safe_point = snapshot.order_book_snapshot.last_input_seq;
        let bytes = snapshot.to_canonical_bytes();
        let symbol_dir = self.symbol_dir(&symbol);

        fs::create_dir_all(&symbol_dir).map_err(io_error)?;

        let final_path = self.snapshot_path(&symbol, safe_point);
        let temporary_path =
            symbol_dir.join(format!("{:020}.{}.tmp", safe_point.0, std::process::id()));

        fs::write(&temporary_path, &bytes).map_err(io_error)?;
        fs::rename(&temporary_path, final_path).map_err(io_error)?;

        self.retain_latest_symbol_snapshots(&symbol)?;

        Ok(SnapshotRecord {
            symbol,
            safe_point,
            bytes,
        })
    }

    fn load_latest_symbol_snapshot(
        &self,
        symbol: &Symbol,
    ) -> Result<Option<SymbolRuntimeSnapshot>, SnapshotStoreError> {
        let Some(record) = self.read_symbol_records(symbol)?.pop() else {
            return Ok(None);
        };

        SymbolRuntimeSnapshot::from_canonical_bytes(&record.bytes)
            .map(Some)
            .map_err(SnapshotStoreError::SnapshotSerialization)
    }
}

fn encode_symbol_for_path(symbol: &Symbol) -> String {
    symbol
        .0
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn safe_point_from_snapshot_path(path: &Path) -> Option<JournalSeq> {
    let file_name = path.file_name()?.to_str()?;
    let safe_point = file_name.strip_suffix(".snap")?.parse().ok()?;

    Some(JournalSeq(safe_point))
}

fn io_error(error: std::io::Error) -> SnapshotStoreError {
    SnapshotStoreError::Io(error.to_string())
}
