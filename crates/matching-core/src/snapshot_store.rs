use crate::snapshot_restore::{SnapshotSerializationError, SymbolRuntimeSnapshot};
use crate::types::{Checksum, JournalSeq, Symbol};
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotSelectionReport {
    pub selected: Option<SymbolRuntimeSnapshot>,
    pub selected_record: Option<SnapshotRecord>,
    pub rejected: Vec<RejectedSnapshotRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedSnapshotSelectionReport {
    pub selected: Option<SymbolRuntimeSnapshot>,
    pub selected_record: Option<SnapshotRecord>,
    pub skipped_unverified: Vec<SnapshotRecord>,
    pub rejected: Vec<RejectedVerifiedSnapshotRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RejectedSnapshotRecord {
    pub record: SnapshotRecord,
    pub error: SnapshotSerializationError,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RejectedVerifiedSnapshotRecord {
    pub record: SnapshotRecord,
    pub error: SnapshotVerificationError,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotVerificationError {
    ManifestInvalid,
    SnapshotDigestMismatch,
    SnapshotChecksumMismatch,
    SnapshotSafePointMismatch,
    SnapshotSymbolMismatch,
    SnapshotSerialization(SnapshotSerializationError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotVerificationManifest {
    pub symbol: Symbol,
    pub safe_point: JournalSeq,
    pub snapshot_digest: u64,
    pub snapshot_checksum: Checksum,
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

    pub fn snapshot_bytes_digest(bytes: &[u8]) -> u64 {
        let mut digest = 0xcbf29ce484222325_u64;

        for byte in bytes {
            digest ^= *byte as u64;
            digest = digest.wrapping_mul(0x100000001b3);
        }

        digest
    }

    pub fn write_raw_symbol_snapshot_bytes(
        &self,
        symbol: Symbol,
        safe_point: JournalSeq,
        bytes: Vec<u8>,
    ) -> Result<SnapshotRecord, SnapshotStoreError> {
        let symbol_dir = self.symbol_dir(&symbol);

        fs::create_dir_all(&symbol_dir).map_err(io_error)?;
        fs::write(self.snapshot_path(&symbol, safe_point), &bytes).map_err(io_error)?;
        self.retain_latest_symbol_snapshots(&symbol)?;

        Ok(SnapshotRecord {
            symbol,
            safe_point,
            bytes,
        })
    }

    pub fn mark_symbol_snapshot_verified(
        &self,
        symbol: &Symbol,
        safe_point: JournalSeq,
    ) -> Result<Option<SnapshotRecord>, SnapshotStoreError> {
        let Some(record) = self
            .read_symbol_records(symbol)?
            .into_iter()
            .find(|record| record.safe_point == safe_point)
        else {
            return Ok(None);
        };

        let snapshot = SymbolRuntimeSnapshot::from_canonical_bytes(&record.bytes)
            .map_err(SnapshotStoreError::SnapshotSerialization)?;
        let manifest = SnapshotVerificationManifest {
            symbol: symbol.clone(),
            safe_point,
            snapshot_digest: Self::snapshot_bytes_digest(&record.bytes),
            snapshot_checksum: snapshot.order_book_snapshot.checksum,
        };

        fs::write(
            self.verified_marker_path(symbol, safe_point),
            encode_verification_manifest(&manifest),
        )
        .map_err(io_error)?;

        Ok(Some(record))
    }

    pub fn load_symbol_snapshot_verification_manifest(
        &self,
        symbol: &Symbol,
        safe_point: JournalSeq,
    ) -> Result<Option<SnapshotVerificationManifest>, SnapshotStoreError> {
        let path = self.verified_marker_path(symbol, safe_point);

        if !path.exists() {
            return Ok(None);
        }

        let bytes = fs::read(path).map_err(io_error)?;

        Ok(decode_verification_manifest(&bytes))
    }

    pub fn select_latest_valid_symbol_snapshot(
        &self,
        symbol: &Symbol,
    ) -> Result<SnapshotSelectionReport, SnapshotStoreError> {
        let records = self.read_symbol_records(symbol)?;
        let mut rejected = Vec::new();

        for record in records.into_iter().rev() {
            match SymbolRuntimeSnapshot::from_canonical_bytes(&record.bytes) {
                Ok(snapshot) => {
                    return Ok(SnapshotSelectionReport {
                        selected: Some(snapshot),
                        selected_record: Some(record),
                        rejected,
                    });
                }
                Err(error) => {
                    rejected.push(RejectedSnapshotRecord { record, error });
                }
            }
        }

        Ok(SnapshotSelectionReport {
            selected: None,
            selected_record: None,
            rejected,
        })
    }

    pub fn select_latest_verified_symbol_snapshot(
        &self,
        symbol: &Symbol,
    ) -> Result<VerifiedSnapshotSelectionReport, SnapshotStoreError> {
        let records = self.read_symbol_records(symbol)?;
        let mut skipped_unverified = Vec::new();
        let mut rejected = Vec::new();

        for record in records.into_iter().rev() {
            if !self
                .verified_marker_path(symbol, record.safe_point)
                .exists()
            {
                skipped_unverified.push(record);
                continue;
            }

            match self.verify_record_manifest(symbol, &record) {
                Ok(snapshot) => {
                    return Ok(VerifiedSnapshotSelectionReport {
                        selected: Some(snapshot),
                        selected_record: Some(record),
                        skipped_unverified,
                        rejected,
                    });
                }
                Err(error) => rejected.push(RejectedVerifiedSnapshotRecord { record, error }),
            }
        }

        Ok(VerifiedSnapshotSelectionReport {
            selected: None,
            selected_record: None,
            skipped_unverified,
            rejected,
        })
    }

    fn verify_record_manifest(
        &self,
        symbol: &Symbol,
        record: &SnapshotRecord,
    ) -> Result<SymbolRuntimeSnapshot, SnapshotVerificationError> {
        let Some(manifest) = self
            .load_symbol_snapshot_verification_manifest(symbol, record.safe_point)
            .map_err(|_| SnapshotVerificationError::ManifestInvalid)?
        else {
            return Err(SnapshotVerificationError::ManifestInvalid);
        };

        if manifest.symbol != *symbol {
            return Err(SnapshotVerificationError::SnapshotSymbolMismatch);
        }
        if manifest.safe_point != record.safe_point {
            return Err(SnapshotVerificationError::SnapshotSafePointMismatch);
        }
        if manifest.snapshot_digest != Self::snapshot_bytes_digest(&record.bytes) {
            return Err(SnapshotVerificationError::SnapshotDigestMismatch);
        }

        let snapshot = SymbolRuntimeSnapshot::from_canonical_bytes(&record.bytes)
            .map_err(SnapshotVerificationError::SnapshotSerialization)?;

        if snapshot.order_book_snapshot.symbol != *symbol {
            return Err(SnapshotVerificationError::SnapshotSymbolMismatch);
        }
        if snapshot.order_book_snapshot.last_input_seq != record.safe_point {
            return Err(SnapshotVerificationError::SnapshotSafePointMismatch);
        }
        if snapshot.order_book_snapshot.checksum != manifest.snapshot_checksum {
            return Err(SnapshotVerificationError::SnapshotChecksumMismatch);
        }

        Ok(snapshot)
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

    fn verified_marker_path(&self, symbol: &Symbol, safe_point: JournalSeq) -> PathBuf {
        self.symbol_dir(symbol)
            .join(format!("{:020}.verified", safe_point.0))
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
            let marker_path = self.verified_marker_path(symbol, record.safe_point);
            if marker_path.exists() {
                fs::remove_file(marker_path).map_err(io_error)?;
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

fn encode_verification_manifest(manifest: &SnapshotVerificationManifest) -> Vec<u8> {
    format!(
        "matching-core snapshot verification v1\nsymbol={}\nsafe_point={}\nsnapshot_digest={}\nsnapshot_checksum={}\n",
        manifest.symbol.0, manifest.safe_point.0, manifest.snapshot_digest, manifest.snapshot_checksum.0
    )
    .into_bytes()
}

fn decode_verification_manifest(bytes: &[u8]) -> Option<SnapshotVerificationManifest> {
    let text = std::str::from_utf8(bytes).ok()?;
    let mut lines = text.lines();

    if lines.next()? != "matching-core snapshot verification v1" {
        return None;
    }

    let symbol = lines.next()?.strip_prefix("symbol=")?.to_string();
    let safe_point = lines.next()?.strip_prefix("safe_point=")?.parse().ok()?;
    let snapshot_digest = lines
        .next()?
        .strip_prefix("snapshot_digest=")?
        .parse()
        .ok()?;
    let snapshot_checksum = lines
        .next()?
        .strip_prefix("snapshot_checksum=")?
        .parse()
        .ok()?;

    Some(SnapshotVerificationManifest {
        symbol: Symbol(symbol),
        safe_point: JournalSeq(safe_point),
        snapshot_digest,
        snapshot_checksum: Checksum(snapshot_checksum),
    })
}

fn io_error(error: std::io::Error) -> SnapshotStoreError {
    SnapshotStoreError::Io(error.to_string())
}
