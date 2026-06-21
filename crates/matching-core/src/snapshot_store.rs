use crate::snapshot_restore::{SnapshotSerializationError, SymbolRuntimeSnapshot};
use crate::types::{Checksum, JournalSeq, Symbol};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

const ED25519_SIGNATURE_ALGORITHM: &str = "Ed25519";

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
    ManifestUnsigned,
    UntrustedVerifier,
    SignatureMismatch,
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
    pub snapshot_sha256: [u8; 32],
    pub snapshot_checksum: Checksum,
    pub verified_by: Option<String>,
    pub key_id: Option<String>,
    pub signature_algorithm: Option<String>,
    pub signature: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct SnapshotManifestSigner {
    verifier_id: String,
    key_id: String,
    signing_key: SigningKey,
}

impl SnapshotManifestSigner {
    pub fn ed25519(
        verifier_id: impl Into<String>,
        key_id: impl Into<String>,
        signing_key_bytes: [u8; 32],
    ) -> Self {
        Self {
            verifier_id: verifier_id.into(),
            key_id: key_id.into(),
            signing_key: SigningKey::from_bytes(&signing_key_bytes),
        }
    }

    pub fn verifier_id(&self) -> &str {
        &self.verifier_id
    }

    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    pub fn public_key_bytes(&self) -> [u8; 32] {
        self.signing_key.verifying_key().to_bytes()
    }

    fn sign_manifest(&self, manifest: &mut SnapshotVerificationManifest) {
        manifest.verified_by = Some(self.verifier_id.clone());
        manifest.key_id = Some(self.key_id.clone());
        manifest.signature_algorithm = Some(ED25519_SIGNATURE_ALGORITHM.to_string());
        manifest.signature = None;

        let signature = self
            .signing_key
            .sign(&verification_manifest_signing_payload(manifest));
        manifest.signature = Some(signature.to_bytes().to_vec());
    }
}

#[derive(Debug, Clone, Default)]
pub struct SnapshotManifestVerifier {
    trusted_ed25519_keys: HashMap<(String, String), VerifyingKey>,
}

impl SnapshotManifestVerifier {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn trust_ed25519_key(
        mut self,
        verifier_id: impl Into<String>,
        key_id: impl Into<String>,
        public_key_bytes: [u8; 32],
    ) -> Self {
        let verifying_key = VerifyingKey::from_bytes(&public_key_bytes)
            .expect("ed25519 public key bytes should be valid");
        self.trusted_ed25519_keys
            .insert((verifier_id.into(), key_id.into()), verifying_key);
        self
    }

    fn verify_manifest(
        &self,
        manifest: &SnapshotVerificationManifest,
    ) -> Result<(), SnapshotVerificationError> {
        let Some(verified_by) = &manifest.verified_by else {
            return Err(SnapshotVerificationError::ManifestUnsigned);
        };
        let Some(key_id) = &manifest.key_id else {
            return Err(SnapshotVerificationError::ManifestUnsigned);
        };
        let Some(signature_algorithm) = &manifest.signature_algorithm else {
            return Err(SnapshotVerificationError::ManifestUnsigned);
        };
        let Some(signature_bytes) = &manifest.signature else {
            return Err(SnapshotVerificationError::ManifestUnsigned);
        };

        if signature_algorithm != ED25519_SIGNATURE_ALGORITHM {
            return Err(SnapshotVerificationError::ManifestInvalid);
        }

        let Some(verifying_key) = self
            .trusted_ed25519_keys
            .get(&(verified_by.clone(), key_id.clone()))
        else {
            return Err(SnapshotVerificationError::UntrustedVerifier);
        };

        let signature_bytes: [u8; 64] = signature_bytes
            .as_slice()
            .try_into()
            .map_err(|_| SnapshotVerificationError::ManifestInvalid)?;
        let signature = Signature::from_bytes(&signature_bytes);

        verifying_key
            .verify(&verification_manifest_signing_payload(manifest), &signature)
            .map_err(|_| SnapshotVerificationError::SignatureMismatch)
    }
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

    pub fn snapshot_bytes_sha256(bytes: &[u8]) -> [u8; 32] {
        Sha256::digest(bytes).into()
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
            snapshot_sha256: Self::snapshot_bytes_sha256(&record.bytes),
            snapshot_checksum: snapshot.order_book_snapshot.checksum,
            verified_by: None,
            key_id: None,
            signature_algorithm: None,
            signature: None,
        };

        self.write_symbol_snapshot_verification_manifest(&manifest)?;

        Ok(Some(record))
    }

    pub fn mark_symbol_snapshot_verified_by(
        &self,
        symbol: &Symbol,
        safe_point: JournalSeq,
        signer: &SnapshotManifestSigner,
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
        let mut manifest = SnapshotVerificationManifest {
            symbol: symbol.clone(),
            safe_point,
            snapshot_digest: Self::snapshot_bytes_digest(&record.bytes),
            snapshot_sha256: Self::snapshot_bytes_sha256(&record.bytes),
            snapshot_checksum: snapshot.order_book_snapshot.checksum,
            verified_by: None,
            key_id: None,
            signature_algorithm: None,
            signature: None,
        };

        signer.sign_manifest(&mut manifest);
        self.write_symbol_snapshot_verification_manifest(&manifest)?;

        Ok(Some(record))
    }

    pub fn write_symbol_snapshot_verification_manifest(
        &self,
        manifest: &SnapshotVerificationManifest,
    ) -> Result<(), SnapshotStoreError> {
        fs::write(
            self.verified_marker_path(&manifest.symbol, manifest.safe_point),
            encode_verification_manifest(manifest),
        )
        .map_err(io_error)
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

    pub fn select_latest_trusted_verified_symbol_snapshot(
        &self,
        symbol: &Symbol,
        verifier: &SnapshotManifestVerifier,
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

            match self.verify_record_manifest_with_trust(symbol, &record, verifier) {
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
        verify_manifest_matches_record(symbol, record, &manifest)?;

        let snapshot = SymbolRuntimeSnapshot::from_canonical_bytes(&record.bytes)
            .map_err(SnapshotVerificationError::SnapshotSerialization)?;

        verify_snapshot_matches_manifest(symbol, record, &snapshot, &manifest)?;

        Ok(snapshot)
    }

    fn verify_record_manifest_with_trust(
        &self,
        symbol: &Symbol,
        record: &SnapshotRecord,
        verifier: &SnapshotManifestVerifier,
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

        verifier.verify_manifest(&manifest)?;
        verify_manifest_matches_record(symbol, record, &manifest)?;

        let snapshot = SymbolRuntimeSnapshot::from_canonical_bytes(&record.bytes)
            .map_err(SnapshotVerificationError::SnapshotSerialization)?;

        verify_snapshot_matches_manifest(symbol, record, &snapshot, &manifest)?;

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
    let mut text = format!(
        "matching-core snapshot verification v1\nsymbol={}\nsafe_point={}\nsnapshot_digest={}\nsnapshot_sha256={}\nsnapshot_checksum={}\n",
        manifest.symbol.0,
        manifest.safe_point.0,
        manifest.snapshot_digest,
        encode_hex(&manifest.snapshot_sha256),
        manifest.snapshot_checksum.0
    );

    if let Some(verified_by) = &manifest.verified_by {
        text.push_str(&format!("verified_by={verified_by}\n"));
    }
    if let Some(key_id) = &manifest.key_id {
        text.push_str(&format!("key_id={key_id}\n"));
    }
    if let Some(signature_algorithm) = &manifest.signature_algorithm {
        text.push_str(&format!("signature_algorithm={signature_algorithm}\n"));
    }
    if let Some(signature) = &manifest.signature {
        text.push_str(&format!("signature={}\n", encode_hex(signature)));
    }

    text.into_bytes()
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
    let next_line = lines.next()?;
    let (snapshot_sha256, snapshot_checksum) =
        if let Some(snapshot_sha256_hex) = next_line.strip_prefix("snapshot_sha256=") {
            let snapshot_sha256 = decode_hex_array_32(snapshot_sha256_hex)?;
            let snapshot_checksum = lines
                .next()?
                .strip_prefix("snapshot_checksum=")?
                .parse()
                .ok()?;
            (snapshot_sha256, snapshot_checksum)
        } else {
            let snapshot_checksum = next_line.strip_prefix("snapshot_checksum=")?.parse().ok()?;
            ([0_u8; 32], snapshot_checksum)
        };

    let mut verified_by = None;
    let mut key_id = None;
    let mut signature_algorithm = None;
    let mut signature = None;

    for line in lines {
        if let Some(value) = line.strip_prefix("verified_by=") {
            verified_by = Some(value.to_string());
        } else if let Some(value) = line.strip_prefix("key_id=") {
            key_id = Some(value.to_string());
        } else if let Some(value) = line.strip_prefix("signature_algorithm=") {
            signature_algorithm = Some(value.to_string());
        } else if let Some(value) = line.strip_prefix("signature=") {
            signature = Some(decode_hex_vec(value)?);
        }
    }

    Some(SnapshotVerificationManifest {
        symbol: Symbol(symbol),
        safe_point: JournalSeq(safe_point),
        snapshot_digest,
        snapshot_sha256,
        snapshot_checksum: Checksum(snapshot_checksum),
        verified_by,
        key_id,
        signature_algorithm,
        signature,
    })
}

fn verify_manifest_matches_record(
    symbol: &Symbol,
    record: &SnapshotRecord,
    manifest: &SnapshotVerificationManifest,
) -> Result<(), SnapshotVerificationError> {
    if manifest.symbol != *symbol {
        return Err(SnapshotVerificationError::SnapshotSymbolMismatch);
    }
    if manifest.safe_point != record.safe_point {
        return Err(SnapshotVerificationError::SnapshotSafePointMismatch);
    }
    if manifest.snapshot_digest != FileSnapshotStore::snapshot_bytes_digest(&record.bytes) {
        return Err(SnapshotVerificationError::SnapshotDigestMismatch);
    }
    if manifest.snapshot_sha256 != [0_u8; 32]
        && manifest.snapshot_sha256 != FileSnapshotStore::snapshot_bytes_sha256(&record.bytes)
    {
        return Err(SnapshotVerificationError::SnapshotDigestMismatch);
    }

    Ok(())
}

fn verify_snapshot_matches_manifest(
    symbol: &Symbol,
    record: &SnapshotRecord,
    snapshot: &SymbolRuntimeSnapshot,
    manifest: &SnapshotVerificationManifest,
) -> Result<(), SnapshotVerificationError> {
    if snapshot.order_book_snapshot.symbol != *symbol {
        return Err(SnapshotVerificationError::SnapshotSymbolMismatch);
    }
    if snapshot.order_book_snapshot.last_input_seq != record.safe_point {
        return Err(SnapshotVerificationError::SnapshotSafePointMismatch);
    }
    if snapshot.order_book_snapshot.checksum != manifest.snapshot_checksum {
        return Err(SnapshotVerificationError::SnapshotChecksumMismatch);
    }

    Ok(())
}

fn verification_manifest_signing_payload(manifest: &SnapshotVerificationManifest) -> Vec<u8> {
    format!(
        "matching-core snapshot verification signature v1\nsymbol={}\nsafe_point={}\nsnapshot_digest={}\nsnapshot_sha256={}\nsnapshot_checksum={}\nverified_by={}\nkey_id={}\nsignature_algorithm={}\n",
        manifest.symbol.0,
        manifest.safe_point.0,
        manifest.snapshot_digest,
        encode_hex(&manifest.snapshot_sha256),
        manifest.snapshot_checksum.0,
        manifest.verified_by.as_deref().unwrap_or_default(),
        manifest.key_id.as_deref().unwrap_or_default(),
        manifest
            .signature_algorithm
            .as_deref()
            .unwrap_or_default()
    )
    .into_bytes()
}

fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn decode_hex_array_32(text: &str) -> Option<[u8; 32]> {
    let bytes = decode_hex_vec(text)?;

    bytes.try_into().ok()
}

fn decode_hex_vec(text: &str) -> Option<Vec<u8>> {
    if text.len() % 2 != 0 {
        return None;
    }

    let mut bytes = Vec::with_capacity(text.len() / 2);
    for index in (0..text.len()).step_by(2) {
        let byte = u8::from_str_radix(&text[index..index + 2], 16).ok()?;
        bytes.push(byte);
    }

    Some(bytes)
}

fn io_error(error: std::io::Error) -> SnapshotStoreError {
    SnapshotStoreError::Io(error.to_string())
}
