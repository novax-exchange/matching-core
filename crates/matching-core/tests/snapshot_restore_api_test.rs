use matching_core::journal_adapter::{JournalInputEntry, JournalInputReader};
use matching_core::order::{Command, Order};
use matching_core::order_book::OrderBook;
use matching_core::per_symbol_execution_loop::SymbolRuntime;
use matching_core::replay_runner::ReplayRunner;
use matching_core::snapshot_restore::{
    OrderBookSnapshot, SnapshotSerializationError, SnapshotVerificationOrchestrator,
    SnapshotVerificationSchedulingOutcome, SymbolRuntimeSnapshot,
};
use matching_core::snapshot_store::{
    FileSnapshotStore, InMemorySnapshotStore, SnapshotManifestSigner, SnapshotManifestVerifier,
    SnapshotStore, SnapshotStoreError, SnapshotVerificationError,
};
use matching_core::types::*;
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn snapshot_can_be_created_and_restored_from_public_api() {
    let symbol = Symbol("BTC-USDT".to_string());
    let mut book = OrderBook::new(symbol.clone());

    book.insert(Order {
        order_id: OrderId(1),
        symbol: symbol.clone(),
        side: Side::Buy,
        price: Price(100),
        quantity: Quantity(5),
    });

    let snapshot = OrderBookSnapshot::from_order_book(&book, JournalSeq(10));
    let restored = snapshot.restore_order_book();

    assert_eq!(snapshot.last_input_seq, JournalSeq(10));
    assert_eq!(restored.symbol(), &symbol);
    assert_eq!(restored.checksum(), book.checksum());
    assert_eq!(restored.resting_orders(), snapshot.resting_orders);
}

fn symbol_runtime_snapshot() -> SymbolRuntimeSnapshot {
    let symbol = Symbol("BTC-USDT".to_string());

    SymbolRuntimeSnapshot {
        order_book_snapshot: OrderBookSnapshot {
            symbol: symbol.clone(),
            last_input_seq: JournalSeq(10),
            checksum: Checksum(123),
            resting_orders: vec![Order {
                order_id: OrderId(1),
                symbol,
                side: Side::Buy,
                price: Price(100),
                quantity: Quantity(5),
            }],
        },
        next_trade_seq: 7,
        next_market_seq: 9,
        seen_command_ids: vec![CommandId(1), CommandId(2)],
        seen_order_ids: vec![OrderId(1), OrderId(2)],
    }
}

fn symbol_runtime_snapshot_at_safe_point(safe_point: u64) -> SymbolRuntimeSnapshot {
    let mut snapshot = symbol_runtime_snapshot();

    snapshot.order_book_snapshot.last_input_seq = JournalSeq(safe_point);
    snapshot.order_book_snapshot.checksum = Checksum(1000 + safe_point);

    snapshot
}

#[test]
fn symbol_runtime_snapshot_can_round_trip_through_canonical_bytes_from_public_api() {
    let snapshot = symbol_runtime_snapshot();

    let encoded = snapshot.to_canonical_bytes();
    let decoded = SymbolRuntimeSnapshot::from_canonical_bytes(&encoded)
        .expect("canonical bytes should decode");

    assert_eq!(decoded, snapshot);
}

#[test]
fn symbol_runtime_snapshot_canonical_bytes_sort_recoverable_identity_sets() {
    let mut first = symbol_runtime_snapshot();
    let mut second = symbol_runtime_snapshot();

    first.seen_command_ids = vec![CommandId(1), CommandId(2)];
    first.seen_order_ids = vec![OrderId(1), OrderId(2)];
    second.seen_command_ids = vec![CommandId(2), CommandId(1)];
    second.seen_order_ids = vec![OrderId(2), OrderId(1)];

    assert_eq!(first.to_canonical_bytes(), second.to_canonical_bytes());
}

#[test]
fn symbol_runtime_snapshot_rejects_invalid_canonical_bytes_magic_from_public_api() {
    let mut encoded = symbol_runtime_snapshot().to_canonical_bytes();
    encoded[0] = b'X';

    assert_eq!(
        SymbolRuntimeSnapshot::from_canonical_bytes(&encoded),
        Err(SnapshotSerializationError::InvalidMagic)
    );
}

#[test]
fn in_memory_snapshot_store_saves_and_loads_latest_symbol_snapshot_from_public_api() {
    let snapshot = symbol_runtime_snapshot();
    let mut store = InMemorySnapshotStore::new();

    let record = store
        .save_symbol_snapshot(&snapshot)
        .expect("snapshot should be saved");
    let loaded = store
        .load_latest_symbol_snapshot(&snapshot.order_book_snapshot.symbol)
        .expect("stored snapshot should decode")
        .expect("stored snapshot should exist");

    assert_eq!(record.symbol, snapshot.order_book_snapshot.symbol);
    assert_eq!(
        record.safe_point,
        snapshot.order_book_snapshot.last_input_seq
    );
    assert_eq!(loaded, snapshot);
}

#[test]
fn in_memory_snapshot_store_returns_none_for_missing_symbol_from_public_api() {
    let store = InMemorySnapshotStore::new();

    assert_eq!(
        store.load_latest_symbol_snapshot(&Symbol("ETH-USDT".to_string())),
        Ok(None)
    );
}

#[test]
fn in_memory_snapshot_store_rejects_corrupt_snapshot_bytes_from_public_api() {
    let mut store = InMemorySnapshotStore::new();
    let symbol = Symbol("BTC-USDT".to_string());
    let mut bytes = symbol_runtime_snapshot().to_canonical_bytes();

    bytes[0] = b'X';
    store.write_raw_symbol_snapshot_bytes(symbol.clone(), bytes);

    assert_eq!(
        store.load_latest_symbol_snapshot(&symbol),
        Err(SnapshotStoreError::SnapshotSerialization(
            SnapshotSerializationError::InvalidMagic
        ))
    );
}

#[test]
fn in_memory_snapshot_store_retains_latest_symbol_snapshots_within_limit_from_public_api() {
    let symbol = Symbol("BTC-USDT".to_string());
    let mut store = InMemorySnapshotStore::new_with_retention_limit(2);

    store
        .save_symbol_snapshot(&symbol_runtime_snapshot_at_safe_point(10))
        .expect("first snapshot should be saved");
    store
        .save_symbol_snapshot(&symbol_runtime_snapshot_at_safe_point(11))
        .expect("second snapshot should be saved");
    store
        .save_symbol_snapshot(&symbol_runtime_snapshot_at_safe_point(12))
        .expect("third snapshot should be saved");

    let records = store.symbol_snapshot_records(&symbol);
    let loaded = store
        .load_latest_symbol_snapshot(&symbol)
        .expect("latest snapshot should decode")
        .expect("latest snapshot should exist");

    assert_eq!(
        records
            .iter()
            .map(|record| record.safe_point)
            .collect::<Vec<_>>(),
        vec![JournalSeq(11), JournalSeq(12)]
    );
    assert_eq!(loaded.order_book_snapshot.last_input_seq, JournalSeq(12));
}

fn temporary_snapshot_dir(test_name: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    let path = std::env::temp_dir().join(format!("matching-core-{test_name}-{unique}"));

    fs::create_dir_all(&path).expect("temporary snapshot dir should be created");
    path
}

#[test]
fn file_snapshot_store_saves_and_loads_latest_symbol_snapshot_from_public_api() {
    let dir = temporary_snapshot_dir("file-store-round-trip");
    let snapshot = symbol_runtime_snapshot();

    let mut writer = FileSnapshotStore::new(dir.clone());
    writer
        .save_symbol_snapshot(&snapshot)
        .expect("snapshot should be written to disk");

    let reader = FileSnapshotStore::new(dir.clone());
    let loaded = reader
        .load_latest_symbol_snapshot(&snapshot.order_book_snapshot.symbol)
        .expect("stored snapshot should decode")
        .expect("stored snapshot should exist");

    assert_eq!(loaded, snapshot);

    fs::remove_dir_all(dir).expect("temporary snapshot dir should be removed");
}

#[test]
fn file_snapshot_store_retains_latest_symbol_snapshots_within_limit_from_public_api() {
    let dir = temporary_snapshot_dir("file-store-retention");
    let symbol = Symbol("BTC-USDT".to_string());
    let mut store = FileSnapshotStore::new_with_retention_limit(dir.clone(), 2);

    store
        .save_symbol_snapshot(&symbol_runtime_snapshot_at_safe_point(10))
        .expect("first snapshot should be written");
    store
        .save_symbol_snapshot(&symbol_runtime_snapshot_at_safe_point(11))
        .expect("second snapshot should be written");
    store
        .save_symbol_snapshot(&symbol_runtime_snapshot_at_safe_point(12))
        .expect("third snapshot should be written");

    let records = store
        .symbol_snapshot_records(&symbol)
        .expect("snapshot records should be listed");
    let loaded = store
        .load_latest_symbol_snapshot(&symbol)
        .expect("latest snapshot should decode")
        .expect("latest snapshot should exist");

    assert_eq!(
        records
            .iter()
            .map(|record| record.safe_point)
            .collect::<Vec<_>>(),
        vec![JournalSeq(11), JournalSeq(12)]
    );
    assert_eq!(loaded.order_book_snapshot.last_input_seq, JournalSeq(12));

    fs::remove_dir_all(dir).expect("temporary snapshot dir should be removed");
}

#[test]
fn file_snapshot_store_selects_latest_valid_snapshot_when_latest_is_corrupt_from_public_api() {
    let dir = temporary_snapshot_dir("file-store-corrupt-latest");
    let symbol = Symbol("BTC-USDT".to_string());
    let mut store = FileSnapshotStore::new_with_retention_limit(dir.clone(), 3);

    store
        .save_symbol_snapshot(&symbol_runtime_snapshot_at_safe_point(10))
        .expect("first snapshot should be written");
    store
        .save_symbol_snapshot(&symbol_runtime_snapshot_at_safe_point(11))
        .expect("second snapshot should be written");

    let mut corrupt_latest = symbol_runtime_snapshot_at_safe_point(12).to_canonical_bytes();
    corrupt_latest[0] = b'X';
    store
        .write_raw_symbol_snapshot_bytes(symbol.clone(), JournalSeq(12), corrupt_latest)
        .expect("corrupt latest snapshot should be written");

    let report = store
        .select_latest_valid_symbol_snapshot(&symbol)
        .expect("snapshot selection should read the file store");

    assert_eq!(
        report
            .selected
            .expect("older retained snapshot should be selected")
            .order_book_snapshot
            .last_input_seq,
        JournalSeq(11)
    );
    assert_eq!(report.rejected.len(), 1);
    assert_eq!(report.rejected[0].record.safe_point, JournalSeq(12));
    assert_eq!(
        report.rejected[0].error,
        SnapshotSerializationError::InvalidMagic
    );

    fs::remove_dir_all(dir).expect("temporary snapshot dir should be removed");
}

#[test]
fn file_snapshot_store_reports_rejections_when_no_valid_snapshot_exists_from_public_api() {
    let dir = temporary_snapshot_dir("file-store-all-corrupt");
    let symbol = Symbol("BTC-USDT".to_string());
    let store = FileSnapshotStore::new_with_retention_limit(dir.clone(), 2);

    let mut first = symbol_runtime_snapshot_at_safe_point(10).to_canonical_bytes();
    first[0] = b'X';
    store
        .write_raw_symbol_snapshot_bytes(symbol.clone(), JournalSeq(10), first)
        .expect("first corrupt snapshot should be written");

    let mut second = symbol_runtime_snapshot_at_safe_point(11).to_canonical_bytes();
    second[0] = b'X';
    store
        .write_raw_symbol_snapshot_bytes(symbol.clone(), JournalSeq(11), second)
        .expect("second corrupt snapshot should be written");

    let report = store
        .select_latest_valid_symbol_snapshot(&symbol)
        .expect("snapshot selection should read corrupt records");

    assert_eq!(report.selected, None);
    assert_eq!(report.selected_record, None);
    assert_eq!(
        report
            .rejected
            .iter()
            .map(|rejected| rejected.record.safe_point)
            .collect::<Vec<_>>(),
        vec![JournalSeq(11), JournalSeq(10)]
    );

    fs::remove_dir_all(dir).expect("temporary snapshot dir should be removed");
}

#[test]
fn file_snapshot_store_selects_latest_verified_snapshot_and_skips_unverified_from_public_api() {
    let dir = temporary_snapshot_dir("file-store-verified-selection");
    let symbol = Symbol("BTC-USDT".to_string());
    let mut store = FileSnapshotStore::new_with_retention_limit(dir.clone(), 3);

    store
        .save_symbol_snapshot(&symbol_runtime_snapshot_at_safe_point(10))
        .expect("first snapshot should be written");
    store
        .save_symbol_snapshot(&symbol_runtime_snapshot_at_safe_point(11))
        .expect("second snapshot should be written");
    store
        .mark_symbol_snapshot_verified(&symbol, JournalSeq(11))
        .expect("snapshot should be marked verified")
        .expect("snapshot should exist before marking verified");
    store
        .save_symbol_snapshot(&symbol_runtime_snapshot_at_safe_point(12))
        .expect("newer unverified snapshot should be written");

    let report = store
        .select_latest_verified_symbol_snapshot(&symbol)
        .expect("snapshot selection should read verified markers");

    assert_eq!(
        report
            .selected_record
            .expect("verified snapshot should be selected")
            .safe_point,
        JournalSeq(11)
    );
    assert_eq!(
        report
            .selected
            .expect("verified snapshot should decode")
            .order_book_snapshot
            .last_input_seq,
        JournalSeq(11)
    );
    assert_eq!(
        report
            .skipped_unverified
            .iter()
            .map(|record| record.safe_point)
            .collect::<Vec<_>>(),
        vec![JournalSeq(12)]
    );

    fs::remove_dir_all(dir).expect("temporary snapshot dir should be removed");
}

#[test]
fn file_snapshot_store_verified_manifest_records_snapshot_identity_from_public_api() {
    let dir = temporary_snapshot_dir("file-store-verified-manifest");
    let symbol = Symbol("BTC-USDT".to_string());
    let mut store = FileSnapshotStore::new_with_retention_limit(dir.clone(), 2);

    let snapshot = symbol_runtime_snapshot_at_safe_point(11);
    let record = store
        .save_symbol_snapshot(&snapshot)
        .expect("snapshot should be written");
    store
        .mark_symbol_snapshot_verified(&symbol, JournalSeq(11))
        .expect("snapshot should be marked verified")
        .expect("snapshot should exist before marking verified");

    let manifest = store
        .load_symbol_snapshot_verification_manifest(&symbol, JournalSeq(11))
        .expect("manifest should be readable")
        .expect("manifest should exist");

    assert_eq!(manifest.symbol, symbol);
    assert_eq!(manifest.safe_point, JournalSeq(11));
    assert_eq!(
        manifest.snapshot_digest,
        FileSnapshotStore::snapshot_bytes_digest(&record.bytes)
    );
    assert_eq!(
        manifest.snapshot_checksum,
        snapshot.order_book_snapshot.checksum
    );

    fs::remove_dir_all(dir).expect("temporary snapshot dir should be removed");
}

#[test]
fn file_snapshot_store_rejects_verified_snapshot_when_manifest_digest_does_not_match_from_public_api(
) {
    let dir = temporary_snapshot_dir("file-store-verified-digest-mismatch");
    let symbol = Symbol("BTC-USDT".to_string());
    let mut store = FileSnapshotStore::new_with_retention_limit(dir.clone(), 2);

    store
        .save_symbol_snapshot(&symbol_runtime_snapshot_at_safe_point(11))
        .expect("snapshot should be written");
    store
        .mark_symbol_snapshot_verified(&symbol, JournalSeq(11))
        .expect("snapshot should be marked verified")
        .expect("snapshot should exist before marking verified");
    store
        .write_raw_symbol_snapshot_bytes(
            symbol.clone(),
            JournalSeq(11),
            symbol_runtime_snapshot_at_safe_point(12).to_canonical_bytes(),
        )
        .expect("snapshot bytes should be replaced after verification");

    let report = store
        .select_latest_verified_symbol_snapshot(&symbol)
        .expect("verified snapshot selection should read manifest");

    assert_eq!(report.selected, None);
    assert_eq!(report.selected_record, None);
    assert_eq!(report.rejected.len(), 1);
    assert_eq!(report.rejected[0].record.safe_point, JournalSeq(11));
    assert_eq!(
        report.rejected[0].error,
        matching_core::snapshot_store::SnapshotVerificationError::SnapshotDigestMismatch
    );

    fs::remove_dir_all(dir).expect("temporary snapshot dir should be removed");
}

fn test_snapshot_manifest_signer() -> SnapshotManifestSigner {
    SnapshotManifestSigner::ed25519(
        "snapshot-verifier-a",
        "snapshot-verifier-a-key-1",
        [7_u8; 32],
    )
}

fn trusted_test_snapshot_manifest_verifier() -> SnapshotManifestVerifier {
    let signer = test_snapshot_manifest_signer();

    SnapshotManifestVerifier::new().trust_ed25519_key(
        "snapshot-verifier-a",
        "snapshot-verifier-a-key-1",
        signer.public_key_bytes(),
    )
}

fn snapshot_after_journal_prefix(
    journal: &TestJournalInputReader,
    count: usize,
) -> SymbolRuntimeSnapshot {
    let mut runtime = SymbolRuntime::new(Symbol("BTC-USDT".to_string()));

    for entry in journal.read_from(JournalSeq(1)).into_iter().take(count) {
        let request = runtime.process_entry_to_output_request(entry);
        runtime
            .mark_output_committed(request.journal_seq)
            .expect("test fixture should commit an ordered prefix");
    }

    runtime.snapshot().expect("snapshot requires a safe point")
}

#[test]
fn snapshot_verification_orchestrator_writes_signed_manifest_after_replay_match_from_public_api() {
    let dir = temporary_snapshot_dir("snapshot-verification-orchestrator-match");
    let symbol = Symbol("BTC-USDT".to_string());
    let mut store = FileSnapshotStore::new_with_retention_limit(dir.clone(), 2);
    let signer = test_snapshot_manifest_signer();
    let verifier = trusted_test_snapshot_manifest_verifier();
    let mut journal = TestJournalInputReader::new();

    journal.append(
        CommandId(1),
        Command::PlaceLimit(Order {
            order_id: OrderId(1),
            symbol: symbol.clone(),
            side: Side::Buy,
            price: Price(100),
            quantity: Quantity(5),
        }),
    );
    journal.append(
        CommandId(2),
        Command::PlaceLimit(Order {
            order_id: OrderId(2),
            symbol: symbol.clone(),
            side: Side::Sell,
            price: Price(105),
            quantity: Quantity(3),
        }),
    );

    let snapshot = snapshot_after_journal_prefix(&journal, 1);
    let full_result = ReplayRunner::new(symbol.clone()).replay_result(&journal);
    let expected_tail_result = matching_core::replay_runner::ReplayResult {
        checksum: full_result.checksum,
        last_replayed_seq: full_result.last_replayed_seq,
        output_entries: full_result.output_entries[1..].to_vec(),
    };

    store
        .save_symbol_snapshot(&snapshot)
        .expect("snapshot should be written before verification");

    let report = SnapshotVerificationOrchestrator::new(symbol.clone())
        .verify_and_sign_snapshot_candidate(
            &snapshot,
            &journal,
            &expected_tail_result,
            &store,
            &signer,
        )
        .expect("snapshot verification should run");

    assert!(report.comparison.is_match());
    assert!(report.verified_manifest_written);

    let selection = store
        .select_latest_trusted_verified_symbol_snapshot(&symbol, &verifier)
        .expect("trusted verified snapshot should be selectable");

    assert_eq!(
        selection
            .selected_record
            .expect("signed snapshot should be selected")
            .safe_point,
        JournalSeq(1)
    );

    fs::remove_dir_all(dir).expect("temporary snapshot dir should be removed");
}

#[test]
fn snapshot_verification_orchestrator_does_not_write_manifest_after_replay_mismatch_from_public_api(
) {
    let dir = temporary_snapshot_dir("snapshot-verification-orchestrator-mismatch");
    let symbol = Symbol("BTC-USDT".to_string());
    let mut store = FileSnapshotStore::new_with_retention_limit(dir.clone(), 2);
    let signer = test_snapshot_manifest_signer();
    let mut journal = TestJournalInputReader::new();

    journal.append(
        CommandId(1),
        Command::PlaceLimit(Order {
            order_id: OrderId(1),
            symbol: symbol.clone(),
            side: Side::Buy,
            price: Price(100),
            quantity: Quantity(5),
        }),
    );
    journal.append(
        CommandId(2),
        Command::PlaceLimit(Order {
            order_id: OrderId(2),
            symbol: symbol.clone(),
            side: Side::Sell,
            price: Price(105),
            quantity: Quantity(3),
        }),
    );

    let snapshot = snapshot_after_journal_prefix(&journal, 1);
    let full_result = ReplayRunner::new(symbol.clone()).replay_result(&journal);
    let mismatched_expected_tail_result = matching_core::replay_runner::ReplayResult {
        checksum: Checksum(full_result.checksum.0 + 1),
        last_replayed_seq: full_result.last_replayed_seq,
        output_entries: full_result.output_entries[1..].to_vec(),
    };

    store
        .save_symbol_snapshot(&snapshot)
        .expect("snapshot should be written before verification");

    let report = SnapshotVerificationOrchestrator::new(symbol.clone())
        .verify_and_sign_snapshot_candidate(
            &snapshot,
            &journal,
            &mismatched_expected_tail_result,
            &store,
            &signer,
        )
        .expect("snapshot verification should run");

    assert!(!report.comparison.is_match());
    assert!(!report.verified_manifest_written);
    assert_eq!(
        store
            .load_symbol_snapshot_verification_manifest(&symbol, JournalSeq(1))
            .expect("manifest lookup should read store"),
        None
    );

    fs::remove_dir_all(dir).expect("temporary snapshot dir should be removed");
}

#[test]
fn snapshot_verification_orchestrator_run_once_signs_latest_unverified_candidate_from_public_api() {
    let dir = temporary_snapshot_dir("snapshot-verification-orchestrator-run-once-latest");
    let symbol = Symbol("BTC-USDT".to_string());
    let mut store = FileSnapshotStore::new_with_retention_limit(dir.clone(), 3);
    let signer = test_snapshot_manifest_signer();
    let verifier = trusted_test_snapshot_manifest_verifier();
    let mut journal = TestJournalInputReader::new();

    journal.append(
        CommandId(1),
        Command::PlaceLimit(Order {
            order_id: OrderId(1),
            symbol: symbol.clone(),
            side: Side::Buy,
            price: Price(100),
            quantity: Quantity(5),
        }),
    );
    journal.append(
        CommandId(2),
        Command::PlaceLimit(Order {
            order_id: OrderId(2),
            symbol: symbol.clone(),
            side: Side::Sell,
            price: Price(105),
            quantity: Quantity(3),
        }),
    );
    journal.append(
        CommandId(3),
        Command::PlaceLimit(Order {
            order_id: OrderId(3),
            symbol: symbol.clone(),
            side: Side::Buy,
            price: Price(99),
            quantity: Quantity(2),
        }),
    );

    store
        .save_symbol_snapshot(&snapshot_after_journal_prefix(&journal, 1))
        .expect("first snapshot should be written");
    store
        .save_symbol_snapshot(&snapshot_after_journal_prefix(&journal, 2))
        .expect("second snapshot should be written");

    let report = SnapshotVerificationOrchestrator::new(symbol.clone())
        .run_once(&journal, &store, &signer)
        .expect("verification scheduling should run");

    assert_eq!(report.candidate_safe_point, Some(JournalSeq(2)));
    assert_eq!(
        report.outcome,
        SnapshotVerificationSchedulingOutcome::Verified
    );
    assert_eq!(report.skipped_already_verified_safe_points, Vec::new());
    assert!(report
        .verification
        .as_ref()
        .expect("latest unverified candidate should be verified")
        .comparison
        .is_match());
    assert!(
        report
            .verification
            .expect("latest unverified candidate should be verified")
            .verified_manifest_written
    );

    let selection = store
        .select_latest_trusted_verified_symbol_snapshot(&symbol, &verifier)
        .expect("trusted verified snapshot should be selectable");

    assert_eq!(
        selection
            .selected_record
            .expect("signed snapshot should be selected")
            .safe_point,
        JournalSeq(2)
    );

    fs::remove_dir_all(dir).expect("temporary snapshot dir should be removed");
}

#[test]
fn snapshot_verification_orchestrator_run_once_skips_verified_candidate_from_public_api() {
    let dir = temporary_snapshot_dir("snapshot-verification-orchestrator-run-once-skip-verified");
    let symbol = Symbol("BTC-USDT".to_string());
    let mut store = FileSnapshotStore::new_with_retention_limit(dir.clone(), 3);
    let signer = test_snapshot_manifest_signer();
    let mut journal = TestJournalInputReader::new();

    journal.append(
        CommandId(1),
        Command::PlaceLimit(Order {
            order_id: OrderId(1),
            symbol: symbol.clone(),
            side: Side::Buy,
            price: Price(100),
            quantity: Quantity(5),
        }),
    );
    journal.append(
        CommandId(2),
        Command::PlaceLimit(Order {
            order_id: OrderId(2),
            symbol: symbol.clone(),
            side: Side::Sell,
            price: Price(105),
            quantity: Quantity(3),
        }),
    );

    store
        .save_symbol_snapshot(&snapshot_after_journal_prefix(&journal, 1))
        .expect("first snapshot should be written");
    store
        .save_symbol_snapshot(&snapshot_after_journal_prefix(&journal, 2))
        .expect("second snapshot should be written");
    store
        .mark_symbol_snapshot_verified_by(&symbol, JournalSeq(2), &signer)
        .expect("latest snapshot should be signed")
        .expect("latest snapshot should exist before signing");

    let report = SnapshotVerificationOrchestrator::new(symbol.clone())
        .run_once(&journal, &store, &signer)
        .expect("verification scheduling should run");

    assert_eq!(report.candidate_safe_point, Some(JournalSeq(1)));
    assert_eq!(
        report.outcome,
        SnapshotVerificationSchedulingOutcome::Verified
    );
    assert_eq!(
        report.skipped_already_verified_safe_points,
        vec![JournalSeq(2)]
    );
    assert!(
        report
            .verification
            .expect("older unverified candidate should be verified")
            .verified_manifest_written
    );
    assert!(store
        .load_symbol_snapshot_verification_manifest(&symbol, JournalSeq(1))
        .expect("manifest lookup should read store")
        .is_some());

    fs::remove_dir_all(dir).expect("temporary snapshot dir should be removed");
}

#[test]
fn snapshot_verification_orchestrator_run_once_reports_no_candidate_from_public_api() {
    let dir = temporary_snapshot_dir("snapshot-verification-orchestrator-run-once-no-candidate");
    let symbol = Symbol("BTC-USDT".to_string());
    let store = FileSnapshotStore::new_with_retention_limit(dir.clone(), 3);
    let signer = test_snapshot_manifest_signer();
    let journal = TestJournalInputReader::new();

    let report = SnapshotVerificationOrchestrator::new(symbol.clone())
        .run_once(&journal, &store, &signer)
        .expect("verification scheduling should run");

    assert_eq!(report.symbol, symbol);
    assert_eq!(
        report.outcome,
        SnapshotVerificationSchedulingOutcome::NoCandidate
    );
    assert_eq!(report.candidate_safe_point, None);
    assert_eq!(report.skipped_already_verified_safe_points, Vec::new());
    assert_eq!(report.verification, None);

    fs::remove_dir_all(dir).expect("temporary snapshot dir should be removed");
}

#[test]
fn snapshot_verification_orchestrator_run_once_reports_mismatch_without_manifest_from_public_api() {
    let dir = temporary_snapshot_dir("snapshot-verification-orchestrator-run-once-mismatch");
    let symbol = Symbol("BTC-USDT".to_string());
    let mut store = FileSnapshotStore::new_with_retention_limit(dir.clone(), 3);
    let signer = test_snapshot_manifest_signer();
    let mut journal = TestJournalInputReader::new();

    journal.append(
        CommandId(1),
        Command::PlaceLimit(Order {
            order_id: OrderId(1),
            symbol: symbol.clone(),
            side: Side::Buy,
            price: Price(100),
            quantity: Quantity(5),
        }),
    );
    journal.append(
        CommandId(2),
        Command::PlaceLimit(Order {
            order_id: OrderId(2),
            symbol: symbol.clone(),
            side: Side::Sell,
            price: Price(105),
            quantity: Quantity(3),
        }),
    );

    let mut snapshot = snapshot_after_journal_prefix(&journal, 1);
    snapshot.next_market_seq += 1;
    store
        .save_symbol_snapshot(&snapshot)
        .expect("drifted snapshot should be written");

    let report = SnapshotVerificationOrchestrator::new(symbol.clone())
        .run_once(&journal, &store, &signer)
        .expect("verification scheduling should run");

    assert_eq!(report.candidate_safe_point, Some(JournalSeq(1)));
    assert_eq!(
        report.outcome,
        SnapshotVerificationSchedulingOutcome::Mismatch
    );
    assert!(
        !report
            .verification
            .expect("mismatched candidate should still produce comparison evidence")
            .verified_manifest_written
    );
    assert!(store
        .load_symbol_snapshot_verification_manifest(&symbol, JournalSeq(1))
        .expect("manifest lookup should read store")
        .is_none());

    fs::remove_dir_all(dir).expect("temporary snapshot dir should be removed");
}

#[test]
fn file_snapshot_store_selects_snapshot_verified_by_trusted_ed25519_key_from_public_api() {
    let dir = temporary_snapshot_dir("file-store-ed25519-verified-selection");
    let symbol = Symbol("BTC-USDT".to_string());
    let mut store = FileSnapshotStore::new_with_retention_limit(dir.clone(), 2);
    let signer = test_snapshot_manifest_signer();
    let verifier = trusted_test_snapshot_manifest_verifier();

    store
        .save_symbol_snapshot(&symbol_runtime_snapshot_at_safe_point(11))
        .expect("snapshot should be written");
    store
        .mark_symbol_snapshot_verified_by(&symbol, JournalSeq(11), &signer)
        .expect("snapshot should be signed as verified")
        .expect("snapshot should exist before signing verified manifest");

    let report = store
        .select_latest_trusted_verified_symbol_snapshot(&symbol, &verifier)
        .expect("trusted verified snapshot selection should read manifest");

    assert_eq!(
        report
            .selected
            .expect("trusted verified snapshot should decode")
            .order_book_snapshot
            .last_input_seq,
        JournalSeq(11)
    );
    assert_eq!(report.rejected, Vec::new());

    fs::remove_dir_all(dir).expect("temporary snapshot dir should be removed");
}

#[test]
fn file_snapshot_store_rejects_snapshot_verified_by_untrusted_ed25519_key_from_public_api() {
    let dir = temporary_snapshot_dir("file-store-ed25519-untrusted-verifier");
    let symbol = Symbol("BTC-USDT".to_string());
    let mut store = FileSnapshotStore::new_with_retention_limit(dir.clone(), 2);
    let signer = test_snapshot_manifest_signer();
    let verifier = SnapshotManifestVerifier::new();

    store
        .save_symbol_snapshot(&symbol_runtime_snapshot_at_safe_point(11))
        .expect("snapshot should be written");
    store
        .mark_symbol_snapshot_verified_by(&symbol, JournalSeq(11), &signer)
        .expect("snapshot should be signed as verified")
        .expect("snapshot should exist before signing verified manifest");

    let report = store
        .select_latest_trusted_verified_symbol_snapshot(&symbol, &verifier)
        .expect("trusted verified snapshot selection should read manifest");

    assert_eq!(report.selected, None);
    assert_eq!(report.selected_record, None);
    assert_eq!(
        report.rejected[0].error,
        SnapshotVerificationError::UntrustedVerifier
    );

    fs::remove_dir_all(dir).expect("temporary snapshot dir should be removed");
}

#[test]
fn file_snapshot_store_rejects_verified_snapshot_when_ed25519_signature_does_not_match_from_public_api(
) {
    let dir = temporary_snapshot_dir("file-store-ed25519-signature-mismatch");
    let symbol = Symbol("BTC-USDT".to_string());
    let mut store = FileSnapshotStore::new_with_retention_limit(dir.clone(), 2);
    let signer = test_snapshot_manifest_signer();
    let verifier = trusted_test_snapshot_manifest_verifier();

    store
        .save_symbol_snapshot(&symbol_runtime_snapshot_at_safe_point(11))
        .expect("snapshot should be written");
    store
        .mark_symbol_snapshot_verified_by(&symbol, JournalSeq(11), &signer)
        .expect("snapshot should be signed as verified")
        .expect("snapshot should exist before signing verified manifest");

    let mut manifest = store
        .load_symbol_snapshot_verification_manifest(&symbol, JournalSeq(11))
        .expect("manifest should be readable")
        .expect("manifest should exist");
    manifest.snapshot_checksum = Checksum(manifest.snapshot_checksum.0 + 1);
    store
        .write_symbol_snapshot_verification_manifest(&manifest)
        .expect("tampered manifest should be written");

    let report = store
        .select_latest_trusted_verified_symbol_snapshot(&symbol, &verifier)
        .expect("trusted verified snapshot selection should read tampered manifest");

    assert_eq!(report.selected, None);
    assert_eq!(report.selected_record, None);
    assert_eq!(
        report.rejected[0].error,
        SnapshotVerificationError::SignatureMismatch
    );

    fs::remove_dir_all(dir).expect("temporary snapshot dir should be removed");
}

#[test]
fn file_snapshot_store_rejects_corrupt_verified_snapshot_and_falls_back_from_public_api() {
    let dir = temporary_snapshot_dir("file-store-corrupt-verified");
    let symbol = Symbol("BTC-USDT".to_string());
    let mut store = FileSnapshotStore::new_with_retention_limit(dir.clone(), 3);

    store
        .save_symbol_snapshot(&symbol_runtime_snapshot_at_safe_point(10))
        .expect("first snapshot should be written");
    store
        .save_symbol_snapshot(&symbol_runtime_snapshot_at_safe_point(11))
        .expect("second snapshot should be written");
    store
        .mark_symbol_snapshot_verified(&symbol, JournalSeq(11))
        .expect("older snapshot should be marked verified")
        .expect("older snapshot should exist");

    store
        .save_symbol_snapshot(&symbol_runtime_snapshot_at_safe_point(12))
        .expect("latest snapshot should be written");
    store
        .mark_symbol_snapshot_verified(&symbol, JournalSeq(12))
        .expect("latest snapshot should be marked verified")
        .expect("latest snapshot should exist");

    let mut corrupt_latest = symbol_runtime_snapshot_at_safe_point(12).to_canonical_bytes();
    corrupt_latest[0] = b'X';
    store
        .write_raw_symbol_snapshot_bytes(symbol.clone(), JournalSeq(12), corrupt_latest)
        .expect("verified latest snapshot should be overwritten as corrupt");

    let report = store
        .select_latest_verified_symbol_snapshot(&symbol)
        .expect("verified snapshot selection should read retained snapshots");

    assert_eq!(
        report
            .selected_record
            .expect("older verified snapshot should be selected")
            .safe_point,
        JournalSeq(11)
    );
    assert_eq!(report.rejected.len(), 1);
    assert_eq!(report.rejected[0].record.safe_point, JournalSeq(12));
    assert_eq!(
        report.rejected[0].error,
        matching_core::snapshot_store::SnapshotVerificationError::SnapshotDigestMismatch
    );

    fs::remove_dir_all(dir).expect("temporary snapshot dir should be removed");
}

struct TestJournalInputReader {
    entries: Vec<JournalInputEntry>,
}

impl TestJournalInputReader {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
}

impl JournalInputReader for TestJournalInputReader {
    fn append(&mut self, command_id: CommandId, command: Command) -> JournalSeq {
        let seq = JournalSeq(self.entries.len() as u64 + 1);

        self.entries.push(JournalInputEntry {
            seq,
            command_id,
            command,
        });

        seq
    }

    fn read_from(&self, from: JournalSeq) -> Vec<JournalInputEntry> {
        self.entries
            .iter()
            .filter(|entry| entry.seq >= from)
            .cloned()
            .collect()
    }

    fn latest_seq(&self) -> Option<JournalSeq> {
        self.entries.last().map(|entry| entry.seq)
    }
}

#[test]
fn restored_snapshot_can_continue_replay_from_public_api() {
    let symbol = Symbol("BTC-USDT".to_string());

    let mut snapshot_book = OrderBook::new(symbol.clone());
    snapshot_book.insert(Order {
        order_id: OrderId(1),
        symbol: symbol.clone(),
        side: Side::Buy,
        price: Price(100),
        quantity: Quantity(5),
    });

    let snapshot = OrderBookSnapshot::from_order_book(&snapshot_book, JournalSeq(1));
    let restored = snapshot.restore_order_book();

    let mut journal = TestJournalInputReader::new();
    journal.append(
        CommandId(1),
        Command::PlaceLimit(Order {
            order_id: OrderId(1),
            symbol: symbol.clone(),
            side: Side::Buy,
            price: Price(100),
            quantity: Quantity(5),
        }),
    );
    journal.append(
        CommandId(2),
        Command::PlaceLimit(Order {
            order_id: OrderId(2),
            symbol: symbol.clone(),
            side: Side::Sell,
            price: Price(105),
            quantity: Quantity(3),
        }),
    );

    let full_checksum = ReplayRunner::new(symbol.clone()).replay(&journal);
    let resumed_checksum = ReplayRunner::new(symbol).replay_from_order_book(
        restored,
        &journal,
        JournalSeq(snapshot.last_input_seq.0 + 1),
    );

    assert_ne!(full_checksum, Checksum(0));
    assert_eq!(resumed_checksum, full_checksum);
}
