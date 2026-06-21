use matching_core::journal_adapter::{JournalInputEntry, JournalInputReader};
use matching_core::order::{Command, Order};
use matching_core::order_book::OrderBook;
use matching_core::replay_runner::ReplayRunner;
use matching_core::snapshot_restore::{
    OrderBookSnapshot, SnapshotSerializationError, SymbolRuntimeSnapshot,
};
use matching_core::snapshot_store::{InMemorySnapshotStore, SnapshotStore, SnapshotStoreError};
use matching_core::types::*;

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
