mod support;

use matching_core::engine::matching::MatchingEngine;
use matching_core::snapshot::snapshot::{restore_snapshot, write_snapshot};
use matching_core::types::*;
use rust_decimal_macros::dec;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static SNAPSHOT_ID: AtomicU64 = AtomicU64::new(1);

fn tmp_path() -> PathBuf {
    std::env::temp_dir().join(format!(
        "novax_snap_{}_{}.bin",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        SNAPSHOT_ID.fetch_add(1, Ordering::Relaxed)
    ))
}

fn setup_engine() -> (MatchingEngine, u64) {
    let symbol = Symbol("BTCUSDT".into());
    let mut engine = MatchingEngine::new(symbol.clone());
    engine.process(
        MatchingCommand::PlaceOrder(OrderCommand {
            command_id: CommandId(1),
            order_id: OrderId(1),
            symbol: symbol.clone(),
            side: Side::Bid,
            order_type: OrderType::Limit,
            price: dec!(100),
            quantity: dec!(5),
            config_version: ConfigVersion(1),
            timestamp_ns: 0,
        }),
        JournalSeq(1),
    );
    engine.process(
        MatchingCommand::PlaceOrder(OrderCommand {
            command_id: CommandId(2),
            order_id: OrderId(2),
            symbol,
            side: Side::Ask,
            order_type: OrderType::Limit,
            price: dec!(101),
            quantity: dec!(3),
            config_version: ConfigVersion(1),
            timestamp_ns: 0,
        }),
        JournalSeq(2),
    );
    let checksum = engine.order_book().checksum();
    (engine, checksum)
}

#[test]
fn snapshot_restore_produces_same_checksum() {
    let (engine, original_checksum) = setup_engine();
    let path = tmp_path();
    let manifest =
        write_snapshot(path.to_str().unwrap(), &engine, JournalSeq(2), ConfigVersion(1)).unwrap();
    assert_eq!(manifest.order_book_checksum, original_checksum);

    let (restored_engine, restored_seq) = restore_snapshot(path.to_str().unwrap()).unwrap();
    assert_eq!(restored_seq, JournalSeq(2));
    assert_eq!(restored_engine.order_book().checksum(), original_checksum);
}

#[test]
fn snapshot_manifest_contains_correct_metadata() {
    let (engine, _) = setup_engine();
    let path = tmp_path();
    let manifest =
        write_snapshot(path.to_str().unwrap(), &engine, JournalSeq(5), ConfigVersion(2)).unwrap();
    assert_eq!(manifest.last_input_seq, JournalSeq(5));
    assert_eq!(manifest.config_version, ConfigVersion(2));
}

#[test]
fn snapshot_plus_replay_restores_same_checksum() {
    use matching_core::engine::gateway::{CommandGateway, GatewayResult};
    use matching_core::journal::traits::InputJournal;
    use support::in_memory_journal::InMemoryInputJournal;

    let input = InMemoryInputJournal::default();
    let symbol = Symbol("BTCUSDT".into());
    let config = SymbolConfig {
        price_tick: dec!(0.01),
        quantity_tick: dec!(0.001),
        min_quantity: dec!(0.001),
        config_version: ConfigVersion(1),
    };

    for id in 1..=3 {
        input.append(
            symbol.clone(),
            MatchingCommand::PlaceOrder(OrderCommand {
                command_id: CommandId(id),
                order_id: OrderId(id),
                symbol: symbol.clone(),
                side: Side::Bid,
                order_type: OrderType::Limit,
                price: dec!(100),
                quantity: dec!(1),
                config_version: ConfigVersion(1),
                timestamp_ns: id as i64,
            }),
        );
    }

    let mut engine = MatchingEngine::new(symbol.clone());
    let mut gateway = CommandGateway::new(symbol.clone(), config.clone());
    for entry in input.read_from(&symbol, JournalSeq(1)).into_iter().take(2) {
        if let GatewayResult::Accept(command) = gateway.validate(entry.command, entry.seq) {
            engine.process(command, entry.seq);
        }
    }

    let snapshot_path = tmp_path();
    write_snapshot(
        snapshot_path.to_str().unwrap(),
        &engine,
        JournalSeq(2),
        ConfigVersion(1),
    )
    .unwrap();

    let (mut restored, last_seq) = restore_snapshot(snapshot_path.to_str().unwrap()).unwrap();
    assert_eq!(last_seq, JournalSeq(2));
    let mut replay_gateway = CommandGateway::new(symbol.clone(), config.clone());
    for entry in input.read_from(&symbol, last_seq.next()) {
        if let GatewayResult::Accept(command) = replay_gateway.validate(entry.command, entry.seq) {
            restored.process(command, entry.seq);
        }
    }

    let mut full_engine = MatchingEngine::new(symbol.clone());
    let mut full_gateway = CommandGateway::new(symbol.clone(), config);
    for entry in input.read_from(&symbol, JournalSeq(1)) {
        if let GatewayResult::Accept(command) = full_gateway.validate(entry.command, entry.seq) {
            full_engine.process(command, entry.seq);
        }
    }

    assert_eq!(
        restored.order_book().checksum(),
        full_engine.order_book().checksum()
    );
}
