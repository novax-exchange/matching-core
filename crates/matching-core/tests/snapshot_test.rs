use matching_core::engine::matching::MatchingEngine;
use matching_core::snapshot::snapshot::{restore_snapshot, write_snapshot};
use matching_core::types::*;
use rust_decimal_macros::dec;
use std::path::PathBuf;

fn tmp_path() -> PathBuf {
    std::env::temp_dir().join(format!(
        "novax_snap_{}.bin",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
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
