use crate::engine::matching::MatchingEngine;
use crate::types::{ConfigVersion, JournalSeq, Order, OrderId, OrderType, Side, Symbol};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::io;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SnapshotManifest {
    pub symbol: String,
    pub config_version: ConfigVersion,
    pub last_input_seq: JournalSeq,
    pub order_book_checksum: u64,
    pub next_trade_id: u64,
    pub created_at_ns: i64,
}

#[derive(Serialize, Deserialize, Debug)]
struct SnapshotData {
    manifest: SnapshotManifest,
    bids: Vec<(String, Vec<SerializableOrder>)>,
    asks: Vec<(String, Vec<SerializableOrder>)>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct SerializableOrder {
    order_id: OrderId,
    symbol: Symbol,
    side: Side,
    order_type: OrderType,
    price: String,
    quantity: String,
    remaining: String,
    journal_seq: JournalSeq,
    timestamp_ns: i64,
}

pub fn write_snapshot(
    path: &str,
    engine: &MatchingEngine,
    last_input_seq: JournalSeq,
    config_version: ConfigVersion,
) -> io::Result<SnapshotManifest> {
    let checksum = engine.order_book().checksum();
    let (bids, asks) = engine.export_state();
    let manifest = SnapshotManifest {
        symbol: engine.symbol().0.clone(),
        config_version,
        last_input_seq,
        order_book_checksum: checksum,
        next_trade_id: engine.next_trade_id(),
        created_at_ns: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64,
    };

    let data = SnapshotData {
        manifest: manifest.clone(),
        bids: bids
            .into_iter()
            .map(|(price, orders)| (price.to_string(), serialize_orders(orders)))
            .collect(),
        asks: asks
            .into_iter()
            .map(|(price, orders)| (price.to_string(), serialize_orders(orders)))
            .collect(),
    };

    let bytes = bincode::serialize(&data).map_err(to_io_error)?;
    std::fs::write(path, bytes)?;
    Ok(manifest)
}

pub fn restore_snapshot(path: &str) -> io::Result<(MatchingEngine, JournalSeq)> {
    let bytes = std::fs::read(path)?;
    let data: SnapshotData = bincode::deserialize(&bytes).map_err(to_io_error)?;

    let symbol = Symbol(data.manifest.symbol.clone());
    let bids = parse_levels(data.bids)?;
    let asks = parse_levels(data.asks)?;
    let engine = MatchingEngine::from_state(symbol, bids, asks, data.manifest.next_trade_id);

    let restored_checksum = engine.order_book().checksum();
    if restored_checksum != data.manifest.order_book_checksum {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "checksum mismatch: expected {}, got {}",
                data.manifest.order_book_checksum, restored_checksum
            ),
        ));
    }

    Ok((engine, data.manifest.last_input_seq))
}

fn parse_levels(
    levels: Vec<(String, Vec<SerializableOrder>)>,
) -> io::Result<Vec<(Decimal, Vec<Order>)>> {
    levels
        .into_iter()
        .map(|(price, orders)| {
            price
                .parse::<Decimal>()
                .map_err(to_io_error)
                .and_then(|price| deserialize_orders(orders).map(|orders| (price, orders)))
        })
        .collect()
}

fn serialize_orders(orders: Vec<Order>) -> Vec<SerializableOrder> {
    orders
        .into_iter()
        .map(|order| SerializableOrder {
            order_id: order.order_id,
            symbol: order.symbol,
            side: order.side,
            order_type: order.order_type,
            price: order.price.to_string(),
            quantity: order.quantity.to_string(),
            remaining: order.remaining.to_string(),
            journal_seq: order.journal_seq,
            timestamp_ns: order.timestamp_ns,
        })
        .collect()
}

fn deserialize_orders(orders: Vec<SerializableOrder>) -> io::Result<Vec<Order>> {
    orders
        .into_iter()
        .map(|order| {
            Ok(Order {
                order_id: order.order_id,
                symbol: order.symbol,
                side: order.side,
                order_type: order.order_type,
                price: order.price.parse::<Decimal>().map_err(to_io_error)?,
                quantity: order.quantity.parse::<Decimal>().map_err(to_io_error)?,
                remaining: order.remaining.parse::<Decimal>().map_err(to_io_error)?,
                journal_seq: order.journal_seq,
                timestamp_ns: order.timestamp_ns,
            })
        })
        .collect()
}

fn to_io_error(error: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}
