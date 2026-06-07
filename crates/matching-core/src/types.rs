use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct OrderId(pub u64);

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TradeId(pub u64);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Symbol(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct CommandId(pub u64);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ConfigVersion(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct JournalSeq(pub u64);

impl JournalSeq {
    pub fn next(self) -> Self {
        JournalSeq(self.0 + 1)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Side {
    Bid,
    Ask,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderType {
    Limit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RejectReason {
    DuplicateOrderId,
    InvalidSymbol,
    InvalidPrice,
    InvalidQuantity,
    ConfigVersionMismatch,
    UnknownCommand,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CancelRejectReason {
    OrderNotFound,
    AlreadyFilled,
    AlreadyCancelled,
    DuplicateCommandId,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Order {
    pub order_id: OrderId,
    pub symbol: Symbol,
    pub side: Side,
    pub order_type: OrderType,
    pub price: Decimal,
    pub quantity: Decimal,
    pub remaining: Decimal,
    pub journal_seq: JournalSeq,
    pub timestamp_ns: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderCommand {
    pub command_id: CommandId,
    pub order_id: OrderId,
    pub symbol: Symbol,
    pub side: Side,
    pub order_type: OrderType,
    pub price: Decimal,
    pub quantity: Decimal,
    pub config_version: ConfigVersion,
    pub timestamp_ns: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CancelCommand {
    pub command_id: CommandId,
    pub order_id: OrderId,
    pub symbol: Symbol,
    pub config_version: ConfigVersion,
    pub timestamp_ns: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MatchingCommand {
    PlaceOrder(OrderCommand),
    CancelOrder(CancelCommand),
}

#[derive(Debug, Clone)]
pub struct SymbolConfig {
    pub price_tick: Decimal,
    pub quantity_tick: Decimal,
    pub min_quantity: Decimal,
    pub config_version: ConfigVersion,
}
