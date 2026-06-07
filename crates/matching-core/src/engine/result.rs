use crate::types::{
    CancelRejectReason, CommandId, JournalSeq, OrderId, RejectReason, Symbol, TradeId,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum OrderAck {
    Accepted {
        order_id: OrderId,
        journal_seq: JournalSeq,
    },
    PartiallyFilled {
        order_id: OrderId,
        filled_qty: Decimal,
        remaining: Decimal,
        journal_seq: JournalSeq,
    },
    FullyFilled {
        order_id: OrderId,
        filled_qty: Decimal,
        journal_seq: JournalSeq,
    },
    Rejected {
        order_id: Option<OrderId>,
        command_id: CommandId,
        reason: RejectReason,
        journal_seq: JournalSeq,
    },
    Cancelled {
        order_id: OrderId,
        remaining: Decimal,
        journal_seq: JournalSeq,
    },
    CancelRejected {
        order_id: OrderId,
        command_id: CommandId,
        reason: CancelRejectReason,
        journal_seq: JournalSeq,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TradeEvent {
    pub trade_id: TradeId,
    pub symbol: Symbol,
    pub maker_id: OrderId,
    pub taker_id: OrderId,
    pub price: Decimal,
    pub quantity: Decimal,
    pub journal_seq: JournalSeq,
    pub timestamp_ns: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketDataEvent {
    pub symbol: Symbol,
    pub last_price: Decimal,
    pub last_qty: Decimal,
    pub best_bid: Option<Decimal>,
    pub best_ask: Option<Decimal>,
    pub journal_seq: JournalSeq,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MatchResult {
    pub order_ack: OrderAck,
    pub trades: Vec<TradeEvent>,
    pub market_event: Option<MarketDataEvent>,
}
