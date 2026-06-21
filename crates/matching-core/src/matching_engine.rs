mod command_ingress;

pub use command_ingress::{CommandIngress, IngressError};

use crate::order::Order;
use crate::types::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Trade {
    pub maker_order_id: OrderId,
    pub taker_order_id: OrderId,
    pub price: Price,
    pub quantity: Quantity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchResult {
    pub trades: Vec<Trade>,
    pub resting_order_id: Option<OrderId>,
    pub resting_order: Option<Order>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RejectReason {
    InvalidPrice,
    InvalidQuantity,
    SymbolMismatch,
    DuplicateCommandId,
    DuplicateOrderId,
    OrderNotFound,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradeEvent {
    pub trade_id: TradeId,
    pub market_seq: MarketSeq,
    pub command_id: CommandId,
    pub journal_seq: JournalSeq,
    pub maker_order_id: OrderId,
    pub taker_order_id: OrderId,
    pub price: Price,
    pub quantity: Quantity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderAddedEvent {
    pub market_seq: MarketSeq,
    pub command_id: CommandId,
    pub journal_seq: JournalSeq,
    pub order_id: OrderId,
    pub side: Side,
    pub price: Price,
    pub quantity: Quantity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderCancelledEvent {
    pub market_seq: MarketSeq,
    pub command_id: CommandId,
    pub journal_seq: JournalSeq,
    pub order_id: OrderId,
    pub side: Side,
    pub price: Price,
    pub quantity: Quantity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PriceLevelChangedEvent {
    pub market_seq: MarketSeq,
    pub command_id: CommandId,
    pub journal_seq: JournalSeq,
    pub side: Side,
    pub price: Price,
    pub quantity_after: Quantity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarketEvent {
    OrderAdded(OrderAddedEvent),
    OrderCancelled(OrderCancelledEvent),
    PriceLevelChanged(PriceLevelChangedEvent),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderAck {
    Accepted {
        command_id: CommandId,
        order_id: OrderId,
        journal_seq: JournalSeq,
    },
    Rejected {
        command_id: CommandId,
        order_id: Option<OrderId>,
        journal_seq: JournalSeq,
        reason: RejectReason,
    },
    Cancelled {
        command_id: CommandId,
        order_id: OrderId,
        journal_seq: JournalSeq,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineEvent {
    OrderAck(OrderAck),
    Trade(TradeEvent),
    Market(MarketEvent),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trade_can_be_constructed() {
        let trade = Trade {
            maker_order_id: OrderId(1),
            taker_order_id: OrderId(2),
            price: Price(100),
            quantity: Quantity(3),
        };

        assert_eq!(trade.price, Price(100));
        assert_eq!(trade.quantity, Quantity(3));
    }

    #[test]
    fn order_ack_can_represent_accepted_order() {
        let ack = OrderAck::Accepted {
            command_id: CommandId(1),
            order_id: OrderId(10),
            journal_seq: JournalSeq(100),
        };

        assert_eq!(
            ack,
            OrderAck::Accepted {
                command_id: CommandId(1),
                order_id: OrderId(10),
                journal_seq: JournalSeq(100),
            }
        );
    }

    #[test]
    fn order_ack_can_represent_rejected_order() {
        let ack = OrderAck::Rejected {
            command_id: CommandId(1),
            order_id: Some(OrderId(10)),
            journal_seq: JournalSeq(100),
            reason: RejectReason::InvalidPrice,
        };

        assert_eq!(
            ack,
            OrderAck::Rejected {
                command_id: CommandId(1),
                order_id: Some(OrderId(10)),
                journal_seq: JournalSeq(100),
                reason: RejectReason::InvalidPrice,
            }
        );
    }

    #[test]
    fn order_ack_can_represent_cancelled_order() {
        let ack = OrderAck::Cancelled {
            command_id: CommandId(2),
            order_id: OrderId(10),
            journal_seq: JournalSeq(101),
        };

        assert_eq!(
            ack,
            OrderAck::Cancelled {
                command_id: CommandId(2),
                order_id: OrderId(10),
                journal_seq: JournalSeq(101),
            }
        );
    }

    #[test]
    fn trade_event_can_be_constructed() {
        let event = TradeEvent {
            trade_id: TradeId(1),
            market_seq: MarketSeq(1),
            command_id: CommandId(10),
            journal_seq: JournalSeq(100),
            maker_order_id: OrderId(1),
            taker_order_id: OrderId(2),
            price: Price(100),
            quantity: Quantity(3),
        };

        assert_eq!(event.trade_id, TradeId(1));
        assert_eq!(event.market_seq, MarketSeq(1));
        assert_eq!(event.command_id, CommandId(10));
        assert_eq!(event.journal_seq, JournalSeq(100));
        assert_eq!(event.price, Price(100));
        assert_eq!(event.quantity, Quantity(3));
    }

    #[test]
    fn market_event_can_represent_order_added() {
        let event = MarketEvent::OrderAdded(OrderAddedEvent {
            market_seq: MarketSeq(1),
            command_id: CommandId(10),
            journal_seq: JournalSeq(100),
            order_id: OrderId(1),
            side: Side::Buy,
            price: Price(100),
            quantity: Quantity(3),
        });

        assert_eq!(
            event,
            MarketEvent::OrderAdded(OrderAddedEvent {
                market_seq: MarketSeq(1),
                command_id: CommandId(10),
                journal_seq: JournalSeq(100),
                order_id: OrderId(1),
                side: Side::Buy,
                price: Price(100),
                quantity: Quantity(3),
            })
        );
    }

    #[test]
    fn engine_event_can_wrap_order_ack() {
        let ack = OrderAck::Accepted {
            command_id: CommandId(1),
            order_id: OrderId(10),
            journal_seq: JournalSeq(100),
        };

        let event = EngineEvent::OrderAck(ack.clone());

        assert_eq!(event, EngineEvent::OrderAck(ack));
    }

    #[test]
    fn engine_event_can_wrap_trade_event() {
        let trade = TradeEvent {
            trade_id: TradeId(1),
            market_seq: MarketSeq(1),
            command_id: CommandId(10),
            journal_seq: JournalSeq(100),
            maker_order_id: OrderId(1),
            taker_order_id: OrderId(2),
            price: Price(100),
            quantity: Quantity(3),
        };

        let event = EngineEvent::Trade(trade.clone());

        assert_eq!(event, EngineEvent::Trade(trade));
    }

    #[test]
    fn engine_event_can_wrap_market_event() {
        let market_event = MarketEvent::OrderCancelled(OrderCancelledEvent {
            market_seq: MarketSeq(1),
            command_id: CommandId(10),
            journal_seq: JournalSeq(100),
            order_id: OrderId(1),
            side: Side::Sell,
            price: Price(100),
            quantity: Quantity(3),
        });

        let event = EngineEvent::Market(market_event.clone());

        assert_eq!(event, EngineEvent::Market(market_event));
    }
}
