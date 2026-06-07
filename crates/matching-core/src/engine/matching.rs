use crate::engine::result::{MarketDataEvent, MatchResult, OrderAck, TradeEvent};
use crate::order_book::book::OrderBook;
use crate::types::*;
use rust_decimal::Decimal;

pub struct MatchingEngine {
    symbol: Symbol,
    order_book: OrderBook,
    next_trade_id: u64,
}

impl MatchingEngine {
    pub fn new(symbol: Symbol) -> Self {
        MatchingEngine {
            order_book: OrderBook::new(symbol.clone()),
            symbol,
            next_trade_id: 1,
        }
    }

    pub fn process(&mut self, command: MatchingCommand, seq: JournalSeq) -> MatchResult {
        match command {
            MatchingCommand::PlaceOrder(command) => self.place_order(command, seq),
            MatchingCommand::CancelOrder(command) => self.cancel_order(command, seq),
        }
    }

    pub fn order_book(&self) -> &OrderBook {
        &self.order_book
    }

    pub fn checksum(&self) -> u64 {
        self.order_book.checksum()
    }

    pub fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    pub fn export_state(&self) -> (Vec<(Decimal, Vec<Order>)>, Vec<(Decimal, Vec<Order>)>) {
        (self.order_book.export_bids(), self.order_book.export_asks())
    }

    pub fn from_state(
        symbol: Symbol,
        bids: Vec<(Decimal, Vec<Order>)>,
        asks: Vec<(Decimal, Vec<Order>)>,
        next_trade_id: u64,
    ) -> Self {
        MatchingEngine {
            order_book: OrderBook::from_levels(symbol.clone(), bids, asks),
            symbol,
            next_trade_id,
        }
    }

    pub fn next_trade_id(&self) -> u64 {
        self.next_trade_id
    }

    fn place_order(&mut self, command: OrderCommand, seq: JournalSeq) -> MatchResult {
        let mut taker = Order {
            order_id: command.order_id.clone(),
            symbol: command.symbol.clone(),
            side: command.side,
            order_type: command.order_type,
            price: command.price,
            quantity: command.quantity,
            remaining: command.quantity,
            journal_seq: seq,
            timestamp_ns: command.timestamp_ns,
        };

        let trades = self.match_against_book(&mut taker, seq);
        let filled_qty = command.quantity - taker.remaining;

        let order_ack = if taker.remaining == Decimal::ZERO {
            OrderAck::FullyFilled {
                order_id: command.order_id,
                filled_qty,
                journal_seq: seq,
            }
        } else if filled_qty > Decimal::ZERO {
            let remaining = taker.remaining;
            self.order_book.insert(taker);
            OrderAck::PartiallyFilled {
                order_id: command.order_id,
                filled_qty,
                remaining,
                journal_seq: seq,
            }
        } else {
            self.order_book.insert(taker);
            OrderAck::Accepted {
                order_id: command.order_id,
                journal_seq: seq,
            }
        };

        let market_event = trades.last().map(|trade| MarketDataEvent {
            symbol: command.symbol,
            last_price: trade.price,
            last_qty: trade.quantity,
            best_bid: self.order_book.best_bid(),
            best_ask: self.order_book.best_ask(),
            journal_seq: seq,
        });

        MatchResult {
            order_ack,
            trades,
            market_event,
        }
    }

    fn cancel_order(&mut self, command: CancelCommand, seq: JournalSeq) -> MatchResult {
        match self.order_book.remove(&command.order_id) {
            Some(order) => MatchResult {
                order_ack: OrderAck::Cancelled {
                    order_id: command.order_id,
                    remaining: order.remaining,
                    journal_seq: seq,
                },
                trades: Vec::new(),
                market_event: None,
            },
            None => MatchResult {
                order_ack: OrderAck::CancelRejected {
                    order_id: command.order_id,
                    command_id: command.command_id,
                    reason: CancelRejectReason::OrderNotFound,
                    journal_seq: seq,
                },
                trades: Vec::new(),
                market_event: None,
            },
        }
    }

    fn match_against_book(&mut self, taker: &mut Order, seq: JournalSeq) -> Vec<TradeEvent> {
        let mut trades = Vec::new();
        while taker.remaining > Decimal::ZERO {
            let Some((maker_id, fill, fill_price)) =
                self.order_book
                    .fill_best_opposing(taker.side, taker.price, taker.remaining)
            else {
                break;
            };

            taker.remaining -= fill;
            trades.push(TradeEvent {
                trade_id: TradeId(self.next_trade_id),
                symbol: taker.symbol.clone(),
                maker_id,
                taker_id: taker.order_id.clone(),
                price: fill_price,
                quantity: fill,
                journal_seq: seq,
                timestamp_ns: taker.timestamp_ns,
            });
            self.next_trade_id += 1;
        }
        trades
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::result::OrderAck;
    use rust_decimal_macros::dec;

    fn sym() -> Symbol {
        Symbol("BTCUSDT".into())
    }

    fn seq(n: u64) -> JournalSeq {
        JournalSeq(n)
    }

    fn bid_cmd(id: u64, price: rust_decimal::Decimal, qty: rust_decimal::Decimal) -> MatchingCommand {
        MatchingCommand::PlaceOrder(OrderCommand {
            command_id: CommandId(id),
            order_id: OrderId(id),
            symbol: sym(),
            side: Side::Bid,
            order_type: OrderType::Limit,
            price,
            quantity: qty,
            config_version: ConfigVersion(1),
            timestamp_ns: id as i64,
        })
    }

    fn ask_cmd(id: u64, price: rust_decimal::Decimal, qty: rust_decimal::Decimal) -> MatchingCommand {
        MatchingCommand::PlaceOrder(OrderCommand {
            command_id: CommandId(id),
            order_id: OrderId(id),
            symbol: sym(),
            side: Side::Ask,
            order_type: OrderType::Limit,
            price,
            quantity: qty,
            config_version: ConfigVersion(1),
            timestamp_ns: id as i64,
        })
    }

    fn cancel_cmd(cmd_id: u64, order_id: u64) -> MatchingCommand {
        MatchingCommand::CancelOrder(CancelCommand {
            command_id: CommandId(cmd_id),
            order_id: OrderId(order_id),
            symbol: sym(),
            config_version: ConfigVersion(1),
            timestamp_ns: 0,
        })
    }

    #[test]
    fn bid_no_asks_rests_in_book() {
        let mut engine = MatchingEngine::new(sym());
        let result = engine.process(bid_cmd(1, dec!(100), dec!(5)), seq(1));
        assert!(result.trades.is_empty());
        match result.order_ack {
            OrderAck::Accepted { order_id, .. } => assert_eq!(order_id, OrderId(1)),
            other => panic!("expected Accepted, got {:?}", other),
        }
    }

    #[test]
    fn ask_fully_fills_resting_bid() {
        let mut engine = MatchingEngine::new(sym());
        engine.process(bid_cmd(1, dec!(100), dec!(5)), seq(1));
        let result = engine.process(ask_cmd(2, dec!(100), dec!(5)), seq(2));
        assert_eq!(result.trades.len(), 1);
        let trade = &result.trades[0];
        assert_eq!(trade.price, dec!(100));
        assert_eq!(trade.quantity, dec!(5));
        assert_eq!(trade.maker_id, OrderId(1));
        assert_eq!(trade.taker_id, OrderId(2));
        match result.order_ack {
            OrderAck::FullyFilled {
                order_id,
                filled_qty,
                ..
            } => {
                assert_eq!(order_id, OrderId(2));
                assert_eq!(filled_qty, dec!(5));
            }
            other => panic!("expected FullyFilled, got {:?}", other),
        }
    }

    #[test]
    fn ask_partially_fills_and_rests_remainder() {
        let mut engine = MatchingEngine::new(sym());
        engine.process(bid_cmd(1, dec!(100), dec!(3)), seq(1));
        let result = engine.process(ask_cmd(2, dec!(100), dec!(5)), seq(2));
        assert_eq!(result.trades.len(), 1);
        assert_eq!(result.trades[0].quantity, dec!(3));
        match result.order_ack {
            OrderAck::PartiallyFilled {
                order_id,
                filled_qty,
                remaining,
                ..
            } => {
                assert_eq!(order_id, OrderId(2));
                assert_eq!(filled_qty, dec!(3));
                assert_eq!(remaining, dec!(2));
            }
            other => panic!("expected PartiallyFilled, got {:?}", other),
        }
    }

    #[test]
    fn price_priority_best_bid_matched_first() {
        let mut engine = MatchingEngine::new(sym());
        engine.process(bid_cmd(1, dec!(99), dec!(2)), seq(1));
        engine.process(bid_cmd(2, dec!(101), dec!(2)), seq(2));
        let result = engine.process(ask_cmd(3, dec!(99), dec!(2)), seq(3));
        assert_eq!(result.trades[0].maker_id, OrderId(2));
        assert_eq!(result.trades[0].price, dec!(101));
    }

    #[test]
    fn time_priority_same_price_fifo() {
        let mut engine = MatchingEngine::new(sym());
        engine.process(bid_cmd(1, dec!(100), dec!(2)), seq(1));
        engine.process(bid_cmd(2, dec!(100), dec!(2)), seq(2));
        let result = engine.process(ask_cmd(3, dec!(100), dec!(2)), seq(3));
        assert_eq!(result.trades[0].maker_id, OrderId(1));
    }

    #[test]
    fn cancel_removes_resting_order() {
        let mut engine = MatchingEngine::new(sym());
        engine.process(bid_cmd(1, dec!(100), dec!(5)), seq(1));
        let result = engine.process(cancel_cmd(2, 1), seq(2));
        assert!(result.trades.is_empty());
        match result.order_ack {
            OrderAck::Cancelled {
                order_id,
                remaining,
                ..
            } => {
                assert_eq!(order_id, OrderId(1));
                assert_eq!(remaining, dec!(5));
            }
            other => panic!("expected Cancelled, got {:?}", other),
        }
    }

    #[test]
    fn cancel_not_found_returns_reject() {
        let mut engine = MatchingEngine::new(sym());
        let result = engine.process(cancel_cmd(1, 999), seq(1));
        match result.order_ack {
            OrderAck::CancelRejected { reason, .. } => {
                assert_eq!(reason, CancelRejectReason::OrderNotFound);
            }
            other => panic!("expected CancelRejected, got {:?}", other),
        }
    }

    #[test]
    fn trade_ids_are_monotonically_increasing() {
        let mut engine = MatchingEngine::new(sym());
        engine.process(bid_cmd(1, dec!(100), dec!(5)), seq(1));
        engine.process(bid_cmd(2, dec!(100), dec!(5)), seq(2));
        let first = engine.process(ask_cmd(3, dec!(100), dec!(3)), seq(3));
        let second = engine.process(ask_cmd(4, dec!(100), dec!(3)), seq(4));
        assert!(first.trades[0].trade_id.0 < second.trades[0].trade_id.0);
    }
}
