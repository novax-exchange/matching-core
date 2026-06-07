use super::price_level::PriceLevel;
use crate::types::{Order, OrderId, Side, Symbol};
use rust_decimal::Decimal;
use std::cmp::Reverse;
use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct BidPrice(Reverse<Decimal>);

pub struct OrderBook {
    symbol: Symbol,
    bids: BTreeMap<BidPrice, PriceLevel>,
    asks: BTreeMap<Decimal, PriceLevel>,
    order_index: HashMap<OrderId, (Side, Decimal)>,
}

impl OrderBook {
    pub fn new(symbol: Symbol) -> Self {
        OrderBook {
            symbol,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            order_index: HashMap::new(),
        }
    }

    pub fn insert(&mut self, order: Order) {
        let price = order.price;
        let side = order.side;
        self.order_index
            .insert(order.order_id.clone(), (side, price));
        match side {
            Side::Bid => {
                self.bids
                    .entry(BidPrice(Reverse(price)))
                    .or_insert_with(|| PriceLevel::new(price))
                    .push(order);
            }
            Side::Ask => {
                self.asks
                    .entry(price)
                    .or_insert_with(|| PriceLevel::new(price))
                    .push(order);
            }
        }
    }

    pub fn remove(&mut self, order_id: &OrderId) -> Option<Order> {
        let (side, price) = self.order_index.remove(order_id)?;
        match side {
            Side::Bid => {
                let bid_price = BidPrice(Reverse(price));
                let level = self.bids.get_mut(&bid_price)?;
                let order = level.remove(order_id)?;
                if level.is_empty() {
                    self.bids.remove(&bid_price);
                }
                Some(order)
            }
            Side::Ask => {
                let level = self.asks.get_mut(&price)?;
                let order = level.remove(order_id)?;
                if level.is_empty() {
                    self.asks.remove(&price);
                }
                Some(order)
            }
        }
    }

    pub fn best_bid(&self) -> Option<Decimal> {
        self.bids
            .keys()
            .next()
            .map(|BidPrice(Reverse(price))| *price)
    }

    pub fn best_ask(&self) -> Option<Decimal> {
        self.asks.keys().next().copied()
    }

    pub fn fill_best_opposing(
        &mut self,
        taker_side: Side,
        taker_price: Decimal,
        taker_remaining: Decimal,
    ) -> Option<(OrderId, Decimal, Decimal)> {
        match taker_side {
            Side::Bid => {
                let best_ask = *self.asks.keys().next()?;
                if taker_price < best_ask {
                    return None;
                }
                let level = self.asks.get_mut(&best_ask).unwrap();
                let (maker_id, fill) = {
                    let front = level.front()?;
                    (front.order_id.clone(), taker_remaining.min(front.remaining))
                };
                level.fill_front(fill);
                if level.is_empty() {
                    self.asks.remove(&best_ask);
                }
                if !self.order_still_resting(&maker_id) {
                    self.order_index.remove(&maker_id);
                }
                Some((maker_id, fill, best_ask))
            }
            Side::Ask => {
                let BidPrice(Reverse(best_bid)) = *self.bids.keys().next()?;
                if taker_price > best_bid {
                    return None;
                }
                let bid_price = BidPrice(Reverse(best_bid));
                let level = self.bids.get_mut(&bid_price).unwrap();
                let (maker_id, fill) = {
                    let front = level.front()?;
                    (front.order_id.clone(), taker_remaining.min(front.remaining))
                };
                level.fill_front(fill);
                if level.is_empty() {
                    self.bids.remove(&bid_price);
                }
                if !self.order_still_resting(&maker_id) {
                    self.order_index.remove(&maker_id);
                }
                Some((maker_id, fill, best_bid))
            }
        }
    }

    pub fn checksum(&self) -> u64 {
        let mut hash: u64 = 14695981039346656037;
        let prime: u64 = 1099511628211;
        let mix = |hash: &mut u64, value: &str| {
            for byte in value.bytes() {
                *hash ^= byte as u64;
                *hash = hash.wrapping_mul(prime);
            }
        };

        for (BidPrice(Reverse(price)), level) in &self.bids {
            mix(&mut hash, &format!("B{}:{}", price, level.total_quantity()));
        }
        for (price, level) in &self.asks {
            mix(&mut hash, &format!("A{}:{}", price, level.total_quantity()));
        }
        hash
    }

    pub fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    pub fn contains_order(&self, order_id: &OrderId) -> bool {
        self.order_index.contains_key(order_id)
    }

    pub fn depth_entries(&self) -> (Vec<(Decimal, Decimal)>, Vec<(Decimal, Decimal)>) {
        let bids = self
            .bids
            .iter()
            .map(|(BidPrice(Reverse(price)), level)| (*price, level.total_quantity()))
            .collect();
        let asks = self
            .asks
            .iter()
            .map(|(price, level)| (*price, level.total_quantity()))
            .collect();
        (bids, asks)
    }

    fn order_still_resting(&self, order_id: &OrderId) -> bool {
        let Some((side, price)) = self.order_index.get(order_id) else {
            return false;
        };
        match side {
            Side::Bid => self
                .bids
                .get(&BidPrice(Reverse(*price)))
                .and_then(PriceLevel::front)
                .is_some_and(|order| &order.order_id == order_id),
            Side::Ask => self
                .asks
                .get(price)
                .and_then(PriceLevel::front)
                .is_some_and(|order| &order.order_id == order_id),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{JournalSeq, Order, OrderId, OrderType, Side, Symbol};
    use rust_decimal_macros::dec;

    fn make_bid(id: u64, price: rust_decimal::Decimal, qty: rust_decimal::Decimal) -> Order {
        Order {
            order_id: OrderId(id),
            symbol: Symbol("BTCUSDT".into()),
            side: Side::Bid,
            order_type: OrderType::Limit,
            price,
            quantity: qty,
            remaining: qty,
            journal_seq: JournalSeq(id),
            timestamp_ns: id as i64,
        }
    }

    fn make_ask(id: u64, price: rust_decimal::Decimal, qty: rust_decimal::Decimal) -> Order {
        Order {
            side: Side::Ask,
            ..make_bid(id, price, qty)
        }
    }

    fn btc() -> Symbol {
        Symbol("BTCUSDT".into())
    }

    #[test]
    fn best_bid_is_highest() {
        let mut book = OrderBook::new(btc());
        book.insert(make_bid(1, dec!(99), dec!(1)));
        book.insert(make_bid(2, dec!(101), dec!(1)));
        book.insert(make_bid(3, dec!(100), dec!(1)));
        assert_eq!(book.best_bid(), Some(dec!(101)));
    }

    #[test]
    fn best_ask_is_lowest() {
        let mut book = OrderBook::new(btc());
        book.insert(make_ask(1, dec!(101), dec!(1)));
        book.insert(make_ask(2, dec!(99), dec!(1)));
        assert_eq!(book.best_ask(), Some(dec!(99)));
    }

    #[test]
    fn remove_order_updates_index() {
        let mut book = OrderBook::new(btc());
        book.insert(make_bid(1, dec!(100), dec!(5)));
        let removed = book.remove(&OrderId(1));
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().order_id, OrderId(1));
        assert!(book.best_bid().is_none());
    }

    #[test]
    fn fill_best_opposing_bid_matches_lowest_ask() {
        let mut book = OrderBook::new(btc());
        book.insert(make_ask(10, dec!(100), dec!(3)));

        let result = book.fill_best_opposing(Side::Bid, dec!(100), dec!(2));
        assert!(result.is_some());
        let (maker_id, fill_qty, fill_price) = result.unwrap();
        assert_eq!(maker_id, OrderId(10));
        assert_eq!(fill_qty, dec!(2));
        assert_eq!(fill_price, dec!(100));
        assert_eq!(book.best_ask(), Some(dec!(100)));
    }

    #[test]
    fn fill_best_opposing_removes_level_when_empty() {
        let mut book = OrderBook::new(btc());
        book.insert(make_ask(10, dec!(100), dec!(2)));
        book.fill_best_opposing(Side::Bid, dec!(100), dec!(2));
        assert!(book.best_ask().is_none());
    }

    #[test]
    fn fill_best_opposing_returns_none_when_price_not_crossed() {
        let mut book = OrderBook::new(btc());
        book.insert(make_ask(10, dec!(105), dec!(2)));
        let result = book.fill_best_opposing(Side::Bid, dec!(100), dec!(2));
        assert!(result.is_none());
    }

    #[test]
    fn checksum_is_deterministic() {
        let mut book1 = OrderBook::new(btc());
        book1.insert(make_bid(1, dec!(100), dec!(5)));

        let mut book2 = OrderBook::new(btc());
        book2.insert(make_bid(1, dec!(100), dec!(5)));
        assert_eq!(book1.checksum(), book2.checksum());
    }
}
