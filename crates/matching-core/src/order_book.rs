use crate::{
    engine::{MatchResult, Trade},
    order::Order,
    types::{Checksum, OrderId, Price, Side, Symbol},
};
use slotmap::{new_key_type, SlotMap};
use std::collections::{BTreeMap, HashMap};

new_key_type! {
    pub struct OrderNodeKey;
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OrderNode {
    order: Order,
    prev: Option<OrderNodeKey>,
    next: Option<OrderNodeKey>,
}

#[derive(Clone)]
pub struct PriceLevel {
    price: Price,
    head: Option<OrderNodeKey>,
    tail: Option<OrderNodeKey>,
    nodes: SlotMap<OrderNodeKey, OrderNode>,
}

#[derive(Clone)]
pub struct OrderBook {
    symbol: Symbol,
    bids: BTreeMap<Price, PriceLevel>,
    asks: BTreeMap<Price, PriceLevel>,
    order_index: HashMap<OrderId, OrderLocation>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OrderLocation {
    side: Side,
    price: Price,
    node_key: OrderNodeKey,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CancelError {
    OrderNotFound,
}

impl OrderBook {
    pub fn new(symbol: Symbol) -> Self {
        OrderBook {
            symbol: symbol,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            order_index: HashMap::new(),
        }
    }

    pub fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    pub fn best_ask(&self) -> Option<&Price> {
        self.asks.keys().next()
    }

    pub fn best_bid(&self) -> Option<&Price> {
        self.bids.keys().next_back()
    }

    pub fn insert(&mut self, order: Order) -> OrderNodeKey {
        let side = order.side;
        let price = order.price;
        let order_id = order.order_id;

        let levels = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };

        let level = levels
            .entry(price)
            .or_insert_with(|| PriceLevel::new(price));

        let node_key = level.push_back(order);

        self.order_index.insert(
            order_id,
            OrderLocation {
                side,
                price,
                node_key,
            },
        );

        node_key
    }

    pub fn level_front(&self, side: Side, price: Price) -> Option<&Order> {
        let levels = match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
        };

        levels.get(&price)?.front()
    }

    pub fn contains_order(&self, order_id: OrderId) -> bool {
        self.order_index.contains_key(&order_id)
    }

    pub fn resting_orders(&self) -> Vec<Order> {
        let mut orders = Vec::new();

        for level in self.bids.values() {
            orders.extend(level.orders().into_iter().cloned());
        }

        for level in self.asks.values() {
            orders.extend(level.orders().into_iter().cloned());
        }

        orders
    }

    pub fn place_limit(&mut self, mut order: Order) -> MatchResult {
        let mut result = MatchResult {
            trades: Vec::new(),
            resting_order_id: None,
        };

        match order.side {
            Side::Buy => {
                while !order.is_filled() {
                    let best_ask = match self.best_ask().copied() {
                        Some(price) if price <= order.price => price,
                        _ => break,
                    };

                    let (removed_maker_id, ask_level_empty) = {
                        let level = self
                            .asks
                            .get_mut(&best_ask)
                            .expect("best ask level must exist");

                        let removed_maker_id = Self::match_order_against_level(
                            &mut order,
                            level,
                            best_ask,
                            &mut result.trades,
                        );
                        let level_empty = level.front().is_none();

                        (removed_maker_id, level_empty)
                    };

                    if let Some(maker_order_id) = removed_maker_id {
                        self.order_index.remove(&maker_order_id);
                    }

                    if ask_level_empty {
                        self.asks.remove(&best_ask);
                    }
                }

                if !order.is_filled() {
                    let order_id = order.order_id;
                    self.insert(order);
                    result.resting_order_id = Some(order_id);
                }
            }
            Side::Sell => {
                while !order.is_filled() {
                    let best_bid = match self.best_bid().copied() {
                        Some(price) if price >= order.price => price,
                        _ => break,
                    };

                    let (removed_maker_id, bid_level_empty) = {
                        let level = self
                            .bids
                            .get_mut(&best_bid)
                            .expect("best bid level must exist");

                        let removed_maker_id = Self::match_order_against_level(
                            &mut order,
                            level,
                            best_bid,
                            &mut result.trades,
                        );
                        let level_empty = level.front().is_none();

                        (removed_maker_id, level_empty)
                    };

                    if let Some(maker_order_id) = removed_maker_id {
                        self.order_index.remove(&maker_order_id);
                    }

                    if bid_level_empty {
                        self.bids.remove(&best_bid);
                    }
                }

                if !order.is_filled() {
                    let order_id = order.order_id;
                    self.insert(order);
                    result.resting_order_id = Some(order_id);
                }
            }
        }
        result
    }

    fn match_order_against_level(
        order: &mut Order,
        level: &mut PriceLevel,
        price: Price,
        trades: &mut Vec<Trade>,
    ) -> Option<OrderId> {
        let maker_snapshot = level.front().expect("maker must exist").clone();

        if maker_snapshot.quantity <= order.quantity {
            let maker = level.pop_front().expect("maker level must have an order");
            let trade_quantity = maker.quantity;

            order
                .reduce_quantity(trade_quantity)
                .expect("trade quantity must not exceed taker quantity");

            trades.push(Trade {
                maker_order_id: maker.order_id,
                taker_order_id: order.order_id,
                price,
                quantity: trade_quantity,
            });

            Some(maker.order_id)
        } else {
            let trade_quantity = order.quantity;
            let taker_order_id = order.order_id;
            let maker = level.front_mut().expect("maker must exist");

            maker
                .reduce_quantity(trade_quantity)
                .expect("trade quantity must not exceed maker quantity");
            order
                .reduce_quantity(trade_quantity)
                .expect("trade quantity must not exceed taker quantity");

            trades.push(Trade {
                maker_order_id: maker.order_id,
                taker_order_id,
                price,
                quantity: trade_quantity,
            });

            None
        }
    }

    pub fn cancel(&mut self, order_id: OrderId) -> Result<Order, CancelError> {
        let location = self
            .order_index
            .remove(&order_id)
            .ok_or(CancelError::OrderNotFound)?;

        let levels = match location.side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };

        let level = levels
            .get_mut(&location.price)
            .expect("indexed order price level must exist");

        let order = level
            .remove(location.node_key)
            .expect("indexed order node must exist");

        if level.front().is_none() {
            levels.remove(&location.price);
        }

        Ok(order)
    }

    pub fn checksum(&self) -> Checksum {
        let mut value = 0_u64;

        for (price, level) in &self.bids {
            value = value.wrapping_add(1);
            value = value.wrapping_mul(31).wrapping_add(price.0);
            for order in level.orders() {
                value = value.wrapping_mul(31).wrapping_add(order.order_id.0);
                value = value.wrapping_mul(31).wrapping_add(order.quantity.0);
            }
        }

        for (price, level) in &self.asks {
            value = value.wrapping_add(2);
            value = value.wrapping_mul(31).wrapping_add(price.0);
            for order in level.orders() {
                value = value.wrapping_mul(31).wrapping_add(order.order_id.0);
                value = value.wrapping_mul(31).wrapping_add(order.quantity.0);
            }
        }

        Checksum(value)
    }
}

impl PriceLevel {
    pub fn new(price: Price) -> Self {
        Self {
            price: price,
            head: None,
            tail: None,
            nodes: SlotMap::with_key(),
        }
    }

    pub fn price(&self) -> Price {
        self.price
    }

    pub fn front(&self) -> Option<&Order> {
        let head = self.head?;
        self.nodes.get(head).map(|node| &node.order)
    }

    pub fn front_mut(&mut self) -> Option<&mut Order> {
        let head = self.head?;
        self.nodes.get_mut(head).map(|node| &mut node.order)
    }

    pub fn push_back(&mut self, order: Order) -> OrderNodeKey {
        let old_tail = self.tail;
        let new_key = self.nodes.insert(OrderNode {
            order: order,
            prev: old_tail,
            next: None,
        });

        if let Some(old_tail) = old_tail {
            let old_tail_node = self.nodes.get_mut(old_tail).expect("tail key must exist");
            old_tail_node.next = Some(new_key);
        } else {
            self.head = Some(new_key);
        }

        self.tail = Some(new_key);
        new_key
    }

    pub fn pop_front(&mut self) -> Option<Order> {
        let old_head = self.head?;
        let old_head_node = self.nodes.remove(old_head)?;

        self.head = old_head_node.next;

        if let Some(new_head) = self.head {
            let new_head_node = self.nodes.get_mut(new_head).expect("head key must exists");
            new_head_node.prev = None;
        } else {
            self.tail = None;
        }

        Some(old_head_node.order)
    }

    pub fn get(&self, key: OrderNodeKey) -> Option<&Order> {
        self.nodes.get(key).map(|node| &node.order)
    }

    pub fn remove(&mut self, key: OrderNodeKey) -> Option<Order> {
        let node = self.nodes.remove(key)?;

        match node.prev {
            Some(prev) => {
                self.nodes.get_mut(prev)?.next = node.next;
            }
            None => {
                self.head = node.next;
            }
        }

        match node.next {
            Some(next) => {
                self.nodes.get_mut(next)?.prev = node.prev;
            }
            None => {
                self.tail = node.prev;
            }
        }

        Some(node.order)
    }

    pub fn orders(&self) -> Vec<&Order> {
        let mut orders = Vec::new();
        let mut current = self.head;

        while let Some(key) = current {
            let node = self
                .nodes
                .get(key)
                .expect("price level link must point to exist node");
            orders.push(&node.order);
            current = node.next;
        }

        orders
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::order::*;
    use crate::types::*;
    use slotmap::SlotMap;

    #[test]
    fn slotmap_can_store_order_nodes() {
        let mut nodes: SlotMap<OrderNodeKey, OrderNode> = SlotMap::with_key();

        // 后面再 insert
        let order = make_order(1);
        let order_node = OrderNode {
            order: order,
            prev: None,
            next: None,
        };
        let key: OrderNodeKey = nodes.insert(order_node.clone());
        let node = nodes.get(key);

        assert_eq!(node, Some(&order_node));
    }

    #[test]
    fn price_level_can_be_created_empty() {
        let level = PriceLevel::new(Price(100));

        assert_eq!(level.price(), Price(100));
        assert!(level.front().is_none());
    }

    #[test]
    fn push_back_keeps_first_order_at_front() {
        let mut level = PriceLevel::new(Price(100));

        level.push_back(make_order(1));
        level.push_back(make_order(2));

        assert_eq!(level.front().unwrap().order_id, OrderId(1));
    }

    #[test]
    fn pop_front_returns_orders_in_fifo_order() {
        let mut level = PriceLevel::new(Price(100));

        level.push_back(make_order(1));
        level.push_back(make_order(2));

        assert_eq!(level.pop_front().unwrap().order_id, OrderId(1));
        assert_eq!(level.pop_front().unwrap().order_id, OrderId(2));
        assert!(level.pop_front().is_none());
    }

    #[test]
    fn push_back_returns_key_that_can_find_order() {
        let mut level = PriceLevel::new(Price(100));

        let key = level.push_back(make_order(42));

        assert_eq!(level.get(key).unwrap().order_id, OrderId(42));
    }

    fn make_order(id: u64) -> Order {
        Order {
            order_id: OrderId(id),
            symbol: Symbol("BTC-USDT".to_string()),
            side: Side::Buy,
            price: Price(100),
            quantity: Quantity(5),
        }
    }

    #[test]
    fn order_book_starts_empty() {
        let book = OrderBook::new(Symbol("BTC-USDT".to_string()));

        assert_eq!(book.symbol(), &Symbol("BTC-USDT".to_string()));
        assert!(book.best_bid().is_none());
        assert!(book.best_ask().is_none());
    }

    #[test]
    fn order_book_tracks_best_bid_and_best_ask_after_insert() {
        let mut book = OrderBook::new(Symbol("BTC-USDT".to_string()));

        book.insert(make_order_with_side_and_price(1, Side::Buy, Price(100)));
        book.insert(make_order_with_side_and_price(2, Side::Buy, Price(101)));
        book.insert(make_order_with_side_and_price(3, Side::Sell, Price(105)));
        book.insert(make_order_with_side_and_price(4, Side::Sell, Price(104)));

        assert_eq!(book.best_bid(), Some(&Price(101)));
        assert_eq!(book.best_ask(), Some(&Price(104)));
    }

    fn make_order_with_side_and_price(id: u64, side: Side, price: Price) -> Order {
        Order {
            order_id: OrderId(id),
            symbol: Symbol("BTC-USDT".to_string()),
            side,
            price,
            quantity: Quantity(5),
        }
    }

    #[test]
    fn order_book_keeps_fifo_within_same_price_level() {
        let mut book = OrderBook::new(Symbol("BTC-USDT".to_string()));

        book.insert(make_order_with_side_and_price(1, Side::Buy, Price(100)));
        book.insert(make_order_with_side_and_price(2, Side::Buy, Price(100)));

        let front = book.level_front(Side::Buy, Price(100)).unwrap();

        assert_eq!(front.order_id, OrderId(1));
    }

    #[test]
    fn order_book_indexes_inserted_orders() {
        let mut book = OrderBook::new(Symbol("BTC-USDT".to_string()));

        book.insert(make_order_with_side_and_price(1, Side::Buy, Price(100)));

        assert!(book.contains_order(OrderId(1)));
        assert!(!book.contains_order(OrderId(2)));
    }

    #[test]
    fn buy_limit_order_fully_matches_best_ask_at_same_price() {
        let mut book = OrderBook::new(Symbol("BTC-USDT".to_string()));

        book.insert(make_order_with_side_and_price_and_quantity(
            1,
            Side::Sell,
            Price(100),
            Quantity(3),
        ));

        let result = book.place_limit(make_order_with_side_and_price_and_quantity(
            2,
            Side::Buy,
            Price(100),
            Quantity(3),
        ));

        assert_eq!(
            result.trades,
            vec![Trade {
                maker_order_id: OrderId(1),
                taker_order_id: OrderId(2),
                price: Price(100),
                quantity: Quantity(3),
            }]
        );

        assert_eq!(result.resting_order_id, None);
        assert!(book.best_ask().is_none());
        assert!(!book.contains_order(OrderId(1)));
    }

    fn make_order_with_side_and_price_and_quantity(
        id: u64,
        side: Side,
        price: Price,
        quantity: Quantity,
    ) -> Order {
        Order {
            order_id: OrderId(id),
            symbol: Symbol("BTC-USDT".to_string()),
            side,
            price,
            quantity,
        }
    }

    #[test]
    fn buy_limit_order_rests_remaining_quantity_after_partial_match() {
        let mut book = OrderBook::new(Symbol("BTC-USDT".to_string()));

        book.insert(make_order_with_side_and_price_and_quantity(
            1,
            Side::Sell,
            Price(100),
            Quantity(3),
        ));

        let result = book.place_limit(make_order_with_side_and_price_and_quantity(
            2,
            Side::Buy,
            Price(100),
            Quantity(5),
        ));

        assert_eq!(
            result.trades,
            vec![Trade {
                maker_order_id: OrderId(1),
                taker_order_id: OrderId(2),
                price: Price(100),
                quantity: Quantity(3),
            }]
        );

        assert_eq!(result.resting_order_id, Some(OrderId(2)));
        assert_eq!(book.best_bid(), Some(&Price(100)));

        let resting = book.level_front(Side::Buy, Price(100)).unwrap();
        assert_eq!(resting.order_id, OrderId(2));
        assert_eq!(resting.quantity, Quantity(2));
    }

    #[test]
    fn buy_limit_order_partially_fills_maker_and_keeps_remaining_maker() {
        let mut book = OrderBook::new(Symbol("BTC-USDT".to_string()));

        book.insert(make_order_with_side_and_price_and_quantity(
            1,
            Side::Sell,
            Price(100),
            Quantity(5),
        ));

        let result = book.place_limit(make_order_with_side_and_price_and_quantity(
            2,
            Side::Buy,
            Price(100),
            Quantity(3),
        ));

        assert_eq!(
            result.trades,
            vec![Trade {
                maker_order_id: OrderId(1),
                taker_order_id: OrderId(2),
                price: Price(100),
                quantity: Quantity(3),
            }]
        );

        assert_eq!(result.resting_order_id, None);
        assert_eq!(book.best_ask(), Some(&Price(100)));

        let maker = book.level_front(Side::Sell, Price(100)).unwrap();
        assert_eq!(maker.order_id, OrderId(1));
        assert_eq!(maker.quantity, Quantity(2));
        assert!(book.contains_order(OrderId(1)));
    }

    #[test]
    fn sell_limit_order_fully_matches_best_bid_at_same_price() {
        let mut book = OrderBook::new(Symbol("BTC-USDT".to_string()));

        book.insert(make_order_with_side_and_price_and_quantity(
            1,
            Side::Buy,
            Price(100),
            Quantity(3),
        ));

        let result = book.place_limit(make_order_with_side_and_price_and_quantity(
            2,
            Side::Sell,
            Price(100),
            Quantity(3),
        ));

        assert_eq!(
            result.trades,
            vec![Trade {
                maker_order_id: OrderId(1),
                taker_order_id: OrderId(2),
                price: Price(100),
                quantity: Quantity(3),
            }]
        );

        assert_eq!(result.resting_order_id, None);
        assert!(book.best_bid().is_none());
        assert!(!book.contains_order(OrderId(1)));
    }

    #[test]
    fn sell_limit_order_rests_remaining_quantity_after_partial_match() {
        let mut book = OrderBook::new(Symbol("BTC-USDT".to_string()));

        book.insert(make_order_with_side_and_price_and_quantity(
            1,
            Side::Buy,
            Price(100),
            Quantity(3),
        ));

        let result = book.place_limit(make_order_with_side_and_price_and_quantity(
            2,
            Side::Sell,
            Price(100),
            Quantity(5),
        ));

        assert_eq!(
            result.trades,
            vec![Trade {
                maker_order_id: OrderId(1),
                taker_order_id: OrderId(2),
                price: Price(100),
                quantity: Quantity(3),
            }]
        );

        assert_eq!(result.resting_order_id, Some(OrderId(2)));
        assert_eq!(book.best_ask(), Some(&Price(100)));

        let resting = book.level_front(Side::Sell, Price(100)).unwrap();
        assert_eq!(resting.order_id, OrderId(2));
        assert_eq!(resting.quantity, Quantity(2));
    }

    #[test]
    fn sell_limit_order_partially_fills_maker_and_keeps_remaining_maker() {
        let mut book = OrderBook::new(Symbol("BTC-USDT".to_string()));

        book.insert(make_order_with_side_and_price_and_quantity(
            1,
            Side::Buy,
            Price(100),
            Quantity(5),
        ));

        let result = book.place_limit(make_order_with_side_and_price_and_quantity(
            2,
            Side::Sell,
            Price(100),
            Quantity(3),
        ));

        assert_eq!(
            result.trades,
            vec![Trade {
                maker_order_id: OrderId(1),
                taker_order_id: OrderId(2),
                price: Price(100),
                quantity: Quantity(3),
            }]
        );

        assert_eq!(result.resting_order_id, None);
        assert_eq!(book.best_bid(), Some(&Price(100)));

        let maker = book.level_front(Side::Buy, Price(100)).unwrap();
        assert_eq!(maker.order_id, OrderId(1));
        assert_eq!(maker.quantity, Quantity(2));
        assert!(book.contains_order(OrderId(1)));
    }

    #[test]
    fn buy_limit_order_matches_across_multiple_ask_levels() {
        let mut book = OrderBook::new(Symbol("BTC-USDT".to_string()));

        book.insert(make_order_with_side_and_price_and_quantity(
            1,
            Side::Sell,
            Price(100),
            Quantity(2),
        ));
        book.insert(make_order_with_side_and_price_and_quantity(
            2,
            Side::Sell,
            Price(101),
            Quantity(2),
        ));

        let result = book.place_limit(make_order_with_side_and_price_and_quantity(
            3,
            Side::Buy,
            Price(101),
            Quantity(4),
        ));

        assert_eq!(
            result.trades,
            vec![
                Trade {
                    maker_order_id: OrderId(1),
                    taker_order_id: OrderId(3),
                    price: Price(100),
                    quantity: Quantity(2),
                },
                Trade {
                    maker_order_id: OrderId(2),
                    taker_order_id: OrderId(3),
                    price: Price(101),
                    quantity: Quantity(2),
                },
            ]
        );

        assert_eq!(result.resting_order_id, None);
        assert!(book.best_ask().is_none());
        assert!(!book.contains_order(OrderId(1)));
        assert!(!book.contains_order(OrderId(2)));
    }

    #[test]
    fn sell_limit_order_matches_across_multiple_bid_levels() {
        let mut book = OrderBook::new(Symbol("BTC-USDT".to_string()));

        book.insert(make_order_with_side_and_price_and_quantity(
            1,
            Side::Buy,
            Price(101),
            Quantity(2),
        ));
        book.insert(make_order_with_side_and_price_and_quantity(
            2,
            Side::Buy,
            Price(100),
            Quantity(2),
        ));

        let result = book.place_limit(make_order_with_side_and_price_and_quantity(
            3,
            Side::Sell,
            Price(100),
            Quantity(4),
        ));

        assert_eq!(
            result.trades,
            vec![
                Trade {
                    maker_order_id: OrderId(1),
                    taker_order_id: OrderId(3),
                    price: Price(101),
                    quantity: Quantity(2),
                },
                Trade {
                    maker_order_id: OrderId(2),
                    taker_order_id: OrderId(3),
                    price: Price(100),
                    quantity: Quantity(2),
                },
            ]
        );

        assert_eq!(result.resting_order_id, None);
        assert!(book.best_bid().is_none());
        assert!(!book.contains_order(OrderId(1)));
        assert!(!book.contains_order(OrderId(2)));
    }

    #[test]
    fn price_level_can_remove_middle_order_by_key() {
        let mut level = PriceLevel::new(Price(100));

        let first = level.push_back(make_order(1));
        let second = level.push_back(make_order(2));
        let third = level.push_back(make_order(3));

        let removed = level.remove(second).unwrap();

        assert_eq!(removed.order_id, OrderId(2));
        assert_eq!(level.pop_front().unwrap().order_id, OrderId(1));
        assert_eq!(level.pop_front().unwrap().order_id, OrderId(3));
        assert!(level.pop_front().is_none());

        assert!(level.get(first).is_none());
        assert!(level.get(third).is_none());
    }

    #[test]
    fn price_level_can_remove_head_order_by_key() {
        let mut level = PriceLevel::new(Price(100));

        let first = level.push_back(make_order(1));
        level.push_back(make_order(2));

        let removed = level.remove(first).unwrap();

        assert_eq!(removed.order_id, OrderId(1));
        assert_eq!(level.front().unwrap().order_id, OrderId(2));
    }

    #[test]
    fn price_level_can_remove_tail_order_by_key() {
        let mut level = PriceLevel::new(Price(100));

        level.push_back(make_order(1));
        let second = level.push_back(make_order(2));

        let removed = level.remove(second).unwrap();

        assert_eq!(removed.order_id, OrderId(2));
        assert_eq!(level.pop_front().unwrap().order_id, OrderId(1));
        assert!(level.pop_front().is_none());
    }

    #[test]
    fn price_level_can_remove_only_order_by_key() {
        let mut level = PriceLevel::new(Price(100));

        let only = level.push_back(make_order(1));

        let removed = level.remove(only).unwrap();

        assert_eq!(removed.order_id, OrderId(1));
        assert!(level.front().is_none());
        assert!(level.pop_front().is_none());
    }

    #[test]
    fn order_book_can_cancel_existing_bid_order() {
        let mut book = OrderBook::new(Symbol("BTC-USDT".to_string()));

        book.insert(make_order_with_side_and_price_and_quantity(
            1,
            Side::Buy,
            Price(100),
            Quantity(5),
        ));

        let cancelled = book.cancel(OrderId(1)).unwrap();

        assert_eq!(cancelled.order_id, OrderId(1));
        assert!(!book.contains_order(OrderId(1)));
        assert!(book.best_bid().is_none());
    }

    #[test]
    fn order_book_can_cancel_existing_ask_order() {
        let mut book = OrderBook::new(Symbol("BTC-USDT".to_string()));

        book.insert(make_order_with_side_and_price_and_quantity(
            1,
            Side::Sell,
            Price(100),
            Quantity(5),
        ));

        let cancelled = book.cancel(OrderId(1)).unwrap();

        assert_eq!(cancelled.order_id, OrderId(1));
        assert!(!book.contains_order(OrderId(1)));
        assert!(book.best_ask().is_none());
    }

    #[test]
    fn cancelling_unknown_order_returns_error() {
        let mut book = OrderBook::new(Symbol("BTC-USDT".to_string()));

        let result = book.cancel(OrderId(999));

        assert_eq!(result, Err(CancelError::OrderNotFound));
    }

    #[test]
    fn cancelling_middle_order_keeps_price_level_fifo_order() {
        let mut book = OrderBook::new(Symbol("BTC-USDT".to_string()));

        book.insert(make_order_with_side_and_price_and_quantity(
            1,
            Side::Buy,
            Price(100),
            Quantity(5),
        ));
        book.insert(make_order_with_side_and_price_and_quantity(
            2,
            Side::Buy,
            Price(100),
            Quantity(5),
        ));
        book.insert(make_order_with_side_and_price_and_quantity(
            3,
            Side::Buy,
            Price(100),
            Quantity(5),
        ));

        let cancelled = book.cancel(OrderId(2)).unwrap();

        assert_eq!(cancelled.order_id, OrderId(2));
        assert!(!book.contains_order(OrderId(2)));

        let first = book.cancel(OrderId(1)).unwrap();
        let third = book.cancel(OrderId(3)).unwrap();

        assert_eq!(first.order_id, OrderId(1));
        assert_eq!(third.order_id, OrderId(3));
        assert!(book.best_bid().is_none());
    }

    #[test]
    fn same_order_book_state_has_same_checksum() {
        let mut first = OrderBook::new(Symbol("BTC-USDT".to_string()));
        let mut second = OrderBook::new(Symbol("BTC-USDT".to_string()));

        first.insert(Order {
            order_id: OrderId(1),
            symbol: Symbol("BTC-USDT".to_string()),
            side: Side::Buy,
            price: Price(100),
            quantity: Quantity(5),
        });

        second.insert(Order {
            order_id: OrderId(1),
            symbol: Symbol("BTC-USDT".to_string()),
            side: Side::Buy,
            price: Price(100),
            quantity: Quantity(5),
        });

        assert_eq!(first.checksum(), second.checksum());
    }

    #[test]
    fn different_order_book_state_has_different_checksum() {
        let mut first = OrderBook::new(Symbol("BTC-USDT".to_string()));
        let mut second = OrderBook::new(Symbol("BTC-USDT".to_string()));

        first.insert(Order {
            order_id: OrderId(1),
            symbol: Symbol("BTC-USDT".to_string()),
            side: Side::Buy,
            price: Price(100),
            quantity: Quantity(5),
        });

        second.insert(Order {
            order_id: OrderId(1),
            symbol: Symbol("BTC-USDT".to_string()),
            side: Side::Buy,
            price: Price(101),
            quantity: Quantity(5),
        });

        assert_ne!(first.checksum(), second.checksum());
    }

    #[test]
    fn same_input_sequence_produces_same_checksum() {
        let mut first = OrderBook::new(Symbol("BTC-USDT".to_string()));
        let mut second = OrderBook::new(Symbol("BTC-USDT".to_string()));

        let orders = vec![
            Order {
                order_id: OrderId(1),
                symbol: Symbol("BTC-USDT".to_string()),
                side: Side::Buy,
                price: Price(100),
                quantity: Quantity(5),
            },
            Order {
                order_id: OrderId(2),
                symbol: Symbol("BTC-USDT".to_string()),
                side: Side::Sell,
                price: Price(110),
                quantity: Quantity(3),
            },
        ];

        for order in orders.clone() {
            first.insert(order);
        }

        for order in orders {
            second.insert(order);
        }

        assert_eq!(first.checksum(), second.checksum());
    }

    #[test]
    fn checksum_includes_all_orders_at_same_price_level() {
        let mut first = OrderBook::new(Symbol("BTC-USDT".to_string()));
        let mut second = OrderBook::new(Symbol("BTC-USDT".to_string()));

        first.insert(make_order_with_side_and_price_and_quantity(
            1,
            Side::Buy,
            Price(100),
            Quantity(5),
        ));
        first.insert(make_order_with_side_and_price_and_quantity(
            2,
            Side::Buy,
            Price(100),
            Quantity(5),
        ));

        second.insert(make_order_with_side_and_price_and_quantity(
            1,
            Side::Buy,
            Price(100),
            Quantity(5),
        ));
        second.insert(make_order_with_side_and_price_and_quantity(
            3,
            Side::Buy,
            Price(100),
            Quantity(5),
        ));

        assert_ne!(first.checksum(), second.checksum());
    }

    #[test]
    fn checksum_distinguishes_bid_and_ask_sides() {
        let mut bid_book = OrderBook::new(Symbol("BTC-USDT".to_string()));
        let mut ask_book = OrderBook::new(Symbol("BTC-USDT".to_string()));

        bid_book.insert(make_order_with_side_and_price_and_quantity(
            1,
            Side::Buy,
            Price(100),
            Quantity(5),
        ));

        ask_book.insert(make_order_with_side_and_price_and_quantity(
            1,
            Side::Sell,
            Price(100),
            Quantity(5),
        ));

        assert_ne!(bid_book.checksum(), ask_book.checksum());
    }

    #[test]
    fn checksum_distinguishes_fifo_order_at_same_price_level() {
        let mut first = OrderBook::new(Symbol("BTC-USDT".to_string()));
        let mut second = OrderBook::new(Symbol("BTC-USDT".to_string()));

        first.insert(make_order_with_side_and_price_and_quantity(
            1,
            Side::Buy,
            Price(100),
            Quantity(5),
        ));
        first.insert(make_order_with_side_and_price_and_quantity(
            2,
            Side::Buy,
            Price(100),
            Quantity(5),
        ));

        second.insert(make_order_with_side_and_price_and_quantity(
            2,
            Side::Buy,
            Price(100),
            Quantity(5),
        ));
        second.insert(make_order_with_side_and_price_and_quantity(
            1,
            Side::Buy,
            Price(100),
            Quantity(5),
        ));

        assert_ne!(first.checksum(), second.checksum());
    }
}
