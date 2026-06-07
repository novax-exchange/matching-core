use crate::types::{Order, OrderId};
use rust_decimal::Decimal;
use std::collections::VecDeque;

pub struct PriceLevel {
    pub price: Decimal,
    orders: VecDeque<Order>,
    total_quantity: Decimal,
}

impl PriceLevel {
    pub fn new(price: Decimal) -> Self {
        PriceLevel {
            price,
            orders: VecDeque::new(),
            total_quantity: Decimal::ZERO,
        }
    }

    pub fn push(&mut self, order: Order) {
        self.total_quantity += order.remaining;
        self.orders.push_back(order);
    }

    pub fn remove(&mut self, order_id: &OrderId) -> Option<Order> {
        let pos = self.orders.iter().position(|order| &order.order_id == order_id)?;
        let order = self.orders.remove(pos).unwrap();
        self.total_quantity -= order.remaining;
        Some(order)
    }

    pub fn front(&self) -> Option<&Order> {
        self.orders.front()
    }

    pub fn fill_front(&mut self, fill_qty: Decimal) -> Decimal {
        let front = match self.orders.front_mut() {
            Some(front) => front,
            None => return Decimal::ZERO,
        };
        let actual = fill_qty.min(front.remaining);
        front.remaining -= actual;
        self.total_quantity -= actual;
        if front.remaining == Decimal::ZERO {
            self.orders.pop_front();
        }
        actual
    }

    pub fn total_quantity(&self) -> Decimal {
        self.total_quantity
    }

    pub fn is_empty(&self) -> bool {
        self.orders.is_empty()
    }

    pub fn orders(&self) -> impl Iterator<Item = &Order> {
        self.orders.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{JournalSeq, Order, OrderId, OrderType, Side, Symbol};
    use rust_decimal_macros::dec;

    fn make_order(id: u64, remaining: rust_decimal::Decimal) -> Order {
        Order {
            order_id: OrderId(id),
            symbol: Symbol("BTCUSDT".into()),
            side: Side::Bid,
            order_type: OrderType::Limit,
            price: dec!(100),
            quantity: remaining,
            remaining,
            journal_seq: JournalSeq(1),
            timestamp_ns: 0,
        }
    }

    #[test]
    fn push_increases_total_quantity() {
        let mut level = PriceLevel::new(dec!(100));
        level.push(make_order(1, dec!(5)));
        level.push(make_order(2, dec!(3)));
        assert_eq!(level.total_quantity(), dec!(8));
    }

    #[test]
    fn fill_front_partial_updates_remaining_and_total() {
        let mut level = PriceLevel::new(dec!(100));
        level.push(make_order(1, dec!(5)));
        let filled = level.fill_front(dec!(2));
        assert_eq!(filled, dec!(2));
        assert_eq!(level.total_quantity(), dec!(3));
        assert_eq!(level.front().unwrap().remaining, dec!(3));
    }

    #[test]
    fn fill_front_exact_removes_order() {
        let mut level = PriceLevel::new(dec!(100));
        level.push(make_order(1, dec!(5)));
        let filled = level.fill_front(dec!(5));
        assert_eq!(filled, dec!(5));
        assert!(level.is_empty());
        assert_eq!(level.total_quantity(), dec!(0));
    }

    #[test]
    fn remove_by_id_updates_total() {
        let mut level = PriceLevel::new(dec!(100));
        level.push(make_order(1, dec!(5)));
        level.push(make_order(2, dec!(3)));
        let removed = level.remove(&OrderId(1));
        assert!(removed.is_some());
        assert_eq!(level.total_quantity(), dec!(3));
    }

    #[test]
    fn fifo_order_preserved() {
        let mut level = PriceLevel::new(dec!(100));
        level.push(make_order(1, dec!(2)));
        level.push(make_order(2, dec!(3)));
        assert_eq!(level.front().unwrap().order_id, OrderId(1));
        level.fill_front(dec!(2));
        assert_eq!(level.front().unwrap().order_id, OrderId(2));
    }
}
