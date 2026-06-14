use crate::types::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Order {
    pub order_id: OrderId,
    pub symbol: Symbol,
    pub side: Side,
    pub price: Price,
    pub quantity: Quantity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderError {
    FilledQuantityExceedsRemaining,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    PlaceLimit(Order),
    Cancel { order_id: OrderId, symbol: Symbol },
}

impl Command {
    pub fn symbol(&self) -> &Symbol {
        match self {
            Command::Cancel { symbol, .. } => symbol,
            Command::PlaceLimit(order) => &order.symbol,
        }
    }
}

impl Order {
    pub fn reduce_quantity(&mut self, filled: Quantity) -> Result<(), OrderError> {
        if filled.0 > self.quantity.0 {
            return Err(OrderError::FilledQuantityExceedsRemaining);
        }

        self.quantity = Quantity(self.quantity.0 - filled.0);
        Ok(())
    }

    pub fn is_filled(&self) -> bool {
        self.quantity.0 == 0
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::types::{OrderId, Price, Quantity, Side, Symbol};

    #[test]
    fn limit_order_can_be_created() {
        let order = Order {
            order_id: OrderId(1),
            symbol: Symbol("BTC-USDT".to_string()),
            side: Side::Buy,
            price: Price(100),
            quantity: Quantity(5),
        };

        assert_eq!(order.order_id, OrderId(1));
        assert_eq!(order.side, Side::Buy);
    }

    #[test]
    fn command_can_represent_place_limit() {
        let order = Order {
            order_id: OrderId(1),
            symbol: Symbol("BTC-USDT".to_string()),
            side: Side::Buy,
            price: Price(100),
            quantity: Quantity(5),
        };

        let command = Command::PlaceLimit(order.clone());

        match command {
            Command::PlaceLimit(inner) => assert_eq!(inner, order),
            _ => panic!("expected place limit command"),
        }
    }

    #[test]
    fn command_can_represent_cancel() {
        let command = Command::Cancel {
            order_id: OrderId(1),
            symbol: Symbol("BTC-USDT".to_string()),
        };

        match command {
            Command::Cancel { order_id, symbol } => {
                assert_eq!(order_id, OrderId(1));
                assert_eq!(symbol, Symbol("BTC-USDT".to_string()));
            }
            _ => panic!("expected cancel command"),
        }
    }

    #[test]
    fn command_returns_its_symbol() {
        let symbol = Symbol("BTC-USDT".to_string());

        let order = Order {
            order_id: OrderId(1),
            symbol: symbol.clone(),
            side: Side::Buy,
            price: Price(100),
            quantity: Quantity(5),
        };

        let place = Command::PlaceLimit(order);
        assert_eq!(place.symbol(), &symbol);

        let cancel = Command::Cancel {
            order_id: OrderId(1),
            symbol: symbol.clone(),
        };
        assert_eq!(cancel.symbol(), &symbol);
    }

    #[test]
    fn reducing_quantity_more_than_remaining_returns_error() {
        let mut order = Order {
            order_id: OrderId(1),
            symbol: Symbol("BTC-USDT".to_string()),
            side: Side::Buy,
            price: Price(100),
            quantity: Quantity(3),
        };

        let result = order.reduce_quantity(Quantity(5));

        assert_eq!(result, Err(OrderError::FilledQuantityExceedsRemaining));
        assert_eq!(order.quantity, Quantity(3));
    }

    #[test]
    fn order_is_filled_when_quantity_is_zero() {
        let order = Order {
            order_id: OrderId(1),
            symbol: Symbol("BTC-USDT".to_string()),
            side: Side::Buy,
            price: Price(100),
            quantity: Quantity(0),
        };

        assert!(order.is_filled());
    }

    #[test]
    fn order_is_not_filled_when_quantity_is_positive() {
        let order = Order {
            order_id: OrderId(1),
            symbol: Symbol("BTC-USDT".to_string()),
            side: Side::Buy,
            price: Price(100),
            quantity: Quantity(1),
        };

        assert!(!order.is_filled());
    }

    #[test]
    fn order_quantity_can_be_reduced() {
        let mut order = Order {
            order_id: OrderId(1),
            symbol: Symbol("BTC-USDT".to_string()),
            side: Side::Buy,
            price: Price(100),
            quantity: Quantity(10),
        };

        let result = order.reduce_quantity(Quantity(3));

        assert_eq!(result, Ok(()));
        assert_eq!(order.quantity, Quantity(7));
    }
}
