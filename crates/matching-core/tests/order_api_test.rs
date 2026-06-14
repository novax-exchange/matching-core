use matching_core::order::*;
use matching_core::types::*;

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn order_and_command_are_available_from_public_api() {
        let symbol = Symbol("BTC-USDT".to_string());

        let mut order = Order {
            order_id: OrderId(1),
            symbol: symbol.clone(),
            side: Side::Buy,
            price: Price(100),
            quantity: Quantity(10),
        };

        assert_eq!(order.reduce_quantity(Quantity(3)), Ok(()));
        assert_eq!(order.quantity, Quantity(7));

        let place = Command::PlaceLimit(order.clone());
        assert_eq!(place.symbol(), &symbol);

        let cancel = Command::Cancel {
            order_id: OrderId(1),
            symbol: symbol.clone(),
        };
        assert_eq!(cancel.symbol(), &symbol);
    }
}
