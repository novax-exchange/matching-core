use crate::order::Command;
use crate::types::{Price, Quantity, Symbol};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngressError {
    InvalidPrice,
    InvalidQuantity,
    SymbolMismatch,
}

pub struct CommandIngress {
    symbol: Symbol,
}

impl CommandIngress {
    pub fn new(symbol: Symbol) -> Self {
        CommandIngress { symbol }
    }

    pub fn validate(&mut self, command: Command) -> Result<Command, IngressError> {
        if command.symbol() != &self.symbol {
            return Err(IngressError::SymbolMismatch);
        }

        match &command {
            Command::PlaceLimit(order) if order.price == Price(0) => {
                Err(IngressError::InvalidPrice)
            }
            Command::PlaceLimit(order) if order.quantity == Quantity(0) => {
                Err(IngressError::InvalidQuantity)
            }
            _ => Ok(command),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::command_ingress::{CommandIngress, IngressError};
    use crate::order::{Command, Order};
    use crate::types::{OrderId, Price, Quantity, Side, Symbol};

    fn limit_order(price: Price, quantity: Quantity) -> Command {
        Command::PlaceLimit(Order {
            order_id: OrderId(1),
            symbol: Symbol("BTC-USDT".to_string()),
            side: Side::Buy,
            price,
            quantity,
        })
    }

    #[test]
    fn rejects_zero_price_limit_order() {
        let mut ingress = CommandIngress::new(Symbol("BTC-USDT".to_string()));

        let result = ingress.validate(limit_order(Price(0), Quantity(10)));

        assert_eq!(result, Err(IngressError::InvalidPrice));
    }

    #[test]
    fn rejects_zero_quantity_limit_order() {
        let mut ingress = CommandIngress::new(Symbol("BTC-USDT".to_string()));

        let result = ingress.validate(limit_order(Price(100), Quantity(0)));

        assert_eq!(result, Err(IngressError::InvalidQuantity));
    }

    #[test]
    fn rejects_command_for_different_symbol() {
        let mut ingress = CommandIngress::new(Symbol("BTC-USDT".to_string()));

        let command = Command::PlaceLimit(Order {
            order_id: OrderId(1),
            symbol: Symbol("ETH-USDT".to_string()),
            side: Side::Buy,
            price: Price(100),
            quantity: Quantity(10),
        });

        let result = ingress.validate(command);

        assert_eq!(result, Err(IngressError::SymbolMismatch));
    }

    #[test]
    fn accepts_valid_limit_order() {
        let mut ingress = CommandIngress::new(Symbol("BTC-USDT".to_string()));
        let command = limit_order(Price(100), Quantity(10));

        let result = ingress.validate(command.clone());

        assert_eq!(result, Ok(command));
    }

    #[test]
    fn rejects_cancel_for_different_symbol() {
        let mut ingress = CommandIngress::new(Symbol("BTC-USDT".to_string()));

        let command = Command::Cancel {
            order_id: OrderId(1),
            symbol: Symbol("ETH-USDT".to_string()),
        };

        let result = ingress.validate(command);

        assert_eq!(result, Err(IngressError::SymbolMismatch));
    }
}
