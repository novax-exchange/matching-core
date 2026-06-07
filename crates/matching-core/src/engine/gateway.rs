use crate::types::*;
use rust_decimal::Decimal;
use std::collections::HashMap;

pub type RawCommand = MatchingCommand;

#[derive(Debug, Clone, PartialEq)]
pub enum GatewayResult {
    Accept(MatchingCommand),
    Reject {
        command_id: CommandId,
        reason: RejectReason,
    },
    Duplicate {
        command_id: CommandId,
        original_seq: JournalSeq,
    },
}

pub struct CommandGateway {
    symbol: Symbol,
    config: SymbolConfig,
    seen_commands: HashMap<CommandId, JournalSeq>,
}

impl CommandGateway {
    pub fn new(symbol: Symbol, config: SymbolConfig) -> Self {
        CommandGateway {
            symbol,
            config,
            seen_commands: HashMap::new(),
        }
    }

    pub fn validate(&mut self, raw: RawCommand, seq: JournalSeq) -> GatewayResult {
        match raw {
            MatchingCommand::PlaceOrder(command) => self.validate_order(command, seq),
            MatchingCommand::CancelOrder(command) => self.validate_cancel(command, seq),
        }
    }

    fn validate_order(&mut self, command: OrderCommand, seq: JournalSeq) -> GatewayResult {
        if let Some(original_seq) = self.seen_commands.get(&command.command_id).copied() {
            return GatewayResult::Duplicate {
                command_id: command.command_id,
                original_seq,
            };
        }
        if command.symbol != self.symbol {
            return GatewayResult::Reject {
                command_id: command.command_id,
                reason: RejectReason::InvalidSymbol,
            };
        }
        if command.config_version != self.config.config_version {
            return GatewayResult::Reject {
                command_id: command.command_id,
                reason: RejectReason::ConfigVersionMismatch,
            };
        }
        if command.price <= Decimal::ZERO || !is_multiple_of_tick(command.price, self.config.price_tick) {
            return GatewayResult::Reject {
                command_id: command.command_id,
                reason: RejectReason::InvalidPrice,
            };
        }
        if command.quantity < self.config.min_quantity
            || command.quantity <= Decimal::ZERO
            || !is_multiple_of_tick(command.quantity, self.config.quantity_tick)
        {
            return GatewayResult::Reject {
                command_id: command.command_id,
                reason: RejectReason::InvalidQuantity,
            };
        }

        let command_id = command.command_id.clone();
        self.seen_commands.insert(command_id, seq);
        GatewayResult::Accept(MatchingCommand::PlaceOrder(command))
    }

    fn validate_cancel(&mut self, command: CancelCommand, seq: JournalSeq) -> GatewayResult {
        if let Some(original_seq) = self.seen_commands.get(&command.command_id).copied() {
            return GatewayResult::Duplicate {
                command_id: command.command_id,
                original_seq,
            };
        }
        if command.symbol != self.symbol {
            return GatewayResult::Reject {
                command_id: command.command_id,
                reason: RejectReason::InvalidSymbol,
            };
        }
        if command.config_version != self.config.config_version {
            return GatewayResult::Reject {
                command_id: command.command_id,
                reason: RejectReason::ConfigVersionMismatch,
            };
        }

        let command_id = command.command_id.clone();
        self.seen_commands.insert(command_id, seq);
        GatewayResult::Accept(MatchingCommand::CancelOrder(command))
    }
}

fn is_multiple_of_tick(value: Decimal, tick: Decimal) -> bool {
    tick > Decimal::ZERO && (value % tick) == Decimal::ZERO
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn config() -> SymbolConfig {
        SymbolConfig {
            price_tick: dec!(0.01),
            quantity_tick: dec!(0.001),
            min_quantity: dec!(0.001),
            config_version: ConfigVersion(1),
        }
    }

    fn gw() -> CommandGateway {
        CommandGateway::new(Symbol("BTCUSDT".into()), config())
    }

    fn valid_bid(cmd_id: u64, order_id: u64) -> MatchingCommand {
        MatchingCommand::PlaceOrder(OrderCommand {
            command_id: CommandId(cmd_id),
            order_id: OrderId(order_id),
            symbol: Symbol("BTCUSDT".into()),
            side: Side::Bid,
            order_type: OrderType::Limit,
            price: dec!(100.00),
            quantity: dec!(1.000),
            config_version: ConfigVersion(1),
            timestamp_ns: 0,
        })
    }

    #[test]
    fn valid_command_is_accepted() {
        let mut gateway = gw();
        let result = gateway.validate(valid_bid(1, 1), JournalSeq(1));
        assert!(matches!(result, GatewayResult::Accept(_)));
    }

    #[test]
    fn duplicate_command_id_is_rejected() {
        let mut gateway = gw();
        gateway.validate(valid_bid(1, 1), JournalSeq(1));
        let result = gateway.validate(valid_bid(1, 2), JournalSeq(2));
        assert!(matches!(result, GatewayResult::Duplicate { .. }));
    }

    #[test]
    fn wrong_config_version_is_rejected() {
        let mut gateway = gw();
        let command = MatchingCommand::PlaceOrder(OrderCommand {
            command_id: CommandId(1),
            order_id: OrderId(1),
            symbol: Symbol("BTCUSDT".into()),
            side: Side::Bid,
            order_type: OrderType::Limit,
            price: dec!(100),
            quantity: dec!(1),
            config_version: ConfigVersion(999),
            timestamp_ns: 0,
        });
        let result = gateway.validate(command, JournalSeq(1));
        assert!(matches!(
            result,
            GatewayResult::Reject {
                reason: RejectReason::ConfigVersionMismatch,
                ..
            }
        ));
    }

    #[test]
    fn zero_price_is_rejected() {
        let mut gateway = gw();
        let command = MatchingCommand::PlaceOrder(OrderCommand {
            command_id: CommandId(1),
            order_id: OrderId(1),
            symbol: Symbol("BTCUSDT".into()),
            side: Side::Bid,
            order_type: OrderType::Limit,
            price: dec!(0),
            quantity: dec!(1),
            config_version: ConfigVersion(1),
            timestamp_ns: 0,
        });
        assert!(matches!(
            gateway.validate(command, JournalSeq(1)),
            GatewayResult::Reject {
                reason: RejectReason::InvalidPrice,
                ..
            }
        ));
    }

    #[test]
    fn below_min_quantity_is_rejected() {
        let mut gateway = gw();
        let command = MatchingCommand::PlaceOrder(OrderCommand {
            command_id: CommandId(1),
            order_id: OrderId(1),
            symbol: Symbol("BTCUSDT".into()),
            side: Side::Bid,
            order_type: OrderType::Limit,
            price: dec!(100),
            quantity: dec!(0.0001),
            config_version: ConfigVersion(1),
            timestamp_ns: 0,
        });
        assert!(matches!(
            gateway.validate(command, JournalSeq(1)),
            GatewayResult::Reject {
                reason: RejectReason::InvalidQuantity,
                ..
            }
        ));
    }
}
