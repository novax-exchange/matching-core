#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OrderId(pub u64);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Symbol(pub String);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Price(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Quantity(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CommandId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TradeId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MarketSeq(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Checksum(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JournalSeq(pub u64);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_id_can_be_compared() {
        let first = OrderId(1);
        let second = OrderId(1);

        assert_eq!(first, second);
    }

    #[test]
    fn symbol_can_be_cloned_and_compared() {
        let btc = Symbol("BTC-USDT".to_string());
        let same = btc.clone();

        assert_eq!(btc, same);
    }

    #[test]
    fn price_and_quality_can_be_compared() {
        let price = Price(100);
        let same_price = Price(100);

        let quantity = Quantity(5);
        let same_quantity = Quantity(5);

        assert_eq!(price, same_price);
        assert_eq!(quantity, same_quantity);
    }

    #[test]
    fn side_distinguished_buy_and_sell() {
        assert_eq!(Side::Buy, Side::Buy);
        assert_ne!(Side::Buy, Side::Sell);
    }

    #[test]
    fn command_id_and_journal_seq_can_be_compared() {
        assert_eq!(CommandId(1), CommandId(1));
        assert_eq!(JournalSeq(10), JournalSeq(10));
        assert!(JournalSeq(10) < JournalSeq(11));
        assert!(TradeId(1) < TradeId(2));
        assert!(MarketSeq(1) < MarketSeq(2));
    }

    #[test]
    fn checksum_can_be_compared() {
        assert_eq!(Checksum(123), Checksum(123));
        assert_ne!(Checksum(123), Checksum(456));
    }
}
