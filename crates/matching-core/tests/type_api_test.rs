use matching_core::types::*;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn domain_types_are_available_from_public_api() {
        assert_eq!(OrderId(1), OrderId(1));
        assert_ne!(Side::Buy, Side::Sell);
    }
}
