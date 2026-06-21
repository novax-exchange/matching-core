use matching_core::shard_execution_core::ShardExecutionCore;
use matching_core::types::Symbol;

fn symbol(value: &str) -> Symbol {
    Symbol(value.to_string())
}

#[test]
fn shard_execution_core_is_available_from_public_api() {
    let mut core = ShardExecutionCore::new();
    let btc = symbol("BTC-USDT");

    core.add_symbol(btc.clone());

    assert_eq!(core.symbols(), vec![btc]);
}
