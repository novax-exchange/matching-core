use matching_core::shard_runtime::ShardRuntime;
use matching_core::types::Symbol;

fn symbol(value: &str) -> Symbol {
    Symbol(value.to_string())
}

#[test]
fn shard_runtime_is_available_from_public_api() {
    let btc = symbol("BTC-USDT");
    let runtime = ShardRuntime::new_for_symbols(vec![btc.clone()], 4, 8);

    assert_eq!(runtime.last_input_seq(&btc), Some(None));
}
