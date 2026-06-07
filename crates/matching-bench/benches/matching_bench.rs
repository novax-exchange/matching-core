use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use matching_core::engine::matching::MatchingEngine;
use matching_core::types::*;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

fn sym() -> Symbol {
    Symbol("BTCUSDT".into())
}

fn make_bid(id: u64, price: Decimal) -> MatchingCommand {
    MatchingCommand::PlaceOrder(OrderCommand {
        command_id: CommandId(id),
        order_id: OrderId(id),
        symbol: sym(),
        side: Side::Bid,
        order_type: OrderType::Limit,
        price,
        quantity: dec!(1),
        config_version: ConfigVersion(1),
        timestamp_ns: id as i64,
    })
}

fn make_ask(id: u64, price: Decimal) -> MatchingCommand {
    MatchingCommand::PlaceOrder(OrderCommand {
        command_id: CommandId(id),
        order_id: OrderId(id),
        symbol: sym(),
        side: Side::Ask,
        order_type: OrderType::Limit,
        price,
        quantity: dec!(1),
        config_version: ConfigVersion(1),
        timestamp_ns: id as i64,
    })
}

fn bench_no_fill(c: &mut Criterion) {
    c.bench_function("limit_order_no_fill", |b| {
        b.iter_batched(
            || MatchingEngine::new(sym()),
            |mut engine| {
                engine.process(make_bid(1, dec!(100)), JournalSeq(1));
            },
            BatchSize::SmallInput,
        )
    });
}

fn bench_full_fill(c: &mut Criterion) {
    c.bench_function("limit_order_full_fill_1_level", |b| {
        b.iter_batched(
            || {
                let mut engine = MatchingEngine::new(sym());
                engine.process(make_bid(1, dec!(100)), JournalSeq(1));
                engine
            },
            |mut engine| {
                engine.process(make_ask(2, dec!(100)), JournalSeq(2));
            },
            BatchSize::SmallInput,
        )
    });
}

fn bench_throughput_100k(c: &mut Criterion) {
    let command_count = 100_000u64;
    let commands: Vec<_> = (1..=command_count)
        .map(|id| {
            (
                make_bid(id, dec!(100) - Decimal::from(id % 50)),
                JournalSeq(id),
            )
        })
        .collect();

    let mut group = c.benchmark_group("throughput");
    group.throughput(Throughput::Elements(command_count));
    group.bench_function("100k_place_no_fill", |b| {
        b.iter_batched(
            || (MatchingEngine::new(sym()), commands.clone()),
            |(mut engine, commands)| {
                for (command, seq) in commands {
                    engine.process(command, seq);
                }
            },
            BatchSize::LargeInput,
        )
    });
    group.finish();
}

criterion_group!(benches, bench_no_fill, bench_full_fill, bench_throughput_100k);
criterion_main!(benches);
