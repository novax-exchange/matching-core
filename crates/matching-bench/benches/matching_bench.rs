use criterion::{criterion_group, criterion_main, Criterion};

fn placeholder_bench(_c: &mut Criterion) {}

criterion_group!(benches, placeholder_bench);
criterion_main!(benches);
