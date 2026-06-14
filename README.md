# NovaX Matching Core

English | [Simplified Chinese](README.zh-CN.md)

NovaX Matching Core is a Rust learning project that rebuilds the core of a centralized exchange matching engine step by step. The goal is not to finish the code as quickly as possible, but to learn Rust while implementing a production-oriented matching subsystem with explicit tests, recovery boundaries, journal contracts, replay, snapshots, and deterministic state transitions.

This repository is part of the broader NovaX exchange system. It focuses on the matching core: domain types, orders, order books, matching results, command ingress, journals, replay, snapshots, symbol runtimes, and multi-symbol runtime management.

## Goals

The long-term target is a complete matching engine subsystem with:

- Multi-symbol runtime support.
- Single-writer execution per symbol.
- Journal-before-matching input flow.
- Output journal confirmation before safe-point advancement.
- Snapshot, replay, and checksum verification.
- Batch input processing with clear failure semantics.
- RingBuffer-style bounded input handoff and output isolation.
- Observability, benchmarks, and performance-oriented data structure evolution.
- Standby replay, leader lease, fencing, failover, and zero-downtime upgrade validation.

The project intentionally starts with simple implementations where useful, but the public contracts and tests are designed to point toward a production-grade architecture.

## Current Status

Current progress:

| Item | Status |
|---|---|
| Completed phases | Phase 0-15 |
| Current milestone | Multi-symbol runtime management |
| Next phase | Phase 16: Symbol router |
| Latest verification | `cargo test` |

Implemented capabilities:

- Core domain types and command model.
- FIFO price levels and indexed order book.
- Limit order matching and cancellation.
- Command ingress validation.
- Engine output events, including order acknowledgements and trade events.
- Deterministic checksum support.
- In-memory input and output journal contracts.
- Replay runner.
- Order book snapshot and restore.
- Single-symbol `SymbolRuntime` with safe-point processing.
- Batch processing with retry-safe failure behavior.
- Multi-symbol `RuntimeManager` with per-symbol state isolation.

Run the test suite:

```bash
cargo test
```

## Learning Method

This project is developed in learning mode:

1. Define the phase goal and Rust concept.
2. Write a minimal failing test.
3. Implement the smallest behavior that makes the test pass.
4. Review the design choice and Rust concept.
5. Run verification before moving to the next phase.
6. Commit and push each completed phase.

The learning rule is: simple implementations are allowed, but the boundaries must remain compatible with the production direction. For example, cloning an order book can be used to prove retry semantics early, but later phases replace hot-path mechanisms with more realistic output isolation and recovery behavior.

## Roadmap

| Phase | Status | Goal | Verification |
|---|---|---|---|
| 0 | Completed | Minimal Rust workspace | Empty tests pass |
| 1 | Completed | Core domain types | Type construction tests |
| 2 | Completed | Order and command model | Place/cancel command tests |
| 3 | Completed | Price level | FIFO tests |
| 4 | Completed | Order book structure | Best bid/ask and index tests |
| 5 | Completed | Limit order matching | Full/partial/resting match tests |
| 6 | Completed | Cancellation | Successful and failed cancel tests |
| 7 | Completed | Command ingress | Invalid symbol/price/quantity tests |
| 8 | Completed | Output event model | Ack and trade event tests |
| 9 | Completed | Deterministic checksum | Same input gives same checksum |
| 10 | Completed | Journal contract | Append/read/latest sequence tests |
| 11 | Completed | Replay runner | Replay checksum consistency |
| 12 | Completed | Snapshot | Snapshot/restore checksum consistency |
| 13 | Completed | Symbol runtime | Output commit advances safe point |
| 14 | Completed | Batch processing | Batch failure stops at safe point |
| 15 | Completed | Runtime manager | BTC/ETH runtimes remain isolated |
| 16 | Next | Symbol router | Entries route by symbol |
| 17 | Planned | Bounded input handoff | Full queue and ordered consumption tests |
| 18 | Planned | Thread model | Journal reader and runtime separation |
| 19 | Planned | Output isolation | Slow output does not block input directly |
| 20 | Planned | Durable journal adapter | Restart and replay recovery |
| 21 | Planned | Admin/query API | Cursor, checksum, and depth queries |
| 22 | Planned | Observability | Tracing and metrics visibility |
| 23 | Planned | Benchmarks | Single-symbol and multi-symbol baselines |
| 24 | Planned | Order book data structure evolution | Benchmark comparison |
| 25 | Planned | RingBuffer-style handoff optimization | Queue benchmark improvements |
| 26 | Planned | CPU stability optimization | p99 jitter comparison |
| 27 | Planned | Sharding and hot-symbol placement | Symbol ownership and migration tests |
| 28 | Planned | Standby replay | Standby checksum catches up |
| 29 | Planned | Leader lease and fencing | Lost lease stops processing |
| 30 | Planned | Failover drill | Promoted standby state is consistent |
| 31 | Planned | Zero-downtime upgrade validation | Old/new versions replay consistently |
| 32 | Planned | Final acceptance | Tests, benchmarks, drills, and docs |

## Architecture Principles

- The order book for a single symbol has exactly one writer.
- Input commands are confirmed by a journal before matching.
- Output events must be committed before the runtime advances its safe point.
- Replay and snapshot behavior must remain deterministic.
- Backpressure and output isolation are explicit system boundaries, not afterthoughts.
- Performance work is driven by benchmark evidence, not premature optimization.

## Repository Layout

```text
crates/
  matching-core/     Core matching engine library
  matching-service/  Service entry point placeholder
  matching-bench/    Benchmark crate placeholder
```

## Development

Useful commands:

```bash
cargo test
cargo test -p matching-core
```

Commit messages are written in English using concise Conventional Commit style, for example:

```text
feat(core): add runtime manager
```
