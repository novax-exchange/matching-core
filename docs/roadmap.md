# Matching Core Roadmap

This document is the repository-local roadmap for NovaX Matching Core. It records the current learning and implementation plan for this codebase. Do not rely on external NovaX project notes for the active status of this repository.

## Current Position

| Item | Status |
| --- | --- |
| Completed phases | Phase 0-20 |
| Current milestone | Service-facing query boundary |
| Current phase | Phase 21: Admin/query API |
| Verification command | `cargo test -p matching-core` |

The project has completed the single-process matching core path:

```text
Journal Adapter input reader
  -> SymbolRouting
  -> BoundedHandoff
  -> RuntimeLoop
  -> SymbolRuntime
  -> OrderBook
  -> OutputQueue
  -> OutputCommitLoop
  -> Journal output adapter contract
```

The next major shift is the service-facing query boundary: exposing safe runtime state queries without letting API reads mutate deterministic matching state.

## Phase Roadmap

| Phase | Status | Goal | Verification |
| --- | --- | --- | --- |
| 0 | Completed | Minimal Rust workspace | Empty tests pass |
| 1 | Completed | Core domain types | Type construction tests |
| 2 | Completed | Order and command model | Place/cancel command tests |
| 3 | Completed | Price level | FIFO tests |
| 4 | Completed | Order book structure | Best bid/ask and index tests |
| 5 | Completed | Limit order matching | Full, partial, and resting match tests |
| 6 | Completed | Cancellation | Successful and failed cancel tests |
| 7 | Completed | Command ingress | Invalid symbol, price, and quantity tests |
| 8 | Completed | Output event model | Ack and trade event tests |
| 9 | Completed | Deterministic checksum | Same input gives same checksum |
| 10 | Completed | Journal adapter contract | Append, read, and latest sequence tests |
| 11 | Completed | Replay runner | Replay checksum consistency |
| 12 | Completed | Snapshot | Snapshot/restore checksum consistency |
| 13 | Completed | Symbol runtime | Output commit advances safe point |
| 14 | Completed | Batch processing | Batch failure stops at safe point |
| 15 | Completed | Runtime manager | BTC/ETH runtimes remain isolated |
| 16 | Completed | Symbol routing | Entries route by symbol |
| 17 | Completed | Bounded handoff | Full queue, ordered consumption, and watermarks |
| 18 | Completed | Runtime loop worker | Journal reader and runtime separation shape |
| 19 | Completed | Output isolation | Runtime can enqueue output requests before commit confirmation |
| 20 | Completed | Confirmed input consumer | Read confirmed input through Journal Adapter and route to symbol handoffs |
| 21 | In progress | Admin/query API | Cursor, checksum, and depth queries |
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

## Module Progress

| Module | Status | Current implementation |
| --- | --- | --- |
| Domain model | Completed | `types.rs`, `order.rs` |
| Order book | Completed | `order_book.rs` |
| Command ingress | Completed | `command_ingress.rs` |
| Event model | Completed | `engine.rs` |
| Journal adapter contract | Completed for current stage | `journal_adapter.rs`; input reader and output appender contracts exist |
| Replay runner | Partial | `replay.rs`; deterministic replay exists; production recovery flow is later |
| Snapshot restore | Partial | `snapshot.rs`; in-memory snapshot/restore exists; persistence integration is later |
| Symbol runtime | Completed for current stage | `symbol_runtime.rs` |
| Runtime manager | Completed for current stage | `runtime_manager.rs` |
| Symbol routing | Completed | `symbol_routing.rs` |
| Bounded handoff | Completed | `bounded_handoff.rs` |
| Runtime loop | Completed for current stage | `runtime_loop.rs` |
| Output queue | Completed | `output_queue.rs` |
| Output commit boundary | Completed for current stage | `output_committer.rs`, `output_commit_loop.rs` |
| Confirmed input consumer | Completed for current stage | `confirmed_input_consumer.rs`; bounded batch read, gap detection, and backpressure-safe enqueue |
| Service/API boundary | Planned | `matching-service` placeholder |
| Benchmarks | Planned | `matching-bench` placeholder |

## Current Phase: Admin/query API

The current phase starts the service-facing query boundary. It should expose read-only state needed by operators and service code without changing matching state.

Initial scope:

- query registered symbols;
- query per-symbol runtime safe point / last committed input sequence;
- query per-symbol checksum;
- keep query APIs read-only and deterministic.
