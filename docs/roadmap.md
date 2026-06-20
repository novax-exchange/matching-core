# Matching Core Roadmap

This document is the repository-local roadmap for NovaX Matching Core. It records the current learning and implementation plan for this codebase. Do not rely on external NovaX project notes for the active status of this repository.

## Learning Positioning

This repository is primarily a learning project for matching-core technical challenges and architecture patterns. The main thread is the matching engine itself: deterministic state mutation, single-writer runtime ownership, journal-driven recovery, output safe points, internal concurrency, bounded handoff, backpressure, replay, snapshots, checksums, observability signals, benchmark-driven performance work, and later scaling patterns.

The first system property is determinism. Given the same confirmed input sequence and effective control-state sequence, live execution, replay, recovery, standby catch-up, and compatible upgrades must produce the same matching output, order book state, checksums, and safe points. Performance is the second major concern and must be studied only after the relevant determinism boundary is explicit.

Rust is the implementation language used to make the architecture concrete. Rust learning is valuable, but secondary to understanding matching-core design.

Service Runtime, RPC, gRPC, HTTP APIs, deployment, and operational frameworks are deferred topics. They should be studied later as hosting and operational layers around the matching core, not as the current driver of the roadmap.

## Learning Method

Future phases should be driven by realistic matching-engine pressure and failure scenarios, not by framework features or convenient API surfaces.

Each phase should:

- start with a realistic internal problem, such as hot-symbol overload, slow output commit, queue saturation, retry ambiguity, shutdown during in-flight work, or multi-symbol interference;
- state what the current minimal implementation hides or avoids;
- define the invariants that must survive, such as single writer per symbol, deterministic replay, no safe-point advancement before durable output, no duplicate trade ids, and bounded memory growth;
- extract the relevant architecture-document shape into code: responsibility, state ownership, contracts, boundary rules, flows, failure modes, validation evidence, and review triggers;
- add a focused test, benchmark, or small experiment that makes the difficulty visible;
- evolve the implementation by the smallest useful step;
- record newly discovered difficulties in the roadmap so the hard parts emerge from the work instead of being guessed only at the beginning.

## Current Position

| Item | Status |
| --- | --- |
| Completed phases | Phase 0-21 |
| Current milestone | Determinism proof layers |
| Current phase | Phase 22: Deterministic Output Identity and Duplicate Policy |
| Verification command | `cargo test -p matching-core` |

The project has completed the single-process matching core path:

```text
Journal Adapter input reader
  -> SymbolRouting
  -> BoundedHandoff
  -> PerSymbolExecutionLoop
  -> SymbolRuntime
  -> OrderBook
  -> PendingOutputBuffer
  -> OutputCommitBoundary
  -> Journal output adapter contract
```

The next major shift is deterministic output identity: proving that output records can be retried, replayed, compared, and deduplicated without changing trade identity, safe-point progress, or order book state.

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
| 18 | Completed | Per-symbol execution loop worker | Journal reader and runtime separation shape |
| 19 | Completed | Output isolation | Runtime can enqueue output requests before commit confirmation |
| 20 | Completed | Confirmed input consumer | Read confirmed input through Journal Adapter and route to symbol handoffs |
| 21 | Completed | SymbolRuntime output determinism | Two fresh runtimes process the same input sequence and produce identical output entries and safe point |
| 22 | In progress | Deterministic output identity and duplicate policy | Stable trade identity model; duplicate order id rejected or resolved before mutation |
| 23 | Planned | Replay output equivalence | Live path and replay path produce comparable output sequence, state, checksum, and safe point |
| 24 | Planned | Snapshot restore output determinism | Snapshot restore plus replay tail equals full replay for state, output identity, and safe point |
| 25 | Planned | Output commit ambiguity and safe-point discipline | Unknown / failed output commit does not advance safe point or consume future deterministic identity |
| 26 | Planned | Internal runtime concurrency and pressure | Long-running worker, queue pressure, retry, and safe-point tests |
| 27 | Planned | Multi-symbol concurrency and hot-symbol isolation | Slow or saturated symbol does not corrupt or block unrelated symbols |
| 28 | Planned | Output commit pressure | Slow or failing output commit does not create ambiguous safe-point progress |
| 29 | Planned | Runtime state view boundary | Cursor, checksum, queue, and deterministic status queries |
| 30 | Planned | Observability | Tracing and metrics visibility |
| 31 | Planned | Benchmarks | Single-symbol and multi-symbol baselines |
| 32 | Planned | Order book data structure evolution | Benchmark comparison |
| 33 | Planned | RingBuffer-style handoff optimization | Queue benchmark improvements |
| 34 | Planned | CPU stability optimization | p99 jitter comparison |
| 35 | Planned | Sharding and hot-symbol placement | Symbol ownership and migration tests |
| 36 | Planned | Standby replay | Standby checksum catches up |
| 37 | Planned | Leader lease and fencing | Lost lease stops processing |
| 38 | Planned | Failover drill | Promoted standby state is consistent |
| 39 | Planned | Zero-downtime upgrade validation | Old/new versions replay consistently |
| 40 | Planned | Final acceptance | Tests, benchmarks, drills, and docs |

## Module Progress

| Module | Status | Current implementation |
| --- | --- | --- |
| Domain model | Completed | `types.rs`, `order.rs` |
| Order book | Completed | `order_book.rs` |
| Matching engine | Completed for current stage | `matching_engine.rs`, `matching_engine/command_ingress.rs`; command ingress and output event model exist |
| Journal adapter contract | Completed for current stage | `journal_adapter.rs`; input reader and output appender contracts exist |
| Replay runner | Partial | `replay_runner.rs`; checksum replay exists, output replay equivalence is not implemented yet |
| Snapshot restore | Partial | `snapshot_restore.rs`; in-memory snapshot/restore exists, output identity and safe-point restore are not implemented yet |
| Per-symbol execution loop | Completed for current stage | `per_symbol_execution_loop.rs`; bounded input draining, retry requeue, pending output handoff, and one-shot worker exist |
| Symbol runtime | Completed for current stage | `per_symbol_execution_loop/symbol_runtime.rs`; deterministic output generation, rollback, and safe-point advancement are covered for the current layer |
| Runtime manager | Completed for current stage | `runtime_manager.rs` |
| Symbol routing | Completed | `symbol_routing.rs` |
| Bounded handoff | Completed | `bounded_handoff.rs` |
| Pending output buffer | Completed | `output_commit_boundary/pending_output_buffer.rs` |
| Output commit boundary | Completed for current stage | `output_commit_boundary.rs`, `output_commit_boundary/output_journal_client.rs`, `output_commit_boundary/output_batch_coordinator.rs`, `output_commit_boundary/pending_output_buffer.rs` |
| Confirmed input consumer | Completed for current stage | `confirmed_input_consumer.rs`; bounded batch read, gap detection, and backpressure-safe enqueue |
| Messaging reliability boundary | Identified, deferred | `messaging_reliability_boundary.rs`; reliability responsibilities are named, but envelope validation, offset tracking, deduplication, retry, and dead-letter behavior are not complete yet |
| Governance control boundary | Identified, deferred | `governance_control_boundary.rs`; deterministic control facts and local control state are not implemented yet |
| Evidence boundary | Identified, deferred | `evidence_boundary.rs`; explicit evidence records are not implemented yet |
| Runtime state view boundary | Deferred | Safe read-only runtime views will follow determinism and internal pressure work |
| Service/API boundary | Deferred | `matching-service` placeholder; protocol and Service Runtime work are later learning topics |
| Benchmarks | Planned | `matching-bench` placeholder |

## Current Phase: Deterministic Output Identity and Duplicate Policy

The current phase starts the next determinism layer after stable `SymbolRuntime` output: output identity and duplicate input policy. The goal is to make every generated trade and output record stable enough to survive retry, replay comparison, recovery, and duplicate input scenarios without silent drift.

Initial scenario:

A confirmed input sequence contains a crossing order, a retry after output handoff failure, and a duplicate order id. The runtime should produce stable output identity across retry and should reject or resolve the duplicate before mutating the order book. If duplicate commands can mutate state or consume trade ids differently, replay and live execution cannot be compared safely.

Initial scope:

- define the deterministic identity contract for trade ids and output records at the current learning level;
- add a focused duplicate-order scenario before mutation;
- verify that retry paths do not consume new trade ids or change output events;
- decide whether `next_trade_id` remains acceptable for this phase or should be derived from journal / per-symbol sequence context;
- record remaining identity gaps, such as distinct market sequence, output batch identity, control-state sequence, duplicate input commands, and output append unknown handling;
- keep performance, concurrency scaling, runtime state views, RPC, Service Runtime, and external API work deferred until these invariants are explicit.

Out of scope for this phase:

- choosing or deepening an RPC framework;
- building Service Runtime lifecycle infrastructure;
- turning the service crate into the main learning track;
- broad performance tuning before determinism and safe progress are understood.

## Difficulty Backlog

This backlog records hard problems discovered or expected during scenario-driven work. It is intentionally incomplete and should grow as tests and experiments expose new issues.

| Area | Difficulty | Current learning status |
| --- | --- | --- |
| Determinism | Same confirmed input must produce the same output events, order book state, checksums, and safe points across live execution and replay | Partially covered; live/replay output equivalence needs proof |
| Architecture extraction | Code should reflect responsibilities, state ownership, contracts, boundary rules, flows, failure modes, and validation from the architecture docs | Needs explicit inventory |
| Single writer | Each symbol order book must have exactly one mutation owner even when runtimes run concurrently | Basic symbol isolation exists; long-running worker model is next |
| Backpressure | Bounded handoff and pending output buffer saturation must stop unsafe progress without unbounded memory growth | Basic bounded transfer buffers exist; pressure behavior needs deeper scenarios |
| Output commit | Matching output must become durable before the runtime advances safe progress | Basic output commit loop exists; slow/failed downstream pressure needs study |
| Output identity | Output batches need stable identity so retry does not duplicate or drift | Current phase |
| Market sequence | Per-symbol market sequence should be distinct from global journal sequence | Not yet modeled |
| Control state | Matching-affecting config must enter at deterministic sequence positions | Not yet modeled |
| Messaging reliability | Reliable handoff needs explicit offset tracking, deduplication, retry, and poison/dead-letter behavior | Boundary identified; details deferred |
| Governance control | Halt, resume, symbol config, market mode, price-band, reduce-only, and fencing must become deterministic facts | Boundary identified; details deferred |
| Evidence | Matching, output commit, replay, recovery, and discrepancy decisions need explicit evidence records | Boundary identified; details deferred |
| Hot symbols | A saturated symbol should not corrupt unrelated symbols and should not hide overload signals | Not yet studied |
| Batch behavior | Batch size trades off throughput, latency, fairness, and retry cost | Not yet studied |
| Shutdown | Stopping during in-flight work must leave enough durable state to recover or retry safely | Not yet studied |
| Recovery | Snapshot plus replay must reconstruct state after failure and unknown outcomes | Partially covered; production recovery flow is later |
| Observability | Metrics and runtime views must reveal lag, queue depth, safe point, and checksum without mutating state | Deferred until after internal pressure work |
