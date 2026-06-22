# Matching Core Roadmap

This document is the repository-local roadmap for Matching Core. It records the current learning and implementation plan for this codebase. Do not rely on external project notes for the active status of this repository.

## Learning Positioning

This repository is primarily a learning project for matching-core technical challenges and architecture patterns. The main thread is the matching engine itself: deterministic state mutation, single-writer runtime ownership, journal-driven recovery, output safe points, internal concurrency, bounded handoff, backpressure, replay, snapshots, checksums, observability signals, benchmark-driven performance work, and later scaling patterns.

The first system property is determinism. Given the same confirmed input sequence and effective control-state sequence, live execution, replay, recovery, standby catch-up, and compatible upgrades must produce the same matching output, order book state, checksums, and safe points. Performance is the second major concern and must be studied only after the relevant determinism boundary is explicit.

Rust is the implementation language used to make the architecture concrete. Rust learning is valuable, but secondary to understanding matching-core design.

Service Runtime, RPC, gRPC, HTTP APIs, deployment, and operational frameworks are deferred topics. They should be studied later as hosting and operational layers around the matching core, not as the current focus of the roadmap.

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
| Completed phases | Phase 0-27 |
| Current milestone | Runtime execution and pressure |
| Current phase | Phase 28: Async-task-per-shard execution mode |
| Verification command | `cargo test -p matching-core` |

The project has completed the single-process matching core path:

```text
Journal Adapter input reader
  -> SymbolRouting
  -> BoundedHandoff
  -> SymbolRuntime
  -> MatchingEngine
  -> OrderBook
  -> PendingOutputBuffer
  -> OutputCommitBoundary
  -> Journal output adapter contract
```

The asynchronous output commit discipline is now established at the learning-project level. The matching execution path generates deterministic output requests and enqueues them locally; the output commit path batches and durably appends those requests to Journal; safe-point advancement consumes only confirmed durable prefixes, not attempted remote calls or generated output.

The inline and thread-per-shard runtime contracts are now explicit enough to serve as the reference for async execution. The next major shift is implementing async-task-per-shard execution while preserving the same input preflight, lifecycle, pressure, remaining-work, blocked-symbol, output durability, and safe-point rules.

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
| 15 | Completed | Shard execution core | BTC/ETH runtimes remain isolated |
| 16 | Completed | Symbol routing | Entries route by symbol |
| 17 | Completed | Bounded handoff | Full queue, ordered consumption, and watermarks |
| 18 | Completed | Symbol runtime step boundary | Journal reader and runtime separation shape |
| 19 | Completed | Output isolation | Runtime can enqueue output requests before commit confirmation |
| 20 | Completed | Confirmed input consumer | Read confirmed input through Journal Adapter and route to symbol handoffs |
| 21 | Completed | SymbolRuntime output determinism | Two fresh runtimes process the same input sequence and produce identical output entries and safe point |
| 22 | Completed | Deterministic output identity and duplicate policy | Stable trade identity model; duplicate order id rejected or resolved before mutation |
| 23 | Completed | Replay output equivalence | Live path and replay path produce comparable output sequence, state, checksum, and safe point |
| 24 | Completed | Snapshot restore output determinism | Snapshot restore plus replay tail equals full replay for state, output identity, and safe point |
| 25 | Completed | Output commit ambiguity and safe-point discipline | Missing / incomplete / durable / conflict output commit evidence is surfaced through ShardExecutionCore; unknown / failed output commit does not advance safe point beyond the confirmed durable prefix or consume future deterministic identity |
| 26 | Completed | Runtime execution modes and pressure | MatchingRuntime inline execution is the reference contract; input preflight, close, drain, shutdown, pressure, remaining work, and blocked-symbol semantics are explicit before threaded or async execution is implemented |
| 27 | Completed | Thread-per-shard execution mode | Threaded execution preserves the same shard ownership, input close, drain, shutdown, pressure, output metadata, and safe-point semantics as inline execution |
| 28 | In progress | Async-task-per-shard execution mode | Async execution preserves deterministic ownership and bounded pressure without introducing a second writer for a symbol |
| 29 | Planned | Multi-symbol concurrency and hot-symbol isolation | Slow or saturated symbol does not corrupt or block unrelated symbols beyond the chosen shard-level policy |
| 30 | Planned | Output commit pressure | Slow or failing output commit does not create ambiguous safe-point progress |
| 31 | Planned | Runtime state view boundary | Cursor, checksum, queue, pressure, blocked-symbol, and deterministic status queries |
| 32 | Planned | Observability | Tracing and metrics visibility |
| 33 | Planned | Benchmarks | Single-symbol, multi-symbol, and shard execution baselines |
| 34 | Planned | Order book data structure evolution | Benchmark comparison |
| 35 | Planned | RingBuffer-style handoff optimization | Queue benchmark improvements |
| 36 | Planned | CPU stability optimization | p99 jitter comparison |
| 37 | Planned | Sharding and hot-symbol placement | Symbol ownership and migration tests |
| 38 | Planned | Standby replay | Standby checksum catches up |
| 39 | Planned | Leader lease and fencing | Lost lease stops processing |
| 40 | Planned | Failover drill | Promoted standby state is consistent |
| 41 | Planned | Zero-downtime upgrade validation | Old/new versions replay consistently |
| 42 | Planned | Final acceptance | Tests, benchmarks, drills, and docs |

## Module Progress

| Module | Status | Current implementation |
| --- | --- | --- |
| Domain model | Completed | `types.rs`, `order.rs` |
| Order book | Completed | `order_book.rs` |
| Matching engine | Completed for current stage | `matching_engine.rs`, `matching_engine/command_ingress.rs`; command ingress and output event model exist |
| Journal adapter contract | Completed for current stage | `journal_adapter.rs`; input reader and output appender contracts exist |
| Replay runner | Partial | `replay_runner.rs`; checksum replay exists, and replay result now regenerates comparable output entries for the current live-vs-replay proof |
| Snapshot restore | Partial | `snapshot_restore.rs`; in-memory order-book snapshot/restore exists, and `SymbolRuntimeSnapshot` now captures runtime identity state for restore |
| Symbol runtime | Completed for current stage | `symbol_runtime.rs`, `symbol_runtime/runtime.rs`; deterministic output generation, bounded input draining, retry requeue, pending output handoff, rollback, safe-point advancement, and one-shot execution support are covered for the current layer |
| Matching runtime | Completed for current stage | `matching_runtime.rs`, `shard_runtime_set.rs`; configured execution mode, input preflight, input close / drain / shutdown boundaries, post-shutdown and repeated-shutdown rejection, runtime lifecycle state, shard status, symbol-level remaining-work / blocked-symbol reporting, aggregate run stop reasons, final run-until-idle / shutdown status, inline multi-shard execution, thread-per-shard execution, per-shard output writers, worker shutdown join, and explicit threaded request / response error paths are covered for the current layer |
| Shard runtime | Completed for current stage | `shard_runtime.rs`; bounded input handoff, shard ownership, run-once / run-limited execution, topology construction, remaining-work reporting, and blocked-symbol reporting are covered for the current layer |
| Shard execution core | Completed for current stage | `shard_execution_core.rs` |
| Symbol routing | Completed | `symbol_routing.rs` |
| Bounded handoff | Completed | `bounded_handoff.rs` |
| Pending output buffer | Completed | `output_commit_boundary/pending_output_buffer.rs` |
| Output commit boundary | Partial | `output_commit_boundary.rs`, `output_commit_boundary/output_journal_client.rs`, `output_commit_boundary/output_batch_coordinator.rs`, `output_commit_boundary/pending_output_buffer.rs`; append results are classified into commit outcomes, exact durable unknowns can be resolved, confirmed prefixes are reported, and unresolved / unavailable / rejected requests stay pending with explicit retry / resolution decisions. The intended production shape is asynchronous batch commit, not per-order synchronous remote Journal calls |
| Confirmed input consumer | Completed for current stage | `confirmed_input_consumer.rs`; bounded batch read, gap detection, and backpressure-safe enqueue |
| Messaging reliability boundary | Identified, deferred | `messaging_reliability_boundary.rs`; reliability responsibilities are named, but envelope validation, offset tracking, deduplication, retry, and dead-letter behavior are not complete yet |
| Governance control boundary | Identified, deferred | `governance_control_boundary.rs`; deterministic control facts and local control state are not implemented yet |
| Evidence boundary | Identified, deferred | `evidence_boundary.rs`; explicit evidence records are not implemented yet |
| Runtime state view boundary | Deferred | Safe read-only runtime views will follow determinism and internal pressure work |
| Service/API boundary | Deferred | `matching-service` placeholder; protocol and Service Runtime work are later learning topics |
| Benchmarks | Planned | `matching-bench` placeholder |

## Completed Phase: Deterministic Output Identity and Duplicate Policy

The current phase starts the next determinism layer after stable `SymbolRuntime` output: output identity and duplicate input policy. The goal is to make every generated trade and output record stable enough to survive retry, replay comparison, recovery, and duplicate input scenarios without silent drift.

Initial scenario:

A confirmed input sequence contains a crossing order, a retry after output handoff failure, and a duplicate order id. The runtime should produce stable output identity across retry and should reject or resolve the duplicate before mutating the order book. If duplicate commands can mutate state or consume trade ids differently, replay and live execution cannot be compared safely.

Progress so far:

- duplicate `order_id` values are rejected before matching mutation, including ids from already-filled orders;
- duplicate `command_id` values are rejected before matching mutation;
- duplicate rejection does not remove the original resting order or consume trade identity;
- `TradeEvent` now carries a per-symbol `market_seq` for trade outputs;
- output append and pending-output handoff failures roll back command, order, trade, and market-sequence identity state for safe retry.

Initial scope:

- define the deterministic identity contract for trade ids and output records at the current learning level;
- keep duplicate order-id rejection explicit before mutation;
- verify that retry paths do not consume new trade ids, market sequence values, or change output events;
- keep `next_trade_seq` and `next_market_seq` as in-memory per-symbol runtime state for now, with durability/replay proof deferred to the next recovery layers;
- record remaining identity gaps, such as non-trade market events, output batch identity, control-state sequence, and output append unknown handling;
- keep performance, concurrency scaling, runtime state views, RPC, Service Runtime, and external API work deferred until these invariants are explicit.

Out of scope for this phase:

- choosing or deepening an RPC framework;
- building Service Runtime lifecycle infrastructure;
- turning the service crate into the main learning track;
- broad performance tuning before determinism and safe progress are understood.

## Completed Phase: Replay Output Equivalence

This phase extends replay from checksum-only validation to comparable output regeneration. The goal is to prove that live execution and replay can produce the same output entries, deterministic identity values, final checksum, and safe progress for the same confirmed input sequence.

Progress so far:

- `ReplayResult` now exposes regenerated `JournalOutputEntry` values;
- replay output generation uses an isolated `SymbolRuntime`, so `OrderAck`, `TradeEvent`, `trade_id`, and trade-output `market_seq` follow the same deterministic runtime rules as live execution;
- a live-vs-replay test compares regenerated replay output entries against live `SymbolRuntime` output entries for a crossing trade.
- a broader live-vs-replay test now covers accepted orders, trade output, successful cancel, cancel rejection, invalid command rejection, duplicate order-id rejection, duplicate command-id rejection, checksum, and safe-point equivalence.
- `ReplayComparisonResult` now reports output-entry, checksum, and safe-point match dimensions, with `is_match()` as the aggregate verifier-style decision.
- comparison results now carry basic mismatch evidence: first output mismatch index, actual / expected output entry at that index, actual / expected checksum, and actual / expected safe point.

Current scope:

- add output digest or surrounding mismatch window only once verifier evidence needs more compact or broader diagnostics;
- keep replay comparison inside `ReplayRunner` and tests for now, not inside the primary execution path;
- keep production standby digest / evidence modeling deferred until complete output equivalence is proven.

## Completed Phase: Snapshot Restore Output Determinism

This phase extends snapshot recovery from order-book checksum reconstruction to runtime output identity reconstruction. The goal is to restore enough runtime state that processing after a snapshot continues deterministic `trade_id`, trade-output `market_seq`, duplicate command policy, duplicate order-id policy, checksum, and safe-point behavior.

Progress so far:

- `SymbolRuntimeSnapshot` now captures `OrderBookSnapshot`, `next_trade_seq`, `next_market_seq`, `seen_command_ids`, and `seen_order_ids`;
- `SymbolRuntime::snapshot()` creates a runtime-level snapshot only after the runtime has a safe point;
- `SymbolRuntime::restore_from_snapshot()` rebuilds the order book, command ingress, safe point, trade sequence, market sequence, and duplicate identity state;
- a restore test proves that a post-snapshot trade continues with `TradeId(2)` and `MarketSeq(2)` instead of restarting from `1`.
- restore tests prove that duplicate `command_id` and `order_id` values seen before the snapshot are still rejected after restore.
- `ReplayRunner::replay_result_from_snapshot()` restores a runtime snapshot and regenerates only the journal tail output;
- a full-vs-restored replay test proves that snapshot restore plus tail replay matches full replay tail output, final checksum, and safe point.

Deferred:

- keep durable snapshot serialization format deferred until the in-memory recovery contract is clear.

## Current Phase: Output Commit Ambiguity and Safe-Point Discipline

This phase studies what happens when the runtime cannot tell whether an output append or downstream commit actually became durable. The goal is to prevent unsafe safe-point advancement and prevent deterministic identity from drifting when commit outcome is failed, unknown, retried, or later discovered.

Initial scope:

- distinguish definite append failure from unknown commit outcome;
- make output commit an asynchronous batch path behind `PendingOutputBuffer`, not a per-order synchronous remote Journal call from the matching path;
- keep safe-point advancement tied to confirmed durable output, not attempted output generation;
- prove unknown commit does not silently consume future `trade_id` or `market_seq` in a way that makes replay incomparable;
- model bounded pending output and backpressure as the safety mechanism when Journal commit falls behind;
- keep service-level RPC, standby promotion, and external operational automation deferred until the core ambiguity contract is explicit.

Progress so far:

- `JournalAdapterError` now distinguishes `AppendFailed` from `CommitOutcomeUnknown`;
- `OutputBatchCoordinator` preserves its conservative behavior for unresolved unknown outcomes: stop at the ambiguous request, requeue that request plus the uncommitted tail, and return no successful commit result for safe-point advancement;
- `OutputJournalClient::is_request_durable()` can query the output journal for an exact output request match;
- if an unknown append outcome is followed by an exact durable output match, the coordinator treats that request as committed and continues the batch;
- `OutputCommitOutcome` now names the current output commit result classes: accepted, duplicate-accepted, unknown, unavailable, and rejected;
- `OutputBatchCommitStepReport` preserves the confirmed prefix even when a later output request blocks the batch;
- the safe-point controller can advance from that confirmed prefix without advancing into the blocked request;
- `OutputCommitRetryTracker` turns blocked output reports into explicit actions: retry later for short `Unavailable`, resolve for `Unknown`, and stop / escalate for `Rejected` or repeated `Unavailable`;
- the main SymbolRuntime and ReplayRunner determinism tests now use the pending-output plus commit-report path for live execution evidence, rather than treating direct output append as the production commit model;
- `run_symbol_runtime_step_with_output_batch_commit()` now provides the first integrated step shape for execution, pending output, Output Batch Coordinator commit, confirmed-prefix safe-point advancement, and blocked-tail retention;
- `ShardExecutionCore::run_symbol_step_with_output_batch_commit()` exposes that integrated step at the symbol runtime boundary, with one pending output buffer per registered symbol;
- Shard execution core tests now cover a blocked output tail across two iterations: the first step advances only the confirmed prefix, and the next step retries the pending tail before advancing the safe point;
- `ShardExecutionCore::run_symbol_output_batch_commit_step()` can commit pending output without draining new input, which gives the long-running loop a clear way to relieve pending-output pressure before consuming more commands;
- `SymbolRuntimeStatus` reports pending output length, capacity, and full-state so pressure is visible at the runtime boundary;
- `ShardExecutionCore::run_symbol_pressure_aware_step()` uses that pressure state for scheduling: if pending output is already full, it runs output-only commit before draining new input;
- `ShardExecutionCore::run_symbol_retry_aware_step()` records blocked output reports per symbol and returns retry / resolve / escalate decisions to the caller;
- `SymbolRuntimeStatus` now exposes the current output commit escalation decision when a rejected output or repeated unavailable output reaches `StopAndEscalate`;
- once a symbol has an output commit escalation, `run_symbol_retry_aware_step()` pauses that symbol instead of draining new input or retrying the pending output; other symbols remain independent;
- `ShardExecutionCore::clear_symbol_output_commit_escalation()` clears the pause record and resets retry tracking for that symbol; pending output must still be committed successfully before safe point advances;
- `ShardExecutionCore::quarantine_symbol_output_commit_escalation()` moves the escalation into a quarantine record without removing pending output or advancing safe point;
- `ShardExecutionCore::clear_symbol_output_commit_quarantine()` clears the quarantine record for an explicit manual retry; pending output still remains the source to commit before safe point can move;
- `SymbolRuntimeStatus::output_commit_blockage` gives callers a single summary of the active escalation or quarantine, including the decision and current pending-output pressure;
- `OutputBatchIdentity` gives each output commit attempt a stable symbol, input sequence range, entry count, matching-output version, and deterministic output digest;
- output batch id identifies the batch position and version, while output digest identifies the batch content;
- `JournalOutputEntry` can carry `JournalOutputCommitMetadata`, and `run_output_batch_commit_step_report_with_identity()` writes that metadata when the Journal appender supports the metadata append path;
- `OutputCommitMetadataIndex` can be rebuilt from durable `JournalOutputEntry` records, provides a disposable `batch_id -> metadata` lookup layer, reports missing / incomplete / complete lookup states, and treats a batch as complete only after observed entries reach `entry_count`;
- `OutputJournalClient::commit_one_with_metadata()` rejects a commit attempt when durable output already has the same batch id with a different digest;
- `OutputJournalClient::is_output_batch_durable()` uses the metadata index to resolve whether an entire output batch is already durable;
- `OutputJournalClient::query_output_batch()` returns explicit missing / incomplete / durable / conflict states for recovery and unknown-outcome resolution;
- `OutputJournalClient` maintains a rebuildable recent output metadata cache that can be warmed by successful metadata appends or rebuilt from Journal output; failed conflict rebuilds do not replace the previous usable cache;
- missing, incomplete, durable, and conflict query statuses are surfaced through output-only, integrated, pressure-aware, and retry-aware runtime reports when they explain output commit progress or blockage;
- `ShardExecutionCore::run_symbol_output_batch_commit_step()` now exposes that output batch identity for output-only commit attempts;
- `run_symbol_runtime_step_with_output_batch_commit()` and `ShardExecutionCore::run_symbol_retry_aware_step()` now also surface the attempted output batch identity;
- `SymbolRuntimeStatus::output_commit_blockage` preserves output batch query evidence while a symbol is escalated or quarantined, so paused symbols still explain whether they are blocked by missing, incomplete, durable, or conflict evidence;
- public API tests cover unresolved unknown, resolved-durable unknown, incomplete durable prefix, unavailable, rejected, and conflicting output outcomes in the middle of an output batch.
- `MatchingRuntimeConfig` now centralizes runtime-policy knobs for topology, runtime execution mode, handoff capacity, shard-runtime step size, output commit capacity / retry / batch size, snapshot retention, and snapshot verification. MatchingRuntime, ShardRuntime, and ShardExecutionCore can now be constructed from that config surface instead of keeping separate default constants or loose constructor parameters only.

Accepted mechanism:

- `SymbolRuntime` should produce deterministic output requests and place them into bounded pending output state.
- Output commit should run as a separate step, thread, or async task that drains pending output in batches and talks to Journal.
- Remote Journal latency should affect safe-point lag and pending-output pressure, not force each matching command to synchronously wait for a remote append.
- If pending output grows beyond the configured bound, matching intake or execution must be throttled rather than allowing unbounded memory growth or unsafe safe-point advancement.
- Safe point is the largest continuous Journal input sequence whose output has been confirmed durable.

Completion boundary:

- Phase 25 is complete for the learning-project contract: output commit ambiguity is represented explicitly, durable prefixes advance safe point conservatively, unresolved tails remain pending, batch identity and digest conflict are detected, and ShardExecutionCore exposes the evidence needed to pause, clear, quarantine, or retry a symbol.
- Threaded / async execution modes, production shutdown behavior, standby promotion, operational automation, and cross-symbol scheduling pressure move to Phase 26 and later phases.

## Completed Phase: Runtime Execution Modes and Pressure

This phase made inline `MatchingRuntime` execution the reference contract for future threaded and async modes. The goal was not to add threads yet, but to remove ambiguity from the runtime boundary before multiple execution modes can exist.

Progress so far:

- `MatchingRuntime` exposes configured inline execution, run-until-idle, drain, and shutdown entry points.
- Runtime input has explicit preflight behavior and rejects partial multi-shard enqueue when any target handoff cannot accept the batch.
- `close_input()` and `shutdown()` are distinct lifecycle operations: closed input can still drain, while shutdown rejects future input, execution, drain, and repeated shutdown calls.
- Runtime reports expose aggregate stop reasons, final status, lifecycle state, remaining work, blocked shards, blocked symbols, full input pressure, and full output pressure.
- Run-until-idle, drain, and shutdown reports carry enough final-state evidence for a service layer to decide whether work drained, blocked, or remains pending.
- Thread-per-shard and async-task-per-shard modes are intentionally still rejected until they preserve the same inline contract.

Completion boundary:

- Phase 26 is complete for the learning-project contract: inline execution now defines the lifecycle, pressure, remaining-work, blocked-symbol, and shutdown semantics that threaded and async modes must preserve.
- Phase 27 should start by implementing thread-per-shard execution against that contract instead of inventing a separate lifecycle model.

## Completed Phase: Thread-per-shard Execution Mode

This phase turned the thread-per-shard runtime from a configuration placeholder into a real execution mode while keeping the inline runtime as the behavioral reference.

Progress so far:

- `ThreadPerShardRuntimeSet` owns shard worker handles and can run each shard on its own worker thread.
- Matching input is still preflighted before enqueue, so a multi-shard batch is rejected without partial mutation when any target shard cannot accept it.
- Threaded workers process write-input, run-once, run-limited, status, and shutdown requests through explicit request / response payloads.
- Per-shard output writers are supported through a public output-factory constructor, allowing each shard to own its output append boundary.
- Output metadata now records optional `shard_id` and `shard_sequence`, giving downstream systems enough causal evidence without requiring matching output to be globally ordered.
- Shutdown captures final runtime status before worker teardown and joins threaded workers after shutdown responses.
- Unexpected worker requests and responses now return explicit `ShardRuntimeSetError` values instead of relying on panic paths.

Completion boundary:

- Phase 27 is complete for the learning-project contract: threaded execution preserves the same lifecycle, pressure, remaining-work, blocked-symbol, output durability, and safe-point semantics as inline execution.
- Phase 28 should implement async-task-per-shard execution against the same runtime-set contract instead of creating a separate async-only lifecycle.

## Difficulty Backlog

This backlog records hard problems discovered or expected during scenario-driven work. It is intentionally incomplete and should grow as tests and experiments expose new issues.

| Area | Difficulty | Current learning status |
| --- | --- | --- |
| Determinism | Same confirmed input must produce the same output events, order book state, checksums, and safe points across live execution and replay | Partially covered; replay can now regenerate output entries and match live output / checksum / safe point across representative command outcomes |
| Architecture extraction | Code should reflect responsibilities, state ownership, contracts, boundary rules, flows, failure modes, and validation from the architecture docs | Needs explicit inventory |
| Single writer | Each symbol order book must have exactly one mutation owner even when runtimes run concurrently | Basic symbol isolation exists; MatchingRuntime, ShardRuntime, ShardExecutionCore, and SymbolRuntime are now named as separate runtime ownership layers. Threaded and async execution modes must preserve this model |
| Backpressure | Bounded handoff and pending output buffer saturation must stop unsafe progress without unbounded memory growth | Basic bounded transfer buffers exist; MatchingRuntime status exposes pending-output pressure, output-only commit can relieve pressure before more input is consumed, and pressure-aware scheduling now does that automatically when pending output is full. Escalated symbols are paused without stopping unrelated symbols. Slow Journal scheduling is partially covered; production pacing still needs study |
| Output commit | Matching output must become durable before the runtime advances safe progress | Phase 25 learning contract complete. Output results are classified into explicit commit outcomes; exact durable unknowns can be resolved; incomplete batches advance only the confirmed prefix; conflicts are surfaced as deterministic output evidence; ShardExecutionCore preserves blockage evidence across escalation and quarantine. The adopted production direction is async batch Journal commit with bounded pending output and backpressure; threaded / async execution pressure, shutdown, and operational automation move to Phase 26+ |
| Output identity | Output batches need stable identity so retry does not duplicate or drift | Basic trade and market-sequence identity covered for trade outputs. Output commit attempts have `OutputBatchIdentity` metadata with symbol, input sequence range, entry count, matching-output version, and deterministic output digest. Batch id is separated from digest: same batch id with a different digest is treated as a conflict and rejected by the output journal client. `OutputCommitMetadataIndex` is a rebuildable lookup layer over durable Journal output metadata, and `OutputJournalClient` keeps a recent cache that can be warmed or rebuilt without becoming the source of truth |
| Market sequence | Per-symbol market sequence should be distinct from global journal sequence | Trade outputs carry `market_seq`; resting-order, cancel, and book-delta market events are not yet modeled |
| Control state | Matching-affecting config must enter at deterministic sequence positions | Not yet modeled |
| Messaging reliability | Reliable handoff needs explicit offset tracking, deduplication, retry, and poison/dead-letter behavior | Boundary identified; details deferred |
| Governance control | Halt, resume, symbol config, market mode, price-band, reduce-only, and fencing must become deterministic facts | Boundary identified; details deferred |
| Evidence | Matching, output commit, replay, recovery, and discrepancy decisions need explicit evidence records | Boundary identified; details deferred |
| Hot symbols | A saturated symbol should not corrupt unrelated symbols and should not hide overload signals | Not yet studied |
| Batch behavior | Batch size trades off throughput, latency, fairness, and retry cost | Not yet studied |
| Shutdown | Stopping during in-flight work must leave enough durable state to recover or retry safely | Not yet studied |
| Recovery | Snapshot plus replay must reconstruct state after failure and unknown outcomes | Snapshot restore plus replay tail now covers output identity, checksum, and safe point for the in-memory contract; production recovery flow is later |
| Observability | Metrics and runtime views must reveal lag, queue depth, safe point, and checksum without mutating state | Deferred until after internal pressure work |
