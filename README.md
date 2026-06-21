# NovaX Matching Core

[English](README.md) | [中文](README.zh-CN.md)

NovaX Matching Core is the deterministic matching subsystem for the NovaX centralized exchange architecture. It implements the core mechanics required to process confirmed order commands, mutate per-symbol order books, emit matching output, advance safe points, and rebuild state through replay and snapshots.

The repository currently focuses on the deterministic core and recovery proof path. The service host, public APIs, deployment model, and benchmark suite are still under construction.

## Scope

### In Scope

- Confirmed input processing from a journal-backed command stream.
- Per-symbol single-writer execution.
- Price-time priority order book mutation.
- Deterministic matching output generation.
- Output commit coordination before safe-point advancement.
- Replay, snapshot, restore, and verification primitives.
- Runtime boundaries for routing, handoff, backpressure, and pending output.

### Out of Scope

- Public order-entry APIs.
- Account, wallet, settlement, and custody logic.
- Product configuration authority.
- Market-data distribution as a standalone service.
- Full deployment, failover, and operations platform.

## Architecture

```text
Confirmed Journal Input
        |
        v
Confirmed Input Consumer
        |
        v
Symbol Routing
        |
        v
Bounded Handoff
        |
        v
Per-Symbol Execution Loop
        |
        v
Matching Engine + Order Book
        |
        v
Output Commit Boundary
        |
        v
Durable Output / Safe Point
```

Replay Runner and Snapshot Restore rebuild and verify state from durable facts. A snapshot is only a recovery acceleration point; the journal remains the source of truth for replay.

## Design Principles

- Deterministic replay is a first-class requirement.
- Each symbol has one writable execution owner.
- Input must be journal-confirmed before matching.
- Output must be durable before the safe point advances.
- Bounded queues expose backpressure instead of hiding overload.
- Snapshot capture and restore are tied to safe points.
- Replay and restore must match output, checksum, and safe point.
- External services are not called from the matching hot path.

## Current Capabilities

### Matching Kernel

- Core domain types and command model.
- Command ingress validation.
- Limit order matching and cancellation.
- Order acknowledgements, trade events, and market-data related events.
- Deterministic trade and market sequence generation.

### Order Book

- Price-time priority bid / ask books.
- FIFO queues at the same price level.
- Indexed order lookup and cancellation.
- Deterministic checksum support.
- Snapshot capture and restore of recoverable order-book state.

### Runtime Path

- Single-symbol `SymbolRuntime`.
- Multi-symbol `RuntimeManager`.
- Registered-symbol routing.
- Bounded handoff queues with watermarks and retry prepend.
- Per-symbol execution-loop steps.
- Runtime policy configuration surface for operational tuning.

### Output Commit

- Pending output buffer isolation.
- Output batch identity and digest metadata.
- Output journal client and batch coordinator.
- Unknown / unavailable / rejected output outcome handling.
- Safe-point advancement only after confirmed durable output.

### Replay And Snapshot

- Replay Runner for deterministic rebuild and comparison.
- Snapshot Store for canonical snapshot bytes and verified manifests.
- Snapshot Restore for safe-point restore and replay-tail processing.
- Snapshot verification orchestration with structured mismatch evidence.

## Repository Layout

```text
crates/
  matching-core/       deterministic matching library
  matching-service/    service host boundary, under construction
  matching-bench/      benchmark workspace, under construction

docs/
  matching-service-reference/
                       local architecture reference symlink
```

## Development

Useful commands:

```bash
cargo fmt -p matching-core
cargo test -p matching-core
```

Run the full workspace when service-level changes are involved:

```bash
cargo test
```

Commit messages use concise Conventional Commit style:

```text
feat(core): add runtime manager
```

## Documentation

The detailed Matching Service architecture reference is maintained in the NovaX architecture workspace. In the local development environment, this repository exposes it through:

```text
docs/matching-service-reference
```

The reference covers component boundaries, deterministic recovery strategy, output commit and safe-point rules, snapshot restore, replay, runtime management, and backpressure behavior.

## Status

Matching Core is currently focused on deterministic execution, durable output coordination, replay, snapshot restore, and verification support. The service host, external interfaces, production deployment model, and benchmark suite are active follow-up areas.
