# NovaX Matching Core

NovaX Matching Core is a Rust learning project that rebuilds the core of a centralized exchange matching subsystem step by step. The repository is now the project source of truth for implementation progress and roadmap.

The target architecture follows a Matching Service reference model:

- `Matching Service` is the runtime container: confirmed input consumption, symbol routing, bounded handoff, per-symbol execution loops, output commit, snapshot coordination, recovery, and service-facing runtime behavior.
- `Matching Core` is the deterministic matching kernel inside that runtime: command application, order book mutation, matching result generation, checksums, replay, and snapshots.

This repository implements the project in stages. It does not copy the whole target architecture at once; it extracts the active capabilities needed for the current phase while keeping names, boundaries, and direction aligned with the target model.

The architecture reference remains the external CEX Matching Service reference directory. This repository keeps a symlink for convenient lookup:

```text
docs/matching-service-reference
```

The symlink points to:

```text
/Users/andrew/Library/Mobile Documents/iCloud~md~obsidian/Documents/My vault/28 - CEX/Architecture/Application/Matching Service
```

Use that reference directory for the application architecture, component documents, and engineering strategy notes. If implementation work reveals that the reference architecture needs adjustment, update the CEX reference documents through the symlink rather than creating a separate architecture source in this repository.

## Current Status

| Item | Status |
| --- | --- |
| Completed phases | Phase 0-20 |
| Current milestone | Service-facing query boundary |
| Current phase | Phase 21: Admin/query API |
| Latest verification | `cargo test -p matching-core` |

Implemented capabilities:

- Core domain types and command model.
- FIFO price levels and indexed order book.
- Limit order matching and cancellation.
- Command ingress validation.
- Engine output events, including order acknowledgements and trade events.
- Deterministic checksum support.
- Journal adapter input reader and output appender contracts.
- Replay runner.
- Order book snapshot and restore.
- Single-symbol `SymbolRuntime` with safe-point processing.
- Batch processing with retry-safe failure behavior.
- Multi-symbol `RuntimeManager` with per-symbol state isolation.
- `SymbolRouting` with registered-symbol routing and queue enqueue support.
- `BoundedHandoff` with bounded capacity, FIFO drain, watermarks, and retry prepend.
- Runtime loop step and one-shot worker thread.
- Output queue isolation.
- Output committer and output commit loop.
- Confirmed input consumer with bounded batch reads, gap detection, and backpressure-safe enqueue.
- Project roadmap document in this repository.

## Documentation

- [Roadmap](docs/roadmap.md)

This file replaces external project-progress notes for this repository. Architecture direction should still be read from `docs/matching-service-reference`.

## Architecture Principles

- A single symbol's order book has exactly one writer.
- Input commands are confirmed by a journal before matching.
- Output events must be committed before the runtime advances its safe point.
- Replay and snapshot behavior must remain deterministic.
- Bounded handoff is a runtime transfer and backpressure boundary, not a recovery source.
- Slow downstream consumers must not directly block the matching execution loop.
- Performance work is driven by benchmark evidence, not premature optimization.

## Repository Layout

```text
crates/
  matching-core/     Core matching engine library
  matching-service/  Service entry point placeholder
  matching-bench/    Benchmark crate placeholder

docs/
  roadmap.md         Project phase plan and current progress
  matching-service-reference/
                     Symlink to the external CEX Matching Service reference directory
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
