# Architecture Documentation

[Simplified Chinese](README.zh-CN.md)

This directory contains the GitHub-facing architecture notes for NovaX Matching Core. These files are adapted from the longer design notes used during the learning project, but rewritten so they can stand alone in the repository without depending on Obsidian links or local vault paths.

## Documents

| Document | Purpose |
|---|---|
| [Matching Engine Architecture](matching-engine.md) | Defines the matching subsystem boundary, ownership model, runtime rules, and recovery assumptions. |
| [Journal Model](journal-model.md) | Defines the relationship between matching input journals, output event logs, safe points, and replay. |
| [Runtime Model](runtime-model.md) | Describes how `SymbolRuntime`, `RuntimeManager`, batching, and future routing/queue stages fit together. |

## Core Principles

- A single symbol has exactly one order book writer.
- Matching input must be confirmed by a journal before it is processed.
- Matching output must be committed before the runtime advances its safe point.
- Replay, snapshot restore, and checksum validation must remain deterministic.
- Runtime queues and output isolation are operational boundaries, not sources of truth.

