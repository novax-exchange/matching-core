# Matching Engine Architecture

[Simplified Chinese](matching-engine.zh-CN.md)

## Scope

NovaX Matching Core owns the deterministic matching state for one or more trading symbols. It is responsible for applying confirmed matching commands to per-symbol order books and producing deterministic output events.

In scope:

- Domain types, orders, commands, and engine events.
- Per-symbol order book state.
- Price-time priority matching.
- Cancellation.
- Command ingress validation.
- Deterministic replay, snapshots, and checksums.
- Runtime safe-point management.
- Multi-symbol runtime management.

Out of scope:

- Authentication, rate limits, and API request signing.
- Account balance, margin, position, and settlement state.
- Order query projections and full order lifecycle ownership.
- Market data aggregation and external push delivery.
- Durable production journal implementation.
- Cluster-level leader election and failover implementation.

## Ownership Model

The order book is authoritative matching state, but only inside the matching subsystem. External services must not modify it directly.

The intended ownership model is:

| State | Owner | Notes |
|---|---|---|
| Order book | Per-symbol runtime | Single writer only. |
| Input command sequence | Matching Input Journal | Durable source of matching inputs. |
| Output events | Matching Output Event Log | Durable source of matching facts. |
| Snapshot | Snapshot store | Recovery optimization, always tied to a journal sequence. |
| Query projection | Downstream services | Derived from output events, not from direct mutation. |

## Runtime Rule

Each symbol is processed by one sequential runtime. That runtime owns the order book, command ingress, matching state, trade ID sequence, and safe point for that symbol.

Multiple symbols can run independently, but a single symbol must not have multiple concurrent writers. This preserves deterministic price-time priority and makes replay meaningful.

## Event Boundary

Input commands are not considered safe to process until they have been confirmed by the input journal.

Output events are not considered durable facts until they have been committed to the output event log. A runtime only advances `last_input_seq` after the corresponding output append succeeds.

Current implementation uses in-memory journal contracts and rollback-friendly runtime tests to prove this behavior. Later phases replace the simple mechanism with output isolation and durable journal adapters.

## Recovery Model

Recovery is based on:

1. Load the latest snapshot for a symbol.
2. Continue replay from `snapshot.last_input_seq + 1`.
3. Rebuild the order book deterministically.
4. Compare checksum and replay result.

The core requirement is that the same confirmed input sequence must produce the same order book state and output events.

