# Runtime Model

[Simplified Chinese](runtime-model.zh-CN.md)

## Current Runtime Layers

The current implementation has two runtime layers:

| Layer | Responsibility |
|---|---|
| `SymbolRuntime` | Owns one symbol's order book, ingress validation, trade ID sequence, and safe point. |
| `RuntimeManager` | Owns a registry of symbol runtimes and routes entries by command symbol. |

This is the first step from a single-symbol engine toward a multi-symbol service.

## SymbolRuntime

`SymbolRuntime` processes one input entry at a time:

1. Validate the command through `CommandIngress`.
2. Apply the command to the order book if valid.
3. Generate output events.
4. Append output events to `OutputJournal`.
5. Advance `last_input_seq` only after append succeeds.

For commands that mutate state before output append, the current learning implementation stores a rollback snapshot before processing. This is intentionally simple and will later be replaced by output isolation and recovery-oriented behavior.

## RuntimeManager

`RuntimeManager` manages multiple `SymbolRuntime` instances:

- `add_symbol(symbol)` registers a runtime.
- `process_entry(entry, output)` routes by `entry.command.symbol()`.
- `process_batch(entries, output)` processes entries in input order and stops on first error.
- `last_input_seq(symbol)` exposes per-symbol progress.

Unknown symbols return `RuntimeManagerError::UnknownSymbol` instead of panicking.

Output append failures are mapped to `RuntimeManagerError::OutputAppendFailed`, while the underlying `SymbolRuntime` preserves its safe-point semantics.

## Batch Semantics

Batch processing is ordered and stop-on-failure:

```text
seq 1 succeeds -> keep effects and advance safe point
seq 2 fails    -> rollback its effects and stop
seq 3          -> not processed
```

For multi-symbol batches, input order remains global, while safe points are tracked per symbol.

Example:

```text
seq 1 BTC -> BTC last_input_seq = 1
seq 2 ETH -> ETH last_input_seq = 2
seq 3 BTC -> BTC last_input_seq = 3
```

## Future Runtime Stages

The roadmap continues with:

- `SymbolRouter`: explicit routing boundary from journal consumer to per-symbol runtime queues.
- Bounded input handoff: RingBuffer-style queue with backpressure.
- Thread model: journal reader and runtimes separated into controlled execution loops.
- Output isolation: commit output without letting slow I/O directly block input handling.
- Durable journal adapter: restart and replay against persistent logs.

The current manager is intentionally simpler than the final service runtime, but it establishes the core ownership and routing semantics.

