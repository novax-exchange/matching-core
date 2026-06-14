# Journal Model

[Simplified Chinese](journal-model.zh-CN.md)

## Purpose

The journal model defines the durable boundary around the matching engine. Matching Core depends on journal contracts, but the production journal service is a separate subsystem.

The matching engine uses two logical streams:

| Stream | Purpose |
|---|---|
| Matching Input Journal | Defines the ordered command sequence that matching must process. |
| Matching Output Event Log | Defines the durable facts produced by matching, such as acknowledgements and trades. |

## Input Journal

The input journal answers:

- What command should be processed?
- In what order?
- Under which stable sequence number?
- With which idempotent command identity?

Matching must not process mutating commands directly from an API gateway or order service. Those systems first append commands to the input journal; matching consumes confirmed entries.

## Output Event Log

The output event log answers:

- What did matching produce?
- Which input sequence caused it?
- Which command ID does it correspond to?
- Which durable facts can settlement, audit, market data, and replay consume?

Settlement and audit must rely on durable output events, not on in-memory callbacks from the matching runtime.

## Safe Point

`last_input_seq` is the runtime safe point. It means:

> All effects up to this input sequence have been applied and their output events have been successfully committed.

If output append fails, the runtime must not advance the safe point. In the current learning implementation, state changes are rolled back for retry-safety. In later phases, output isolation and durable journal handling will make the production behavior more realistic.

## Unknown Append Result

Production journal append can have an unknown result, such as timeout after the request reached the journal service. That state is neither success nor failure.

The intended production rule is:

- Retry or query with the same idempotency key.
- Do not generate duplicate output events.
- Do not advance the safe point until the append result is known.

The current in-memory contract only models success and append failure. Unknown result handling is left for durable journal adapter phases.

## Replay

Replay reconstructs matching state from:

1. Snapshot, if available.
2. Matching Input Journal after the snapshot sequence.
3. Deterministic matching logic.

The output event log is used for audit and comparison. The engine should be able to regenerate expected output from the input sequence.

