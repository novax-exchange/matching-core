# Matching Core

[English](README.md) | [中文](README.zh-CN.md)

Matching Core is a deterministic matching library for a Matching Service.

This repository is not the whole exchange and not yet the production service process. It is the part that must stay explainable under replay: confirmed input goes in, order books change in a deterministic way, matching output is committed durably, and safe points move only after that output is known to be durable.

## What This Repo Owns

- Per-symbol order book state.
- Price-time priority matching behavior.
- Confirmed-input routing into symbol runtimes.
- Bounded handoff and runtime pressure signals.
- Output commit tracking before safe-point advancement.
- Snapshot, restore, replay, checksum, and verification primitives.

It does not own order-entry APIs, account balances, custody, settlement, fee calculation, or external market-data fan-out. Those belong to neighboring services.

## Architecture

The diagrams below mirror the Matching Service architecture reference.

### Service Context

```mermaid
flowchart LR
    subgraph MatchingGroup["Matching Service"]
        RuntimeShell["Service Runtime Shell"]
        Interface["Service Interface Boundary"]
        StreamBoundary["Messaging Reliability Boundary"]
        MatchingRuntime["Matching Runtime"]
        Execution["Per-Symbol Execution Pipeline"]
    end

    Transport["MQ / Stream Transport"]

    Input["Confirmed input stream"]
    Output["Durable output append"]

    subgraph GovernanceZone["Governance"]
        Governance["Product Configuration\nPlatform Risk Control\nOps Controls"]
    end

    subgraph InfraZone["Infrastructure"]
        Coordination["Leader Election"]
        SnapshotStore[("Snapshot Store")]
    end

    Input -->|"confirmed commands"| Transport
    Transport --> StreamBoundary --> MatchingRuntime
    MatchingRuntime -->|"per-symbol command"| Execution
    Execution -->|"OrderAck / TradeEvent / MarketDataEvent"| MatchingRuntime
    MatchingRuntime --> StreamBoundary -->|"matching output"| Transport
    Transport --> Output
    Governance -->|"governed control"| Transport
    RuntimeShell -.->|"health / readiness / scheduled tasks"| MatchingRuntime
    RuntimeShell -.-> Interface
    Interface --> MatchingRuntime
    MatchingRuntime <-->|"snapshot / restore"| SnapshotStore
    MatchingRuntime <-->|"leader lease / fencing"| Coordination

    style GovernanceZone fill:#f8fafc,stroke:#cbd5e1,stroke-dasharray: 4 4,color:#64748b;
    style InfraZone fill:#f8fafc,stroke:#cbd5e1,stroke-dasharray: 4 4,color:#64748b;
```

### Component View

```mermaid
%%{init: {"flowchart": {"nodeSpacing": 28, "rankSpacing": 38, "diagramPadding": 12, "subGraphTitleMargin": {"top": 8, "bottom": 10}}}}%%
flowchart TB
    Journal["Trading Event Journal"]
    SnapshotBytes[("Snapshot Bytes\n.snap")]
    VerifiedManifest[("Verified Manifest\n.verified")]

    EventBus["MQ / Derived Streams"]

    subgraph Boundary["Interface / Messaging Boundary"]
        RuntimeShell["Service Runtime Shell"]
        Interface["Service Interface Boundary"]
        Messaging["Messaging Reliability Boundary"]
        JournalAdapter["Journal Adapter"]
    end

    subgraph Control["Control"]
        Governance["Governance Control Boundary"]
        Evidence["Evidence Boundary"]
    end

    subgraph Input["Input"]
        Consumer["Confirmed Input Consumer"]
        Router["Symbol Routing"]
        Handoff["Bounded Handoff"]
    end

    subgraph Execution["Execution"]
        MatchingRuntime["Matching Runtime"]
        ShardRuntime["Shard Runtime(s)\nshard 0, shard 1, ..."]
        ExecutionCore["Shard Execution Core"]
        SymbolRuntime["Symbol Runtime(s)\nBTC-USDT, ETH-USDT, ..."]
        Engine["Matching Engine"]
        Book["Order Book"]
    end

    subgraph Output["Output"]
        Commit["Output Commit Boundary"]
    end

    subgraph Recovery["Recovery"]
        Snapshot["Snapshot Restore"]
        SnapshotStore["Snapshot Store"]
        Replay["Replay Runner"]
    end

    Journal --> JournalAdapter --> Consumer
    JournalAdapter --> Messaging
    EventBus --> Messaging
    RuntimeShell -.->|"runtime context"| Interface
    RuntimeShell -.->|"dependency / pressure context"| Messaging
    RuntimeShell -.->|"governed runtime context"| Governance
    RuntimeShell -.->|"trace / degradation context"| Evidence
    Interface --> Governance
    Governance -.-> MatchingRuntime
    MatchingRuntime -.->|"status"| Interface
    RuntimeShell -.->|"lifecycle"| MatchingRuntime
    MatchingRuntime --> ShardRuntime
    Consumer --> Router
    Router --> Handoff
    Handoff --> ShardRuntime
    Governance -.-> ShardRuntime
    ShardRuntime --> ExecutionCore
    ExecutionCore --> SymbolRuntime
    SymbolRuntime --> Engine
    Engine --> Book
    Engine --> SymbolRuntime
    SymbolRuntime --> ExecutionCore
    ExecutionCore --> Commit
    Commit --> Messaging --> EventBus
    Commit --> JournalAdapter --> Journal
    Commit -.->|"safe point"| ExecutionCore
    Governance -.-> Evidence
    MatchingRuntime -.-> Evidence
    ShardRuntime -.-> Evidence
    ExecutionCore -.-> Evidence
    SymbolRuntime -.-> Evidence
    Commit -.-> Evidence
    RuntimeShell -.->|"scheduled snapshot verification task"| Snapshot
    SymbolRuntime --> Snapshot
    Snapshot --> SnapshotStore
    Snapshot -.->|"verification replay / comparison"| Replay
    Snapshot -.->|"signed verification evidence"| SnapshotStore
    Snapshot -.->|"mismatch evidence"| Evidence
    SnapshotStore --> SnapshotBytes
    SnapshotBytes --> SnapshotStore
    SnapshotStore --> VerifiedManifest
    VerifiedManifest --> SnapshotStore
    SnapshotStore --> Snapshot
    Interface -.-> Replay
    JournalAdapter -.-> Replay
    Replay --> SymbolRuntime
```

`Matching Runtime` owns the in-process core runtime. `Shard Runtime` owns shard-level scheduling and pressure; `Shard Execution Core` owns the per-shard symbol set and safe-point discipline; `Symbol Runtime` owns one symbol's deterministic order book execution. Snapshot bytes and verified manifests are stored recovery artifacts, not live runtime components.

## Current State

The core crate already has working pieces for:

- Domain types, command validation, limit orders, cancellation, acknowledgements, trades, and market events.
- Deterministic bid / ask books with FIFO price levels, indexed cancellation, checksum, snapshot, and restore.
- Multi-symbol runtime management, symbol routing, bounded handoff queues, configured inline matching runtime runs, input-batch preflight, drain boundaries, pending output pressure, and runtime policy configuration.
- Output batch identity, output commit retry / query handling, and safe-point advancement after durable output.
- Replay, snapshot storage, verified manifests, and snapshot verification evidence.

The service crate is still a boundary under construction. Public APIs, deployment, production operations, and benchmark reporting are not finished here yet.

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
feat(core): add shard runtime scheduling
```

## Documentation

The full Matching Service architecture reference is maintained outside this repository. The repository-local roadmap lives in:

```text
docs/roadmap.md
```
