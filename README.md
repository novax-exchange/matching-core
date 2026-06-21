# NovaX Matching Core

[English](README.md) | [中文](README.zh-CN.md)

NovaX Matching Core is the deterministic matching library behind the NovaX Matching Service.

This repository is not the whole exchange and not yet the production service host. It is the part that must stay explainable under replay: confirmed input goes in, order books change in a deterministic way, matching output is committed durably, and safe points move only after that output is known to be durable.

## What This Repo Owns

- Per-symbol order book state.
- Price-time priority matching behavior.
- Confirmed-input routing into symbol runtimes.
- Bounded handoff and runtime pressure signals.
- Output commit tracking before safe-point advancement.
- Snapshot, restore, replay, checksum, and verification primitives.

It does not own order-entry APIs, account balances, custody, settlement, fee calculation, or external market-data fan-out. Those belong to neighboring services.

## Architecture

The diagrams below come from the Matching Service architecture reference in `docs/matching-service-reference/Matching Service.md`.

### Service Context

```mermaid
flowchart LR
    subgraph MatchingGroup["Matching Service"]
        RuntimeShell["Service Runtime Shell"]
        Interface["Service Interface Boundary"]
        StreamBoundary["Messaging Reliability Boundary"]
        Service["Matching Service"]
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
    Transport --> StreamBoundary --> Service
    Service -->|"per-symbol command"| Execution
    Execution -->|"OrderAck / TradeEvent / MarketDataEvent"| Service
    Service --> StreamBoundary -->|"matching output"| Transport
    Transport --> Output
    Governance -->|"governed control"| Transport
    RuntimeShell -.->|"health / readiness / scheduled tasks"| Service
    RuntimeShell -.-> Interface
    Interface --> Service
    Service <-->|"snapshot / restore"| SnapshotStore
    Service <-->|"leader lease / fencing"| Coordination

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
        Manager["Runtime Manager"]
        Loops["Per-Symbol Execution Loops\n(BTC-USDT, ETH-USDT, ...)"]
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
    Governance -.-> Manager
    Manager -.->|"status view"| Interface
    RuntimeShell -.->|"startup / drain / readiness"| Manager
    Manager -.->|"lifecycle"| Loops
    Consumer --> Router
    Router --> Handoff
    Handoff --> Loops
    Governance -.-> Loops
    Loops --> Engine
    Engine --> Book
    Engine --> Loops
    Loops --> Commit
    Commit --> Messaging --> EventBus
    Commit --> JournalAdapter --> Journal
    Commit -.->|"safe point"| Loops
    Governance -.-> Evidence
    Loops -.-> Evidence
    Commit -.-> Evidence
    Manager -.-> Evidence
    RuntimeShell -.->|"scheduled snapshot verification task"| Snapshot
    Loops --> Snapshot
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
    Replay --> Loops
```

`Per-Symbol Execution Loops` represents many runtime instances, usually one per symbol or symbol group. Snapshot bytes and verified manifests are stored recovery artifacts, not live runtime components.

## Current State

The core crate already has working pieces for:

- Domain types, command validation, limit orders, cancellation, acknowledgements, trades, and market events.
- Deterministic bid / ask books with FIFO price levels, indexed cancellation, checksum, snapshot, and restore.
- Multi-symbol runtime management, symbol routing, bounded handoff queues, budgeted runtime-loop scheduling, pending output pressure, and runtime policy configuration.
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
feat(core): add budgeted runtime loop scheduling
```

## Documentation

The full Matching Service architecture reference is maintained in the NovaX architecture workspace and exposed locally through:

```text
docs/matching-service-reference
```

The repository-local roadmap lives in:

```text
docs/roadmap.md
```
