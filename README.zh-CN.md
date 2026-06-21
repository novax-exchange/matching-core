# NovaX Matching Core

[English](README.md) | [中文](README.zh-CN.md)

NovaX Matching Core 是 NovaX Matching Service 背后的确定性撮合库。

这个仓库不是完整交易所，也还不是生产级 service host。它负责那条必须能被 replay 解释清楚的核心路径：confirmed input 进入系统，order book 以确定性方式变化，matching output 先完成 durable commit，然后 safe point 才能推进。

## 这个仓库负责什么

- 每个 symbol 的 order book state。
- Price-time priority 撮合行为。
- Confirmed input 到 symbol runtime 的路由。
- Bounded handoff 和 runtime pressure signal。
- Safe point 推进前的 output commit tracking。
- Snapshot、restore、replay、checksum 和 verification primitives。

它不负责 order-entry API、account balance、custody、settlement、fee calculation 或对外 market-data fan-out。这些属于相邻服务。

## 架构

下面两张图直接来自 Matching Service 架构参考：`docs/matching-service-reference/Matching Service.md`。

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
        Manager["Runtime Manager"]
        Loops["Symbol Runtimes\n(BTC-USDT, ETH-USDT, ...)"]
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

`Symbol Runtimes` 表示多个 runtime instance，通常每个 symbol 一个。Snapshot bytes 和 verified manifests 是恢复用的持久化 artifact，不是 live runtime component。

## 当前状态

`matching-core` crate 目前已经覆盖：

- Domain types、command validation、limit order、cancel、ack、trade 和 market event。
- 确定性的 bid / ask book、同价位 FIFO、indexed cancellation、checksum、snapshot 和 restore。
- Multi-symbol runtime management、symbol routing、bounded handoff queue、limited runtime-loop scheduling、pending output pressure 和 runtime policy config。
- Output batch identity、output commit retry / query handling，以及 durable output 之后的 safe-point advancement。
- Replay、snapshot storage、verified manifest 和 snapshot verification evidence。

`matching-service` crate 仍然是建设中的 service boundary。公开 API、部署、生产运维和 benchmark 报告还没有在这个仓库里完成。

## 开发

常用命令：

```bash
cargo fmt -p matching-core
cargo test -p matching-core
```

涉及 service-level 改动时，运行完整 workspace：

```bash
cargo test
```

Commit message 使用简洁的 Conventional Commit 风格：

```text
feat(core): add limited runtime loop scheduling
```

## 文档

完整的 Matching Service 架构参考维护在 NovaX architecture workspace 中，本地通过下面路径暴露：

```text
docs/matching-service-reference
```

仓库内 roadmap 在：

```text
docs/roadmap.md
```
