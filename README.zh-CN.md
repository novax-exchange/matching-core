# NovaX Matching Core

[English](README.md) | [中文](README.zh-CN.md)

NovaX Matching Core 是 NovaX 中心化交易所架构中的确定性撮合子系统。它实现 confirmed order command 的处理、按交易对维护订单簿、生成撮合输出、推进 safe point，以及通过 replay 和 snapshot 重建状态所需要的核心能力。

当前仓库聚焦 deterministic core 和 recovery proof path。Service host、公开 API、部署模型和 benchmark suite 仍在建设中。

## 范围

### 包含

- 从 journal-backed command stream 读取 confirmed input。
- 按 symbol 进行 single-writer execution。
- 维护 price-time priority order book。
- 生成确定性的 matching output。
- 在 safe point 推进前完成 output commit coordination。
- 提供 replay、snapshot、restore 和 verification primitives。
- 提供 routing、handoff、backpressure、pending output 等 runtime boundary。

### 不包含

- 面向外部的 order-entry API。
- Account、wallet、settlement、custody 逻辑。
- Product configuration authority。
- 独立的 market-data distribution service。
- 完整的 deployment、failover 和 operations platform。

## 架构

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

Replay Runner 和 Snapshot Restore 会基于 durable facts 重建并验证状态。Snapshot 只是恢复加速点；Journal 仍然是 replay 的事实来源。

## 设计原则

- Deterministic replay 是一等要求。
- 每个 symbol 只有一个可写 execution owner。
- Input 必须先经过 journal confirmation，再进入 matching。
- Output 必须 durable 后，safe point 才能推进。
- Bounded queue 用来暴露 backpressure，而不是隐藏 overload。
- Snapshot capture 和 restore 必须绑定 safe point。
- Replay 和 restore 必须匹配 output、checksum 和 safe point。
- Matching hot path 不调用外部服务。

## 当前能力

### Matching Kernel

- Core domain types 和 command model。
- Command ingress validation。
- Limit order matching 和 cancellation。
- Order acknowledgement、trade event、market-data related event。
- 确定性的 trade sequence 和 market sequence 生成。

### Order Book

- Price-time priority bid / ask books。
- 同价位 FIFO queue。
- Indexed order lookup 和 cancellation。
- Deterministic checksum。
- 可恢复 order-book state 的 snapshot capture / restore。

### Runtime Path

- Single-symbol `SymbolRuntime`。
- Multi-symbol `RuntimeManager`。
- Registered-symbol routing。
- 带 watermark 和 retry prepend 的 bounded handoff queue。
- Per-symbol execution-loop step。
- 用于运行期调优的 runtime policy configuration surface。

### Output Commit

- Pending output buffer isolation。
- Output batch identity 和 digest metadata。
- Output journal client 和 batch coordinator。
- Unknown / unavailable / rejected output outcome handling。
- 只有 confirmed durable output 才能推进 safe point。

### Replay And Snapshot

- Replay Runner 用于 deterministic rebuild 和 comparison。
- Snapshot Store 用于 canonical snapshot bytes 和 verified manifest。
- Snapshot Restore 用于 safe-point restore 和 replay-tail processing。
- Snapshot verification orchestration 以及结构化 mismatch evidence。

## 仓库结构

```text
crates/
  matching-core/       deterministic matching library
  matching-service/    service host boundary, under construction
  matching-bench/      benchmark workspace, under construction

docs/
  matching-service-reference/
                       local architecture reference symlink
```

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
feat(core): add runtime manager
```

## 文档

详细的 Matching Service 架构参考维护在 NovaX architecture workspace 中。在本地开发环境里，本仓库通过下面的路径暴露这份参考：

```text
docs/matching-service-reference
```

这份参考覆盖 component boundary、deterministic recovery strategy、output commit 和 safe-point 规则、snapshot restore、replay、runtime management 以及 backpressure behavior。

## 状态

Matching Core 当前聚焦 deterministic execution、durable output coordination、replay、snapshot restore 和 verification support。Service host、external interfaces、production deployment model 和 benchmark suite 是后续重点。
