# NovaX 撮合核心

[English](README.md) | 简体中文

NovaX Matching Core 是一个用 Rust 逐步重建中心化交易所撮合核心的学习项目。这个项目的目标不是最快把代码写完，而是在模拟商业化 CEX 的约束下，边学习 Rust，边实现一个具备生产方向的撮合子系统。

本仓库属于 NovaX 交易所系统的一部分，聚焦撮合核心：领域类型、订单、订单簿、撮合结果、命令入口校验、Journal、Replay、Snapshot、单交易对 Runtime 和多交易对 Runtime 管理。

## 目标

长期目标是交付一个完整的撮合引擎子系统，具备：

- 多交易对运行能力。
- 同一交易对内部保持单写入者撮合循环。
- Journal before matching 输入流程。
- Output Journal 确认后才推进安全点。
- Snapshot、Replay 和 checksum 校验。
- 批量输入处理和明确的失败语义。
- RingBuffer-style bounded input handoff 和 output isolation。
- 可观测性、性能基准和面向性能的数据结构演进。
- Standby replay、leader lease、fencing、failover 和 zero-downtime upgrade 验证。

项目早期允许使用简单实现来证明语义，但模块边界、错误语义和测试契约必须保持生产级方向。

## 当前状态

当前进度：

| 项目 | 状态 |
|---|---|
| 已完成阶段 | Phase 0-17 |
| 当前里程碑 | 有界输入交接 |
| 下一阶段 | Phase 18：线程模型 |
| 最新验证方式 | `cargo test` |

已实现能力：

- 核心领域类型和命令模型。
- FIFO 价格档位和带索引的订单簿。
- 限价单撮合和撤单。
- CommandIngress 命令入口校验。
- Engine 输出事件，包括 OrderAck 和 TradeEvent。
- 确定性 checksum。
- 内存 InputJournal / OutputJournal contract。
- Replay Runner。
- 订单簿 Snapshot 和 Restore。
- 单交易对 `SymbolRuntime`，支持安全点推进。
- 批量处理，支持失败后可重试语义。
- 多交易对 `RuntimeManager`，支持 per-symbol 状态隔离。
- `SymbolRouter`，支持已注册 symbol 路由和 batch 分组。
- `PerSymbolInputQueue`，支持有界容量、FIFO drain、水位状态和 router enqueue。

运行测试：

```bash
cargo test
```

## 学习方式

本项目默认采用学习模式：

1. 明确当前 Phase 的目标和 Rust 学习点。
2. 先写一个最小失败测试。
3. 写最小实现让测试通过。
4. Review 设计选择和 Rust 概念。
5. 进入下一阶段前运行验证命令。
6. 每完成一个阶段，提交并推送到 GitHub。

学习原则是：实现可以阶段性简单，但边界不能走偏。比如早期可以用 clone 订单簿来证明失败重试语义，但后续会通过 output isolation、replay 和恢复机制替换热路径里的简单方案。

## 路线图

| Phase | 状态 | 阶段目标 | 验证方式 |
|---|---|---|---|
| 0 | 已完成 | 创建最小 Rust workspace | 空测试通过 |
| 1 | 已完成 | 定义核心领域类型 | 类型构造测试 |
| 2 | 已完成 | 定义订单与命令模型 | 下单 / 撤单命令测试 |
| 3 | 已完成 | 实现价格档位 | FIFO 测试 |
| 4 | 已完成 | 实现订单簿基础结构 | best bid / best ask / index 测试 |
| 5 | 已完成 | 实现限价单撮合 | 完全成交、部分成交、挂单测试 |
| 6 | 已完成 | 实现撤单 | 撤单成功 / 失败测试 |
| 7 | 已完成 | 实现 CommandIngress | 非法 symbol / price / quantity 测试 |
| 8 | 已完成 | 实现输出事件模型 | Ack / TradeEvent 测试 |
| 9 | 已完成 | 实现确定性 checksum | 相同输入 checksum 一致 |
| 10 | 已完成 | 实现 Journal contract | append / read / latest seq 测试 |
| 11 | 已完成 | 实现 Replay Runner | replay checksum 一致 |
| 12 | 已完成 | 实现 Snapshot | snapshot / restore checksum 一致 |
| 13 | 已完成 | 实现 SymbolRuntime | output commit 成功后推进安全点 |
| 14 | 已完成 | 实现批量处理 | batch 失败停在安全点 |
| 15 | 已完成 | 实现 RuntimeManager | BTC / ETH runtime 状态隔离 |
| 16 | 已完成 | 实现 SymbolRouter | 按 symbol 分发输入 |
| 17 | 已完成 | 实现 bounded input handoff | 队列满、顺序消费、水位测试 |
| 18 | 下一步 | 引入线程模型 | Journal reader 与 runtime 分离 |
| 19 | 计划中 | 实现 output isolation | 慢输出不直接阻塞输入读取 |
| 20 | 计划中 | 实现持久化 Journal adapter | 重启后 replay 恢复 |
| 21 | 计划中 | 实现 Admin / Query API | 查询 cursor、checksum、depth |
| 22 | 计划中 | 实现可观测性 | tracing 和 metrics 可见 |
| 23 | 计划中 | 建立性能基准 | 单 symbol / 多 symbol benchmark |
| 24 | 计划中 | 强化订单簿数据结构 | benchmark 对比报告 |
| 25 | 计划中 | 强化 RingBuffer-style handoff | queue benchmark 改善 |
| 26 | 计划中 | 引入 CPU 稳定性优化 | p99 jitter 对比 |
| 27 | 计划中 | 实现 shard / hot-symbol placement | symbol ownership 和迁移测试 |
| 28 | 计划中 | 实现 standby replay | standby checksum 追平 |
| 29 | 计划中 | 实现 leader lease / fencing | 失去 lease 后停止处理 |
| 30 | 计划中 | 实现 failover 演练 | standby 晋升后状态一致 |
| 31 | 计划中 | 实现 zero-downtime upgrade 验证 | 新旧版本 replay 一致 |
| 32 | 计划中 | 完整验收与文档回填 | 测试、bench、故障演练和文档 |

## 架构原则

- 同一 symbol 的订单簿只有一个写入者。
- 输入命令必须先被 Journal 确认，再进入撮合。
- 输出事件提交成功后，Runtime 才能推进安全点。
- Replay 和 Snapshot 必须保持确定性。
- 背压和输出隔离是明确的系统边界，不是事后补丁。
- 性能优化必须基于 benchmark，而不是提前猜测。

## 仓库结构

```text
crates/
  matching-core/     撮合核心库
  matching-service/  服务入口占位
  matching-bench/    Benchmark crate 占位
```

## 开发命令

常用命令：

```bash
cargo test
cargo test -p matching-core
```

Commit message 使用英文和简洁的 Conventional Commit 风格，例如：

```text
feat(core): add runtime manager
```
