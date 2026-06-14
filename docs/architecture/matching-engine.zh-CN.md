# 撮合引擎架构

[English](matching-engine.md)

## 范围

NovaX Matching Core 负责一个或多个交易对的确定性撮合状态。它消费已经确认的撮合命令，修改对应 symbol 的订单簿，并产生确定性的输出事件。

范围内：

- 领域类型、订单、命令和引擎事件。
- per-symbol 订单簿状态。
- 价格时间优先撮合。
- 撤单。
- CommandIngress 入口校验。
- 确定性 Replay、Snapshot 和 checksum。
- Runtime 安全点管理。
- 多交易对 Runtime 管理。

范围外：

- 鉴权、限流和 API 签名。
- 账户余额、保证金、仓位和结算状态。
- 订单查询投影和完整订单生命周期所有权。
- 行情聚合和外部推送。
- 生产级持久化 Journal 实现。
- 集群级 leader election 和 failover 实现。

## 所有权模型

订单簿是撮合权威状态，但只属于撮合子系统内部。外部服务不能直接修改订单簿。

目标所有权模型：

| 状态 | 拥有者 | 说明 |
|---|---|---|
| 订单簿 | Per-symbol runtime | 单写入者。 |
| 输入命令序列 | Matching Input Journal | 撮合输入的持久事实源。 |
| 输出事件 | Matching Output Event Log | 撮合事实的持久事实源。 |
| Snapshot | Snapshot store | 恢复优化，必须绑定 journal sequence。 |
| 查询投影 | 下游服务 | 从输出事件派生，不能直接修改撮合状态。 |

## Runtime 规则

每个 symbol 由一个顺序 runtime 处理。这个 runtime 拥有该 symbol 的订单簿、CommandIngress、撮合状态、trade id 序列和安全点。

多个 symbol 可以独立运行，但同一个 symbol 不能有多个并发写入者。这样才能保持价格时间优先的确定性，并让 Replay 有意义。

## 事件边界

输入命令只有在 Input Journal 确认后，才能进入撮合。

输出事件只有在 Output Event Log 提交成功后，才是可靠事实。Runtime 只有在对应 output append 成功后，才能推进 `last_input_seq`。

当前实现使用内存 Journal contract 和可回滚的 runtime 测试来证明这个语义。后续阶段会用 output isolation 和持久化 journal adapter 替换当前的简单机制。

## 恢复模型

恢复基于：

1. 加载某个 symbol 的最新 snapshot。
2. 从 `snapshot.last_input_seq + 1` 继续 replay。
3. 确定性重建订单簿。
4. 比较 checksum 和 replay 结果。

核心要求是：同一段已确认输入序列，必须产生同样的订单簿状态和输出事件。

