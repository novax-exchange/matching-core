# Journal 模型

[English](journal-model.md)

## 目的

Journal 模型定义撮合引擎周围的持久化边界。Matching Core 依赖 Journal contract，但生产级 Journal Service 是独立子系统。

撮合引擎使用两类逻辑流：

| 日志流 | 作用 |
|---|---|
| Matching Input Journal | 定义撮合必须处理的有序命令序列。 |
| Matching Output Event Log | 定义撮合产生的持久事实，例如 ack 和 trade。 |

## Input Journal

Input Journal 回答：

- 要处理什么命令？
- 按什么顺序处理？
- 稳定的 sequence number 是什么？
- 幂等 command identity 是什么？

撮合不能直接处理来自 API Gateway 或 Order Service 的可变更命令。上游系统必须先把命令 append 到 Input Journal，撮合只消费已确认的 entry。

## Output Event Log

Output Event Log 回答：

- 撮合产生了什么？
- 哪个 input sequence 触发了它？
- 对应哪个 command id？
- 哪些持久事实可以被结算、审计、行情和 replay 消费？

Settlement 和 Audit 必须依赖持久化输出事件，不能依赖撮合 runtime 的内存回调。

## 安全点

`last_input_seq` 是 runtime 的安全点。它表示：

> 到这个 input sequence 为止的所有状态变化都已经应用，并且对应输出事件已经成功提交。

如果 output append 失败，runtime 不能推进安全点。当前学习实现通过回滚状态变化来保证可重试语义。后续阶段会通过 output isolation 和持久化 Journal 处理，让生产行为更真实。

## Unknown Append Result

生产中的 Journal append 可能出现 unknown result，比如请求已经到达 Journal Service，但调用方超时。这个状态既不是成功，也不是失败。

目标生产规则是：

- 使用同一个 idempotency key 查询或重试。
- 不能生成重复输出事件。
- 在 append 结果明确前，不能推进安全点。

当前内存 contract 只建模 success 和 append failure。unknown result 会留到 durable journal adapter 阶段处理。

## Replay

Replay 从以下内容重建撮合状态：

1. Snapshot，如果存在。
2. Snapshot sequence 之后的 Matching Input Journal。
3. 确定性的撮合逻辑。

Output Event Log 用于审计和比对。引擎应该能从输入序列重新生成期望输出。

