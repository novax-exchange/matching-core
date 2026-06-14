# Runtime 模型

[English](runtime-model.md)

## 当前 Runtime 分层

当前实现有两层 runtime：

| 层级 | 职责 |
|---|---|
| `SymbolRuntime` | 拥有单个 symbol 的订单簿、入口校验、trade id 序列和安全点。 |
| `RuntimeManager` | 拥有多个 symbol runtime 的注册表，并按 command symbol 路由 entry。 |

这是从单交易对引擎走向多交易对服务的第一步。

## SymbolRuntime

`SymbolRuntime` 每次处理一条 input entry：

1. 通过 `CommandIngress` 校验命令。
2. 如果命令合法，把它应用到订单簿。
3. 生成输出事件。
4. 将输出事件 append 到 `OutputJournal`。
5. 只有 append 成功后，才推进 `last_input_seq`。

对于会在 output append 前修改状态的命令，当前学习实现会在处理前保存一个可回滚状态。这是有意保持简单的实现，后续会用 output isolation 和面向恢复的机制替代。

## RuntimeManager

`RuntimeManager` 管理多个 `SymbolRuntime`：

- `add_symbol(symbol)` 注册 runtime。
- `process_entry(entry, output)` 根据 `entry.command.symbol()` 路由。
- `process_batch(entries, output)` 按输入顺序处理，遇到第一个错误停止。
- `last_input_seq(symbol)` 暴露 per-symbol 进度。

未知 symbol 返回 `RuntimeManagerError::UnknownSymbol`，而不是 panic。

Output append 失败会被映射为 `RuntimeManagerError::OutputAppendFailed`，底层 `SymbolRuntime` 仍然保证自己的安全点语义。

## Batch 语义

Batch 处理是有序、遇错停止：

```text
seq 1 成功 -> 保留效果并推进安全点
seq 2 失败 -> 回滚该 entry 的效果并停止
seq 3      -> 不处理
```

对于多 symbol batch，输入顺序仍然是全局顺序，但安全点按 symbol 分别维护。

例子：

```text
seq 1 BTC -> BTC last_input_seq = 1
seq 2 ETH -> ETH last_input_seq = 2
seq 3 BTC -> BTC last_input_seq = 3
```

## 后续 Runtime 阶段

路线图后续包括：

- `SymbolRouter`：从 JournalConsumer 到 per-symbol runtime queue 的显式路由边界。
- Bounded input handoff：RingBuffer-style queue 和背压。
- Thread model：Journal reader 和 runtime 拆成受控执行循环。
- Output isolation：提交输出时，不让慢 I/O 直接阻塞输入处理。
- Durable journal adapter：基于持久化日志重启和 replay。

当前 manager 比最终服务 runtime 简单，但已经建立了核心所有权和路由语义。

