# 架构文档

[English](README.md)

本目录保存 NovaX Matching Core 面向 GitHub 的架构文档。这些内容来自学习项目中的长篇设计笔记，但已经整理成可以在仓库中独立阅读的版本，不依赖 Obsidian 链接或本地知识库路径。

## 文档列表

| 文档 | 说明 |
|---|---|
| [撮合引擎架构](matching-engine.zh-CN.md) | 定义撮合子系统边界、状态所有权、运行规则和恢复假设。 |
| [Journal 模型](journal-model.zh-CN.md) | 定义 Matching Input Journal、Output Event Log、安全点和 Replay 的关系。 |
| [Runtime 模型](runtime-model.zh-CN.md) | 说明 `SymbolRuntime`、`RuntimeManager`、批处理，以及后续 Router / Queue 阶段如何衔接。 |

## 核心原则

- 同一 symbol 的订单簿只有一个写入者。
- 撮合输入必须先被 Journal 确认，再进入撮合。
- 撮合输出必须先提交成功，runtime 才能推进安全点。
- Replay、Snapshot restore 和 checksum 校验必须保持确定性。
- Runtime queue 和 output isolation 是运行边界，不是事实源。

