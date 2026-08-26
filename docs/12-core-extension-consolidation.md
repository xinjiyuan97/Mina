# Core/Extension 两包收敛

> 状态：物理合并已完成。Rust workspace 只保留 `agent-core`、`agent-extension` 两个内部库，Server/CLI 只直接依赖这两个包。

## 1. 最终边界

```text
apps ───────> agent-core <────── agent-extension
  └───────────────────────────────────────┘
```

- `agent-core`：稳定契约、Agent/Harness、AgentLoop、Run/Session、Context/Memory/Skill 编排、事件与脚本运行时；
- `agent-extension`：OpenAI Compatible、内置 Tools、Sandbox adapter、SQLite/Filesystem Store、Tracing/Langfuse/数据库 Dump；
- Core 永远不能依赖 Extension；应用是唯一 composition root。

目录名使用 `core/extension`；Cargo 包使用 `agent-core/agent-extension`，避免与 Rust 内置 `core` crate 冲突。

## 2. 当前模块入口

```text
agent_core::harness
agent_core::context
agent_core::memory
agent_core::skill
agent_core::tool
agent_core::observability
agent_core::sandbox
agent_core::script

agent_extension::provider
agent_extension::tool
agent_extension::sandbox
agent_extension::store
agent_extension::observability
```

Extension 使用 Feature 控制外部依赖：`openai`、`builtin-tools`、`host-sandbox`、`sqlite`、`filesystem`、`observability-tracing`。Server 使用 `full`，CLI 不启用数据库和 Filesystem Skill Store。

## 3. QuickJS 归属

QuickJS 放在 `agent-core::script`，因为脚本是 AgentMachine/Flow 的可选执行方式，不是某个外部供应商 Adapter。当前已经定义：

- `ScriptRuntime`；
- `ScriptExecutionRequest/Output`；
- `ScriptLimits`；
- `ScriptErrorKind`；
- Runtime descriptor。

后续通过 Core 的可选 `quickjs` Feature 引入 `rquickjs`。默认构建不链接 QuickJS。Host capability 仍然只通过 Core contract 注入，脚本不能直接连接数据库、事件 Broker、文件系统或网络。

## 4. 已完成的物理合并

源码已按以下顺序完成移动，每个 Slice 均保持序列化格式、SQLite migration 和公开 JSON Schema 不变：

1. 将 `tool-contract`、`observability-contract`、`process-sandbox` 契约移入 Core 对应模块；
2. 将 `mina-harness` 的 Agent/Run/Session/Event 移入 `core::harness`；
3. 将 Context/Memory/Skill contract 与 runtime 移入 Core；
4. 将 Provider、Tools、Store、Sandbox 和 Observability adapter 移入 Extension；
5. 更新内部 import，删除兼容 re-export；
6. 从 Workspace 删除旧 package，并用依赖检查阻止 Core 反向依赖 Extension。

Core 不依赖 Extension；依赖数据库实现的 ContextEngine 组合测试位于 Extension 集成测试中，从测试层同样避免反向依赖。旧 package 已从 workspace 和文件树移除。
