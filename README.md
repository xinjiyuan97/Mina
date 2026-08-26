# Mina

Mina 是一个用于持续迭代 Agent Harness 的 Rust + Next.js monorepo。当前采用模块化单体：前端和后端是接入层，`agent-core` 保存稳定契约与纯运行逻辑，`agent-extension` 保存所有外部实现。

## 目录

```text
mina/
├── apps/
│   ├── web/                  # Next.js 前端
│   ├── server/               # Rust/Axum 后端
│   └── agent-cli/            # 可直接运行的 Agent CLI
├── crates/
│   ├── core/                 # agent-core：Harness、契约、Context/Memory/Skill
│   └── extension/            # agent-extension：Provider/Tool/Sandbox/Store/Tracing
├── contracts/tool/v1/       # C ABI 头文件与 JSON Schema
└── docs/                     # 架构与协议设计
```

## 本地运行

要求：Node.js 22+、pnpm 10+、Rust 1.92+。

```bash
pnpm install
pnpm dev
```

默认地址：

- Web：<http://localhost:3000>
- Server：<http://127.0.0.1:8787>
- 健康检查：<http://127.0.0.1:8787/healthz>

也可以分别执行 `pnpm dev:web` 和 `pnpm dev:server`。Web 开发服务器会把 `/api/*` 和 `/healthz` 转发给 Rust 后端，因此浏览器端不需要单独配置 CORS。

## 常用命令

```bash
pnpm check       # TypeScript、ESLint 和 Rust 检查
pnpm test        # Rust workspace 测试
pnpm build       # 构建 Web 和 Rust workspace
```

如需修改后端地址，在 `apps/web` 下复制 `.env.local.example` 为 `.env.local`。后端监听地址可通过 `MINA_SERVER_ADDR` 设置。

Run、Session、上下文 artifact 和 Memory 默认持久化到 `data/runs.sqlite3`。可通过 `MINA_RUN_STORE_PATH` 指定其他 SQLite 文件。页面断开不会结束后台 run；重新连接可使用 `GET /api/v1/runs/{run_id}/events?after_seq=<seq>` 续传，状态可通过 `GET /api/v1/runs/{run_id}` 查询。Server 重启后，旧进程遗留的非终态 run 会得到明确的 `run_interrupted` 终态，关联 Session 会自动释放 busy 状态。

## 模型配置

Harness 使用强类型 TOML 配置。复制示例并通过环境变量告诉后端配置位置：

```bash
cp config/mina.example.toml config/mina.toml
# 编辑 config/mina.toml，把 api_key 替换为真实值
MINA_CONFIG=config/mina.toml pnpm dev
```

`api_key` 直接保存在本地 TOML 配置中。`config/mina.toml` 已被 Git 忽略，配置对象的调试输出也会隐藏密钥。仍可选用 `{ env = "ENV_NAME" }` 引用环境变量。配置加载会校验默认模型、URL、模态、上下文窗口等字段，并自动把 `base_url` 规范化为以 `/` 结尾。

Agent 模式由同一份配置选择：

```toml
[agent]
kind = "agent-loop" # 模型可以在一个 run 内调用工具并继续执行
system_prompt = "You are Mina, a helpful agent."
max_steps = 8
run_timeout_seconds = 300
model_timeout_seconds = 120
tool_timeout_seconds = 30
```

上层编排同样由 TOML 选择具体实现和策略。默认组合是 filesystem SkillStore、SQLite FTS MemoryRetriever、Host MemoryWritePolicy、Hybrid Compressor 和启发式 TokenEstimator。替换这些实现不需要修改 AgentLoop。模型返回完整且自洽的 usage 时以模型数据为准；缺失或不可比较的字段才由 estimator 根据实际请求和流式回复内容补齐，Run 的 `usage.source` 会标记为 `provider_reported`、`mixed` 或 `estimator_fallback`。

```toml
[orchestration]
skill_directory = "skills"
memory_enabled = true
memory_scope = "global"
compressor = "hybrid"
context_policy_version = 1
```

`skill_directory` 中每个一级子目录代表一个 Skill package，至少包含 `skill.toml` 和 `SKILL.md`。仓库内置的 [`rust-project-guide`](./skills/rust-project-guide) 示例声明为 `routable`，输入包含 Rust、Cargo、crate、workspace、编译、测试或依赖时会自动加载。调试工作台的 Skills 页签会显示 Store、版本、digest、激活方式和路由 hints；最终选中的 Skill refs 同时写入 Run execution manifest。

开发前端但不希望调用模型时使用 `kind = "echo"`；只允许一次模型调用时使用 `kind = "single-turn"`。要启用带工具循环的真实 Agent，将本地 `config/mina.toml` 的 `api_key` 换成真实值，并使用 `kind = "agent-loop"` 后重启后端。

## 当前调用链

应用层只依赖两个内部入口：`agent-core` 提供契约、Harness 与纯运行逻辑，`agent-extension` 按 Feature 提供 Provider、Tools、Sandbox、Store 和可观测性实现。细粒度 crate 已完成物理合并，不再参与 workspace 构建。

```text
Next.js -> Session submit -> SkillOrchestrator -> ContextEngine
                                      │                ├-> SessionStore
                                      └-> SkillStore    ├-> MemoryRetriever
                                                       └-> ContextCompressor
                                                -> Harness
                                              -> EchoAgent
                                              -> SingleTurnAgent -> ModelPort -> Extension Provider
                                              -> AgentLoop -> ApprovalPort
                                                           -> ToolRegistry -> Extension Tools
                                                           -> ModelPort -> Extension Provider
```

`EchoAgent` 用于本地 UI 开发；`SingleTurnAgent` 只调用模型一次；`AgentLoop` 可以消费流式 tool call、校验 JSON Schema，并在执行中高风险工具前等待用户审批。拒绝结果会返回模型且不会执行工具。三者使用同一个 HTTP API，切换时不需要修改前端。前端停止按钮会调用 `POST /api/v1/runs/{run_id}/cancel`，取消信号贯穿 run、审批、模型和工具执行。

`agent-extension::tool` 当前提供 `get_current_time`、`read`、`list_directory`、`search`、`write`、`edit` 和 `run_command`。`read/list/search` 默认 Low，`write/edit` 默认 Medium，`run_command` 始终 High。文件工具限制在 workspace 内并拒绝密钥配置、`.env`、私钥和 `.git` 路径；`search` 通过可替换的 `SearchBackend` 工作，默认 adapter 是不联网的 workspace text search。

`run_command` 通过 `agent-core::sandbox::ProcessSandbox` 契约执行，当前 `agent-extension::sandbox` 提供 `HostProcessSandbox`：无 shell、固定 cwd、清空继承环境并限制输出，但它会如实报告 `isolation=none`，不是内核级安全边界。生产环境应替换为 bubblewrap/nsjail、OCI/gVisor 或 microVM adapter；详细选型见 [进程沙箱与工具后端](./docs/10-process-sandbox-and-tool-backends.md)。

Tool failure 统一包含稳定 `code`、`category`、安全 `message`、`retryable` 和可选 `retry_after_ms`；失败会进入 RunEvent/RunSnapshot 并作为结构化 tool result 返回模型，不会因为普通工具失败直接终止整个 Agent。`agent-core::observability` 定义同步、非阻塞 Hook，`agent-extension::observability` 提供 ModelPort/ToolPort 装饰器和结构化 tracing adapter，Host 可以继续接 OpenTelemetry/Langfuse，而 Harness 不依赖具体监控厂商。

外部工具通过 [`contracts/tool/v1`](./contracts/tool/v1/README.md) 接入。公共边界采用版本化 C ABI，manifest、调用和结果使用 JSON Schema；Rust 内部 trait、Tokio future 和 allocator 不跨语言暴露。

前端聊天界面使用 npm 发布的 `@xinjiyuan97/chat-core` 和 `@xinjiyuan97/chat-ui`。自定义 transport 会创建或恢复 Session，通过 `POST /api/v1/sessions/{session_id}/runs` 提交消息，再订阅 Run SSE；工具审批映射为组件库的 `permission-request` / `permission-resolved`。

详细设计见：

- [架构与边界划分](./docs/01-architecture-boundaries.md)
- [Gateway–Agent 交互协议](./docs/02-gateway-agent-protocol.md)
- [Gateway–上游协议](./docs/03-gateway-upstream-protocol.md)
- [单次任务运行时](./docs/04-single-task-runtime.md)
- [Agent Loop 与工具协议](./docs/05-agent-loop-tools.md)
- [独立事件运行时与订阅系统](./docs/06-event-runtime-subscriptions.md)
- [工具包与跨语言静态链接契约](./docs/07-tool-packages-and-ffi.md)
- [单次 Run 状态与持久化](./docs/08-single-run-state-storage.md)
- [下一阶段：Session、上下文编排与可恢复流程框架](./docs/09-next-flow-framework.md)
- [进程沙箱与工具后端](./docs/10-process-sandbox-and-tool-backends.md)
- [单 Agent 生产化边界](./docs/11-single-agent-production-hardening.md)
- [Core/Extension 两包收敛](./docs/12-core-extension-consolidation.md)
