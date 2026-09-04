# Mina

Mina 是一个用于持续迭代 Agent Harness 的 Rust + Next.js monorepo。当前采用模块化单体：`agent-core` 保存稳定契约与 Agent 决策逻辑，`agent-harness` 负责宿主运行、耐久编排和可选 QuickJS，`agent-extension` 保存外部实现。

## 目录

```text
mina/
├── apps/
│   ├── web/                  # Next.js 前端
│   ├── server/               # Rust/Axum 后端
│   └── agent-cli/            # 可直接运行的 Agent CLI
├── crates/
│   ├── core/                 # agent-core：AgentLoop、稳定契约、Context/Memory/Skill
│   ├── harness/              # agent-harness：Run runtime、Event/Job/Flow、QuickJS/ADF
│   └── extension/            # agent-extension：Provider/Tool/Sandbox/Store/Tracing/ADF adapters
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

Run、Session、上下文 artifact、Memory、Flow checkpoint/inbox/outbox、Event/Subscription/Timer 和 Job 默认持久化到 `data/runs.sqlite3`。可通过 `MINA_RUN_STORE_PATH` 指定其他 SQLite 文件。页面断开不会结束后台 run；重新连接可使用 `GET /api/v1/runs/{run_id}/events?after_seq=<seq>` 续传，状态可通过 `GET /api/v1/runs/{run_id}` 查询。Server 重启后，已经提交 checkpoint 的 `WaitingEvent` run 会恢复；崩溃时仍在模型或同步 Tool 内执行、没有安全 checkpoint 的 activation 才会得到明确的 `run_interrupted` 终态。

## 模型配置

Harness 使用强类型 TOML 配置。复制示例并通过环境变量告诉后端配置位置：

```bash
cp config/mina.example.toml config/mina.toml
# 编辑 config/mina.toml，把 api_key 替换为真实值
MINA_CONFIG=config/mina.toml pnpm dev
```

`api_key` 直接保存在本地 TOML 配置中。`config/mina.toml` 已被 Git 忽略，配置对象的调试输出也会隐藏密钥。仍可选用 `{ env = "ENV_NAME" }` 引用环境变量。配置加载会校验默认模型、URL、模态、上下文窗口等字段，并自动把 `base_url` 规范化为以 `/` 结尾。

服务端只提供原生 reducer 驱动的 `agent-loop`：

```toml
[agent]
kind = "agent-loop" # 模型可以在一个 run 内调用工具并继续执行
system_prompt = "You are Mina, a helpful agent."
max_steps = 8
run_timeout_seconds = 900
model_timeout_seconds = 600
tool_timeout_seconds = 30
tool_call_strategy = "parallel-safe" # sequential | parallel-safe

[approval]
review_level = 50 # Low=10, Medium=50, High=90；0=全部审核，100=关闭审核

[script.quickjs]
enabled = true
timeout_ms = 2000
memory_bytes = 33554432
max_concurrent_executions = 2

[adf]
enabled = true
max_active_per_run = 8

[jobs]
enabled = true
allowed_kinds = ["builtin.delay"]
```

Tool 风险由 Tool 作者声明，Host 使用 `[approval].review_level` 统一过滤：仅当 `tool_level >= review_level` 时进入用户审批。默认值 `50` 保持 Medium/High 需要审批；设为 `90` 时只审批 High，设为 `0` 时所有 Tool 都审批，设为 `100` 时关闭 Tool 人工审批。配置只接受 `0..=100`。外部 Tool manifest 仍使用 `low/medium/high`，因此不会破坏现有 JSON Schema 或 C ABI。

启用后，Server 和 CLI 都会注册 `javascript_eval`，并提供 `adf_define`、`adf_list`、`adf_remove`。当前 ADF 只允许 Run-scoped、同步、无 Host capability 的 JavaScript；定义完成后，内容寻址的动态 Tool 从下一个 model step 开始可见。脚本源码和输入在公共 SSE、SQLite RunEvent 和普通观测中只记录大小与 digest。

Harness 还提供 `JavaScriptAgentMachine`：脚本用 `start(context,input)` / `resume(context,checkpoint,events)` 返回受校验的 OutputDelta、Event wait、Timer、Job 或 PublishEvent。每次 activation 都创建全新 QuickJS Context，只把 JSON checkpoint 与 content-addressed artifact ref 写入 Store，不保存 Promise、闭包、全局变量或 VM 栈。

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

`agent.max_steps` 是 Host 上限；`POST /api/v1/runs` 和 `POST /api/v1/sessions/{session_id}/runs` 可为单次 Run 传入更小的 `max_steps`。预算耗尽时，未执行的 tool call 会以结构化 `agent_step_limit_exceeded` 工具错误返回模型，并额外执行一次禁用工具的最终总结，而不是直接终止 Run。

`kind` 目前只接受 `agent-loop`；旧的 `echo` 与 `single-turn` 服务端兼容入口已经移除。需要不调用真实模型的前端测试时，应使用实现 Responses 或 Messages 协议的 mock provider。

## 当前调用链

应用层使用三个内部入口：`agent-core` 提供契约与 Agent 决策逻辑，`agent-harness` 提供 Harness、耐久运行时和可选脚本引擎，`agent-extension` 按 Feature 提供 Provider、Tools、Sandbox、Store 和可观测性实现。依赖始终从 Apps/Extension/Harness 指向 Core，Core 不反向依赖宿主或外部实现。

```text
Next.js -> Session submit -> SkillOrchestrator -> ContextEngine
                                      │                ├-> SessionStore
                                      └-> SkillStore    ├-> MemoryRetriever
                                                       └-> ContextCompressor
                                                -> agent-harness::Harness
                                              -> AgentLoopReducer
                                                   -> Native Effect Runner
                                                        ├-> RunToolSession(revision)
                                                        │    ├-> ToolRegistry -> Extension Tools
                                                        │    └-> Run ADF overlay -> QuickJS
                                                        └-> ModelPort -> Extension Provider
                                             <-> AgentLoopReducerState checkpoint
                                                   <-> Harness Event/Timer/Job Runtime -> Extension SQLite
```

服务端直接组合 `AgentLoop` 的原生 `AgentMachine` 实现。所有决策由 `AgentLoopReducer` 产生，Native Effect Runner 只负责模型、工具、计时与取消等异步效果。前端停止按钮会调用 `POST /api/v1/runs/{run_id}/cancel`，取消信号贯穿 run、审批、模型和工具执行。

`agent-extension::tool` 当前提供 `get_current_time`、`read`、`list_directory`、`search`、`write`、`edit`、`apply_patch`、`shell_command`、`exec_command`、`write_stdin`、`javascript_eval` 和 `async_job`；ADF overlay 另外提供 `adf_define/list/remove` 及动态生成的工具。`read/list/search/javascript_eval` 默认 Low，`write/edit/apply_patch/async_job` 默认 Medium，terminal process tools 始终 High。文件工具限制在 workspace 内并拒绝密钥配置、`.env`、私钥和 `.git` 路径；`search` 通过可替换的 `SearchBackend` 工作，默认 adapter 是不联网的 workspace text search。

Tool 还声明 `idempotency/concurrency/completion/retry`。只有 ReadOnly/Idempotent Tool 可有限自动重试；只有 `parallel_safe + immediate` 且无需审批的调用可并发，写回模型时仍保持调用顺序；MaySuspend Tool 通过 checkpoint + wait/effect 释放 worker，事件到达后跨重启恢复。

AgentLoop 每个 model step 固定使用一份 `ToolSetSnapshot(revision, digest)`，工具调用也携带同一 revision；ADF 修改只影响下一 step。当前 `InMemoryAdfArtifactStore` 适用于单次 Run，Run 结束会清理动态 overlay，尚不承诺跨 Server 重启恢复 ADF。durable artifact/checkpoint 属于下一阶段。

`shell_command/exec_command/write_stdin` 通过 `agent-core::sandbox::ProcessSandbox` 契约执行，当前 `agent-extension::sandbox` 提供 `HostProcessSandbox`：固定 cwd、清空继承环境、管理会话并限制输出，但它会如实报告 `isolation=none`，不是内核级安全边界。生产环境应替换为 bubblewrap/nsjail、OCI/gVisor 或 microVM adapter；详细选型见 [进程沙箱与工具后端](./docs/10-process-sandbox-and-tool-backends.md)。

Tool failure 统一包含稳定 `code`、`category`、安全 `message`、`retryable` 和可选 `retry_after_ms`；失败会进入 RunEvent/RunSnapshot 并作为结构化 tool result 返回模型，不会因为普通工具失败直接终止整个 Agent。`agent-core::observability` 定义脱敏事件和 Hook，`agent-extension::observability` 提供 Model/Tool/AgentMachine 装饰器、有界异步 batch/retry/flush queue、tracing adapter 及通用 exporter port。Host 可以接 OpenTelemetry/Langfuse，而 Harness 不依赖具体监控厂商，监控故障也不会阻塞 Agent。

外部工具通过 [`contracts/tool/v1`](./contracts/tool/v1/README.md) 接入。公共边界采用版本化 C ABI，manifest、调用和结果使用 JSON Schema；Rust 内部 trait、Tokio future 和 allocator 不跨语言暴露。

前端聊天界面使用 npm 发布的 `@xinjiyuan97/chat-core` 和 `@xinjiyuan97/chat-ui`。工作台通过 `GET /api/v1/sessions?status=active` 从 SQLite 恢复会话列表和历史消息，支持新建、切换并记住最近使用的 Session；自定义 transport 通过 `POST /api/v1/sessions/{session_id}/runs` 提交消息，再订阅 Run SSE。工具审批映射为组件库的 `permission-request` / `permission-resolved`。

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
- [Core/Harness/Extension 三包边界](./docs/12-core-extension-consolidation.md)
- [QuickJS 便携脚本运行时设计与执行计划](./docs/13-quickjs-portable-script-runtime.md)
- [Agent Defined Functions（ADF）设计与执行计划](./docs/14-agent-defined-functions.md)
