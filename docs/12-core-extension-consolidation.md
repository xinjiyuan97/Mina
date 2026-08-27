# Core / Harness / Extension 三包边界

> 状态：物理拆分已完成。Workspace 保留三个产品级内部库，不继续拆成 `agent-script`、`agent-event`、`agent-job` 等细粒度 crate。

## 1. 目标依赖方向

```text
agent-core <── agent-harness
     ▲              ▲
     └──── agent-extension
                    ▲
                    │
                  apps
```

允许的依赖：

- `agent-harness -> agent-core`；
- `agent-extension -> agent-core`；
- 只在需要宿主类型的 feature 中允许 `agent-extension -> agent-harness`；
- `apps -> core + harness + extension`。

禁止的依赖：

- `agent-core -> agent-harness`；
- `agent-core -> agent-extension`；
- `agent-harness -> agent-extension`（测试依赖除外）；
- Core 直接依赖 `rquickjs`、数据库、HTTP SDK 或监控厂商 SDK。

## 2. `agent-core`

Core 定义“Agent 是什么、如何决策”，只包含 provider-neutral 的契约和纯 Agent 逻辑：

- `Agent`、`AgentLoop`、`SingleTurnAgent`；
- Model、Tool、Message、Approval、RunEvent 契约；
- `AgentMachine`、checkpoint、wait/effect、Store port 等耐久执行契约；
- Session、Context、Memory、Skill 契约及纯策略；
- ScriptRuntime 契约、ADF DTO/Policy/Store/Executor port；
- Sandbox 与 Observability port。

Core 不拥有进程 worker、数据库连接、QuickJS VM 或 HTTP Server。默认构建不链接 `rquickjs`。

## 3. `agent-harness`

Harness 定义“宿主如何执行、挂起、恢复 Agent”：

- `Harness`、`RunOptions`、运行超时和公共 Run stream；
- `RunRuntime`、活跃 Run 管理、异步聚合持久化；
- checkpoint activation worker、跨重启恢复；
- Event/Subscription/Timer coordinator；
- Job coordinator、Flow effect outbox coordinator；
- TOML composition config；
- ADF definition lock 与 JavaScript executor；
- `JavaScriptAgentMachine`；
- 可选的 `QuickJsRuntime`。

QuickJS 是 Harness 的可选 feature：

```toml
agent-harness = { path = "crates/harness", features = ["quickjs"] }
```

仅使用 Rust Agent 的程序可以只依赖 `agent-core`，不会链接 QuickJS。需要便携 JavaScript、ADF 或 JavaScript Flow 时才启用 Harness 的 `quickjs` feature。

## 4. `agent-extension`

Extension 只提供外部系统和宿主资源的具体 adapter：

- OpenAI-compatible Provider；
- SQLite Run/Session/Memory/Event/Job/Flow Store；
- filesystem Skill/ADF artifact Store；
- read/write/edit/search/terminal/async_job 等内置 Tool；
- HostProcessSandbox 与未来强 Sandbox；
- tracing、Langfuse、OTLP exporter adapter。

Extension 可以同时实现多个 Core/Harness port，但不能把具体数据库或厂商类型泄漏到稳定契约中。

## 5. 当前物理目录

```text
crates/
├── core/
│   └── src/
│       ├── harness/          # Agent/Model/Tool/Run/Flow contracts + AgentLoop
│       ├── context/
│       ├── memory/
│       ├── skill/
│       ├── script/           # ScriptRuntime contract only
│       ├── adf/              # ADF contracts and policy
│       ├── sandbox.rs
│       └── observability.rs
├── harness/
│   └── src/
│       ├── runtime.rs        # Harness facade
│       ├── run_runtime.rs    # durable Run coordinator
│       ├── event_runtime.rs
│       ├── job_runtime.rs
│       ├── flow_runtime.rs
│       ├── config.rs
│       ├── script/           # QuickJS + JavaScript Flow
│       └── adf/              # lock + JavaScript executor
└── extension/
    └── src/
        ├── provider/
        ├── tool/
        ├── sandbox.rs
        ├── store/
        └── observability.rs
```

`agent-core::harness` 目前仍是历史兼容命名空间，里面只保留 Agent/Run/Flow 契约与 AgentLoop，不再包含宿主 `Harness`、QuickJS、配置加载器或 Event/Job/Flow coordinator。

## 6. Feature 与体积规则

- `agent-core` 没有 QuickJS feature，也没有 `rquickjs` 依赖；
- `agent-harness` 默认不启用 QuickJS；
- Server/CLI 显式启用 `agent-harness/quickjs`；
- `agent-extension` 的 Provider、SQLite、Tool、Sandbox、Observability 继续独立 feature-gate；
- 新 adapter 优先放入 Extension 的目录模块，不为单个实现新增 crate。

## 7. 验收规则

每次边界调整至少验证：

```bash
cargo fmt --all -- --check
cargo test --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
pnpm --dir apps/web check
```

额外使用 `cargo tree -p agent-core` 检查 Core 不出现 `agent-harness`、`agent-extension` 或 `rquickjs`。
