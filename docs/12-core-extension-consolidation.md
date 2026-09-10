# Core / Harness / Extension 三包边界

> 状态：物理拆分已完成。Workspace 保留三个产品级内部库，不继续拆成 `agent-script`、`agent-event`、`agent-job` 等细粒度 crate。

## 1. 目标依赖方向

```text
agent-core <── agent-extension
     ▲              ▲
     └── agent-harness
              ▲
              │
            apps
```

允许的依赖：

- `agent-harness -> agent-core`；
- `agent-harness -> agent-extension`（装配 Provider 配置和扩展实现）；
- `agent-extension -> agent-core`；
- `apps -> core + harness + extension`。

禁止的依赖：

- `agent-core -> agent-harness`；
- `agent-core -> agent-extension`；
- `agent-extension -> agent-harness`，包括正式依赖和测试依赖；
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
- JavaScript ADF executor；
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
- Provider/Model 配置类型及密钥解析；
- SQLite Run/Session/Memory/Event/Job/Flow Store；
- filesystem Skill/ADF artifact Store；
- read/write/edit/search/terminal/async_job 等内置 Tool；
- Workspace FS 契约，以及 Native、OPFS、S3 adapter；
- HostProcessSandbox 与未来强 Sandbox；
- tracing、Langfuse、OTLP exporter adapter。

Extension 实现 Core port，并向 Harness 提供配置和可选 adapter。Extension 不得引用 Harness 类型；需要同时验证 Harness 与 Extension 的集成测试放在 Harness 下。

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
│       ├── adf/              # ADF contracts, policy and definition locking
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
│       └── adf/              # JavaScript executor
└── extension/
    └── src/
        ├── provider/
        ├── workspace/        # WorkspaceFs + Native / OPFS / S3
        ├── tool/
        ├── sandbox.rs
        ├── store/
        └── observability.rs
```

`agent-core::harness` 是 Agent/Run/Flow 契约与 AgentLoop 的领域命名空间，不包含宿主 `Harness`、QuickJS、配置加载器或 Event/Job/Flow coordinator。

## 6. Feature 与体积规则

- `agent-core` 没有 QuickJS feature，也没有 `rquickjs` 依赖；
- `agent-harness` 默认不启用 QuickJS；
- Server/CLI 显式启用 `agent-harness/quickjs`；
- `agent-extension` 的 Provider、SQLite、Tool、Sandbox、Observability 继续独立 feature-gate；
- `workspace-native` 供桌面和服务端本地目录使用，`workspace-opfs` 供浏览器使用，`workspace-s3` 供服务端对象存储使用；
- 新 adapter 优先放入 Extension 的目录模块，不为单个实现新增 crate。

## 7. 验收规则

每次边界调整至少验证：

```bash
cargo fmt --all -- --check
cargo test --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
pnpm --dir apps/web check
```

额外使用 `cargo tree -p agent-core` 检查 Core 不出现 `agent-harness`、`agent-extension` 或 `rquickjs`，并检查 `cargo tree -p agent-extension` 不出现 `agent-harness`。

## 8. 跨包边界的上线前 API 策略

当前项目尚未发布稳定 API；跨包边界重构不保留旧路径或旧构造方式的兼容层：

- 不跨 crate 转导出其他包的契约；调用方必须从契约的真实所有者导入类型；
- 不同时维护新旧构造器或旧命名别名；边界调整时直接迁移所有调用方；
- Core port 新增必需语义时直接增加必需方法，不用默认实现掩盖不完整的 adapter；
- Provider 协议自身要求的兼容能力不属于内部 API 兼容层，应限制在对应 Provider adapter 内；
- schema/ABI/checkpoint 版本检查用于拒绝错误输入，不代表承诺读取历史未发布格式。

因此依赖必须保持显式：Core 契约从 `agent_core` 导入，外部实现从 `agent_extension` 导入，宿主运行时和 QuickJS 从 `agent_harness` 导入。
