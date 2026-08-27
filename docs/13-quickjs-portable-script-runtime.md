# QuickJS 便携脚本运行时设计与执行计划

> 状态：便携运行时、`javascript_eval`、Run-scoped ADF 和可恢复 Flow Machine 均已落地。本文定义 QuickJS 在 Mina 中的产品定位、执行契约、安全边界和后续 hardening。QuickJS 的首要用途仍是为 Agent 提供随 Rust 二进制分发的便携计算工具。

## 0. 当前实现状态（2026-08-26）

已经实现：

- `agent-core::script` 的 provider-neutral 契约，包含 language、purpose、module ref、artifact source、capability、limits、validation、usage、cancellation 和稳定错误；
- Core 默认关闭的 `quickjs` feature，未启用时不链接 `rquickjs`；
- `QuickJsRuntime` 的真实 ES module 编译与 `main(input, context)` 执行、JSON 输入输出和 digest lock；
- `spawn_blocking` 执行隔离、有界并发、source/memory/stack/output 限制，以及 timeout/cancel interrupt；
- 每次调用创建独立 Runtime/Context，基础版本不暴露文件、网络、进程、环境变量或其他 Host capability；
- 正常执行、语法/入口校验、无限循环超时、非法 JSON 结果和 digest mismatch 的测试。
- `agent-extension::tool::JavaScriptEvalTool`，包含 validate/execute、稳定 ToolError 映射和结构化 usage；
- `[script.quickjs]` TOML 配置以及 Server composition，运行时 descriptor 可从 Agent inspection 查看；
- `javascript_eval` 的源码和输入在公共 RunEvent、SSE、SQLite 投影及普通观测中使用 digest-only 表示；
- QuickJS 作为 Run-scoped JavaScript ADF 的共享执行器，并通过完整 AgentLoop 集成测试。
- `JavaScriptAgentMachine` 的 `start(context,input)` / `resume(context,checkpoint,events)` ABI；
- 每次 activation 创建全新 QuickJS Context，不保存 VM、Promise、closure 或 global state；
- content-addressed `ResolvedArtifact`、JSON checkpoint、Host 生成稳定 ID；
- 声明式 OutputDelta、Event wait、Timer、Job 和 PublishEvent，并校验 allow-list 与 Run ownership；
- Timer 与 Job 的 SQLite reopen/resume 集成测试，以及“全局状态不跨 activation”测试。

尚未实现：

- console/log 配额、完整 `mina.events/timers/jobs/tools` Host API 和共享模块 loader；
- 独立 script worker 强隔离与完整 hardening；
- 调试 UI 对 QuickJS budgets/usage 的专门可视化（inspection API 已提供 descriptor）。

因此，当前 Agent 已可直接调用 `javascript_eval`、把同一运行时用于 ADF，或由 Host 选择 QuickJS Flow Machine。Flow 的外部动作来自受校验的声明式返回值，不是脚本直接持有数据库/broker client。QuickJS 仍保持默认无文件/网络/进程能力、进程内隔离的安全定位。

## 1. 结论

QuickJS 不定位为 Python、Shell 或 Node.js 的完整替代品，而定位为：

> Mina Portable Compute Runtime：无 Node/npm 依赖、JSON 输入输出、能力受限、资源有界的 JavaScript 执行环境。

它解决的是客户机器未安装 Python/Node 时，Agent 仍需要可靠执行小型计算脚本的问题。第一版只做纯计算，不默认提供文件、网络、进程和环境变量能力。

推荐的三个消费入口按顺序交付：

```text
javascript_eval                         # 临时执行一次
       │
       ├── Agent Defined Function       # 固化为 Run-scoped Tool
       │
       └── Flow start/resume handler    # 事件到达后重新执行模块
```

三者共享 QuickJS 引擎、资源限制、错误分类和模块校验，但使用不同的入口契约和 capability profile。不能把临时 eval、Tool 调用和 durable Flow 状态混为一个 API。

## 2. 目标与非目标

### 2.1 目标

- Rust 二进制启用 feature 后自带 JS 引擎，最终用户不需要安装 Node.js、npm 或系统级 QuickJS；
- 让模型用短 JS 完成 JSON、文本、规则和小规模数据计算，减少 token 内人工计算；
- 所有执行都有源码、内存、时间、栈、日志、输出和 Host API 次数上限；
- 默认无文件、网络、进程、环境变量和凭据访问；
- 同一 `ScriptRuntime` 可以支撑临时 eval、ADF JavaScript Tool 和后续 Flow handler；
- 输入、输出、错误和观测数据保持 provider-neutral；
- 未启用 `quickjs` feature 时，Core 默认构建不链接 `rquickjs`。

### 2.2 非目标

- 不实现 Node.js 兼容层；
- 不解析 npm package，不允许任意动态模块加载；
- 不替代 pandas、numpy、图像处理、浏览器自动化等 Python 生态；
- 不用 QuickJS Promise、VM 栈或闭包承载跨重启等待；
- 不把进程内 QuickJS Context 宣称为恶意多租户代码的最终隔离边界；
- 不允许模型通过脚本声明提高 Host 设定的资源和 capability 上限。

## 3. 适用场景

适合 QuickJS 的任务：

- JSON filter/map/reduce、排序、分组、去重与聚合；
- 文本解析、正则提取、格式化和批量替换；
- CSV 等轻量结构化数据转换；
- 业务规则、校验规则和确定性后处理；
- 数学、日期、BigInt 和集合计算；
- 将供应商或 Tool 输出映射为另一份 JSON Schema；
- 对模型生成结果做程序化校验；
- 小型 ADF Tool；
- Flow resume 时根据 checkpoint 和事件决定下一状态。

不适合 QuickJS 的任务：

- 大文件和大规模数据处理；
- 需要 Python/Node 第三方库的任务；
- 文件系统、网络、浏览器、编译器和系统命令操作；
- 分钟级任务或需要独立重试、跨重启继续执行的任务；
- 来自不受信任租户、要求强进程隔离的代码。

这些任务应继续走 `ProcessSandbox` 或 Flow Runtime 的异步 Job。

## 4. 所在架构边界

当前 Workspace 使用 `core + harness + extension` 三个产品级内部库，不为 QuickJS 单独增加第四个 crate：

```text
apps/server + apps/agent-cli
              │ composition/config
              ▼
agent-core::script
  ├── ScriptRuntime contract
  ├── Script artifact/module DTO
  ├── capability/limits/error contract
  │
  ▼
agent-harness::script
  ├── QuickJsRuntime               # optional `quickjs` feature
  └── JavaScriptAgentMachine
              ▲
              │ Arc<dyn ScriptRuntime>
agent-extension::tool
  ├── javascript_eval Tool
  └── ADF Tool adapter
```

QuickJS 放在 `agent-harness::script`，因为它是宿主执行能力，而不是 Agent 决策契约或外部供应商 adapter。`agent-core::script` 只保留 `ScriptRuntime` 等稳定契约。`javascript_eval` 是具体内置 Tool，放在 `agent-extension::tool`，由应用 composition root 注入 `Arc<dyn ScriptRuntime>`。

数据库、事件 Broker、文件、网络和进程能力不能成为 QuickJS 的隐式依赖；需要时必须通过 Core capability port 显式注入，并由 Host policy 授权。

## 5. 运行时契约

当前 `ScriptExecutionRequest { source, input, limits }` 是可用起点，但不足以支持 artifact、ADF、取消和 replay。建议演进为以下 provider-neutral DTO：

```rust
pub enum ScriptLanguage {
    JavaScript,
}

pub enum ScriptPurpose {
    Eval,
    Tool,
    FlowStart,
    FlowResume,
}

pub struct ScriptModuleRef {
    pub module_id: String,
    pub revision: u64,
    pub digest: String,
}

pub enum ScriptSource {
    Inline {
        source: String,
        expected_digest: Option<String>,
    },
    Artifact(ScriptModuleRef),
}

pub struct ScriptExecutionRequest {
    pub execution_id: String,
    pub run_id: Option<RunId>,
    pub language: ScriptLanguage,
    pub purpose: ScriptPurpose,
    pub source: ScriptSource,
    pub export: String,
    pub input: Value,
    pub granted_capabilities: Vec<ScriptCapability>,
    pub limits: ScriptLimits,
    pub cancellation: RunCancellation,
}
```

结果保持 JSON 边界：

```rust
pub struct ScriptExecutionOutput {
    pub value: Value,
    pub logs: Vec<ScriptLog>,
    pub usage: ScriptUsage,
    pub module_digest: String,
}

pub struct ScriptUsage {
    pub duration_ms: u64,
    pub peak_memory_bytes: Option<u64>,
    pub host_calls: u32,
    pub output_bytes: u64,
}
```

`ScriptRuntime` 继续是窄端口：

```rust
pub trait ScriptRuntime: Send + Sync + 'static {
    fn descriptor(&self) -> ScriptRuntimeDescriptor;
    fn validate(&self, request: ScriptValidationRequest)
        -> ScriptValidationFuture;
    fn execute(&self, request: ScriptExecutionRequest)
        -> ScriptExecutionFuture;
}
```

`validate` 只检查语法、模块形状、入口点和静态限制；它不是恶意代码检测器。是否允许执行由 Host policy 决定。

## 6. JavaScript 模块协议

### 6.1 临时计算与 ADF Tool

第一版统一使用 `main(input, context)`：

```javascript
export function main(input, context) {
  const active = input.orders.filter(order => order.status !== "cancelled");
  return {
    count: active.length,
    total: active.reduce((sum, order) => sum + order.amount, 0),
  };
}
```

约束：

- `input` 必须是 JSON value；
- 返回值必须可序列化成 JSON；
- 第一版 `context` 只包含只读 execution metadata 和受控 console；
- 模块顶层不能产生持久副作用；
- 不恢复上次调用的全局变量；
- 返回 Promise 可以作为兼容能力支持，但第一版没有异步 Host API，Promise 必须在本次 execution lease 内完成。

### 6.2 Flow handler

Flow Machine 使用两个独立入口：

```javascript
export async function start(context, input) {
  // 返回 Continue/Suspend/Complete/Failed
}

export async function resume(context, checkpoint, events) {
  // 使用持久化 checkpoint 与 inbox events 决定下一状态
}
```

每次 `start`/`resume` 都创建新的 Context。Runtime 只保存 module ref、checkpoint 和声明式 wait，不保存 Promise、closure 或 VM 栈。

## 7. `javascript_eval` Tool

第一阶段对模型公开一个低能力面 Tool：

```json
{
  "name": "javascript_eval",
  "description": "Run a bounded JavaScript function over JSON input.",
  "input_schema": {
    "type": "object",
    "properties": {
      "source": { "type": "string" },
      "input": {},
      "export": { "type": "string", "default": "main" }
    },
    "required": ["source", "input"],
    "additionalProperties": false
  }
}
```

调用链：

```text
AgentLoop
  → ToolRegistry.validate
  → JavaScriptEvalTool
  → ScriptRuntime.validate
  → QuickJsRuntime.execute
  → ToolOutput(JSON envelope)
```

`javascript_eval` 不写 ScriptArtifactStore；源码只属于当前 tool call。RunEvent/Observation 记录 digest、大小、资源使用和稳定错误，默认不记录完整源码与输入内容。

现有 Tool 事件会流式投影完整 arguments，不能直接沿用于脚本源码。Tool binding 需要增加 Host-owned data visibility policy：

```rust
pub enum ToolArgumentVisibility {
    Full,
    Redacted,
    DigestOnly,
}
```

`javascript_eval` 默认使用 `DigestOnly`：AgentLoop 内部仍保留 source/input 供执行和模型消息使用，对公共 RunEvent、SSE、普通日志和观测只暴露 source/input bytes、digest 和安全摘要。该策略由 Host Tool binding 声明，模型不能通过参数修改。调试环境如需查看源码，也必须走单独的受控 artifact/debug API，不能依赖公共事件泄漏。

第一版风险建议为 Low，但必须满足：无 Host capability、严格资源限制、无 module loader、无文件/网络/进程。Host 可以按部署策略提升为 Medium。QuickJS 进程内逃逸风险必须在 runtime descriptor 中明确，不能标记为强隔离。

## 8. Capability 模型

第一版 capability 集为空，只保留受控 console：

```text
JSON / Object / Array / Map / Set
RegExp / BigInt
TextEncoder / TextDecoder（如实现）
bounded console
```

默认禁止：

```text
fetch / WebSocket
process / require
filesystem / environment
child process
npm / arbitrary import
native module
raw database/event broker access
```

后续能力必须使用可枚举枚举，不能使用任意字符串权限：

```rust
pub enum ScriptCapability {
    Console,
    DeterministicClock,
    SeededRandom,
    ToolInvoke { allow: Vec<String> },
    EventCommand { allow_topics: Vec<String> },
    JobCommand { allow_kinds: Vec<String> },
    RunControl,
}
```

有效能力为：

```text
module requested
  ∩ profile grants
  ∩ run grants
  ∩ purpose hard ceiling
  = execution capabilities
```

脚本参数和 manifest 不能扩大 Host grants。

## 9. 资源限制与线程模型

建议默认值：

```toml
[script.quickjs]
enabled = true
eval_enabled = true
max_source_bytes = 262144
timeout_ms = 2000
memory_bytes = 33554432
max_stack_bytes = 1048576
max_output_bytes = 1048576
max_log_lines = 100
max_log_bytes = 65536
max_host_calls = 32
worker_threads = 2
max_queue_depth = 64
```

Agent、ADF manifest 和 JS 源码不能提高这些值，只能请求更小预算。

QuickJS Runtime/Context 不能在任意 Tokio worker 线程之间移动。实现使用固定专用线程或有界 worker pool：

```text
Tokio caller
  → bounded request channel
  → acquire execution lease
  → QuickJS worker thread
  → create Runtime/Context
  → install memory/stack/interrupt limits
  → load in-memory module
  → invoke export
  → drain bounded job queue
  → serialize result
  → destroy Context
```

取消和 deadline 通过 QuickJS interrupt handler 检查共享原子状态。队列已满时返回稳定的 `script_runtime_busy`，不能无界堆积。

## 10. Determinism、Artifact 与 replay

临时 `javascript_eval` 不保证业务结果可重放，但必须记录源码 digest 和 runtime version。

ADF 与 Flow handler 必须引用 content-addressed artifact：

```text
sha256(normalized source + manifest + module ABI version)
```

checkpoint 和 execution manifest 只保存 `module_id + revision + digest`，不保存整个源码。恢复时：

- digest 存在且 runtime/ABI 兼容：允许执行；
- artifact 缺失：`script_artifact_missing`；
- digest 不一致：`script_digest_mismatch`；
- runtime policy 不兼容：显式迁移或失败，不能静默使用新版本。

为提高可测试性，Flow/ADF 默认不直接暴露系统时钟和随机数。需要时使用 Host 注入的逻辑时间和 seeded random，并将 seed/time 写入 activation input。

## 11. 错误协议

建议稳定错误：

| code | category | 含义 |
|---|---|---|
| `script_invalid_source` | invalid_request | 语法或模块形状非法 |
| `script_export_not_found` | not_found | 入口函数不存在 |
| `script_invalid_result` | invalid_request | 返回值不能编码为 JSON/StepOutcome |
| `script_capability_denied` | permission_denied | 请求了未授权能力 |
| `script_timeout` | timeout | 超过 execution deadline |
| `script_cancelled` | cancelled | Run 或 Host 取消 |
| `script_memory_exhausted` | resource_exhausted | Heap/stack 超限 |
| `script_output_too_large` | resource_exhausted | 日志或输出超限 |
| `script_runtime_busy` | unavailable | worker pool/queue 饱和 |
| `script_runtime_failed` | internal | 引擎内部失败 |

进入 ToolPort 时映射为现有 `ToolError`；作为 AgentMachine handler 时映射为 `MachineError`。原始 Rust/QuickJS 栈、宿主路径和敏感值不能直接返回模型或公共 RunEvent。

## 12. Observability 与审计

每次执行至少记录：

- execution/run/tool/activation ID；
- purpose、module ID、revision、digest；
- runtime identity、engine version、ABI version；
- granted capability names；
- queue wait、duration、peak memory、host call、output/log bytes；
- completion/error code；
- 是否命中 timeout/cancel/resource limit。

默认不记录完整源码、input、output 和 console。需要内容采样时必须由显式 observability policy 开启并做脱敏。

## 13. 测试策略

### 13.1 Contract tests

- JSON input/output round-trip；
- 非 JSON 值、循环引用、BigInt 直接返回等非法结果；
- module export 缺失；
- capability 交集和越权；
- digest、ABI 和 runtime descriptor 稳定性。

### 13.2 Runtime tests

- 无限循环被 interrupt deadline 终止；
- heap/stack、source、output、console 和 queue 上限；
- cancellation；
- Context 之间无全局状态泄漏；
- 并发 execution 不跨线程误用 Runtime/Context；
- 异常不泄漏宿主路径或内部栈。

### 13.3 Integration tests

- AgentLoop 调用 `javascript_eval` 并消费结构化结果；
- Tool 失败被送回模型而不直接终止 Run；
- ADF artifact 使用相同源码得到相同 digest；
- Flow resume 使用全新 Context 仍能从 checkpoint 完成；
- 未启用 feature 时 Server 明确不注册 Tool，而不是启动时崩溃。

## 14. 执行计划

### Q0：固定契约与 feature

- 将 `script.rs` 拆为 `script/contract.rs` 与 `script/runtime.rs`；
- 增加 language、purpose、module ref、export、capability、usage 和 cancellation；
- Harness 增加默认关闭的 `quickjs` feature 和可选 `rquickjs` 依赖；
- 增加共享 contract fixtures。

验收：Core 不链接 QuickJS；启用 Harness feature 后公共 DTO 可序列化，旧调用点完成迁移。

### Q1：有界 QuickJS Runtime

- 专用 worker/pool；
- Runtime/Context 生命周期；
- 内存、栈、interrupt deadline、cancel、输出和日志限制；
- 白名单 in-memory module loader；
- 稳定错误映射和 runtime descriptor。

验收：无限循环、内存超限和取消都能在 deadline 内结束；1000 次隔离执行无状态泄漏。

### Q2：`javascript_eval` Tool

- Extension Tool adapter；
- TOML 配置与 Server composition；
- Tool schema、风险、RunEvent 和 Observation；
- Tool argument visibility/redaction，公共事件默认只输出 digest/大小；
- 调试页展示 QuickJS runtime descriptor 与 budgets。

验收：未安装 Node/Python 的环境可以完成 JSON 聚合、文本转换和 BigInt 测试；非法脚本产生结构化 ToolError。

### Q3：ADF JavaScript Runtime

- 接入 ADF artifact/module ref；
- `main(input, context)` Tool ABI；
- Run ToolSet revision 与 digest lock；
- ADF capability profile。

依赖：ADF A0 Core contract 与 A1 RunToolSession 已完成。

验收：同一 Run 定义 JS Tool 后，下一模型 step 能看到并调用；恢复/重放使用锁定 digest。

### Q4：Flow binding（主链已完成）

- `start/resume` ABI；
- 声明式 events/timers/jobs/publish effect binding；完整 `mina.*` Host API 待扩展；
- Rust/JS 共用 Command handler fixtures；
- StepOutcome 校验。

依赖：AgentMachine、checkpoint、Event Runtime、Inbox/Outbox 已完成。

验收：Server 在 `WaitingEvent` 时重启，事件到达后用全新 QuickJS Context 调用 `resume` 并完成原 Run。

### Q5：Hardening

- fuzz module/result decoder；
- 长时间 soak、并发和资源泄漏测试；
- runtime/version compatibility policy；
- 多租户部署切换为独立 script worker 的 adapter；
- 可选的内置审计 ES module。

## 15. 关键决策

1. QuickJS 的第一价值是便携计算，不以 Event Runtime 为前置条件；
2. 第一交付入口是 `javascript_eval`，随后复用于 ADF，最后复用于 Flow；
3. 默认纯计算，无文件、网络、进程、环境变量和 npm；
4. QuickJS 位于 Harness 可选 feature，Core 只保留脚本契约，具体内置 Tool 位于 Extension；
5. 每次执行创建隔离 Context，不保存 Promise、closure 或 VM 栈；
6. 长任务转为 Job，不让 Script Future 跨重启存活；
7. ADF/Flow 使用 content-addressed artifact 与 digest lock；
8. 进程内 QuickJS 不是恶意多租户的最终安全边界。
