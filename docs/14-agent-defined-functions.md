# Agent Defined Functions（ADF）设计与执行计划

> 状态：Run-scoped JavaScript ADF MVP 已落地；通用 durable Tool suspension、Job Runtime 和 QuickJS Flow 已就绪，ADF durable artifact/promotion 与 Python/Shell adapter 仍待实施。ADF 使用现有 Tool 执行策略、风险审批、RunEvent 和 Sandbox 边界；异步 ADF 应复用现有 Job/wait/inbox，不让 Tool Future 跨重启存活。

## 0. 当前实现状态（2026-08-26）

已经实现：

- `agent-core::adf` 的 definition、locked artifact/ref、runtime/scope/execution mode、owner/capability、Store/Policy/Executor ports 和稳定错误；
- `RunScopedJavaScriptPolicy`：第一阶段只允许 Run-scoped、同步、无 Host capability 的 JavaScript，并校验名称、入口、源码大小和 JSON Schema；
- Host 注入 runtime descriptor 后的 definition lock、canonical name、source digest 和 manifest digest；
- `JavaScriptAdfExecutor`：将锁定 ADF 映射到 `ScriptRuntime`，执行前后分别校验 input/output Schema，并稳定映射脚本错误；
- `agent-extension::adf::InMemoryAdfArtifactStore`：digest 校验、幂等 replay/conflict、get/list/deactivate；
- Core ADF policy、Harness definition lock/QuickJS executor 和 Extension in-memory store 的基础测试。
- `ToolSetSnapshot`/binding/revision/digest 契约，以及 AgentLoop 每个 model step 重新获取 snapshot、整轮固定 revision；
- `RunAdfToolSession` 的 Run 隔离、历史 revision binding、并发 mutation 串行化和结束清理；
- 模型可见的 `adf_define/list/remove`，定义完成后从下一 model step 暴露动态 Tool；
- `allow_run_adf` 与静态 allow-list 分离，动态内容寻址名称不能绕过 ADF grant；
- `tool_set_updated` RunEvent 和 RunSnapshot 投影；`adf_define` 源码参数使用 digest-only 公共事件；
- Server 默认组合 QuickJS、`javascript_eval` 和 Run-scoped ADF，并有“定义 → 下一 step 发现 → 调用”的 AgentLoop 集成测试。
- Tool definition 已携带幂等/并发/完成/retry policy；JavaScript ADF 声明为 `read_only + parallel_safe + immediate`；
- 普通 Tool 已能返回 durable suspension，`async_job` 已用 `StartJob + job.completed/job.failed` 打通 SQLite reopen/resume；
- QuickJS Flow 已能声明式提交 Job/Timer 并在全新 Context 中 resume。

尚未实现：

- SQLite/filesystem durable ADF artifact store、ADF revision 重启恢复、Python/Shell adapter、ADF JobSpec/promotion。
- ADF 专用管理/调试 UI（公共事件和 Run snapshot 已提供 revision/digest/动态名称）；
- capability 非空及高风险 ADF 的审批 profile（当前 policy 只允许无 capability 的低风险 JavaScript）。

因此，当前模型已经可以完成“定义 → 下一 step 发现 → 调用 → 列表/移除”的单 Run 生命周期，而且不会修改全局 `ToolRegistry`。下一阶段的边界是 durable artifact/checkpoint 与事件唤醒，不应把进程内 overlay 扩大成跨重启承诺。

## 1. 定义与结论

ADF（Agent Defined Function）表示：

> Agent 根据当前任务生成一份带名称、说明、JSON Schema、脚本和 capability 请求的 Tool definition，经 Host 校验与授权后，作为当前 Run 的动态 Tool 提供给后续模型 step。

它不是让模型任意修改全局 `ToolRegistry`，也不是把一段 shell 字符串伪装成低风险 Tool。ADF 必须经过以下生命周期：

```text
Requested
   → Validated
   → PolicyEvaluated
   → ArtifactStored
   → Activated(run tool overlay)
   → Invoked through ToolPort
   → Deactivated/Expired
```

第一版只支持 Run-scoped、同步 ADF。之后再增加异步 Job 和受审批的 Session/Workspace promotion。

## 2. 目标与非目标

### 2.1 目标

- Agent 可以在任务中定义新的、provider-neutral Tool；
- 新 Tool 在下一次模型调用中以标准 `ToolDefinition` 出现；
- 动态 Tool 继续使用现有 Schema 校验、风险审批、ToolError 和 RunEvent；
- JavaScript 在嵌入式 QuickJS 中运行，不要求客户安装 Python/Node；
- Python/Shell 只能通过 `ProcessSandbox` adapter 执行；
- Tool source、manifest、runtime、capability 和策略结果用 digest 锁定；
- 同一 Run 的 ToolSet 具有稳定 revision，支持 checkpoint/resume；
- ADF Store、Runtime 和 Policy 都由 Core port 抽象，可替换具体实现。

### 2.2 非目标

- 第一版不安装 npm/pip package；
- 第一版不允许 ADF 修改全局 Tool catalog；
- 不根据模型声明直接信任 `risk_level`；
- 不把源码扫描当作安全边界；
- 不让同步 Tool Future 负责分钟级任务或跨重启恢复；
- 不允许 ADF 绕过 Run 的 effective tool/capability policy；
- 不支持同名 Tool 静默覆盖 Host built-in 或 Skill Tool。

## 3. 所在架构边界

ADF 横跨 Tool、Script/Sandbox 和 Orchestration，但每层只拥有自己的语义：

```text
Orchestrator / PolicyEngine
  │ 计算 Run 初始 grants 和 ADF 创建权限
  ▼
ADF Runtime（Core）
  │ validate → lock → activate → snapshot
  ├───────────────┐
  ▼               ▼
AdfArtifactStore  RunToolSession / ToolSetRevision
  │               │
  ▼               ▼
ScriptRuntime     AgentLoop 每个 model step 获取 snapshot
  ├─ QuickJS
  └─ ProcessSandbox adapter
```

职责划分：

| 层 | 负责 | 不负责 |
|---|---|---|
| `agent-core::adf` | 稳定 DTO、artifact/store/executor port、policy 输入输出 | VM、artifact adapter、ToolSet overlay 生命周期 |
| `agent-core::tool` | ToolDefinition、调用和错误协议 | ADF 生命周期和源码存储 |
| `agent-core::script` | 脚本执行契约 | QuickJS VM、全局 Tool 注册和持久化 |
| `agent-harness::adf` | definition lock、JavaScript executor | 数据库和 Provider |
| `agent-harness::script` | QuickJS 可选实现、JavaScript Flow Machine | Tool adapter 和数据库 |
| `agent-extension::adf` | artifact store、RunToolSession/ToolSet overlay、未来 Python/Shell executor | AgentLoop 策略 |
| `agent-extension::tool` | `adf_define/list/remove` Tool adapter | 绕过 ADF command handler |
| Apps | TOML、profile、composition、管理 API/调试 UI | ToolSet 一致性和风险语义 |

## 4. ADF Manifest 与锁定结果

Agent 提交的是请求，不是可信执行定义：

```rust
pub struct AdfDefinitionRequest {
    pub requested_name: String,
    pub description: String,
    pub input_schema: Value,
    pub output_schema: Option<Value>,
    pub runtime: AdfRuntimeRequest,
    pub source: String,
    pub entrypoint: String,
    pub requested_capabilities: Vec<AdfCapability>,
    pub requested_scope: AdfScope,
    pub execution_mode: AdfExecutionMode,
    pub idempotency_key: String,
}

pub enum AdfRuntimeRequest {
    JavaScript,
    Python { interpreter_profile: Option<String> },
    Shell { shell_profile: Option<String> },
}

pub enum AdfScope {
    Run,
    Session,
    Workspace,
}

pub enum AdfExecutionMode {
    Sync,
    JobCapable,
}
```

Host 校验和授权后生成不可变锁定结果：

```rust
pub struct LockedAdfDefinition {
    pub adf_id: AdfId,
    pub revision: u64,
    pub canonical_name: String,
    pub description: String,
    pub input_schema: Value,
    pub output_schema: Option<Value>,
    pub runtime: AdfRuntimeDescriptor,
    pub source_ref: AdfArtifactRef,
    pub source_digest: String,
    pub manifest_digest: String,
    pub effective_risk: ToolRiskLevel,
    pub granted_capabilities: Vec<AdfCapability>,
    pub scope: AdfScope,
    pub execution_mode: AdfExecutionMode,
    pub created_by_run_id: RunId,
    pub created_at_ms: i64,
}
```

模型提供的名称只作为 slug 输入。Canonical name 由 Host 产生，并满足上游 function name 限制：

```text
adf_<slug>_<short_digest>
```

逻辑身份使用 `adf_id + revision`，不能只依赖名称。

## 5. 定义校验

`adf_define` 至少执行以下检查：

1. name、description、entrypoint 和 source 大小限制；
2. input/output JSON Schema 可以编译且处于支持的 draft；
3. canonical name 不与 built-in、Skill 或其他 active ADF 冲突；
4. runtime 已在 Host profile 中启用；
5. source 可以通过对应 runtime 的语法/module validation；
6. requested scope、capability 和 execution mode 由 PolicyEngine 授权；
7. Host 根据 runtime/capability 计算 effective risk；
8. normalized manifest/source 生成 digest；
9. artifact 写入 Store 后才能激活；
10. idempotency key 的重复请求必须返回同一个锁定定义，内容冲突则报错。

静态分析可以拒绝明显不支持的 import/API，但不能证明脚本安全。最终安全边界仍是 QuickJS capability/limit 或 ProcessSandbox。

## 6. 动态 ToolSet

### 6.1 原始限制与当前结果

原始 `ToolRegistry` 只在 Server composition 时创建，无法让后续 model step 看见 ADF。当前已经用 `ToolSetSnapshot(revision,digest)` 与 `RunAdfToolSession` 解决：AgentLoop 每个 model step 获取一次 snapshot，并将该 revision 固定到整轮 validation/dispatch。

ADF 需要增加 Run 级动态 overlay，而不是修改全局 Registry：

```rust
pub struct ToolSetSnapshot {
    pub revision: u64,
    pub digest: String,
    pub definitions: Vec<ToolDefinition>,
    pub bindings: Vec<ToolBindingRef>,
}

pub trait RunToolSession: Send + Sync {
    fn snapshot(&self) -> ToolSetFuture<ToolSetSnapshot>;
    fn activate(&self, definition: LockedAdfDefinition)
        -> ToolSetFuture<ToolSetSnapshot>;
    fn deactivate(&self, adf_id: AdfId)
        -> ToolSetFuture<ToolSetSnapshot>;
    fn invoke(&self, request: ToolCallRequest, revision: u64)
        -> ToolCallFuture;
}
```

有效 ToolSet：

```text
Host built-in tools
  ∩ Skill/ExecutionPlan requested tools
  ∩ Host policy grants
  + Policy-approved Run ADF overlay
  = ToolSetSnapshot(revision, digest)
```

`AgentLoop` 每个模型 step 开始前获取一次 snapshot，整轮 model/tool call 固定使用同一个 revision。`adf_define` 完成后 overlay revision 增加，下一模型 step 才能看到新 Tool。

执行时必须携带或内部绑定 `tool_set_revision`：

- revision 仍可解析且 binding/digest 匹配：执行；
- Tool 已移除或 revision 不匹配：`adf_tool_revision_mismatch`；
- 不能按当前同名 Tool 静默执行。

### 6.2 与 `allowed_tools` 的关系

ADF 不能通过动态名称绕过初始 allow-list。ExecutionPlan 应把授权拆为：

```rust
pub struct ToolGrants {
    pub static_tools: Vec<String>,
    pub allow_run_adf: bool,
    pub allowed_adf_runtimes: Vec<AdfRuntimeKind>,
    pub allowed_adf_capabilities: Vec<AdfCapability>,
    pub max_active_adf: u32,
}
```

`adf_define` 只有在 `allow_run_adf=true` 时才能产生 `ActivateRunTool`。不能通过把 `allowed_tools` 改为 `None` 实现动态工具。

## 7. 模型可见的内置 Tools

### 7.1 `adf_define`

请求示例：

```json
{
  "requested_name": "calculate_order_totals",
  "description": "Calculate totals for active orders grouped by user.",
  "input_schema": {
    "type": "object",
    "properties": {
      "orders": { "type": "array" }
    },
    "required": ["orders"],
    "additionalProperties": false
  },
  "runtime": { "type": "javascript" },
  "entrypoint": "main",
  "source": "export function main(input) { /* ... */ }",
  "requested_capabilities": [],
  "requested_scope": "run",
  "execution_mode": "sync",
  "idempotency_key": "run-id:calculate-order-totals:v1"
}
```

安全返回：

```json
{
  "ok": true,
  "adf_id": "...",
  "revision": 1,
  "canonical_name": "adf_calculate_order_totals_a13f92c1",
  "source_digest": "sha256:...",
  "effective_risk": "low",
  "granted_capabilities": [],
  "scope": "run",
  "tool_set_revision": 3,
  "instruction": "The tool is available starting with the next model step."
}
```

### 7.2 其他管理 Tool

第一版：

```text
adf_define
adf_list
adf_remove
```

后续：

```text
adf_promote       # Run → Session/Workspace，需要显式审批
adf_update        # 创建新 revision，不原地覆盖
adf_inspect       # 返回 manifest/digest/能力，不默认返回完整源码
```

所有模型 Tool、HTTP 管理 API 和未来 JS binding 必须调用同一个 `AdfCommandHandler`，避免权限和幂等语义漂移。

ADF source 通过 function-call arguments 进入 AgentLoop，但不能原样进入公共 SSE 和普通运行日志。`adf_define` binding 默认使用 `DigestOnly` argument visibility：内部执行路径拿到完整 source，RunEvent/UI 只展示 language、entrypoint、source bytes、digest、requested/effective capability 和风险。完整 source 只存入受 Store policy 管理的 artifact；未来的源码查看 API 必须单独授权。

## 8. 执行 Runtime

所有 ADF 最终适配为普通 `Tool`：

```rust
pub struct AdfTool {
    pub definition: LockedAdfDefinition,
    pub executor: Arc<dyn AdfExecutor>,
}
```

调用链：

```text
AgentLoop ToolCall
  → RunToolSession(revision).validate
  → existing approval policy
  → AdfTool.call
  → executor
       ├─ JavaScript → ScriptRuntime/QuickJS
       ├─ Python     → ProcessSandbox adapter
       └─ Shell      → ProcessSandbox adapter
  → output schema/size validation
  → ToolOutput or ToolError
```

### 8.1 JavaScript

- 使用 [QuickJS 便携脚本运行时](./13-quickjs-portable-script-runtime.md)；
- 第一版只允许 JSON input/output 和空 capability；
- 不要求 Node/npm；
- 默认 Run scope；
- 每次调用使用新 Context。

### 8.2 Python/Shell

- 必须通过 `ProcessSandbox`；
- interpreter/shell 只能来自 Host allow-list profile；
- source 写入 sandbox 临时工作区或通过 stdin/module wrapper 传入；
- stdin 为 JSON request envelope，stdout 为唯一 JSON result envelope；
- stderr 只进入有界内部日志，不直接作为 ToolOutput；
- 默认断网、最小文件挂载、清空环境、无 API Key；
- Host adapter 的 `isolation=none` 只能用于本地开发，生产启用 ADF Python/Shell 时必须 fail closed 到强隔离 adapter。

Python/Shell 不能宣称“客户无依赖”：解释器必须随 sandbox image/worker 提供。

## 9. 风险与 Capability 策略

模型不能设置最终风险。建议基线：

| Runtime/能力 | 最低风险 |
|---|---|
| 纯 QuickJS、无 Host capability、Run scope | Low，可由 Host 提升 |
| QuickJS 调用只读 Tool | Medium |
| QuickJS 写文件、网络、Job/Run control | High |
| Python | High |
| Shell | High |
| Session/Workspace promotion | High |

`adf_define` 本身的风险与被定义 Tool 的调用风险分开：

- Host 可以允许自动创建纯 Run-scoped QuickJS ADF；
- 创建 Python/Shell ADF 或请求高风险 capability 时，定义阶段就需要审批；
- 每次调用是否再次审批由 effective Tool risk 和 approval grant policy 决定；
- 一次定义审批不能自动变成永久 Workspace grant。

Capability 是具体枚举和 allow-list，不允许任意字符串：

```text
tool.invoke:[read, search]
filesystem.read:[workspace/data/**]
filesystem.write:[workspace/output/**]
network.egress:[api.example.com:443]
jobs.start:[report-worker]
events.publish:[agent.*]
```

第一版 ADF 不开放上述 Host capability；先固定纯 QuickJS 与完全隔离的 ProcessSandbox 输入输出。

## 10. Scope、持久化与生命周期

### 10.1 Run scope

- 默认且第一版唯一支持；
- artifact 可以持久化用于审计/replay，但 active binding 只属于 `run_id`；
- Run 终态后自动 deactivated；
- checkpoint 保存 active ADF refs 和 ToolSet revision/digest；
- 迟到调用返回终态/失效错误，不能重新激活 Run。

### 10.2 Session scope

- 需要 promotion command 和用户审批；
- 后续 Run 通过 Session ToolLock 引用；
- 更新创建新 revision，旧 Run 继续使用旧 digest；
- Session archive 后默认不可再激活。

### 10.3 Workspace scope

- 相当于安装本地扩展；
- 需要管理员/Host policy 授权；
- 必须支持 list、disable、revoke、audit 和版本 pin；
- Python/Shell 还需要锁定 sandbox image/profile digest。

## 11. Artifact Store 契约

Core 定义：

```rust
pub trait AdfArtifactStore: Send + Sync {
    fn put(&self, request: PutAdfArtifact)
        -> AdfFuture<AdfArtifactRef>;
    fn get(&self, reference: AdfArtifactRef)
        -> AdfFuture<Option<AdfArtifact>>;
    fn list(&self, query: AdfArtifactQuery)
        -> AdfFuture<Vec<AdfArtifactDescriptor>>;
    fn deactivate(&self, command: DeactivateAdfArtifact)
        -> AdfFuture<()>;
    fn descriptor(&self) -> ComponentDescriptor;
}
```

Extension 第一版提供 SQLite metadata + filesystem/content blob，或全部存入 SQLite。无论实现如何，必须满足：

- content-addressed digest；
- `(owner_scope, idempotency_key)` 唯一；
- artifact immutable；
- revision 单调；
- descriptor 不泄漏绝对路径和连接串；
- contract fixtures 可同时验证 in-memory fake 与 SQLite adapter。

## 12. 与 Run、Checkpoint 和 Manifest 的关系

Execution manifest 增加：

```json
{
  "tool_set": {
    "revision": 3,
    "digest": "sha256:...",
    "adf": [
      {
        "adf_id": "...",
        "revision": 1,
        "canonical_name": "adf_calculate_order_totals_a13f92c1",
        "manifest_digest": "sha256:...",
        "runtime": "quickjs",
        "runtime_version": "...",
        "effective_risk": "low",
        "capabilities": []
      }
    ]
  }
}
```

checkpoint 至少保存：

- active `adf_id/revision/digest` refs；
- current ToolSet revision/digest；
- 已向模型暴露 Tool definition 的 step/revision；
- ADF invocation 与 ToolCall correlation；
- 累计 invocation/Job/resource budgets。

不把 API Key、绝对路径、进程 ID、临时文件路径或完整 sandbox handle 放入 checkpoint。

## 13. 同步与异步 ADF

短 ADF 继续使用现有 `ToolPort::call`：

```text
model → ADF Tool → executor → ToolOutput → next model step
```

预计较长、需要 webhook、独立重试或跨重启的 ADF 必须转为 Job：

```text
Agent → jobs.start(tool_ref, arguments, idempotency_key)
      → job_id
      → create once wait(job.completed|job.failed, correlation=job_id)
      → suspend or continue

Job worker → resolve locked ADF artifact
           → execute in sandbox/runtime
           → publish trusted job.completed/job.failed

Flow Runtime → inbox + wake
AgentMachine.resume → 将 Job result 作为 Tool result 继续模型循环
```

不能用魔法 JSON 字段让普通 `ToolOutput` 隐式改变 Run 状态。`StartJob`、`Subscribe` 和 `Suspend` 是显式 `EffectRequest/StepOutcome`，并通过 transactional outbox 原子提交。

第一版 ADF `execution_mode=sync`；Job 能力依赖 AgentMachine 和通用 Event Runtime 完成后再启用。

## 14. RunEvent、DomainEvent 与观测

建议增加面向消费者的非终态 RunEvent：

```text
adf_definition_requested
adf_defined
adf_definition_failed
adf_activated
adf_deactivated
```

普通调用仍使用已有事件：

```text
tool_call_started
tool_execution_started
tool_execution_completed
tool_execution_failed
approval_requested/resolved
```

Job 完成属于可信 Workflow DomainEvent，不允许 Agent 直接伪造：

```text
job.started
job.completed
job.failed
```

观测至少记录：

- adf/tool/run/call ID；
- revision、manifest/source digest；
- runtime/sandbox descriptor；
- effective risk 与 capability names；
- validate/queue/execute duration 和资源使用；
- approval、completion/error code；
- ToolSet before/after revision。

默认不记录完整源码、arguments 和 output。

## 15. 错误协议

建议稳定错误：

| code | category | 含义 |
|---|---|---|
| `adf_invalid_definition` | invalid_request | manifest/name/entrypoint 非法 |
| `adf_invalid_schema` | invalid_request | input/output Schema 非法 |
| `adf_invalid_source` | invalid_request | 语法或模块 ABI 非法 |
| `adf_name_conflict` | conflict | canonical/logical name 冲突 |
| `adf_runtime_not_allowed` | permission_denied | Runtime 未授权 |
| `adf_capability_denied` | permission_denied | Capability 未授权 |
| `adf_scope_denied` | permission_denied | Scope/promotion 未授权 |
| `adf_limit_exceeded` | resource_exhausted | active 数量、源码或预算超限 |
| `adf_artifact_missing` | not_found | 锁定 artifact 不存在 |
| `adf_digest_mismatch` | conflict | Artifact 内容与锁定 digest 不同 |
| `adf_tool_revision_mismatch` | conflict | ToolSet revision/binding 不一致 |
| `adf_runtime_unavailable` | unavailable | QuickJS/Sandbox/解释器不可用 |
| `adf_execution_failed` | internal | 运行时安全失败 |
| `adf_output_invalid` | invalid_request | 输出不满足 JSON/output Schema |

运行时的 timeout/cancel/resource error 继续使用 Script/Sandbox 具体稳定 code，并映射成 `ToolError`。

## 16. 配置建议

```toml
[adf]
enabled = true
allow_run_scope = true
allow_session_scope = false
allow_workspace_scope = false
max_active_per_run = 8
max_definitions_per_run = 16
max_source_bytes = 262144
max_invocations_per_run = 64
allowed_runtimes = ["javascript"]

[adf.javascript]
runtime = "quickjs"
default_risk = "low"

[adf.python]
enabled = false
sandbox_profile = "python-worker-v1"

[adf.shell]
enabled = false
sandbox_profile = "shell-worker-v1"
```

Run 上层可以请求更小的 ADF 数量/调用预算，不能超过 Host 上限。实际配置和 component descriptors 进入 execution manifest。

## 17. 测试策略

### 17.1 Contract tests

- manifest、lock、artifact ref 序列化；
- schema、name、digest 和 idempotency；
- capability/risk 交集；
- Store in-memory/SQLite 共享 fixtures。

### 17.2 ToolSet tests

- ADF 在定义它的 model step 中不可提前出现，下一 step 可见；
- revision 单调且相同 idempotent command 不重复增加；
- 同名冲突不覆盖 built-in；
- 旧 revision 不能调用新 binding；
- deactivation 后新 snapshot 不包含 Tool，旧 checkpoint 显式失败或按锁恢复。

### 17.3 Runtime tests

- JavaScript input/output Schema；
- Python/Shell 只能使用配置 sandbox profile；
- timeout、cancel、output limit、非零 exit 和非法 JSON；
- Host adapter 在生产 profile 下 fail closed；
- ADF 不能访问未授予 Tool/文件/网络能力。

### 17.4 Recovery tests

- Server 重启后锁定 artifact/digest 仍可解析；
- WaitingEvent resume 后使用相同 ToolSet digest；
- 重复 Job completion 不重复产生 Tool result；
- promotion/update 不改变历史 Run 的 revision。

## 18. 执行计划

### A0：固定 ADF Core 契约

- 新增 `core::adf`；
- manifest、locked definition、artifact ref、scope、capability、runtime DTO；
- `AdfArtifactStore`、`AdfPolicy`、`AdfExecutor` port；
- 稳定错误和 contract fixtures；
- TOML Host policy DTO。

验收：Core 不依赖 SQLite、文件系统或 Extension；所有 DTO 可版本化序列化。

### A1：RunToolSession 与 ToolSet revision

- built-in/Skill Tool snapshot；
- Run-scoped overlay；
- AgentLoop 每个 model step 获取 snapshot；
- binding 与 revision/digest 校验；
- execution manifest 和 RunSnapshot 投影。

验收：fake ADF 在 step N 定义后只从 step N+1 出现；并发 Run 的 overlay 完全隔离。

### A2：Run-scoped JavaScript ADF MVP

- `adf_define/list/remove`；
- in-memory artifact store；
- QuickJS `main(input, context)` executor；
- 空 capability profile；
- ADF source/input argument redaction 与 digest-only 公共事件；
- definition/activation RunEvent 和 tracing；
- 调试页显示 active ADF、digest、runtime、risk。

依赖：[QuickJS 执行计划 Q0–Q2](./13-quickjs-portable-script-runtime.md#14-执行计划)。

验收：Agent 自己定义 JSON 聚合 Tool，下一 step 调用成功；客户环境无需 Node/Python。

### A3：Durable artifact 与恢复

- SQLite/filesystem adapter；
- content-addressed source；
- idempotent define；
- checkpoint ToolSet refs；
- 启动恢复和 digest/runtime compatibility 校验。

验收：Server 重启后 Run 使用相同 ADF revision/digest；缺失和冲突显式失败。

### A4：Python/Shell Sandbox adapter

- JSON stdin/stdout ABI；
- interpreter/shell profile；
- 强隔离 descriptor 和 fail-closed policy；
- resource/output/cancel mapping；
- 高风险审批。

验收：未配置强 sandbox 时生产 profile 不注册 Python/Shell ADF；配置 worker 后执行不继承 Host secrets。

### A5：异步 ADF Job

- `JobSpec` 引用 locked ADF；
- StartJob + subscription + suspend transactional outbox；
- Job worker、retry、cancel、dead letter；
- completion 写 Run inbox 并 resume AgentMachine。

依赖：AgentMachine、Event Runtime 和 durable wait 已完成。

验收：长 ADF 执行期间 Server 重启，Job completion 最终只产生一次逻辑 Tool result 并恢复原 Run。

### A6：Promotion 与管理面

- Run → Session/Workspace promotion；
- approval/admin policy；
- revision update、disable、revoke、audit；
- 管理 API 和调试 UI；
- retention/garbage collection。

验收：历史 Run 始终锁定旧 revision；撤销阻止新 Run 激活但不篡改审计记录。

## 19. 联合交付顺序

QuickJS 与 ADF 的推荐组合：

```text
Q0 Script contract
  → Q1 QuickJS Runtime
  → Q2 javascript_eval
  → A0 ADF contract
  → A1 RunToolSession
  → Q3/A2 JavaScript ADF MVP
  → A3 durable artifact

AgentMachine/Event Runtime
  → Q4 Flow binding
  → A5 async ADF Job

强 ProcessSandbox
  → A4 Python/Shell ADF

最后：A6 Session/Workspace promotion
      Q5 QuickJS hardening 持续执行
```

`javascript_eval` 可以最早独立提供价值；ADF 在其上增加可发现、可复用、可锁定的 Tool 生命周期；异步事件最后增加跨 activation 的等待和恢复。三条能力共用底层契约，但可以按上述顺序独立验收。

## 20. 关键决策

1. ADF 是动态 Tool，不是全局 Registry 的任意修改接口；
2. 第一版只支持 Run-scoped 同步 JavaScript ADF；
3. AgentLoop 每个 model step 使用固定 ToolSet snapshot/revision；
4. ADF 不能绕过 ExecutionPlan 的 ToolGrants；
5. 模型只能请求 capability/risk，最终值由 Host policy 计算；
6. JavaScript 使用 QuickJS，Python/Shell 使用 ProcessSandbox；
7. 长 ADF 转为 Job，不保存 Tool Future；
8. Artifact immutable、content-addressed，并进入 checkpoint/execution manifest；
9. Session/Workspace promotion 是受审批安装行为，不是默认能力；
10. 模型 Tool、HTTP API 和 JS binding 共用同一个 ADF command handler。
