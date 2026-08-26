# Mina 独立事件运行时与订阅系统设计

> 状态：设计阶段。本文定义一套独立于 Chat UI、HTTP Gateway、模型供应商和具体 Agent 实现的事件底座。Agent 可以通过 Rust 内置能力或 JS 沙箱绑定发布事件、创建订阅、安排 Timer、启动后台任务并挂起 run。
>
> 本文描述事件子系统本身；结合已完成的单次 Run Store 后，项目整体实施顺序以 [下一阶段：Session、上下文编排与可恢复流程框架](./09-next-flow-framework.md) 为准，先完成 Session、Context/Memory/Compression 和 Skill 上层编排，再接 AgentMachine 与 durable wait。

## 1. 设计目标

事件系统需要统一支持：

- 用户完成审批后唤醒等待中的 Agent；
- 同步或异步工具执行完成后发送结果事件；
- 一次性 Timer、周期任务或 Cron 触发事件；
- 大型后台任务启动后释放当前 worker，完成时恢复原 run；
- 一个 run 挂起时，运行时继续执行其他 run；
- Rust Agent 和 JS Agent 使用相同的能力协议；
- 不启动 Chat UI 也能通过 CLI、桌面端、HTTP、worker 或测试使用；
- 进程重启后订阅、等待关系和未消费事件可以恢复。

非目标：

- 不把每个模型 token 都写入持久化事件总线；
- 不承诺端到端 exactly-once；
- 不保存任意 Rust Future、JS Promise 或 JS VM 调用栈；
- 不让 Agent 上传任意闭包作为持久化订阅过滤器。

## 2. 所在边界

事件系统不应直接塞进 `AgentLoop`，也不应属于 Gateway。它作为 `agent-core` 内的独立模块由 Harness 通过端口组合，持久化和定时器 adapter 放在 `agent-extension`：

```text
┌──────────────────────────────────────────────────────────────┐
│ Gateway / CLI / Desktop / Webhook / Worker                  │
└──────────────────────────────┬───────────────────────────────┘
                               │ Command / Event
┌──────────────────────────────▼───────────────────────────────┐
│ agent-core Event Runtime                                      │
│ EventLog · SubscriptionStore · Dispatcher · Timer · Outbox   │
└───────────────┬──────────────────────────────┬───────────────┘
                │ delivery                     │ Rust / JS API
┌───────────────▼────────────────┐  ┌──────────▼───────────────┐
│ agent-core RunCoordinator     │  │ Built-in capabilities    │
│ suspend · wake · resume       │  │ events/timers/tasks      │
└───────────────┬────────────────┘  └──────────────────────────┘
                │
┌───────────────▼──────────────────────────────────────────────┐
│ AgentMachine / AgentLoop / Rust Agent / JS Agent             │
└──────────────────────────────────────────────────────────────┘
```

建议的物理模块划分：

```text
crates/
├── core/src/event/          # 协议、dispatcher、run bridge 与状态机
├── core/src/script/         # 可选 QuickJS runtime contract/实现
└── extension/src/event/     # SQLite、timer worker 和外部 broker adapter
```

事件契约不依赖具体 Agent 实现。通用 worker、定时器甚至非 Agent 程序也可以使用同一端口；Harness 只把订阅投递映射为 run 的唤醒输入。

## 3. Command、Event、Effect 与 RunEvent

四种消息不能混为一类：

| 类型 | 含义 | 示例 |
|---|---|---|
| `Command` | 请求系统执行动作，可能失败 | `ResolveApproval`、`ScheduleTimer` |
| `DomainEvent` | 已经发生且不可修改的事实 | `approval.resolved`、`timer.fired` |
| `Effect` | Agent 请求 Harness 调用外部能力 | `Subscribe`、`StartJob`、`SuspendRun` |
| `RunEvent` | 面向 run 消费者的状态投影 | `run_waiting`、`run_resumed` |

用户点击“批准”首先产生 `ResolveApproval` Command。审批服务验证身份、状态和约束后才追加 `approval.resolved` Event。Agent 不能通过直接发布 `approval.resolved` 绕过审批策略。

现有 SSE `RunEvent` 继续服务 UI、CLI 和日志。事件运行时可以订阅并投影重要 RunEvent，但高频 `output_delta` 默认只走流式通道，不进入持久化总线。

## 4. 事件信封

```rust
pub struct EventEnvelope {
    pub event_id: EventId,
    pub topic: Topic,
    pub event_type: EventType,
    pub schema_version: u32,
    pub source: EventSource,
    pub subject: Option<String>,
    pub occurred_at: Timestamp,
    pub recorded_at: Timestamp,
    pub correlation_id: Option<String>,
    pub causation_id: Option<EventId>,
    pub trace_id: Option<String>,
    pub payload: serde_json::Value,
}
```

字段语义：

- `event_id`：全局唯一，用于幂等去重；
- `topic`：路由名称，例如 `approval.resolved`；
- `event_type + schema_version`：定义 payload 契约及演进版本；
- `source`：Gateway、tool worker、timer、agent 或 system；
- `subject`：主要实体，例如 `run/{run_id}` 或 `job/{job_id}`；
- `correlation_id`：把审批、工具调用或 Job 与等待条件对应起来；
- `causation_id`：记录哪个事件直接导致当前事件；
- `occurred_at` 与 `recorded_at` 分开，允许接收延迟事件；
- `payload` 必须先通过该 event type 对应的 JSON Schema。

推荐 topic：

```text
run.started
run.waiting
run.resumed
run.completed
run.failed
run.cancelled

approval.requested
approval.resolved
approval.expired

tool.started
tool.completed
tool.failed

job.started
job.progressed
job.completed
job.failed

timer.fired
schedule.triggered
```

## 5. 订阅模型

```rust
pub struct Subscription {
    pub subscription_id: SubscriptionId,
    pub owner: SubscriptionOwner,
    pub scope: SubscriptionScope,
    pub filter: EventFilter,
    pub delivery: DeliveryTarget,
    pub mode: SubscriptionMode,
    pub status: SubscriptionStatus,
    pub cursor: Option<EventCursor>,
    pub expires_at: Option<Timestamp>,
    pub max_deliveries: Option<u32>,
    pub created_at: Timestamp,
}

pub enum SubscriptionMode {
    Once,
    Continuous,
}

pub enum SubscriptionScope {
    Run(RunId),
    Agent(AgentId),
    Workspace(WorkspaceId),
    Global,
}

pub enum DeliveryTarget {
    WakeRun { run_id: RunId, wait_key: WaitKey },
    StartRun { agent_id: AgentId, input_template: Value },
    InvokeRustHandler { handler: HandlerName },
    InvokeJsHandler { module: ModuleId, export: String },
    Webhook { endpoint_id: EndpointId },
}
```

订阅状态：

```text
Active → Delivering → Active
   │          ├──────→ Completed     # once 或达到 max_deliveries
   │          └──────→ DeadLettered  # 超过重试次数
   ├─────────────────→ Paused
   ├─────────────────→ Expired
   └─────────────────→ Cancelled
```

### 5.1 Filter 必须声明式

```rust
pub struct EventFilter {
    pub topics: Vec<TopicPattern>,
    pub event_types: Vec<EventTypePattern>,
    pub sources: Vec<SourcePattern>,
    pub subject: Option<StringPattern>,
    pub correlation_id: Option<String>,
    pub payload: Vec<PayloadPredicate>,
}

pub enum PayloadPredicate {
    Eq { path: JsonPointer, value: Value },
    Exists { path: JsonPointer },
    In { path: JsonPointer, values: Vec<Value> },
}
```

第一版只支持有限的 JSON Pointer 谓词，不接受任意脚本。这样过滤可以被 SQLite/PostgreSQL adapter 重放、审计和索引，也避免订阅执行不可信代码。

### 5.2 事件匹配顺序

1. Event 先持久化到 append-only log；
2. Dispatcher 查找可能匹配的 Active subscription；
3. 执行声明式 filter；
4. 创建唯一 `Delivery(subscription_id, event_id)`；
5. 投递目标执行成功后推进 cursor；
6. `Once` 订阅原子地转为 Completed；
7. 失败按策略重试，超过限制进入 dead letter。

事件不会因为当前没有订阅者而丢失；后来创建的订阅是否读取历史，由 `start_position` 明确决定：`Now`、`Beginning` 或指定 cursor/time。

## 6. Run 挂起和唤醒

```text
Runnable → Running
              │
              ├→ Suspend { checkpoint, subscriptions[] }
              ▼
           Waiting
              │ matching delivery
              ▼
           Runnable → Running → Completed | Failed | Waiting
```

Agent 返回 Suspend 时，Coordinator 在一个事务中：

1. 保存可序列化 checkpoint；
2. 创建或确认 subscriptions；
3. 把 run 状态改为 Waiting；
4. 追加 `run.waiting`；
5. 释放 worker lease。

匹配事件到达时，Coordinator 原子地：

1. 写入 delivery 去重记录；
2. 将 event 写入 run inbox；
3. 把 Waiting run 改为 Runnable；
4. 追加 `run.resumed`；
5. 将 run 放入 runnable queue。

重新执行时，Agent 收到的是 `ResumeInput { checkpoint, events[] }`，而不是恢复原来的 Future。

## 7. Rust 内置能力

Rust Agent、内置 Tool 和系统组件使用同一个 `EventClient`：

```rust
#[async_trait]
pub trait EventClient {
    async fn publish(&self, command: PublishEvent) -> Result<EventId, EventError>;
    async fn subscribe(&self, command: CreateSubscription)
        -> Result<SubscriptionId, EventError>;
    async fn unsubscribe(&self, id: SubscriptionId) -> Result<(), EventError>;
    async fn get_subscription(&self, id: SubscriptionId)
        -> Result<Subscription, EventError>;
}

#[async_trait]
pub trait TimerClient {
    async fn schedule_once(&self, command: ScheduleOnce) -> Result<TimerId, TimerError>;
    async fn schedule_cron(&self, command: ScheduleCron) -> Result<ScheduleId, TimerError>;
    async fn cancel(&self, id: TimerOrScheduleId) -> Result<(), TimerError>;
}

#[async_trait]
pub trait JobClient {
    async fn start(&self, command: StartJob) -> Result<JobId, JobError>;
    async fn cancel(&self, job_id: JobId) -> Result<(), JobError>;
}
```

向模型暴露时，可以注册为内置 Tool：

```text
events_publish
events_subscribe
events_unsubscribe
timer_schedule_once
schedule_create
job_start
job_cancel
run_suspend
```

Tool 只负责把 JSON 参数校验后转换成 Command。权限、topic allow-list、订阅数量限制和幂等处理仍由 event runtime 执行，不能由模型参数绕过。

## 8. JS 沙箱 API

JS Runtime 注入与 Rust 相同的 host capability，不允许脚本直接连接数据库或事件 broker：

```javascript
const subscription = await mina.events.subscribe({
  mode: "once",
  scope: { type: "run", runId: mina.run.id },
  filter: {
    topics: ["approval.resolved"],
    correlationId: approvalId,
  },
  delivery: {
    type: "wake_run",
    runId: mina.run.id,
    waitKey: `approval:${approvalId}`,
  },
});

await mina.events.publish({
  topic: "approval.requested",
  correlationId: approvalId,
  payload: { approvalId, toolCallId, summary, risk: "high" },
});

return mina.run.suspend({
  checkpoint: { step: "waiting_approval", approvalId },
  subscriptions: [subscription.id],
});
```

事件到达后，运行时启动一次新的 JS 调用，而不是恢复旧 Promise：

```javascript
export async function resume(context, checkpoint, events) {
  const resolved = events.find(
    (event) => event.topic === "approval.resolved" &&
      event.correlationId === checkpoint.approvalId,
  );

  if (resolved.payload.decision === "approve") {
    return context.continue({ step: "execute_tool" });
  }
  return context.continue({ step: "tool_rejected" });
}
```

不保存 JS Promise/VM 栈是独立性和可恢复性的关键。如果需要短时间等待，可以提供仅进程内的 `mina.events.next({ timeoutMs })`，但它不能作为 durable workflow 的基础，也不能跨重启恢复。

### 8.1 Rust/JS 能力一致性

Rust trait、内置 Tool 和 JS binding 必须调用同一组 Command handler：

```text
Rust EventClient ─┐
Model built-in Tool ─┼→ CommandBus → Event Runtime
JS mina.events ────┘
```

不能为 JS 另做一套事件协议，否则权限、幂等、过滤规则和测试会产生行为漂移。

### 8.2 JS 引擎选型：rquickjs / QuickJS

第一版明确选择 [`rquickjs`](https://crates.io/crates/rquickjs) 嵌入 QuickJS，放在独立且可选的 `mina-js-runtime` crate 中。

选择原因：

- 部署时不要求安装 Node.js 或独立 JS Runtime；
- QuickJS 可以随 Rust 程序一起构建并链接，启动和 Context 创建成本适合短脚本；
- 相比 V8/`deno_core`，二进制、构建和运行时成本更符合轻量 Agent 脚本环境；
- JavaScript 是模型熟悉度很高的语言，模型生成、修复和解释脚本的成功率通常优于专用小众 DSL；
- `rquickjs` 可以把 Rust async host function 映射为受控 Promise，同时不需要暴露 Node API；
- QuickJS 提供内存限制和执行中断能力，可以实现每次调用的资源预算。

这里的“无依赖”指最终用户不需要 Node.js、npm 或系统级 JS Runtime，不代表 Cargo 零依赖。`rquickjs` 仍包含 Rust binding 和 QuickJS 原生引擎构建；不启用 JS 的 Mina 组件不应编译它。

```toml
# crates/mina-js-runtime/Cargo.toml
[dependencies]
rquickjs = { version = "...", features = ["futures", "loader"] }
mina-events = { path = "../mina-events" }

# 其他 crate 不直接依赖 rquickjs。
```

如果最终将 JS adapter 合并进某个二进制，也必须保持 feature 可选：

```toml
[features]
default = []
js = ["dep:mina-js-runtime"]
```

备选方案：

| 引擎 | 优点 | 不作为第一选择的原因 |
|---|---|---|
| `boa_engine` | 纯 Rust，不需要 C/C++ 引擎 | 依赖和运行成本不一定更小，兼容性与生产成熟度需单独验证 |
| Rhai | Rust-native、易嵌入、能力面很小 | 不是 JavaScript，模型生成脚本的稳定性和可迁移性较弱 |
| V8 / `deno_core` | JS 兼容性、性能和生态完整 | 构建、二进制、内存与启动成本明显更高 |
| 外部 Node.js | npm 生态完整 | 破坏单二进制与无外部运行时目标，权限面过大 |

### 8.3 执行模型

每次 JS 调用都是隔离且有界的一次执行：

```text
领取 JS execution lease
  → 创建或从池中取得 Runtime/Context
  → 安装内存、时间、Host API 调用预算
  → 注入只读 context 与 mina capability object
  → 加载白名单 module
  → 调用 start() 或 resume()
  → 校验 StepOutcome
  → 清空任务队列并丢弃/回收 Context
```

QuickJS Runtime/Context 不在任意 Tokio worker 线程间共享。`mina-js-runtime` 使用固定的专用 worker 线程或有界 worker pool，每个 execution lease 在同一线程内完成；Rust async capability 通过消息和受控 Promise 与外部 runtime 交互。

建议的默认预算：

| 资源 | 默认策略 |
|---|---|
| 脚本源码 | 256 KiB 上限 |
| JS heap | 32 MiB 上限 |
| 单次执行时间 | 5 秒，可按 capability 调整 |
| Host API 调用 | 每次执行最多 64 次 |
| 返回值/checkpoint | 1 MiB 上限且必须可 JSON 序列化 |
| module 数量 | 仅允许 manifest 白名单 |
| 并发 Context | 由有界 worker pool 控制 |

具体数值应配置化，但不能允许 Agent 自行提高预算。执行超时通过 QuickJS interrupt handler 终止；超内存、超时、返回值非法和 Host API 越权都映射成稳定的脚本错误，不返回内部堆栈或敏感数据。

### 8.4 默认能力面

默认只注入：

```text
JSON、基础 ECMAScript 内建对象
mina.events
mina.timers
mina.jobs
mina.tools
mina.run
受控 console（脱敏、限量）
```

默认不提供：

```text
fetch / WebSocket
process / require
文件系统
环境变量
动态原生模块
npm 包解析
任意系统时间和随机源（需要时由 host 提供可测试版本）
```

Module loader 只加载已注册的内存 module 或签名资源。所有 `mina.*` 方法都经过 capability、schema、配额和审计检查；JS 对象本身不是安全边界。

### 8.5 安全边界

QuickJS Context 隔离适合第一版受控的 Agent 脚本，但进程内脚本引擎不能被视为针对恶意多租户代码的最终安全边界。生产阶段按风险分层：

- 低风险 JSON 处理和事件编排可在进程内 QuickJS 执行；
- 需要文件、网络、编译器或高资源预算的脚本转为独立 Job；
- 不受信任的多租户代码使用子进程、容器或 WASM/系统 sandbox；
- 高风险 Job 只通过事件和结构化结果与 Agent 通信，不把系统能力重新暴露给 JS Context。

## 9. 四条典型链路

### 9.1 用户审批

```text
Agent → subscribe approval.resolved(correlation=approval_id)
Agent → Command(RequestApproval)
Runtime → Event(approval.requested)
Agent → Suspend
Gateway/UI → Command(ResolveApproval)
Approval service → Event(approval.resolved)
Dispatcher → WakeRun
Agent.resume → execute or reject tool
```

### 9.2 异步工具

```text
Agent → StartJob(tool invocation) → job_id
Agent → subscribe job.completed|job.failed(correlation=job_id)
Agent → Suspend
Worker → Event(job.completed)
Dispatcher → WakeRun
Agent.resume → append tool result → continue model loop
```

普通毫秒级工具仍可直接 await `ToolPort::call`。只有预计长时间运行、需要 webhook 或需要独立重试的工具才转成 Job。

### 9.3 Timer

```text
Agent → timer.schedule_once(at, payload) → timer_id
Agent → subscribe timer.fired(correlation=timer_id)
Agent → Suspend
Timer worker → Event(timer.fired)
Dispatcher → WakeRun or StartRun
```

### 9.4 周期任务

周期 schedule 通常使用 `StartRun` delivery，而不是唤醒一个永远不终止的 run。每次触发创建独立 run，便于取消、审计、超时和并行控制。

## 10. 投递语义与一致性

系统采用 at-least-once：

- `event_id` 全局唯一；
- `(subscription_id, event_id)` 是 delivery 唯一键；
- handler 必须幂等；
- Command 支持 idempotency key；
- 同一 `subject` 内按 append sequence 有序；
- 不保证不同 subject 的全局业务顺序；
- cursor 只在 delivery 成功后推进；
- retry 使用指数退避和最大次数；
- dead letter 可查询、重放或丢弃；
- Timer 使用持久化 lease，多个 worker 只能有一个成功触发记录。

数据库事务至少需要覆盖：

```text
append event
create deliveries
transition subscription
transition run
write outbox
```

外部 webhook、队列或通知通过 transactional outbox 发送，避免数据库提交成功但外部消息丢失。

## 11. 权限与隔离

每个 Agent/JS module 获得显式 capability：

```text
events.publish: ["agent.*", "approval.requested"]
events.subscribe: ["approval.resolved", "job.*", "timer.fired"]
subscriptions.max_active: 32
timers.max_pending: 16
schedules.create: false
jobs.start: ["code-execution", "browser-task"]
```

规则：

- Agent 不能伪造 system、approval service 或 tool worker source；
- Agent 不能订阅其他 workspace/run 的事件，除非 capability 明确允许；
- payload 先做 schema、大小和敏感字段检查；
- JS binding 只能访问注入的 capability object；
- webhook event 必须验证签名，并先映射为受信任 Command；
- 所有订阅创建、决策、唤醒、重试和取消都写审计事件。

## 12. 存储模型

第一版 SQLite 表建议：

```text
events
  sequence, event_id, topic, event_type, schema_version,
  source, subject, correlation_id, causation_id,
  occurred_at, recorded_at, payload

subscriptions
  subscription_id, owner, scope, filter_json, delivery_json,
  mode, status, cursor, expires_at, max_deliveries, delivery_count

deliveries
  subscription_id, event_id, status, attempts,
  next_attempt_at, last_error, delivered_at

timers
  timer_id, fire_at, topic, payload, status, lease_until

schedules
  schedule_id, cron, timezone, next_fire_at, payload, status

run_checkpoints
  run_id, revision, state, checkpoint, waiting_for, updated_at

run_inbox
  run_id, event_id, consumed_revision

outbox
  outbox_id, kind, payload, attempts, next_attempt_at, status
```

## 13. 与当前 Mina 的集成

当前 [event.rs](../crates/core/src/harness/event.rs) 继续维护 run 内连续 `seq` 和唯一终态；当前 [agent_loop.rs](../crates/core/src/harness/agent_loop.rs) 继续处理短模型/工具循环。新增桥接层后：

```text
Event delivery
  → RunCoordinator.wake(run_id, event)
  → load checkpoint + inbox
  → AgentMachine.resume(...)
  → AgentEvent
  → current RunEvent engine
```

要支持真正挂起，`Agent::run() -> Stream` 最终需要补充状态机接口，而不是直接删除现有接口：

```rust
pub trait AgentMachine {
    fn start(&self, request: StartRequest) -> StepFuture;
    fn resume(&self, request: ResumeRequest) -> StepFuture;
}

pub enum StepOutcome {
    Continue { checkpoint: Value, effects: Vec<Effect> },
    Suspend { checkpoint: Value, subscriptions: Vec<SubscriptionId> },
    Complete { output: String },
    Failed { error: AgentError },
}
```

现有 `AgentLoop` 可以先作为 `AgentMachine` 的一种实现；Gateway 不需要了解 Rust Agent 还是 JS Agent，也不负责保存 checkpoint。

## 14. MVP 顺序

### P0：独立内存事件核心

- 新增 `mina-events` 和 `mina-event-runtime`；
- EventEnvelope、声明式 Filter、Once/Continuous Subscription；
- Rust `EventClient`；
- 内存 EventLog、SubscriptionStore、Dispatcher；
- `publish/subscribe/unsubscribe` 确定性测试；
- 不接 Chat UI。

### P1：Timer 与 Harness bridge

- 一次性 Timer；
- `WakeRun` delivery；
- `AgentMachine start/resume`；
- 可序列化 checkpoint；
- run 挂起时释放 worker；
- cancel 清理等待订阅。

### P2：SQLite durability

- EventLog、Subscription、Delivery、RunCheckpoint 持久化；
- transaction + outbox；
- worker lease、重启恢复、dead letter；
- 审批和异步 Job 完整链路。

### P3：JS bindings

- 独立、可选的 `mina-js-runtime`，使用 `rquickjs`/QuickJS；
- 专用有界 worker pool、内存限制和 interrupt deadline；
- `mina.events`、`mina.timers`、`mina.jobs`、`mina.tools`、`mina.run.suspend`；
- JS module capability manifest；
- 白名单内存 module loader，不提供 Node.js/npm/文件/网络；
- 全新 `resume()` invocation；
- Rust/JS contract test 使用同一组 fixtures。

### P4：更多 adapter

- PostgreSQL、NATS/Kafka bridge；
- webhook ingestion；
- cron/timezone；
- 订阅查询、审计和重放 API。

## 15. 必须保持的设计决策

1. 事件运行时可以脱离 Gateway、Chat UI 和 Harness 单独运行；
2. Rust、模型内置 Tool 和 JS binding 共享同一个 Command handler；
3. 持久化订阅只保存声明式 filter 和 handler 引用，不保存闭包；
4. 挂起保存 checkpoint，不保存 Future、Promise 或 VM 栈；
5. Command 表达意图，Event 表达事实；
6. 采用至少一次投递和幂等处理；
7. 高频 token delta 不进入默认持久化事件总线；
8. 周期任务每次创建独立 run，避免永久 run；
9. Chat UI 只是订阅/审批的一种 adapter，不进入核心协议；
10. 所有外部输入先经过权限和 schema 验证，再成为可信事件。
11. JS adapter 默认使用独立可选的 `mina-js-runtime` + `rquickjs`，事件核心不依赖任何 JS 引擎。
