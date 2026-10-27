# 静态图单 Agent API

正式 server、CLI 和 demo 已统一使用 Core 内的静态图引擎。`MINA_ENGINE_PROFILE` 默认 `operator_v1_tools`，也支持工具禁用的 `operator_v1_text`；两者使用同一运行时。旧固定流水线和原生执行入口已删除，无自动回退。

## 分层

```text
HTTP / CLI / 自定义宿主
  AdmissionFacade：Session、Skills、Memory 与上下文准备
  AgentEngine：接入、后台驱动、审批、恢复与 RunStore 查询投影
  GraphHost / GraphAgent：公开事件持久化、续读 / 便利流接口
  GraphEngine：静态订阅图、算子状态、终态和 token 用量
  OperatorBus / OperatorFabric：订阅、事务、租约、投递与 outbox
        ↓ 注入端口
  agent-extension：Provider、工具、SQLite、Blob、Sandbox

agent-harness::multi：独立的跨 Agent 总线与编排
```

单 Agent 的事件描述一个 Run 内的工作；多 Agent 消息描述 Agent 间的任务与结果，两者不共享内部 checkpoint。Core 正常依赖不包含 Harness、Extension、HTTP 客户端或数据库。

## 底层 API

`OperatorBus` 提供 `subscribe_operator`、`subscribe_events`、`send`、`events_after` 与 `tick`。`GraphOperator` 处理输入事件，返回状态和输出事件；GraphEngine 原子提交状态、用量、输出及消费确认。

通过 `AgentDefinition::from_file` 读取 YAML/JSON，或通过 `AgentGraph` 在 Rust 中构建图，再交给 `AgentBuilder`。两种方式共用协议校验与执行路径。见[图编写指南](static-agent-graph.md)。

## 高层 API

```rust,ignore
let resources = AgentResources::new(fabric)
    .model("primary", "provider-v1", model)
    .toolset("workspace", "tools-v1", tools)
    .wait_service("durable", "wait-v1", bridge);
let engine = Arc::new(AgentEngine::new(resources, run_store));
let mut config = AgentRunConfig::new("model-name", "system prompt");
config.allow_suspend = true;
let started = engine.run(&config, "任务").await?;
```

- `run(config, input)`：直接开始任务。
- `start(config, plan)`：接受已准备的上下文，保存不可变接入记录并启动后台驱动。
- `admit(config, plan, failure)`：只保存接入与图，不启动外部调用；可记录上下文失败。
- `get_run(run_id)`：读取 RunStore 公开状态。
- `events_after(run_id, after, limit)`：读取稳定序号的公开事件，每页最多 128 条。
- `event_pages(run_id, after)`：按需拉取有界事件页；断开订阅不取消运行。
- `subscribe(run_id)`：获取活动运行的广播订阅；落后时用数值游标补齐。
- `cancel(run_id, actor)`：保存取消终态并投影；活动调用收到取消信号。
- `approve(run_id, approval_id, actor, resolution)`：验证绑定、到期时间和运行状态，持久化审批事实；重复相同决定幂等，不同决定冲突。HTTP 层负责提供可信 actor。
- `recover(run_id)`：使用已保存的图及截止时间恢复，并修复接入与建图之间的崩溃窗口。
- `stop_workers()`：停止本实例后台任务，不等价于取消 Run。

后台驱动先投影到 RunStore 再广播，并定期提交审批过期事实。EventRuntime 与 JobRuntime 由宿主装配到 GraphWaitBridge；图中 Wait 算子负责把唤醒转为 Gate 结果。

需要自行管理驱动与查询投影时，直接使用 `GraphHost`。`GraphAgent` 实现标准 Agent 流接口，供 Harness、CLI 和 demo 集成，其流丢弃时会取消 Run。MemoryFabric 只提供进程内保存；跨进程恢复需要持久化 Fabric。

恢复不会盲目重放被中断的模型调用或非幂等外部副作用。资源 revision 与算子描述必须匹配。`max_steps` 计模型轮次，耗尽后直接失败，不再额外调用模型总结。

## 删除与验证

已移除旧 Coordinator / Model / Output / ToolPreparation / ToolOperator / ApprovalIngress 固定流水线，以及其 Harness 导出和 SqlitePipelineStore。公开 SSE 事件名和数值续传接口保留，旧易失性 StreamCursor 格式已删除。旧运行记录不转换为新图。

当前验证入口：

- `crates/core/tests/static_agent_graph.rs`：图协议、用量、恢复、审批、等待、并行与脱敏。
- `crates/core/tests/agent_api.rs`：接入、恢复、取消、有界续读和故障注入。
- `crates/core/tests/graph_retention.rs`：终态回收、租约和共享引用保护。
- server 测试：HTTP、Session 幂等、SSE 重放、多模态和恢复。
- Fabric、存储适配器和 Harness 多 Agent 测试：事务与跨 Agent 边界。

## 任意图的高层运行入口

`AgentEngine::new(resources, run_store)` 使用内置 Registry；自定义节点使用 `AgentEngine::with_registry(resources, run_store, registry)`。Registry 在恢复时也用于重建节点，重启后的宿主必须注册匹配版本。

```rust,ignore
let definition = AgentDefinition::from_file("config/agents/demo.yaml")?;
let engine = Arc::new(AgentEngine::with_registry(resources, run_store, registry));
let started = engine.run_graph(definition, serde_json::json!({
    "input": "查看当前目录", "attachments": [], "prior_messages": []
})).await?;
```

`run_graph` 接收任意 JSON 启动载荷；输入 schema 由图的启动订阅决定，不强制要求聊天对象。`start_graph(definition, payload, plan)` 接受已有 PlannedRun，`plan.input` 是 RunStore 展示文本，payload 才是传给图的原始输入。`admit_graph(definition, payload, plan, failure)` 只持久化接入，`start_graph_outcome` 同时支持上下文失败结果和后台驱动。

图定义、原始输入与准备结果在接入时锁定；相同 Run ID 重复提交不同定义或输入会被拒绝。`recover` 从保存的定义和原始 JSON 恢复，不依赖当前配置重新生成图。未产生模型流片段的自定义图也会将最终输出投影到公开事件和 RunStore。审批、取消、分页重放和恢复与默认图共用同一路径。

默认图的 `run/start/admit` 仍可使用 GraphAgentConfig。两个入口都通过 `AgentDefinition::constrain_run` 收紧内置 Context/ToolPolicy/Tools 的工具白名单、ADF 和模型轮次上限，不扩大图已有权限；自定义节点自己的授权规则仍由实现负责。

正式 Server 的 tools profile 可以通过 `MINA_AGENT_GRAPH=/path/to/graph.yaml` 加载图，资源名为 primary、workspace、durable。HTTP 入口向图发送 `{input, attachments, prior_messages}`；若需要其他业务输入协议，使用 Core 的 run_graph/start_graph 接口。自定义节点先在宿主 Registry 注册，再传给 ServerAgentRuntime::with_graph。修改文件只影响新 Run，已保存的 Run 使用锁定定义。

## 统一上下文准备

宿主准备和图内消息循环职责不同：

- `agent::host::preparation::ContextPreparation`：解析 Skill、检索 Memory、读取 Session 或宿主传入的历史，执行上下文预算，生成 PreparedContext。
- `AdmissionFacade`：将上述准备过程放在持久化接入屏障中，正式 Server 使用此路径。
- `GraphAgent::with_context_preparation(preparer, policy)`：轻量宿主直接复用同一 ContextPreparer 和 prepared_plan 转换，demo 使用此路径。
- 图节点 `agent.context`：接收已准备消息，在图执行期间追加工具结果并控制模型轮次，不重复发现 Skill 或检索 Memory。

ContextRequest 的 prior_messages 用于内存会话等轻量宿主，与 SessionStore 历史二选一；两者都进入 ContextEngine 的预算流程。直接传入的历史应是已完成对话消息，含未结算 tool call 的历史会被拒绝。准备失败会阻止模型执行；直接 Agent 流将返回安全错误，取消也会中断准备等待。

## 公开模块路径

| 规范入口 | 内容 |
| --- | --- |
| `agent_core::contracts` | Agent、Model、Tool、Session、RunStore 等通用契约 |
| `contracts::fabric` | 持久化算子存储协议及通知契约 |
| `contracts::events` | 外部事件、订阅和定时器协议 |
| `contracts::preparation` | ContextRequest、ContextPolicy 和准备结果协议 |
| `agent::graph` | 图校验、节点、执行状态和 GraphHost |
| `agent::host` | AgentEngine、上下文准备、运行记录及 AgentLoop / SingleTurnAgent 等便利执行接口 |
| `agent::executor` / `agent::bus` | 低层算子投递执行与发布订阅 API |

常用 Agent 类型继续从 `agent` 重导出，旧 `core::harness`、根级 context_operator/event_runtime/operator_fabric 路径已移除。Core 内测试也使用真实的 host runner，不再维护仅供测试的另一份 Harness 实现。独立 `agent-harness` crate 仍承载多 Agent 和宿主集成，不等于旧 Core 模块。

## 通知唤醒与存储兜底

OperatorFabric 可通过 `changes()` 提供进程内 watch 通知。MemoryFabric 和 SqliteEventStore 在可观察变更提交成功后通知；只读查询、空投递扫描和失败事务不唤醒消费者。SQLite clone 共享通知，独立打开连接或其他进程的写入通过定时兜底发现。

图驱动、调用期间的终态检测、公开事件投影和续读采用“订阅通知 → 标记代次 → 读取持久状态 → 等待变化”的顺序，避免读取与等待之间丢失唤醒。常规兜底为一秒，图驱动还按 Run 截止时间缩短等待；通知不是持久化事实，恢复仍以日志和 checkpoint 为准。外部 EventRuntime/JobRuntime 的调度属于其宿主，不因内部图通知而自动跨进程推送。

`fabric_notifications` 回归覆盖读后等待竞态、16 个空闲 Run 的存储访问上限、通知触发的及时唤醒、SQLite 成功/失败事务通知以及独立连接的兜底读取。
