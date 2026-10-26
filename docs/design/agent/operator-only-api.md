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
