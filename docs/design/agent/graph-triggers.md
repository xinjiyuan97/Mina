# Trigger 节点与手动触发

Trigger 是图的源节点：一次触发创建一个新的 Run，并向图内发布初始事件。用户输入只是触发数据的一种，图入口不再绑定“用户发消息”。当前实现 `manual`，宿主可从 HTTP、CLI 或代码调用它。

定时、Webhook 等触发类型尚未实现。它们后续负责取得触发数据、确定触发时机和去重身份，再接入相同的 Run 创建与事件发布流程；不会成为长期占用一个 Run 的等待算子。当前填写未实现的类型会报错。

## 图格式

```yaml
schema_version: 2
name: triggered-assistant
triggers:
  start:
    type: manual
    version: 1
    event: agent.start.v1
    config: {}
operators:
  context:
    type: agent.context
    version: 1
    config:
      system_prompt: "你是一个简洁的助手。"
      max_steps: 8
  llm:
    type: agent.model
    version: 1
    config:
      model: primary
      timeout_ms: 60000
subscriptions:
  - id: start-context
    from: start
    subscriber: context
    events: [agent.start.v1]
  - id: request-model
    from: context
    subscriber: llm
    events: [model.requested.v1, context.failed.v1]
lifecycle:
  start:
    trigger: start
  terminal:
    from: llm
    completed: stop.completed.v1
    failed: stop.failed.v1
    cancelled: stop.cancelled.v1
limits:
  timeout_ms: 300000
  max_deliveries: 10000
```

`triggers` 与 `operators` 是两类不同的节点。Trigger 没有输入订阅、算子 handler 或 Run 内 checkpoint；它的触发身份、输入和图定义作为 Run 的初始状态保存。算子仍按原有协议消费事件。

| 字段 | 含义 |
| --- | --- |
| `triggers.<id>` | 触发器实例名，与算子名共享命名空间，不可重名 |
| `type` | 当前仅支持 `manual` |
| `version` | 当前仅支持 `1`，独立于图格式版本 |
| `event` | 触发时发布的事件类型，必须带 `.vN` 协议版本 |
| `config` | 当前 manual 配置必须为 `{}` |
| `lifecycle.start.trigger` | 普通 run/start API 使用的默认触发器 |

每张图允许 1–32 个触发器。每个触发器只发布其声明的一个事件，但允许多个算子订阅。所有消费者对该事件的输入 Schema 必须一致；每个触发器都必须能通过订阅边到达终态节点。触发器不能作为 `subscriber`。

## 调用方式

```rust,ignore
let agent = AgentBuilder::new(registry, resources).from_file("config/agents/text.yaml")?;

// 使用生命周期配置中的默认 trigger。
let first = agent.run(json!({"input": "你好"})).await?;

// 显式选择 trigger。每次调用分配新的 RunId。
let second = agent.trigger("start", json!({"input": "检查今日事项"})).await?;

// 底层宿主可提供稳定 RunId，对同一次触发进行重试。
let run = agent.engine.start_trigger("local", run_id, "start", payload).await?;
```

手动触发的数据不要求是文本，具体由订阅算子的 Schema 决定。内置 Context 使用 `agent.start.v1`，接收非空字符串或 `{input, attachments?, prior_messages?}`；自定义节点可以声明自己的启动事件和 JSON payload。

如果需要 AgentEngine 的持久化接入和公开事件流：

```rust,ignore
let started = engine.trigger_graph(definition, "start", payload).await?;

// 需要指定 RunId、权限或上下文准备结果时，先选择默认入口，再传入 PlannedRun。
let selected = definition.with_start_trigger("start")?;
let started = engine.start_graph(selected, payload, plan).await?;
```

`trigger_graph` 在接入前选择默认入口并锁定该定义。恢复已有 Run 使用保存的图、trigger 和 payload，不读取当前配置文件重新触发。低层 `admit_trigger` 可以只持久化触发事实，稍后 `recover` 启动执行。

## 多个入口

可以增加另一个 manual trigger，例如 `api`，为它声明事件，并添加 `from: api` 的订阅。`agent.trigger("api", payload)` 仅触发该入口，不会同时触发默认入口。来自不同入口的事件即使类型相同，也根据来源实例名分别路由。

Run 创建与首个触发事件原子提交。同一租户、同一 RunId、相同 trigger、payload、图定义及资源契约的重试不会重复发布初始事件；更换其中任意身份数据会被拒绝。每次 `trigger()` 生成新 RunId，因此宿主若需要业务去重，应保存并重用同一次触发的 RunId。

## 与现有 Run 的事件区别

- Trigger：创建新 Run，适合用户请求、未来的定时任务或外部业务通知。
- `$external` / GraphIngress：向已有 Run 发送图中声明允许的事件，适合审批决定和等待唤醒。
- `agent.wait`：在已有 Run 内等待条件满足，不创建新 Run。

当前没有定时任务注册器、Cron 解析器或 Webhook 监听服务，也不会在加载图时启动后台触发。后续扩展需要独立实现调度、时区、重启补偿及触发去重策略。

## 从旧图迁移

1. 将图格式 `schema_version` 改为 `2`。
2. 添加 `triggers.start`，把原 `lifecycle.start.event` 移到它的 `event` 字段。
3. 把原 `from: "$input"` 改为 `from: start`。
4. 将 `lifecycle.start` 改为 `{trigger: start}`。
5. Rust 构图时添加 `.trigger("start", TriggerDefinition::manual("agent.start.v1"))`。

这是破坏性格式变更，不提供 v1 图或旧图 checkpoint 的自动迁移。仓库中的 text、tools、durable-tools 和 demo 图均已更新。完整图规则见 [图编写指南](static-agent-graph.md)。
