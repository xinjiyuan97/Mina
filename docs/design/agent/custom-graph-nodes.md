# 注册自定义 Graph 节点

Mina 支持在 Rust 宿主中注册自定义节点，再通过 YAML/JSON 或 Rust Builder 连接它们。YAML 只引用已注册的类型，不会下载插件、编译源码或加载任意代码。

先阅读[图编写指南](static-agent-graph.md)和[内置节点参考](graph-nodes.md)。可运行的 demo 图见 [demo.yaml](../../../config/agents/demo.yaml)，宿主入口见 [agent_chat_demo.rs](../../../apps/server/examples/agent_chat_demo.rs)。

## 注册和执行边界

```text
宿主实现 GraphOperator
  → 定义 OperatorDescriptor
  → OperatorRegistry.register(descriptor, factory)
  → YAML 引用 type/version/config
  → AgentBuilder 或 GraphAgent 校验并装配
  → 输入事件触发 handle
  → 原子提交状态、输出事件、用量和消费确认
```

实例名与类型名不同：`inspect_request` 可以是图里的实例名，`example.request_observer` 是注册类型。一个类型可以创建多个实例，每个实例拥有自己的状态和订阅。

## 示例：在 Context 与 Model 之间增加请求观察节点

下面节点记录收到请求的次数，原样转发模型请求，不调用外部服务。它复用已注册 Model 的输入 schema，避免两个节点对同一个事件声明不一致的协议。

```rust
use std::sync::Arc;
use agent_core::agent::{
    GraphContext, GraphEvent, GraphOperator, GraphOutput,
    OperatorDescriptor, OperatorRegistry,
    graph::GraphFuture,
};
use agent_core::operator_fabric::FabricError;
use serde_json::json;

struct RequestObserver;

impl GraphOperator for RequestObserver {
    fn handle(&self, context: GraphContext) -> GraphFuture<'_> {
        Box::pin(async move {
            let count = context.state["requests"].as_u64().unwrap_or(0);
            Ok(GraphOutput::new(
                json!({"requests": count.saturating_add(1)}),
                vec![GraphEvent::new(
                    "model.requested.v1",
                    context.event.payload,
                )],
            ))
        })
    }
}

fn registry_with_custom_nodes() -> Result<OperatorRegistry, FabricError> {
    let mut registry = OperatorRegistry::builtins();
    let schema = registry
        .describe("agent.model", 1)
        .expect("built-in model is registered")
        .inputs["model.requested.v1"]
        .clone();
    let mut descriptor = OperatorDescriptor::new("example.request_observer", 1)
        .input("model.requested.v1", schema.clone())
        .output("model.requested.v1", schema);
    // Pure state transition: an interrupted invocation can be replayed safely.
    descriptor.replay_safe = true;
    registry.register(descriptor, |_config, _resources| {
        Ok(Arc::new(RequestObserver))
    })?;
    Ok(registry)
}
```

`OperatorDescriptor::new` 默认配置 schema 为不允许额外字段的空对象，所以这个节点的 `config` 必须是 `{}`。要接收配置，修改 `config_schema`，然后在工厂中解析 `config` 并构造实例；工厂解析失败应返回 `FabricError`，不要把配置错误推迟到模型调用之后。

在 demo 的 `build_app` 中，将传给 `GraphAgent::new` 的 `OperatorRegistry::builtins()` 替换为 `registry_with_custom_nodes()?`，并将上述实现加入宿主或引用的 crate。仅修改 YAML 而不注册实现会在装配时失败。

## 修改 YAML 订阅

在 demo.yaml 的 `operators` 中增加：

```yaml
  inspect_request:
    type: example.request_observer
    version: 1
    config: {}
```

将原有 `request-model` 订阅替换为下面两条，其他订阅不变：

```yaml
  - id: inspect-model-request
    subscriber: inspect_request
    from: context
    events: [model.requested.v1]
  - id: request-model
    subscriber: llm
    from: inspect_request
    events: [model.requested.v1]
```

结果是 `Context → RequestObserver → Model`。不要同时保留原有 Context → Model 直连，否则同一请求会有两条投递路径。`context.failed.v1` 仍从 Context 直接连接 Model，生命周期仍由 llm 的 stop.* 决定。

同名事件可以从不同节点发布，订阅的 `from` 区分来源。当前订阅不按 payload 自动过滤；需要条件分支时，由节点发布不同事件类型，再分别订阅。

## Descriptor 字段

| 字段 | 作用 |
| --- | --- |
| `kind` / `version` | 类型及正整数版本，同一 Registry 不可重复注册相同组合 |
| `config_schema` | 配置 JSON Schema，装配时校验 |
| `inputs` / `outputs` | 事件名到 payload schema 的映射，事件名需要版本后缀 |
| `resources` | 配置字段名到资源类型的映射；是否必需由 config_schema 决定 |
| `replay_safe` | 是否允许恢复时重放被中断的调用，默认 false |
| `stateless_parallel` | 是否允许独立调用重叠，默认 false；启用后必须返回 null 状态 |

同名事件在 Registry 中必须使用一致 schema。订阅连接、节点版本、资源绑定及启动到终态的可达性会在构建时校验。

## 状态、资源和副作用

`GraphContext` 提供 tenant、run_id、instance、event、state 和 cancellation。持久状态应来自 `context.state`，更新后通过 `GraphOutput::new` 返回；不要依赖结构体内存中的计数器维持跨进程状态。长时间操作需要响应 cancellation，并自行设置合适的外部调用超时。

`GraphOutput::with_usage` 接受本次调用用量，而非 Run 累计值。无模型调用时不要虚构 token 数。引擎把返回的状态、事件、用量和消费确认一起提交。

内置资源类型为 Model、Toolset、WaitService。工厂可通过 `AgentResources::get_model` 和 `get_toolset` 获取对应端口；Wait 由 GraphWaitBridge 接入。API key、HTTP 客户端、数据库连接应留在宿主适配器中。

工厂也可以捕获自定义 `Arc` 依赖，但它们不会自动成为引擎的持久化资源绑定。自定义依赖的版本变化需要宿主制定恢复策略，不能假定闭包捕获值会被写入 checkpoint。

纯计算节点可以标记 replay_safe；发邮件、支付、生图等非幂等副作用不能仅为了恢复方便而标记为可重放。引擎的事务不能回滚外部 API 已完成的操作。类型协议或行为不兼容时应升级版本；已有 Run 使用保存的图，修改 YAML 不会热更新其订阅关系。

## 什么时候写节点，什么时候写 Tool

| 需求 | 推荐扩展点 |
| --- | --- |
| 模型按需调用文件读写、终端、生图或业务 API | Extension 中实现 Tool，注册到 ToolRegistry，由 agent.tools 执行 |
| 在执行图中固定增加请求转换、分支、汇总或观察流程 | 实现 GraphOperator，注册到 OperatorRegistry |
| 替换模型服务、存储或沙箱 | 实现 Core 端口，宿主注入 Extension 适配器 |

当前 demo 的 list_directory/read/write/terminal 和 generate_image 均为工具，不是独立监听总线的节点。执行链是 Model → Gate → ToolPolicy → Tools → Gate → Context → Model；配置和测试方式见 [demo 使用说明](../../14-agent-chat-demo.md)。

## Skill 与当前边界

Skill 契约、选择和编译在 [Core skill](../../../crates/core/src/skill/mod.rs)，文件加载在 [Extension SkillStore](../../../crates/extension/src/store/skill.rs)。正式 Server 通过 Core 的 AgentContextPreparer 在进入图前解析 Skill 并准备上下文。

demo 尚未接入 SkillOrchestrator，读取 SKILL.md 文件不等于自动选择和加载 Skill。当前 Registry 没有内置 Skill 或 Compactor 节点；如需把这些逻辑放入图中，需要实现并注册相应节点及协议。
