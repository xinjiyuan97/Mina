# 从配置构建 Agent

Core 提供两种等价入口：文件解析为 `AgentDefinition`，或者用 `AgentGraph` 在代码中生成定义；统一由 `AgentBuilder` 校验并创建 `ConfiguredAgent`，由其 `GraphEngine` 调度单 Agent 内部的 `OperatorBus`。

完整格式、字段、订阅语义和代码示例见 [静态 Agent 图编写指南](static-agent-graph.md)。旧 `version/run/source/consumer` 配置格式已移除。

## 接入步骤

1. 选择 [纯文本图](../../../config/agents/text.yaml) 或 [工具图](../../../config/agents/tools.yaml)。
2. 创建 Fabric、ModelPort，以及工具图所需的 ToolPort。
3. 创建 `OperatorRegistry::builtins()`；需要自定义类型时注册其 `OperatorDescriptor` 与工厂。
4. 用 `AgentResources::new(fabric).model("primary", revision, model)` 绑定模型；工具图还要 `.toolset("workspace", revision, tools)`。
5. 调用 `AgentBuilder::new(registry, resources).from_file(path)`。
6. 调用 `agent.run("问题").await` 接收 Run，通过 `agent.engine.subscribe("local", run.run_id, 0)` 观察事件，或通过 `get_run` 查询终态。

解析不执行算子。构建时检查类型版本、配置、资源、订阅协议、可达性与生命周期；每个 Run 使用独立算子实例。修改文件只影响重新构建后的新 Run，恢复已有 Run 使用持久化快照，并核验协议和资源 revision。

## 当前范围

支持 YAML/JSON、代码构建、任意自定义静态协议图、模型与即时低风险工具循环、取消、预算、日志订阅及持久化恢复。运行中不会重新读取文件或修改订阅。

内置图支持审批、外部等待、幂等重试和可选并行工具。独立 agent_chat_demo 通过 TOML 的 graph 字段读取图配置；CLI 和正式 server 使用 GraphAgentConfig 构建同一种静态图。GraphHost 将内部图事件投影成持久化公开 RunEvent，AgentEngine 提供后台运行与恢复。内部图日志可能包含完整工具输入，不应直接暴露给 HTTP。

行为测试位于 `crates/core/tests/static_agent_graph.rs`，涵盖两种入口一致性、自定义图、来源隔离、工具循环、恢复、取消与预算等。

## 默认图节点

纯文本图为 `agent.context → agent.model`，Model 直接发布 `stop.completed / stop.tool_calls / stop.failed`。工具图把 `stop.tool_calls.v1` 连接到 `agent.gate`，Gate 分发到 ToolPolicy，经授权后进入 Tools，收齐结果后回到 Context。Gate 使用通用 batch_id、分支 id 和不透明 payload，也支持自定义审批等节点。Router、ToolJoin 和 Coordinator 的旧静态图类型已删除。Compactor 尚未实现。

## 分发图的宿主绑定

v3 已提供独立的绑定契约、JSON Schema、`HostBindings` 和 `HostPolicy`，构建时完成解析及权限收紧并保存快照。宿主不依赖节点名；`AgentResources` 继续注入实例。实际用法及限制见[宿主集成 API](host-integration-api.md)，目标扩展见[宿主集成契约](host-integration-contracts.md)。
