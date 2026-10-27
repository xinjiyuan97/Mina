# Graph 节点参考

本文描述 `OperatorRegistry::builtins()` 当前注册的六类单 Agent 节点，版本均为 `1`。图格式和完整示例见[静态 Agent 图编写指南](static-agent-graph.md)；宿主接入见 [API 与分层](operator-only-api.md)。

节点的实例名由图作者指定，例如 `llm`；`type` 标识实现，例如 `agent.model`。同一种实现可以创建多个实例，订阅的 `from` 和 `subscriber` 使用实例名。下文的事件名均包含协议版本后缀 `.v1`。

图的入口另外声明在 `triggers` 中：当前支持 `type: manual` 的源节点，由引擎触发，不在 OperatorRegistry 注册，也不订阅 Run 内事件。详见 [Trigger 节点与手动触发](graph-triggers.md)。

## 节点一览

| type | 能力 | 必需资源 | 必需配置 |
| --- | --- | --- | --- |
| `agent.context` | 维护模型消息、工具快照和轮次预算 | 可选 Toolset | `system_prompt`、`max_steps` |
| `agent.model` | 调用模型、输出流片段和停止事件 | Model | `model`、`timeout_ms` |
| `agent.gate` | 通用 all-of 批次分发与结果汇总 | 无 | 空对象 `{}` |
| `agent.tool_policy` | 校验调用、绑定授权、记录审批 | Toolset | `toolset`、`approval_timeout_ms` |
| `agent.tools` | 执行授权工具，返回结果或等待请求 | Toolset | `toolset`、`timeout_ms` |
| `agent.wait` | 注册外部等待、验证唤醒、清理订阅 | WaitService | `wait_service` |

纯文本图使用 Context + Model；工具图增加 Gate + ToolPolicy + Tools；允许工具挂起时再增加 Wait。节点配置不接受未声明字段，资源名必须对应宿主注入的资源。

## Context：`agent.context`

输入：`agent.start.v1`、`gate.completed.v1`。输出：`model.requested.v1`、`context.failed.v1`。

| 配置 | 必需 / 默认 | 含义 |
| --- | --- | --- |
| `system_prompt` | 必需 | 系统提示词，可为空字符串 |
| `max_steps` | 必需，1–100 | 模型调用轮数上限 |
| `toolset` | 可选 | 要向模型展示的工具集资源名 |
| `allowed_tools` | 可选 | 工具白名单；未指定时不增加此限制 |
| `allow_run_adf` | `false` | 是否允许 Run 内动态 ADF 工具 |

启动输入可以是非空字符串，也可以是 `{input, attachments?, prior_messages?}`；对象输入必须有文本或附件。节点组装 system、历史和本轮用户消息；工具批次完成后，将 assistant/tool 消息追加到上下文，生成下一次模型请求。

状态保存 `messages` 和 `round`。每轮固定一份工具快照，后续校验使用相同 revision/digest。轮数耗尽发布 `context.failed.v1`，应连接到 Model 转为运行失败；不会额外执行总结调用。Context 当前不自动压缩上下文。

## Model：`agent.model`

输入：`model.requested.v1`、`context.failed.v1`。

| 配置 | 必需 / 默认 | 含义 |
| --- | --- | --- |
| `model` | 必需 | Model 资源名，例如 `primary`，不是 API 密钥或 URL |
| `model_name` | 默认使用 `model` 的值 | 传给 Provider 的实际模型名称 |
| `timeout_ms` | 必需，1–86400000 | 单次模型调用超时 |
| `max_output_tokens` | 可选，正整数 | 单次输出 token 上限 |

| 输出事件 | 含义 |
| --- | --- |
| `model.output.v1` | 本轮文本输出事实 |
| `stop.completed.v1` | 最终文本，可由 lifecycle 映射为 Run 完成 |
| `stop.tool_calls.v1` | 工具调用批次，交给 Gate；不是 Run 终态 |
| `stop.failed.v1` | 模型错误、超时、异常停止或 Context 失败 |
| `stop.cancelled.v1` | 声明的业务取消协议；宿主取消不保证产生此事件 |

模型流片段通过公开输出日志向聊天接口发送；图事件用于节点协作。宿主强制取消由引擎提交终态并发送 `engine.terminal.v1`。

模型用量通过 `GraphOutput::with_usage` 提交。`GraphRun.usage` 为累计值，`step_usage` 记录各次节点提交的本次及累计值；未收到用量时可为 null。不要用累计生成 token 直接等同于当前上下文长度，二者是不同指标。

## Gate：`agent.gate`

配置为 `{}`，不依赖模型或工具。输入：`gate.open.v1`、`stop.tool_calls.v1`、`gate.result.v1`；输出：`gate.dispatch.v1`、`gate.completed.v1`。

批次协议：

```json
{
  "batch_id": "batch-1",
  "context": {"purpose": "example"},
  "items": [
    {"id": "a", "payload": {"task": "first"}},
    {"id": "b", "payload": {"task": "second"}}
  ]
}
```

1. 收到 open 后，每个 item 产生一条 dispatch，携带 `batch_id`、`id`、`payload` 和批次 context。
2. 分支返回 `gate.result.v1`，包含 `{batch_id, id, payload}`。
3. 全部结果到齐，产生 `{batch_id, context, results:[{id,payload}]}`，结果保持 items 原始顺序。

每批 1–128 项，item ID 必须唯一。同内容重复打开不再分发，不同内容复用 batch ID 会失败；重复结果采用第一份，未知批次/分支的结果被忽略。先 open 再执行分支，不缓存提前到达的结果。

Gate 可以汇总工具、审批或其他业务结果，但不判断审批是否通过。它也不会按 payload 自动路由到不同工作节点；混合业务需要设计明确的事件协议和订阅。状态保存批次定义、已收结果和完成标记。

## ToolPolicy：`agent.tool_policy`

输入：`gate.dispatch.v1`、`approval.decided.v1`、`approval.expire.v1`。
输出：`tool.authorized.v1`、`approval.requested.v1`、`approval.finished.v1`、`gate.result.v1`。

| 配置 | 必需 / 默认 | 含义 |
| --- | --- | --- |
| `toolset` | 必需 | 工具集资源名 |
| `approval_timeout_ms` | 必需，1–86400000 | 审批有效时间 |
| `review_level` | `50`，范围 0–100 | 工具风险分达到此值时需审批 |
| `allowed_tools` | 可选 | 工具白名单 |
| `allow_run_adf` | `false` | 允许 Run 内动态工具 |

节点校验参数、白名单和工具快照，将授权绑定到具体调用。无需审批或审批通过时发布 authorized；拒绝、到期或校验失败作为 Gate 结果返回。节点本身不执行工具，也不等待 UI。

风险值为 Low=10、Medium=50、High=90；阈值 0 表示全部审核，100 表示关闭人工审核。宿主应通过 `GraphHost.approve` 或 `AgentEngine.approve` 提交可信用户决定，并负责推进到期处理；仅配置节点不意味着具备审批 HTTP 接口。完整载荷和接入流程见[审批与外部事件入口](static-agent-graph.md#17-工具审批和外部事件入口)。

## Tools：`agent.tools`

输入：`gate.dispatch.v1`、`tool.authorized.v1`；输出：`gate.result.v1`、`wait.requested.v1`。

| 配置 | 必需 / 默认 | 含义 |
| --- | --- | --- |
| `toolset` | 必需 | 可执行的工具集资源名 |
| `timeout_ms` | 必需，1–86400000 | 工具执行超时 |
| `allowed_tools` | 可选 | 执行端白名单 |
| `review_level` | `50` | 执行端审批阈值，0–100 |
| `allow_run_adf` | `false` | 允许动态工具 |
| `allow_suspend` | `false` | 允许工具返回外部等待 |
| `parallel` | `false` | 启用符合条件的工具批次并行 |

执行前重新验证调用和授权；直接订阅 dispatch 也不能绕过授权要求。推荐使用完整的 Gate → ToolPolicy → Tools 路径。Context、ToolPolicy 和 Tools 应引用同一个工具集，并保持白名单、ADF 和审批设置一致。

工具结果返回 `{ok, output}` 等业务载荷，失败同样参与 Gate 汇总并返回模型。工具返回 suspension 且允许挂起时，改为发布 wait.requested，由 Wait 负责完成结果。

自动重试受工具声明约束：只有 ReadOnly/Idempotent 工具且有有限重试策略时才重试。并行要求整个调用批次均无需审批、立即完成并声明 ParallelSafe，最多 16 路；其他批次顺序执行。该节点不保存业务状态，执行安全依赖持久化授权、工具声明与运行时恢复策略。

`generate_image` 是 Extension 提供的工具，通过 Tools 执行，不是新的图节点。Demo 将其注册到 workspace 工具集；配置与图片读取见 [Demo 图片生成](../../14-agent-chat-demo.md#图片生成)。

## Wait：`agent.wait`

配置：`wait_service`，必需，指定宿主注入的 `GraphWaitBridge` 资源，例如 `durable`。

输入：`wait.requested.v1`、`wait.woken.v1`、`engine.terminal.v1`。
输出：`gate.result.v1`、`wait.registered.v1`、`wait.completed.v1`。

- 注册外部等待成功后才发布 registered；公开 `run_waiting` 由此投影，避免通知早于订阅就绪。
- 唤醒必须来自宿主外部入口，并匹配持久化投递记录。验证、清理后发布 completed 和 Gate 结果；公开 `run_resumed` 由 completed 投影。
- Run 终态后清理等待、订阅等资源；清理中断可恢复，不重新执行原工具。

订阅通常需要 Tools → Wait 的 wait.requested、`$external` → Wait 的 wait.woken、Wait → Gate 的 gate.result，以及 `$engine` → Wait 的 engine.terminal。工具节点同时启用 `allow_suspend`。完整可执行配置与宿主 Event/Job 装配见[外部等待与代码构建](static-agent-graph.md#18-外部等待与代码构建)。

## 自定义节点与当前边界

完整实现、注册工厂和 YAML 接线示例见 [注册自定义 Graph 节点](custom-graph-nodes.md)。

自定义节点实现 `GraphOperator`，在 Registry 注册 `OperatorDescriptor` 和工厂，再由图引用 type/version。Descriptor 声明配置 schema、输入输出事件 schema、资源需求和重放能力；`handle` 接收事件与状态，返回新状态和输出事件，必要时附带本次 token 用量。状态、输出、用量与消费确认由引擎事务提交。

当前没有内置 Compactor、Router、Coordinator 或 ToolJoin 节点。批次汇总由通用 Gate 完成；压缩需要自定义节点，可插入 Context → Model 之间处理模型请求。图中写入未注册类型不会自动生成实现。

实现依据：[节点注册与 Model/Tools](../../../crates/core/src/agent/graph/builtins.rs)、[Context/Gate](../../../crates/core/src/agent/graph/builtins/flow.rs)、[ToolPolicy](../../../crates/core/src/agent/graph/builtins/policy.rs)、[Wait](../../../crates/core/src/agent/graph/waits.rs)。
