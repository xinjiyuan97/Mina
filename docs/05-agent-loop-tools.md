# Mina Agent Loop 与工具协议

> 状态：MVP 已实现。当前支持 OpenAI-compatible 流式 function calling、串行工具执行、风险分级、用户审批、工具 UI 事件和有界模型循环。

## 1. 边界

工具循环属于 Agent，而不是 Gateway 或 provider adapter：

```text
RunRequest
  → AgentLoop
      → ModelPort.stream(ModelRequest + ToolDefinition[])
      ← ModelEvent::ToolCall*
      → ToolPort.call(ToolCallRequest)
      ← ToolOutput | ToolError
      → ModelPort.stream(messages + tool result)
      ← final model output
  → AgentEvent
  → Harness RunEvent
```

- `ModelPort` 归一化不同模型服务的 tool-call wire format；
- `AgentLoop` 聚合参数、控制步数、执行工具并维护本次 run 的临时消息历史；
- `ToolPort` 是工具能力边界，可以由内置 registry、MCP bridge 或远程工具主机实现；
- Harness 继续负责 `run_id`、连续 `seq` 和唯一 run 终态；
- Gateway 与 Web 只传输、展示工具事件，不执行工具策略。

一次模型调用的 `Completed { finish_reason: tool_call }` 只是该次 `ModelEventStream` 的终态，不是整个 run 的终态。整个 Agent 流只有最终的 `Completed` 或 `Failed` 才会变成 `run_completed` 或 `run_failed`。

## 2. ToolPort

核心契约：

```rust
trait ToolPort {
    fn definitions(&self) -> Vec<ToolDefinition>;
    fn validate(&self, name: &str, arguments: &Value) -> Result<(), ToolError>;
    fn call(&self, request: ToolCallRequest) -> ToolCallFuture;
}
```

`ToolDefinition` 的稳定数据结构位于 `agent-core::tool`，包含稳定名称、给模型看的描述、JSON Schema 和 `risk_level`。`ToolCallRequest` 是进程内运行时类型，包含 `run_id`、上游 `call_id`、工具名称、已解析的 JSON 参数和本次 run 的 cancellation token。结果只能返回安全文本或结构化 `ToolError`；原始内部错误不能直接进入公共事件。

内置 `ToolRegistry` 实现 `ToolPort`。每个具体工具实现 `Tool`，注册时编译 JSON Schema，并拒绝空名称、重复名称或非法 schema；调用前统一完成工具查找和参数校验，再分派到工具实现。未来的 MCP bridge 或远程工具主机仍可直接实现同一个 `ToolPort`。

具体工具实现位于 `agent-extension::tool`，server 通过 `BuiltinToolCatalog` 组合，当前注册：

```text
get_current_time({})
→ {"timezone":"UTC","iso8601":"...","unix_seconds":...}

read({"path":"README.md"})
list_directory({"path":"crates"})
search({"query":"ToolDefinition","limit":20})
write({"path":"notes.txt","content":"complete file content"})
edit({"path":"notes.txt","old_text":"old","new_text":"new"})
run_command({"program":"cargo","args":["test","--workspace"]})
```

文件工具只允许解析 workspace 内的相对路径，并在 canonicalize 后再次检查边界；密钥配置、`.env`、私钥与 `.git` 路径由 Host policy 默认拒绝。`write/edit` 属于 Medium，必须审批。`run_command` 不启动 shell，并通过 Host 注入的 `ProcessSandbox` 执行；当前 Host adapter 只固定工作目录、清理环境和限制输出，不等于内核隔离。它仍属于 High，server 必须显式启用，Agent Loop 还必须等待用户审批。工具名称经过 registry allow-list 分派；未知工具和非法参数都作为工具失败返回模型，使模型有机会修正或解释。

风险由工具作者声明，模型不能通过参数修改：

| 风险 | 默认行为 |
|---|---|
| `low` | schema 校验后自动执行 |
| `medium` | 等待用户审批 |
| `high` | 等待用户审批 |

Agent 在请求审批前调用 `ToolPort::validate`，避免让用户批准一个随后必然因 schema 或 allow-list 失败的调用。审批由 `ApprovalPort` 提供；未安装 adapter 时使用安全默认实现，拒绝所有需要审批的调用。

## 3. OpenAI-compatible 映射

请求将工具映射为 Chat Completions function tools：

```json
{
  "tools": [
    {
      "type": "function",
      "function": {
        "name": "get_current_time",
        "description": "...",
        "parameters": {
          "type": "object",
          "properties": {},
          "additionalProperties": false
        }
      }
    }
  ]
}
```

流式响应中的以下字段会被归一化：

```text
choices[].delta.tool_calls[].index
choices[].delta.tool_calls[].id
choices[].delta.tool_calls[].function.name
choices[].delta.tool_calls[].function.arguments
```

`arguments` 是分片字符串。adapter 按 `index` 关联分片，产生：

```text
ModelEvent::ToolCallStarted { call_id, name }
ModelEvent::ToolCallArgumentsDelta { call_id, delta }
```

Agent 按 `call_id` 聚合完整字符串，只在模型以 `finish_reason = tool_calls` 结束后解析 JSON。缺少 id/name、调用中途改变 id/name、参数先于 start 或非 tool-call 终止原因携带调用，都会成为不可重试的协议错误。

工具执行后，下一次模型请求会包含：

1. assistant 消息及完整 `tool_calls`；
2. 对应 `role = tool`、`tool_call_id` 和工具结果；
3. 若兼容模型产生过 thinking，assistant 消息还会带回 `reasoning_content`。

## 4. 公共 RunEvent

Agent Loop 增加以下非终态事件：

| 事件 | 关键字段 | 含义 |
|---|---|---|
| `tool_call_started` | `call_id`, `name` | 模型开始请求工具 |
| `tool_call_arguments_delta` | `call_id`, `delta` | JSON 参数字符串增量 |
| `approval_requested` | `approval_id`, `call_id`, `tool_name`, `risk_level`, `arguments` | Agent 等待用户决定 |
| `approval_resolved` | `approval_id`, `call_id`, `resolution` | 用户已批准或拒绝 |
| `tool_execution_started` | `call_id`, `arguments` | 参数已解析，准备执行 |
| `tool_execution_completed` | `call_id`, `output` | 工具成功 |
| `tool_execution_failed` | `call_id`, `code`, `message`, `retryable` | 工具失败 |

前端分别映射为 ChatUIComponent 的：

```text
tool-input-start
tool-input-delta
tool-input-available
permission-request | permission-resolved
tool-executing
tool-output | tool-error
```

thinking、工具调用、工具结果和最终文本因此是同一 assistant message 中相互独立的结构化 parts。

## 5. 状态机与限制

当前状态循环：

```text
WaitingModel
  ├─ final output ───────────────→ Completed
  ├─ model failure ──────────────→ Failed
  └─ tool_calls
       → ParsingArguments
       → ValidatingArguments
       → WaitingApproval (medium/high)
       → ExecutingTool (串行)
       → AppendingToolResult
       └─────────────────────────→ WaitingModel
```

`agent.max_steps` 限制一次 run 中的模型调用次数，默认 8，合法范围 1–64。达到限制时产生唯一的 `agent_step_limit_exceeded` 终态。在最后一个可用 step 上模型再次请求工具时，不再执行该工具，避免执行副作用后却没有机会让模型消费结果。

三个独立 deadline 通过 `[agent]` 配置：完整 run 默认 300 秒、每轮模型调用默认 120 秒、每次工具执行默认 30 秒。模型超时以 `model_timeout` 终止 run；工具超时以可重试的 `tool_timeout` 结果送回模型，让它决定降级或解释；完整 run 超时产生 `run_timeout`。显式取消则产生唯一 `run_cancelled`。

当前 Gateway 使用进程内 approval registry，并通过以下接口接收决定：

```text
POST /api/v1/runs/{run_id}/approvals/{approval_id}
{ "decision": "allow-once" | "deny", "reason"?: "..." }
```

同一决定可以幂等重放，不同决定返回冲突。run 完成、取消或超时后清理进程内审批等待；SSE 断开不再结束 run。拒绝不会执行工具，而是把 `tool_rejected` 和可选原因送回模型。审批事件及其投影已经持久化，但等待中的 Future 仍是进程内对象；它将在事件运行时阶段替换为持久化 subscription + checkpoint resume。

当前尚未实现：

- 并行工具调度；
- MCP discovery/session；
- checkpoint resume 与可跨进程恢复的审批订阅；单次 run 事件持久化已由 `RunStore` 完成。
- C ABI v1 的 Rust host loader；当前已发布数据契约、JSON Schema 和头文件，loader 将作为隔离且可审计的 FFI adapter 提供。

下一步应让事件运行时接管等待审批的挂起与恢复，再让 MCP adapter 实现同一个 `ToolPort`。MCP 协议不应进入 `AgentLoop` 或公共 `RunEvent`。
