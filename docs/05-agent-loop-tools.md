# Mina Agent Loop 与工具协议

> 状态：单 Agent 主链已实现。当前支持 OpenAI-compatible 流式 function calling、显式 Tool 执行策略、风险审批、有限自动重试、保序并发、耐久挂起/恢复、异步 Job、工具 UI 事件和有界模型循环。

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

`ToolDefinition` 的稳定数据结构位于 `agent-core::tool`，包含稳定名称、给模型看的描述、JSON Schema、`risk_level` 和 `execution`。`ToolCallRequest` 是进程内运行时类型，包含 `run_id`、上游 `call_id`、工具名称、已解析的 JSON 参数和本次 run 的 cancellation token。结果只能返回安全文本、声明式挂起请求或结构化 `ToolError`；原始内部错误不能直接进入公共事件。

`execution` 由 Tool 作者声明、Registry 注册时校验，不能由模型参数修改：

```text
idempotency = unknown | read_only | idempotent
concurrency = exclusive | parallel_safe
completion  = immediate | may_suspend
retry       = max_attempts + bounded exponential backoff
```

`retryable=true` 只是错误提示，不授权自动重放。只有 `read_only/idempotent` 才能配置 2–5 次自动尝试；`unknown` 默认只执行一次。`parallel_safe + may_suspend` 属于非法组合并在注册时拒绝。Host 通过 `[agent].tool_call_strategy = "sequential" | "parallel-safe"` 决定是否启用并发；即使启用，需审批、Exclusive 或可挂起 Tool 仍保持串行。并发完成顺序可以不同，但写回模型的 Tool result 永远按模型原调用顺序排列。

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
shell_command({"command":"cargo test --workspace","timeout_ms":300000})
exec_command({"cmd":"server --watch","yield_time_ms":1000})
write_stdin({"session_id":"<run>:<call>","chars":"reload\n"})
apply_patch({"patch":"*** Begin Patch\n...\n*** End Patch"})
async_job({"kind":"builtin.delay","input":{"delay_ms":1000,"value":{...}}})
```

文件工具只允许解析 workspace 内的相对路径，并在 canonicalize 后再次检查边界；密钥配置、`.env`、私钥与 `.git` 路径由 Host policy 默认拒绝。`write/edit/apply_patch` 属于 Medium，在默认 `review_level = 50` 下需要审批。Mina 的 provider-neutral `ToolDefinition` 当前只支持 JSON function schema，因此 Codex 的 freeform `apply_patch` 在这里适配为必填 `{patch: string}`；解析和写入仍位于 workspace tool，绝不通过 shell 或 process adapter 绕过路径策略。

`shell_command` 是有 wall-time 限制的一次性 shell 调用。`exec_command` 启动可持续会话，初次等待后若进程仍运行就返回 `session_id`；`write_stdin` 用该 id 写字符、空写轮询、关闭 stdin 或终止进程组，并返回本次新增的有界输出。当前实现使用 plain pipes，不宣称 PTY。三个进程工具都通过 Host 注入的同一个 `ProcessSandbox` 实例执行并声明 High；server 必须调用 `enable_terminal_tools()` 显式启用，在默认审批策略下 Agent Loop 会在调用前等待用户决定。

Codex 的 `request_permissions` 承载 attached environment 的 filesystem/network permission profile 和 turn/session grant 生命周期。Mina 当前没有等价的 permission-profile contract；其已有 `ApprovalPort` 已完整覆盖工具执行前的用户决定。因此 catalog 不注册 `request_permissions`，也不创建可绕过审批的旁路。兼容规则是：所有 terminal definition 固定为 High，由统一的 `ToolPort::validate -> ToolApprovalPolicy -> ApprovalPort -> ToolPort::call` 链处理；策略要求审核但未配置 ApprovalPort 时 fail closed。

### 2.1 数值审核阈值

Tool 对外仍声明分类风险，Host 将其映射为稳定数值并像日志级别一样过滤：

| Tool 风险 | 数值 |
|---|---:|
| Low | 10 |
| Medium | 50 |
| High | 90 |

```toml
[approval]
review_level = 50
```

判断规则为 `tool_level >= review_level`。`0` 审核全部 Tool；默认 `50` 审核 Medium/High；`90` 仅审核 High；`100` 关闭 Tool 人工审核。合法范围是 `0..=100`，所以也可以用中间阈值，例如 `60` 与 `90` 的效果相同。阈值属于 Host/Run 执行策略，不进入模型参数；耐久 AgentLoop 会把生效策略写入 checkpoint，重启恢复时不会因配置变化而让同一 Run 的策略漂移。

当前 `HostProcessSandbox` 只固定工作目录、清理环境、限制输出、管理 stdin/进程组和回收直接子进程，不等于内核隔离；其 descriptor 始终为 `host_process / isolation=none`。工具名称经过 registry allow-list 分派；未知工具和非法参数都作为安全的结构化工具失败返回模型，使模型有机会修正或解释，宿主 I/O 错误不会原样泄漏。

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
       → WaitingApproval (tool_level >= review_level)
       → ExecutingTool (Exclusive 串行 / ParallelSafe 有界并发)
       → AppendingToolResult
       └─────────────────────────→ WaitingModel
```

`agent.max_steps` 是 Host 配置的模型步骤上限，默认 8，合法范围 1–100。上层可在单次 Run 请求中传入更小的 `max_steps`；省略时使用 Host 上限，超过 Host 上限或小于 1 时返回 `invalid_max_steps`。

最后一个正常 step 再次请求工具时不会直接让 Run 失败，也不会执行这些工具。AgentLoop 会为每个待执行调用产生 `tool_execution_failed`：错误码为 `agent_step_limit_exceeded`、类别为 `resource_exhausted`，并把同样的结构化 ToolError 追加到模型消息。随后额外进行一次不携带任何 ToolDefinition 的最终总结调用，要求模型使用已有信息回答并说明未完成工作；这次总结调用不计入 `max_steps`。总结正常结束时 Run 仍为 `run_completed`，只有总结调用超时、上游失败或违规继续请求工具时才进入 `run_failed`。

```json
{
  "input": "...",
  "max_steps": 4
}
```

三个独立 deadline 通过 `[agent]` 配置：完整 run 默认 300 秒、每轮模型调用默认 120 秒、每次工具执行默认 30 秒。模型超时以 `model_timeout` 终止 run；工具超时以可重试的 `tool_timeout` 结果送回模型，让它决定降级或解释；完整 run 超时产生 `run_timeout`。显式取消则产生唯一 `run_cancelled`。

Gateway 通过以下接口接收决定：

```text
POST /api/v1/runs/{run_id}/approvals/{approval_id}
{ "decision": "allow-once" | "deny", "reason"?: "..." }
```

同一决定可以幂等重放，不同决定返回冲突。耐久 `AgentMachine` 会把待审批调用写入 versioned checkpoint，并在同一提交中创建 once subscription/outbox；run 进入 `WaitingEvent` 后释放 worker。Server 重启后，HTTP 审批产生 `tool.approval.resolved`，事件写入 run inbox 并恢复原调用。拒绝不会执行工具，而是把 `tool_rejected` 和可选原因送回模型。兼容的 `Agent::run` 仍使用进程内 `ApprovalPort`，不承诺跨重启等待。

普通 Tool 也可以声明 `completion=may_suspend` 并返回 `ToolOutput::suspend(waits, effects)`。Harness 校验 wait/effect 数量、Run ownership、subscription 和 effect 契约，保存 `pending_tool` checkpoint；匹配事件到达后把 inbox 投影为 Tool result，产生 `tool_execution_completed` 并继续模型循环。同一模型轮中尚未执行的其余调用会收到 `tool_deferred_by_suspension`，避免在挂起边界后意外产生副作用。

内置 `async_job` 使用这条通路提交 `StartJob`，以 `job.completed/job.failed` 为 once wait。当前 allow-list 默认只有 `builtin.delay`；审批 → Tool suspend → SQLite reopen → Job execute-once → event → Agent resume → 最终总结已有集成测试。

仍未实现的是 MCP discovery/session、C ABI v1 Rust host loader，以及真正的 Linux/容器强 Sandbox adapter。它们都应继续实现同一个 `ToolPort`/`ProcessSandbox` 边界，不进入 `AgentLoop` 或公共 `RunEvent`。
