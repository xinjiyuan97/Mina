# 单 Agent 生产化边界

> 状态：独立 Sandbox 契约、Tool failure 扩展元数据、Model/Tool Observability middleware 和 tracing adapter 已实现。Bubblewrap/gVisor 等强隔离 adapter 与 Langfuse 网络 exporter 尚未实现。

## 1. Process Sandbox

`ProcessSandbox` 是独立基础设施端口。契约位于 `agent-core::sandbox`，具体 adapter 位于 `agent-extension::sandbox`：

```text
AgentLoop -> run_command -> agent-core Sandbox contract
                              ├─ HostProcessSandbox (development, isolation=none)
                              ├─ Bubblewrap adapter
                              ├─ OCI/gVisor worker
                              └─ microVM worker
```

Sandbox 使用自己的 `SandboxErrorKind`、稳定错误码、取消令牌、descriptor 和 execution output。`agent-extension::tool` 是映射边界，只把安全错误映射为 `ToolError`。未来强隔离 adapter 失败必须 fail closed，不得自动退回 Host adapter。

## 2. Tool failure

普通工具失败不是 Run 失败。AgentLoop 将失败事件持久化并向模型返回统一信封，使模型可以修正参数、换工具或向用户解释：

```json
{
  "ok": false,
  "error": {
    "code": "tool_timeout",
    "category": "timeout",
    "message": "tool execution exceeded the configured timeout",
    "retryable": true,
    "retry_after_ms": null
  }
}
```

稳定 category 是 `invalid_request / not_found / permission_denied / conflict / resource_exhausted / timeout / cancelled / unavailable / internal / unknown`。`retryable` 只表达是否值得重试，不授权 Agent 自动重放副作用；自动重试必须由 Tool policy 明确声明幂等性后再增加。超时、取消、审批拒绝、schema 校验和 Sandbox 错误均进入同一失败投影。

## 3. Observability hooks

可观测性使用端口装饰器，不把 Langfuse SDK 放进 Harness：

```text
OpenAiCompatibleProvider -> ObservedModel -> ModelPort
ToolRegistry             -> ObservedTools -> ToolPort
                                      │
                                      v
                              ObservationHook
                                ├─ TracingObservationHook
                                ├─ OpenTelemetry exporter
                                ├─ Langfuse exporter
                                └─ test recorder
```

当前事件覆盖 model/tool start、finish、duration、finish/error status、Token usage、错误分类和 retry 元数据。`run_id` 是 trace correlation id，model invocation 与 tool call 有独立 observation id。

默认不采集 prompt、reasoning、工具参数、工具输出、HTTP body、API Key 或内部 error chain。Hook 是同步且无返回值的；Exporter 应写入有界队列并异步批量发送。Hook panic 会在 middleware 边界隔离，监控故障不能改变 Agent 结果。

`TracingObservationHook` 已在 Server 和 CLI composition root 接入，并通过 `agent-extension::observability` 暴露。Langfuse 推荐通过 OpenTelemetry/OTLP subscriber 或 Extension 内的专用 `ObservationHook` adapter 导出；认证、批量、重试、flush 和数据保留策略属于 Host，不属于 AgentLoop。

## 4. 下一步

1. 实现 Linux `BubblewrapProcessSandbox`，加入只读 root、workspace capability、默认断网和资源预算。
2. 为 ToolDefinition 增加明确的幂等/重试 policy，再实现有界自动重试。
3. 增加 OTLP exporter crate，并用 Langfuse 测试环境验证 trace/generation/span 映射。
4. 增加 metrics 聚合：run latency、model TTFT、tool latency/error rate、token/cost 和 approval wait。
