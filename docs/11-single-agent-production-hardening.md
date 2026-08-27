# 单 Agent 生产化边界

> 状态：单 Agent 的 Tool 执行策略和非阻塞观测主链已实现。强 Sandbox 明确保留为 `agent-extension` adapter TODO，不阻塞本阶段封版；具体 OTLP/Langfuse 网络 adapter 可在现有 exporter port 上独立增加。

## 1. Process Sandbox

`ProcessSandbox` 是独立基础设施端口。契约位于 `agent-core::sandbox`，具体 adapter 位于 `agent-extension::sandbox`：

```text
AgentLoop -> shell_command/exec_command/write_stdin -> agent-core Sandbox contract
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

稳定 category 是 `invalid_request / not_found / permission_denied / conflict / resource_exhausted / timeout / cancelled / unavailable / internal / unknown`。`retryable` 只表达是否值得重试，不授权 Agent 自动重放副作用。`ToolDefinition.execution` 额外声明 `idempotency/concurrency/completion/retry`；Registry 拒绝“未知副作用却配置自动重试”及“可挂起却声明 parallel-safe”的组合。只有 `read_only/idempotent` Tool 会按 1–5 次与有界指数 backoff 自动重试。超时、取消、审批拒绝、schema 校验和 Sandbox 错误均进入同一失败投影。

Host 可配置 `tool_call_strategy = "parallel-safe"`。只有 Low risk、无需审批、`immediate + parallel_safe` 的连续调用会重叠；Exclusive/MaySuspend/需审批 Tool 保持串行。完成事件可以按实际完成时间出现，但 Tool result 写回模型时恢复原调用顺序。耐久 `AgentMachine` 与兼容 `Agent::run` 使用相同策略。

## 3. Observability hooks

可观测性使用端口装饰器，不把 Langfuse SDK 放进 Harness：

```text
OpenAiCompatibleProvider -> ObservedModel -> ModelPort
ToolRegistry             -> ObservedTools -> ToolPort
AgentMachine             -> ObservedMachine
                                      │
                                      v
                              ObservationHook
                                ├─ TracingObservationHook
                                ├─ OpenTelemetry exporter
                                ├─ Langfuse exporter
                                └─ test recorder
```

当前事件覆盖 model start/first-token/finish、TTFT、Token usage、每次 Tool attempt 的 latency/error/retry 元数据、审批 requested/resolved，以及 durable activation 的 start/continue/suspend/complete/fail、wait/effect 数量。`run_id` 是 trace correlation id，model invocation、每次 Tool attempt、approval 和 activation 有独立 observation id。

默认不采集 prompt、reasoning、工具参数、工具输出、HTTP body、API Key 或内部 error chain。Hook 是同步且无返回值的，panic 会在 middleware 边界隔离。

`AsyncObservationHook` 已实现 Host 侧有界队列、batch、周期/显式 flush、有限重试、队列/最终导出失败丢弃计数和 shutdown flush。`record()` 只做 `try_send`，队列满或 worker 不可用立即丢弃，绝不等待网络。`ObservationExporter` 是 OTLP、Langfuse 或自建 collector 的 adapter 契约；`HookObservationExporter` 可把现有 tracing sink 搬到异步 worker。Server 和 CLI 已使用该组合，监控故障不能改变 Agent 结果或 latency。

`TracingObservationHook`、队列和 exporter port 通过 `agent-extension::observability` 暴露。具体 Langfuse/OTLP adapter 只负责认证和 wire mapping；批量、重试、flush 已由通用 worker 提供。数据保留、内容采样和成本表仍属于 Host policy，不属于 AgentLoop。

## 4. 封版边界与后置项

单 Agent封版不再缺 Core 状态机能力。以下均是可独立交付的 Extension/Host 项：

1. Linux `BubblewrapProcessSandbox` 或 OCI/gVisor adapter：只读 root、workspace capability、默认断网和资源预算；
2. 具体 OTLP/Langfuse HTTP exporter 与测试环境 wire mapping；
3. 在现有 observation events 上做 Prometheus/metrics 聚合与模型价格表驱动的 cost；
4. ADF durable artifact store/promotion 与 Python/Shell 强 Sandbox profile。

其中第 1 项是大工程且已经划到外部 adapter，本阶段明确保留 TODO。任何生产 profile 在没有强 adapter 时都必须展示 `isolation=none` 并 fail closed，不得把 `HostProcessSandbox` 宣称为强隔离。
