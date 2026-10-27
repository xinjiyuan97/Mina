# P4 Durable Wait

> 历史设计说明：本文涉及的固定流水线已移除。当前实现、持久化事件与恢复 API 以[静态图指南](static-agent-graph.md)和[单 Agent API](operator-only-api.md)为准。

日期：2026-10-23。本文记录 Harness/SQLite 实现。后续 Server 集成及完整门禁见 `p4-server-streaming.md`，不缩减原 P4 验收要求。

## 本轮文件

- `crates/core/src/effect_pipeline.rs`
- `crates/core/src/event_runtime.rs`
- `crates/core/src/harness/agent_loop.rs`
- `crates/core/src/harness/agent_loop/native_reducer.rs`
- `crates/core/src/harness/mod.rs`
- `crates/extension/src/store/event.rs`
- `crates/extension/tests/p4_wait_cursor.rs`
- `crates/harness/src/event_runtime.rs`
- `crates/harness/src/job_runtime.rs`
- `crates/harness/src/pipeline/mod.rs`
- `crates/harness/src/pipeline/effect_coordinator.rs`
- `crates/harness/src/pipeline/effect_wait.rs`
- `crates/harness/src/pipeline/tools_runtime.rs`
- `crates/harness/tests/p4_wait_pipeline.rs`
- `docs/design/agent/p4-reducer-effects.md`
- `docs/design/agent/p4-durable-waits.md`

派发前已有的其余 P4 改动保留，没有回滚或重复实现。

## 事件链与职责

```text
tool.suspended
  → Coordinator / AgentLoopReducer::ToolFinished
  → RunWaiting + EffectWaitRequest + subscription bindings + wait.register.requested
  → EffectWaitBridge：先注册 native subscription，再重放 native effect
      job.requested → JobStore.submit → job.started → JobRuntime
      timer.schedule.requested → EventStore.schedule_once → timer.scheduled
      external：等待可信 Gateway / worker event
  → native EventRuntime delivery → bridge 持久化 wake + job result / timer.fired
  → wait.resolved → Coordinator / AgentLoopReducer::ToolResumed
  → RunResumed → reducer 原有下一轮 toolset/model → terminal
```

Bridge 不持有 ModelPort，不调用 Model，不修改 reducer。JobRouter wrapper 只核验 Run wait fence、发布 started 事实并委托既有 JobRouter。等待期间无挂起的 worker future；空闲 runtime tick 返回零。宿主须显式用 `with_wait_ports` 提供持久化端口；缺少端口时报错，不回退 legacy。

`ToolResumed.content` 直接复用 native reducer 的 `resumed_tool_content`，保留既有 `{ok, events}` JSON 结构。wait/resume 使用既有 RunEventKind 与连续数字 seq；后续真实 HTTP fixture 已验证 Server RunStore/SSE live==replay，不以 Harness checkpoint 序列代替 HTTP 证据。

## 事务与恢复边界

1. Coordinator 在 Fabric `TransactMany` 中原子提交 reducer suspended 状态、RunWaiting、不可变 wait request、subscription→Run 绑定、request outbox、源 tool fact 的 receipt/ACK。
2. Bridge 消费 durable request 后，先调用 native subscription，再执行 native job/timer/publish adapter。Native port 与 Fabric **不是跨端口同一个事务**；恢复依靠先落盘的请求和 native 稳定 ID/内容冲突校验。adapter 完成后提交 Fabric receipt/ACK。
3. Native event delivery 先校验持久化 event、event ID/sequence、Run subject、delivery target、source/correlation/filter 与有效时间窗；再用 Fabric CAS 写首个 wake checkpoint 和 outbox。成功返回后才由 EventRuntime ACK native delivery。
4. 在 wake commit 后、native ACK 前崩溃，重投看到既有 wake，不重复恢复。同一 wait 的第一个 wake 胜出；其余事件仍可在 native event ledger 审计。
5. Coordinator 的 Run checkpoint CAS 决定 resume/cancel 谁先推进。已取消 Run 对迟到 wait.resolved 只 ACK，不产生 RunResumed 或下一轮模型。
6. resume/cancel 原子发布 wait.cancel.requested；bridge 清理剩余订阅并发送 job/timer cancel intent。已完成 Job、已 fired Timer 不伪报 cancelled；未确认的 native 操作可重放。注册后再次检查 Run fence，处理与取消并发的清理。

## 注册空窗

普通 MaySuspend 的 `StartPosition::Now` 在 Coordinator 锁定为新增的 `RecordedSince { recorded_at_ms: subscription.created_at_ms }`，该定义随 wait request 持久化。

SQLite 历史匹配与后续 publish 都检查下界。因此通知即使发生在 durable suspension 与 native subscription 注册之间，也能在 reopen 后补投；早于 wait 创建时间的通知不匹配。原 `Now`/`Beginning`/`After` 行为不变。此时间窗依赖 native 可信 ingress 的 recorded_at_ms，以及工具提供正确的 wait 创建时间；source/correlation/Run scope 同时参与绑定，不能仅凭时间匹配。

## 测试证据

- 初始 `p4_wait_pipeline` 编译 RED：缺少 EffectWaitBridge / with_wait_ports；实现后 job/timer/external 三种完整恢复链通过。
- cancel RED：native subscription 仍为 Active；修复 Coordinator cleanup intent 与 bridge native cancellation 后通过。
- 注册空窗 RED：注册前 external fact 导致 Run 永久 Suspended；新增可恢复时间下界后通过。
- Job 在 notification publish 后、ACK 前真实关闭/reopen；lease 到期重新发布通知，不重复执行。该测试额外暴露既有 JobRuntime 重投改变 recorded_at_ms 的内容冲突（RED）；现在读取已持久化事件并沿用首次 recorded_at_ms，其余字段仍由 EventStore 做严格内容比较（GREEN）。
- Timer 在 publish 后、complete_timer 前真实关闭/reopen；lease 到期重投同一事件，只恢复一次。
- native delivery 已 claim 后取消 Run，再 reopen 并回调两次；事实可审计，Run 不恢复。
- `p4_wait_cursor` 检查历史下界、reopen 重复订阅、乱序旧时间戳，并确认旧 Now 不回放历史事件。

所有新增测试使用 fake Model/Tool/Job、实际 SQLite 文件与显式调度时间；没有 sleep 猜测顺序，没有真实账号或上游调用。

## Wait 增量的历史门禁

- `cargo fmt --all -- --check`：通过。
- P4 Harness 定向六个 test target：29 passed；新增 SQLite cursor target：1 passed。
- `cargo test -p agent-core -p agent-harness -p agent-extension -p mina-server`：318 passed（含 doc-tests），0 failed。沙箱首次运行被本机 fake listener bind 限制阻止；获批在沙箱外重跑通过。
- `bash scripts/verify.sh`：通过，含原 SSE frame fixture。
- `cargo clippy --workspace --all-targets -- -D warnings`：通过。
- `git diff --check`：通过。
- 范围搜索：Coordinator/Model/Output/Wait bridge 无 ToolPort 调用，工具调用仍在 ToolOperator；Wait bridge 无 ModelPort 调用。

历史日志：`/tmp/mina-p4-wait-four-packages-final.log`、`/tmp/mina-p4-wait-clippy-final.log`、`/tmp/mina-p4-wait-verify-final.log`、`/tmp/mina-p4-wait-green-final.log`。这些是接入 Server 之前的增量结果；完整结果见 `p4-server-streaming.md`。

## 后续集成

- tools 模型输出已复用 P3 bounded streaming / output.batch / coverage / seal 数据面。
- Server operator_v1_tools profile、manifest 固定、admission/recovery/cancel、native router 分流已接通。
- tools/approval/wait/resume 的真实 HTTP SSE live==replay fixture 包括 SQLite reopen 和每个 Last-Event-ID 截断点。
- Run deadline、暂停状态机定义、取消/注册及流式中断恢复测试见最终集成文档。暂停 API 未开放。

以上均属于 P4；没有开启灰度、压力测试、GC 或删除 legacy。
