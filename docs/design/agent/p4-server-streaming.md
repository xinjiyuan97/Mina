# P4 Server、统一输出与最终验收

后续灰度、版本隔离、故障/资源边界与保留接口见 [P5 运维记录](./p5-rollout-recovery-operations.md)。
本文的测试数字与“本轮未实现”均是 P4 历史状态，不代表 P5 的实现或门禁结果。

日期：2026-10-23。基线 HEAD `5f9c965`，分支 `feat/event-driven-operator-pipeline`。保留 P0–P3 和既有 P4 成果；没有 commit、push 或创建分支。

## 1. 显式启用与恢复

- `MINA_ENGINE_PROFILE` 未设置或为 `legacy` 时保持 legacy。
- `operator_v1_text` 保留 P3 text-only 限制。
- `operator_v1_tools` 显式启用完整 reducer effects；未知值启动失败，不隐式降级。
- ContextRequest 锁定 execution_profile；旧请求缺少该字段时仍按原 engine 解释（旧 operator 为 text）。Run manifest 同时锁定 engine/profile/projection version。恢复按持久化请求和 manifest，不按当前环境变量重新选择。
- Server 使用原 ModelPort/ToolPort adapter，但仅 ModelOperator 调模型、ToolOperator 调工具；没有在 HTTP、Coordinator 或 Output 中执行它们。
- Context 失败也写入 tools Run 失败终态；不借道 text runtime 或 legacy。
- 原 HTTP 路由提取为 app_router，生产与集成 fixture 使用同一 Router；main.rs 只增加组合装配。

## 2. 事件链与事务边界

```text
Context completed → toolset requested/completed → model.requested
  → model.started → transient deltas → output.batch
  → ModelFinal artifact → model.completed/failed → output.sealed/failed
  → Coordinator / AgentLoopReducer
  → tool.validation.requested/completed/failed
  → approval.requested → Gateway approval.resolved（或 expired/cancelled）
  → tool.requested → tool.completed/failed/suspended/cancelled/uncertain
  → 原序 join → 下一轮 toolset/model → output.sealed → run.completed

tool.suspended → durable wait + job/timer requests
  → native JobRuntime / EventRuntime → durable completion fact
  → wait.resolved → Coordinator ToolResumed → 下一轮 model
```

- Coordinator 使用 Fabric TransactMany 原子写 reducer checkpoint、effect request、outbox、输入 receipt 和 ACK。只有它决定 pending effects、下一轮模型与终态。
- ToolSet/Validator 锁定 revision/digest、schema、allowed_tools、ADF grant、risk 和执行策略。approval 绑定 call/effect、参数与 revision 摘要；旧批准不能授权改变后的调用。
- ToolOperator 先保存 execution intent，再执行 ToolPort；确认结果、receipt/outbox/ACK 原子提交。跨外部副作用不宣称全局 exactly-once。
- Native Job/Timer store 与 Fabric 不是一个跨端口事务；稳定 ID、请求内容校验、持久化绑定和重投 receipt 关闭 publish-before-ACK 空窗。
- 公共 RunStore 与 Fabric 之间采用 durable projection：Run checkpoint 中稳定数字 seq 的事件可重复 append，内容冲突不覆盖。没有双写 legacy execution。

## 3. P3 数据面复用

- tools 和 text 使用同一 bounded_deltas、DeltaItem、OutputBuffer、batch_end、batch 窗口、coverage、OutputBatch、ModelFinal/FinalRef、seal 契约及 RunEventProjector append。
- tools 增加 Control channel 编码非终态模型元事件，保留 provider tools/usage/reasoning 生命周期；不合并相邻 Control JSON。text profile 显式拒绝 ToolCall/Control，不悄悄扩展 text-only 能力。
- Model/Output 并发消费；模型等待输出 drained 后才提交 model receipt/ACK，避免 Output 的 guarded batch 写入丢失有效 lease。
- Coordinator 在 batch 到达时通过现有 reducer 转换公共事件，包括原工具参数脱敏；output_through 与 reducer/receipt 一起提交，重复 batch 不重复 SSE。
- 模型 terminal 只在 artifact、已投影前缀、coverage 和 seal 一致后进入 reducer。坏 final 产生 durable output.failed，不能成功结束未 seal 的输出。
- Server 使用既有 FilesystemBlobStore；默认 Harness 使用 FabricBlobStore 的分块 artifact adapter，manifest 最后发布，真实 reopen 后仍能读取。

## 4. 并发、suspend 与控制

- 沿用 AgentLoopReducer 的批次与 call order：parallel-safe 工具可真实 overlap；逆序完成仍按模型原序 join；exclusive/MaySuspend 保持原约束。
- suspended reducer、wait specs、native effects 与 outbox 在同一事务落盘；不持有执行中的 Tool worker/lease 等待外部事件。Server 的异步恢复/投影任务可以空闲轮询，不是把 Tool future 留在 worker 上等待。
- native notification 只发布事实；Bridge 不调用 ModelPort。Wake 校验 Run、subscription、event、correlation/filter 与时间窗，首个 wake 胜出；重复/迟到消息不产生第二次 resume。
- HTTP cancel/approval 只发布服务端 `gateway:http` actor 的 durable intent/fact，不直接 dispatch reducer 或调用 Tool/Model；actor 是服务身份，不是新增终端用户认证体系。
- Run 的绝对 deadline 保存在 Fabric checkpoint；恢复读取原 deadline，不把剩余时间重置。控制任务扫描持久 deadline，原子写 fired marker 与 timeout outbox，再由 Coordinator 处理。模型/工具仍保留各自 timeout。
- timeout/cancel/success 由 Run checkpoint CAS 决定首个有效提交；失败/取消会发布子 model/tool/job/timer cancel intent，晚到结果只留审计，不能推进已终态 Run。
- 非幂等未确认执行/超时产生 operator.execution.uncertain，不自动重试；幂等工具仅按声明预算重试。取消不能证明外部世界中已开始的副作用被撤销。
- `RunControl` 定义并测试 revision-fenced pause/resume/cancel/terminal 状态机；**没有公共 pause/resume API，也没有启用 runtime pause 调度**。是否在 P5 暴露需另行决定；本轮未实施该 P5 工作。

## 5. HTTP/SSE 兼容证据

`apps/server/src/p4_tools_tests.rs` 使用真实 TCP axum Router、reqwest、fake Model/Tool、FilesystemBlobStore 和真实 SQLite：

1. tools model→approval（同决议重复 HTTP）→tool→next model→terminal。
2. MaySuspend→external fact→wait/resume→next model→terminal。
3. fake provider 在最终 delta 后等待 Notify；只有客户端看到 live `done` 才放行 Completed，排除先缓冲到终态再伪装 streaming。
4. 断言 SSE id 是数字且等于 seq，event name 与 payload type 一致，终态最后。
5. abort 并 await HTTP/Context/tools tasks、释放 adapter 后真实 reopen；把配置改为 legacy，持久 tools Run 不重选 engine。
6. 对每个 Last-Event-ID 截断点请求原 replay endpoint，逐事件等于 live 后缀。
7. 在 pending approval 和 pending wait 时真实重启，恢复不重执行第一轮模型；停掉 driver 后 HTTP cancel 只改变 durable intent，不直接改变 reducer，之后显式 tick 才取消。

另有 `p4_approval_tests` 验证 operator approval 不唤醒 legacy broker；原 P0 fixtures、P2 admission 和 P3 text SSE fixture 均回归通过。

## 6. RED→GREEN 与恢复矩阵

| 边界 | 测试/证据 |
| --- | --- |
| streaming 不能等 final | p4_streaming；RED 为 provider 阻塞时无 durable output，GREEN 为 batch 实时投影 |
| final coverage 损坏 | p4_streaming；RED 为 output_gap 返回导致 Run 不推进，GREEN 为 durable output.failed 和失败终态 |
| Server profile/Router | p4_tools_tests；缺 API 的编译 RED 后接线并完成真实 HTTP fixture |
| Context 失败 | p4_tools_tests；RED 无 durable snapshot，GREEN tools-only 失败终态、零模型/工具调用 |
| deadline / pause | 缺控制 API/状态类型编译 RED，新增真实 reopen deadline 与 revision/terminal 状态测试后 GREEN |
| validation / approval | p4_tool_preparation、p4_authorization_reopen、p4_approval_ingress |
| tool execution / uncertain / retry | p4_tool_operator，包含副作用成功后 receipt 前崩溃、撤销旧 allow、过期 lease、迟到成功 |
| join / next-model | p4_effect_pipeline，每个 tick 释放并 reopen SQLite；Barrier 验证 overlap 和逆序返回 |
| wait / job / timer | p4_wait_pipeline，包含 notification publish 后 ACK 前崩溃、取消后已 claim 通知重投 |
| batch 后 final 前 | p4_streaming，确认 durable prefix 后 abort/await，真实 reopen，保留前缀、禁止重调 provider、失败终态 |
| blocked model / deadline | p4_streaming，provider Notify 阻塞时控制面仍终止 Run，reopen 不重启 provider |
| success / late timeout | p4_streaming，成功终态之后 deadline 不改写公共事件 |

测试没有真实上游或账号调用，不用 sleep 猜执行顺序，不用 SQL 调用次数替代事务/恢复证明。

RED 日志：`/tmp/mina-p4-streaming-red.log`、`/tmp/mina-p4-output-gap-red.log`、`/tmp/mina-p4-server-red.log`、`/tmp/mina-p4-context-failure-red.log`、`/tmp/mina-p4-deadline-red.log`、`/tmp/mina-p4-pause-red.log`。既有授权/wait 增量证据见相邻 P4 文档。

## 7. 最终门禁

- `cargo fmt --all -- --check`：通过。
- P4 Harness 定向目标：34 passed；Core 8、Extension 5、Server P4 5，均包含在完整四包回归。
- `cargo test -p agent-core -p agent-harness -p agent-extension -p mina-server`：328 passed，0 failed，含 doc-tests。
- `bash scripts/verify.sh`：通过，包含 agent-chat 构建/回归和 16 个 SSE frame fixture。
- `cargo clippy --workspace --all-targets -- -D warnings`：通过。
- `git diff --check`：通过。
- 范围检查：Coordinator/Model/Output 无 ToolPort 或 call/call_at；operator 路径唯一工具执行为 ToolOperator.call_at；Wait bridge 无 ModelPort/stream。legacy ToolPort 路径保留，不在本轮删除。

完整日志：`/tmp/mina-p4-final-four-packages.log`、`/tmp/mina-p4-final-verify.log`、`/tmp/mina-p4-final-clippy.log`。Server 定向日志 `/tmp/mina-p4-final-server.log`，流式崩溃定向日志 `/tmp/mina-p4-stream-crash.log`。

## 8. 范围与限制

- 默认仍 legacy，只有显式 tools profile；没有百分比灰度、SLO 压测、历史 GC 或旧 checkpoint reader/legacy machine 删除。
- 没有 Kafka/NATS/Redis，没有真实模型、账号或 API 探测；测试中的 HTTP 都是本机 fake fixture。
- 不承诺未知外部副作用的 exactly-once；uncertain 需宿主/用户处理，取消不等于撤销外部副作用。
- ArtifactStore 与 Fabric 不作跨存储事务；artifact 先写、durable reference 后发布。失败留下的未引用 artifact 本轮不 GC。
- 网关 actor 使用现有服务信任边界；没有新增用户认证。pause/resume 只到纯状态机定义与测试，不声称开放公共能力。
- 本地 deadline/control polling 是持久事实的执行适配，不是唯一事实源；停机期间等待，重启扫描继续推进。没有新增运行时灰度或性能承诺。

## 9. 修改文件（含保留的前序 P4 成果）

- `Cargo.lock`

### Core

- `crates/core/src/context_operator.rs`
- `crates/core/src/effect_pipeline.rs`
- `crates/core/src/event_runtime.rs`
- `crates/core/src/harness/agent_loop.rs`
- `crates/core/src/harness/agent_loop/native_reducer.rs`
- `crates/core/src/harness/agent_loop/reducer.rs`
- `crates/core/src/harness/mod.rs`
- `crates/core/src/lib.rs`
- `crates/core/src/operator_fabric.rs`
- `crates/core/src/pipeline.rs`
- `crates/core/src/tool_authorization.rs`
- `crates/core/src/tool_operator.rs`
- `crates/core/src/tool_preparation.rs`
- `crates/core/tests/p4_operator_contracts.rs`
- `crates/core/tests/p4_tool_authorization.rs`

### Harness

- `crates/harness/src/approval_operator.rs`
- `crates/harness/src/context_operator.rs`
- `crates/harness/src/event_runtime.rs`
- `crates/harness/src/job_runtime.rs`
- `crates/harness/src/lib.rs`
- `crates/harness/src/operator_fabric.rs`
- `crates/harness/src/pipeline/coordinator.rs`
- `crates/harness/src/pipeline/effect_coordinator.rs`
- `crates/harness/src/pipeline/effect_model.rs`
- `crates/harness/src/pipeline/effect_output.rs`
- `crates/harness/src/pipeline/effect_wait.rs`
- `crates/harness/src/pipeline/mod.rs`
- `crates/harness/src/pipeline/output.rs`
- `crates/harness/src/pipeline/projector.rs`
- `crates/harness/src/pipeline/tools_runtime.rs`
- `crates/harness/src/tool_operator.rs`
- `crates/harness/src/tool_preparation.rs`
- `crates/harness/tests/context_operator.rs`
- `crates/harness/tests/p4_approval_ingress.rs`
- `crates/harness/tests/p4_authorization_reopen.rs`
- `crates/harness/tests/p4_effect_pipeline.rs`
- `crates/harness/tests/p4_streaming.rs`
- `crates/harness/tests/p4_tool_operator.rs`
- `crates/harness/tests/p4_tool_preparation.rs`
- `crates/harness/tests/p4_wait_pipeline.rs`

### Extension

- `crates/extension/Cargo.toml`
- `crates/extension/src/operator_fabric.rs`
- `crates/extension/src/pipeline.rs`
- `crates/extension/src/pipeline/artifacts.rs`
- `crates/extension/src/store/event.rs`
- `crates/extension/src/store/operator.rs`
- `crates/extension/tests/p4_checkpoint_transaction.rs`
- `crates/extension/tests/p4_wait_cursor.rs`

### Server

- `apps/server/Cargo.toml`
- `apps/server/src/admission.rs`
- `apps/server/src/main.rs`
- `apps/server/src/p4_approval_tests.rs`
- `apps/server/src/p4_tools_tests.rs`
- `apps/server/src/pipeline.rs`
- `apps/server/src/tools_pipeline.rs`

### 设计文档

- `docs/design/agent/p4-authorization-foundation.md`
- `docs/design/agent/p4-durable-waits.md`
- `docs/design/agent/p4-reducer-effects.md`
- `docs/design/agent/p4-server-streaming.md`
