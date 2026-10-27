# P4 工具与审批基础

> 历史设计说明：本文涉及的固定流水线已移除。当前实现、持久化事件与恢复 API 以[静态图指南](static-agent-graph.md)和[单 Agent API](operator-only-api.md)为准。

日期：2026-10-23。

本文件记录授权基础与早期 RED→GREEN 证据；Job/Timer/External wait 见 `p4-durable-waits.md`，Server tools profile 和最终验收见 `p4-server-streaming.md`。
权威范围仍以 `event-driven-operator-pipeline.md` 的 P4 迁移表为准。

## 本次基础改动

`agent_core::tool_authorization` 提供无 I/O 的预执行校验和审批绑定数据：

- 重建 ToolSetSnapshot，检查定义、binding、顺序和 digest 的一致性。
- 根据 snapshot 校验工具存在、JSON arguments schema、execution/retry policy。
- risk、completion、concurrency、idempotency、retry 从工具定义读取，不接受模型声明覆盖。
- capability 过滤与现有 AgentLoopReducer 一致：静态工具遵守 allowed_tools；ADF management 同时需要 allow_run_adf 和静态允许；AgentDefined 使用显式 ADF grant。
- toolset revision/digest、call ID/name、arguments digest、执行策略共同形成 binding digest。revision 变化即使 metadata digest 不变，也不能重用旧审批。
- public_arguments 直接复用现有 digest-only 脱敏函数。
- 审批状态携带 run/effect ID、稳定 approval ID、call、请求/过期时间、actor、decision、决议时间。
- 同 decision 的重投返回幂等成功并保留首次 actor/reason/time；不同 decision 冲突。到期、取消、deny、尚未决议均不能通过授权检查。

## 信任与事务边界

这些对象只可来自可信宿主或可信 checkpoint；SHA-256 digest 是一致性绑定，**不是签名或身份认证**。
`BoundApproval` 包含工具私有 arguments，不能直接充当公共 SSE payload。
HTTP approval 入口现在根据持久化 Run manifest 的 engine_version 分流。
operator_v1 仅写入 Gateway 决议事实，不调用 Tool，也不进入 legacy broker。
Gateway actor 为服务端固定的 `gateway:http`，不是客户端自行提交的用户名；没有新增用户认证系统。
Ingress 校验 tenant/run/approval ID；ToolOperator 执行前读取最新 durable approval，而不是相信请求中保存的旧 allow 副本。
`authorize` 是纯检查，不执行工具，也不独立保证 allow-once 的副作用只执行一次。

新增 SQLite 测试使用真实 SqliteEventStore：

1. 在 P1 Commit 中原子保存授权对象 receipt、结果 outbox 和 ACK。
2. 关闭并 reopen，读取原 receipt/outbox/offset，再 dispatch 未发送 outbox。
3. 对未 commit 的 delivery 关闭并 reopen，确认 receipt 缺失、offset 未推进、重投 epoch 增加，旧 lease commit 被拒绝。

上述原有 `p4_authorization_reopen` 测试证明的是授权对象依托 P1 事务恢复，不是 suspended Run 已恢复。
该测试里的 event/schema 是 fixture；新增 `p4_approval_ingress`、`p4_tool_operator` 使用对应组件的事件契约。

## 本轮新增的事务与执行组件

- Fabric 增加 versioned checkpoint CAS、`Transact` 和 `GuardedTransact`。
- `Transact` 可以在同一事务内提交 checkpoint、receipt、outbox、ACK，事件分区序号由事务分配。
- `GuardedTransact` 校验当前 delivery lease 后保存执行意图，但不 ACK，避免外部副作用发生前丢失重投机会。
- SQLite 沿用 P1 同一事务 adapter；缺失 checkpoint 字段的旧 Fabric JSON 可继续读取。
- ToolOperator 仅消费 durable `tool.requested`，先保存 execution claim，再调用 ToolPort。
- 已完成 receipt 的重复投递只补 ACK，不重执行；不同订阅的同一 consumer 也复用 receipt。
- 重启后非幂等未确认执行发布 `operator.execution.uncertain`，不自动重试；幂等恢复受声明的 retry budget 约束。
- 工具结果、结果引用事件、receipt 与 ACK 原子提交；支持 completed/failed/suspended/cancelled。
- 独立工具取消使用相同 execution checkpoint CAS fencing；晚到成功不能覆盖取消，另存 `operator.result.stale` 审计事实。
- 工具 timeout 对非幂等执行产生 uncertain；执行前重查 toolset revision/digest 和 durable approval。
- Approval ingress 原子提交审批状态与 requested/resolved/expired/cancelled outbox；并发相同 request 幂等，不同 decision 只能有一个赢家。
- 过期 resolution 请求会先持久化 expired；取消与过期状态在 reopen 后仍拒绝授权。
- ToolPreparationOperator 消费 `toolset.requested` / `tool.validation.requested`，锁定规范化快照并返回对应 completed/failed 事实；不会执行 ToolPort call。
- Preparation request 使用 tenant/run/effect 派生的不可变 checkpoint；validation 引用同一 Run 的已锁定 ToolSet，检查当前 revision/digest、schema、allowlist、ADF 和宿主 validate_at。
- Preparation result、receipt、outbox 和 ACK 原子提交，公开事实仅携带结果引用，不携带私有参数；过期 lease 不留下结果或 ACK，reopen 后重投恢复。
- Fabric 增加 `ClaimForConsumer`，Context worker 只领取 Context consumer 的订阅；旧无过滤 Claim 保留兼容。工具并行 overlap 与原序 join 另由 `p4_effect_pipeline` 的 Barrier 测试验证。

新增 `ToolsRunCoordinator` / `ToolsPipelineRuntime` 已通过 Fabric 将这些组件串接，继续复用 `AgentLoopReducer`，没有新增业务 reducer。详细边界见 `p4-reducer-effects.md`。
独立工具取消、Harness Run 取消及后续 Server durable cancel ingress 均有测试；完整 suspend/resume 由 wait bridge 与 Coordinator 接通。
ToolOperator 结果提交的 receipt 去重也不意味着外部副作用可以与 SQLite 达成 exactly-once。

## RED → GREEN

- 首次新增定向测试因不存在 `tool_authorization` 模块而编译失败，然后实现模块并通过。
- 加入 AgentDefined 不依赖静态 allowlist 的兼容断言后失败，再与现有 reducer 语义对齐。
- 加入同 decision、不同 reason 的幂等断言后失败，再修正为比较 decision 并保留首次审计信息。
- SQLite 测试未使用 sleep 或 SQL 调用次数断言。
- 新增 ToolOperator 缺失模块、审批 ingress 缺失模块和 checkpoint 事务缺失 API 的 RED，再实现相应组件。
- 使用 Notify 验证取消与在途工具成功竞争；先因缺少 cancel API 失败，再补实现；随后增加 stale audit 断言并修复。
- 撤销审批后的旧 allow 副本曾导致工具执行，RED 测试复现后改为读取 durable approval。
- Barrier 强制并发 request 同时读到不存在的 checkpoint，复现 Fenced 后补幂等重读。
- HTTP 测试先复现 operator_v1 唤醒 legacy broker，再增加按 manifest 分流，证明没有双投。
- Preparation 模块缺失的编译 RED 后实现并通过三个 SQLite 测试：快照/验证恢复及 revision 变化、过期事务回滚及 outbox 恢复、无效参数/allowlist/重复 call ID。
- consumer 隔离的缺失 API RED 后实现 ClaimForConsumer；SQLite reopen 证明工具 worker 未领取 Context delivery，后者仍为首次 epoch/attempt。

## 后续集成与部署边界

后续已接通 Job/Timer/External wait、Server 显式 tools profile、durable cancel/deadline、P3 流式数据面与 SSE fixture；证据矩阵见 `p4-server-streaming.md`。暂停/恢复仅定义并测试状态机，公共 API 未开放。

默认 legacy，显式 operator_v1_text 保持 text-only；operator_v1_tools 只通过显式配置启用。
未删除 legacy machine、旧 checkpoint reader，未进行灰度、压力测试、历史 GC 或 P5 工作。
