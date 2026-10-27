# P4 Harness reducer effects

> 历史设计说明：本文涉及的固定流水线已移除。当前实现、持久化事件与恢复 API 以[静态图指南](static-agent-graph.md)和[单 Agent API](operator-only-api.md)为准。

日期：2026-10-23。仍以原始迁移设计为权威；本文记录实际实现，不改变验收范围。

## 已接通的链

`ToolsRunCoordinator` 独占 `EffectRun.reducer`。`ToolsPipelineRuntime` 只调度算子，不替代 `AgentLoopReducer` 的业务决策：

```text
admit → toolset.requested → toolset.completed
→ model.requested → model.completed/failed → output.sealed
→ tool.validation.requested → tool.validation.completed/failed
→ approval.requested → approval.resolved/expired/cancelled（需要时）
→ tool.requested → tool.completed/failed/suspended/uncertain
→ reducer join → 下一轮 toolset/model → run.completed/failed
```

新增 core 契约位于 `effect_pipeline.rs`；Harness 分为 `effect_coordinator.rs`、`effect_model.rs`、`effect_output.rs`、`tools_runtime.rs`。
Model/Output/Coordinator 不调用 ToolPort；工具调用仍仅在 ToolOperator。

## 持久化与恢复

- `TransactMany` 在现有 Fabric SQLite 事务内同时写 Run reducer checkpoint、不可变 effect request checkpoints、outbox、当前事实 receipt/ACK。任一 CAS/schema/容量/lease 校验失败全部回滚。
- effect 订阅使用独立 topic，使并行安全工具具有独立 delivery lease/ACK；consumer 同时按 tenant/run/role 隔离。
- 订阅可从持久化 routes 重建；先提交 checkpoint/outbox，再恢复订阅和派发，不依赖内存任务存活。
- approval ID 与现有 reducer 的 run/step/call 派生方式一致；绑定 revision/digest 与参数 digest 保持不变。
- Model 执行前保存 execution intent；没有确认 final 的过期执行不会盲目重调 provider，而会产生中断失败。
- tools Model 使用 P3 bounded delta transport；Output 使用相同 OutputBuffer、batch 窗口、coverage、ModelFinal artifact 和 seal 协议。Coordinator 按 batch 推进非终态 reducer 输入，仅在 coverage/seal 一致后接受模型终态。
- 公共事件由现有 `AgentEvent::into_run_event_kind` 转换并稳定编号，保存在 Run checkpoint，通过公共 RunEventProjector 写入 Server RunStore，再进入原 HTTP SSE/replay 路径。

## 并发、审批和取消

- reducer 决定并行批次；runtime 并发驱动独立 ToolOperator。Barrier/Notify 测试证明两个工具确实 overlap、第二个先返回，但后续 model messages 按 first/second 顺序 join。
- exclusive 工具保持单个 pending effect。未增加另一套调度业务规则。
- allow-once 后执行；deny/expired 返回原 reducer 生成的工具拒绝消息并进入下一轮，不执行 ToolPort。
- `request_cancel` 只持久化带 actor 的 Gateway 请求。Coordinator 首先消费取消并 fence Run，原子提交终态、pending model/tool cancel intent 和 pending approval cancelled。
- model/tool cancel handler 用各自 execution checkpoint CAS 处理取消；ToolOperator 在执行前再次检查 durable Run pending effects。工具晚到成功不能覆盖取消，并写 stale 审计。
- Server tools profile 的 HTTP cancel 只写 Gateway durable intent；后台 Coordinator 消费并 fence。Job/Timer cancellation 由 wait bridge 接通。绝对 Run deadline 持久化，重启不重置；暂停/恢复只定义带 revision fence 的纯控制状态机，不开放 HTTP API。

## 本轮测试证据

`p4_effect_pipeline` 使用 fake Model/Tool 和真实 SQLite，覆盖：

1. model→validation→approval→tool→next model→terminal；approval 等待时关闭/reopen、重复 resolution。
2. parallel overlap、逆序返回与原序 join；exclusive 单 effect。
3. 两个 tenant 的独立 approval 等待；测试先暴露 subscription 冲突，再修复 consumer namespace。
4. deny/expired 不调用工具并继续 model。
5. 每个 tick 关闭/reopen，在 validation、execution、join、next model 边界恢复。
6. Run cancel request 后 reopen，取消 pending approval；取消后 allow 被拒绝。
7. Notify 控制的在途 Tool 成功与 Run cancel 竞争，迟到成功 Fenced、不请求下一轮 Model、终态一次。
8. 模型失败必须发 `model.failed`，经 seal 后失败终态；先 RED 后修复错误的 `model.completed` 分类。

`p4_checkpoint_transaction` 新增多 checkpoint CAS 的整体回滚和 SQLite reopen/outbox 恢复测试。

## 部署边界

MaySuspend 的 durable wait 注册和恢复、Job/Timer bridges 见 `p4-durable-waits.md`。Server admission/recovery、统一输出、HTTP fixture 和最终验收见 `p4-server-streaming.md`。
默认 legacy；显式 `operator_v1_text` 不变；新增显式 `operator_v1_tools`，同 Run 的 engine/profile 固定，不允许 fallback。
未做 P5 灰度、压力、GC、legacy 删除；未 commit/push。
