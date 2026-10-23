# P2 请求与 Context 算子

日期：2026-10-23。基线：P0 `4c17d95`、P1 `f2236ce`。
范围仅为总设计迁移表 P2；没有启用 `operator_v1`，没有迁移 Model、Output、Tool、Approval、Job 或 Timer。

## 文件与所有权

| 层 | 文件 | 职责 |
| --- | --- | --- |
| core | `crates/core/src/context_operator.rs` | 四种稳定事件名称、payload schema、ContextRequest/Policy、PreparedContext、SkillLock 与结果/错误引用 |
| core | `crates/core/src/operator_fabric.rs` | 增加只读 ReadEvent / ScanEvents，无领域执行接口 |
| extension | `crates/extension/src/operator_fabric.rs` | 点查/扫描、受限 legacy Context 发布、同 Run engine 不可切换 |
| extension | `crates/extension/src/store/operator.rs` | 新查询走原 SQLite adapter 的只读事务；没有新增数据库或 broker |
| harness | `crates/harness/src/context_operator.rs` | AdmissionFacade、ContextOperator、可注入 ContextPreparer、AgentContextPreparer、临时 legacy_plan |
| harness | `crates/harness/src/run_runtime.rs` | 幂等启动桥、识别有 durable Context 请求的待准备 Run |
| server | `apps/server/src/admission.rs` | DTO/错误映射、请求任务所有权、临时桥恢复接线、HTTP/Session 测试 |
| server | `apps/server/src/main.rs` | 注入 SQLite/Blob/Context ports，删除原请求准备业务实现；保留路由与公共 DTO |
| 测试 | `crates/harness/tests/context_operator.rs` | Context 等价性、事务、超时、断连、重复投递、真实 SQLite reopen |

`core/lib.rs`、`harness/lib.rs` 导出模块；harness 的 dev-dependency 增加 filesystem feature 以测试真实 blob 引用。

## 请求时序

```text
HTTP extractor / max_steps / blob / input validation
  Session only: existing begin_run CAS + idempotency/hash/revision/active-run check
  immutable request artifact (BlobStore)
  SQLite PublishBatch [run.requested(seq=1), context.requested(seq=2)]
  ContextOperator claim(context.requested)
    SkillOrchestrator.resolve + compile_skills
    ContextEngine.build
    immutable prepared/error artifact (BlobStore)
  SQLite Commit [receipt.result + terminal outbox + ACK]
  DispatchOutbox [context.completed OR context.failed(seq=3)]
  AdmissionFacade barrier
    success: PreparedRun -> start_context_plan -> existing start_planned/start_persisted
    failure: original 400/422 JSON, before SSE headers
```

事件使用稳定 `EventId::stable(run_id, event_type)` 身份、同一 effect/invocation、attempt 1、`engine_version=legacy_machine`。Session identity 在 envelope，request hash、max_steps 在业务 payload；Context fingerprint、SkillLock、Memory/artifact refs 在 completion；完整 pack、消息、输入与冻结的 host Context policy 在 blob 引用中。事件不包含模型配置或 provider 凭据。错误正文也放入 artifact，避免长 Skill 错误撞到 64 KiB fabric 限制，同时 HTTP 仍取回原始错误正文。

Context policy 的选择属于 host admission 配置映射；Context/Skills 的业务执行只在 harness。Skill 描述列表仍由 server 的 inspection API 读取，不属于请求编排。

## 事务与恢复边界

1. **请求事务**：同一个 P1 PublishBatch 原子提交两条请求事实。失败无半条 Run/Context 请求；同 Run 相同完整请求复用已有引用，不产生重复准备。并发 publish 的输家读取胜者，再比较请求内容；不同请求拒绝。
2. **Context 事务**：handler 只返回结果与 terminal outbox。receipt、outbox 预留序号与 ACK 使用原 P1 Commit 单事务。`context.failed` 是正常的领域终态提交，不进入运输重试。
3. **Artifact 边界**：先完整 put，再提交引用。BlobStore 和 SQLite 不伪装成跨库事务；提交前崩溃可能遗留无引用 blob，不能产生已提交的部分 Context 结果。沿用现有文件 BlobStore 的恢复/持久化保证，不额外宣称掉电 fsync 保证。
4. **重复/重启**：已提交 receipt/outbox 恢复派发；跨同 consumer 的重复 subscription/delivery 复用 receipt，不重做业务提交。claim 后、commit 前崩溃允许重算 Context；旧 lease 被 fencing。这里的 exactly-once 范围是提交结果，不是外部组件调用次数。
5. **等待者生命周期**：`wait` 有显式 Duration deadline，drop/abort 只取消等待。server 请求任务脱离 HTTP waiter 所有权；HTTP 断连不会撤销 durable 工作。HTTP Context barrier 为 300 秒，超时返回 504，已提交请求保留。恢复 worker 后续只在成功事实存在时启动 legacy。
6. **Legacy 启动**：同一 Run 的本进程启动被共享 async mutex 串行化，已有 active Run/终态直接复用；生产 durable 模式以原 FlowStore 的唯一 Run row 作为启动凭据。RunSnapshot 写入后、Flow create 前崩溃可以补齐 Flow；已有 Flow 的重投只重连，不再次 initial checkpoint / start。没有把 Flow、RunSnapshot 和 fabric receipt 谎称为一次事务。
7. **启动恢复**：先识别 durable request 对应 Run，原 `recover_interrupted` 不将尚未准备的 Session 占位错误关闭；原有已执行 Flow 恢复行为保持。server 的临时桥扫描 durable 请求，继续 Context 终态后的启动或失败 Session 收尾。新扫描是只读、分页、稀疏存储 cursor，不是订阅 ACK/业务 sequence，不能替代 P1 连续 offset。
8. **Session 占位窗口**：原 `begin_run` 事务在 PublishBatch 之前；没有引入假的跨 RunStore/fabric 原子性。HTTP 断连由 owned task 覆盖此窗口。进程若在占位后、durable publish 前崩溃，仍由原 interrupted recovery 失败收尾，而不是声称未持久化的请求可恢复执行。成功返回 admission 的请求已经经过 durable publish。

## 兼容性证据

- 原 `raw_request_hash` 未修改；Session begin_run 的 key/hash/revision/busy 决策与 accepted response 不变。
- stateless 每次仍分配新 RunId；没有引入隐式 idempotency key。
- 原 request digest 字节拼接、默认/显式 Skill 选择顺序、memory scopes、预算、ContextBuildRequest、有效工具和 execution manifest 保持。
- 对照测试独立执行旧的 resolve/build，然后经 SQLite 算子路径比较完整 ContextPack、fingerprint、SkillLock、request digest、messages 和有效工具，并使用真实 Skill 文件与 Memory 数据。
- 失败测试检查 `stream=true` 仍返回 JSON Content-Type 与原 400/422 错误码，未先发送 SSE headers。
- P0 四组 golden fixture 测试全部通过；fixture 文件、RunEvent projection、数字 SSE id、after_seq、Last-Event-ID 与 SSE helper 未修改。
- Session Context 尚未完成时并发测试同 key 重放、同 key 不同 hash、新 key busy、revision conflict；只存在一个 durable 请求。
- 请求顺序测试在 Context 阻塞时观察 durable request 已存在而 legacy 未启动；真实 SQLite 两次 reopen 测试分别覆盖 stateless 与 Session，直到 legacy Flow 只创建一次。

## RED → GREEN

原始记录保存在 `p2-evidence/`，完整执行日志位于 `/tmp/mina-p2-*.log`。

| RED | GREEN |
| --- | --- |
| AdmissionFacade stub，两个 durable publish / cancel / reopen / timeout 测试行为失败 | P1 PublishBatch + Context receipt/outbox + barrier 实现后通过 |
| HTTP waiter 在 Context claim 后 abort，Context 完成但 legacy 没有启动 | owned HTTP task 后相同测试通过 |
| 130 KB Skill 错误直接进入事件，Context commit 数为 0、错误被当成运输重试 | error artifact/ref 后提交领域失败，reopen 后恢复原错误 |

附加覆盖：claim 后未提交真实 close/reopen、commit 后未派发真实 close/reopen、相同 commit、同 consumer 重复 delivery、同 Run engine 切换拒绝、130 KB 输入不进入事件、两条启动调用并发与已有 Flow reopen 复用。

测试不使用 sleep 推测时序；阻塞点通过 Notify/mpsc 控制，通过可观测 store/flow/receipt 判定进展。timeout 用于 API 边界和测试最终截止，不作为排序假设。

## 临时桥期限与非目标

**桥的有效期仅 P2。后续阶段在接入第一条 operator_v1 执行链之前必须删除/替换 `legacy_plan`、`start_context_plan` 和 server Context→legacy 恢复接线，不能带入 P5 作为 fallback。**

- 没有发布 model.requested、tool.requested 或 output batching 事件。
- 没有新增生产 ModelProvider/ToolPort 执行调用点；原 native machine、reducer、provider、tool 实现未修改。
- 没有新 RunEvent 投影、provider shadow/double execution、外部 broker 或 operator_v1 Run。
- 失败永远提交/读取 context.failed；不存在回退到旧 prepare_run_plan 的业务实现。

## 限制与开放问题

- 沿用 P1 快照 O(历史) 成本；临时桥每秒扫描历史请求，读取完整请求 artifact。正常 Context 唤醒使用 Notify，轮询只用于恢复/跨实例观察。尚未做吞吐与容量 SLO 验证，不能作为高并发生产性能承诺。
- P2 部署仍是现有单 server/runtime coordinator；本进程启动互斥与 SQLite Flow 唯一性不是多宿主分布式启动协调协议。
- Context handler lease 300 秒，运输故障最多 10 次尝试。永久存储/缺失 artifact/超大 completion 元数据故障进入 P1 的 retry/DLQ 或阻止 barrier，需运维恢复；未新增 DLQ 管理 API。
- 不保证重算期间 Skill/Memory 外部来源不变化；提交后结果固定且不重新 resolve/build。可注入 summary/compressor 若调用外部服务，其计费/外部副作用不受 fabric 本地事务 exactly-once 保护。
- artifact orphan 清理、保留策略、磁盘故障/OS SIGKILL/掉电测试留待后续；本轮恢复测试是真实关闭全部相关 SQLite handles 再 open，不是 mock SQL 次数。
- 保留原 Session finalization 和 post-run memory extraction 路径；没有借本轮迁移它们的执行协议。

验收命令与范围检查结果见 `p2-evidence/acceptance.md`。
