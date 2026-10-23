# P1 Fabric 与事务基础

日期：2026-10-23。基线：`4c17d95`，分支 `feat/event-driven-operator-pipeline`。
范围严格限于总设计迁移表 P1；没有驱动真实 Agent，没有开始 P2–P5。

## 接口与所有权

- `agent_core::operator_fabric`：可替换的 `OperatorFabric` 命令/响应端口，显式能力描述；不依赖 extension/harness。
- `Publish` / `PublishBatch`：稳定 event ID、P0 envelope 校验、可信调用方提供的 payload schema。
- `Subscribe` / `Replay` / `Offset`：持久 consumer/subscription 身份、不可变 topic/type/tenant filter、从头回放及分页。
- `Claim`：批量获取订阅头部 delivery；lease 包含 consumer/subscription/offset/worker/epoch/到期时间/投递 attempt。投递 attempt 不改变 envelope 的业务 attempt；delivery epoch 不替换 envelope 的领域 lease epoch。
- `Commit`：成功 handler 的结果 + receipt + outbox 意图 + ACK，只有一个原子入口。空 outbox 同样通过此入口 ACK，不提供绕过 receipt 的裸 ACK。
- `Retry` / `DeadLetters`：有界重试、退避与持久 DLQ；`Receipt` 返回已提交结果；`DispatchOutbox` 恢复派发已提交意图。
- `agent_harness::operator_fabric::FabricRuntime`：一次 tick，检查能力、派发恢复 outbox、批量 claim、查询 receipt、调用 handler、提交或退避/DLQ，再派发 outbox。不运行 reducer，不决定下一 Agent phase。
- `FabricHandler` 只返回 `HandlerOutput { result, outbox }`。重复投递先查 receipt，不重复提交副作用；handler 必须是可重复计算或自行协调外部幂等性。端口不声称 SQLite 能对外部网络副作用提供 exactly-once。

## 语义

### 发布、大小与顺序

1. 同 event ID、相同完整命令内容返回相同 IDs；内容不同 `Conflict`。schema/topic/envelope 都参与内容比较。
2. batch 只接受同 tenant/run 分区；不支持 best-effort。所有输入先验证，任何非法、序号不连续或冲突都整体回滚。
3. batch 内完全相同的重复 ID 合并为一个存储事件；返回 IDs 仍保留输入位置。冲突重复 ID 整批失败。batch 重试使用相同稳定事件 IDs，不需要额外 batch ID。
4. 每 tenant/run 的 `operator_seq` 从 1 连续递增，重复事件不推进序号；预留的 outbox 也占用序号，防止后续事实抢占其位置。
5. 单个完整事件（含 schema/envelope）及 result/error 最大 64 KiB；batch 最大 128 项、序列化事件字节合计最大 1 MiB；查询/claim 最大 128；订阅身份、topic/filter 最大 256 bytes。空发布 batch 拒绝，空 outbox 合法。
6. `DispatchOutbox.limit` 的单位是**原子 batch**，返回值是事件数：绝不为了满足一个事件数量上限拆散一个已提交 batch。一次至多 128 个有界 batch。

### 订阅、lease 与 receipt

1. offset 是每个不可变订阅匹配流的连续计数，从 1 起；不是 P0 的 operator/log/delta/public sequence。filter 不匹配项不制造洞。
2. 同订阅只 claim 当前未确认头部，因此一个 batch 可跨订阅，但不会同时发出同订阅的两个未确认位置。ACK 大于 watermark + 1 返回 `AckHole`。
3. 当前实现以保守的 pending-outbox 前缀屏障保持 replay offset 稳定；屏障会阻挡其后的其他分区。跨分区**不提供可依赖的全局业务顺序承诺**；这个实现细节可以优化，但不能导致已经可见的 offset 重编号。
4. 订阅 lifetime 内 epoch 单调增加，reclaim 后旧 lease 的完整 token 不再匹配；`now_ms >= expires_at_ms` 即过期。成功提交、重试、DLQ 都检查 owner/epoch/offset/到期时间。
5. 同一 lease 的相同 Commit 在有效期内幂等返回原 receipt；内容不同拒绝；过期 token 即使已有 receipt 也不能写。可通过只读 `Receipt` 查询既有结果。
6. receipt 唯一键：consumer + event ID + effect ID + invocation ID + envelope attempt。跨同 consumer 的订阅复用同一 receipt；不同 consumer 独立消费。
7. handler 后重新读取 clock，不复用 claim 时间。时钟、schema、consumer/worker 身份来自可信宿主，不是未授权 HTTP 输入。P1 不提供远程认证/授权或分布式时钟同步。
8. handler 错误按指数退避到配置上限，达到 max_attempts 进入 DLQ；反复 lease 过期也在 claim 时进入 DLQ，避免只覆盖显式失败而无限恢复崩溃 worker。调用方应在 Claim/Retry 使用一致的 max_attempts。
9. DLQ 是唯一允许推进失败头部的显式审计动作，保存原 event ID、完整投递 lease/attempt、错误与 dead-letter 时间。原事件仍可 replay；不是删除消息或静默跳洞。

## SQLite 事务边界与现有系统关系

没有新增公开的 SqliteOperatorStore/broker。直接为现有 `SqliteEventStore` 实现 `OperatorFabric`，复用原数据库连接、WAL、busy timeout、blocking IO 和连接锁；原 `EventStore` / `FlowStore` / `JobStore` 实现保留。

`store/event.rs` 的既有 migration 只增加 `workflow_operator_fabric_v1` 表。该表是版本化的事务状态快照，包含日志、订阅/lease/offset、receipt 与 pending outbox、DLQ；不是另一个独立总线服务。

- 写入：`BEGIN IMMEDIATE` → 读取最新快照 → 执行共享纯状态转换 → 写回完整状态 → `COMMIT`。
- 读取：deferred transaction，读同一个一致性快照，不写回。
- handler 的业务结果以 receipt.result 保存。receipt、outbox 意图、ACK 只发生一次 SQLite COMMIT；验证失败、SQL 写入或 COMMIT 失败均不留下部分结果。
- 派发：已提交 outbox 从 pending 改为可见，本地日志发布与 pending 完成标记在同一事务，不存在先外部 publish 再独立标记的双写窗口。后续外部 broker 适配不在 P1。
- 内存 adapter 在锁内复制候选状态，只在成功后替换，因此和 SQLite 的提交/回滚边界相同；其 `durable = false`，不声称进程重启可恢复。
- `workflow_events` 仍是旧 EventRuntime/Flow/Job 路径的事实源；P1 namespace 尚未有生产调用者。**不双写同一真实 run，也不自动桥接旧事件**，因为 P0 已说明旧消息不具备构造新 envelope 所需的领域身份。
- `crates/multi/src/sqlite_bus.rs` 完全不改。它继续负责 multi-agent 协议；不复制新 operator 事实进其 messages/offset 表。若未来桥接，必须单独设计显式边界和幂等投影，不能把三个库的提交称为一个事务。
- 本轮不把 RunStore/FlowStore 的业务表更新纳入新事务，也不提供假的跨库 Unit-of-Work。后续领域 Coordinator 接入必须明确 checkpoint/CAS 的同库事务扩展；本轮的原子业务结果是通用 receipt.result，不是假装已经提交 Agent checkpoint。

## TDD 与恢复证据

原始测试输出在 `p1-evidence/`；完整门禁日志运行时位于 `/tmp/mina-p1-evidence/`。

| RED → GREEN | 证据 |
| --- | --- |
| 最小端口 + adapter stub，17 个行为测试运行失败 → 两 adapter 同套测试通过 | `01-contract-red.log`、`02-contract-green.log` |
| 协调器 stub，4 个测试运行失败 → receipt 去重、退避/DLQ、handler 后到期、SQLite reopen 通过 | `03-runtime-red.log`、`07-runtime-green.log` |
| 跨分区 pending 后 offset 重新编号，两个 adapter 都失败 → 前缀屏障后通过 | `04-offset-red.log`、`06-contract-green.log` |
| outbox batch 被 limit 拆开，两个 adapter 都失败 → 按原子 batch 派发后通过 | `05-outbox-batch-red.log`、`06-contract-green.log` |
| crash/reclaim 无限重试，两个 adapter 都失败 → claim 时阈值 DLQ 后通过 | `08-crash-retry-red.log`、`09-contract-recovery-green.log` |

保留的 contract suite 覆盖 single replay/conflict、batch rollback/retry/重复 ID、ACK hole、乱序/重复投递、filter 连续 offset、过期与 stale fencing、receipt/outbox/ACK 原子性、大小边界及跨分区 batch 拒绝。没有 mock SQL 调用次数。

SQLite 专项：

- `sqlite_reopen_kill_points`：commit 前验证失败后关闭/reopen，零 receipt/outbox/ACK 可见；reclaim 拒绝旧 token；commit 后派发前关闭/reopen，receipt 与 ACK 已存在、outbox 尚不可见；恢复派发再 reopen 无重复。
- `sqlite_sql_write_before_commit_failure_rolls_back_everything`：真实 SQLite deferred FK + AFTER UPDATE trigger，在状态写入后制造 **COMMIT 失败**。连同 trigger 的额外行一起回滚；关闭全部连接再 reopen，receipt/outbox/ACK 均不存在。移除故障后重试 commit，再次 reopen 恢复完整两事件 outbox batch。
- `sqlite_independent_connections_serialize_claims_and_publish`：两次独立 open 并发相同 publish 和 claim，只有一份事件和一个 lease；全部关闭/reopen 后 epoch 正确递增。
- `sqlite_dlq_survives_reopen`：DLQ 错误、attempt 与 offset 可在关闭/reopen 后读取。
- 故障为确定性 kill-point 模拟及真实 SQL COMMIT 失败，不宣称已做 OS SIGKILL、掉电或磁盘损坏测试。

## 限制与开放问题

- P1 优先证明事务正确性，状态快照读写为 O(保留历史)，且 pending 屏障存在跨分区队头阻塞。未压测，不声称高吞吐；生产规模接入前应演进到索引化行存储、保留/清理策略并复用本套契约测试。
- 没有自动后台服务或生产启动 wiring；调用者显式 tick。处理超时后的 fencing 不等于能撤销已经发生的外部副作用。
- 未实现外部副作用 reconcile、授权、领域 revision CAS/checkpoint、Context/Model/Tool/Output 算子、RunEvent 投影或灰度；这些不能由通用 Fabric receipt 推导。
- 未承诺新快照格式已经有未来版本 migration；表名 v1 隔离本轮格式。旧库升级仅是 additive create，已有表及外部 HTTP/SSE fixtures 不改写。

门禁结果和范围搜索见 `p1-evidence/acceptance.md`。
