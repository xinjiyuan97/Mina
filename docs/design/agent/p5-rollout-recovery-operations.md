# P5：灰度、恢复与运维边界

日期：2026-10-23。基线：P4 `f849523`，分支 `feat/event-driven-operator-pipeline`。
本文补充总设计与 P0–P4 文档，不覆盖它们的历史验收记录。

P6 输入扩展及新增恢复/附件身份边界见 `p6-multimodal-inputs.md`；P5 的历史验收结论保留。

## 1. 新 admission 路由

| 配置 | 默认 | 语义 |
| --- | --- | --- |
| `MINA_ENGINE_PROFILE` | `legacy` | `legacy` / `operator_v1_text` / `operator_v1_tools`；未知值拒绝启动 |
| `MINA_OPERATOR_BASIS_POINTS` | `10000` | 整数 0–10000；只作用于显式 operator profile |
| `MINA_OPERATOR_ALLOWLIST` | 空 | 逗号分隔的完整 Session UUID 或 stateless Run UUID；最多 1024 项，每项最多 256 bytes |

未配置 profile 时，即使比例为 10000 或命中 allowlist，也不会启用 operator。
保留 P4 显式 profile 的默认全量行为；真正灰度必须同时设置比例。

```sh
export MINA_ENGINE_PROFILE=operator_v1_tools
export MINA_OPERATOR_BASIS_POINTS=100
export MINA_OPERATOR_ALLOWLIST='已知的-session-uuid'
```

示例是 1% 加 allowlist，不是生产推荐比例。比例与 allowlist 都只决定新 admission。
身份优先取 Session UUID；stateless 请求取服务端新生成的 Run UUID。因此 Session 可提前加入
allowlist，stateless 请求通常使用比例，不能依靠重试请求体固定其新 Run ID。

桶算法冻结为 `SHA256("mina:rollout:v1:" + identity)` 的前八字节、大端 u64、模 10000。
不使用进程随机种子或 Rust 默认 Hasher。相同身份、配置产生相同选择；扩大比例不会挤出原桶。
allowlist 不是权限机制，不能授权模型、工具、审批或跨租户访问。

选择结果先随 ContextRequest 持久化，再启动唯一 engine。manifest 同时保存 engine、profile、
projection version。恢复读取原请求和 manifest，绝不重新抽签。AppState 不再重复存储可与
profile 不一致的 engine 字段。没有 shadow、没有真实 Model/Tool 双执行。

## 2. 灰度与回滚操作

1. 备份 Run/Event SQLite 数据库及 blob 目录，使用 SQLite backup 或停写快照；不能仅复制
   正在写入的主数据库文件而遗漏 WAL。保留配置版本与二进制版本。
2. 在 fake 环境运行本文门禁及 smoke；确认恢复、DLQ、outbox age 与 projection lag 可观测。
3. 从显式的小比例或已知 Session allowlist 开始，按实际测量评审扩大；本文不规定生产 SLO。
4. 回滚设 `MINA_ENGINE_PROFILE=legacy`。或者设比例 0 **并清空 allowlist**，保留 operator profile。
5. 保留 operator workers、ledger、blobs、旧 reader 与投影；让已接受的 operator Run 排空。
   不删除 ledger，不改写旧 Run 的 engine，不把未决 Tool 当作未执行来重试。
6. `uncertain` 必须走人工确认/业务补偿。Model 已有 durable prefix 时按原 P3/P4 策略失败恢复，
   不重调 provider 拼接第二个答案。取消不代表撤销已发生的外部副作用。

这里的回滚是**支持双 reader 的二进制内切新流量**。不能把回滚解释为可随意部署不认识
operator 的历史二进制。未知未来格式的数据库必须留在隔离环境，由支持它的 reader 处理。

## 3. 恢复与版本矩阵

| 持久状态 | 当前 reader / 恢复行为 | 测试证据 |
| --- | --- | --- |
| 无 engine manifest 的 legacy checkpoint | 原 native Machine/Flow reader；不迁改 reducer ABI | RunRuntime legacy approval / wait / reopen；P0 HTTP fixtures |
| P0 envelope / P1 unversioned Fabric snapshot | 兼容 reader 按历史 v1 解释；新写入带 `schema_version=1` | `p5_unknown_ledger_version_is_quarantined_without_rewrite`、P1 reopen |
| pending Context（legacy/operator） | 原请求、policy、engine/profile 固定；outbox 重投 | `context_operator`、Server admission reopen |
| P3 text ledger | `operator_runs_v1` reader；未知持久格式在结构反序列化前拒绝 | `p3_pipeline`、`p5_text_ledger_unknown_versions_fail_closed_before_decode` |
| pending Model / 有 batch 无 final | 保存前缀；不重复 provider；按 interrupted/deadline 终止 | `p3_pipeline`、`p4_streaming` |
| pending Approval | 原参数/授权绑定与审批 ingress；配置回 legacy 不换引擎 | `p4_authorization_reopen`、`p4_approval_ingress`、Server rollback fixture |
| pending Tool | intent、fencing、receipt/reconcile/uncertain；不猜外部副作用 | `p4_tool_operator`、`p4_effect_pipeline` |
| pending Wait / Job / Timer | 原 subscription、wake fence 与 native ACK bridge | `p4_wait_pipeline`、`p4_wait_cursor`、Server reopen |
| terminal / projection backlog | 幂等补投影；数字 seq 不重编号；terminal replay 不重调模型 | `p3_pipeline`、Server SSE reopen、独立进程 smoke |
| 未知 manifest engine/profile/schema/projection | `unsupported_run_version`；保留记录、不进入 legacy driver | Core manifest test、启动/恢复/activation guards |
| 未知 Fabric schema/envelope/operator/projection | `unsupported_fabric_version` / `unsupported_fabric_event_version`；不改写原始 JSON | P5 unknown-version test |
| 历史 / 未知 artifact manifest | 无标记 reader 保留；未知标记先拒绝、再考虑 body | `p5_artifact_reader_preserves_legacy_and_rejects_future_before_decode` |

Fabric 当前是单 snapshot 的事务 adapter，未知版本隔离粒度是该 Fabric 存储，不是把未来事件
悄悄跳过后继续消费同一序列。单 Run manifest 异常则在 admission recovery 中跳过该 Run。
缺失历史 version 标记属于明确的兼容 reader，不是尝试多种格式猜测反序列化。

## 4. 故障与有界资源

| 边界 | 实现/行为 |
| --- | --- |
| Context admission backlog | `PublishBounded` 在同一 Fabric 事务内注册订阅、计算 pending、发布；最多 128 个未 ACK Context 请求；跨 worker 不依赖内存计数 |
| 已满时重投 | 已存在的同内容 event 幂等返回；不因满队列拒绝已保存事实的确认 |
| HTTP 限流 | 新 admission 返回 503 `admission_backlog_full`；Session 已预留的 attempt 保存失败终态并释放占用；重试相同幂等键仍指向原 attempt，新 attempt 使用新键 |
| event / result | 默认 64 KiB；Fabric batch 默认 1 MiB，字节限制可配置，见 [FabricLimits](fabric-limits.md)；checkpoint 4 MiB、batch 最多 128 项不变 |
| delta 平面 | 延用 P3/P4：32 KiB 消息、最多 128 项、channel capacity 最多 32、输出正文 4 MiB 与 delta 数上限 |
| wait | 新 suspension 最多 32 waits、32 native effects；在 Coordinator 写绑定前拒绝。Bridge 保留历史结构 reader，不用新上限阻断已保存的 P4 wait |
| Context 引用 | artifact / memory / skill refs 各最多 128；超限保存 `context_resource_limit`，不调用 Model/Tool |
| Fabric artifact | 新写入最大 16 MiB、256 KiB/chunk，manifest 最后提交；超限不写第一个 chunk |
| 历史 artifact | 保留无 schema 标记的旧 reader，不用新写入限额阻断旧数据恢复；读取仍核对 manifest 声明长度。历史大 artifact 恢复需要相应内存 |
| SSE | 512-event replay page；broadcast lag 回 durable replay；慢客户端不持有 operator lease |

拒绝 Context 时，先写 artifact、后发布 reference 的既有跨存储窗口仍可能留下 staging orphan。
因此以上不是“全库磁盘永久有界”或“全局活跃 Run/租户配额”的承诺。磁盘容量、活动 Run 数、
历史 ledger 总量仍需宿主限制和运维监控。

故障证据均为 fake，没有调用真实上游账号：

- 两个独立 SQLite 连接的 worker claim、过期 epoch fencing、DLQ/reopen 沿用 P1 契约测试。
- 固定 seed `0x5eed`，32 次发布后 / claim 后 / commit 后 publish 前 / dispatch 后关闭重开；
  四个点必须全部命中，校验 offset 与最终事件数。这里是确定性的逻辑 crash/reopen，**不是掉电测试**。
- 真实 `BEGIN IMMEDIATE` 锁竞争触发 SQLite busy，释放后重 claim；失败时 offset 不前进。
- adapter 连接 `max_page_count` 注入真实 `SQLITE_FULL`；reopen 后 checkpoint/receipt/ACK 均不存在。
- 延迟外键约束在 SQL 写完成、commit 前失败，整个 receipt/outbox/ACK 回滚；已有测试继续覆盖。
- SSE 消费者读取第一个 frame 后停止 poll，producer 持久化到 seq 1026，并溢出容量 2 的 broadcast；
  恢复读取和 `Last-Event-ID=511` 的 HTTP 重连均无丢失、无重复。

慢 SSE 测试发现并修复了真实竞态：首个 replay page 返回后，客户端暂停，producer 已完成，
旧代码见 terminal snapshot 就退出，漏掉 cursor 之后的事件。现在先检查 snapshot watermark，
只在追平后结束 SSE。

## 5. 可观测性

新增结构化字段只包含计数、耗时、engine/profile/operator，不输出请求体、凭据、私密 arguments
或 reasoning 正文。不要把 Run/Event ID 用作 metric label。

- admission：`admission_latency_ms`、成功标志、engine/profile。
- Fabric delivery / tools operator batch：`operator_latency_ms`、delivery count、retry/DLQ/fenced count。
- 每分钟 ledger health：outbox backlog、oldest outbox age、DLQ count、active leases、持久 lease reclaim count。
- text/tools streaming：durable delta batch items/序列化 bytes；不把这些数值当 provider token usage。
- text/tools projection：补投影前 `projection_lag_events`；这是事件数差，不是毫秒。
- recovery：`recovery_attempt_count`；是恢复尝试而非独立 Run 的去重数量，不应误报为新执行次数。

当前是 tracing 字段与只读 inventory，不新增 Prometheus endpoint、histogram backend 或生产告警阈值。
text Model 的 latency、batch、projection lag 及过期 lease reclaim 同样带 engine/profile。
日志捕获测试检查关键字段，并确认 input/output/reasoning 的私密标记没有进入日志。
该测试使用独立 integration-test 进程，避免与其他异步测试共享 tracing callsite/dispatcher 的干扰；
不通过串行化整个 workspace 或删除指标断言来绕过验证。
统计成功提交后的事件；storage failure 不转成成功 ACK。

## 6. 保留与 GC 的最小运维接口

默认不自动删除任何 artifact、outbox、DLQ、terminal ledger。新增
`FabricCommand::InspectRetention { now_ms, grace_ms, external_roots }` 和只读 inventory 示例：

```sh
cargo run -p agent-extension --features sqlite --example operator_retention -- \
  snapshot.db NOW_MS GRACE_MS roots.json
```

请对一致性快照运行。`roots.json` 是外部 BlobId 数组，最多 4096 项；需要包含 RunStore、Session、
其他 reader 和 artifact 内容中引用的完整保留闭包。grace 必须为正数，不能用未来时间规避保留窗口。
清单不输出 payload，只输出数量、age、BlobId。它不是删除授权或完整 tracing GC：

1. checkpoint、可见/未派发事件、receipt、DLQ、external roots 的直接引用被保护。
2. 只报告有 manifest、且已过 grace 的**候选** orphan；缺 manifest 的 chunk 无可靠创建时间，保守保留。
3. 没有收齐外部和间接引用时，不得据候选列表删文件。FilesystemBlobStore 的文件不由此接口删除。
4. outbox 未 dispatch、DLQ 未人工归档处理、活跃 checkpoint/lease、未完成投影均阻止相关数据退休。
5. terminal 不等于可删：必须覆盖重试 receipt、SSE replay、Session 投影与历史 reader 的保留期。
6. 不直接删 `events` 数组或订阅 offset，不用 SQL 删除中间 event；当前 offset 来自稳定位置，
   逐条 compact 会破坏恢复。仅在整个存储代际停止写入、全部排空、投影完成、DLQ 已审计、备份可恢复、
   所有 reader 的保留期结束后，才可由宿主退役整个存储代际。

当前实现选择最小保守接口，不提供自动 apply-GC。P5 测试验证 active checkpoint/outbox/DLQ roots、
external roots、grace 以及 inventory 不修改原数据。

## 7. 清理与 API 兼容

- 删除 AppState 重复的 engine 缓存及 `effective_profile` 迁移分支；由 profile 单点派生。
- 移除已过期的“Context 永远 legacy”“P3/P4 后删除 legacy_plan”说明；该函数仍承担旧 Run 恢复。
- 不删除 native driver：默认 legacy 与旧 Flow checkpoint 仍有实际调用。纯 reducer、WASM port、
  legacy compatibility reader、P3 text reader 全部保留。
- 不改路由、公开 DTO、成功状态码、SSE event name 或数字 ID；新增的是限流/资源失败语义与追赶修复。
- operator_v1 两个 profile 均不支持附件/多模态，新 admission 以 422 明确拒绝；legacy 保持原行为。

## 8. 验证与证据

环境：Linux 6.8.0-110-generic x86_64，8 个在线 CPU，ext4；Rust/Cargo 1.92.0。
所有 fake、数据库与时间边界由 fixture 控制，不使用真实 provider/账号。

RED → GREEN 包括：路由/manifest 接口缺失；未知 Fabric body 被猜测反序列化；未知 projection 被继续
接受；容量入口缺失；artifact 超限仍写 chunk；Context 引用超限仍成功（及新增错误码未被 schema 接受）；
保留/lease inventory 接口缺失；SSE terminal 竞态；Session 503 后仍 busy；text metrics 缺失。
各项先运行失败测试再修复。

必须执行的命令：

```sh
cargo fmt --all -- --check
cargo test --workspace p5_
cargo test --workspace
bash scripts/verify.sh
cargo clippy --workspace --all-targets -- -D warnings
git diff --check
cargo build -p mina-server
python3 scripts/p5-server-smoke.py
```

smoke 是独立 Server 二进制加 loopback fake Responses provider，验证 operator HTTP/SSE、重启后 legacy
配置下 replay、数字 Last-Event-ID、新 legacy 非流式请求；预期恰好两次 provider 调用，无双执行。
finally 终止子进程、关闭 fake provider，并按两个端口验证不再监听。临时数据库/config/log 自动清理。

本轮的 10000 个路由样本、32 次 seeded reopen、128 个排队 Context、1026 个 SSE 事件是功能/边界
压力证据，**不是持续负载基准、吞吐承诺、p99 延迟或生产 SLO**。最终门禁结果见
[最终验收记录](./p5-validation.md)。

## 9. 明确限制

- 没有多模态 operator、shadow、自动 uncertain 补偿、自动 GC、broker 或多租户授权体系。
- SQLite Fabric 仍是 whole-snapshot adapter；长期数据量、写放大、全局 active Run 配额与长时间 soak
  未在本轮定标。默认 legacy、operator 显式灰度仍是必须的部署边界。
- 无标记历史 artifact reader 为兼容保留；不能把新写入 16 MiB 上限反推成所有历史数据的内存上限。
- 历史已保存 wait 保留原容量；新 32 项上限是写入边界，不是删除或拒绝旧 checkpoint 的依据。
- inventory 候选不是跨所有存储的可删除证明；必须保留间接/外部引用，当前没有自动清理。
- 新字段以 tracing/inventory 暴露，尚未接入统一 metrics exporter、histogram 或生产告警阈值。
- 已证明格式与配置回滚兼容；未声称任意历史/未来二进制能互相降级恢复，也未模拟真实机器掉电。

合入候选评审应以这些显式边界和门禁为依据；上线放量仍需部署环境的容量、长时负载与告警验证。
