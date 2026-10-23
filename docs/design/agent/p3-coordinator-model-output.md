# P3：Coordinator / Model / Output 纯文本纵切

日期：2026-10-23。范围限定于 `operator_v1_text`；不迁移 Tool、Approval、Job、Timer、ADF，不删除 legacy，不做百分比灰度。

## 入口与所有权

- 默认不设置 `MINA_ENGINE_PROFILE`（或设为 `legacy`）：继续 P2 Context → legacy。显式设置 `MINA_ENGINE_PROFILE=operator_v1_text` 才选择新引擎。
- 引擎在持久化 ContextRequest 时固定；旧 request 缺少该字段时解码为 legacy，且 legacy 序列化省略该字段，保持 P2 fingerprint/golden。
- 新 profile 的 Context 可用工具为空，ADF 关闭，附件拒绝。模型返回本地或 provider 工具调用，均以 `unsupported_profile` 失败，不执行任何工具，也不回退。
- 每条控制 envelope 固定 engine/projection 版本；RunSnapshot execution manifest 标记 `operator_v1`/`1`。恢复按原 request 选择引擎，不按重启后环境变量重选。
- server 的 `pipeline.rs` 独立处理路由、现有 ConfiguredModelProvider 注入；main 仅负责组合。测试全部注入 fake ModelPort。
- RunCoordinator 唯一修改 Run ledger/reducer 与执行 epoch；Model 仅发布模型事实；Output 仅维护输出 coverage/seal；RunEventProjector 唯一给新引擎分配公共 seq。

## 状态机与事务

`Preparing → AwaitingModel → OutputDraining → Terminal`。`context.completed` 只产生一次 `run.started`、`model.requested`。模型结果与输出结果任意先后到达，均须 join 后才产生一个终态。Context 失败直接形成无模型、空输出的失败终态。

内部沿用 `AgentLoopReducer`：启动时供应空 ToolSetSnapshot（纯数据，不调用 ToolPort），保存其 InvokeModel effect ID；final artifact 到达时在内存一次重建模型输入，持久化同一个 reducer checkpoint，丢弃其重复输出投影。没有逐 delta reducer dispatch。

SQLite `operator_runs_v1` 以 Run 为单位保存：

- generation CAS 与独立 Run revision；phase、inbox、稳定 effect/invocation、attempt、issued_revision、lease epoch。
- 原始 start 摘要、模型请求 artifact ref、final ref/usage/error。
- 合并 batch、durable_through、prefix digest、seal/terminal barrier。
- durable control facts/outbox 和独立的公共 RunEvent 投影 outbox。

每次变更使用 `BEGIN IMMEDIATE`：校验 expected generation → 校验追加事实与 invocation token/schema → 原子替换记录 → commit。Coordinator transition/inbox/effects 一起提交；Output batch/coverage/outbox 一起提交。旧 token、旧 CAS 和不一致重复拒绝。原始 transient delta 不写此表或 P1 fabric。

控制 outbox 通过稳定 event ID 幂等发布到 P1 Fabric，Context 的前三个 operator sequence 后继续编号。Fabric 不参与决策。P3 本地执行器读取 durable ledger 中的 model.requested；该记录同时是本地持久工作队列，P1 是可重放的控制日志，不额外启动第二个订阅执行器。

公共投影先 CAS 保存确定的 RunEvent outbox，再通过现有 RunStore 的幂等 append_events 写入；两步不是跨库原子事务。中间崩溃可重放同一 seq/内容，不重新分配 seq。旧执行器入口与 fail_persisted 拒绝 operator_v1 manifest；server 不为新 Run 创建 legacy Flow。

## 数据面与 seal

- 每 stream 使用 `tokio::mpsc`：最多 32 个消息，每消息最多 128 项、JSON 编码 32 KiB；send await 是真实背压，receiver 关闭返回错误，所有 sender 释放后 drain 再结束。
- `DeltaMessage::{Single,Batch}` 统一为 DeltaItem（invocation、delta_seq、channel、fragment）。Text/Reasoning/ToolCall 有独立类型。
- 每轮权威 journal/OutputBuffer 上限 4 MiB 文本、131072 fragments。超限明确失败，不无限积累。
- 窗口默认 40 ms、128 项、32 KiB，任一先到 flush。只合并同 channel、相邻 seq；每 batch 保存合并 segments、范围、稳定 ID、完整 prefix digest、合并 artifact ref。
- 同 seq 同内容去重，不同内容冲突；乱序只在有界 buffer 内等待，洞之后不提交。关闭时不把有洞尾部伪装成完整输出。
- final artifact 一次保存完整规范 journal、usage、finish reason、digest，不把 journal 转回逐 token 数据库事件。
- seal 读取 final artifact，验证已有 durable prefix、journal 顺序与 digest，补齐缺口再提交 output.sealed。缺失/不一致 artifact 形成 output.failed；模型事实与输出失败 join 后 run.failed，不静默丢字。
- Projector 只读 durable batches；即使 output.sealed 先到，也要等模型完成。最终 usage/terminal 在所有 durable output 之后。没有向旧客户端暴露 transient preview。

## 恢复策略

Run 执行 lease 为 120 秒，模型 wall-clock timeout 不超过 90 秒（包括背压）。恢复等待旧 lease 到期，再由 Coordinator 提升 epoch。

| 崩溃边界 | 行为 |
|---|---|
| model.requested 后、started 前 | 从 request artifact 执行 |
| started 后、final 前 | 不盲目再次调用 provider；记录 model_interrupted |
| 部分 transient delta 后 | 无可验证 final journal 时明确失败，不宣称恢复了未持久内容 |
| batch commit 后、final 前 | 保留已提交公共前缀；无法匹配 final 时 output.failed |
| final commit 后、缺尾 | 从完整 journal 修复并 seal |
| seal 后、terminal 前 | reducer/inbox join，终态一次 |
| public outbox 后、RunStore append 前 | 重放相同公共 seq/内容 |

已提交旧 epoch 事实属于合法历史，恢复可消费；新追加旧 epoch 事实拒绝。P3 不实现 provider exactly-once、不自动重试未知外部执行，不把中断伪装成成功。

## 兼容与测试证据

- core `p3_pipeline`：纯 reducer 去重、revision/attempt/lease fencing、两种终态顺序、single/batch、重复/乱序/缺洞、capacity、contract schemas。
- harness `p3_pipeline`：fake 模型完整闭环、工具拒绝、bounded Pending/drain/close、真实 SQLite reopen 五断点、公共 outbox 到 RunStore 之间故障重放、缺尾修复、缺失 artifact 失败、batch 重投、并发 admission、旧 CAS/epoch、single/batch/乱序规范化等价、Context 失败、provider 安全错误/重试语义与静态调用边界。
- server `p3_opt_in_live_replay_last_event_id_and_default_legacy`：真实 create_run/SSE/replay、每个 Last-Event-ID 截点、数字 id/event_name、最终文本/usage、非流式、默认 legacy、无 adapter 时禁止 fallback。
- P0/P2 golden 文件未改；原 API 路径、DTO、SSE 编码与 Last-Event-ID 解析未改。
- RED 记录：最初缺少 core pipeline；恢复测试发现 final artifact 缺失导致恢复返回 Storage 而非终态；Context failure 测试先缺少 fail_admission；provider 错误测试发现安全消息/retryable 被抹平。随后分别实现并转 GREEN。日志位于本次环境 `/tmp/mina-p3-*.log`，不是仓库持久证据。

## 限制与后续边界

- 当前为本地 SQLite 单 Run 聚合记录；每次 batch commit 重写该 Run 的有界元数据/合并输出。不是高吞吐 broker，也没有声明性能目标；后续可按 batch 拆表优化。
- 内存是 per-stream 有界，并未实现全局/租户配额；默认关闭，显式 profile 使用者应限制并发。
- 不透明 provider stream 在进程丢失后不能续传；没有 final artifact 时采用失败而非再次调用 provider。
- 生产 SSE 复用现有 durable replay polling，输出帧不等同 provider chunk；非流式等待器使用 durable 完成通知。
- P3 不接管取消、Approval、Tool、Job、Timer、ADF。新引擎不承诺 legacy 的工具/审批/取消能力；不得对需要这些能力的 profile 启用。
- 仍保留 P2 legacy 桥和全部 legacy 执行器；本轮不做 P4/P5 清理。

## 本轮验收结果

- `cargo fmt --all -- --check`：通过。
- P3 定向：core 4 项、harness 10 项、server 1 项通过。
- `cargo test -p agent-core -p agent-harness -p agent-extension -p mina-server`：276 项通过，含 4 个 compile-fail doctest；0 失败。
- `bash scripts/verify.sh`：通过，包含 16 个 agent-chat 帧的 Node 校验。
- `cargo clippy --workspace --all-targets -- -D warnings`：通过。
- `git diff --check`：通过。
- 范围检索：Coordinator 无 ModelPort/stream 调用点，P3 无 ToolPort/invoke 调用点；原设计、P0/P1/P2 记录与 P0 golden fixture 无变更。
- 完整测试首次在沙箱内因本地 fake HTTP listener 权限失败；获准在沙箱外重跑通过，没有访问真实模型。
- 分支保持 `feat/event-driven-operator-pipeline`，HEAD 保持 `cee341c`，未 commit/push。
