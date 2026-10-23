# P0 基线与协议冻结记录

日期：2026-10-23。唯一设计依据：`event-driven-operator-pipeline.md`。

**目标契约已定义 ≠ 全链路已实现。** 本轮没有 Event Fabric、Coordinator、Context/Model/Tool/Output 算子实现，没有 broker、SQLite schema 变更、执行器切换或灰度。原设计文档保留原样。

## 1. 当前执行链保持原样

- `apps/server/src/main.rs`：`create_run` / `submit_session_run` → `prepare_run_plan` → Skill resolution / `context.build` → `runtime.start_planned` / `start_persisted`。
- `crates/harness/src/run_runtime.rs`：`start_machine` / `drive_flows_once` 继续驱动原 `MachineStream`、Flow activation 和 `persist_run_events`。
- `crates/core/src/harness/agent_loop/machine.rs`、`native_reducer.rs`：仍由 native driver 在 activation 内执行 reducer effect，逐条处理模型 delta。
- `reducer.rs`：继续独占 Agent 下一步决策。本轮不改变 effect、input、checkpoint 或 phase ABI。
- `main.rs` 的 `get_run_events` / `run_event_response` / `sse_event` 仍读取原 RunStore / broadcast；未改成 operator event 投影。

本轮 `main.rs` 的差异只在 `#[cfg(test)]` 模块声明和测试 state 构造器。公开路由注册、handler、请求/响应 DTO、状态码及 SSE 生成逻辑均未修改。

## 2. Foundation 契约

`agent_core::operator` 是独立模块，不依赖 harness 或 extension，不定义异步执行入口。

- `EngineVersion` 区分 `legacy_machine` / `operator_v1`；envelope schema 与 projection 当前只接受版本 1。未知 engine 字符串反序列化失败；未知 schema/projection 版本在 `validate` 时拒绝。
- envelope 包含 event、tenant、run、可选 session、effect、invocation、attempt、issued revision、lease epoch、operator name/version、correlation/causation、发生时间及 payload。
- `DeliveryClass::{Durable, Transient}` 为不同枚举值；payload contract 绑定 event type 和 delivery class，错配拒绝。声明 durable 不意味着已持久化，envelope 不是 receipt。
- `OperatorSequence`、`DeltaSequence`、`PublicRunSequence`、`LogSequence`、`RunRevision`、`LeaseEpoch` 为六个不同 newtype，wire 仍是数字，不提供隐式互转。四个 compile-fail doctest 和 TypeId 两两不同测试保护边界。
- operator sequence、attempt、lease epoch 从 1 起；issued revision 允许 0。P0 只校验局部结构，不假装验证跨消息单调性、pending effect、lease 有效期或 revision CAS。
- `PayloadContract` 由可信调用方给出 JSON Schema、event type、delivery class 和 payload byte 上限。`validate` 按 compact UTF-8 JSON 实际字节数检查上限，再验证 schema 和 payload；支持每事件不同限额，不把设计中的待压测窗口参数变成全局默认。
- `schema_version = 1` 冻结当前 envelope/payload reader 版本；operator 实现 version 是身份元数据（正整数），不冒充已注册版本。具体业务 payload schema/注册表仍属于后续阶段，P0 不定义假 ModelResult/ToolResult。
- serde 解码和 `validate` 是分开的：解码成功不等于经过校验，更不代表授权。未来 admission/adapter 必须显式调用校验。JSON Schema 可进一步约束嵌套、字符串或数组；通用递归深度防护、artifact 安全与 transport 分配前限流不在本轮实现。

## 3. 离线兼容对照（不是执行 adapter）

`agent_harness::operator_compat` 只返回借用原对象的只读 view 或 effect 描述，不发布、订阅、调用 Model/Tool 或改变状态。

| 旧对象 | P0 对照 | 不允许推导的含义 |
| --- | --- | --- |
| `PublishEvent` | 原 event_id/type/topic/schema/source/correlation/causation/timestamps/payload 原样借用；尚无 log sequence | 不编造 tenant/run、effect、invocation、attempt 或 lease；旧 durable 意图不等于新事务 receipt |
| `EventEnvelope.sequence` | 显式包装 `LogSequence` | 不是 operator sequence、delta sequence 或 public SSE id |
| `RunEvent.seq` | 显式包装 `PublicRunSequence`，保持原 `event_name()` 和 payload | 原 live event 不一定已落盘；不能将其自动标记为新 durable output |
| `LoadToolSet` | `toolset.requested` / toolset / AwaitingToolSet | 不执行 |
| `InvokeModel` | `model.requested` / model / AwaitingModel | 不改变 purpose，不调用 provider |
| `ValidateTools` | `tool.validation.requested` / validator / AwaitingValidation | 不执行工具 |
| `RequestApproval` | `approval.requested` / approval / AwaitingApproval | 不制造批准事实 |
| `InvokeTool` | `tool.requested` / tool / AwaitingTools | 不调用 ToolPort |

五个 effect 的稳定 `effect_id` 原样保留。target phase 为对照标签，不引入第二个状态机。Preparing/OutputDraining 等目标 phase 尚未实现。已有 Job/Timer 的 topic/type/payload 不在 P0 重命名；`timer.fired` fixture 证明存储 view 不重写它们。不能仅凭现有消息构造合法 `OperatorEnvelope`：缺失身份必须由未来可信 admission/coordinator/task ledger 提供，不能填默认值伪装完成迁移。

## 4. 外部协议冻结

`apps/server/src/p0_contract_tests.rs` 直接调用现有 handler，通过真实 Response/SSE body 验证 wire；假 Responses provider 使用本地临时端口，不调用真实模型或工具。

- `runs.json`：stream/non-stream 的完整 JSON 形状、usage、六条事件 trace、工具集 digest、数字 id、名字和 payload。只有随机 run UUID 归一化。
- replay/follow：从 0、header cursor、query 优先于 header、非法 header 回退 0、`u64::MAX` 空回放；非流式 Run 的持久回放与流式输出一致。Session 测试同时覆盖活跃 Run 的 follow 至取消终态。
- `event-catalog.json`：当前全部 18 种 RunEvent 的序列化与 SSE 名字/id/payload，另逐字节断言无数字 id 的 `run_stream_error` 特殊帧。没有增加新 SSE 类型。
- `errors-approval.json`：非法/不存在 run、非法 session、空幂等键、空输入，以及 approval 接受、同决议重放、冲突决议的 400/404/200/409 和完整 error body。
- `session-cancel.json`：submit 202、相同 key 重放且不创建第二个 run、同 key 不同输入 409、单活跃 Run 409、取消 202、终态再次取消 409。仅归一化随机身份和经非空/相等检查的 context fingerprint；状态、revision、max_steps、错误信息和 replay 标志不归一化。

这些是旧路径的特征测试，不声称旧执行链具备目标设计的 durable output barrier、崩溃恢复或 exactly-once。未对真实外部工具副作用进行双跑。

## 5. TDD 证据

日志暂存 `/tmp/mina-p0/`。所有 RED 都实际完成编译后失败，不以编译笔误作为证据。

| 命令/测试 | RED 原因 | GREEN |
| --- | --- | --- |
| `cargo test -p agent-core --test operator_contract` | 初始序号为别名，`sequence_domains_are_distinct` 报 domains must not alias；初始 validator 尚为空，版本/schema/大小测试得到 `Ok(())` 而非指定错误 | 4 passed |
| `cargo test -p agent-harness --test operator_compat` | effect target 为空；stored view 缺 `LogSequence(81)`；public view 返回 0 而非 42 | 3 passed |
| `cargo test -p mina-server p0_ -- --nocapture` | 原 handler 已产生完整 trace，但 `runs.json` 和 `errors-approval.json` 尚未冻结，fixture 读取 NotFound | 添加经检查的旧输出 golden 后通过 |
| `cargo test -p mina-server p0_session -- --nocapture` | Session/取消 trace 已生成，`session-cancel.json` 尚未冻结 | 添加 golden 后通过 |
| `cargo test -p mina-server p0_all_sse -- --nocapture` | `event-catalog.json` 尚未冻结 | 添加 18 种旧事件 golden 后通过 |

日志：`core-red.log` / `core-green.log`、`compat-red.log` / `compat-green.log`、`server-red.log`、`session-red.log`、`catalog-red.log`、`server-green.log`。Server RED 首次遇到沙箱拒绝 bind，随后经授权在允许回环端口的环境重跑；权限失败不计作 TDD RED。

具体测试名：

- core：`sequence_domains_are_distinct`、`rejects_unknown_versions_and_invalid_identity`、`rejects_schema_and_delivery_mismatch`、`stable_wire_roundtrip_and_exact_utf8_limit`（RED：4 failed；GREEN：4 passed）。
- harness：`every_reducer_effect_has_an_offline_target_and_phase`、`legacy_publish_keeps_names_payload_and_only_storage_sequence`、`public_event_sequence_is_not_an_operator_or_delta_sequence`（RED：3 failed；GREEN：3 passed）。
- server：`p0_runs_stream_nonstream_replay_follow_fixture`、`p0_error_and_approval_fixtures`、`p0_session_submit_idempotency_busy_and_cancel_fixture`、`p0_all_sse_names_payloads_and_error_frame_fixture`（分三轮 RED：2/1/1 failed；最终 GREEN：4 passed）。

## 6. 验证与边界审计

最终验证全部通过，完整输出保存于 `/tmp/mina-p0/`：

| 命令 | 输出摘要 | 日志 |
| --- | --- | --- |
| `cargo fmt --all -- --check` | exit 0 | `fmt.log` |
| `cargo test -p agent-core -p agent-harness -p agent-extension -p mina-server` | 共 218 passed，0 failed：core 72 单测 + 4 契约 + 4 compile-fail doctest；extension 92；harness 34；server 12 | `full-test.log` |
| `bash scripts/verify.sh` | core 72 + 4 契约 + 4 doctest；extension 11；frame-shapes 16 events，PASS；“本地闸门全部通过” | `verify.log` |
| `cargo clippy --workspace --all-targets -- -D warnings` | 全 workspace exit 0，无需基线豁免 | `clippy.log` |
| `git diff --check` | exit 0 | 终端审计 |

四 crate 测试启用了 server 依赖的 full/quickjs 等 feature，因此 extension 测试数量高于仅 agent-chat 的 verify 基线，非基线测试被替换。

边界审计命令：

```sh
git diff -- apps/server/src/main.rs crates/core/src/lib.rs crates/harness/src/lib.rs
rg -n 'operator_compat|operator::|OperatorEnvelope|effect_target|public_event_view|stored_event_view' crates apps --glob '*.rs'
sha256sum -c /tmp/mina-p0/design.sha256
git status --short
```

原设计 SHA-256：`7b6bc4c1f156eb72566eae441ddbae0cd203159d5441dd5bafa8645b3f89d62b`。

搜索预期仅命中新模块的定义/文档、导出和测试。不得在 `run_runtime`、AgentLoop driver、Context、ModelProvider、ToolPort 或 server handler 中出现新契约调用。P1–P5 工作仍未开始。

本轮实际搜索符合上述范围（`/tmp/mina-p0/call-sites.log`）；设计文档哈希核验返回 OK。未创建分支、commit 或 push。

验证边界：上述 HTTP 契约测试调用现有 handler 并读取真实响应 body，不替代部署后反向代理/鉴权链的端到端测试；没有进行真实模型/工具调用、负载测试或新 Fabric 崩溃恢复测试。这些不是 P0 已实现能力，也不是本轮测试失败。
