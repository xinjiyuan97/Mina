# 输出适配器：Mina run 事件流 → `agent-chat/1`（Chat UI 协议）

> 本文件是**需求的唯一来源**。实现时反复读它，不要凭记忆发明字段名。
>
> **目标协议规范**（另一个仓库，绝对路径可直接读）：
> `/data/work/src/github.com/xinjiyuan97/ChatUIComponent/docs/protocol/agent-chat-1.md`
> 同仓还有 `PROTOCOL.md` 与 `docs/protocol/IMPACT.md` 可作背景。
> **规范是权威**：字段名、事件名、终局取值，一律以它为准；本文件只描述**映射与交付要求**。

## 0. 目标与非目标

**目标**：让 Mina 能把自己的 run 输出，以一个**符合 `agent-chat/1` 的事件流**交给 Chat UI 前端。
即：`RunEvent` / `AgentEvent` 流 → `agent-chat/1` 的 `ChatEvent` 流（SSE）。

**非目标（本阶段明确不做）**：

- **不做输入方向**（前端 → Mina 的 `input[]`：`message` / `tool-result` / `permission-decision` /
  `a2ui-action` / `cancel`）。**这是一条已拍板的边界：输入端由调用者自己实现**（见 §7）。
  本阶段只做**输出**。
- **不改 Mina 现有的 gateway 协议**（`docs/02`、`docs/03`）：适配器是**新增的一条输出面**，
  不得重写或挪动既有协议文档/结构。
- **不动 `apps/web`**（Chat UI 前端自身的接入是另一件事）。
- 不引入新的第三方依赖（见 §4）。

## 1. 代码组织：落点由你决定，但要在 extension 的既有范式内

`crates/extension`（`agent-extension`）就是本仓库的**外部实现目录**，它的定义是：

```toml
description = "External providers, tools, stores, sandboxes and observability adapters"
```

```rust
//! Concrete external implementations for `agent-core` ports.
//! Every integration is feature-gated so a small CLI does not link database or
//! monitoring dependencies that only the server needs.
pub use agent_core as core;
#[cfg(feature = "openai")]        pub mod provider;
#[cfg(feature = "builtin-tools")] pub mod tool;
#[cfg(feature = "host-sandbox")]  pub mod sandbox;
#[cfg(any(...))]                  pub mod store;
#[cfg(feature = "observability-tracing")] pub mod observability;
```

**由你决定**：模块名与目录布局（如 `src/output/`、`src/adapter/` 或别的）、feature flag 的命名与默认值、
是否要同时按 `contracts/tool/v1/` 的既有范式补一份 `contracts/agent-chat/v1/`（schema / 示例帧），
以及**它到底该是实现 `agent-core` 的一个 port trait，还是一个独立的序列化器**。

但**必须**：

1. 遵守 `docs/01-architecture-boundaries.md` 的四条边界与依赖方向；`scripts/check-rust-boundaries.sh` 必须仍然通过。
2. 与 extension 里其他模块一致：**feature-gated**（小 CLI 不该被迫链入无关依赖）。
3. 纯函数/无副作用的那一层要**可单独测试**，不要把网络/进程耦合进来。
4. 报告里**说明你为什么这样组织**（对照上面几个候选，给出取舍理由）。

## 2. 映射要求（本适配器的实质内容）

源事件在 `crates/core/src/contracts/event.rs`（`AgentEvent` 是语义事件，`RunEvent = { run_id, seq, kind }`。
它的注释写明：*transport-neutral; SSE event names are derived at the Gateway boundary* ——
**这个适配器正是那个 boundary**）。至少要覆盖：

| Mina 侧 | `agent-chat/1` 侧 |
|---|---|
| `OutputDelta{AssistantText}` | 助手文本块的增量（`text-delta`），连续块复用 `text:0` / `text:1` / … 形式的稳定 `blockId` |
| `OutputDelta{AssistantReasoning}` | 推理块的 start/delta/end 三态，连续块复用 `reasoning:0` / `reasoning:1` / … 形式的稳定 `blockId` |
| `ToolCallStarted` / `ToolCallArgumentsDelta` | 工具块的输入流式（含 `call_id` → 稳定块身份） |
| `ToolExecutionStarted` / `Completed` / `Failed` | 工具结果 / 结构化错误（`scope`/`code`/`retryable`/`retryAfterMs`） |
| `ApprovalRequested` | 权限块 + **流以 `finishReason: 'awaiting-permission'` 正常结束**（不是错误！） |
| `ApprovalResolved` | 权限决定落位（已决后同一块转为只读记录） |
| `UsageUpdated` | 用量字段 |
| `Cancelled` | 取消终局 —— **必须与 `error` 区分**，也不等于 `stop` |
| `Failed` | 结构化 error 终局 |
| `Completed{finish_reason}` | `message-end` + `finishReason` 映射 |
| `RunEvent.run_id` / `.seq` | `runId` / 事件 `eventId`（**run 内严格单调**） |

硬要求：

1. **六种终局必须分得清**：`stop` / `length` / `tool-calls` / `awaiting-permission` / `cancelled` / `error`。
   `FinishedReason` → `finishReason` 的映射表要显式写在代码里并测试。
2. **身份集要完整**：`conversationId` / `turnId` / `runId` / `messageId` / `blockId` / `eventId`。
   Mina 侧缺什么（例如没有 `conversationId`/`turnId` 概念），**由适配器显式补或显式声明缺省**，
   不许留空、不许编造语义 —— 在报告里列出「Mina 没有、需要宿主提供」的字段。
3. **帧规则**（照规范）：一帧一个事件、JSON 不跨行、心跳注释行**不推进** `eventId`、`[DONE]` 兼容但业务终局是 `message-end`。
4. **`#[non_exhaustive]` 的枚举要安全**：`AgentEvent`/`RunEventKind`/`OutputChannel` 都是 `non_exhaustive`，
   适配器**不能因为遇到未知变体就 panic**；未识别的东西要么降级为可诊断的事件，要么记日志并跳过 ——
   选一种，写清楚，并测试。
5. **幂等与重放**：同一 `run_id` + `seq` 重复投递不产生重复块（规范要求重复投递合法且安全）。

## 3. 测试要求（这块是交付判据，不是可选）

1. **映射单测**：上表每一行至少一个用例 —— 输入一个 Mina 事件，断言产出的 `ChatEvent` JSON（逐字段）。
2. **终局用例**：六种 `finishReason` 各一条，且必须有一条**证明 `cancelled` 不等于 `error`**。
3. **帧规则用例**：`eventId` 单调；心跳不推进；重复投递不重复建块。
4. **黄金用例（golden）**：把规范文档里给的**示例帧**抄成 fixture，喂一段脚本化的 run 事件流，
   断言产出的帧与 fixture **逐字节一致**。规范里的示例是唯一权威，不要自己另造一套。
5. `cargo test --workspace` 全绿；**既有用例一条都不许变红**。

## 4. 依赖与工程约束

- **不新增第三方依赖**。需要 JSON 序列化就用 workspace 里已有的（`serde` / `serde_json` 已在用）。
  如果确实认为非加不可，**先在报告里说明理由，不要直接加**。
- `cargo fmt --check`、`cargo clippy --workspace --all-targets -- -D warnings` 必须通过。
- `pnpm check`（含 `scripts/check-rust-boundaries.sh`）必须通过。
- 注释与文档：与仓库现有风格一致（英文注释，中文文档视同处风格）。

## 5. 验收（贴真实输出）

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
pnpm check                      # 含 check:web 与 check:rust-boundaries
git diff --check
```

## 6. 报告格式

1. 你决定的**代码落点与结构**，以及**为什么这么选**（对照 §1 的候选）；
2. 完整 `git diff`；
3. **映射表**：Mina 事件 → `agent-chat/1` 事件的逐条对应（含你为每种情况选的块类型与 `blockId` 生成策略）；
4. **Mina 侧缺失、需要宿主提供的字段清单**（如 `conversationId` / `turnId`）；
5. 六种终局分别映射到什么、`cancelled` 与 `error` 如何被区分开；
6. 未知变体（`non_exhaustive`）的处理策略与对应测试；
7. §5 每条命令的真实输出 + `cargo test` 前后用例数对比；
8. **明确列出你无法自行验证的部分**（含糊的「已验证」不算验证）。

**不要 commit、不要 push、不要建分支**：工作区保持未提交，由上级验收后处理。

---

# 7. 输入端：由调用者实现（已拍板的边界）

**决定**：本适配器只负责**输出方向**（Mina → 前端的事件流）。反向的 `input[]`（前端 → Mina）
**由调用者自己实现**，不放进本适配器。下面写清「接缝在哪、调用者要对到什么」，避免两边各写各的。

## 7.1 适配器已提供、调用者可直接依赖的部分

| 能力 | 由谁提供 | 代码落点 |
|---|---|---|
| 事件信封 + `eventId`（run 内单调） | 适配器 | `ChatFrame { event, event_id }` / `frame_id()` |
| SSE 帧形态（含 `id:`，供 `lastEventId` 续传） | 适配器 | `ChatFrame::to_sse()` → `id: …\ndata: …\n\n` |
| 重复投递安全（同 `run_id`+`seq` 不重复建块） | 适配器 | `State::seen: HashSet<(RunId, u64)>` |
| 终局只发一次 | 适配器 | `State::finished` |
| 能力/限制的声明载荷 | 适配器（**取值由宿主配置**） | `Capabilities` / `Limits` / `AgentChatConfig` |
| 必须由宿主给的关联 id | **宿主/调用者** | `conversation_id` / `turn_id` / `message_id`（必填） |

## 7.2 调用者实现输入端时必须对齐的要求

1. **五类输入都要认**：`message` / `tool-result` / `permission-decision` / `a2ui-action` / `cancel`，
   字段与语义照规范的 §2.2，**不要自行改名**。
2. **`inputId` 是幂等键**：同一个 `inputId` 重复投递必须**只生效一次**（重连、重试、多设备回灌都会发生）。
3. **权限往返的语义别搞错**：等待权限时，输出流是**以 `awaiting-permission` 正常结束**的；决定到达后是
   **同一个 turn 里的新 run**，不是把旧流接着写。`permission-decision` 最终要落到 Mina 的审批决定上
   （适配器已引用 `ApprovalDecision`，方向相反）。
4. **取消**：`cancel` 对到 Mina 的 `RunCancellation`（适配器亦已引用该类型）。
5. **续传**：客户端带 `lastEventId` 重连时由承载层按适配器发出的 `id:` 重放。适配器保证帧**可安全重放**；
   但「从哪一条开始重放」属于承载层职责。
6. **能力声明要诚实（最容易出错的一条）**：`Capabilities` 的位是**给前端的承诺**。
   - `clientTools` 只有在调用者**真的实现了 `tool-result` 回传**之后才能置 `true`；
   - `a2ui` 只有在真的处理 `a2ui-action` 之后才能置 `true`；
   - 适配器会为每个文本/推理块分配唯一 `blockId`，因此 `blockIds:true`；其它默认值仍是
     `resume:false` / `clientTools:false` / `a2ui:false`；适配器尚未提供事件重放/续传实现，故不能声明
     `resume` 能力；对应的 `resumeWindowEvents` 为 `0`。
7. **不要为了输入端去改输出适配器的契约**：接缝是「配置 + 帧格式」，两边靠**规范**对齐，不靠互改代码。

> 以上是**给调用者的对齐要求**，不是本适配器要实现的代码。若将来输入端也要进本仓，
> 建议同样落在 `crates/extension`（`agent-extension` 就是「外部实现」目录），与本模块并列。
