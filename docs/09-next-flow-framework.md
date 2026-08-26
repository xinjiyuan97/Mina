# 下一阶段：Session、上下文编排与可恢复流程框架

> 状态：实施设计。本文承接已经完成的单次 Run 状态层，定义 Mina 接下来从“独立任务执行器”演进为“多轮 Agent Harness + Skill/Context/Memory 上层编排 + 可挂起流程运行时”的顺序、边界和核心契约。

## 1. 总体结论

接下来不应该直接把一个通用工作流引擎塞进 `AgentLoop`。推荐拆成三个正交层：

```text
Session layer        Orchestration layer             Flow layer
消息事实与 run 归属    Skill、Memory、压缩与 ContextPack  checkpoint、等待、唤醒

Session 1 ─ Run A ─ Run B ── ExecutionPlan ── Run B
                                                ├─ activation 1 ─ Wait
                                                └─ activation 2 ─ Complete
```

- **Session** 回答“哪些消息和 run 属于同一段对话，它们的规范顺序是什么”；
- **Skill Orchestrator** 回答“这个请求需要加载哪些上层能力包、指令与资源”；
- **Context Engine** 回答“历史、Memory、Skill 和当前输入如何在预算内组成模型上下文”；
- **Run** 仍表示一次用户提交触发的完整任务，拥有唯一 `run_id` 和终态；
- **Activation** 是 run 的一次进程内执行租约，从 start/resume 开始，到 suspend/terminal 结束；
- **Flow Runtime** 回答“run 在等待什么事件，进程重启后如何从 checkpoint 重新激活”；
- **Event Runtime** 提供审批、后台 Job、Timer、Webhook 等统一唤醒事实；
- **Gateway/UI** 只提交命令和订阅投影，不拥有 Session 或 Flow 状态机。

实施顺序必须是：

```text
P0 Session
  → P1 Context/Memory/Compression + Skill orchestration
  → P2 AgentMachine/checkpoint
  → P3 Event/Job/Timer
  → P4 JS
```

## 2. 与当前单次 Run 的兼容

现有能力保持不变：

- `RunId + seq` 仍是单个 run 的事件顺序；
- `RunStore` 仍负责 run snapshot 和 append-only events；
- `POST /api/v1/runs` 继续作为无 Session 的独立任务入口；
- 当前 `Agent::run() -> AgentEventStream` 在 P0 保留；
- 当前 Server 重启会把没有 checkpoint 的 active run 标记为 `run_interrupted`。

新增能力采用扩展而非替换：

```text
POST /api/v1/runs                     # 现有：stateless run
POST /api/v1/sessions/{id}/runs       # 新增：session-scoped run
```

P2 以后，只有处于 durable waiting 且已经提交 checkpoint 的 run 才能跨重启恢复；崩溃时仍在执行模型或同步工具的 activation 继续使用 `run_interrupted`，不能假装从任意指令位置恢复。

## 3. P0：多轮 Session

### 3.1 Session 只保存规范化对话

Session 不保存 `AgentLoop` 的全部内部消息。模型中间 reasoning、tool call fragment 和供应商私有字段已经存在 RunEvent 中，不应默认回灌下一轮模型上下文。

Session message 第一版只保留：

```rust
pub struct SessionMessage {
    pub message_id: MessageId,
    pub session_id: SessionId,
    pub ordinal: u64,
    pub role: ConversationRole,       // user | assistant | system_note
    pub content: Vec<ContentPart>,
    pub source_run_id: Option<RunId>,
    pub created_at_ms: i64,
}

pub enum ContentPart {
    Text { text: String },
    BlobRef { blob_id: BlobId, media_type: String },
}
```

P0 只启用 text；现在就用 tagged union，避免以后为图片、文档和音频重做消息表。

默认上下文规则：

- 用户提交时立即追加一条 user message；
- run 成功后追加最终 assistant output；
- run failed/cancelled 不自动把 partial output 加入下一轮上下文；
- reasoning 和工具执行详情只进入 run 历史，除非后续 ContextPolicy 明确选择摘要；
- system prompt 来自 Agent 配置，不重复写进每个 Session。

### 3.2 Session 状态

```rust
pub struct SessionSnapshot {
    pub session_id: SessionId,
    pub agent_profile: String,
    pub status: SessionStatus,
    pub title: Option<String>,
    pub revision: u64,
    pub next_message_ordinal: u64,
    pub active_run_id: Option<RunId>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

pub enum SessionStatus {
    Active,
    Archived,
}
```

“Idle/Running”由 `active_run_id` 表达，不与用户可控制的 `Active/Archived` 混成一个枚举。第一版每个 Session 只允许一个 active run，不同 Session 可以并行。

### 3.3 revision 与幂等

每个会修改 Session 的命令都携带 `expected_revision`：

```rust
pub struct BeginSessionRun {
    pub session_id: SessionId,
    pub run_id: RunId,
    pub expected_revision: u64,
    pub idempotency_key: String,
    pub request_hash: String,
    pub input: Vec<ContentPart>,
    pub created_at_ms: i64,
}

pub struct BeginRunResult {
    pub session: SessionSnapshot,
    pub run_id: RunId,
    pub context_through_ordinal: u64,
    pub replayed: bool,
}
```

约束：

- revision 不匹配返回 `revision_conflict`；
- `active_run_id != None` 返回 `session_busy`；
- 同一 `(session_id, idempotency_key)` 重放返回同一个 `run_id`；
- 相同幂等键、不同 input 返回 `idempotency_conflict`；
- Session archive 后不再接受新 run，但历史仍可查询。

### 3.4 原子边界

创建 Session run 不能由 Gateway 分别执行“写消息、占用 Session、创建 Run”三个无关调用，否则任一步崩溃都会留下半状态。Store 契约需要提供复合事务：

```rust
pub trait SessionStore: Send + Sync + 'static {
    fn create_session(&self, command: CreateSession)
        -> StoreFuture<'_, SessionSnapshot>;

    fn begin_run(&self, command: BeginSessionRun, initial_run: RunSnapshot)
        -> StoreFuture<'_, BeginRunResult>;

    fn finalize_run(&self, command: FinalizeSessionRun)
        -> StoreFuture<'_, SessionSnapshot>;

    fn get_session(&self, id: SessionId)
        -> StoreFuture<'_, Option<SessionSnapshot>>;

    fn messages(&self, id: SessionId, before: Option<u64>, limit: usize)
        -> StoreFuture<'_, Vec<SessionMessage>>;
}
```

`begin_run` 在同一事务中：

1. 校验 revision、busy 和 idempotency；
2. 创建初始 `RunSnapshot`；
3. 追加 user message；
4. 设置 `active_run_id`；
5. 增加 Session revision。

`finalize_run` 在 run terminal event 已经落库后执行：

- completed：追加 assistant message并释放 `active_run_id`；
- failed/cancelled：只释放 `active_run_id`；
- 操作按 `(session_id, run_id)` 幂等；
- Server 启动时扫描“run 已终态但 Session 仍 busy”的记录并重放 finalize，修复两个事务之间的崩溃窗口。

SQLite adapter 应与当前 `runs/run_events` 使用同一个数据库文件，确保 `begin_run` 可以真正原子提交。`SessionStore` 是新端口，不把 SQLite 类型引入 Harness。

建议的新增表：

```text
sessions
  session_id, agent_profile, status, title, revision,
  next_message_ordinal, active_run_id, created_at_ms, updated_at_ms,
  snapshot_json

session_messages
  session_id, ordinal, message_id, role, source_run_id,
  content_json, created_at_ms
  primary key (session_id, ordinal)
  unique (message_id)

session_runs
  session_id, run_id, idempotency_key, request_hash,
  context_through_ordinal, finalized_at_ms
  unique (session_id, idempotency_key)
  unique (run_id)
```

RunSnapshot 不增加必填 `session_id`，避免破坏独立 run；关联由 `session_runs` 保存。`request_hash` 使用规范化 input 和影响执行语义的选项计算，用于识别同一幂等键被不同请求误用。

### 3.5 ContextBuilder

Session Store 负责事实，`ContextBuilder` 负责选择送给 Agent 的上下文：

```rust
pub trait ContextBuilder {
    fn build(&self, request: BuildContextRequest) -> ContextFuture;
}

pub struct BuildContextRequest {
    pub session_id: SessionId,
    pub through_message_ordinal: u64,
    pub model_context_window: Option<u32>,
    pub reserved_output_tokens: Option<u32>,
}
```

`begin_run` 返回追加本次 user message 之前的 `context_through_ordinal`。ContextBuilder 只读取不超过该 ordinal 的历史，当前 input 仍由 Agent 追加一次，避免同一 user message 被重复送给模型。

P0 策略保持确定性：按 ordinal 读取最近消息，受最大消息数和最大字符数约束。P1 再加入 token estimator 和 summary segment。摘要必须作为新记录保存并标明覆盖的 revision 范围，不能覆盖或删除原始消息。

Harness 的 `RunRequest` 增加只读 `prior_messages`，Agent 仍负责加入自己的 system prompt 和当前 user input：

```rust
pub struct RunRequest {
    pub run_id: RunId,
    pub input: String,
    pub prior_messages: Vec<ModelMessage>,
    pub cancellation: RunCancellation,
}
```

无 Session 的现有入口传空数组，因此不会产生行为分支。

### 3.6 Session HTTP API

```text
POST   /api/v1/sessions
GET    /api/v1/sessions/{session_id}
GET    /api/v1/sessions/{session_id}/messages?before=&limit=
POST   /api/v1/sessions/{session_id}/runs
POST   /api/v1/sessions/{session_id}/archive
```

新的 Session submit 使用“接受命令”和“订阅事件”两个请求，避免把 run 身份绑在一个不可重放的 POST SSE 连接上：

```text
POST /sessions/{id}/runs -> 202 RunAccepted {
  session_id, run_id, session_revision, replayed
}
GET /runs/{run_id}/events?after_seq=0 -> existing SSE
```

当前 `POST /api/v1/runs { stream: true }` 为兼容前端继续保留。历史状态继续使用已有 run 查询接口。

### 3.7 SessionCoordinator

Session 入口不能继续直接调用当前“自动生成 RunId 并自行 create snapshot”的 `RunRuntime::start`。增加应用层 Coordinator：

```text
allocate run_id
  -> SessionStore.begin_run(initial RunSnapshot)
  -> ContextBuilder.build(through=context_through_ordinal)
  -> RunRuntime.start_persisted(run_id, input, prior_messages)
  -> return RunAccepted
```

需要给 Harness/RunRuntime 增加显式 `run_id` 的启动入口；现有 stateless `start(input)` 仍可作为“分配 ID + create + start_persisted”的便利封装。

如果 begin_run 之后构建上下文或启动 executor 失败，Coordinator 必须给该 run 追加稳定失败终态并幂等 finalize Session。run 结束后的处理从当前匿名 cleanup closure 演进为明确的生命周期观察端口：

```rust
pub trait RunLifecycleObserver {
    fn terminal(&self, snapshot: RunSnapshot) -> ObserverFuture;
}
```

Session observer 负责 `finalize_run`，审批 adapter 负责释放进程内资源；观察者失败进入持久化 retry/outbox，不能反向改写已经提交的 run 终态。

## 4. P1：Context、Memory、压缩与 Skill 编排

这四项能力必须先于 durable Flow Runtime，因为 checkpoint 需要保存已经解析的 Skill 版本和上下文来源，resume 也必须知道哪些内容可以重建。它们属于 Agent 上层的 orchestration plane，不属于 Provider，也不应硬编码进 `AgentLoop`。

完整的调用顺序：

```text
RunSubmission
  -> SkillOrchestrator.resolve
       └-> SkillStore
  -> ContextEngine.build
       ├-> Session messages
       ├-> Skill instructions/resources
       ├-> MemoryRetriever -> MemoryStore / indexes
       ├-> TokenEstimator
       ├-> ContextArtifactStore
       └-> ContextCompressor -> SummaryGenerator
  -> PolicyEngine.intersect capabilities
  -> ExecutionPlan
  -> Harness / AgentLoop
```

### 4.1 Skill 是上层能力包，不是 Tool

三者必须区分：

| 对象 | 作用 | 是否直接执行副作用 |
|---|---|---|
| Skill | 描述完成某类任务的方法、资源、上下文策略和工具需求 | 否 |
| Tool | 一个可调用的原子能力 | 可以，受风险与审批策略约束 |
| Flow | Skill/Agent 在一次 run 中形成的执行、等待和恢复过程 | 通过 Effect 间接执行 |

Skill 可以请求使用某些 Tool，但不能授予权限。最终工具集合必须取交集：

```text
registered tools
∩ agent profile allow-list
∩ host/user policy
∩ skill requested tools
∩ current-run risk constraints
```

Skill instruction 同样不是系统安全策略。Host policy 永远位于最高优先级，Skill 内容不能覆盖审批、路径、网络、数据隔离或资源预算。

### 4.2 Skill package 与 manifest

第一版 Skill 是版本化、只读、可计算 digest 的目录包：

```text
skills/<name>/
├── SKILL.md
├── skill.toml
├── references/
├── schemas/
└── scripts/          # 可选；P4 前只作为资源，不自动执行 JS
```

```rust
pub struct SkillManifest {
    pub skill_id: SkillId,
    pub version: String,
    pub description: String,
    pub compatibility: SkillCompatibility,
    pub activation: SkillActivation,
    pub required_tools: Vec<String>,
    pub optional_tools: Vec<String>,
    pub required_capabilities: Vec<CapabilityName>,
    pub context: SkillContextPolicy,
    pub resources: Vec<SkillResourceRef>,
}

pub enum SkillActivation {
    ExplicitOnly,
    ProfileDefault,
    Routable { hints: Vec<String> },
}
```

Manifest 负责机器可校验的元数据，`SKILL.md` 负责给 Agent 的行为指令。Loader 必须：

- 限制单文件、总包大小和引用深度；
- 禁止路径逃逸和循环依赖；
- 对 manifest、instructions 和资源计算统一 digest；
- 校验版本、schema 和 Harness 兼容范围；
- 默认不执行包内脚本，不开放 npm、网络或环境变量。

### 4.3 SkillStore 契约

Registry 不直接读取目录。Skill 的存取首先收口成独立端口：

```rust
pub trait SkillStore: Send + Sync + 'static {
    fn descriptor(&self) -> SkillStoreDescriptor;
    fn capabilities(&self) -> SkillFuture<SkillStoreCapabilities>;
    fn list(&self, query: SkillStoreQuery)
        -> SkillFuture<Vec<SkillDescriptor>>;
    fn get(&self, locator: SkillLocator)
        -> SkillFuture<Option<SkillPackage>>;
    fn put(&self, command: PutSkillPackage)
        -> SkillFuture<SkillPackageRef>;
    fn delete(&self, command: DeleteSkillPackage)
        -> SkillFuture<()>;
}

pub enum SkillLocator {
    IdVersion { skill_id: SkillId, version: String },
    Locked { skill_id: SkillId, version: String, digest: String },
}
```

约束：

- descriptor 提供稳定逻辑 identity、adapter kind 和版本，不包含本地绝对路径、远端凭据或连接串；
- `get(Locked)` 必须精确匹配 digest，不能返回“当前最新版本”；
- package 内容包含规范化 manifest、instructions 和具名资源，不向上暴露本地绝对路径；
- `put/delete` 使用 expected digest 做乐观并发；只读实现通过 capabilities 明确拒绝写入；
- Store 只保存和读取内容，不解析依赖、不选择 Skill、不授予 capability；
- 同一 `skill_id + version` 出现不同 digest 时必须报冲突。

第一版提供 filesystem adapter；后续可替换或组合：

```text
FilesystemSkillStore   本地开发、目录热加载
EmbeddedSkillStore     随单二进制发布的内置 Skill，只读
SqliteSkillStore       桌面端安装和版本管理
RemoteSkillStore       企业仓库或对象存储
LayeredSkillStore      按显式 mount 顺序组合多个 Store
```

`LayeredSkillStore` 的 mount 配置包含 store name、优先级、trust level 和允许的 Skill scope。不同 Store 的冲突不能依靠目录顺序静默覆盖。

### 4.4 Registry、Selector、Resolver、Compiler

Skill 上层编排拆成四个职责：

```text
Registry   已安装什么、版本和 digest
Selector   本次 run 想使用哪些 Skill
Resolver   解析依赖、版本、冲突和资源预算
Compiler   生成指令层、资源引用和工具需求
```

```rust
pub trait SkillRegistry {
    fn list(&self, query: SkillQuery) -> SkillFuture<Vec<SkillDescriptor>>;
    fn load(&self, skill: SkillRef) -> SkillFuture<SkillPackage>;
}

pub trait SkillSelector {
    fn select(&self, request: SkillSelectionRequest)
        -> SkillFuture<Vec<SkillSelection>>;
}

pub struct SkillSelectionRequest {
    pub run_id: RunId,
    pub agent_profile: String,
    pub explicit_skills: Vec<SkillRef>,
    pub current_input: Vec<ContentPart>,
    pub available_capabilities: Vec<CapabilityName>,
    pub max_skills: u32,
}

pub struct ResolvedSkill {
    pub skill_id: SkillId,
    pub version: String,
    pub digest: String,
    pub activation_reason: SkillActivationReason,
    pub instruction: String,
    pub resources: Vec<ResolvedResource>,
    pub requested_tools: Vec<String>,
}
```

Registry 是 `SkillStore` 之上的索引、缓存和可见性服务。缓存 key 必须包含 store identity 和 digest；Selector/Resolver/Compiler 只依赖 Registry/contract，不知道 Skill 来自目录、SQLite 还是远端。

选择优先级：

1. API/用户显式选择；
2. Agent profile 的固定默认 Skill；
3. 受控 router 自动选择；
4. 不允许模型在循环中绕过 Orchestrator 临时加载未批准 Skill。

自动 router 第一版使用确定性的关键词/规则和 allow-list。将来可以增加模型路由，但路由输出仍必须经过 Resolver、权限和预算校验。

### 4.5 Skill pinning 与恢复

Skill 解析结果在 run 开始时冻结：

```rust
pub struct SkillLock {
    pub skills: Vec<LockedSkill>,      // id + version + digest
    pub compiler_version: u32,
    pub compiled_instruction_digest: String,
}
```

- RunSnapshot 或关联的 execution manifest 保存 `SkillLock`；
- Skill 文件在 run 中途更新不会改变正在运行的任务；
- P2 checkpoint 只保存 lock/ref，不把任意目录路径当作恢复依据；
- resume 时 digest 不存在返回 `skill_artifact_missing`，内容不同返回 `skill_digest_mismatch`；
- 是否允许迁移到新版本必须由显式 migration policy 决定，不能静默替换。

### 4.6 Context Engine 的输入输出

P0 的简单 `ContextBuilder` 在 P1 演进为独立 `ContextEngine`：

```rust
pub struct ContextBuildRequest {
    pub run_id: RunId,
    pub session_id: Option<SessionId>,
    pub through_message_ordinal: Option<u64>,
    pub current_input: Vec<ContentPart>,
    pub agent_profile: String,
    pub model_profile: String,
    pub resolved_skills: Vec<ResolvedSkill>,
    pub policy: ContextPolicy,
}

pub struct ContextPack {
    pub instruction_layers: Vec<InstructionLayer>,
    pub messages: Vec<ModelMessage>,
    pub memory_refs: Vec<MemoryRef>,
    pub artifact_refs: Vec<ContextArtifactRef>,
    pub skill_lock: SkillLock,
    pub effective_tools: Vec<String>,
    pub budget: ContextBudgetReport,
    pub fingerprint: String,
}
```

`ContextPack` 是 Provider 调用之前的唯一规范化输入。AgentLoop 不再自行读取 Session、Skill 目录或 Memory 数据库，只消费已经构建并通过策略校验的 Pack。

指令层按稳定顺序渲染：

```text
1. Host security/policy                 不可被后续层覆盖
2. Agent profile/system instruction
3. Resolved Skill instructions
4. Context artifacts and retrieved memory
5. Canonical recent conversation
6. Current user input
```

每一层保留 `source`、`trust_level` 和 digest。检索到的网页、文件、Memory 和模型摘要默认是 untrusted context，不因为进入 prompt 就升级为 system authority。

### 4.7 Context budget

Context Engine 使用显式预算，不通过“拼完以后从头截断”处理超长上下文：

```rust
pub struct ContextBudget {
    pub model_context_tokens: u32,
    pub reserved_output_tokens: u32,
    pub reserved_tool_schema_tokens: u32,
    pub max_skill_tokens: u32,
    pub max_memory_tokens: u32,
    pub max_history_tokens: u32,
}
```

保留优先级：

1. Host policy、当前输入、输出预算；
2. 当前未完成任务和最近必要工具结果；
3. Agent profile 与显式选择的 Skill；
4. 最近 Session 消息；
5. 高置信 Memory；
6. 老历史摘要和自动路由 Skill 的低优先资源。

超预算时返回完整 `ContextBudgetReport`：选中了什么、压缩了什么、丢弃了什么以及估算误差。Context fingerprint 由 policy version、所有来源 digest 和最终排序计算，写入 run execution manifest，便于调试同一输入为何产生不同结果。

### 4.8 Memory 契约与分层

Memory 不能成为“把所有聊天内容再存一遍”的无边界数据库。明确区分：

| 层 | 内容 | 权威 Store |
|---|---|---|
| Conversation history | 原始 user/assistant 消息 | SessionStore |
| Working state | 当前 run 消息、step、等待点 | Checkpoint/RunStore |
| Long-term semantic memory | 稳定事实、偏好、项目约束 | MemoryStore |
| Episodic memory | 某次任务的结构化结论或摘要 | MemoryStore |
| Procedural knowledge | 如何完成一类任务 | Skill package |

因此“操作流程”进入 Skill，“当前执行到了哪里”进入 checkpoint，“用户曾明确表达的稳定偏好”才适合进入 long-term memory。

```rust
pub struct MemoryRecord {
    pub memory_id: MemoryId,
    pub scope: MemoryScope,
    pub kind: MemoryKind,              // semantic | episodic
    pub content: Vec<ContentPart>,
    pub source_refs: Vec<MemorySourceRef>,
    pub confidence: f32,
    pub salience: f32,
    pub version: u64,
    pub expires_at_ms: Option<i64>,
    pub supersedes: Option<MemoryId>,
    pub created_at_ms: i64,
}

pub trait MemoryStore: Send + Sync + 'static {
    fn descriptor(&self) -> MemoryComponentDescriptor;
    fn put(&self, command: PutMemory) -> MemoryFuture<MemoryRecord>;
    fn get(&self, locator: MemoryLocator) -> MemoryFuture<Option<MemoryRecord>>;
    fn list(&self, query: MemoryListQuery) -> MemoryFuture<MemoryPage>;
    fn supersede(&self, command: SupersedeMemory) -> MemoryFuture<MemoryRecord>;
    fn forget(&self, command: ForgetMemory) -> MemoryFuture<()>;
}

pub trait MemoryRetriever: Send + Sync + 'static {
    fn descriptor(&self) -> MemoryComponentDescriptor;
    fn retrieve(&self, request: MemoryRetrieveRequest)
        -> MemoryFuture<Vec<RankedMemory>>;
}

pub trait MemoryExtractor: Send + Sync + 'static {
    fn descriptor(&self) -> MemoryComponentDescriptor;
    fn extract(&self, request: MemoryExtractionRequest)
        -> MemoryFuture<Vec<MemoryCandidate>>;
}

pub trait MemoryWritePolicy: Send + Sync + 'static {
    fn descriptor(&self) -> MemoryComponentDescriptor;
    fn decide(&self, candidate: MemoryCandidate)
        -> MemoryFuture<MemoryWriteDecision>;
}

pub enum MemoryWriteDecision {
    Accept { normalized: PutMemory },
    Reject { reason: String },
    RequireApproval { request: MemoryApprovalRequest },
}
```

四个端口的边界必须保持独立：

- `MemoryStore` 只负责记录的持久化、版本、分页、supersede 和 forget，不负责相关性搜索或排序；
- `MemoryRetriever` 根据当前输入和 scope 返回 `RankedMemory`；每个结果携带不可变的 `MemoryRecord` 版本、score、score explanation 和 index/strategy version；
- `MemoryExtractor` 只从允许的 Session message、RunEvent 或 artifact 中提出 `MemoryCandidate`，没有可信写权限；
- `MemoryWritePolicy` 根据来源、scope、敏感性、置信度和 Host policy 返回接受、拒绝或要求审批，只有接受结果才能交给 Store；
- Context Engine 只调用 `MemoryRetriever`，不根据 Store 类型自行拼 SQL、向量查询或排序。

`MemoryRetrieveRequest` 使用文本、scope、kind、时间/来源过滤和 `top_k` 等 provider-neutral 字段，不在核心 DTO 中暴露 embedding vector、SQLite rowid 或某个向量数据库的 filter 语法。FTS、vector、hybrid、reranker 都是 `MemoryRetriever` 的不同实现；它们可以在内部组合 `MemoryStore`、lexical index、embedding adapter 和 vector index。SQLite 第一版可以由同一个 adapter 同时实现 `MemoryStore` 与 FTS `MemoryRetriever`，但两个契约和测试仍然分开。

P1 先实现 SQLite FTS/关键词检索和确定性排序；embedding 是独立可选 adapter，不能让核心契约依赖某个向量数据库。

Memory 写入规则：

- 模型只能产生 `MemoryCandidate`，不能直接写入可信 Memory；
- 每个 candidate 必须经过 `MemoryWritePolicy`，不能由 extractor 或 post-run hook 直接调用 Store；
- 每条 Memory 必须能追溯到 Session message、RunEvent 或用户显式输入；
- 冲突事实通过新版本/supersede 表达，不在原记录上静默改写；
- 支持过期、forget 和按 scope 隔离；API Key、reasoning、临时工具输出默认禁止写入。

P1 先支持用户/Host 显式写入和受策略控制的同步 post-run extraction。P3 有 Job Runtime 后，再把大规模提取、合并和 embedding 生成迁移为 durable background job；Memory 契约和检索路径不因此改变。

所有实现必须通过同一套 Memory contract fixtures，至少覆盖 scope 隔离、乐观版本、supersede、forget、稳定分页、source refs 保留和重复写入幂等。Store identity、Retriever 名称/版本、命中的 memory id/version 和排序说明进入 context manifest/fingerprint；identity 是不含数据库路径或凭据的逻辑标识。

### 4.9 上下文压缩契约与摘要

压缩不修改 Session 原始事实，而是生成可缓存的派生 artifact：

```rust
pub struct ContextArtifact {
    pub artifact_id: ContextArtifactId,
    pub kind: ContextArtifactKind,     // summary | extracted_facts | reduced_tool_result
    pub source_refs: Vec<ContextSourceRef>,
    pub covered_range: Option<MessageRange>,
    pub policy_version: u32,
    pub generator: ArtifactGenerator,
    pub content: Vec<ContentPart>,
    pub source_digest: String,
    pub content_digest: String,
    pub created_at_ms: i64,
}

pub trait ContextCompressor: Send + Sync + 'static {
    fn descriptor(&self) -> ContextComponentDescriptor;
    fn compress(&self, request: CompressionRequest)
        -> ContextFuture<CompressionResult>;
}

pub trait TokenEstimator: Send + Sync + 'static {
    fn descriptor(&self) -> ContextComponentDescriptor;
    fn estimate(&self, request: TokenEstimateRequest)
        -> ContextFuture<TokenEstimate>;
}

pub trait SummaryGenerator: Send + Sync + 'static {
    fn descriptor(&self) -> ContextComponentDescriptor;
    fn summarize(&self, request: SummaryRequest)
        -> ContextFuture<SummaryResult>;
}

pub trait ContextArtifactStore: Send + Sync + 'static {
    fn descriptor(&self) -> ContextComponentDescriptor;
    fn find_reusable(&self, query: ReusableArtifactQuery)
        -> ContextFuture<Vec<ContextArtifactRef>>;
    fn get(&self, artifact: ContextArtifactRef)
        -> ContextFuture<Option<ContextArtifact>>;
    fn put(&self, command: PutContextArtifact)
        -> ContextFuture<ContextArtifactRef>;
    fn invalidate(&self, command: InvalidateContextArtifacts)
        -> ContextFuture<u64>;
}
```

`CompressionRequest` 至少包含：

- 带稳定 source refs/digests 的候选项及其 `required | high | normal | low` 优先级；
- 可用于本次模型的 token budget、保留的输出/tool schema 预算和 model profile；
- 已验证 source digest 仍有效的 reusable artifacts；
- context/compression policy version 和调用限制。

`CompressionResult` 必须完整返回：

- retained items 及最终顺序；
- dropped items、原因和原始 source ref；
- 待保存的 `ContextArtifactCandidate`，而不是已经写库的 artifact；
- 估算输入 token、误差/置信区间和最终余量；
- compressor 名称/版本、summary generator 版本和 result fingerprint。

Context Engine 先通过 `ContextArtifactStore` 查找可复用产物，再调用 Compressor，验证结果后统一落库 artifact candidate。Compressor 自身不直接修改 Session、不直接写 Store，也不把摘要自动写成 Memory。这样同一个 `SlidingWindowCompressor` 可以搭配 SQLite、对象存储或纯内存 fake，压缩失败也不会产生半写入状态。

实现必须遵守以下不变量：

- Host policy、当前用户输入和所有标记为 `required` 的项不能被丢弃；它们本身已超预算时明确返回 `context_budget_exceeded`；
- `TokenEstimator` 是独立契约，允许按 model profile 替换启发式估算或精确 tokenizer，Compressor 不能硬编码某个 Provider tokenizer；
- `SummaryGenerator` 由构造参数注入；模型摘要 adapter 可以复用底层模型客户端，但 Compressor 不直接依赖 Provider；
- 一次模型摘要调用拥有独立 timeout、usage attribution、输入/输出 digest 和失败策略，且不能递归调用 Context Engine；
- source digest、policy version、strategy version 或 generator version 不匹配时，旧 artifact 不可复用。

内置策略按同一个 `ContextCompressor` 契约提供：

```text
NoopFail       只做预算验证，超限即失败，适合测试和严格模式
SlidingWindow  确定性保留 required 与最近消息，不调用模型
Summary        复用或生成分段摘要，并保留 source refs
Hybrid         大工具结果缩减 + 已有摘要 + 最近窗口 + 必要时新摘要
```

压缩阶梯：

1. 去除默认不进入对话上下文的内部 token/tool delta；
2. 对大型工具输出保存原始 artifact，只注入结构化摘要和引用；
3. 合并已有、source digest 仍有效的 Session summary；
4. 保留最近若干完整轮次；
5. 必要时生成更高层摘要；
6. 仍超限则明确返回 `context_budget_exceeded`，不静默截断 Host policy 或当前输入。

摘要必须记录覆盖范围、生成模型/算法、policy version 和 digest。源消息变化后旧摘要失效；模型生成摘要属于可能有误的派生数据，不能覆盖用户原话，也不能自动升级为高置信 Memory。

模型参与压缩时，它是一次明确的 orchestration 调用：必须有独立 timeout、usage 归属、输入 digest 和失败策略，且不能递归触发同一个 Context Engine。第一版优先做确定性裁剪/复用已有摘要，再按配置启用模型摘要器。

策略切换只发生在 Server composition root/config：替换 Compressor、Retriever、Store、Estimator 或 SummaryGenerator 不应修改 SessionStore、AgentLoop 或 Provider 协议。选择结果的逻辑 component identity、策略名称/版本、输入 source digests 和产物 digest 全部进入 execution manifest 与 context fingerprint，保证重启、回放和问题定位时可解释。

### 4.10 模型 Token usage 与 estimator 的职责

上下文预算估算和模型调用后的实际用量是两件事。`TokenEstimator` 始终负责调用前预算；调用结束后，Provider 返回的 usage 只有在字段完整且满足 `total = input + output` 时才作为权威数据。缺失或不自洽的字段，才按照实际发送给模型的消息、工具定义、最终 reasoning 与正文调用 estimator 补齐，不能使用原始用户输入或预估的最大输出代替真实内容。

最终 `TokenUsage` 记录来源：

- `provider_reported`：输入、输出和总量全部来自完整且自洽的 Provider usage；
- `mixed`：保留可比较的 Provider 字段，仅对缺失或不自洽部分使用 estimator；
- `estimator_fallback`：Provider 未返回可用 usage，输入和输出均由 estimator 计算。

usage 的选择与补齐属于 Provider adapter 的规范化职责；Harness、Context Engine 和 UI 只消费统一的 `TokenUsage` 契约。execution manifest 同时记录 `provider_usage_preferred=true` 与 fallback 类型，以便回放时解释计费数字的来源。

### 4.10 ExecutionPlan 与 Harness 边界

上层编排最终产生：

```rust
pub struct ExecutionPlan {
    pub run_id: RunId,
    pub context: ContextPack,
    pub skill_lock: SkillLock,
    pub component_lock: OrchestrationComponentLock,
    pub allowed_tools: Vec<String>,
    pub limits: RunLimits,
    pub plan_fingerprint: String,
}
```

`OrchestrationComponentLock` 保存各组件的逻辑 identity、adapter/strategy 名称、schema/策略版本和非敏感配置 digest。它不是要求永远恢复旧进程对象，而是让 replay/resume 能验证“当前装配是否与原 run 兼容”；不兼容时显式迁移或失败，不能悄悄改用新检索/压缩策略。

`allowed_tools` 是 PolicyEngine 计算后的结果，不是 Skill manifest 原样复制。Harness 的输入演进为 Provider-neutral `RunContext`；Context 与 Skill 编排都在 `agent-core` 内通过端口协作，由 orchestration 层把 `ContextPack` 映射为 Harness DTO。

AgentLoop 仍使用 Host 注册的 `ToolRegistry`，但每次模型请求只暴露 `allowed_tools` 对应定义，实际执行前再次校验工具名仍在该 run 的 allow-set 中。只在 prompt 中隐藏工具不是权限控制。

stateless `/runs` 同样走 Orchestrator，只是 `session_id=None`、没有 Session history；不能维护两套 Agent 调用链。

## 5. P2：AgentMachine 与 durable checkpoint

Session 和上层上下文编排完成后，再解决等待审批时释放 Future。P2 在保留 `Agent` 兼容层的同时新增显式状态机。由于模型输出仍需流式传递，接口不能简化成只返回一个 `StepOutcome` 的 Future，而应返回“多个语义事件 + 唯一 yield”的流：

```rust
pub trait AgentMachine: Send + Sync + 'static {
    fn metadata(&self) -> AgentMetadata;
    fn start(&self, request: MachineStartRequest) -> MachineStream;
    fn resume(&self, request: MachineResumeRequest) -> MachineStream;
}

pub struct MachineResumeRequest {
    pub run_id: RunId,
    pub checkpoint: CheckpointEnvelope,
    pub inbox: Vec<WorkflowEvent>,
    pub cancellation: RunCancellation,
}

pub enum MachineOutput {
    Event(AgentEvent),
    Yield(StepOutcome),
}

pub enum StepOutcome {
    Continue {
        checkpoint: CheckpointEnvelope,
        effects: Vec<EffectRequest>,
    },
    Suspend {
        checkpoint: CheckpointEnvelope,
        waits: Vec<WaitSpec>,
        effects: Vec<EffectRequest>,
    },
    Complete {
        finish_reason: FinishReason,
    },
    Failed {
        error: MachineError,
    },
}
```

每次 `MachineStream` 必须且只能产生一个 `Yield`，之后结束；文本、reasoning 和工具进度在它之前持续流式输出。Runtime 将 `Complete/Failed` 映射为现有唯一终态，将 `Suspend` 映射为 durable waiting，而不是让 Agent 自行修改 Store。

### 5.1 Checkpoint 是 Agent 所有的版本化数据

```rust
pub struct CheckpointEnvelope {
    pub agent_kind: String,
    pub schema_version: u32,
    pub codec: CheckpointCodec,       // json first
    pub payload: Value,
}
```

Store 不解释 payload；Agent 必须负责版本兼容或显式迁移。checkpoint 不允许包含 API Key、文件句柄、socket、Future、Promise、进程 ID 或临时路径 capability。

`AgentLoop` 第一版 checkpoint 至少包含：

- provider-neutral `Vec<ModelMessage>`；
- P1 生成的 context fingerprint、SkillLock、Memory/artifact refs；
- 当前 loop step 与预算消耗；
- 已完成工具结果；
- 正在等待的 approval/job correlation ID；
- agent/checkpoint schema version。

### 5.2 Run 的执行态与持久态分开

当前 `RunStatus` 扩展为：

```text
Accepted -> Runnable -> Running
                         ├-> WaitingEvent -> Runnable
                         ├-> Completed
                         ├-> Failed
                         └-> Cancelled
```

- `Running` 表示当前持有 activation lease；
- `WaitingEvent` 必须已经持久化 checkpoint 和 wait specs，不占 Tokio task；
- `Runnable` 表示 inbox 已有唤醒事件，等待 worker 获取 lease；
- Server 重启时 `WaitingEvent` 保持等待，`Runnable` 重新入队；
- 没有 durable checkpoint 的 `Running` 仍终结为 `run_interrupted`。

这里不新增 `FlowId`：第一版 Flow Runtime 管理的就是一个可跨 activation 的 Run。只有未来需要一个 DAG 同时编排多个独立 run 时，才增加更高层的 `FlowId`，不提前制造重复身份。

### 5.3 跨 activation 的事件序号

同一个 run resume 后不能重新从 `seq = 1` 开始，也不能再次发送 `RunStarted`。需要把当前 per-Future sequencer 演进为 activation-aware sequencer：

```text
activation 1: RunStarted(seq=1) ... RunWaiting(seq=20)
activation 2: RunResumed(seq=21) ... RunCompleted(seq=42)
```

新增稳定 RunEvent：

```rust
RunWaiting {
    checkpoint_revision: u64,
    wait_count: u32,
}
RunResumed {
    activation_id: ActivationId,
    checkpoint_revision: u64,
}
```

Runtime 从 Store 读取 `last_seq` 后为新 activation 创建 sequencer；activation lease 保证同一时刻只有一个 writer，Store 的 `last_seq + 1` 校验仍是最终防线。

### 5.4 Suspend 事务

一次 suspend 必须原子完成：

```text
save checkpoint
create waits/subscriptions
persist requested effects and transactional outbox
set run = WaitingEvent
release activation lease
append run_waiting event
```

如果事件先于订阅到达，dispatcher 必须能够从持久化 event log 匹配；不能依赖“先订阅、后发生”的时间运气。

唤醒事务：

```text
dedupe (subscription_id, event_id)
append event to run inbox
complete once-subscription
set run = Runnable
enqueue outbox wake item
```

worker 获取有期限的 lease 后调用 `AgentMachine::resume`。lease 超时可被其他 worker 重领，因此 resume 和所有 Effect 必须携带幂等键。

### 5.5 取消、deadline 与预算

- WaitingEvent run 收到 cancel 时，在一个事务中取消 waits、清空 runnable outbox 并追加 `RunCancelled`；
- activation 的 model/tool timeout 只计算实际执行时间，等待用户或 Timer 的时间不消耗它；
- run 可以额外配置持久化 `overall_deadline_at`，到期由 Timer 产生取消 Command；
- max steps、token usage、effect 次数等累计预算保存在 checkpoint，resume 后不能重置；
- 迟到的 approval/job event 可以进入审计日志，但不能重新唤醒已终态 run。

## 6. P3：统一 Event、Job 和 Timer

P2 先用审批打通一条最窄的 durable wait，再抽象为通用事件系统。事件契约沿用 [独立事件运行时设计](./06-event-runtime-subscriptions.md)：

```rust
pub struct WorkflowEvent {
    pub event_id: EventId,
    pub topic: String,
    pub schema_version: u32,
    pub source: EventSource,
    pub subject: Option<String>,
    pub correlation_id: Option<String>,
    pub causation_id: Option<EventId>,
    pub payload: Value,
}
```

第一批可信 Command：

```text
ResolveApproval -> approval.resolved
CompleteJob     -> job.completed | job.failed
FireTimer       -> timer.fired
CancelRun       -> run.cancel_requested
```

Agent 可以请求 Effect，但不能伪造可信 Event：

```text
RequestApproval
StartJob
ScheduleTimer
PublishAgentEvent     # 仅允许 agent.* topic
Suspend
```

投递语义采用 at-least-once + 幂等消费，不承诺 exactly-once。高频 `output_delta` 继续保留在 RunEvent Store，不复制进通用 EventLog。

## 7. P4：Rust/JS 统一流程接口

Rust Agent、模型工具和 QuickJS binding 都必须调用同一个 command handler：

```text
Rust FlowClient ───────┐
Model built-in tools ──┼─> FlowCommandBus -> Flow Runtime
JS mina.* bindings ────┘
```

推荐 host capability：

```text
mina.events.publish / subscribe
mina.timers.schedule
mina.jobs.start / cancel
mina.tools.invoke
mina.run.suspend
```

JS 继续使用独立、可选的 QuickJS adapter。每次 `start()`/`resume()` 都是全新、有资源上限的调用；不恢复旧 Promise 或 VM 栈。JS module 只返回 `StepOutcome`，不直接操作数据库和队列。

## 8. 物理包与模块边界

为避免细粒度 crate 膨胀，当前 workspace 已收敛为两个内部库：

```text
crates/
├── core/                       # agent-core
│   └── src/
│       ├── harness/            # Run/Session/AgentLoop 与纯状态机
│       ├── context/            # Context 契约、引擎与默认策略
│       ├── memory/             # Memory 契约与写入协调
│       ├── skill/              # Skill 契约、选择、解析和编译
│       ├── tool/               # Tool port 与跨语言 DTO
│       ├── sandbox.rs          # ProcessSandbox port
│       ├── observability.rs    # vendor-neutral Hook
│       └── script.rs           # ScriptRuntime；QuickJS 可选实现归属
└── extension/                  # agent-extension
    └── src/
        ├── provider/           # OpenAI-compatible 与未来 Provider
        ├── tool/               # 内置 Tool
        ├── sandbox.rs          # Host/强隔离 adapter
        ├── store/              # SQLite/Filesystem adapter
        └── observability.rs    # tracing/Langfuse 等 adapter
```

稳定 DTO、错误分类和 port trait 只放 Core；数据库、文件系统、Provider 和监控 SDK 只放 Extension。即使同一个具体 struct 实现多个 Store 端口，也不能合并端口语义和契约测试。

依赖方向固定为：

```text
apps/server + apps/agent-cli ──> agent-core
              │
              └───────────────> agent-extension ──> agent-core

agent-core -X-> agent-extension / axum / reqwest / rusqlite / tracing
```

QuickJS 属于 Harness 的可选执行能力，因此放在 Core；数据库、事件 Broker、网络与文件能力只能通过显式 capability port 注入。OpenAI-compatible Provider 可在应用装配层实现 `SummaryGenerator`，Context 策略不依赖具体 Provider。

具体选择只在 Server composition root 根据配置完成。默认组合建议是 `FilesystemSkillStore + SqliteMemoryStore/SqliteFtsRetriever + RuleMemoryExtractor + HostMemoryWritePolicy + HybridCompressor + HeuristicTokenEstimator`；测试替换为 in-memory fake，未来替换远端 Store、向量 Retriever 或新的 Compressor 时，不改 Gateway、Session、Harness 和 AgentLoop。

## 9. 关键失败与恢复规则

| 失败点 | 恢复规则 |
|---|---|
| begin Session run 后、后台 task 启动前崩溃 | run 无 checkpoint，启动时 `run_interrupted`，再幂等 finalize Session |
| run terminal 已落库、Session 尚未 finalize | 启动 reconcile 自动 finalize |
| Skill 在 run 开始后被更新或删除 | 使用 SkillLock 的版本和 digest；不可用时明确失败，不静默换版本 |
| Memory 在 ContextPack 构建后被 supersede | 当前 run 使用已记录的 memory version；下一 run 才检索新版本 |
| summary source digest 已变化 | artifact 失效并重新构建；原始 Session message 不受影响 |
| suspend 事务提交前崩溃 | activation lease 到期；无完整 wait 状态时失败或重试当前 activation |
| suspend 事务提交后崩溃 | WaitingEvent + checkpoint 保留，不运行 Future |
| 唤醒事件重复投递 | `(subscription_id, event_id)` 去重，inbox 只写一次 |
| resume worker 崩溃 | lease 到期后重领，同一 activation id 幂等 |
| checkpoint 版本不支持 | `checkpoint_incompatible` 明确失败，不创建空上下文 |
| Session revision 冲突 | 拒绝命令且不修改消息、run 或 active_run_id |

## 10. API 与 UI 流程

P0 的正常聊天链路：

```text
UI -> create/get Session
UI -> submit(input, expected_revision, idempotency_key)
SessionStore -> atomic begin_run
SkillOrchestrator -> resolved SkillLock
ContextEngine -> ContextPack(history + skill + memory + compression)
PolicyEngine -> effective tools and limits
RunRuntime -> current AgentLoop(ExecutionPlan)
UI <- RunAccepted(session_id, run_id, revision)
UI -> GET run events SSE
RunStore -> durable RunEvents
UI <- SSE
Run terminal -> SessionStore.finalize_run
UI refresh -> load Session messages + reconnect active run events
```

P2 的审批链路：

```text
AgentMachine -> RequestApproval + Suspend(checkpoint, wait)
FlowStore -> WaitingEvent
UI <- approval.requested
UI -> ResolveApproval command
EventRuntime -> approval.resolved -> run inbox -> Runnable
worker -> AgentMachine.resume(checkpoint, inbox)
UI <- approval.resolved + following tool events
```

前端只需要理解 Session、RunEvent 和审批视图，不需要理解 checkpoint、lease、subscription cursor 或 JS VM。

## 11. 建议实施切片

### Slice A：Session contract 与 SQLite

- `SessionId`、`MessageId`、`SessionSnapshot`、`SessionMessage`；
- 在 `agent-extension::store` 中让同一 SQLite adapter 实现 `RunStore` 与 `SessionStore` migration；
- `begin_run/finalize_run` 原子和幂等测试；
- startup reconcile；
- 暂不改 UI。

验收：重启后 Session 历史存在；重复 submit 不创建第二个 run；同一 Session 并发提交只有一个成功。

### Slice B：Context Engine 骨架与 Session API（P1）

- `ContextPack`、`ContextPolicy`、`ContextBudgetReport` 和 source provenance；
- `RunRequest`/Harness DTO 接收编排后的上下文；
- AgentLoop/SingleTurn 消费规范化 ContextPack 映射；
- Session CRUD、messages、submit API；
- 当前 Chat UI 保存并恢复 `session_id`。

验收：连续两轮时第二轮模型能看到第一轮 user/assistant；每个 run 可查询 context fingerprint 和预算报告；独立 `/runs` 行为不变。

### Slice C：Skill 上层编排（P1）

- `agent-core::skill` 的 manifest、resource、compatibility、lock 和 `SkillStore`；
- `agent-extension::store` 的 filesystem adapter、in-memory fake、安全 package loader 与共享 contract fixtures；
- Registry 只通过 `SkillStore` 建索引/缓存，不直接读取目录；
- package digest、Store identity、版本 pinning 与冲突检测；
- 显式/Profile/规则 Router 三级 Selector；
- Resolver/Compiler 输出 instruction layers 和 requested tools；
- PolicyEngine 计算 effective tools；
- run execution manifest 保存 SkillLock。

验收：同一个 run resume/replay 始终使用相同 Skill digest；filesystem Store 可替换为 in-memory fake 而 Registry/Selector 不变；所有 Store adapter 通过相同契约测试；Skill 不能绕过工具 allow-list 和风险审批；stateless/session run 走同一 Orchestrator。

### Slice D：Memory 与上下文压缩（P1）

- `agent-core::memory` 的 `MemoryStore/Retriever/Extractor/WritePolicy`、scope、source refs、版本 DTO 和写入协调；
- `agent-extension::store` 的 SQLite Store、FTS Retriever、in-memory fake 与共享 contract fixtures；
- `agent-core::context` 的 `ContextCompressor/TokenEstimator/SummaryGenerator/ContextArtifactStore` 以及 Noop/窗口/摘要/Hybrid 策略；
- `agent-extension::store` 的 SQLite ArtifactStore；
- Context Engine 负责 reusable artifact 查询、预算分区、压缩结果验证与 artifact 提交；
- 模型摘要通过注入的 `SummaryGenerator`，不直接依赖 OpenAI-compatible adapter；
- ContextPack 记录 memory/artifact refs。

验收：长 Session 不超出模型预算且压缩不删除原始消息；Memory 可追溯、可覆盖、可遗忘；替换 Retriever 不改 Context Engine，替换 Compressor 不改 Session/AgentLoop；SQLite 与 in-memory adapter 使用相同契约 fixtures；策略名称/版本、Store identity、source/artifact digest 均进入 fingerprint，同一输入和锁定版本产生确定结果。

### Slice E：AgentMachine + durable approval（P2）

- checkpoint envelope、StepOutcome、activation lease；
- 将高风险工具审批从 oneshot Future 改成 Suspend；
- checkpoint 固定 Context fingerprint、SkillLock 和 Memory/artifact refs；
- approval event、run inbox 和 resume；
- WaitingEvent 重启恢复。

验收：等待审批时重启 Server，审批后仍能执行原工具并继续模型循环。

### Slice F：Job、Timer 与通用订阅（P3）

- EventLog、声明式 filter、delivery 去重；
- 长工具 Job、一次性 Timer；
- retry/dead letter/outbox；
- 运维查询 API。

验收：后台 Job 和 Timer 可跨重启唤醒 run；重复事件不造成重复副作用。

### Slice G：QuickJS adapter（P4）

- `start/resume` module contract；
- 资源预算与 capability manifest；
- Rust/JS 共用 command fixtures；
- 无 Node/npm/文件/网络默认能力。

## 12. 当前推荐的下一步

立即实现 **Slice A**，不要同时开发 JS、Cron 或通用 webhook。它会先固定 Session 的身份、revision、消息与 run 原子边界，是后续 Context Engine、Skill Orchestrator 和 Flow Runtime 都必须依赖的地基。

Slice A 完成后按 **B → C → D** 完成整个 P1，再进入 AgentMachine。先用 EchoAgent 做“创建 Session → 连续两个 run → 重启 → 查询历史”，再用 fake model 做“Skill 解析 → Memory 检索 → 压缩 → Context fingerprint 固定”的确定性测试。
