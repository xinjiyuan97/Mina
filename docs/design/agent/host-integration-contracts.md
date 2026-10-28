# 宿主集成契约：Run 检查、恢复包、图绑定与事务边界

状态：2026-10-28 目标设计。首版代码已落地，实际可调用接口、验证证据及支持边界见[宿主集成 API](host-integration-api.md)。本文保留完整目标，不把尚未完成的通用依赖目录、跨存储导入、热导出、可变宿主记录或完成记录压缩标为已实现；下文建议类型/语法以实际 API 文档为准。

当前产品范围已明确拆分：崩溃后从最近已提交 checkpoint 恢复等待和待处理工作；导出仅包含 Agent 已结束 Run 的完整历史。具体已实现契约见[恢复与历史](recovery-and-history.md)。本文的恢复包迁移为独立能力，热恢复备份不是当前需求。

## 1. 问题与设计决定

Pudbot 等宿主需要分发图、检查 Run、迁移恢复以及补齐 Session/Profile 记录。这些需求应通过公开逻辑接口完成，不能要求宿主读取 Mina 私有表、猜测 checkpoint key 或修改固定节点名称对应的 JSON。

源码基线：

| 当前实现 | 缺口 | 本次决定 |
| --- | --- | --- |
| `contracts/fabric.rs` 的 `ReadCheckpoint` 按已知 key 读取；Extension `store/operator.rs` 将记录写入 `workflow_fabric_records` | 没有按 Run 的完整逻辑目录和恢复导出协议 | 增加检查接口和独立的恢复包协议；维护逻辑所有权及依赖索引 |
| `agent/definition.rs` 解析定义，`AgentResources` 注入资源，执行参数留在各算子 config | 宿主依赖 `context/llm/policy/tools` 等内部实例名 | 新图格式声明版本化的类型绑定；宿主仅使用稳定绑定名称 |
| `SqliteEventStore::open` 创建私有连接；当前连接设置 WAL、5 秒 busy timeout 和 foreign keys，未显式设置 synchronous | 不能配置完整耐久性策略，也不能把宿主事务并入 Fabric 提交 | 增加显式打开配置；同库用标准宿主记录，独立库用完成事实与幂等投影 |

Core 定义逻辑协议、依赖描述和验证；Extension 实现 SQLite、文件 BlobStore 和导入暂存；Agent 层解析图、形成 Run 快照及完成事实；宿主负责凭据、路径、租户授权、资源注入和自己的业务投影。保持现有 crate 依赖方向，不新增总线或重复事实源。

## 2. 按 Run 检查

建议在受信任宿主的 Run 管理门面提供以下逻辑接口；tenant 来自已认证的宿主上下文，不能只凭 run_id 跨租户查找：

```text
inspect_run(run_id, cursor?, limit) -> RunInspectionPage
export_run(run_id, options) -> RunBundle
validate_import(bundle, resources, host_policy) -> ImportPlan
import_run(bundle, plan) -> ImportedRun
```

`RunInspectionPage` 包含 `api_version`、`tenant`、`run_id`、`snapshot_id`、Run 状态、`items`、`next_cursor`。checkpoint 项使用稳定 `logical_id`、`kind`、`schema_version`、`revision`、逻辑 owner、状态摘要和依赖引用；有权读取完整内容时另返回版本化 payload。区分 payload 协议版本与 CAS revision，不把数据库 key、section、position 或表名作为公共身份。

- 第一页创建有时限的不可变检查快照；后续 opaque cursor 绑定 tenant、Run、快照和排序位置。按 `(kind, logical_id)` 稳定排序，不使用 SQL OFFSET 作为公共游标。
- `limit` 为 1–128，另有页字节上限；大内容返回逻辑引用，不突破单页上限。快照过期返回 `SnapshotExpired`，宿主重新开始，不把不同时间的页拼成同一快照。
- 活跃 Run 支持检查，返回该快照的状态和水位；检查快照不是可恢复备份。读取不 claim、不续租、不 ACK、不触发模型或工具。
- 默认返回状态摘要；内部日志、prompt、工具参数的原文需要宿主显式授权。诊断脱敏结果标记 `diagnostic_only`，不能传给 `import_run`。
- 不存在、已退休、无权限和依赖缺失具有明确错误语义；外部接口可以按宿主策略合并不存在与无权限错误，避免泄露租户信息。

### 逻辑目录与依赖闭包

新增 Run 目录，将每个恢复记录的 owner、逻辑类型、格式版本和引用边与记录本身原子写入。目录覆盖 checkpoint、增量 journal 链、日志、投递状态和外部桥接依赖；共享对象使用引用关系，不把它们复制成另一 Run 的私有所有物。

每种持久化算子状态和外部 adapter 声明版本化的依赖枚举器。未知类型、无法解析的引用或缺失对象必须报告 `IncompleteDependencies`。不能扫描任意 JSON 字符串并猜测 key 前缀来宣称完整性。BlobId 由受支持的类型/字段语义识别。

旧库的目录由 Mina 自己按已知历史格式迁移；无法证明完整的 Run 可以检查并显示缺项，但不能导出为可恢复包。SQLite schema 变化只修改 adapter 和迁移，宿主 DTO 不随物理布局变化。

## 3. 恢复包与一致边界

### 3.1 首版范围：暂停后的非终态 Run 可以导出

首版支持终态 Run，以及通过维护屏障暂停后的运行中、等待中、审批中 Run；**不支持写入持续进行的热导出**。`export_run` 必须取得或验证跨进程维护屏障，不能仅依赖当前对象的互斥锁。无法取得完整屏障返回 `RunBusy` 或 `UnsupportedConsistency`，不能降级生成看似成功的恢复包。

导出过程：

1. 阻止该 Run 新 ingress、claim、effect 启动和后台投影写入；协调相关 EventStore、JobStore、Timer、BlobStore 以及其他进程的写入者。终态 Run 仍需覆盖清理任务和终态投影。
2. 排空可完成的在途提交；无法安全收敛的外部执行保留 `in_flight/unknown` 及稳定 operation/effect/invocation ID。屏障不能撤销已发出的网络请求，也不能伪造成功回执。
3. 在 SQLite 一致读事务内固定 Run revision、各日志水位、订阅水位和依赖清单。多个存储必须在同一屏障下稳定；无法暂停的外部恢复状态阻止可恢复导出。
4. pin 所有可达 blob（含 journal previous 链）并阻止相关退休/GC；读取字节、校验摘要，完成 manifest 后才发布包。跨存储 pin 必须覆盖复制全过程；现有全局 GC 无细粒度 pin 时，使用其要求的全局维护屏障。
5. 导出结束或失败均释放快照和 pin。默认保留暂停状态，由宿主显式恢复执行；暂停失败时报告已取得的屏障及清理结果。导出不是转移执行所有权。

未来热导出只有在 adapter 声明并验证多存储一致快照、引用 pin 和执行切换协议后才能启用独立 capability，不能仅凭 WAL 就声明支持。

### 3.2 包内容与版本

`RunBundle` 是逻辑归档，可流式写入；不是 SQLite 文件副本。manifest 至少包含：

- `format_version`、`bundle_id`、生成器版本、tenant/Run 身份、快照 revision/水位、导出时间、一致性模式、`purpose = recovery`、依赖完整性声明。
- 原图摘要、已解析图、绑定 schema/值快照、有效权限、算子协议和资源 revision/capability 清单。
- 每项逻辑记录的类型、版本、ID、大小、摘要、依赖列表；按规范化 manifest 和内容摘要计算包摘要，摘要用于完整性检查，不充当来源认证。
- 所有 checkpoint 与增量链、内部/公开日志及水位、订阅与 ACK/重试/DLQ、receipt、待派发 outbox、执行账本与未确认 effect。
- 等待和审批的关联、决定、绑定摘要及截止时间；等待桥的外部订阅、定时器、任务及去重记录。仅有 Fabric checkpoint 不足以覆盖独立 EventStore/JobStore。
- 所有恢复所需 blob 的实际内容和摘要。凭据、模型/工具客户端、连接、任意宿主磁盘路径不进入包；外部服务以资源兼容要求表示。
- 稳定完成记录、标准宿主记录和投影确认信息（适用时）。独立 Session/Profile 数据库不自动纳入包，manifest 必须列明它们由宿主另行备份/重建的边界。

对象缺失、未完成 blob、未知必需记录、未导出的外部等待依赖均使恢复导出失败。允许另行输出带缺项列表的诊断文件，但其 `purpose = diagnostic`，导入器必须拒绝。恢复包保证 Mina 可重建持久化状态，不保证外部副作用可以自动继续。

### 3.3 导入与恢复

`validate_import` 无业务副作用，检查格式支持范围、大小/数量上限、内容摘要、依赖闭包、身份一致性、日志及 ACK 连续性、算子协议、资源 revision 和目标宿主权限。未知必需版本拒绝；格式升级只能经过显式版本转换器。校验计划绑定包摘要、目标身份和资源/策略摘要，提交前再次核验，防止校验后发生变化。

导入默认保留 tenant、Run、event、effect、invocation、operation 身份；首版不提供任意重写 ID 或克隆为新 Run。目标 Run 已存在时只允许同一包的幂等重试；与已有执行或退休墓碑冲突则拒绝，不覆盖、不合并日志。

先在不可执行的暂存区域写入 blob 和记录并验证，再以单个目标 Fabric 事务发布 Run 可见性。外部等待/任务服务必须提供可幂等暂存和激活能力；所有依赖就绪前保持 `ImportedPaused`。没有这种能力的 adapter 拒绝导入，不能把多个数据库提交描述为原子提交。失败清理暂存对象或按宽限期回收；任何阶段崩溃都不得出现可执行的半包。

导入不恢复旧 worker lease；重新建立执行 epoch 并拒绝旧 token。跨存储迁移恢复前，宿主还须确认源 Run 已停止并被 fencing，目标库递增 epoch 本身不能阻止源库继续执行。目标资源与权限核验通过后才允许显式 resume。

审批和等待保留绝对截止时间，不因导入重置时钟；过期项进入既有过期路径。未确认外部 effect 保留不确定状态，只允许基于稳定幂等键查询/reconcile，或人工解决；不能因导入自动重发非幂等操作。当前宿主权限收紧使原授权无效时，拒绝执行并要求重新审批。

## 4. 分发图的稳定宿主绑定

### 4.1 定义与构建

引入图格式 v3，增加 `bindings` 声明及结构化引用；现有 v2 继续按原规则使用，不能把新字段塞入拒绝未知字段的 v2。绑定契约有独立 `id/version`，与图格式、算子和事件版本分开管理。

以下是 **v3 提案片段，不可直接交给当前解析器**：

```yaml
schema_version: 3
bindings:
  id: pudbot.assistant
  version: 1
  inputs:
    system_prompt: {type: string, required: true}
    context: {type: object, required: true}
    model_name: {type: string, required: true}
    max_tokens: {type: integer, minimum: 1, required: true}
    authorization: {type: host_policy, required: true}
operators:
  answer_model:
    type: agent.model
    version: 1
    config:
      model: primary
      model_name: {$binding: model_name}
      max_tokens: {$binding: max_tokens}
      timeout_ms: 60000
```

图作者在内部配置中引用绑定；宿主只传 `HostBindings { contract_id, version, values }`，不传节点名或 JSON patch 路径。system_prompt/context 的使用由图内部 Context 配置引用声明；对应算子须支持这些字段并有类型 Schema。`context` 首版是受大小约束的 JSON object，具体字段由绑定 schema 定义；它与启动事件的用户 input 分别验证。每 Run 的上下文在生成该 Run 的解析快照前绑定，不能修改正在运行的图。

构建顺序固定为：解析版本 → 校验绑定 schema 和必填值 → 解析结构化引用 → 校验算子 config/协议/资源 → 计算有效权限 → 保存解析快照 → 允许启动。错误返回绑定名和定义字段路径；缺值、未知值、类型错误、越界、契约版本不兼容或未消费的必需绑定均在模型/工具调用前失败。绑定引用仅替换完整值，不提供字符串脚本、环境变量读取或任意文件插值；schema 无默认值的 required 字段不能从节点硬编码值补齐。

ModelPort、ToolPort、Fabric、wait service 仍使用 `AgentResources` 注入；`model_name` 是注入模型资源内允许选择的名称，不是创建任意 provider 的指令。checkpoint/数据库/blob 根路径及凭据只由宿主提供，图不能指定磁盘位置。

### 4.2 权限不是普通配置覆盖

`authorization` 是宿主生成的类型化策略，远程图只能声明需求和进一步限制。现有 `constrain_run` 的 tools/max_steps/ADF 收紧语义保留，并扩展到统一的有效执行约束：

| 参数 | 合成规则 |
| --- | --- |
| 工具、模型允许集合 | 宿主、图声明和 Run 请求的交集；显式空集合表示禁止，不解释为无限制 |
| max_steps、token 上限、超时与预算 | 所有已声明上限取更小值；请求越过硬上限时构建失败，不能静默扩大 |
| ADF、其他可选能力 | 宿主允许且图与 Run 均显式启用 |
| 审批/风险策略 | 以允许执行的操作集合求交、需要审批的条件求并；由策略类型定义偏序，不猜测 review_level 数值越大或越小越严格 |

同一份有效策略传给 Context、Policy、Tools，避免不同节点 config 漂移。必须在资源调用边界再次检查，不能只依赖图中存在 Policy 节点；远程图绕过节点或注册不同名称也不能扩大权限。自定义算子只允许使用宿主审核注册的实现和受约束资源，任意本机代码不由图 schema 提供安全隔离。

### 4.3 保存与恢复

每 Run 原子保存原图摘要、规范化解析图、绑定契约 ID/版本、绑定值快照/摘要、有效权限、资源名称/revision 和算子协议版本。敏感绑定按宿主存储策略保护；诊断可脱敏，但恢复所需值不能仅保留不可逆摘要。凭据使用宿主资源引用，恢复时重新注入。

恢复使用已保存的解析结果，不重新下载远程图、不把当前默认 prompt/model 覆盖旧值。当前宿主策略可以继续收紧；需要扩大权限、缺少原资源或协议不兼容时显式拒绝恢复，不能静默替换。节点重命名只影响新图内部连线，新 Run 的宿主绑定代码保持不变；已有 Run 按旧快照恢复，不尝试重映射旧算子状态。

## 5. SQLite 配置和宿主提交

### 5.1 显式打开选项

建议提供 `open_with_options(path, identity, SqliteOpenOptions)`，以及返回实际配置的只读 descriptor。配置如下：

| 选项 | 契约 |
| --- | --- |
| journal_mode | 类型化 WAL / DELETE；明确文件库和内存库支持范围，不支持或设置失败直接返回错误 |
| synchronous | 类型化 FULL / NORMAL；新显式选项默认 FULL，NORMAL 的掉电持久性差异写入 API 文档；不承诺硬件之外的耐久性 |
| busy_timeout | 显式 Duration，默认 5 秒；超时返回可识别 Busy，不无限等待 |
| migration | Apply / ValidateOnly；Apply 在独占迁移协调下事务升级，失败回滚；ValidateOnly 遇到需迁移的库拒绝打开 |
| foreign_keys | 强制开启，不能由 graph 关闭 |

打开成功前读取实际 PRAGMA 核验；不接受静默回退。不同连接共享同一文件时必须使用兼容配置，journal 模式变更不得在活跃运行时隐式执行。所有 Fabric 延迟迁移也受 migration 策略约束，不能在 ValidateOnly 打开成功后首次读写时绕过。

旧 `open(path, identity)` 暂保留当前行为并标为兼容入口，不声称它已经显式保证 FULL。推荐新宿主选择 options；后续改变旧入口默认值需单独发布迁移说明。

建议 `from_connection(connection, identity, options)` 只接受空闲连接并**转移所有权**；拒绝已有事务，后续连接由 store 管理并在最后一个 owner 释放时关闭。提供连接不代表接入宿主当前事务。不提供借用连接、公开锁或锁内 async 回调；SQLite 工作仍在阻塞执行区完成。

### 5.2 同库：标准宿主记录

首版选择标准记录接口，不开放任意 SQL 回调。新增 `HostRecordWrite { namespace, key, operation_id, expected_revision, schema_version, payload }`，作为受控 Fabric commit 的可选组成部分，与 checkpoint、receipt、ACK、outbox 在同一个 SQLite 事务中提交。namespace 由宿主注册，图不能任意写宿主命名空间；限定条数/字节数，不允许网络调用或 await。

`operation_id` 在 tenant + namespace 内唯一，相同内容重试返回原结果，内容不同报 Conflict；更新使用 CAS。提交失败包括 COMMIT 失败必须全部回滚。公开读取/分页 API 返回逻辑 HostRecord，宿主不读取其物理表。

宿主可把 Session 最终消息或 Profile 回执的权威载荷保存为标准记录，以获得上述原子性。若继续写宿主自己的任意业务表，即使表在相同 DB 路径、甚至来自另一条同文件连接，也不属于这个原子边界，仍按投影处理。任意宿主 SQL 事务扩展点不在首版范围。

### 5.3 独立库：稳定完成事实与可重放投影

增加不可变 `RunCompletion`：`schema_version`、tenant/Run、稳定 `completion_id`、终态 revision/status、输出或 blob 引用及摘要、usage、完成时间、绑定快照摘要、宿主 admission/operation 关联。每 Run 只有一个权威完成事实，与引擎终态在同一个 Fabric 事务提交；公开日志最终水位由后续投影记录关联，不能把尚未持久化的公开终态当作完成事实。

提供按 tenant 的分页 `list_completions(after_cursor, limit)`、按 ID 读取和按 consumer 确认接口。游标是稳定逻辑提交序列，确认只能推进连续完成前缀；订阅通知只是唤醒信号，丢通知后仍可扫描追平。它与通用 handler receipt、工具 effect receipt 分开，不从任意 checkpoint JSON 猜测 Session 是否完成。

宿主投影流程：

1. 消费完成事实，以 admission 映射获得 Session/Profile 和稳定 operation_id。
2. 在宿主数据库事务内写最终消息/回执，并同时插入唯一的 `(consumer, completion_id)` 幂等账本及内容摘要；相同内容重复读取直接成功，不同内容报冲突。
3. 宿主事务成功后再确认 Mina 完成记录。崩溃发生在宿主提交前则重试，提交后确认前则靠幂等账本去重，确认后无需重做。

Session 和 Profile 分属两个库时使用两个独立 consumer/账本，不宣称一次确认使两库原子完成。投影失败可重试、告警及人工修复，不能反向撤销已提交 Run 终态。终态恢复不会再次调用模型。

注册的必需 consumer 未确认时，完成记录、输出依赖和关联 admission 不得被 RetireRun/GC 删除；慢 consumer 会产生可观察的积压。注销 consumer 或放弃投影必须显式审计。备份回滚导致宿主账本倒退时，需从保留的完成记录重新扫描，源端 ACK 不能替代宿主数据备份。

## 6. 实施次序与验收

以下均为待实现工作，不改变已有优化记录中的历史完成状态。

| 阶段 | 交付 | 必须通过的验收 |
| --- | --- | --- |
| P0：公开检查与目录 | 版本化 DTO、所有权/依赖目录、Memory/SQLite 检查 | 私有 SQL schema 改版后宿主测试不变；多页并发写入仍为单快照；租户隔离、游标过期和未知类型错误 |
| P0：稳定绑定 | v3 解析、HostBindings、有效策略及解析快照 | 重命名 context/llm/policy/tools 后宿主代码不变；缺值/类型/版本错误在零模型调用时失败；绕过 Policy 不能越权；恢复不随远程图漂移 |
| P0：打开配置与完成投影 | options、实际配置查询、RunCompletion 和消费接口 | WAL/DELETE、FULL/NORMAL、Busy、迁移拒绝/回滚和连接所有权测试；Session/Profile 各崩溃窗口重复执行只有一份结果 |
| P1：标准宿主记录 | commit 扩展、CAS 和幂等接口 | 注入写入/COMMIT 失败时宿主记录、checkpoint、ACK、receipt、outbox 全部回滚；另一连接不被误认为同一事务 |
| P1：恢复包 | 屏障、依赖遍历、校验、暂存导入和显式恢复 | 等待、审批、未确认 effect、pending outbox、journal 链和 blob 完整导出；诊断包拒绝导入；损坏/缺依赖/不兼容资源拒绝；导入崩溃无可执行半包；旧源 worker 被 fencing |
| P2：在线导出 | 可选 adapter capability | 跨存储一致性和 GC 竞争测试通过前不开放热导出 |

恢复包还需覆盖共享 blob、跨 Run 订阅依赖、目标已有 Run、重复导入、过期审批、外部 effect 结果不确定和独立宿主库未恢复的情况。无法安全隔离共享订阅时首版拒绝导出，不能截取其 offset 并损坏其他 Run。逻辑内容闭包验证与恢复行为测试共同作为验收依据，不能仅用 JSON round-trip 或文件成功生成替代。

Pudbot 在接口落地前继续使用不可变接入记录、稳定 operation_id 与幂等回执；现有只含 checkpoint JSON 的文件标记为诊断材料。接口落地后移除宿主对私有 schema 和固定节点名称的依赖；独立数据库继续遵守投影边界，不升级为跨库原子恢复承诺。
