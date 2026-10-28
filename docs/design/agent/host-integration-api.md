# 宿主集成 API

2026-10-28 首版实现。目标及后续扩展见[宿主集成契约](host-integration-contracts.md)。以下接口用于受信任的本地宿主；HTTP 层仍负责认证、tenant 选择和敏感内容脱敏。

日常崩溃恢复使用 `recover_run`，Agent 完整历史使用 `export_agent_history`（仅已结束 Run），见[崩溃恢复与历史导出](recovery-and-history.md)。下文 `export_run` / `import_run` 专用于暂停后的恢复包迁移，历史导出不需要它们的维护屏障。

## 图绑定

完整可解析示例：[host-bound.yaml](../../../config/agents/host-bound.yaml)。v3 使用独立的绑定契约 ID/version、闭合 JSON Schema 和完整值引用 `{$binding: name}`，不做字符串插值。宿主不需要知道图内节点名。

```rust,ignore
use agent_core::agent::{AgentBuilder, HostBindings, HostPolicy};
use serde_json::json;

let bindings = HostBindings {
    contract_id: "mina.assistant".into(),
    version: 1,
    values: [
        ("system_prompt".into(), json!("你是一个助手。")),
        ("context".into(), json!({"prior_messages": []})),
        ("model_name".into(), json!("approved-model")),
        ("max_output_tokens".into(), json!(2048)),
    ].into(),
};
let policy = HostPolicy {
    allowed_tools: Default::default(),
    allowed_models: ["approved-model".into()].into(),
    max_steps: 8,
    max_output_tokens: 2048,
    timeout_ms: 900_000,
    review_level: 50,
    allow_run_adf: false,
};
let agent = AgentBuilder::new(registry, resources)
    .from_file_with_bindings("config/agents/host-bound.yaml", bindings, policy)?;
```

`AgentDefinition::bind` 和 `AgentBuilder::build_with_bindings` 提供相同代码入口。绑定缺失、类型/版本错误、未消费的必需字段、禁用模型和未适配的自定义算子在构建时失败。权限通过独立的 `HostPolicy` 参数提供，不由远程 JSON 声明授予。工具集合求交，步数/token/超时取较小上限，ADF 必须双重允许；审批阈值根据现有 `ToolApprovalPolicy` 定义收紧，并在 Policy/Tools 之间统一。

`agent.context@2` 新增 `context.prior_messages`，位于 system prompt 之后、本次启动消息之前。原 `agent.context@1` 描述保持不变，已有 v2 图和 checkpoint 仍使用旧协议。模型实例、工具集和等待服务继续通过 `AgentResources` 按名称/revision 注入。存储路径和凭据不属于图绑定。

`build_with_bindings` 同时把宿主策略固定到 AgentResources；直接组装可调用 `AgentResources::with_host_policy`，在构建和恢复时重新检查当前权限。解析结果、绑定值、源摘要、有效宿主策略和解析摘要随 `AgentDefinition` 保存到 Run；恢复仍校验算子协议和资源 revision。绑定后直接修改解析图会被拒绝；应在绑定前选定 trigger/修改定义。`constrain_run` 支持继续收紧并更新解析摘要。自定义算子的权限适配器尚未开放，带 HostPolicy 的构建会拒绝未知算子。

## 检查和导出

```rust,ignore
use agent_core::agent::RunManager;
let manager = RunManager::new("local", fabric.clone());
let page = manager.inspect_run(run_id, None, 32).await?;
// 后续把 page.next_cursor 传回；游标绑定 manager、tenant、Run 和不可变快照。
```

页返回逻辑 ID、记录种类、协议版本、revision 和 payload，不暴露 SQL 表/行定位。第一页捕获一致快照，后续页不会因 Run 写入而漂移。每页 1–128 项、最多 4 MiB，大项返回摘要；每个 manager 最多缓存 8 个快照，每个快照最多 128 MiB，60 秒到期。该接口返回完整内部诊断内容，包括 prompt/工具参数；不能直接作为公开 HTTP 输出。检查结果不是恢复包。

首版针对已知内置图格式枚举依赖，目录由框架适配器维护逻辑映射，宿主不拼 checkpoint 前缀。尚未提供任意自定义 checkpoint 的所有权注册器；无法枚举的算子类型会阻止恢复导出。

```rust,ignore
// 先停止所有相关 worker/ingress，排空调用和有效 lease。
manager.begin_maintenance("host-generated-unique-token", now_ms).await?;
let bundle = manager.export_run(run_id, "host-generated-unique-token").await?;
let encoded = serde_json::to_vec(&bundle)?;
// 本地继续执行时显式释放；迁移时保持源端屏障，不要同时恢复两端。
manager.end_maintenance("host-generated-unique-token").await?;
```

维护屏障为 **整个 Fabric/SQLite EventStore**，不是只锁一个 Run。有效租约存在时拒绝获取；token 由宿主生成，`now_ms` 必须来自可信时钟。屏障跨 SQLite 连接和进程持久化，重开后仍有效。除检查/读取、导出和受控导入外，Fabric 写入以及同库 EventStore/JobStore 的 SQL 写入均被拒绝。它不会自动停止网络请求，宿主仍须先停止所有执行器。导出成功或失败都保留屏障，宿主负责显式释放或继续保持迁移隔离。

恢复包包含 Run checkpoint、解析图、输入、算子状态、未确认 invocation、内部/公开日志及水位、订阅与 ACK/重试状态、receipt、DLQ、pending outbox、按 Run 关联的标准宿主记录和完成事实，以及可达内嵌 blob 内容。包有独立格式版本和全内容 SHA-256 校验，最多 128 MiB。摘要不是来源认证；包来自可信宿主存储或经宿主验证的来源。

当前支持的依赖边界：

- Memory/SQLite 的内置图、审批和内嵌 `FabricBlobStore`；blob 元数据、完整标记、分块和字节长度均校验。
- SQLite 等待桥的 EventStore/JobStore 与 Fabric 在同一个受屏障保护的数据库中，且外部记录可证明属于该 Run。包使用版本化逻辑事件、订阅、投递、定时器和任务 DTO，不包含 SQL schema。
- 已发生但未确认的模型/工具 invocation 原样保存，仍受原运行时的不重放规则约束。处于 Running 且结果不确定的外部 Job 要先 reconcile，不能当作 pending job 自动导出后重跑。
- 自定义算子、共享外部订阅、独立等待数据库、文件系统/远端 blob、缺失依赖和不兼容版本会明确报错。热导出、跨存储暂存激活和外部 blob 导入适配器尚未实现；不能用诊断 JSON 替代。

## 导入与恢复

```rust,ignore
let target = RunManager::new("local", target_fabric.clone());
target.validate_import(&bundle, &registry, &target_resources, Some(&current_policy))?;
target.begin_maintenance("target-token", now_ms).await?;
target.import_run(bundle, "target-token", &registry, &target_resources,
                  Some(&current_policy)).await?;
// 源端保持停止/隔离；目标尚未运行。确认唯一执行所有权后：
target.end_maintenance("target-token").await?;
target_agent.engine.recover("local", run_id).await?;
```

导入再次验证摘要、依赖闭包、Run 身份、日志水位/序号、订阅 ACK、资源 revision 和算子描述；v3 要求提供当前 HostPolicy。当前策略若使原解析图发生变化则拒绝导入，要求宿主重新授权，不静默扩大或替换权限。受信任的旧 v2 图可传 `None`，其信任与资源许可由宿主承担。

同一包重复导入幂等，已有 Run 或退休墓碑冲突则拒绝。旧 Fabric lease 清空，epoch 递增；等待截止时间、审批绑定和未确认 invocation 不重置。目标仍保留维护屏障，导入本身不调用模型/工具。

SQLite 在一个事务中恢复 Fabric 与同库等待依赖，失败全部回滚；外部等待记录的目标区域必须为空，避免重写事件 sequence 和其他 Run 的游标。临时解除 SQL 写屏障仅发生在该事务内部，提交前恢复；其他连接看不到中间状态。通用跨库导入尚不提供。

## SQLite 选项与所有权

`SqliteEventStore::open_with_options(path, identity, SqliteOpenOptions)` 支持：

| 字段 | 值/默认 |
| --- | --- |
| `journal_mode` | `Wal`（默认）或 `Delete` |
| `synchronous` | `Full`（默认）或 `Normal` |
| `busy_timeout` | 默认 5 秒，毫秒精度 |
| `migration` | `Apply`（默认）或 `ValidateOnly` |

打开后检查实际 PRAGMA，`configuration().await` 返回实际值。foreign keys 强制开启；WAL 用于不支持它的内存数据库会失败。NORMAL 与 FULL 的掉电持久性不同，宿主必须按需求选择。旧 `open` 保留既有默认行为。

`from_connection` 消费并拥有空闲 `rusqlite::Connection`，拒绝当前有事务的连接；最后一个 store clone 释放时关闭。它不加入宿主原事务，也不提供锁内 async 回调。ValidateOnly 验证完整 schema、维护触发器和 Fabric 版本；旧格式或未初始化的 Fabric 明确拒绝，不延迟迁移。

首次使用维护屏障、标准宿主事务或新完成事实会把 SQLite 兼容标记提升到 5，旧二进制会拒绝继续访问；普通旧接口仍能读取既有版本 4。升级没有自动降级。完成事实由新引擎终态提交产生，旧版本已经结束的 Run 不会自动补造完成事实。

## 同库宿主记录与跨库投影

标准宿主记录首版为不可变 `HostRecord { tenant, namespace, run_id?, operation_id, schema_version, payload }`。通过 `RunManager::commit_host_records(transaction, records)` 与一个 Fabric commit 同事务提交。tenant 必须一致；`$` 前缀为框架保留 namespace。相同 namespace/operation_id、相同内容幂等，不同内容冲突。`read_host_record` 公开读取，不需要 SQL。需要可更新聚合时使用新的 operation_id；任意宿主表/CAS 聚合扩展尚未开放。

每次新 Run 终态与不可变 `RunCompletion`、按 tenant 的完成日志在同一个事务提交。使用 `completion(run_id)` 或 `list_completions(after, limit)` 读取，通过 `acknowledge_completion(consumer, sequence, completion_id)` 确认连续前缀，用 `completion_offset(consumer)` 续读。

宿主按以下顺序投影：

1. 根据完成事实的 Run 身份找到不可变 admission/operation_id 映射。
2. 在宿主自己的事务中写 Session 最终消息或 Profile 回执，同时写唯一 `(consumer, completion_id)` 幂等账本。
3. 宿主提交成功后再 ACK Mina；提交后 ACK 前崩溃会重复读取，账本确保不重复产生结果。

Session/Profile 在两个库时使用两个 consumer，各自提交和确认。相同路径上的独立连接也不代表同一事务。首版永久保留完成日志、完成记录及其 blob 引用，ACK 不删除它们；尚未提供 consumer 注册/注销和自动压缩策略。这避免未完成投影被回收，但宿主应监测存储增长。旧事件日志与完成日志使用不同游标。

## 验证入口

可直接运行的跨库宿主示例、导出/导入验证和进程退出验收见[宿主接入示例与验收](host-integration-acceptance.md)。

- `crates/core/tests/host_integration.rs`：类型绑定、节点重命名、缺参零调用、权限上限、Memory/SQLite 导入恢复、完成确认、标准记录回滚和 blob 完整性。
- `crates/core/tests/static_agent_graph.rs`：原图行为回归及新增等待/审批归档、未确认模型调用迁移不重放。
- `crates/extension/tests/host_integration.rs`：实际 PRAGMA、ValidateOnly、连接所有权、真实 COMMIT 失败回滚、跨连接和重开后的持久维护屏障。
- 既有 Fabric、checkpoint、等待游标和 SQLite 并发契约测试继续承担事务、租约和恢复回归。
