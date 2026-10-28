# 崩溃恢复与 Agent 历史导出

这两个能力分别使用运行状态和历史归档，不共用导入入口。

## 崩溃恢复

```rust,ignore
agent.engine.recover_run("local", run_id).await?;
```

`recover_run` 是现有 `recover` 的明确命名入口，复用相同的事务和恢复语义。读取最后一个成功提交的 Run checkpoint，验证保存的图、绑定、资源和算子协议，再恢复调度。没有提交的状态变化被存储事务回滚，不扫描历史去挑选更早的成功步骤。

checkpoint、日志追加、消息消费确认和 outbox 在同一事务提交。恢复后保留等待、审批和原截止时间；没有触发时继续等待，已有持久化待处理事件和已到期定时器则继续处理。不会为恢复重新生成启动事件，也不会重新向宿主发送整段历史输出。宿主仍应持久化自己的事件消费游标。

原有效租约按现有 fencing/过期规则处理，不能单凭“正在恢复”就清除另一执行器的有效租约。恢复不会回滚外部世界：已发出但未确认的模型/工具调用继续遵循运行时的不重放规则；结果未知时失败或进入人工核对，而不是自动重复副作用。宿主使用稳定 operation_id 与工具的幂等/结果查询机制处理此边界。

## Agent 归属

```rust,ignore
let resources = AgentResources::new(fabric.clone())
    .with_agent_id("host-owned-stable-agent-id")
    .model("primary", "revision-1", model);
```

Agent ID 来自宿主，不从 YAML 名称或节点名推断。归属和初始 Run checkpoint 在同一事务写入，图重命名、版本更新不会拆散该 Agent 的历史。Run 只能属于一个 Agent；tenant 参与隔离。恢复或导入时如 resources 指定 Agent ID，必须与保存的身份一致。

旧 Run 没有 Agent 归属，框架不能猜测。宿主可依据自己的权威记录调用 `manager.register_run(agent_id, run_id)`，通过 checkpoint 版本校验原子补登记。重复同一归属允许，不同归属拒绝；并发更新冲突时重新读取再重试。已退休且失去状态的旧 Run 不能补登记。未登记的旧 Run 不在导出范围内，宿主上线前须完成归属迁移后才能对用户宣称 Agent 历史完整。

首次登记归属或导入带 Agent 身份的 Run 会将 SQLite 兼容标记提升至 6，防止旧程序继续写入并丢失身份字段；后续普通宿主事务不会降低该标记。ValidateOnly 支持版本 6。

## 历史导出

```rust,ignore
let manager = RunManager::new("local", fabric);
let history = manager.export_agent_history("host-owned-stable-agent-id").await?;
history.verify()?;
let bytes = serde_json::to_vec(&history)?;
```

每次调用在一个一致存储快照内固定归属、Run 清单及内容。Memory 使用同一锁内视图，SQLite 使用一个读事务；无需停止 worker、等待租约或设置维护屏障。导出只包含该快照中处于 Completed、Failed 或 Cancelled 的 Run。运行中、等待触发和等待审批的 Run 不包含；随后才完成的 Run 留到下次。

`AgentHistory` 使用独立的 `agent_history` purpose 和格式版本，包含：

- tenant、Agent ID，以及确定的 Run 清单。
- 每个 Run 的最终状态，包括实际图、绑定快照、输入、输出、错误和算子状态。
- 完整图执行事件及截止序号，包括调用、审批、等待、触发和终态事实；保存路由事件的原始时间、因果标识和 payload schema，以及已有的完成记录。
- 读取边界上已持久化的公开事件及其截止序号。公开事件是派生投影，可能晚于图终态落盘；权威终态和完整执行历史以图事件及最终状态为准，不为导出伪造公开输出事件。
- 历史引用的内嵌 blob 元数据和字节，以及归档内容 SHA-256 校验。

已登记 Run 被退休、日志长度与 checkpoint 水位不一致、附件缺失或未完成时，整个导出失败，不返回部分历史。当前内置算子支持完整依赖枚举；未知自定义算子和外部文件/远端 blob 必须有适配器，否则明确拒绝。归档上限为 128 MiB，超过时返回 Capacity，不截断历史；流式大归档是后续扩展。

格式不包含 SQLite 表结构，也不含可激活的租约/订阅恢复契约，不能传给 `import_run`。`verify` 校验格式和内容完整性，不证明来源可信。宿主仍负责访问授权、敏感信息处理和自己的 Session/Profile 历史。

原 `export_run` / `import_run` 保留为独立的暂停迁移能力，不是崩溃恢复或 Agent 历史导出的必经步骤。

## 验证

- `host_integration`：Memory/SQLite 相同导出契约；成功、失败、取消纳入，运行中排除；Agent/tenant 隔离；图改名；导出边界固定；内嵌附件；篡改检测；缺失历史/附件拒绝；旧 Run 归属登记和不可改属。
- `static_agent_graph`：重开 SQLite 后调用 `recover_run`，未触发时模型和工具计数不变，收到原等待条件的触发后继续；既有审批、未确认调用和原子 checkpoint 测试继续适用。
- extension `host_integration`：Agent 目录兼容标记单调升级且 ValidateOnly 能重新打开。

可运行示例见[宿主接入验收](host-integration-acceptance.md)。
