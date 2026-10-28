# 宿主接入示例与验收

可运行示例：[host_completion_projection.rs](../../../crates/extension/examples/host_completion_projection.rs)。只使用公开 Mina API；SQL 仅操作示例自己拥有的 Session 数据库。模型使用离线固定响应，不需要凭据或网络。

```sh
cargo run -p agent-extension --features sqlite \
  --example host_completion_projection -- /tmp/mina-host-demo
```

示例完成以下流程：

1. 用显式 SQLite 打开选项创建 `mina.db`，另外创建 `session.db`。
2. 启动时先按持久化 consumer offset 补投影已有完成事实。
3. 通过 `mina.assistant@1` 绑定 prompt、历史上下文、模型与 token 上限；独立 HostPolicy 限制权限，不访问图内节点名。
4. 在宿主库先保存 Run → Session 接入映射，再用同一 RunId 启动图。
5. 将终态事实和唯一 `(consumer, completion_id)` 账本放在宿主同一事务中提交，然后 ACK Mina。失败和取消也作为终态记录保存，不能冒充成功消息。
6. 分页输出 `inspection.json`，明确标记 `diagnostic_only`。
7. 通过稳定的宿主 Agent ID 导出 `agent-history.json`，仅包含读取边界上已成功、失败或取消的 Run。此操作不需要维护屏障，历史归档不能用于执行恢复。

示例遇到未知 admission、重复身份但内容不同或宿主事务失败时停止并保留未 ACK 状态。真实宿主应记录失败原因并重试，不能跳过该事实。Profile 在另一个库时使用独立 consumer 和账本；不要共用 Session ACK。

Mina 历史归档不包含独立的 `session.db`。Session/Profile 业务历史、admission 和幂等账本仍由宿主保存及导出。进程崩溃后使用原存储中的 checkpoint 恢复，不能读取历史归档重新执行。

此示例为便于阅读直接执行少量同步宿主 SQL。生产异步服务应将宿主 SQL 事务整体放在专用存储线程或 `spawn_blocking` 中，等其提交成功后再异步 ACK；不要把事务或锁跨越 await。示例不提供 admission 扫描恢复器，也不在进程启动时自动重启未完成 Run；真实宿主需根据不可变接入记录区分未 admission、执行中和已终态，使用相同 RunId 恢复，不能创建新 Run 重放不确定 effect。

## 验收命令

```sh
cargo test -p agent-extension --features sqlite --test completion_projection
cargo test -p agent-core -p agent-extension \
  --features agent-extension/sqlite,agent-extension/filesystem \
  --test host_integration --test static_agent_graph
```

`completion_projection` 在独立子进程中调用 `process::exit`，不运行 Rust 析构，分别中断在宿主事务提交前、提交后 ACK 前以及 ACK 后。父进程重新打开两个数据库验证：

| 中断点 | 重启前宿主消息数 | Mina offset | 重投影后 |
| --- | --- | --- | --- |
| 提交前 | 0 | 0 | 一条消息，offset 为 1 |
| 提交后、ACK 前 | 1 | 0 | 仍一条消息，offset 为 1 |
| ACK 后 | 1 | 1 | 无待处理记录，仍一条消息 |

测试还检查未知接入记录不 ACK、冲突内容不覆盖消息、其他 consumer 的水位不受影响。这里测试的是进程退出与跨库投影契约，不是断电模拟；终态事实通过公开事务接口预置。真实引擎生成完成事实由可运行示例及既有图测试验证。

既有 `static_agent_graph` 测试继续覆盖等待迁移后唤醒、审批绑定恢复和未确认模型调用迁移不重放；`host_integration` 覆盖节点重命名、缺少绑定零模型调用、权限收紧、内嵌 blob 完整性和 SQLite 事务边界。

外部文件/远端 blob 适配器、Pudbot 仓库接入均不在此示例范围内；完整支持边界见[公开 API](host-integration-api.md)。
