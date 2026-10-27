# 框架六项优化执行记录

目标：完成六项，不以部分实现或窄范围测试作为全部完成。

| 项目 | 实现与证据 | 状态 |
| --- | --- | --- |
| 调度按需读取 | Extension `store/operator/indexed.rs` 使用索引领取、分页读取，按命令加载协调记录并映射原位置；无关不兼容记录测试及 EXPLAIN 索引验证通过 | 完成 |
| 内存事务复制 | Core `agent/memory_fabric.rs` 使用局部 undo、append 水位，覆盖部分 claim 溢出时回滚；测试验证历史容器地址不变 | 完成 |
| 流式背压 | 32 条消息、单条 32 KiB、队列 256 KiB 字节预算；拉取式 event_pages，单页 4 MiB；取消/关闭释放与 600 delta 慢读重放测试通过 | 完成 |
| checkpoint 增量写入 | journal previous 分块链、增量摘要、独立提交锁；序列化一致快照后释放状态锁再做 IO；阻塞存储下预览可读、每个 delta 只写一次、崩溃恢复测试通过 | 完成 |
| 资源回收 | RetireRun、ArtifactRoots、CollectArtifacts，以及 Extension 文件系统回收入口；引用链/会话根/宽限期/中断上传/旧版孤立分块、租约保护、删除失败回滚和退休 fencing 测试通过 | 完成 |
| 性能观测 | runtime.metrics 提供调度事件等待、Fabric 耗时、checkpoint 耗时与新增字节、租约/CAS 冲突、发送等待和实时缓冲量；成功、失败、取消路径有测试 | 完成 |

## 验证范围

- Core：83 项单元测试、5 项 agent_api 测试。
- Extension：启用 sqlite/filesystem 的 23 项单元测试（含磁盘写满和文件系统回收）、35 项 Fabric 契约、8 项 checkpoint 事务测试。
- Harness：11 项流式、10 项 effect pipeline、4 项 wait pipeline、2 项进程恢复测试。
- 资源回收：11 项 p7_retention 测试，覆盖 Memory/SQLite、关闭重开、dry_run、跨运行保留、外部字符串引用不授予删除权、活跃租约、待发布 outbox、跨运行可见性屏障后的未确认事件、共享引用、退休后重放/发布/迟到提交、删除失败原子回滚、内嵌 blob 链、损坏引用停止清理和异常上传。
- Clippy：Core/Extension sqlite+filesystem，`-D warnings -A clippy::collapsible_if`（沿用既有 collapsible_if 豁免）。
- 架构边界脚本、diff 空白检查、agent_chat_demo 编译。

## 约束与使用方式

默认流式状态仅在 checkpoint 落盘；失败只回到最后成功的 checkpoint，不重新调用中断的模型，不撤销工具外部副作用。Core 不依赖 SQLite，持久化具体实现留在 Extension。

回收是显式维护 API，不自动删除用户数据。宿主必须在取得根引用快照前暂停写入和运行，直到回收结束；文件系统实例锁不能替代跨进程和跨存储的维护屏障。会话附件根必须由宿主提供。策略、调用顺序和非原子文件删除的重试语义见 [资源回收](resource-retention.md)。

SQLite 单库版本标记升级为 4；运行退休保留小型标记和事件槽位以维持原 offset，回收载荷而不承诺元数据零增长。领取仍可能遍历相关索引前缀，维护盘点仍为全局操作。Demo HTTP 的 Last-Event-ID 恢复仍不支持，Core 提供游标恢复和明确 reset；断开是否取消由所选 API 决定。

所有回收和迁移验证使用临时存储；未打开用户运行库或重启服务。
