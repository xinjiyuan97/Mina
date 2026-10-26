# SQLite 增量存储

实现位于 Extension 的 `store/operator.rs`，Core 保持 OperatorFabric 抽象及共享事务校验，不引入 SQLite 依赖。

## 存储布局

`workflow_fabric_records` 使用 `(section, identity, position)` 复合主键。checkpoint 按 key 独立存储；公开日志按 log key 和连续序号逐条追加，并独立保存水位；协调事件、订阅（含租约）、回执、死信各自分行存储。仅新增或修改发生变化的记录。

公开日志读取直接使用 SQL 主键范围查询，只解析请求的页。读取 checkpoint 只加载对应 key。写入 checkpoint 只加载本事务涉及的 checkpoint；追加公开日志只校验 SQL 中的水位，不加载旧日志内容。保留期检查仍需加载完整数据以核对引用。

调度读取使用部分表达式索引：consumer/subscription、订阅到期时间、事件 topic/partition/id/可见性，以及回执 consumer/event id。Claim 按到期和租约状态筛选，只解码目标订阅及下一条事件；Replay、ReadEvent、ScanEvents、Offset、Receipt、DeadLetters 直接按索引或位置分页。

提交事务按涉及的订阅、topic、tenant、分区尾事件和 outbox id 加载协调记录，checkpoint 无 outbox/delivery 时不加载协调历史。仍复用 Core 状态转移校验。Core 的紧凑数组位置会映射回数据库原位置，新增记录追加到全局尾部，避免覆盖无关运行。全局 outbox 可见性屏障继续保留。

一个目标订阅自身的 topic 历史仍可能用于提交校验，领取的 OFFSET 查询也可能遍历相关索引前缀；本实现避免的是整库历史解码，不宣称所有调度操作为恒定开销。保留期盘点属于全局操作。

## 事务与迁移

checkpoint、日志追加、水位、outbox、租约确认和回执处于同一个 SQLite 事务。租约或版本冲突、日志追加失败、磁盘写满、提交失败都不会留下部分确认。

首次 Fabric 操作会在事务中将旧版 schema 1/2 JSON 拆分为记录，并将原 singleton 替换为 schema 4 标记。已有 schema 3 分行库仅升级版本标记。迁移失败会整体回滚。后续不再把整个 Fabric 序列化写回 singleton。旧二进制会因不支持 schema 4 而拒绝读取迁移后的数据库；此迁移不提供自动降级。

此变更只修改代码和测试临时库，不主动打开或迁移用户运行中的数据库。实际迁移由新版本首次访问对应数据库触发。

## 验证

覆盖旧日志和 checkpoint 迁移、跨连接事务、租约 fencing、日志水位冲突、SQL 追加故障和提交故障回滚、磁盘写满、关闭重开及进程恢复。使用数据库触发器验证追加新事件不会更新历史日志或无关 checkpoint；使用无关损坏记录验证 checkpoint 和日志页读取不解码整库。


索引查询验证：在同库放入其他运行的不兼容事件、订阅、回执及死信，目标运行的发布、领取、提交、回放和 checkpoint 仍成功；EXPLAIN QUERY PLAN 验证 consumer 查询使用订阅索引。35 项 Fabric 契约测试和 8 项 checkpoint 事务测试通过。
