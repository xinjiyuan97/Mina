# Fabric 字节限制

64 KiB 是内部完整序列化事件的默认上限，包含 envelope、payload 和 schema，不仅是 payload 文本。模型请求携带历史消息，因此也受限制。

Server 与 CLI 的配置文件，以及 agent_chat_demo 的配置文件，支持独立顶层配置段：

```toml
[fabric]
max_event_bytes = 524288  # 512 KiB，默认 65536
max_batch_bytes = 2097152 # 2 MiB，默认 1048576
```

省略该段或某个字段时使用原默认值。配置需满足 `0 < max_event_bytes <= max_batch_bytes <= 64 MiB`，非法配置启动失败。这里的 64 MiB 是允许配置的最高批次字节值，不是新的默认值。

## 生效范围

- GraphEngine 提交预检从 `OperatorFabric::limits()` 读取实际适配器限制。
- MemoryFabric 和 SqliteEventStore 的共享状态转换层校验单事件、批次、outbox、receipt result、retry error 和 HostRecord payload。
- result/error/HostRecord payload 沿用单事件字节上限。
- 批次按完整序列化事件的字节总和计算。图的订阅路由可能生成多个投递副本，因此单事件能放下不代表整批一定能放下。
- 超限事务不部分写入检查点、事件或消费确认。图输出超限仍返回 `output_capacity_exceeded`。

配置由宿主拥有，不写进图 YAML、数据库 schema 或恢复包。这样同一个宿主的预检和存储使用同一份限制。限制不影响模型 token 窗口，也不替代 Compactor。

## Rust 装配

```rust,ignore
use agent_core::contracts::fabric::FabricLimits;
use agent_core::agent::memory_fabric::MemoryFabric;
use agent_extension::store::{SqliteEventStore, SqliteOpenOptions};

let limits = FabricLimits {
    max_event_bytes: 512 * 1024,
    max_batch_bytes: 2 * 1024 * 1024,
};
let memory = MemoryFabric::with_limits(limits)?;
let sqlite = SqliteEventStore::open_with_options(
    "data/events.sqlite3", "events",
    SqliteOpenOptions { fabric_limits: limits, ..Default::default() },
).await?;
```

`SqliteEventStore::open(...).await?.with_fabric_limits(limits)?` 也可使用；请在共享或 clone 适配器前配置。clone 会复制配置。自定义 OperatorFabric 包装器应转发底层 `limits()`；未覆盖该方法的适配器使用历史默认值。

## 重启和恢复

配置不是持久化数据，重启或独立打开 SQLite 连接时要重新传入。Server/CLI/demo 会从配置文件重新装配。降低上限后，已持久化的大事件仍可读取；新发布、恢复后的新输出和恢复包导入遵循当前宿主上限。迁移大事件的 Run 时，目标宿主需要足够大的配置。

本次不修改其他限制：每批最多 128 项、检查点最多 4 MiB（GraphEngine 另预留 64 KiB），工具/模型输出自身的大小上限仍有效。大量历史即使放宽事件限制，也可能触及检查点上限；应配合压缩或 Blob 引用。
