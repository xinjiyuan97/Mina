# 单次 Run 状态与持久化

## 1. 范围

这一层只管理一次独立执行：一个输入、一个 `run_id`、一条有序事件流和一个唯一终态。它不引入 `session_id`，不把上一轮消息传给下一轮，也不实现跨 run 的记忆；这些属于下一阶段的 Session 层。

目标是让单次 run 具备以下性质：

- 状态机定义在 Harness，而不是 HTTP Gateway 或数据库实现中；
- 每个事件按 `(run_id, seq)` 追加并与状态投影原子提交；
- SSE 连接不是 run 的所有者，客户端断线不影响后台执行；
- 已持久化事件可以查询和断线续传；
- Server 重启后不存在永久卡住的“运行中”记录；
- 不能恢复的旧进程 Future 会被显式标记为 `run_interrupted`。

## 2. 边界

```text
Agent -> AgentEvent
            |
         Harness                 负责 run_id、seq、唯一终态
            |
         RunEvent
            |
       Server RunRuntime         负责后台执行、取消、实时广播和持久化队列
          |          |
       RunStore      SSE         非终态实时广播，终态通过持久化屏障
          |
    SQLite adapter              长连接、批量事务
```

`agent-core::harness::RunStore` 是端口，`agent-extension::store` 提供 adapter。Core 不依赖 SQLite，其他宿主可以实现 PostgreSQL、远端 KV 或自定义事件数据库。

## 3. 状态契约

`RunSnapshot` 是当前状态的可序列化投影，包含：

- `schema_version`、`run_id`、`revision`、`last_seq`；
- 原始 `input`、累积 `output`、独立 `reasoning`、token usage；
- `status`、finish reason 或稳定 failure；
- 工具调用的参数、执行阶段、输出或错误；
- 审批请求及其 resolution；
- 创建和更新时间。

兼容执行与 durable Flow 合并后的 run 状态机：

```text
Accepted -> Running -> WaitingApproval/WaitingEvent -> Running
                   \-> ExecutingTool                  -> Running

Accepted | Running | WaitingApproval | WaitingEvent | ExecutingTool
                   -> Completed | Failed | Cancelled
```

`WaitingEvent` 必须同时有 versioned checkpoint、wait subscriptions 和已提交 outbox，启动后可以被匹配事件唤醒。`Accepted/Running/ExecutingTool -> Failed(run_interrupted)` 仍是有意允许的恢复路径：进程可能在没有安全 checkpoint 时退出，运行时不能重放任意模型流或未知副作用 Tool。

`RunSnapshot::apply` 是状态转换的唯一实现，所有 Store adapter 都必须复用它。它校验：

- event 的 `run_id` 与 snapshot 一致；
- `seq` 必须等于 `last_seq + 1`；
- 终态后不能继续追加；
- 审批和工具事件必须引用已存在的对象；
- revision 每成功应用一个事件增加一次。

## 4. RunStore 端口

```rust
pub trait RunStore: Send + Sync + 'static {
    fn create_run(&self, snapshot: RunSnapshot) -> RunStoreFuture<'_, RunSnapshot>;
    fn get_run(&self, run_id: RunId) -> RunStoreFuture<'_, Option<RunSnapshot>>;
    fn append_event(
        &self,
        event: RunEvent,
        observed_at_ms: i64,
    ) -> RunStoreFuture<'_, RunSnapshot>;
    fn append_events(
        &self,
        events: Vec<ObservedRunEvent>,
    ) -> RunStoreFuture<'_, RunSnapshot>;
    fn events_after(
        &self,
        run_id: RunId,
        after_seq: u64,
        limit: usize,
    ) -> RunStoreFuture<'_, Vec<RunEvent>>;
    fn unfinished_runs(&self) -> RunStoreFuture<'_, Vec<RunSnapshot>>;
}
```

adapter 必须保证：

1. event 插入和 snapshot 更新处于同一事务；
2. `append_events` 按输入顺序原子提交一个非空、连续的事件批次，只在批次末尾写一次最终 snapshot；
3. 相同 `(run_id, seq)` 和相同内容可以幂等重放；
4. 相同 key、不同内容返回 `EventConflict`；
5. 缺号、跳号和终态后追加由统一投影拒绝；
6. `events_after` 严格按 `seq` 升序返回。

## 5. SQLite adapter

默认 Server 使用 `agent-extension::store::SqliteRunStore`，数据库路径为：

```text
data/runs.sqlite3
```

可通过环境变量覆盖：

```bash
MINA_RUN_STORE_PATH=/absolute/path/to/runs.sqlite3 pnpm dev:server
```

SQLite 开启 WAL、foreign key 和 busy timeout。Run、Session 与 Context 共用一个进程级长生命周期连接，Memory 使用另一个长生命周期连接；PRAGMA 只在 adapter 启动时配置，不再为每次 Store 操作重新打开和关闭同一个文件。逻辑表为：

```text
runs
  run_id, status, last_seq, created_at_ms, updated_at_ms, snapshot_json

run_events
  run_id, seq, observed_at_ms, event_json
  primary key (run_id, seq)
```

JSON 保存版本化契约，索引列只服务查询和并发校验。Store 还提供 `InMemoryRunStore`，只用于测试或明确不需要重启持久化的嵌入场景。

## 6. 执行与订阅

`RunRuntime` 在创建持久化 snapshot 后启动后台 task。后台 task 持有 Harness event stream；HTTP SSE 只持有广播 receiver。因此：

- SSE 断开不会隐式取消 run；
- 客户端停止操作仍通过显式 cancel API；
- 非终态事件先实时广播，再进入容量受限的后台持久化队列，SQLite 延迟不会直接阻断模型事件消费和 SSE；
- writer 在最多 40ms 或 128 条事件内聚合写入；`output_delta` 和 `tool_call_arguments_delta` 可以等待短窗口，工具、审批、usage 和生命周期边界会立即刷新当前批次；
- 聚合不会删除事件或改变 `seq`：批次中的事件仍可逐条精确重放，但只使用一个事务并只更新一次最终 snapshot；
- `run_completed`、`run_failed` 和 `run_cancelled` 在后台 writer 清空并成功提交后才广播，形成终态持久化屏障；
- 单次批量写入有 10 秒上限；队列有界，积压超过容量时向 Agent event stream 传播背压而不是无限占用内存；
- 广播 receiver lagged 时从 Store 补齐缺失 seq；
- 重连时先订阅活动广播，再读取历史，最后按 seq 去重，避免查询与订阅之间丢事件。

## 7. HTTP API

### 创建并流式接收

```http
POST /api/v1/runs
Content-Type: application/json

{"input":"...","stream":true}
```

响应是 SSE。每条事件包含 SSE `id: <seq>`，现有事件名称和 JSON envelope 保持不变。

### 查询状态

```http
GET /api/v1/runs/{run_id}
```

返回当前 `RunSnapshot`，终态 run 在 Server 重启后仍可查询。

### 续传事件

```http
GET /api/v1/runs/{run_id}/events?after_seq=12
Last-Event-ID: 12
```

`after_seq` 优先于 `Last-Event-ID`。接口先重放 `seq > after_seq` 的持久化事件；run 尚未结束时继续等待实时事件，读到终态后关闭流。

### 取消

```http
POST /api/v1/runs/{run_id}/cancel
```

只有当前进程内的 active run 可以取消。已终结或由旧进程遗留的 run 返回 `run_not_active`，而不是伪造一次取消。

## 8. 重启语义

当前阶段不会序列化 Rust Future、Tokio task、模型连接或工具进程。Server 启动时查询全部非终态 snapshot，并依次追加：

```json
{
  "type": "run_failed",
  "code": "run_interrupted",
  "message": "the server restarted before this run reached a terminal state",
  "retryable": true
}
```

这样历史 run 始终获得唯一、可解释的终态，客户端也能通过续传 API 读到该终态。真正“从等待点继续执行”需要 checkpoint、subscription 和 effect inbox，属于后续事件运行时，不在单次 run 状态层假装实现。

## 9. 数据与安全

数据库会保存用户输入、模型 reasoning/output、工具参数和工具输出，属于敏感运行数据。当前本地版本依赖文件系统权限保护；生产 adapter 应增加访问控制、加密、保留期限和删除策略。模型 API Key 不进入 `RunEvent` 或 `RunSnapshot`。
