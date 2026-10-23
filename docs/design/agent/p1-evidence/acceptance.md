# P1 验收记录

日期：2026-10-23。初始工作区 clean，HEAD 为 `4c17d95`；分支仍为 `feat/event-driven-operator-pipeline`。没有创建分支、commit、push。

## 文件清单

| 文件 | 修改 |
| --- | --- |
| `crates/core/src/operator_fabric.rs` | 新增最小端口、命令/响应、能力描述、lease/receipt/outbox/DLQ DTO 和输入上限 |
| `crates/core/src/lib.rs` | 仅导出新模块 |
| `crates/extension/src/operator_fabric.rs` | 共享转换引擎及内存 adapter |
| `crates/extension/src/lib.rs` | 仅导出新模块 |
| `crates/extension/src/store/operator.rs` | 为现有 SqliteEventStore 实现同一个端口，真实 SQLite 事务 |
| `crates/extension/src/store/event.rs` | 既有 migration additive 增加一个版本化状态表，旧方法不变 |
| `crates/extension/src/store/mod.rs` | 注册 SQLite 扩展实现模块 |
| `crates/harness/src/operator_fabric.rs` | 无业务决策的 tick、handler 端口、receipt 去重、退避/DLQ、过期检查 |
| `crates/harness/src/lib.rs` | 仅导出新模块 |
| `crates/extension/tests/operator_fabric_contract.rs` | 同套 memory/SQLite contract suite、独立连接并发、SQL 故障、reopen |
| `crates/harness/tests/operator_fabric_runtime.rs` | 协调器 4 个行为测试 |
| `docs/design/agent/p1-fabric-transactions.md` | 接口、事务、迁移边界、限制与 TDD 证据索引 |
| `docs/design/agent/p1-evidence/` | 原始 RED/GREEN、门禁、范围搜索日志与本记录；局部 `.gitignore` 仅允许保存此目录的 `.log` |

## 验收门禁

| 命令 | 最终结果 | 原始日志 |
| --- | --- | --- |
| `cargo fmt --all -- --check` | PASS，exit 0，无输出 | 本次执行输出为空；后续 verify 再次运行相同检查 |
| `cargo test -p agent-extension --features sqlite --test operator_fabric_contract` | PASS，26 passed，0 failed | `17-final-contract.log` |
| `cargo test -p agent-harness --test operator_fabric_runtime` | PASS，4 passed，0 failed | `18-final-runtime.log` |
| `cargo test -p agent-core -p agent-harness -p agent-extension -p mina-server` | PASS，18 个 test result 段合计 248 passed，0 failed，0 ignored（含 doctests） | `15-package-tests-unrestricted.log` |
| `bash scripts/verify.sh` | PASS，102 tests，frame-shapes 16 个事件校验 PASS | `21-final-verify.log` |
| `cargo clippy --workspace --all-targets -- -D warnings` | PASS，exit 0 | `19-final-clippy.log` |
| `git diff --check` | PASS，exit 0，无输出；另检查未跟踪新增文件 | 下述最终检查记录 |
| 禁止执行点与受保护路径搜索 | PASS，无新真实 ModelProvider/ToolPort/ContextEngine 调用 | `20-scope-search.log` |

完整四包门禁第一次在沙箱内失败：3 个**既有** provider 测试绑定本地 TCP listener 时得到 `PermissionDenied / Operation not permitted`。保留 `13-package-tests.log`。随后请求并获准在沙箱外重跑相同命令，全部通过；没有修改或跳过这些测试，没有真实 provider/tool 网络调用。

归档日志仅规范化空行以通过 whitespace 检查，命令输出内容及测试结果未改；未经规范化的完整输出仍在 `/tmp/mina-p1-evidence/`。最终 diff/新增文件检查记录为 `22-final-diff-check.log`。

`verify.sh` 未设置 SKIP_FRAMES，实际执行 Node 帧形状校验。没有更改 fixtures 让门禁通过。

## RED → GREEN

- 原始 17 个存储契约测试全部在 stub 上运行失败，而不是只记录编译失败：`01-contract-red.log` → `02-contract-green.log`。
- 4 个协调器测试在 stub 上运行失败：`03-runtime-red.log` → `07-runtime-green.log`。
- 两 adapter 的 offset 稳定性回归：`04-offset-red.log` → `06-contract-green.log`。
- 两 adapter 的 outbox batch 原子可见性回归：`05-outbox-batch-red.log` → `06-contract-green.log`。
- 两 adapter 的 crash/reclaim 重试上限回归：`08-crash-retry-red.log` → `09-contract-recovery-green.log`。
- 最终定向结果为 26 + 4 全部通过，且完整四包门禁再次覆盖这些测试。

## 恢复与原子性证据

contract suite 的 11 个行为分别跑 memory 和 SQLite，共 22 个；加 4 个 SQLite 专项，共 26 个。

1. Single replay/conflict、batch 非法项及冲突回滚、batch retry、相同/冲突重复 ID。
2. ACK hole 拒绝、过滤后连续 offset、乱序序号拒绝、重复 ACK/receipt、跨分区 offset 不漂移。
3. 过期 lease、reclaim epoch 增长、旧 token 的 Commit/Retry 拒绝。
4. Commit 失败时 receipt/outbox/ACK 都不可见；成功后 receipt/ACK 可见、outbox 等待派发；派发完整 batch，不拆分。
5. 实际关闭并 reopen SQLite 验证 commit 前失败、commit 后未派发恢复、恢复后再 reopen 无重复。
6. 真实 SQL 写入后，通过 deferred FK 使 COMMIT 失败，检查连 trigger side write 一起回滚，再关闭/reopen 检查三项状态。不是 mock 调用次数，也不是只测试 publish。
7. 显式 retry/backoff → DLQ，反复 lease 过期 → DLQ，DLQ/offset reopen 后仍存在。
8. 独立 SQLite 连接并发 publish/claim，证明不是仅依靠同一个 Rust Mutex 的互斥。

## 没有进入 P2–P5

- 新模块仅由自身实现/导出及测试引用；不存在 Server/RunRuntime/FlowRuntime 的新执行 wiring。
- `apps/`、`crates/multi/`、`operator.rs`、`operator_compat.rs`、`run_runtime.rs`、`scripts/fixtures/`、总设计和 P0 冻结文档的 diff 均为空。
- 不迁移 prepare_run_plan，不定义 Context/Model/Tool/Output operator，不调用真实 provider/tool，不改变 RunEvent、HTTP/SSE 路径和 DTO。
- 没有引入 Kafka/NATS/Redis，没有删除 EventStore/FlowStore/JobStore/multi，也没有改依赖或 Cargo.lock。

## 未验证事项

确定性故障点和真实 COMMIT 失败已经验证；未做 OS SIGKILL/掉电/磁盘损坏测试。快照式存储的规模性能、历史清理、跨分区队头阻塞未压测。外部副作用 exactly-once、领域 checkpoint/CAS 与跨库事务不在本轮保证内。详见主报告“限制与开放问题”。
