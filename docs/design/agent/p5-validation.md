# P5 最终验收记录

基线：`f849523 feat: complete event-driven tool pipeline`。
分支保持 `feat/event-driven-operator-pipeline`；没有创建分支、commit 或 push。
实现、恢复矩阵与运维步骤见 [P5 运行手册](./p5-rollout-recovery-operations.md)。

## 1. 最终门禁

以下是历史 Wait reader 兼容修复、text 指标及日志测试进程隔离之后的最终完整运行结果。

| 门禁 | 结果 |
| --- | --- |
| `cargo fmt --all -- --check` | PASS |
| `cargo test --workspace p5_` | 20 passed，0 failed |
| `cargo test --workspace` | 428 passed，0 failed，包含 doctests |
| `bash scripts/verify.sh` | PASS；包含 agent-chat 构建与 16 个 SSE frame fixture |
| `cargo clippy --workspace --all-targets -- -D warnings` | PASS |
| `git diff --check` | PASS |
| 新增/变更内容高风险密钥、私钥启发式扫描 | 0 findings；不是所有潜在秘密的形式化证明 |
| 新增/变更内容绝对临时路径扫描 | 0 findings |
| scope / 生成文件审计 | 无日志、数据库、pyc 等运行垃圾；纯 reducer、WASM、Cargo.lock 未改 |
| `cargo build -p mina-server` | PASS |
| `python3 scripts/p5-server-smoke.py` | PASS |

Server/HTTP fixture 首次在沙箱内遇到 loopback 监听权限限制，随后获准在沙箱外运行。
全部 provider 为 fake，没有调用真实模型或账号。

## 2. 独立进程 smoke

实际启动 `mina-server` 二进制与 Python 标准库 fake Responses provider，而非仅调用 Router：

- operator tools profile 的 HTTP/SSE 成功，数字 seq 连续。
- 停止并重启 Server，改为 legacy / 比例 0；旧 operator terminal replay 与原 live 一致。
- `Last-Event-ID=1` 得到精确后缀；恢复没有额外 provider 调用。
- 新 legacy 非流式请求成功；两个 Run 合计恰好两次 fake provider 调用。
- 最终 Server 端口 `54977`、fake provider 端口 `46499`；脚本连接探测均确认关闭。
- 额外 `ss -lntp` 按这两个端口复核，只有表头，无残留监听。
- config、SQLite、blob、Server 日志都在自动清理的临时目录，不加入仓库。

## 3. RED → GREEN 与故障证据

| 增量 | RED / 修复 / GREEN |
| --- | --- |
| 灰度与固定 manifest | 先新增路由/manifest 测试，接口缺失；实现确定性桶、allowlist、版本校验后通过 |
| 未知 Fabric / text / artifact 版本 | 旧代码会尝试解析未来 body 或接受未知 projection；现在先验版本、保留原始数据并稳定拒绝 |
| admission capacity | 缺少事务性限流入口；新增 `PublishBounded`，幂等重投与 drain 测试通过 |
| Session busy 泄漏 | 128 个 pending Context 下返回 503，但旧处理留下 active Run；修复后持久失败并释放 Session，Model/Tool 调用为 0 |
| artifact / Context refs | 超大 artifact 仍写 chunks、129 refs 仍成功；新增写入边界及受 schema 接受的持久失败 |
| 历史 Wait reader | 新 32 项限制误伤历史 33-wait 请求；分离新写入校验与历史 reader，不削弱身份/授权校验 |
| SSE terminal 竞态 | 慢消费场景只得到 1 帧而非 1026；按 snapshot watermark 补 replay 后 live/重连完整 |
| inventory / lease reclaim | 先新增接口与计数断言；实现只读清单、grace、引用保护与持久 reclaim 计数 |
| text observability | 初始缺 latency/batch/lag 字段；实现后验证字段、engine/profile、reclaim，且私密标记不进入日志 |

日志捕获测试曾在与 P3 其他异步测试同进程并行时失败。改为独立 integration-test 进程后，
在 workspace 默认并行配置下通过；没有删除断言、降低测试线程数或跳过测试。

故障与压力参数：

- 固定 seed `0x5eed`、32 次逻辑 kill/reopen，发布、claim、commit-before-publish、dispatch 四个点全部命中。
- 真实 SQLite busy（持有独立 writer transaction）、真实 `SQLITE_FULL`（限制页数）、SQL 写后 commit 前失败。
- 失败事务不产生 ACK、receipt、checkpoint；reopen 后可验证，原 P1 双连接 claim/fencing/DLQ 测试全部回归。
- 10000 个确定性路由样本、128 个 pending admission、1026 个 SSE 事件、容量 2 的广播慢消费者。
- 原 P2–P4 pending Context/Model/Approval/Tool/Wait、native ACK 空窗、terminal projection/replay 测试全部保留并执行。

环境：Linux 6.8.0-110-generic x86_64、8 个在线 CPU、ext4、Rust/Cargo 1.92.0、Python 3.11.15。
上述结果是可重复的功能/边界压力证据，不是长时 soak、掉电测试、吞吐基准或生产 SLO。

## 4. 文件清单与删除项

共 36 个文件：30 个修改、6 个新增。

### Server

- `apps/server/src/rollout.rs`（新增）
- `apps/server/src/admission.rs`
- `apps/server/src/main.rs`
- `apps/server/src/pipeline.rs`
- `apps/server/src/tools_pipeline.rs`
- `apps/server/src/p4_tools_tests.rs`

### Core

- `crates/core/src/context_operator.rs`
- `crates/core/src/operator.rs`
- `crates/core/src/operator_fabric.rs`
- `crates/core/tests/operator_contract.rs`

### Extension

- `crates/extension/Cargo.toml`
- `crates/extension/src/operator_fabric.rs`
- `crates/extension/src/pipeline.rs`
- `crates/extension/src/pipeline/artifacts.rs`
- `crates/extension/src/store/operator.rs`
- `crates/extension/tests/operator_fabric_contract.rs`
- `crates/extension/examples/operator_retention.rs`（新增）

### Harness

- `crates/harness/src/context_operator.rs`
- `crates/harness/src/operator_fabric.rs`
- `crates/harness/src/pipeline/coordinator.rs`
- `crates/harness/src/pipeline/effect_coordinator.rs`
- `crates/harness/src/pipeline/effect_output.rs`
- `crates/harness/src/pipeline/effect_wait.rs`
- `crates/harness/src/pipeline/model.rs`
- `crates/harness/src/pipeline/output.rs`
- `crates/harness/src/pipeline/projector.rs`
- `crates/harness/src/pipeline/tools_runtime.rs`
- `crates/harness/src/run_runtime.rs`
- `crates/harness/tests/context_operator.rs`
- `crates/harness/tests/p5_observability.rs`（新增）

### 文档与 smoke

- `docs/01-architecture-boundaries.md`
- `docs/08-single-run-state-storage.md`
- `docs/design/agent/p4-server-streaming.md`
- `docs/design/agent/p5-rollout-recovery-operations.md`（新增）
- `docs/design/agent/p5-validation.md`（新增）
- `scripts/p5-server-smoke.py`（新增）

没有删除文件。删除的是重复 engine 缓存、`effective_profile` 迁移分支和过期 P2-only/临时桥说明。
native driver 仍由默认 legacy 与历史 Flow activation 调用，不能安全删除；纯 reducer、旧 native
checkpoint reader、P3 reader、历史 artifact/wait reader 全部保留。

## 5. 候选结论与边界

达到“默认 legacy、operator 显式灰度”的代码合入候选状态，等待独立最终审查。
这不等于批准无条件生产放量：

- operator_v1 多模态仍不支持；新 admission 明确拒绝附件。
- 保留接口是只读 inventory，不提供自动 apply-GC；候选 orphan 不是完整跨存储引用证明。
- whole-snapshot SQLite、历史数据总量、全局活跃 Run 配额和长时间负载仍需部署级容量验证。
- 兼容旧 artifact/wait 的 reader 不能被新的写入限额替代。
- 没有生产 SLO/掉电保证、任意旧二进制降级保证、真实外部副作用 exactly-once 或自动 uncertain 补偿。
- 指标以 tracing/inventory 提供；统一 exporter、histogram、告警阈值由部署侧接入与实测。

独立审查应确认接受上述明确边界；不应以本地测试数字替代上线容量和风险评审。
