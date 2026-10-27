# P6 第二轮 review 阻断项修复

日期：2026-10-24。基线：`3490ad310a0d202d0d24d6f6649af1f85d1954c4`。
本轮只修复坏请求枚举隔离和旧版名称重放；不改写此前提交，不 push，不调用真实上游。

## High：枚举阶段错误绕过逐 Run 隔离

根因：`requests()` 在循环中使用 `?` 解码 payload、加载 request artifact。
因此 `recover_once` 尚未进入逐 Run 处理就返回，启动时的同一调用也会阻塞服务器启动。

修复：新增 `scan_requests()`，为每个事件返回 envelope 中的 run_id 和独立的
`Result<ContextRequest, FabricError>`。分页游标正常推进，单项错误不会截断枚举。
恢复扫描逐项记录固定 `run_recovery_failed` 日志，继续处理健康 Run，扫描结束返回首个错误。
启动路径也使用该接口；损坏 Run 的 ID 留在 pending 集合中，避免 legacy interrupted 清理
误终结它，同时日志报告故障。原 `requests()` 保持完整集合的严格 Err 契约，不默默丢记录。
Fabric 整页读取失败仍返回错误，不伪装为逐 Run 成功。

测试直接修改 SQLite 中第一个 Run 的 `run.requested` payload，或删除其 Blob artifact，
或写入等长度的无效 artifact JSON。Context worker 在注入前停止，后续健康 Run 已持久化。
三个测试均验证扫描返回错误、健康 Run 实际调用 fake Model、坏 Run 不获执行 lease。

## Medium：旧 null 被无条件拒绝

根因：旧记录没有 attachment_names provenance，但规范名称可来自 Blob 存储名称。
原实现遇到 null + 非空规范名称就拒绝，破坏旧版曾接受的原样 null 重放。

修复：缺 provenance 时，null 和 omitted 均通过可信 Blob metadata 确认名称来源。
规范名称与存储名称相同才允许；无存储名称的请求别名不能被删除；无法验证则拒绝。
新记录仍严格比较 Omitted/Null/Named，已知身份的离线相同重放和旧 size 校验均保持不变。
不修改旧 request hash、artifact 或 checkpoint。

## 实际 RED → GREEN

在生产代码变更前执行：`cargo test -p mina-server p6_second_review -- --nocapture`。
结果：**4 failed / 5 passed**。没有把原本通过的保护测试或编译错误计作 RED。

| 测试（前缀 p6_second_review_） | RED 原因 | GREEN |
| --- | --- | --- |
| corrupt_payload_isolated | 健康后续 Run 接收模型请求超时 Elapsed(()) | 扫描报错，健康 Run 执行 |
| missing_artifact_isolated | 同上，缺失 artifact 在枚举中传播 Err | 扫描报错，健康 Run 执行 |
| corrupt_artifact_isolated | 同上，无效 artifact JSON 在枚举中传播 Err | 扫描报错，健康 Run 执行 |
| legacy_stored_name_null_replays | 实际 409，预期 202 | 202，返回原 Run ID |

其余五项初次即 GREEN：存储名称 + omitted 原样重放；请求别名改 null/omitted 冲突；
名称来源不可验证时 null/omitted 拒绝。修复后新增测试 **9/9 passed**。

## 验证边界

日志仅记录 Run ID 和固定错误代码，不输出坏 payload、私密名称或 Blob 内容。
输入 BlobStore 仍是二进制事实源；无新 GC、Provider 协议、多模态输出或 UI 修改。
仅使用合成持久记录和 fake Model；不声明验证过真实上游或生产数据全集。

## 最终门禁（真实执行）

| 命令/检查 | 结果 |
| --- | --- |
| `cargo test -p mina-server p6_second_review -- --nocapture` | 9 passed |
| `cargo test --workspace p6` | 41 passed，无失败/忽略 |
| `cargo test --workspace` | 469 passed，无失败/忽略 |
| `bash scripts/verify.sh` | PASS，120 tests；node frame-shapes PASS |
| `cargo fmt --all -- --check` | PASS |
| `cargo clippy --workspace --all-targets -- -D warnings` | PASS |
| `git diff --check` | PASS |
| secret/base64/absolute-temp-path/UI/provider-scope scan | 9 个修改/新增文件，0 findings |
| `cargo build -p mina-server` | PASS，smoke 使用本轮二进制 |
| `python3 scripts/p6-server-smoke.py` | PASS；4 次本地 fake Provider 调用，Session history、legacy、持久层扫描通过 |

smoke 自检 server/provider 端口均已停止，进程已退出；没有真实上游调用。
RED/GREEN 和门禁日志保留于执行环境，本文记录提交内的审计摘要。
