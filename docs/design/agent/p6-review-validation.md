# P6 review 修复证据

日期：2026-10-24。修复基线：`729ef0c7bb50829edc5c415e985ec2b7b80b8398`。
不改写 P5/P6 历史，不改 Provider 协议，不新增多模态输出。

## 根因与修复

1. **High：恢复配置漂移**。text start 使用当前 model/output limit 重算 admission digest，
   与已保存的执行配置冲突。现在只从已存在 reducer 取执行配置，外部提交的 input、ordered
   attachments、prior history、fingerprint 仍通过原 admission digest 校验。恢复扫描由每条
   request 独立处理，失败后继续扫描，并保留首个错误作为本轮结果，不静默吞错。
2. **Medium：缺失元数据通配符**。Option name 混淆 missing/null；旧 size None 被视为任意值。
   现在保存可选 name provenance（Omitted/Null/Named），参与 request hash 和 Context artifact，
   重放逐项严格比较。旧未知 size 的新增断言必须用可信 Blob 元数据确认，否则 409。
   已知的完全相同身份直接使用持久 request hash，不重新读取输入 Blob。
3. **Low：旧 checkpoint 证据不真实**。原测试删除字段后又调用当前 admit，测试的是当前 writer。
   现替换为固定 P5 SQL dump、原始 Blob artifact、reducer、digest，当前代码直接读旧数据。
   旧附件无 size 的读取、原 digest 重放以及 BlobId/MIME/name/size 变化冲突均覆盖。

## 实际 RED → GREEN

首先只增加服务器测试，执行 `cargo test -p mina-server p6_review -- --nocapture`，
10 项中 8 项失败、2 项原本通过；此时未修改生产代码。真实失败如下：

| 测试后缀（均为 p6_review_） | 最初真实失败 | GREEN 断言 |
| --- | --- | --- |
| text_recovery_pins_model | Persistence("conflicting durable fact") | 改默认 model 后仍以 original-model / 77 恢复 |
| text_recovery_pins_output_limit | Persistence("conflicting durable fact") | 改默认 output limit 后仍使用原值 |
| bad_run_does_not_starve_recovery_scan | 健康 Run 接收模型请求超时 Elapsed(()) | 坏 Run 报错，后续健康 Run 仍执行 |
| name_omission_conflicts | 202 而非 409 | 删除原请求 name 拒绝 |
| name_null_conflicts | 202 而非 409 | name 改 null 拒绝 |
| omitted_and_null_names_are_distinct_offline | 202 而非 409 | omitted/null 严格区分，相同离线重放成功 |
| old_size_wrong_conflicts | 202 而非 409 | 旧 size 未知时，错误 size 拒绝 |
| old_size_unverifiable_conflicts | 202 而非 409 | 旧 size 未知且 Blob 不可读时拒绝新 size 断言 |
| name_presence_is_part_of_persisted_request_hash | 两个持久 request_hash 完全相同 | name omitted/null 在 Session reservation 阶段已具有不同 hash |

最后一项单独追加、运行 RED 后才修改 hash 实现。两项恢复测试还分别验证 input、附件顺序、
BlobId、MIME、name、prior history、fingerprint 变化仍 conflict。

以下保护最初已 GREEN，不伪造 RED：旧 size 正确重放；已知 size 变化拒绝及完全相同离线重放。
两项 name 删除测试也先确认完全相同重放在输入 Blob 不可读时仍返回原 Run。

真实 P5 fixture 测试不需要修改生产恢复协议，最终直接恢复通过。测试搭建时曾因 SQL dump
还原顺序的外键约束、iterdump 不导出 user_version 失败；补齐原数据库 user_version=5 并在
导入期间禁用外键后通过。这些是 fixture 搭建错误，不算生产行为 RED。

## 兼容与安全边界

- 新 provenance 字段 default None + skip None，不改变旧 ContextRequest 的序列化身份。
- 旧 ModelAttachment.size_bytes 继续 optional，旧 admission hash/digest 不重写。
- 输入 Blob 无法读取时，相同已知身份不重新验证字节；admission artifact 自身必须可读取。
- 旧记录缺少足够元数据时只信任持久事实或 Blob metadata，不能猜测缺失字段。
- 日志只添加 run_id 和固定错误码；不记录名称、输入文本、二进制或上游凭据。
- 本轮不改变 GC、Provider 能力矩阵、UI；没有真实上游调用或多模态输出。

## 最终门禁（真实执行）

| 命令/检查 | 结果 |
| --- | --- |
| `cargo test -p mina-server p6_review -- --nocapture` | 11 passed |
| `cargo test -p agent-harness --test p6_old_fixture` | 1 passed；两个原始 P5 Run |
| `cargo test --workspace p6` | 32 passed，无失败/忽略 |
| `cargo test --workspace` | 460 passed，无失败/忽略 |
| `bash scripts/verify.sh` | PASS，包含 node frame-shapes 检查 |
| `cargo fmt --all -- --check` | PASS |
| `cargo clippy --workspace --all-targets -- -D warnings` | PASS |
| `git diff --check` | PASS |
| secret/base64/absolute-temp-path/UI/provider-scope scan | 修改与新增文件扫描，0 findings |
| `cargo build -p mina-server` | PASS，smoke 使用本轮编译二进制 |
| `python3 scripts/p6-server-smoke.py` | PASS；4 次本地 fake Provider 请求，Session history、legacy、持久层二进制扫描通过 |

smoke 自检 server/provider 两个临时端口均停止，进程已 wait；未调用真实账号/上游。
运行日志保留在执行环境中；上述表格和 RED 原因是提交内的审计摘要。
无法自行验证真实生产数据全集、真实上游或外部独立验收；本轮无此类成功声明。
