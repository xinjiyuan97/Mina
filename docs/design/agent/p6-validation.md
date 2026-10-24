# P6 验证记录

日期：2026-10-24；起点工作区 clean，分支 `feat/event-driven-operator-pipeline`，
HEAD 为 P5 `0b5ec6e3bdd508e96cb52732b3d4dad863e8ada8`。未改写 P5、未建分支、未 push。

## RED → GREEN（真实执行）

先添加测试并执行，再修改对应生产逻辑。初始定向命令：

```sh
cargo test -p mina-server p6 -- --nocapture
cargo test -p agent-extension --features openai p6 -- --nocapture
cargo test -p agent-harness p6 -- --nocapture
```

以下为实际行为失败，而非把编译失败当成 RED：

| 测试（省略模块前缀） | 最初真实失败原因 | GREEN 后 |
| --- | --- | --- |
| p6_operator_image_attachment_only_and_order_reach_model | admission 返回 422，operator 附件硬拒绝 | 图片/文档有序进入 ModelRequest；空文本合法；重放成功、反转附件冲突 |
| p6_text_profile_preserves_attachments | admission 返回 422；text profile 后续亦有附件拒绝/未传递 | text profile 附件进入模型 |
| p6_request_hash_binds_order_mime_name_and_blob | name 改变后 hash 相同，identity field 3 断言失败 | BlobId/顺序/MIME/name/size 改变均改变 hash |
| p6_blob_validation_contract | 已命名 Blob 的错误 name 被忽略，未返回 metadata mismatch | missing/MIME/name/size/duplicate/>8 均拒绝；SVG/禁用 profile 422；大文件 413 |
| p6_session_idempotency_conflicts_before_metadata_validation | 首次 422；移除限制后换 Blob 返回 400 而非 409 | 原 Run 重放；Blob/顺序/MIME/name 变化 409 |
| 同一 Session 测试的离线重放增强 | 原 BlobStore 不可见且配置改为 text-only 时，同请求返回 409 而非 202 | 从持久 admission 比对并返回原 Run，不重新读取输入二进制 |
| p6_session_history_and_attachment_name_fingerprint | 历史 BlobRef 恢复已通过，但重命名后 request digest 相同 | 历史附件保留；name 同时改变 digest/fingerprint |
| p6_rejects_non_user_attachments | Anthropic System 附件静默丢弃而非错误 | System/Assistant/Tool 都拒绝 |
| p6_rejects_tool_attachments | Responses Tool 提前返回，忽略非法附件 | Tool 附件拒绝 |
| p6_responses_input_matrix | 正向 image 映射已通过，Tool 角色拒绝断言失败 | image/input_file 矩阵与非 User 拒绝通过 |
| p6_anthropic_input_matrix | image/svg+xml 错误作为 image 接受 | image/PDF 成功；SVG/audio/video/普通文件拒绝 |
| p6_chat_input_matrix | image/svg+xml 错误作为 image 接受 | image/WAV/MP3/file 成功；SVG/OGG/video 拒绝 |
| p6_model_artifact_must_match_durable_reducer_request | 等长替换 artifact 中 BlobId 后未返回 Conflict | 与 pending reducer request 比较，调用模型前拒绝 |
| p6_blob_errors_do_not_leak_backend_details | HTTP body 出现测试后端私密路径标记 | 返回固定 blob_store_error 提示 |
| scripts/p6-server-smoke.py | 纯附件 POST /runs 返回真实 HTTP 422 | 本地 fake HTTP/SSE 完整通过 |

诚实记录测试构建过程：最初使用了错误 package 名 `agent-server`，随后改为 `mina-server`；
测试曾误用 PlannedRun.clone、无 filesystem feature 的测试 helper、PairTool tuple 构造，均只修正测试。
artifact 首版注入改变了文件长度，既有 BlobStore 已拒绝；改成等长 BlobId 替换，重新执行 RED，
确认缺少 reducer 对照校验。执行中 Model 恢复测试最初错误期待重复调用；阅读 P5 实现后修正为
`model_interrupted`、调用次数不增加，并在新 Run 另验证工具多轮附件保留。没有为测试改变 P5 恢复策略。

## 既有能力回归与真实进程故障

这些保护在初次运行已是 GREEN，不人为破坏实现来制造 RED：

- pending Context 的持久引用、顺序与四类身份冲突；claimed/uncommitted Context 的 lease 回收与旧 ACK fencing。
- pending Model 的 artifact/event 引用对应、SQLite reopen、旧 text start 缺失 attachments 的兼容读取。
- P5 outbox/checkpoint/external Session roots 的 retention dry-run 保护；本轮没有 Blob 删除任务。
- Session BlobRef 历史恢复。新增 name digest 断言暴露的是身份哈希缺口，而不是历史读取缺口。

新增独立进程测试 `p6_real_process_kill_pending_context_and_model`：
父进程创建输入 Blob，子进程分别提交 pending Context 和 pending Model 后发 ready 标记；
父进程真实 kill 并 wait，删除原 ingress fixture，再仅依赖 SQLite/BlobStore 恢复。
两处均比对完整有序 ModelAttachment，且模型调用一次。子进程 Drop guard 防止失败时遗留进程。

另有 tools Model 执行中 abort/reopen/lease expiry 测试：不重调上游、原附件仍在 checkpoint；
新 Run 的 tool round 和下一次 ModelRequest 继续保留附件。

## 覆盖需求映射

| 需求 | 证据 |
| --- | --- |
| 1–3 图片、纯附件、多附件顺序 | server p6、两个 profile、HTTP/SSE smoke 五种 MIME |
| 4、9 reopen / pending Context / pending Model | context_operator、p3_pipeline、p4_effect_pipeline、独立进程 kill 测试 |
| 5 Session 下一轮历史附件 | context_operator 实际 ContextEngine + smoke 实际 Session API |
| 6–7 profile/MIME 与附件验证错误 | p6_blob_validation_contract |
| 8 幂等与身份冲突 | hash、Context publish、pipeline start、Session HTTP handler；离线/配置变化重放 |
| 10 legacy 不回归 | workspace legacy checkpoint/recovery 测试与 smoke legacy 图片输入 |
| 11 旧纯文本 checkpoint | 缺失 attachments 的 start 反序列化、pending ledger reopen；workspace P3/P5 兼容测试 |
| 12 持久层/SSE 无二进制 | request artifact/事实断言；smoke 扫 SQLite/WAL/日志/Session wire，排除唯一 BlobStore 目录 |
| 13 Provider 矩阵 | 三 adapter 的 p6 matrix、非法角色测试与本地 mock SSE |

## 最终门禁真实结果

最后一次完整门禁链退出码 0；保留原 P2 legacy lease/reopen 测试，另外新增 P6 同场景。

| 门禁 | 实际结果 |
| --- | --- |
| cargo fmt --all -- --check | PASS |
| cargo test --workspace p6 -- --nocapture | 20 passed；含一个子进程 helper |
| cargo test --workspace | 448 passed，0 failed，0 ignored（含 doctest） |
| bash scripts/verify.sh | PASS；core/extension 120 tests，agent-chat build，16 个 SSE frame 校验 |
| cargo clippy --workspace --all-targets -- -D warnings | PASS |
| git diff --check / git diff --cached --check | PASS |
| staged secret/base64/temp-path/scope scan | PASS；无私钥、长凭证/长 Base64、硬编码临时绝对路径、数据库/日志/pycache；无 UI/Cargo/MediaProvider 越界 |
| cargo build -p mina-server | PASS |
| python3 scripts/p6-server-smoke.py | PASS；4 次本地 fake Provider 调用，Session/history/legacy/durable-binary scan 全绿 |
| 服务清理复核 | 最后一次 server 57247、fake Provider 44861 均已关闭；无 mina-server 残留进程 |

secret scan 为本次 staged patch 的正则/范围扫描，不等价于第三方安全审计；
Base64 编码逻辑允许留在 adapter 的执行路径，测试也会检查该协议编码，但持久层不得出现输入字节。
完整命令输出保留于本次运行的临时日志；本文件保留可随提交审查的行为、失败原因与最终结果。

## 验证边界

- 未访问真实 Provider/账号；mock 验证协议编码与执行链路，不证明真实模型理解图片/音频/视频或任意文件。
- Responses audio/video 仅 generic input_file；Chat 无 video；Anthropic 无 audio/video/普通文件。
- P5 retention 仍是只读 inventory，没有自动删除；不能将本轮测试解释成已经实现完整可删除的传递 GC。
- 不实现多模态输出，不新增 MediaProvider，不改 UI。没有实机断电/磁盘损坏或生产负载认证。
- 最终提交信息固定为 `feat: add multimodal inputs to operator pipeline`；提交 SHA 由最终回复给出。
