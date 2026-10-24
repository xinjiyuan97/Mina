# P6：operator_v1 多模态输入

日期：2026-10-24。基线：P5 `0b5ec6e3bdd508e96cb52732b3d4dad863e8ada8`。
本阶段仅扩展输入；没有多模态输出事件、MediaProvider 或 browser-agent UI 改动。

## HTTP 与公共契约

- `POST /api/v1/blobs?name=...` 上传到唯一二进制事实源 BlobStore。
  Run/Session 的 `attachments` 使用有序 BlobId 引用，不接受远程 URL 作为附件。
- `input` 可为空字符串；仅当 trim 后无文本且附件为空时返回 `invalid_input`。
- 每 Run 最多 8 个附件；每个 Blob 最多 10 MiB。上传和引用已有 Blob 都执行大小约束。
- 引用必须存在；显式 MIME、size、已命名 Blob 的 name 必须与元数据一致；同 Blob 不可重复。
  未命名 Blob 允许请求指定展示名称，该名称也绑定请求身份。名称上限 512 bytes。
- HTTP 400：`blob_not_found`、`blob_metadata_mismatch`、`duplicate_attachment`、
  `too_many_attachments`；413：`blob_too_large`；422：`unsupported_input_modality`。
  支持的图片 MIME 为 PNG/JPEG/GIF/WebP；SVG 等图片类型不能伪装成受支持图片。
- 首先校验配置的 `modalities.input`；adapter 再校验协议。配置许可不是协议能力承诺。
  adapter 不支持的输入以非重试 `InvalidRequest` 失败，不回退另一协议/engine。
- 存储异常只返回固定提示，不返回内部路径、文件内容或私密名称。
- `ModelAttachment.size_bytes: Option<u64>` 为新增可选元数据；缺失时默认 None，序列化省略。
  没有更改 checkpoint 版本或旧格式探测逻辑。
- SessionStore 新增只读 `run_for_idempotency_key`，SQLite 使用既有索引/表，无迁移。
  已存在 key 优先读取持久化 admission，比对原附件身份并使用原 request hash；相同重放不依赖
  BlobStore 当前可用性或新 profile 配置。变化返回 409，不吞掉 MIME/name/Blob 的变化。
  admission 尚未写入的短窗口仍走已有存储验证。历史附件 size 未知而重放显式提供 size 时，
  必须使用可信 Blob 元数据确认；无法确认则返回 409，不能把 None 当作通配符。
  完全相同的新请求重放不重新读取输入 Blob，但持久化 admission artifact 仍必须可读。
- ContextRequest 新增可选 `attachment_names`，区分请求 name 的 omitted、null、Named 三种状态；
  同时参与新请求 hash，覆盖 Session reservation 到 Context publication 的窗口。
  旧记录默认 None 并省略序列化，旧 hash/digest 不重写。旧 name 缺失来源信息时保守校验，
  必要时读取 Blob 名称元数据，无法确认则拒绝，不推测原请求。

## 完整链路与身份

1. HTTP 校验附件，使用 BlobStore 的 MIME、size、name 构造 ModelAttachment；
   Session 用户消息保存 `ContentPart::BlobRef`，不保存字节。
2. request hash 对有序附件数组做带域分隔的 JSON 哈希，覆盖 BlobId、MIME、name、size。
   无附件请求保持原哈希算法。ContextRequest artifact 持久化所有引用及原 policy/profile。
3. Context preparer 把相同附件身份绑定 request digest；Context fingerprint 包含该 digest。
   历史 User BlobRef 恢复为带 size 的 ModelAttachment；历史非 User 二进制输入拒绝进入 Context。
4. context.completed 经 legacy_plan 到 PipelineStart/AgentLoopStart；text 与 tools 两个 profile
   都保存有序附件。text profile 的名称仍表示单次无工具执行，不再表示只允许文本输入。
5. coordinator 保存 start/reducer；reopen 比较附件及历史消息，不能因相同文本/fingerprint
   复用另一个附件集合。text ledger 通过 start digest 校验；tools ledger 保留完整 start。
   已存在 text ledger 使用持久 reducer.config 恢复，不使用当前默认 model/output limit；
   input、有序附件、prior history、context fingerprint 仍参与原 start digest 校验。
   恢复扫描隔离每条 Run 的恢复错误，继续处理后续 Run，最后返回首个错误供监控报告。
6. reducer 生成 ModelRequest，当前附件和历史附件在后续工具轮中保留。
   model.requested 事件只有 request_ref；artifact 为含引用的 JSON，不含输入二进制。
7. text ModelOperator 比较 artifact 与 reducer 的 pending InvokeModel；tools ModelOperator
   校验 event、request checkpoint 和 artifact 一致性。只有 adapter 构造上游请求时转换二进制编码。

同 Run 的相同输入可重放；改变 BlobId、顺序、MIME、name、size 均参与冲突判断。
Session 相同 key 重放返回原 Run，变更附件不能生成第二个 Run 或复用第一个 Run。
此处不承诺上游 exactly-once：Model 网络请求仍遵循 P5 lease/fencing/前缀恢复规则。

## Provider 输入矩阵（本仓库 adapter 契约）

| 输入 | OpenAI Chat Completions | OpenAI Responses | Anthropic Messages |
| --- | --- | --- | --- |
| PNG/JPEG/GIF/WebP | image_url（执行时 data URL） | input_image | image/base64 source |
| PDF | file | input_file | document/base64 source |
| WAV / MP3 | input_audio，wav/mp3 format | input_file | 拒绝 |
| 其他 audio | 拒绝 | input_file | 拒绝 |
| video | 明确拒绝 | input_file | 明确拒绝 |
| 普通文件 | 既有 file 语义 | input_file | 拒绝 |
| 非 User 附件 | 拒绝 | 拒绝，包括 Tool 提前返回分支 | 拒绝，包括 System/Assistant |

Responses 的 audio/video **只是 generic file 编码**，不是原生音频/视频理解保证；
Chat 普通 file 的具体格式、模型兼容性也由上游决定。这里的 mock 测试只证明协议映射，
不证明任何真实模型能够读取任意格式。没有调用真实上游或账号。

## 恢复、GC 与运维

- pending Context 从持久化 ContextRequest 恢复，不使用原 HTTP body；重复投递保持附件顺序。
- pending Model 从持久化 start/reducer/request artifact 恢复；尚未执行的 Model 正常执行。
  已开始调用的 Model 在 lease 失效后按 P5 `model_interrupted` 失败恢复，即便尚无输出前缀，
  也不重新调用上游，更不会拼接两次调用的输出。
- 旧纯文本 operator、旧 legacy checkpoint 的附件字段读法保留；新 size 字段缺失合法。
  回滚新 admission 到 legacy 不改变已接受 Run 的 engine。不能将此解释为 P5 二进制支持 P6 输入。
- P5 retention 只有 inventory/dry-run，没有 Blob 删除 API 或自动删除任务。本轮不新增 GC。
  checkpoint/outbox 中引用和 external_roots 中 Session 引用继续保护 artifact。
  独立文件系统输入 Blob 不会被 inventory 删除；未来启用删除前必须加入 Session 与所有
  artifact 的传递引用根，不能仅凭创建时间清理 Blob 目录。
- 一起备份 Run/Event SQLite（含 WAL 一致性）与 BlobStore；不要仅备份数据库。
- 日志仅增加 attachment_count、attachment_total_bytes，不记录名称、正文或 Base64。
  Session API、SSE、ledger/outbox 只暴露 BlobRef/小型元数据；显式 Blob 下载端点仍返回文件。

## 本地验证

```sh
cargo test --workspace p6 -- --nocapture
cargo build -p mina-server
python3 scripts/p6-server-smoke.py
cargo fmt --all -- --check
cargo test --workspace
bash scripts/verify.sh
cargo clippy --workspace --all-targets -- -D warnings
git diff --check
```

Smoke 仅绑定 loopback 随机端口，使用本地 fake Responses SSE；验证五类附件输入、纯附件、
Session 下一轮、幂等、SIGKILL 后 completed replay、legacy、SQLite/日志/Session 二进制扫描，
并在 finally 关闭两个端口。`p6_process_recovery` 另启动独立子进程，在 pending Context 和
pending Model 两处真实 kill，移除原 ingress fixture 后仅从 SQLite/BlobStore 恢复并比对附件。
已进入模型调用的取消/lease 回收另由定向测试验证。
详见 `p6-validation.md` 的 RED/GREEN 证据与验证限制。
