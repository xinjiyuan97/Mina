# P2 验收记录

日期：2026-10-23。分支：`feat/event-driven-operator-pipeline`。
没有创建分支、commit 或 push；没有回滚 P0/P1。

## 命令与结果

| 命令 | 结果 |
| --- | --- |
| `cargo fmt --all -- --check` | PASS |
| `cargo test -p agent-harness --test context_operator` | PASS，7 tests |
| `cargo test -p agent-harness p2_context_bridge` | PASS，1 test |
| `cargo test -p mina-server p2_` | PASS，5 tests |
| `cargo test -p agent-core -p agent-harness -p agent-extension -p mina-server` | PASS，总计 261 tests，0 failed / ignored，含 4 个 compile-fail doctests |
| `bash scripts/verify.sh` | PASS，102 tests；agent-chat 构建和 16 个 frame shape 校验全部通过 |
| `cargo clippy --workspace --all-targets -- -D warnings` | PASS |
| `git diff --check` | PASS |
| P0 fixture / native machine / provider / tool / multi bus diff | 空，未修改 |
| Gateway 业务调用与 P3/P4 搜索 | PASS，见 `05-scope.log` |

完整日志：`/tmp/mina-p2-full-tests.log`、`/tmp/mina-p2-verify.log`、`/tmp/mina-p2-clippy.log`、`/tmp/mina-p2-fmt.log`。定向与 RED 原始日志保存在当前目录。

第一次 server 全量测试在沙箱内无法 bind 本地 mock provider 端口；经授权在沙箱外重跑。中间发现同步 test_state 构造器不能无条件启动 Tokio worker，已修正；最终完整测试包含该回归测试并通过。这些中间失败不计作最终通过。

## 用户批准后的最终复验

在当前未提交工作区重新执行全部最终门禁，所有命令退出码均为 0。本次无失败，无需修改实现或测试代码；仅补充验收证据，未 commit/push，未进入 P3/P4。

- 四包测试：261 passed，0 failed，0 ignored；包含全部 13 项 P2 测试及四组 P0 golden fixture 测试。
- `scripts/verify.sh`：102 passed，0 failed，0 ignored；16 个 SSE frame 校验 PASS。
- `cargo fmt --all -- --check`、workspace clippy `-D warnings`、`git diff --check` 全部 PASS。
- 重新搜索确认 Gateway 不直接调用 Context/Skills 编排、Context 算子不调用 ModelProvider/ToolPort、不发布 model/tool 请求、不启用 operator_v1；core harness、P0 fixtures、provider/tool 和 multi bus 无修改。

本次完整日志：`/tmp/mina-p2-final-full-tests.log`、`/tmp/mina-p2-final-verify.log`、`/tmp/mina-p2-final-clippy.log`、`/tmp/mina-p2-final-fmt.log`、`/tmp/mina-p2-final-diff-check.log`。范围搜索保存在 `06-final-scope.log`。

## 必须覆盖项

| 验收项 | 可执行证据 |
| --- | --- |
| 请求 → Context → legacy 顺序，双入口 | `p2_http_disconnect_after_publish_still_starts_legacy`；`p2_stateless_and_session_reopen_finish_context_then_start_legacy_once` |
| Context input / fingerprint / SkillLock 等价 | `p2_context_matches_legacy_input_fingerprint_skill_lock_and_memory` |
| Context / Skill 400、422；SSE header 前失败 | `p2_preparation_errors_precede_sse_headers_with_legacy_status`；P0 errors fixture |
| waiter cancel / HTTP disconnect / timeout | `p2_durable_request_survives_waiter_cancel_and_sqlite_reopen`；`p2_http_disconnect_after_publish_still_starts_legacy`；`p2_timeout_does_not_remove_durable_request` |
| SQLite 真正关闭/reopen | harness 的三个恢复切点测试；server 双入口两次 reopen 测试；runtime Flow receipt reopen 测试 |
| receipt / duplicate delivery / legacy start 幂等 | `p2_receipt_outbox_reopen_and_duplicate_delivery_commit_once`；`p2_context_bridge_concurrent_starts_and_reopen_reuse_the_flow_receipt` |
| Session 并发/key/hash/revision/busy | `p2_session_concurrency_during_context_preserves_key_hash_revision_and_busy`；P0 Session fixture |
| P0 HTTP/SSE fixtures | `p0_runs_stream_nonstream_replay_follow_fixture`、`p0_session_submit_idempotency_busy_and_cancel_fixture`、`p0_all_sse_names_payloads_and_error_frame_fixture`、`p0_error_and_approval_fixtures`，全部通过 |
| Gateway 不调用 Context/Skills 编排 | `p2_gateway_has_no_context_business_or_new_model_tool_execution`；scope 搜索 |
| 没有新增 Model/Tool 执行点 | scope diff + 同一静态边界测试；执行仍由原 native machine 驱动 |
| 超大输入/错误用 ref | `p2_large_input_is_referenced_and_engine_cannot_switch`；`p2_failure_is_a_committed_fact_not_a_retry`（130 KB 错误） |

## RED → GREEN 原始证据

- `01-contract-red.log`：两个行为测试在 stub publish 处失败；不是把编译失败当成功的 RED。
- `02-disconnect-red.log`：已提交 Context 结果，但 HTTP task abort 后没有 legacy start。
- `03-large-error-red.log`：超长错误导致 commit 数 0，违反“领域失败必须提交”的断言。
- `04-targeted-green.log`：上述场景以及全部 13 项 P2 定向测试最终通过。

边界、临时桥移除期限、跨 store 窗口与未覆盖的部署/性能保证见 `../p2-request-context.md`。没有 P3/P4 实现或真实 provider shadow/double execution。
