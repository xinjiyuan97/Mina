# agent-multi

Multi-agent coordination layer above `agent-harness`.

The crate separates transport (`EventBus`), topology (`Topic`, `Message`,
`AgentNode`) and patterns (`SupervisorPattern`, `DiscussionPattern`). The
in-memory bus is intended for tests and local runs; durable implementations can
implement the same `EventBus` contract.

`EventBus` exposes associated subscription and error types. `SqliteBusHandle`
implements the contract for ephemeral subscriptions; use its named
`subscribe(consumer, topic)` API when replay and explicit acknowledgement are
required.

`SqliteSubscription::recv` performs an immediate replay check, while
`recv_wait` polls asynchronously and `recv_wait_timeout` adds a deadline. None
of these methods acknowledges automatically; callers must process the message
and call `ack` after successful handling.

Messages are at-least-once friendly: every message has a UUID and replies carry
`correlation_id`. Consumers must be idempotent. Standard lifecycle kinds live in
`event_kind`.

`Supervisor::dispatch_registered` enforces worker registration; `dispatch` is
the lower-level free-topology entry point. Task records support bounded retry,
exponential `RetryPolicy`, explicit cancellation requests, and ordered waits
with timeout/cancellation. Terminal events are accepted only from the assigned
worker and current attempt.

`DiscussionSession` provides deterministic round publication, moderator
reduction, complete-round validation, and JSON checkpoints. Every registered
participant must contribute exactly one turn before reduction.

`HarnessAgentNode` implements `AgentNode` directly. Use `serve_node` for a
long-lived consumer, or `serve_node_with_cancel` when the host owns a
`CancellationToken`; cancellation reaches the active Harness execution and
stops the consumer loop.

## Durable bus

`SqliteBus` provides append-only persistence for deployments that need replay
across process restarts. Use `publish_idempotent` when retrying a request with
the same message ID, call `read_after` with a stable consumer name, and `ack`
after successful handling. Offset updates are monotonic. Consumers should be
idempotent because acknowledgement is explicit and delivery is at least once.

Participant failures are handled explicitly with `DiscussionFailurePolicy`:
`Abort` finishes the session, while `Continue` removes the failed participant
and continues with the remaining set (ending when none remain). The resulting
participant set is included in checkpoints.

### Runtime and recovery

`SqliteBus` exposes schema version `1` and rejects unsupported future versions.
Supervisor and Discussion checkpoints use content-based stable IDs, validate run
identity on restore, and support replay after restart. Durable Supervisor helpers
provide bounded result processing, timeout cancellation, and retry backoff.
