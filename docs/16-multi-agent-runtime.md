# Multi-agent runtime

`agent-multi` sits above `agent-harness`. Harness owns one agent execution;
multi owns coordination and transport.

The outer `run_id` identifies the collaboration. Harness execution IDs derive
from the outer run, request ID and worker ID. Replaying the same request for the
same worker preserves its execution ID; distinct workers and retry attempts
get distinct IDs so their checkpoints and effects cannot collide. A stable ID
alone does not deduplicate execution; runtime inbox handling is still required.

The common message envelope always carries `id`, `run_id`, and optional
`correlation_id`. Patterns publish through either `InMemoryBus` or the durable
SQLite bus; durable node execution supports replay and explicit acknowledgement.

`InMemoryBus` is a bounded broadcast transport for local execution and tests.
Capacity and lag are isolated by exact topic. Publish counts only matching
subscribers and fails when none exist. Late subscriptions receive future
messages only; after lag, receiving resumes at the oldest retained message.
Dropping the last bus handle allows subscribers to drain queued messages and
then observe closure. Concurrent publishers share an order within each topic.
Slow consumers receive an explicit lag error. `SqliteBus` is an append-only
durable log with per-consumer, per-topic offsets, explicit acknowledgements, replay after
restart, and idempotent publication by message ID. Consumers must therefore be
idempotent because messages remain available until acknowledged.

Supervisor coordinates worker tasks and tracks terminal status. Discussion
provides round-scoped topics and a session helper for participant registration,
turn publication, and bounded rounds. Both patterns use the same message
envelope and can be replaced or composed by applications.

SQLite acknowledgements must follow each topic's sequence order. Acknowledging
an unknown sequence or skipping an unacknowledged message returns an error.
An old acknowledgement is harmless. Reusing a message ID with different content
is rejected. Failed acknowledgement transactions leave messages available for replay.

The initial prototype used global consumer offsets. Those offsets cannot safely
be assigned to topics, so the new `topic_offsets` table starts from zero when
opening a prototype database. Existing messages are retained and replayed;
applications must tolerate duplicates during this upgrade.

The SQLite API is synchronous internally and does not yet provide a pooled
database connection. Use `SqliteBusHandle` when sharing one connection across threads; it serializes
publish, replay and ack operations. Discussion offers manual and bounded
collected rounds, durable decision commits, and checkpoint persistence/recovery.
Supervisor offers bounded durable result consumption, retry backoff, prerequisite
release and timeout cancellation. The application still owns worker execution
and must treat delivery as at least once.


Supervisor records retain a stable task ID, the current attempt request, assigned
worker, attempt count and terminal result. `retry(task_id, max_attempts)` creates
a new request ID and only updates the record after publication succeeds. The
first valid terminal event wins; events from other runs, workers, topics and old
attempts are ignored. `dispatch_batch` returns dispatched IDs even in its error
so callers can manage partially published work. Subscribe to results before
dispatch. `wait_for_tasks` returns results in input order and caches other task
results encountered while waiting. Cancelling or timing out a wait does not
cancel workers; worker cancellation orchestration is a separate pending step.
`checkpoint()` and `restore_checkpoint()` persist and restore these records,
including a failed task's current attempt so bounded retry can continue after a
restart.

Discussion checkpoints use `DiscussionCheckpoint`; `checkpoint_json()` and
`restore_json()` provide a validated JSON boundary for durable storage. The
application decides when to publish and acknowledge checkpoint messages; the
session helper does not silently persist state.
