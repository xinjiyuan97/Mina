# Multi-agent completion audit

The multi-agent runtime is not yet complete. Passing existing tests is not
evidence of atomic recovery or a fully automatic supervisor state machine.

## Verified consumption fixes

`durable_supervisor_consumption` exercises rejected and duplicate result events,
dependency publication failure injected with a SQLite trigger, and corrupt result
payloads. Rejected events advance only the current consumer's offset. Publication
failures leave the input unacknowledged and replay retries dependency release.
Storage errors propagate instead of being interpreted as an empty queue or timeout.

## Remaining completion requirements

- Persist supervisor state, dispatched dependencies and input acknowledgement in
  one transaction; prove crash and concurrent-consumer recovery.
- Persist the discussion decision, next-round checkpoint and input acknowledgement
  atomically. The current explicit checkpoint write is a separate operation.
- Make checkpoint recovery independent of consumer offsets, validate envelope and
  run identity, and test conflicting or malformed checkpoints.
- Implement independent retry deadlines, immediate exhaustion handling, external
  cancellation and worker cancellation propagation. Current retry helpers do not
  prove these properties.
- Exercise a real Harness worker through supervisor retry, cancellation and restart.
- Validate worker selection failures and registration; never fabricate an empty
  worker when selection fails.
- Validate schema versions, reject unsupported versions, and perform transactional
  migrations. Creating `schema_meta` alone is not a migration protocol.
- Synchronize the runtime guide and run relevant fault, concurrency, integration,
  formatting, lint and workspace checks to completion with captured exit status.

Stable Harness run IDs and idempotent message publication do not by themselves
deduplicate tool effects. Recovery must explicitly account for this boundary.
