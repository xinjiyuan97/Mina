BEGIN TRANSACTION;
PRAGMA user_version = 5;
CREATE TABLE context_artifacts (
                    artifact_id TEXT PRIMARY KEY NOT NULL,
                    source_digest TEXT NOT NULL,
                    content_digest TEXT NOT NULL,
                    policy_version INTEGER NOT NULL,
                    kind TEXT NOT NULL,
                    created_at_ms INTEGER NOT NULL,
                    invalidated INTEGER NOT NULL,
                    artifact_json TEXT NOT NULL,
                    UNIQUE(source_digest, content_digest, policy_version)
                 );
CREATE TABLE operator_runs_v1 (run_id TEXT PRIMARY KEY, generation INTEGER NOT NULL, record_json TEXT NOT NULL);
INSERT INTO "operator_runs_v1" VALUES('00000000-0000-4000-8000-000000000501',1,'{"batches":[],"coverage_digest":"sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","durable_through":0,"facts":[{"attempt":1,"causation_id":"b2eb6023-57e6-5679-bf05-132959dccb47","correlation_id":"00000000-0000-4000-8000-000000000501","delivery_class":"durable","effect_id":"00000000-0000-4000-8000-000000000501:model:1","engine_version":"operator_v1","event_id":"1d61b410-2f01-569e-bf7b-2f9ae73a15ab","event_type":"run.started","invocation_id":"00000000-0000-4000-8000-000000000501:model:1:attempt:1","issued_revision":1,"lease_epoch":1,"occurred_at_ms":1792832565051,"operator":{"name":"coordinator","version":1},"operator_seq":1,"payload":{},"projection_version":1,"run_id":"00000000-0000-4000-8000-000000000501","schema_version":1,"session_id":null,"tenant_id":"local"},{"attempt":1,"causation_id":"1d61b410-2f01-569e-bf7b-2f9ae73a15ab","correlation_id":"00000000-0000-4000-8000-000000000501","delivery_class":"durable","effect_id":"00000000-0000-4000-8000-000000000501:model:1","engine_version":"operator_v1","event_id":"6d68fc3b-e2cb-5f64-9bf2-63a3359adae9","event_type":"model.requested","invocation_id":"00000000-0000-4000-8000-000000000501:model:1:attempt:1","issued_revision":1,"lease_epoch":1,"occurred_at_ms":1792832565051,"operator":{"name":"coordinator","version":1},"operator_seq":2,"payload":{"request_ref":"a0af8a73-e4b3-4e55-9e20-d4ffc6d1ac1e"},"projection_version":1,"run_id":"00000000-0000-4000-8000-000000000501","schema_version":1,"session_id":null,"tenant_id":"local"}],"final_ref":null,"generation":1,"lease_expires_at_ms":0,"lease_owner":null,"ledger":{"context_completed":true,"error":null,"inbox":[{"type":"context_completed"}],"model_done":false,"model_started":false,"output_done":false,"phase":"awaiting_model","revision":1,"run_id":"00000000-0000-4000-8000-000000000501","token":{"attempt":1,"effect_id":"00000000-0000-4000-8000-000000000501:model:1","invocation_id":"00000000-0000-4000-8000-000000000501:model:1:attempt:1","issued_revision":1,"lease_epoch":1}},"model_request_ref":"a0af8a73-e4b3-4e55-9e20-d4ffc6d1ac1e","operator_sequence_base":0,"public_events":[{"run_id":"00000000-0000-4000-8000-000000000501","seq":1,"type":"run_started"}],"reducer":{"completed_usage":{"input_tokens":0,"output_tokens":0,"source":"provider_reported","total_tokens":0},"config":{"allow_run_adf":false,"allowed_tools":[],"approval_policy":50,"max_output_tokens":77,"max_steps":1,"model":"p5-pinned-model","model_timeout_ms":90000,"system_prompt":"","tool_call_strategy":"sequential","tool_timeout_ms":1},"messages":[{"content":"P5 fixture input","role":"user"}],"next_effect_sequence":2,"next_step":0,"phase":{"AwaitingModel":{"effect":{"effect_id":"00000000-0000-4000-8000-000000000501:model:1","purpose":"agent_step","request":{"max_output_tokens":77,"messages":[{"content":"P5 fixture input","role":"user"}],"model":"p5-pinned-model","run_id":"00000000-0000-4000-8000-000000000501","tools":[]},"timeout_ms":90000,"type":"invoke_model"},"tool_set":{"bindings":[],"definitions":[],"digest":"sha256:643d5437104296e21d906ecb15b2c96ad278f20cfc4af53b12bb6069bd853726","revision":1},"turn":{"assistant_reasoning":"","assistant_text":"","pending_calls":[],"purpose":"agent_step","seen_call_ids":[]}}},"previous_tool_set":{"digest":"sha256:643d5437104296e21d906ecb15b2c96ad278f20cfc4af53b12bb6069bd853726","revision":1},"protocol_version":1,"run_id":"00000000-0000-4000-8000-000000000501"},"schema_version":1,"sealed":false,"session_id":null,"start_digest":"sha256:41bbcb7a44b1cc0452751c09243238d813b7b49e1fcbe23ae5363b6717ec029a"}');
INSERT INTO "operator_runs_v1" VALUES('00000000-0000-4000-8000-000000000502',1,'{"batches":[],"coverage_digest":"sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","durable_through":0,"facts":[{"attempt":1,"causation_id":"bea718e5-41e6-51ba-8a74-cae7a2a79c33","correlation_id":"00000000-0000-4000-8000-000000000502","delivery_class":"durable","effect_id":"00000000-0000-4000-8000-000000000502:model:1","engine_version":"operator_v1","event_id":"9aa36250-34bd-56d7-907e-5c47995471ff","event_type":"run.started","invocation_id":"00000000-0000-4000-8000-000000000502:model:1:attempt:1","issued_revision":1,"lease_epoch":1,"occurred_at_ms":1792832565069,"operator":{"name":"coordinator","version":1},"operator_seq":1,"payload":{},"projection_version":1,"run_id":"00000000-0000-4000-8000-000000000502","schema_version":1,"session_id":null,"tenant_id":"local"},{"attempt":1,"causation_id":"9aa36250-34bd-56d7-907e-5c47995471ff","correlation_id":"00000000-0000-4000-8000-000000000502","delivery_class":"durable","effect_id":"00000000-0000-4000-8000-000000000502:model:1","engine_version":"operator_v1","event_id":"891b11db-80af-54fd-9e3d-0302a7925573","event_type":"model.requested","invocation_id":"00000000-0000-4000-8000-000000000502:model:1:attempt:1","issued_revision":1,"lease_epoch":1,"occurred_at_ms":1792832565069,"operator":{"name":"coordinator","version":1},"operator_seq":2,"payload":{"request_ref":"448cf9a3-21bc-4cdf-ba3b-bf01fc9182d7"},"projection_version":1,"run_id":"00000000-0000-4000-8000-000000000502","schema_version":1,"session_id":null,"tenant_id":"local"}],"final_ref":null,"generation":1,"lease_expires_at_ms":0,"lease_owner":null,"ledger":{"context_completed":true,"error":null,"inbox":[{"type":"context_completed"}],"model_done":false,"model_started":false,"output_done":false,"phase":"awaiting_model","revision":1,"run_id":"00000000-0000-4000-8000-000000000502","token":{"attempt":1,"effect_id":"00000000-0000-4000-8000-000000000502:model:1","invocation_id":"00000000-0000-4000-8000-000000000502:model:1:attempt:1","issued_revision":1,"lease_epoch":1}},"model_request_ref":"448cf9a3-21bc-4cdf-ba3b-bf01fc9182d7","operator_sequence_base":0,"public_events":[{"run_id":"00000000-0000-4000-8000-000000000502","seq":1,"type":"run_started"}],"reducer":{"completed_usage":{"input_tokens":0,"output_tokens":0,"source":"provider_reported","total_tokens":0},"config":{"allow_run_adf":false,"allowed_tools":[],"approval_policy":50,"max_output_tokens":77,"max_steps":1,"model":"p5-pinned-model","model_timeout_ms":90000,"system_prompt":"","tool_call_strategy":"sequential","tool_timeout_ms":1},"messages":[{"attachments":[{"blob_id":"f99ab0a3-df9e-4098-b319-5d7ae602d3e3","media_type":"image/png","name":"p5.png"}],"content":"P5 fixture input","role":"user"}],"next_effect_sequence":2,"next_step":0,"phase":{"AwaitingModel":{"effect":{"effect_id":"00000000-0000-4000-8000-000000000502:model:1","purpose":"agent_step","request":{"max_output_tokens":77,"messages":[{"attachments":[{"blob_id":"f99ab0a3-df9e-4098-b319-5d7ae602d3e3","media_type":"image/png","name":"p5.png"}],"content":"P5 fixture input","role":"user"}],"model":"p5-pinned-model","run_id":"00000000-0000-4000-8000-000000000502","tools":[]},"timeout_ms":90000,"type":"invoke_model"},"tool_set":{"bindings":[],"definitions":[],"digest":"sha256:643d5437104296e21d906ecb15b2c96ad278f20cfc4af53b12bb6069bd853726","revision":1},"turn":{"assistant_reasoning":"","assistant_text":"","pending_calls":[],"purpose":"agent_step","seen_call_ids":[]}}},"previous_tool_set":{"digest":"sha256:643d5437104296e21d906ecb15b2c96ad278f20cfc4af53b12bb6069bd853726","revision":1},"protocol_version":1,"run_id":"00000000-0000-4000-8000-000000000502"},"schema_version":1,"sealed":false,"session_id":null,"start_digest":"sha256:e2b98b32cdd68a9843a39afd188502ded125d587baff4f20d326c9dc60b89a0f"}');
CREATE TABLE run_events (
                    run_id TEXT NOT NULL,
                    seq INTEGER NOT NULL,
                    observed_at_ms INTEGER NOT NULL,
                    event_json TEXT NOT NULL,
                    PRIMARY KEY (run_id, seq),
                    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
                 );
INSERT INTO "run_events" VALUES('00000000-0000-4000-8000-000000000501',1,1792832565062,'{"run_id":"00000000-0000-4000-8000-000000000501","seq":1,"type":"run_started"}');
INSERT INTO "run_events" VALUES('00000000-0000-4000-8000-000000000502',1,1792832565076,'{"run_id":"00000000-0000-4000-8000-000000000502","seq":1,"type":"run_started"}');
CREATE TABLE runs (
                    run_id TEXT PRIMARY KEY NOT NULL,
                    status TEXT NOT NULL,
                    last_seq INTEGER NOT NULL,
                    created_at_ms INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL,
                    snapshot_json TEXT NOT NULL
                 );
INSERT INTO "runs" VALUES('00000000-0000-4000-8000-000000000501','running',1,1792832565048,1792832565062,'{"schema_version":1,"run_id":"00000000-0000-4000-8000-000000000501","status":"running","input":"P5 fixture input","execution_manifest":{"engine_version":"operator_v1","projection_version":1},"output":"","reasoning":"","approvals":[],"tools":[],"last_seq":1,"revision":1,"created_at_ms":1792832565048,"updated_at_ms":1792832565062}');
INSERT INTO "runs" VALUES('00000000-0000-4000-8000-000000000502','running',1,1792832565066,1792832565076,'{"schema_version":1,"run_id":"00000000-0000-4000-8000-000000000502","status":"running","input":"P5 fixture input","execution_manifest":{"engine_version":"operator_v1","projection_version":1},"output":"","reasoning":"","approvals":[],"tools":[],"last_seq":1,"revision":1,"created_at_ms":1792832565066,"updated_at_ms":1792832565076}');
CREATE TABLE session_messages (
                    session_id TEXT NOT NULL,
                    ordinal INTEGER NOT NULL,
                    message_id TEXT NOT NULL UNIQUE,
                    role TEXT NOT NULL,
                    source_run_id TEXT,
                    created_at_ms INTEGER NOT NULL,
                    message_json TEXT NOT NULL,
                    PRIMARY KEY (session_id, ordinal),
                    FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE,
                    FOREIGN KEY (source_run_id) REFERENCES runs(run_id)
                 );
CREATE TABLE session_runs (
                    session_id TEXT NOT NULL,
                    run_id TEXT NOT NULL UNIQUE,
                    idempotency_key TEXT NOT NULL,
                    request_hash TEXT NOT NULL,
                    context_through_ordinal INTEGER NOT NULL,
                    finalized_at_ms INTEGER,
                    PRIMARY KEY (session_id, idempotency_key),
                    FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE,
                    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
                 );
CREATE TABLE sessions (
                    session_id TEXT PRIMARY KEY NOT NULL,
                    status TEXT NOT NULL,
                    revision INTEGER NOT NULL,
                    next_message_ordinal INTEGER NOT NULL,
                    active_run_id TEXT,
                    created_at_ms INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL,
                    snapshot_json TEXT NOT NULL,
                    FOREIGN KEY (active_run_id) REFERENCES runs(run_id)
                 );
CREATE INDEX runs_status_created_idx ON runs(status, created_at_ms);
CREATE INDEX session_messages_created_idx
                    ON session_messages(session_id, ordinal);
CREATE INDEX context_artifacts_reuse_idx
                    ON context_artifacts(source_digest, policy_version, invalidated);
COMMIT;
