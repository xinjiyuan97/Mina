use super::*;

#[path = "p6_second_review_tests.rs"]
mod second_review;
use agent_harness::context_operator::prepared_plan;

fn config(model: &str, max_output: u32) -> HarnessConfig {
    HarnessConfig::from_toml_str(&format!(
        r#"
default_model = "primary"
[agent]
kind = "agent-loop"
max_steps = 8
[script.quickjs]
enabled = false
[adf]
enabled = false
[jobs]
enabled = false
[models.primary]
model = "{model}"
max_output_tokens = {max_output}
[models.primary.modalities]
input = ["text", "image", "document"]
[models.primary.provider]
type = "openai-compatible"
base_url = "http://127.0.0.1:1/v1"
api_key = "test-key"
"#
    ))
    .expect("config")
}

async fn text_state(
    path: &std::path::Path,
    sender: tokio::sync::mpsc::UnboundedSender<ModelRequest>,
) -> (AppState, tokio::task::JoinHandle<()>) {
    let (mut state, worker) = fixture(
        path,
        Arc::new(Capture(sender.clone())),
        Arc::new(Tools {
            suspend: false,
            calls: AtomicUsize::new(0),
        }),
    )
    .await;
    state.config = Arc::new(config("original-model", 77));
    state.pipeline_profile = crate::tools_pipeline::EngineProfile::Text;
    state.pipeline = Some(Arc::new(crate::tools_pipeline::ServerAgentRuntime::new(
        agent_core::agent::AgentResources::new(state.admission.fabric())
            .model("primary", "server-v1", Arc::new(Capture(sender)))
            .toolset("workspace", "server-v1", Arc::new(ToolRegistry::new())),
        state.runtime.run_store(),
    )));
    (state, worker)
}

async fn pending(
    state: &AppState,
    run_id: RunId,
    input: &str,
    attachments: Vec<ModelAttachment>,
    inconsistent: bool,
) {
    let prepared = prepare_run_plan(state, prepare_request(run_id, input, attachments))
        .await
        .expect("context");
    let mut plan = prepared.plan;
    if inconsistent {
        plan.input = "inconsistent persisted input".into();
    }
    let mut config = agent_core::agent::GraphAgentConfig::new(
        &state.config.default_model().model,
        &state.config.agent().system_prompt,
    );
    config.max_output_tokens = state.config.default_model().max_output_tokens;
    state
        .pipeline
        .as_ref()
        .expect("pipeline")
        .engine
        .admit(&config, plan, None)
        .await
        .expect("pending graph");
}

async fn config_drift(model: &str, max_output: u32) {
    let directory = tempfile::tempdir().expect("temp");
    let (sender, mut received) = tokio::sync::mpsc::unbounded_channel();
    let run_id = RunId::new();
    let expected;
    {
        let (state, worker) = text_state(directory.path(), sender.clone()).await;
        expected = vec![
            attachment(&state, "image/png", "first.png").await,
            attachment(&state, "application/pdf", "second.pdf").await,
        ];
        pending(&state, run_id, "describe", expected.clone(), false).await;
        worker.abort();
        let _ = worker.await;
    }
    let (mut state, worker) = text_state(directory.path(), sender).await;
    state.config = Arc::new(config(model, max_output));
    admission::recover_once(&state)
        .await
        .expect("configuration drift must not block recovery");
    let request = tokio::time::timeout(Duration::from_secs(5), received.recv())
        .await
        .expect("model deadline")
        .expect("request");
    assert_eq!(request.model, "original-model");
    assert_eq!(request.max_output_tokens, Some(77));
    assert_eq!(request.messages.last().expect("user").attachments, expected);
    for field in 0..7 {
        let original = state
            .admission
            .request(run_id)
            .await
            .expect("read")
            .expect("request");
        let prepared = state
            .admission
            .wait(run_id, Duration::from_secs(1))
            .await
            .expect("prepared");
        let mut plan = prepared_plan(original, prepared);
        match field {
            0 => plan.input.push_str("changed"),
            1 => plan.attachments.reverse(),
            2 => plan.attachments[0].blob_id = BlobId::new(),
            3 => plan.attachments[0].media_type = "image/jpeg".into(),
            4 => plan.attachments[0].name = Some("changed.png".into()),
            5 => plan
                .prior_messages
                .push(ModelMessage::user("different history")),
            _ => plan.context_fingerprint = Some("different fingerprint".into()),
        }
        assert!(
            pipeline::start(&state, plan, false).await.is_err(),
            "identity field {field}"
        );
    }
    worker.abort();
    let _ = worker.await;
}

#[tokio::test]
async fn p6_review_text_recovery_pins_model() {
    config_drift("changed-model", 77).await;
}

#[tokio::test]
async fn p6_review_text_recovery_pins_output_limit() {
    config_drift("original-model", 99).await;
}

#[tokio::test]
async fn p6_review_bad_run_does_not_starve_recovery_scan() {
    let directory = tempfile::tempdir().expect("temp");
    let (sender, mut received) = tokio::sync::mpsc::unbounded_channel();
    let (state, worker) = text_state(directory.path(), sender).await;
    let bad = RunId::new();
    let good = RunId::new();
    pending(&state, bad, "bad", vec![], true).await;
    pending(&state, good, "good", vec![], false).await;
    assert!(
        admission::recover_once(&state).await.is_err(),
        "scan still reports quarantined failures"
    );
    let request = tokio::time::timeout(Duration::from_secs(2), received.recv())
        .await
        .expect("healthy run must still recover")
        .expect("request");
    assert_eq!(request.run_id, good);
    assert_eq!(
        state
            .pipeline
            .as_ref()
            .unwrap()
            .engine
            .host(bad)
            .await
            .unwrap()
            .engine
            .get_run("local", bad)
            .await
            .unwrap()
            .deliveries,
        0
    );
    worker.abort();
    let _ = worker.await;
}

async fn session_fixture(
    path: &std::path::Path,
) -> (
    AppState,
    tokio::task::JoinHandle<()>,
    SessionSnapshot,
    tokio::sync::mpsc::UnboundedReceiver<ModelRequest>,
) {
    let (sender, received) = tokio::sync::mpsc::unbounded_channel();
    let (mut state, worker) = text_state(path, sender).await;
    let sessions = Arc::new(
        SqliteRunStore::open(path.join("runs.db"))
            .await
            .expect("sessions"),
    );
    Arc::get_mut(&mut state.orchestration)
        .expect("unique")
        .sessions = sessions.clone();
    let session = sessions
        .create_session(CreateSession {
            session_id: SessionId::new(),
            agent_profile: "test".into(),
            title: None,
            created_at_ms: 0,
        })
        .await
        .expect("session");
    (state, worker, session, received)
}

async fn response(state: &AppState, session_id: SessionId, body: Value) -> (StatusCode, Value) {
    let response = submit_session_run_owned(
        state.clone(),
        session_id.to_string(),
        serde_json::from_value(body).expect("request"),
    )
    .await;
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 8192)
        .await
        .expect("body");
    (status, serde_json::from_slice(&body).expect("json"))
}

async fn removed_name(null: bool) {
    let directory = tempfile::tempdir().expect("temp");
    let (mut state, worker, session, _received) = session_fixture(directory.path()).await;
    let blob = state
        .blobs
        .put(PutBlob {
            media_type: "image/png".into(),
            name: None,
            data: vec![1, 2, 3],
            created_at_ms: 0,
        })
        .await
        .expect("unnamed blob");
    let original = json!({"input":"", "attachments":[{"blob_id":blob.blob_id,"name":"alias.png"}], "expected_revision":session.revision,"idempotency_key":"name"});
    let accepted = response(&state, session.session_id, original.clone()).await;
    assert_eq!(accepted.0, StatusCode::ACCEPTED);
    state.blobs = admission::test_blobs();
    let replay = response(&state, session.session_id, original.clone()).await;
    assert_eq!(replay.0, StatusCode::ACCEPTED, "identical offline replay");
    assert_eq!(replay.1["run_id"], accepted.1["run_id"]);
    let mut removed = original;
    if null {
        removed["attachments"][0]["name"] = Value::Null;
    } else {
        removed["attachments"][0]
            .as_object_mut()
            .expect("object")
            .remove("name");
    }
    assert_eq!(
        response(&state, session.session_id, removed).await.0,
        StatusCode::CONFLICT,
        "name removal is not a wildcard"
    );
    worker.abort();
    let _ = worker.await;
}

#[tokio::test]
async fn p6_review_name_omission_conflicts() {
    removed_name(false).await;
}
#[tokio::test]
async fn p6_review_name_null_conflicts() {
    removed_name(true).await;
}

async fn old_size(size: u64, offline: bool) {
    let directory = tempfile::tempdir().expect("temp");
    let (mut state, worker, session, _received) = session_fixture(directory.path()).await;
    let template = RunId::new();
    prepare_run_plan(&state, prepare_request(template, "template", vec![]))
        .await
        .expect("template");
    let mut saved = state
        .admission
        .request(template)
        .await
        .expect("read")
        .expect("request");
    let blob = state
        .blobs
        .put(PutBlob {
            media_type: "image/png".into(),
            name: None,
            data: vec![1, 2, 3],
            created_at_ms: 0,
        })
        .await
        .expect("blob");
    saved.run_id = RunId::new();
    saved.session_id = Some(session.session_id);
    saved.input = String::new();
    saved.max_steps = 8;
    saved.attachments = vec![ModelAttachment {
        blob_id: blob.blob_id,
        media_type: blob.media_type.clone(),
        name: None,
        size_bytes: None,
    }];
    saved.request_hash = "old-hash".into();
    state
        .orchestration
        .sessions
        .begin_run(
            BeginSessionRun {
                session_id: session.session_id,
                run_id: saved.run_id,
                expected_revision: session.revision,
                idempotency_key: "old".into(),
                request_hash: saved.request_hash.clone(),
                input: vec![ContentPart::BlobRef {
                    blob_id: blob.blob_id,
                    media_type: blob.media_type,
                    name: None,
                    size_bytes: Some(3),
                }],
                created_at_ms: 0,
            },
            RunSnapshot::new(saved.run_id, "", 0),
        )
        .await
        .expect("historical session index");
    state
        .admission
        .publish(saved.clone())
        .await
        .expect("historical request without size");
    if offline {
        state.blobs = admission::test_blobs();
    }
    let request = json!({"input":"", "attachments":[{"blob_id":blob.blob_id, "size_bytes":size}],"expected_revision":session.revision,"idempotency_key":"old"});
    let result = response(&state, session.session_id, request).await;
    assert_eq!(
        result.0,
        if size == 3 && !offline {
            StatusCode::ACCEPTED
        } else {
            StatusCode::CONFLICT
        }
    );
    if result.0 == StatusCode::ACCEPTED {
        assert_eq!(result.1["run_id"], json!(saved.run_id));
    }
    worker.abort();
    let _ = worker.await;
}

#[tokio::test]
async fn p6_review_old_size_correct_replay() {
    old_size(3, false).await;
}
#[tokio::test]
async fn p6_review_old_size_wrong_conflicts() {
    old_size(999, false).await;
}
#[tokio::test]
async fn p6_review_old_size_unverifiable_conflicts() {
    old_size(3, true).await;
}

#[tokio::test]
async fn p6_review_omitted_and_null_names_are_distinct_offline() {
    let directory = tempfile::tempdir().expect("temp");
    let (mut state, worker, session, _received) = session_fixture(directory.path()).await;
    let blob = state
        .blobs
        .put(PutBlob {
            media_type: "image/png".into(),
            name: None,
            data: vec![1, 2, 3],
            created_at_ms: 0,
        })
        .await
        .expect("blob");
    let original = json!({"input":"", "attachments":[{"blob_id":blob.blob_id}], "expected_revision":session.revision,"idempotency_key":"omitted"});
    assert_eq!(
        response(&state, session.session_id, original.clone())
            .await
            .0,
        StatusCode::ACCEPTED
    );
    state.blobs = admission::test_blobs();
    assert_eq!(
        response(&state, session.session_id, original.clone())
            .await
            .0,
        StatusCode::ACCEPTED
    );
    let mut changed = original;
    changed["attachments"][0]["name"] = Value::Null;
    assert_eq!(
        response(&state, session.session_id, changed).await.0,
        StatusCode::CONFLICT
    );
    worker.abort();
    let _ = worker.await;
}

#[tokio::test]
async fn p6_review_current_size_change_conflicts_offline() {
    let directory = tempfile::tempdir().expect("temp");
    let (mut state, worker, session, _received) = session_fixture(directory.path()).await;
    let blob = attachment(&state, "image/png", "current.png").await;
    let original = json!({"input":"", "attachments":[blob], "expected_revision":session.revision,"idempotency_key":"current"});
    assert_eq!(
        response(&state, session.session_id, original.clone())
            .await
            .0,
        StatusCode::ACCEPTED
    );
    state.blobs = admission::test_blobs();
    assert_eq!(
        response(&state, session.session_id, original.clone())
            .await
            .0,
        StatusCode::ACCEPTED
    );
    let mut changed = original;
    changed["attachments"][0]["size_bytes"] = json!(0);
    assert_eq!(
        response(&state, session.session_id, changed).await.0,
        StatusCode::CONFLICT
    );
    worker.abort();
    let _ = worker.await;
}

#[tokio::test]
async fn p6_review_name_presence_is_part_of_persisted_request_hash() {
    let directory = tempfile::tempdir().expect("temp");
    let (state, worker, session, _received) = session_fixture(directory.path()).await;
    let second = state
        .orchestration
        .sessions
        .create_session(CreateSession {
            session_id: SessionId::new(),
            agent_profile: "test".into(),
            title: None,
            created_at_ms: 0,
        })
        .await
        .expect("second session");
    let blob = state
        .blobs
        .put(PutBlob {
            media_type: "image/png".into(),
            name: None,
            data: vec![1, 2, 3],
            created_at_ms: 0,
        })
        .await
        .expect("blob");
    let mut hashes = vec![];
    for (session, explicit_null) in [(session, false), (second, true)] {
        let mut request = json!({"input":"", "attachments":[{"blob_id":blob.blob_id}], "expected_revision":session.revision,"idempotency_key":"identity"});
        if explicit_null {
            request["attachments"][0]["name"] = Value::Null;
        }
        let (status, body) = response(&state, session.session_id, request).await;
        assert_eq!(status, StatusCode::ACCEPTED);
        let run_id = body["run_id"]
            .as_str()
            .expect("run id")
            .parse()
            .expect("uuid");
        hashes.push(
            state
                .admission
                .request(run_id)
                .await
                .expect("read")
                .expect("request")
                .request_hash,
        );
    }
    assert_ne!(
        hashes[0], hashes[1],
        "session reservation must distinguish omitted and null before Context publication"
    );
    worker.abort();
    let _ = worker.await;
}
