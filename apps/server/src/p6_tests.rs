use super::*;

#[path = "p6_review_tests.rs"]
mod review;

struct Capture(tokio::sync::mpsc::UnboundedSender<ModelRequest>);
impl ModelPort for Capture {
    fn stream(&self, request: ModelRequest) -> ModelEventStream {
        self.0.send(request).expect("capture");
        Box::pin(futures_util::stream::iter([ModelEvent::Completed {
            finish_reason: FinishReason::Stop,
        }]))
    }
}

fn multimodal_config() -> HarnessConfig {
    HarnessConfig::from_toml_str(
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
model = "test-model"
[models.primary.modalities]
input = ["text", "image", "audio", "video", "document"]
[models.primary.provider]
type = "openai-compatible"
base_url = "http://127.0.0.1:1/v1"
api_key = "test-key"
"#,
    )
    .expect("config")
}

async fn attachment(state: &AppState, mime: &str, name: &str) -> ModelAttachment {
    let blob = state
        .blobs
        .put(PutBlob {
            media_type: mime.into(),
            name: Some(name.into()),
            data: b"P6_PRIVATE_BINARY_SENTINEL".to_vec(),
            created_at_ms: 1,
        })
        .await
        .expect("blob");
    ModelAttachment {
        blob_id: blob.blob_id,
        media_type: mime.into(),
        name: blob.name,
        size_bytes: Some(blob.size_bytes),
    }
}

fn prepare_request(
    run_id: RunId,
    input: &str,
    attachments: Vec<ModelAttachment>,
) -> PrepareRunRequest {
    PrepareRunRequest {
        attachment_names: None,
        run_id,
        input: input.into(),
        attachments,
        explicit_skills: vec![],
        max_steps: 3,
        session_id: None,
        through_message_ordinal: None,
    }
}

#[test]
fn p6_request_hash_binds_order_mime_name_and_blob() {
    let first = ModelAttachment {
        blob_id: BlobId::new(),
        media_type: "image/png".into(),
        name: Some("first.png".into()),
        size_bytes: Some(3),
    };
    let second = ModelAttachment {
        blob_id: BlobId::new(),
        ..first.clone()
    };
    let original = vec![first, second];
    let hash = raw_request_hash("", &original, &[], 3);
    for field in 0..5 {
        let mut changed = original.clone();
        match field {
            0 => changed.reverse(),
            1 => changed[0].blob_id = BlobId::new(),
            2 => changed[0].media_type = "image/jpeg".into(),
            3 => changed[0].name = Some("changed.png".into()),
            _ => changed[0].size_bytes = Some(4),
        }
        assert_ne!(
            hash,
            raw_request_hash("", &changed, &[], 3),
            "identity field {field}"
        );
    }
}

#[tokio::test]
async fn p6_blob_errors_do_not_leak_backend_details() {
    struct Broken;
    impl BlobStore for Broken {
        fn put(&self, _: PutBlob) -> BlobStoreFuture<'_, BlobMetadata> {
            Box::pin(async { Err(BlobStoreError::backend("PRIVATE_BACKEND_PATH")) })
        }
        fn get(&self, _: BlobId) -> BlobStoreFuture<'_, Option<BlobObject>> {
            Box::pin(async { Err(BlobStoreError::backend("PRIVATE_BACKEND_PATH")) })
        }
    }
    let mut state = tests::test_state();
    state.blobs = Arc::new(Broken);
    let response = resolve_blob_inputs(
        &state,
        serde_json::from_value(json!([{"blob_id":BlobId::new()}])).expect("input"),
    )
    .await
    .err()
    .expect("error");
    let bytes = axum::body::to_bytes(response.into_body(), 8192)
        .await
        .expect("body");
    assert!(!String::from_utf8_lossy(&bytes).contains("PRIVATE_BACKEND_PATH"));
}

#[tokio::test]
async fn p6_operator_image_attachment_only_and_order_reach_model() {
    for input in ["describe", ""] {
        let directory = tempfile::tempdir().expect("temp");
        let (sender, mut received) = tokio::sync::mpsc::unbounded_channel();
        let (state, worker) = fixture(
            directory.path(),
            Arc::new(Capture(sender)),
            Arc::new(Tools {
                suspend: false,
                calls: AtomicUsize::new(0),
            }),
        )
        .await;
        let attachments = vec![
            attachment(&state, "image/png", "first.png").await,
            attachment(&state, "application/pdf", "second.pdf").await,
        ];
        let run_id = RunId::new();
        let prepared =
            prepare_run_plan(&state, prepare_request(run_id, input, attachments.clone()))
                .await
                .expect("operator must admit attachments");
        pipeline::start(&state, prepared.plan, false)
            .await
            .expect("start");
        let request = tokio::time::timeout(Duration::from_secs(10), received.recv())
            .await
            .expect("model deadline")
            .expect("model request");
        assert_eq!(
            request.messages.last().expect("user").attachments,
            attachments
        );
        let prepared =
            prepare_run_plan(&state, prepare_request(run_id, input, attachments.clone()))
                .await
                .expect("replay");
        pipeline::start(&state, prepared.plan, false)
            .await
            .expect("idempotent replay");
        let mut changed = prepare_run_plan(&state, prepare_request(run_id, input, attachments))
            .await
            .expect("replay")
            .plan;
        changed.attachments.reverse();
        assert!(
            pipeline::start(&state, changed, false).await.is_err(),
            "reopen must compare attachments even with same fingerprint"
        );
        state
            .tools_pipeline
            .as_ref()
            .expect("pipeline")
            .stop_workers()
            .await;
        worker.abort();
        let _ = worker.await;
        let bytes = std::fs::read(directory.path().join("events.db")).expect("database");
        assert!(
            !bytes
                .windows(b"P6_PRIVATE_BINARY_SENTINEL".len())
                .any(|part| part == b"P6_PRIVATE_BINARY_SENTINEL")
        );
    }
}

#[tokio::test]
async fn p6_text_profile_preserves_attachments() {
    let (sender, mut received) = tokio::sync::mpsc::unbounded_channel();
    let mut state = tests::test_state();
    state.pipeline_profile = crate::tools_pipeline::EngineProfile::Text;
    state.pipeline = Some(Arc::new(crate::tools_pipeline::ServerAgentRuntime::new(
        agent_core::agent::AgentResources::new(state.admission.fabric())
            .model("primary", "server-v1", Arc::new(Capture(sender)))
            .toolset("workspace", "server-v1", Arc::new(ToolRegistry::new())),
        state.runtime.run_store(),
    )));
    let attachments = vec![attachment(&state, "image/png", "image.png").await];
    let prepared = prepare_run_plan(
        &state,
        prepare_request(RunId::new(), "", attachments.clone()),
    )
    .await
    .expect("text profile admission");
    pipeline::start(&state, prepared.plan, false)
        .await
        .expect("start");
    let request = tokio::time::timeout(Duration::from_secs(10), received.recv())
        .await
        .expect("deadline")
        .expect("model");
    assert_eq!(
        request.messages.last().expect("user").attachments,
        attachments
    );
}

#[tokio::test]
async fn p6_blob_validation_contract() {
    let mut state = tests::test_state();
    state.config = Arc::new(multimodal_config());
    let stored = attachment(&state, "image/png", "private.png").await;
    let input = json!({"blob_id":stored.blob_id,"media_type":stored.media_type,"name":stored.name});
    let mut changed = input.clone();
    changed["name"] = json!("different.png");
    let cases = vec![
        (vec![json!({"blob_id":BlobId::new()})], "blob_not_found"),
        (
            vec![json!({"blob_id":stored.blob_id,"media_type":"image/jpeg"})],
            "blob_metadata_mismatch",
        ),
        (
            vec![json!({"blob_id":stored.blob_id,"size_bytes":0})],
            "blob_metadata_mismatch",
        ),
        (vec![changed], "blob_metadata_mismatch"),
        (vec![input.clone(), input.clone()], "duplicate_attachment"),
        (vec![input.clone(); 9], "too_many_attachments"),
    ];
    for (inputs, code) in cases {
        let response = match resolve_blob_inputs(
            &state,
            serde_json::from_value(json!(inputs)).expect("inputs"),
        )
        .await
        {
            Err(response) => response,
            Ok(_) => panic!("expected {code}"),
        };
        let bytes = axum::body::to_bytes(response.into_body(), 8192)
            .await
            .expect("body");
        let body: Value = serde_json::from_slice(&bytes).expect("json");
        assert_eq!(body["code"], code);
    }
    let unsupported = attachment(&state, "image/svg+xml", "vector.svg").await;
    let response = resolve_blob_inputs(
        &state,
        serde_json::from_value(json!([unsupported])).expect("input"),
    )
    .await
    .err()
    .expect("unsupported mime");
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let oversized = state
        .blobs
        .put(PutBlob {
            media_type: "application/octet-stream".into(),
            name: None,
            data: vec![0; MAX_BLOB_BYTES + 1],
            created_at_ms: 1,
        })
        .await
        .expect("external oversized blob");
    let response = resolve_blob_inputs(
        &state,
        serde_json::from_value(json!([{"blob_id":oversized.blob_id}])).expect("input"),
    )
    .await
    .err()
    .expect("oversized blob");
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    state.config = tests::test_state().config;
    let response = resolve_blob_inputs(
        &state,
        serde_json::from_value(json!([input])).expect("input"),
    )
    .await
    .err()
    .expect("profile rejection");
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn p6_session_idempotency_conflicts_before_metadata_validation() {
    let directory = tempfile::tempdir().expect("temp");
    let (sender, _received) = tokio::sync::mpsc::unbounded_channel();
    let (mut state, worker) = fixture(
        directory.path(),
        Arc::new(Capture(sender)),
        Arc::new(Tools {
            suspend: false,
            calls: AtomicUsize::new(0),
        }),
    )
    .await;
    state.config = Arc::new(multimodal_config());
    let sessions = Arc::new(
        SqliteRunStore::open(directory.path().join("runs.db"))
            .await
            .expect("sessions"),
    );
    Arc::get_mut(&mut state.orchestration)
        .expect("unique")
        .sessions = sessions.clone();
    let session_id = SessionId::new();
    let session = sessions
        .create_session(CreateSession {
            session_id,
            agent_profile: "test".into(),
            title: None,
            created_at_ms: 1,
        })
        .await
        .expect("session");
    let first = attachment(&state, "image/png", "first.png").await;
    let second = attachment(&state, "image/png", "second.png").await;
    let original = json!({"input":"", "attachments":[first,second], "expected_revision":session.revision, "idempotency_key":"p6-key"});
    let mut original_run = Value::Null;
    let original_blobs = state.blobs.clone();
    let original_config = state.config.clone();
    for variant in 0..6 {
        if variant == 1 {
            state.blobs = admission::test_blobs();
            state.config = tests::test_state().config;
        } else {
            state.blobs = original_blobs.clone();
            state.config = original_config.clone();
        }
        let mut body = original.clone();
        match variant {
            2 => body["attachments"].as_array_mut().expect("array").reverse(),
            3 => body["attachments"][0]["blob_id"] = json!(BlobId::new()),
            4 => body["attachments"][0]["media_type"] = json!("image/jpeg"),
            5 => body["attachments"][0]["name"] = json!("changed.png"),
            _ => {}
        }
        let response = submit_session_run_owned(
            state.clone(),
            session_id.to_string(),
            serde_json::from_value(body).expect("request"),
        )
        .await;
        assert_eq!(
            response.status(),
            if variant < 2 {
                StatusCode::ACCEPTED
            } else {
                StatusCode::CONFLICT
            },
            "variant {variant}"
        );
        let body: Value = serde_json::from_slice(
            &axum::body::to_bytes(response.into_body(), 8192)
                .await
                .expect("body"),
        )
        .expect("json");
        if variant == 0 {
            original_run = body["run_id"].clone();
        }
        if variant == 1 {
            assert_eq!(body["run_id"], original_run);
            assert_eq!(body["replayed"], true);
        }
    }
    state
        .tools_pipeline
        .as_ref()
        .expect("pipeline")
        .stop_workers()
        .await;
    worker.abort();
    let _ = worker.await;
}
