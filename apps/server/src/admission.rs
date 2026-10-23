//! HTTP mapping for the P2 admission barrier; no Context/Skills business execution.

use super::*;
use agent_core::context_operator::{ContextOutcome, ContextPolicy, ContextRequest};
use agent_harness::context_operator::{AdmissionError, legacy_plan};

pub(super) async fn owned(
    work: impl std::future::Future<Output = Response> + Send + 'static,
) -> Response {
    match tokio::spawn(work).await {
        Ok(response) => response,
        Err(error) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "admission_task_failed",
            error.to_string(),
            true,
        ),
    }
}

pub(super) async fn prepare(
    state: &AppState,
    request: PrepareRunRequest,
) -> Result<PreparedRun, Response> {
    if request.input.trim().is_empty() && request.attachments.is_empty() {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "invalid_input",
            "input or attachments must not be empty",
            false,
        ));
    }
    let orchestration = &state.orchestration;
    let config = &orchestration.config;
    let request = ContextRequest {
        request_hash: raw_request_hash(
            &request.input,
            &request.attachments,
            &request.explicit_skills,
            request.max_steps,
        ),
        run_id: request.run_id,
        session_id: request.session_id,
        through_message_ordinal: request.through_message_ordinal,
        input: request.input,
        attachments: request.attachments,
        explicit_skills: request
            .explicit_skills
            .into_iter()
            .map(|skill| (SkillId(skill.skill_id), skill.version))
            .collect(),
        max_steps: request.max_steps,
        policy: ContextPolicy {
            default_skills: config
                .default_skills
                .iter()
                .map(|skill| (SkillId(skill.skill_id.clone()), skill.version.clone()))
                .collect(),
            max_skills: config.max_skills,
            memory_enabled: config.memory_enabled,
            memory_scope: config.memory_scope.clone(),
            max_memories: config.max_memories,
            agent_profile: orchestration.agent_profile.clone(),
            agent_instruction: orchestration.agent_instruction.clone(),
            model_profile: orchestration.model_profile.clone(),
            available_tools: orchestration
                .tools
                .iter()
                .map(|tool| tool.name.clone())
                .collect(),
            budget: ContextBudget {
                model_context_tokens: orchestration.model_context_tokens,
                reserved_output_tokens: orchestration.reserved_output_tokens,
                reserved_tool_schema_tokens: config.reserved_tool_schema_tokens,
                max_skill_tokens: config.max_skill_tokens,
                max_memory_tokens: config.max_memory_tokens,
                max_history_tokens: config.max_history_tokens,
            },
            policy_version: config.context_policy_version,
            allow_run_adf: state.config.adf().enabled,
            script_runtime: serde_json::json!(orchestration.tool_runtime.script_runtime),
        },
    };
    state
        .admission
        .admit(request, Duration::from_secs(300))
        .await
        .map_err(map_error)
}

fn map_error(error: AdmissionError) -> Response {
    match error {
        AdmissionError::Preparation(error) => {
            let status = if error.code == "skill_resolution_failed" {
                StatusCode::BAD_REQUEST
            } else {
                StatusCode::UNPROCESSABLE_ENTITY
            };
            api_error(status, &error.code, error.message, false)
        }
        AdmissionError::Timeout => api_error(
            StatusCode::GATEWAY_TIMEOUT,
            "context_admission_timeout",
            "context preparation is still pending",
            true,
        ),
        AdmissionError::Fabric(error) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "context_admission_failed",
            error.to_string(),
            true,
        ),
    }
}

pub(super) fn start_recovery_worker(state: AppState) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            if let Err(error) = recover_once(&state).await {
                tracing::error!(%error, "P2 Context legacy bridge recovery failed");
            }
        }
    });
}

pub(super) async fn recover_once(
    state: &AppState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for request in state.admission.requests().await? {
        match state.admission.outcome(request.run_id).await? {
            Some(ContextOutcome::Completed(_)) => {
                let prepared = state
                    .admission
                    .wait(request.run_id, Duration::from_secs(5))
                    .await?;
                let persisted = request.session_id.is_some();
                state
                    .runtime
                    .start_context_plan(legacy_plan(request, prepared), persisted)
                    .await?;
            }
            Some(ContextOutcome::Failed(_)) if request.session_id.is_some() => {
                let snapshot = state.runtime.get_run(request.run_id).await?;
                if snapshot.as_ref().is_some_and(|run| !run.is_terminal()) {
                    let snapshot = state
                        .runtime
                        .fail_persisted(
                            request.run_id,
                            "context_build_failed",
                            "context orchestration failed",
                        )
                        .await?;
                    state
                        .orchestration
                        .sessions
                        .finalize_run(FinalizeSessionRun {
                            session_id: request.session_id.expect("session request"),
                            run: snapshot,
                            finalized_at_ms: run_runtime::unix_time_ms(),
                        })
                        .await?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn test_blobs() -> Arc<dyn BlobStore> {
    use agent_core::harness::{BlobMetadata, BlobObject, BlobStoreFuture};
    struct ScopedBlobs {
        store: FilesystemBlobStore,
        _directory: tempfile::TempDir,
    }
    impl BlobStore for ScopedBlobs {
        fn put(&self, command: PutBlob) -> BlobStoreFuture<'_, BlobMetadata> {
            self.store.put(command)
        }
        fn get(&self, blob_id: BlobId) -> BlobStoreFuture<'_, Option<BlobObject>> {
            self.store.get(blob_id)
        }
    }
    let directory = tempfile::tempdir().expect("test blob directory");
    Arc::new(ScopedBlobs {
        store: FilesystemBlobStore::new(directory.path()),
        _directory: directory,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_core::{
        context::{ContextBudgetReport, ContextPack},
        context_operator::PreparedContext,
        skill::SkillLock,
    };
    use agent_harness::context_operator::{AdmissionFacade, ContextPreparer, PrepareFuture};
    use tokio::sync::{Notify, mpsc};

    struct BlockedPreparer {
        entered: mpsc::UnboundedSender<RunId>,
        release: Arc<Notify>,
    }

    impl ContextPreparer for BlockedPreparer {
        fn prepare(&self, request: ContextRequest) -> PrepareFuture<'_> {
            Box::pin(async move {
                self.entered.send(request.run_id).expect("observer");
                self.release.notified().await;
                Ok(PreparedContext {
                    pack: ContextPack {
                        messages: vec![],
                        memory_refs: vec![],
                        artifact_refs: vec![],
                        skill_refs: vec![],
                        effective_tools: vec![],
                        budget: ContextBudgetReport {
                            available_input_tokens: 100,
                            estimated_input_tokens: 0,
                            estimator_confidence: 1.0,
                            retained_item_ids: vec![],
                            dropped: vec![],
                        },
                        component_descriptors: vec![],
                        fingerprint: "test-fingerprint".into(),
                    },
                    skill_lock: SkillLock::default(),
                    request_digest: "digest".into(),
                })
            })
        }
    }

    #[tokio::test]
    async fn p2_http_disconnect_after_publish_still_starts_legacy() {
        let mut state = crate::tests::test_state();
        let directory = tempfile::tempdir().expect("directory");
        let fabric = Arc::new(
            SqliteEventStore::open(directory.path().join("events.sqlite"), "p2")
                .await
                .expect("open"),
        );
        let (entered, mut observed) = mpsc::unbounded_channel();
        let release = Arc::new(Notify::new());
        state.admission = Arc::new(AdmissionFacade::new(
            fabric.clone(),
            state.blobs.clone(),
            Arc::new(BlockedPreparer {
                entered,
                release: release.clone(),
            }),
        ));
        let worker = state.admission.start_worker();
        let http = {
            let state = state.clone();
            tokio::spawn(async move {
                create_run(
                    State(state),
                    Json(
                        serde_json::from_value(serde_json::json!({"input":"hello", "stream":true}))
                            .expect("request"),
                    ),
                )
                .await
            })
        };
        let run_id = observed.recv().await.expect("context claimed");
        assert!(
            state
                .runtime
                .get_run(run_id)
                .await
                .expect("snapshot")
                .is_none()
        );
        assert!(
            state
                .admission
                .request(run_id)
                .await
                .expect("request")
                .is_some()
        );
        http.abort();
        assert!(http.await.expect_err("disconnected").is_cancelled());
        release.notify_one();
        state
            .admission
            .wait(run_id, Duration::from_secs(2))
            .await
            .expect("context durable");
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if state
                    .runtime
                    .get_run(run_id)
                    .await
                    .expect("snapshot")
                    .is_some()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("legacy starts without HTTP waiter");
        worker.abort();
    }

    #[tokio::test]
    async fn p2_preparation_errors_precede_sse_headers_with_legacy_status() {
        for (skills, tokens, status, code) in [
            (
                serde_json::json!([{"skill_id":"absent", "version":"1"}]),
                8192,
                StatusCode::BAD_REQUEST,
                "skill_resolution_failed",
            ),
            (
                serde_json::json!([]),
                1,
                StatusCode::UNPROCESSABLE_ENTITY,
                "context_build_failed",
            ),
        ] {
            let mut state = crate::tests::test_state();
            Arc::get_mut(&mut state.orchestration)
                .expect("owned config")
                .model_context_tokens = tokens;
            let response = create_run(
                State(state.clone()),
                Json(
                    serde_json::from_value(
                        serde_json::json!({"input":"hello", "stream":true, "skills":skills}),
                    )
                    .expect("request"),
                ),
            )
            .await;
            assert_eq!(response.status(), status);
            assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
            let body = axum::body::to_bytes(response.into_body(), 65536)
                .await
                .expect("body");
            let body: serde_json::Value = serde_json::from_slice(&body).expect("JSON");
            assert_eq!(body["code"], code);
            let requests = state.admission.requests().await.expect("durable requests");
            assert_eq!(requests.len(), 1);
            assert!(matches!(
                state
                    .admission
                    .outcome(requests[0].run_id)
                    .await
                    .expect("outcome"),
                Some(ContextOutcome::Failed(_))
            ));
            assert!(
                state
                    .runtime
                    .get_run(requests[0].run_id)
                    .await
                    .expect("snapshot")
                    .is_none()
            );
        }
    }

    #[tokio::test]
    async fn p2_session_concurrency_during_context_preserves_key_hash_revision_and_busy() {
        let mut state = crate::tests::test_state();
        let directory = tempfile::tempdir().expect("directory");
        let fabric = Arc::new(
            SqliteEventStore::open(directory.path().join("events.sqlite"), "p2")
                .await
                .expect("open"),
        );
        let (entered, mut observed) = mpsc::unbounded_channel();
        let release = Arc::new(Notify::new());
        state.admission = Arc::new(AdmissionFacade::new(
            fabric,
            state.blobs.clone(),
            Arc::new(BlockedPreparer {
                entered,
                release: release.clone(),
            }),
        ));
        let worker = state.admission.start_worker();
        let session_id = SessionId::new();
        state
            .orchestration
            .sessions
            .create_session(CreateSession {
                session_id,
                agent_profile: "test".into(),
                title: None,
                created_at_ms: 1,
            })
            .await
            .expect("session");
        let submit = |state: AppState, input: &str, key: &str, revision: u64| {
            let request = serde_json::from_value(serde_json::json!({"input":input, "idempotency_key":key, "expected_revision":revision})).expect("request");
            async move {
                submit_session_run(State(state), Path(session_id.to_string()), Json(request)).await
            }
        };
        let original = tokio::spawn(submit(state.clone(), "hello", "key", 0));
        let run_id = observed.recv().await.expect("context claimed");
        let (replay, conflict, busy, revision) = tokio::join!(
            submit(state.clone(), "hello", "key", 0),
            submit(state.clone(), "different", "key", 0),
            submit(state.clone(), "hello", "other", 1),
            submit(state.clone(), "hello", "other", 99),
        );
        assert_eq!(replay.status(), StatusCode::ACCEPTED);
        let body = axum::body::to_bytes(replay.into_body(), 65536)
            .await
            .expect("body");
        let body: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(body["run_id"], run_id.to_string());
        assert_eq!(body["replayed"], true);
        for response in [conflict, busy, revision] {
            assert_eq!(response.status(), StatusCode::CONFLICT);
        }
        assert_eq!(
            state
                .admission
                .requests()
                .await
                .expect("durable requests")
                .len(),
            1
        );
        assert!(observed.try_recv().is_err());
        release.notify_one();
        assert_eq!(
            original.await.expect("original").status(),
            StatusCode::ACCEPTED
        );
        worker.abort();
    }

    #[test]
    fn p2_gateway_has_no_context_business_or_new_model_tool_execution() {
        let gateway = include_str!("main.rs");
        assert!(!gateway.contains(".resolve(ResolveSkillsRequest"));
        assert!(!gateway.contains(".build(ContextBuildRequest"));
        let operator = include_str!("../../../crates/harness/src/context_operator.rs");
        for forbidden in [
            "ModelProvider",
            "ToolPort",
            "model.requested",
            "tool.requested",
            "EngineVersion::OperatorV1",
        ] {
            assert!(
                !operator.contains(forbidden),
                "P2 must not introduce {forbidden}"
            );
        }
    }

    fn durable_state(
        runs: Arc<SqliteRunStore>,
        fabric: Arc<SqliteEventStore>,
        blobs: Arc<dyn BlobStore>,
        preparer: Arc<dyn ContextPreparer>,
    ) -> AppState {
        let mut state = crate::tests::test_state();
        let hook: Arc<dyn ObservationHook> = Arc::new(TracingObservationHook);
        let (agent, _, _) = build_agent(
            &state.config,
            state.approvals.clone(),
            Arc::new(HeuristicTokenEstimator),
            hook.clone(),
            blobs.clone(),
        )
        .expect("agent");
        state.runtime = RunRuntime::new_durable(
            Harness::new(ObservedMachine::new(agent, hook)),
            runs.clone(),
            fabric.clone(),
            |_| {},
        );
        Arc::get_mut(&mut state.orchestration)
            .expect("owned orchestration")
            .sessions = runs;
        state.admission = Arc::new(AdmissionFacade::new(fabric, blobs, preparer));
        state
    }

    #[tokio::test]
    async fn p2_stateless_and_session_reopen_finish_context_then_start_legacy_once() {
        for session in [false, true] {
            let directory = tempfile::tempdir().expect("directory");
            let path = directory.path().join("state.sqlite");
            let blobs: Arc<dyn BlobStore> =
                Arc::new(FilesystemBlobStore::new(directory.path().join("blobs")));
            let (entered, mut observed) = mpsc::unbounded_channel();
            let release = Arc::new(Notify::new());
            let preparer: Arc<dyn ContextPreparer> = Arc::new(BlockedPreparer {
                entered,
                release: release.clone(),
            });
            let run_id = RunId::new();
            let session_id = session.then(SessionId::new);
            {
                let runs = Arc::new(SqliteRunStore::open(&path).await.expect("open runs"));
                let fabric = Arc::new(
                    SqliteEventStore::open(&path, "p2")
                        .await
                        .expect("open fabric"),
                );
                let state = durable_state(
                    runs.clone(),
                    fabric.clone(),
                    blobs.clone(),
                    preparer.clone(),
                );
                if let Some(session_id) = session_id {
                    runs.create_session(CreateSession {
                        session_id,
                        agent_profile: "test".into(),
                        title: None,
                        created_at_ms: 1,
                    })
                    .await
                    .expect("session");
                    runs.begin_run(
                        BeginSessionRun {
                            session_id,
                            run_id,
                            expected_revision: 0,
                            idempotency_key: "key".into(),
                            request_hash: "hash".into(),
                            input: vec![ContentPart::text("hello")],
                            created_at_ms: 2,
                        },
                        RunSnapshot::new(run_id, "hello", 2),
                    )
                    .await
                    .expect("begin");
                }
                state
                    .admission
                    .publish(ContextRequest {
                        run_id,
                        session_id,
                        through_message_ordinal: session.then_some(1),
                        request_hash: "hash".into(),
                        input: "hello".into(),
                        attachments: vec![],
                        explicit_skills: vec![],
                        max_steps: 10,
                        policy: ContextPolicy {
                            default_skills: vec![],
                            max_skills: 8,
                            memory_enabled: false,
                            memory_scope: "test".into(),
                            max_memories: 8,
                            agent_profile: "test".into(),
                            agent_instruction: "test".into(),
                            model_profile: "test".into(),
                            available_tools: vec![],
                            budget: ContextBudget {
                                model_context_tokens: 8192,
                                reserved_output_tokens: 1024,
                                reserved_tool_schema_tokens: 0,
                                max_skill_tokens: 100,
                                max_memory_tokens: 100,
                                max_history_tokens: 100,
                            },
                            policy_version: 1,
                            allow_run_adf: false,
                            script_runtime: serde_json::Value::Null,
                        },
                    })
                    .await
                    .expect("durable request before shutdown");
                assert!(
                    FlowStore::get(fabric.as_ref(), run_id)
                        .await
                        .expect("flow")
                        .is_none()
                );
            }
            {
                let runs = Arc::new(SqliteRunStore::open(&path).await.expect("reopen runs"));
                let fabric = Arc::new(
                    SqliteEventStore::open(&path, "p2")
                        .await
                        .expect("reopen fabric"),
                );
                let state = durable_state(runs, fabric.clone(), blobs.clone(), preparer.clone());
                let pending = state
                    .admission
                    .requests()
                    .await
                    .expect("requests")
                    .into_iter()
                    .map(|request| request.run_id)
                    .collect();
                assert_eq!(
                    state
                        .runtime
                        .recover_interrupted_except(&pending)
                        .await
                        .expect("startup recovery"),
                    0
                );
                recover_once(&state).await.expect("no context yet");
                assert!(
                    FlowStore::get(fabric.as_ref(), run_id)
                        .await
                        .expect("flow")
                        .is_none()
                );
                release.notify_one();
                state.admission.tick().await.expect("prepare after reopen");
                assert_eq!(observed.recv().await.expect("prepared"), run_id);
                recover_once(&state).await.expect("legacy bridge");
                recover_once(&state).await.expect("duplicate recovery");
                assert_eq!(
                    FlowStore::get(fabric.as_ref(), run_id)
                        .await
                        .expect("flow")
                        .expect("started flow")
                        .revision,
                    0
                );
            }
            let runs = Arc::new(
                SqliteRunStore::open(&path)
                    .await
                    .expect("second reopen runs"),
            );
            let fabric = Arc::new(
                SqliteEventStore::open(&path, "p2")
                    .await
                    .expect("second reopen fabric"),
            );
            let state = durable_state(runs, fabric.clone(), blobs.clone(), preparer.clone());
            recover_once(&state).await.expect("reuse legacy start");
            assert_eq!(
                FlowStore::get(fabric.as_ref(), run_id)
                    .await
                    .expect("flow")
                    .expect("started flow")
                    .revision,
                0
            );
            assert!(observed.try_recv().is_err());
        }
    }
}
