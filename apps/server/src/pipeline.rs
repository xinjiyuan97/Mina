use super::*;
use agent_core::operator_fabric::OperatorFabric;
use agent_extension::pipeline::SqlitePipelineStore;
use agent_harness::PlannedRun;
use agent_harness::pipeline::{PipelineRuntime, PipelineStart};

pub(super) async fn resolve_operator_approval(
    state: &AppState,
    run_id: RunId,
    approval_id: ApprovalId,
    resolution: ApprovalResolution,
) -> Option<Response> {
    use agent_core::{operator_fabric::FabricError, tool_authorization::AuthorizationError};
    let snapshot = match state.runtime.get_run(run_id).await {
        Ok(Some(snapshot)) => snapshot,
        Ok(None) => return None,
        Err(error) => return Some(map_store_error(error)),
    };
    if snapshot
        .execution_manifest
        .as_ref()
        .and_then(|manifest| manifest["engine_version"].as_str())
        != Some("operator_v1")
    {
        return None;
    }
    let ingress = agent_harness::approval_operator::ApprovalIngress::new(state.admission.fabric());
    Some(
        match ingress
            .resolve(
                "local",
                run_id,
                approval_id,
                "gateway:http",
                resolution.clone(),
                agent_harness::unix_time_ms(),
            )
            .await
        {
            Ok(changed) => Json(ResolveApprovalResponse {
                run_id,
                approval_id,
                resolution,
                replayed: !changed,
            })
            .into_response(),
            Err(FabricError::NotFound) => api_error(
                StatusCode::NOT_FOUND,
                "approval_not_found",
                "the approval does not exist for this run",
                false,
            ),
            Err(FabricError::Conflict) => api_error(
                StatusCode::CONFLICT,
                "approval_already_resolved",
                "the approval was already resolved with a different decision",
                false,
            ),
            Err(FabricError::Invalid(message))
                if message == AuthorizationError::Expired.to_string() =>
            {
                api_error(StatusCode::GONE, "approval_expired", message, false)
            }
            Err(FabricError::Invalid(message))
                if message == AuthorizationError::Cancelled.to_string() =>
            {
                api_error(StatusCode::CONFLICT, "approval_cancelled", message, false)
            }
            Err(FabricError::Invalid(message)) => {
                api_error(StatusCode::BAD_REQUEST, "invalid_approval", message, false)
            }
            Err(error) => api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "approval_persistence_error",
                error.to_string(),
                true,
            ),
        },
    )
}

pub(super) async fn fail_context(
    state: &AppState,
    request: &agent_core::context_operator::ContextRequest,
    code: &str,
) -> Result<(), RunRuntimeError> {
    if request.execution_profile.as_deref() == Some(agent_core::effect_pipeline::TOOLS_PROFILE) {
        return crate::tools_pipeline::fail_context(state, request, code).await;
    }
    let pipeline = state.pipeline.as_ref().ok_or_else(|| {
        RunRuntimeError::Machine("operator_v1 is unavailable; legacy fallback forbidden".into())
    })?;
    let mut start = PipelineStart::text(
        request.run_id,
        &request.input,
        &state.config.default_model().model,
    );
    start.context_event_id =
        agent_harness::context_operator::event_id(request.run_id, "context.failed");
    start.context_sequence = 3;
    start.session_id = request.session_id;
    if pipeline
        .store
        .load(request.run_id)
        .await
        .map_err(|error| RunRuntimeError::Persistence(error.to_string()))?
        .is_none()
    {
        pipeline
            .fail_admission(start, code)
            .await
            .map_err(|error| RunRuntimeError::Persistence(error.to_string()))?;
    } else {
        pipeline
            .drive(request.run_id)
            .await
            .map_err(|error| RunRuntimeError::Persistence(error.to_string()))?;
    }
    Ok(())
}

pub(super) fn build(
    path: &std::path::Path,
    runs: Arc<SqliteRunStore>,
    blobs: Arc<dyn BlobStore>,
    fabric: Arc<dyn OperatorFabric>,
    config: &HarnessConfig,
) -> Result<PipelineRuntime, Box<dyn std::error::Error>> {
    let provider = ConfiguredModelProvider::from_model_config(config.default_model())?
        .with_token_estimator(Arc::new(HeuristicTokenEstimator))
        .with_blob_store(blobs.clone());
    Ok(PipelineRuntime::new(
        Arc::new(SqlitePipelineStore::open(path)?),
        runs,
        blobs,
        Arc::new(provider),
    )
    .with_fabric(fabric))
}

pub(super) async fn start(
    state: &AppState,
    plan: PlannedRun,
    persisted: bool,
) -> Result<StartedRun, RunRuntimeError> {
    if let Some(snapshot) = state.runtime.get_run(plan.run_id).await?
        && let Some(manifest) = &snapshot.execution_manifest
    {
        agent_core::operator::validate_execution_manifest(manifest)
            .map_err(|code| RunRuntimeError::Machine(code.into()))?;
    }
    let request = state
        .admission
        .request(plan.run_id)
        .await
        .map_err(|error| RunRuntimeError::Persistence(error.to_string()))?;
    if request
        .as_ref()
        .is_none_or(|request| request.engine_version.is_legacy())
    {
        return state.runtime.start_context_plan(plan, persisted).await;
    }
    if request
        .as_ref()
        .and_then(|request| request.execution_profile.as_deref())
        == Some(agent_core::effect_pipeline::TOOLS_PROFILE)
    {
        return state
            .tools_pipeline
            .as_ref()
            .ok_or_else(|| {
                RunRuntimeError::Machine(
                    "operator_v1_tools unavailable; legacy fallback forbidden".into(),
                )
            })?
            .start(state, plan)
            .await;
    }
    if request
        .as_ref()
        .and_then(|request| request.execution_profile.as_deref())
        .is_some_and(|profile| profile != "operator_v1_text")
    {
        return Err(RunRuntimeError::Machine(
            "unsupported persisted operator profile".into(),
        ));
    }
    let pipeline = state.pipeline.clone().ok_or_else(|| {
        RunRuntimeError::Machine("operator_v1 is unavailable; legacy fallback forbidden".into())
    })?;
    if plan
        .allowed_tools
        .as_ref()
        .is_none_or(|tools| !tools.is_empty())
        || plan.allow_run_adf
        || !plan.attachments.is_empty()
    {
        return Err(RunRuntimeError::Machine(
            "unsupported operator_v1 profile".into(),
        ));
    }
    let run_id = plan.run_id;
    let mut start = PipelineStart::text(run_id, &plan.input, &state.config.default_model().model);
    start.context_sequence = 3;
    start.session_id = request.as_ref().and_then(|request| request.session_id);
    start.start.prior_messages = plan.prior_messages;
    start.start.context_fingerprint = plan.context_fingerprint;
    start.start.config.max_output_tokens = state.config.default_model().max_output_tokens;
    start.start.config.max_steps = plan.max_steps.unwrap_or(1);
    if pipeline
        .store
        .load(run_id)
        .await
        .map_err(|error| RunRuntimeError::Persistence(error.to_string()))?
        .is_none()
    {
        pipeline
            .admit(start)
            .await
            .map_err(|error| RunRuntimeError::Persistence(error.to_string()))?;
    }
    let (sender, events) = broadcast::channel(256);
    tokio::spawn(async move {
        let mut cursor = 0;
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            if let Err(error) = pipeline.drive(run_id).await {
                tracing::error!(%error, %run_id, "operator_v1 drive failed; no legacy fallback");
            }
            loop {
                let Ok(events) = pipeline.runs.events_after(run_id, cursor, 512).await else {
                    break;
                };
                if events.is_empty() {
                    break;
                }
                for event in events {
                    cursor = event.seq;
                    let terminal = event.kind.is_terminal();
                    let _ = sender.send(event);
                    if terminal {
                        return;
                    }
                }
            }
        }
    });
    Ok(StartedRun { run_id, events })
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_core::harness::{
        FinishReason, ModelEvent, ModelEventStream, ModelPort, ModelRequest, TokenUsage,
        TokenUsageSource,
    };
    use axum::body::to_bytes;
    use serde_json::{Value, json};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Fake(Arc<AtomicUsize>);
    impl ModelPort for Fake {
        fn stream(&self, request: ModelRequest) -> ModelEventStream {
            assert!(request.tools.is_empty());
            self.0.fetch_add(1, Ordering::SeqCst);
            Box::pin(futures_util::stream::iter([
                ModelEvent::TextDelta {
                    delta: "hello ".into(),
                },
                ModelEvent::TextDelta {
                    delta: "world".into(),
                },
                ModelEvent::Usage {
                    usage: TokenUsage {
                        input_tokens: 2,
                        output_tokens: 3,
                        total_tokens: 5,
                        source: TokenUsageSource::ProviderReported,
                    },
                },
                ModelEvent::Completed {
                    finish_reason: FinishReason::Stop,
                },
            ]))
        }
    }

    async fn body(response: Response) -> String {
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = tokio::time::timeout(
            Duration::from_secs(10),
            to_bytes(response.into_body(), 1024 * 1024),
        )
        .await
        .expect("deadline")
        .expect("body");
        String::from_utf8(bytes.to_vec()).expect("UTF8")
    }

    fn frames(body: &str) -> Vec<Value> {
        body.split("\n\n")
            .filter_map(|frame| {
                let data = frame.lines().find_map(|line| line.strip_prefix("data: "))?;
                let value: Value = serde_json::from_str(data).expect("JSON");
                let id = frame
                    .lines()
                    .find_map(|line| line.strip_prefix("id: "))
                    .expect("numeric id")
                    .parse::<u64>()
                    .expect("numeric");
                assert_eq!(value["seq"], id);
                assert_eq!(
                    frame
                        .lines()
                        .find_map(|line| line.strip_prefix("event: "))
                        .expect("event"),
                    value["type"].as_str().expect("type")
                );
                Some(value)
            })
            .collect()
    }

    #[tokio::test]
    async fn p3_opt_in_live_replay_last_event_id_and_default_legacy() {
        let mut state = crate::tests::test_state();
        assert!(state.pipeline_profile.engine().is_legacy());
        let directory = tempfile::tempdir().expect("temp");
        let ledger = Arc::new(
            SqlitePipelineStore::open(directory.path().join("pipeline.db")).expect("ledger"),
        );
        let calls = Arc::new(AtomicUsize::new(0));
        state.pipeline_profile = crate::tools_pipeline::EngineProfile::Text;
        state.pipeline = Some(Arc::new(
            PipelineRuntime::new(
                ledger,
                state.runtime.run_store(),
                state.blobs.clone(),
                Arc::new(Fake(calls.clone())),
            )
            .with_fabric(state.admission.fabric()),
        ));
        let response = create_run(
            State(state.clone()),
            Json(serde_json::from_value(json!({"input":"hello", "stream":true})).expect("request")),
        )
        .await;
        let live = frames(&body(response).await);
        assert_eq!(live.last().expect("terminal")["type"], "run_completed");
        let run_id = live[0]["run_id"].as_str().expect("run").to_owned();
        let output: String = live
            .iter()
            .filter_map(|event| event["delta"].as_str())
            .collect();
        assert_eq!(output, "hello world");
        assert_eq!(
            live.iter()
                .find(|event| event["type"] == "usage_updated")
                .expect("usage")["usage"]["total_tokens"],
            5
        );
        for cutoff in 0..live.len() as u64 {
            let mut headers = HeaderMap::new();
            headers.insert("last-event-id", cutoff.to_string().parse().expect("header"));
            let replay = get_run_events(
                State(state.clone()),
                Path(run_id.clone()),
                Query(RunEventsQuery {
                    after_seq: None,
                    follow: Some(false),
                }),
                headers,
            )
            .await;
            assert_eq!(
                frames(&body(replay).await),
                live.iter()
                    .filter(|event| event["seq"].as_u64().expect("seq") > cutoff)
                    .cloned()
                    .collect::<Vec<_>>()
            );
        }
        admission::recover_once(&state)
            .await
            .expect("duplicate recovery");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let run_id = run_id.parse().expect("run id");
        assert!(
            state
                .runtime
                .fail_persisted(run_id, "forbidden", "forbidden")
                .await
                .is_err()
        );
        let response = create_run(
            State(state.clone()),
            Json(
                serde_json::from_value(json!({"input":"nonstream", "stream":false}))
                    .expect("request"),
            ),
        )
        .await;
        let result: Value = serde_json::from_str(&body(response).await).expect("response");
        assert_eq!(result["output"], "hello world");
        assert_eq!(result["finish_reason"], "stop");
        assert_eq!(result["usage"]["total_tokens"], 5);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        state.pipeline = None;
        let response = create_run(
            State(state),
            Json(
                serde_json::from_value(json!({"input":"no fallback", "stream":false}))
                    .expect("request"),
            ),
        )
        .await;
        assert!(response.status().is_server_error());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}
