use super::*;
use crate::tools_pipeline::ServerAgentRuntime;
use agent_core::agent::AgentResources;
use agent_core::contracts::fabric::OperatorFabric;
use agent_harness::PlannedRun;

pub(super) async fn resolve_operator_approval(
    state: &AppState,
    run_id: RunId,
    approval_id: ApprovalId,
    resolution: ApprovalResolution,
) -> Option<Response> {
    use agent_core::{contracts::fabric::FabricError, tool_authorization::AuthorizationError};
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
    let pipeline = if snapshot
        .execution_manifest
        .as_ref()
        .is_some_and(|m| m["profile"] == "operator_v1_text")
    {
        &state.pipeline
    } else {
        &state.tools_pipeline
    };
    let Some(pipeline) = pipeline else {
        return Some(api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "operator_unavailable",
            "graph engine unavailable",
            true,
        ));
    };
    let ingress = &pipeline.engine;
    Some(
        match ingress
            .approve(run_id, approval_id, "gateway:http", resolution.clone())
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
    request: &agent_core::contracts::preparation::ContextRequest,
    code: &str,
) -> Result<(), RunRuntimeError> {
    let pipeline = if request.execution_profile.as_deref() == Some("operator_v1_text") {
        &state.pipeline
    } else {
        &state.tools_pipeline
    };
    let pipeline = pipeline
        .as_ref()
        .ok_or_else(|| RunRuntimeError::Machine("graph engine unavailable".into()))?;
    pipeline
        .start_outcome(
            state,
            PlannedRun {
                run_id: request.run_id,
                input: request.input.clone(),
                attachments: request.attachments.clone(),
                prior_messages: vec![],
                allowed_tools: Some(request.policy.available_tools.clone()),
                allow_run_adf: request.policy.allow_run_adf,
                max_steps: Some(request.max_steps),
                context_fingerprint: None,
                execution_manifest: Some(serde_json::json!({"profile":request.execution_profile})),
            },
            Some(code),
        )
        .await?;
    Ok(())
}

pub(super) fn build(
    _path: &std::path::Path,
    runs: Arc<SqliteRunStore>,
    blobs: Arc<dyn BlobStore>,
    fabric: Arc<dyn OperatorFabric>,
    config: &HarnessConfig,
) -> Result<ServerAgentRuntime, Box<dyn std::error::Error>> {
    let provider = ConfiguredModelProvider::from_model_config(config.default_model())?
        .with_token_estimator(Arc::new(HeuristicTokenEstimator))
        .with_blob_store(blobs);
    Ok(ServerAgentRuntime::new(
        AgentResources::new(fabric)
            .model("primary", "server-v1", Arc::new(provider))
            .toolset("workspace", "server-v1", Arc::new(ToolRegistry::new())),
        runs,
    ))
}

pub(super) async fn start(
    state: &AppState,
    plan: PlannedRun,
    _persisted: bool,
) -> Result<StartedRun, RunRuntimeError> {
    let request = state
        .admission
        .request(plan.run_id)
        .await
        .map_err(|e| RunRuntimeError::Persistence(e.to_string()))?
        .ok_or_else(|| RunRuntimeError::Machine("missing admission".into()))?;
    if request.engine_version.is_legacy() {
        return Err(RunRuntimeError::Machine("legacy execution removed".into()));
    }
    let pipeline = match request.execution_profile.as_deref() {
        Some("operator_v1_text") => {
            if plan
                .allowed_tools
                .as_ref()
                .is_none_or(|tools| !tools.is_empty())
                || plan.allow_run_adf
            {
                return Err(RunRuntimeError::Machine(
                    "text profile cannot execute tools".into(),
                ));
            }
            &state.pipeline
        }
        Some("operator_v1_tools") => &state.tools_pipeline,
        _ => return Err(RunRuntimeError::Machine("unsupported graph profile".into())),
    };
    pipeline
        .as_ref()
        .ok_or_else(|| RunRuntimeError::Machine("graph engine unavailable".into()))?
        .start(state, plan)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_core::contracts::{
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
    async fn operator_text_live_replay_and_last_event_id() {
        let mut state = crate::tests::test_state();
        assert_eq!(
            state.pipeline_profile,
            crate::tools_pipeline::EngineProfile::Tools
        );
        let calls = Arc::new(AtomicUsize::new(0));
        state.pipeline_profile = crate::tools_pipeline::EngineProfile::Text;
        state.pipeline = Some(Arc::new(ServerAgentRuntime::new(
            AgentResources::new(state.admission.fabric())
                .model("primary", "server-v1", Arc::new(Fake(calls.clone())))
                .toolset("workspace", "server-v1", Arc::new(ToolRegistry::new())),
            state.runtime.run_store(),
        )));
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
