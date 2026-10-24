use super::*;
use agent_core::{
    context::{ContextBudgetReport, ContextPack},
    context_operator::{ContextRequest, PreparedContext},
    harness::*,
    skill::SkillLock,
};
use agent_harness::{
    context_operator::{AdmissionFacade, ContextPreparer, PrepareFuture},
    pipeline::{EffectWaitBridge, ToolsPipelineRuntime},
};
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Notify;

#[test]
fn tools_profile_is_explicit_and_default_and_text_are_unchanged() {
    use crate::tools_pipeline::{EngineProfile, parse_profile};
    assert_eq!(parse_profile(None).expect("default"), EngineProfile::Legacy);
    assert_eq!(
        parse_profile(Some("operator_v1_text")).expect("text"),
        EngineProfile::Text
    );
    assert_eq!(
        parse_profile(Some("operator_v1_tools")).expect("tools"),
        EngineProfile::Tools
    );
    assert!(parse_profile(Some("operator_v1")).is_err());
}

#[tokio::test]
async fn p5_canary_applies_only_to_new_admissions_after_rollback() {
    let directory = tempfile::tempdir().expect("temp");
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        release: Arc::new(Notify::new()),
    });
    let tools = Arc::new(Tools {
        suspend: false,
        calls: AtomicUsize::new(0),
    });
    let (mut state, worker) = fixture(directory.path(), model, tools).await;
    let original = RunId::new();
    state.rollout =
        crate::rollout::Rollout::parse(Some("0"), Some(&original.to_string())).expect("rollout");
    let input = |run_id| PrepareRunRequest {
        attachment_names: None,
        run_id,
        input: "canary".into(),
        attachments: vec![],
        explicit_skills: vec![],
        max_steps: 3,
        session_id: None,
        through_message_ordinal: None,
    };
    let prepared = prepare_run_plan(&state, input(original))
        .await
        .expect("admit operator");
    state.rollout = crate::rollout::Rollout::parse(Some("0"), None).expect("rollback");
    state.pipeline_profile = crate::tools_pipeline::EngineProfile::Legacy;
    let new_run = RunId::new();
    prepare_run_plan(&state, input(new_run))
        .await
        .expect("new admission stays legacy");
    assert!(
        state
            .admission
            .request(new_run)
            .await
            .expect("request")
            .expect("saved")
            .engine_version
            .is_legacy()
    );
    let pinned = state
        .admission
        .request(original)
        .await
        .expect("request")
        .expect("saved");
    assert_eq!(
        pinned.execution_profile.as_deref(),
        Some("operator_v1_tools")
    );
    pipeline::start(&state, prepared.plan, false)
        .await
        .expect("old run drains on original engine");
    let snapshot = state
        .runtime
        .get_run(original)
        .await
        .expect("snapshot")
        .expect("run");
    let manifest = snapshot.execution_manifest.expect("manifest");
    assert_eq!(manifest["engine_version"], "operator_v1");
    assert_eq!(manifest["profile"], "operator_v1_tools");
    assert_eq!(manifest["projection_version"], 1);
    state
        .tools_pipeline
        .as_ref()
        .expect("pipeline")
        .stop_workers()
        .await;
    worker.abort();
    let _ = worker.await;
}

#[tokio::test]
async fn p5_session_backlog_rejection_releases_the_session_without_execution() {
    let directory = tempfile::tempdir().expect("temp");
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        release: Arc::new(Notify::new()),
    });
    let tools = Arc::new(Tools {
        suspend: false,
        calls: AtomicUsize::new(0),
    });
    let (mut state, worker) = fixture(directory.path(), model.clone(), tools.clone()).await;
    let original = RunId::new();
    prepare_run_plan(
        &state,
        PrepareRunRequest {
            attachment_names: None,
            run_id: original,
            input: "template".into(),
            attachments: vec![],
            explicit_skills: vec![],
            max_steps: 3,
            session_id: None,
            through_message_ordinal: None,
        },
    )
    .await
    .expect("prepare template");
    worker.abort();
    let _ = worker.await;
    let mut request = state
        .admission
        .request(original)
        .await
        .expect("request")
        .expect("saved");
    for _ in 0..128 {
        request.run_id = RunId::new();
        state
            .admission
            .publish(request.clone())
            .await
            .expect("fill pending queue");
    }
    let sessions = Arc::new(
        SqliteRunStore::open(directory.path().join("runs.db"))
            .await
            .expect("sessions"),
    );
    Arc::get_mut(&mut state.orchestration)
        .expect("unique state")
        .sessions = sessions.clone();
    let session_id = SessionId::new();
    let session = sessions
        .create_session(CreateSession {
            session_id,
            agent_profile: "agent-loop".into(),
            title: None,
            created_at_ms: 0,
        })
        .await
        .expect("session");
    let response = submit_session_run_owned(
        state,
        session_id.to_string(),
        SubmitSessionRunRequest {
            input: "backpressure".into(),
            attachments: vec![],
            expected_revision: session.revision,
            idempotency_key: "capacity-attempt".into(),
            max_steps: Some(3),
            skills: vec![],
        },
    )
    .await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let session = sessions
        .get_session(session_id)
        .await
        .expect("session")
        .expect("saved");
    assert!(
        session.active_run_id.is_none(),
        "capacity rejection must not leave a busy session"
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    assert_eq!(tools.calls.load(Ordering::SeqCst), 0);
}

struct Prepared;
impl ContextPreparer for Prepared {
    fn prepare(&self, request: ContextRequest) -> PrepareFuture<'_> {
        Box::pin(async move {
            Ok(PreparedContext {
                pack: ContextPack {
                    messages: vec![],
                    memory_refs: vec![],
                    artifact_refs: vec![],
                    skill_refs: vec![],
                    effective_tools: request.policy.available_tools,
                    budget: ContextBudgetReport {
                        available_input_tokens: 1000,
                        estimated_input_tokens: 0,
                        estimator_confidence: 1.0,
                        retained_item_ids: vec![],
                        dropped: vec![],
                    },
                    component_descriptors: vec![],
                    fingerprint: "p4-context".into(),
                },
                skill_lock: SkillLock::default(),
                request_digest: request.request_hash,
            })
        })
    }
}

struct FailedContext;
impl ContextPreparer for FailedContext {
    fn prepare(&self, _: ContextRequest) -> PrepareFuture<'_> {
        Box::pin(async {
            Err(agent_core::context_operator::ContextFailed {
                code: "context_build_failed".into(),
                message: "fake context failure".into(),
            })
        })
    }
}

#[tokio::test]
async fn tools_context_failure_is_durable_without_model_tool_or_text_fallback() {
    let directory = tempfile::tempdir().expect("temp");
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        release: Arc::new(Notify::new()),
    });
    let tools = Arc::new(Tools {
        suspend: false,
        calls: AtomicUsize::new(0),
    });
    let (mut state, worker) = fixture(directory.path(), model.clone(), tools.clone()).await;
    worker.abort();
    let _ = worker.await;
    state.admission = Arc::new(AdmissionFacade::new(
        state.admission.fabric(),
        state.blobs.clone(),
        Arc::new(FailedContext),
    ));
    let worker = state.admission.start_worker();
    let (address, server) = serve(state.clone()).await;
    let response = reqwest::Client::new()
        .post(format!("{address}/api/v1/runs"))
        .json(&json!({"input":"fail context", "stream":true}))
        .send()
        .await
        .expect("HTTP");
    assert!(!response.status().is_success());
    let request = state
        .admission
        .requests()
        .await
        .expect("requests")
        .pop()
        .expect("request");
    let snapshot = state
        .runtime
        .get_run(request.run_id)
        .await
        .expect("read")
        .expect("durable failure");
    assert!(snapshot.is_terminal());
    assert_eq!(
        snapshot.execution_manifest.expect("manifest")["profile"],
        "operator_v1_tools"
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    assert_eq!(tools.calls.load(Ordering::SeqCst), 0);
    server.abort();
    let _ = server.await;
    worker.abort();
    let _ = worker.await;
}
struct Model {
    calls: AtomicUsize,
    release: Arc<Notify>,
}
impl ModelPort for Model {
    fn stream(&self, request: ModelRequest) -> ModelEventStream {
        let round = self.calls.fetch_add(1, Ordering::SeqCst);
        let release = self.release.clone();
        Box::pin(async_stream::stream! {
            if round == 0 {
                assert!(request.tools.iter().any(|tool| tool.name == "action"));
                yield ModelEvent::TextDelta { delta: "start".into() };
                yield ModelEvent::ToolCallStarted { call_id: "call".into(), name: "action".into() };
                yield ModelEvent::ToolCallArgumentsDelta { call_id: "call".into(), delta: "{}".into() };
                yield ModelEvent::Completed { finish_reason: FinishReason::ToolCall };
            } else {
                assert_eq!(round, 1, "no duplicate model invocation");
                yield ModelEvent::TextDelta { delta: "done".into() };
                release.notified().await;
                yield ModelEvent::Completed { finish_reason: FinishReason::Stop };
            }
        })
    }
}
struct Tools {
    suspend: bool,
    calls: AtomicUsize,
}

struct LegacyMustNotReceive;
impl DeliveryRouter for LegacyMustNotReceive {
    fn deliver(&self, _: Delivery, _: EventEnvelope) -> EventFuture<'_, ()> {
        panic!("operator wait must not be double-delivered to legacy")
    }
}
impl ToolPort for Tools {
    fn definitions(&self) -> Vec<ToolDefinition> {
        vec![
            ToolDefinition::new("action", "fake", json!({"type":"object"}))
                .with_risk_level(if self.suspend {
                    ToolRiskLevel::Low
                } else {
                    ToolRiskLevel::High
                })
                .with_execution_policy(ToolExecutionPolicy::idempotent().with_completion(
                    if self.suspend {
                        ToolCompletion::MaySuspend
                    } else {
                        ToolCompletion::Immediate
                    },
                )),
        ]
    }
    fn call(&self, request: ToolCallRequest) -> ToolCallFuture {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let suspend = self.suspend;
        Box::pin(async move {
            if !suspend {
                return Ok(ToolOutput::text("tool done"));
            }
            use agent_core::event_runtime::*;
            Ok(ToolOutput::suspend(
                vec![WaitSpec {
                    wait_key: "external".into(),
                    subscription: CreateSubscription {
                        subscription_id: SubscriptionId::stable(
                            "http-wait",
                            &request.run_id.to_string(),
                        ),
                        owner: SubscriptionOwner::Run {
                            run_id: request.run_id,
                        },
                        scope: SubscriptionScope::Run {
                            run_id: request.run_id,
                        },
                        filter: EventFilter {
                            topics: vec!["external.completed".into()],
                            sources: vec![EventSource::Gateway],
                            correlation_id: Some(request.run_id.to_string()),
                            ..Default::default()
                        },
                        delivery: DeliveryTarget::WakeRun {
                            run_id: request.run_id,
                            wait_key: "external".into(),
                        },
                        mode: SubscriptionMode::Once,
                        start_position: StartPosition::Now,
                        expires_at_ms: None,
                        max_deliveries: Some(1),
                        created_at_ms: 0,
                    },
                }],
                vec![],
            ))
        })
    }
}

async fn fixture(
    path: &std::path::Path,
    model: Arc<dyn ModelPort>,
    tools: Arc<Tools>,
) -> (AppState, tokio::task::JoinHandle<()>) {
    let mut state = tests::test_state();
    state.blobs = Arc::new(FilesystemBlobStore::new(path.join("blobs")));
    let runs = Arc::new(
        SqliteRunStore::open(path.join("runs.db"))
            .await
            .expect("runs"),
    );
    let native = Arc::new(
        SqliteEventStore::open(path.join("events.db"), "p4-http")
            .await
            .expect("events"),
    );
    let hook: Arc<dyn ObservationHook> = Arc::new(TracingObservationHook);
    let (legacy, _, _) = build_agent(
        &state.config,
        state.approvals.clone(),
        Arc::new(HeuristicTokenEstimator),
        hook.clone(),
        state.blobs.clone(),
    )
    .expect("construct legacy, never execute");
    state.runtime = RunRuntime::new(
        Harness::new(ObservedMachine::new(legacy, hook)),
        runs.clone(),
        |_| {},
    );
    state.admission = Arc::new(AdmissionFacade::new(
        native.clone(),
        state.blobs.clone(),
        Arc::new(Prepared),
    ));
    let context_worker = state.admission.start_worker();
    state.pipeline_profile = crate::tools_pipeline::EngineProfile::Tools;
    Arc::get_mut(&mut state.orchestration)
        .expect("unique orchestration")
        .tools = tools.definitions();
    let runtime = Arc::new(
        ToolsPipelineRuntime::new(native.clone(), model, tools)
            .with_blobs(state.blobs.clone())
            .with_wait_ports(native.clone(), native.clone())
            .with_projection(runs.clone()),
    );
    state.tools_pipeline = Some(Arc::new(crate::tools_pipeline::ToolsServerPipeline::new(
        runtime,
        runs.clone(),
    )));
    let bridge = Arc::new(EffectWaitBridge::new(
        native.clone(),
        native.clone(),
        native.clone(),
    ));
    state.events = Arc::new(EventRuntime::new(
        native,
        Arc::new(crate::tools_pipeline::EngineDeliveryRouter::new(
            runs,
            Arc::new(LegacyMustNotReceive),
            bridge,
        )),
        EventRuntimeConfig::default(),
    ));
    (state, context_worker)
}

#[path = "p6_tests.rs"]
mod p6;

fn parse_frame(frame: &str) -> Option<Value> {
    let data = frame.lines().find_map(|line| line.strip_prefix("data: "))?;
    let event: Value = serde_json::from_str(data).expect("SSE JSON");
    let id: u64 = frame
        .lines()
        .find_map(|line| line.strip_prefix("id: "))
        .expect("id")
        .parse()
        .expect("numeric id");
    assert_eq!(event["seq"], id);
    assert_eq!(
        frame
            .lines()
            .find_map(|line| line.strip_prefix("event: "))
            .expect("event"),
        event["type"].as_str().expect("type")
    );
    Some(event)
}

#[tokio::test]
async fn p5_slow_sse_repairs_lag_and_disconnect_replays_numeric_cursor() {
    let directory = tempfile::tempdir().expect("temp");
    let model = Arc::new(Model {
        calls: AtomicUsize::new(0),
        release: Arc::new(Notify::new()),
    });
    let tools = Arc::new(Tools {
        suspend: false,
        calls: AtomicUsize::new(0),
    });
    let (state, worker) = fixture(directory.path(), model.clone(), tools.clone()).await;
    let run_id = RunId::new();
    let store = state.runtime.run_store();
    store
        .create_run(RunSnapshot::new(run_id, "slow", 0))
        .await
        .expect("run");
    store
        .append_event(RunEvent::new(run_id, 1, RunEventKind::RunStarted), 0)
        .await
        .expect("start");
    let (sender, receiver) = broadcast::channel(2);
    let response = run_event_response(state.runtime.clone(), run_id, 0, true, Some(receiver));
    let mut body = response.into_body().into_data_stream();
    let first = body.next().await.expect("first").expect("body");
    let first = String::from_utf8(first.to_vec()).expect("UTF8");
    assert_eq!(parse_frame(first.trim()).expect("frame")["seq"], 1);
    let mut events = Vec::new();
    for sequence in 2..=1025 {
        events.push(RunEvent::new(
            run_id,
            sequence,
            RunEventKind::OutputDelta {
                channel: OutputChannel::AssistantText,
                delta: "x".into(),
            },
        ));
    }
    events.push(RunEvent::new(
        run_id,
        1026,
        RunEventKind::RunCompleted {
            finish_reason: FinishReason::Stop,
        },
    ));
    store
        .append_events(
            events
                .iter()
                .cloned()
                .map(|event| ObservedRunEvent::new(event, 1))
                .collect(),
        )
        .await
        .expect("producer commits while consumer does not poll");
    for event in events {
        sender.send(event).expect("receiver still exists");
    }
    assert_eq!(
        store
            .get_run(run_id)
            .await
            .expect("snapshot")
            .expect("run")
            .last_seq,
        1026
    );
    let mut wire = first;
    while let Some(chunk) = body.next().await {
        wire.push_str(std::str::from_utf8(&chunk.expect("chunk")).expect("UTF8"));
    }
    let frames: Vec<_> = wire.split("\n\n").filter_map(parse_frame).collect();
    assert_eq!(frames.len(), 1026);
    for (index, event) in frames.iter().enumerate() {
        assert_eq!(event["seq"], index + 1);
    }
    let (address, server) = serve(state.clone()).await;
    let replay = reqwest::Client::new()
        .get(format!(
            "{address}/api/v1/runs/{run_id}/events?follow=false"
        ))
        .header("Last-Event-ID", "511")
        .send()
        .await
        .expect("reconnect")
        .text()
        .await
        .expect("body");
    assert_eq!(
        replay
            .split("\n\n")
            .filter_map(parse_frame)
            .collect::<Vec<_>>(),
        frames[511..]
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    assert_eq!(tools.calls.load(Ordering::SeqCst), 0);
    server.abort();
    let _ = server.await;
    worker.abort();
    let _ = worker.await;
}

async fn serve(state: AppState) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("local fake server");
    let address = format!("http://{}", listener.local_addr().expect("address"));
    let server = tokio::spawn(async move {
        axum::serve(listener, app_router(state))
            .await
            .expect("server");
    });
    (address, server)
}

async fn until_event(response: reqwest::Response, kind: &str) -> Value {
    let mut bytes = response.bytes_stream();
    let mut pending = String::new();
    tokio::time::timeout(Duration::from_secs(15), async {
        while let Some(chunk) = bytes.next().await {
            pending.push_str(std::str::from_utf8(&chunk.expect("chunk")).expect("UTF8"));
            while let Some(end) = pending.find("\n\n") {
                let frame = pending[..end].to_owned();
                pending.drain(..end + 2);
                if let Some(event) = parse_frame(&frame)
                    && event["type"] == kind
                {
                    return event;
                }
            }
        }
        panic!("stream ended before {kind}");
    })
    .await
    .expect("event deadline")
}

#[tokio::test]
async fn tools_server_reopens_pending_approval_or_wait_and_cancel_only_publishes_fact() {
    use agent_core::{
        effect_pipeline::{RunCancelRequest, run_key},
        operator_fabric::{FabricCommand, FabricReply},
    };
    for suspend in [false, true] {
        let directory = tempfile::tempdir().expect("temp");
        let model = Arc::new(Model {
            calls: AtomicUsize::new(0),
            release: Arc::new(Notify::new()),
        });
        let tools = Arc::new(Tools {
            suspend,
            calls: AtomicUsize::new(0),
        });
        let (state, context_worker) = fixture(directory.path(), model.clone(), tools.clone()).await;
        let (address, server) = serve(state.clone()).await;
        let client = reqwest::Client::new();
        let response = client
            .post(format!("{address}/api/v1/runs"))
            .json(&json!({"input":"recover", "stream":true, "max_steps":3}))
            .send()
            .await
            .expect("create HTTP");
        assert_eq!(response.status(), StatusCode::OK);
        let waiting = until_event(
            response,
            if suspend {
                "run_waiting"
            } else {
                "approval_requested"
            },
        )
        .await;
        let run_id: RunId = waiting["run_id"]
            .as_str()
            .expect("run")
            .parse()
            .expect("id");
        server.abort();
        let _ = server.await;
        state
            .tools_pipeline
            .as_ref()
            .expect("tools")
            .stop_workers()
            .await;
        context_worker.abort();
        let _ = context_worker.await;
        drop(state);

        let (mut state, context_worker) =
            fixture(directory.path(), model.clone(), tools.clone()).await;
        state.pipeline_profile = crate::tools_pipeline::EngineProfile::Legacy;
        admission::recover_once(&state)
            .await
            .expect("recover persisted tools profile");
        state
            .tools_pipeline
            .as_ref()
            .expect("tools")
            .stop_workers()
            .await;
        assert_eq!(model.calls.load(Ordering::SeqCst), 1);
        assert_eq!(tools.calls.load(Ordering::SeqCst), usize::from(suspend));
        let runtime = &state.tools_pipeline.as_ref().expect("tools").runtime;
        let before = runtime.load("local", run_id).await.expect("durable run");
        let (address, server) = serve(state.clone()).await;
        assert_eq!(
            client
                .post(format!("{address}/api/v1/runs/{run_id}/cancel"))
                .send()
                .await
                .expect("cancel HTTP")
                .status(),
            StatusCode::ACCEPTED
        );
        assert_eq!(
            runtime
                .load("local", run_id)
                .await
                .expect("not directly reduced")
                .reducer,
            before.reducer
        );
        let FabricReply::Checkpoint(Some(saved)) = state
            .admission
            .fabric()
            .execute(FabricCommand::ReadCheckpoint(format!(
                "{}:cancel",
                run_key("local", run_id)
            )))
            .await
            .expect("cancel intent")
        else {
            panic!("missing intent");
        };
        let intent: RunCancelRequest =
            serde_json::from_value(saved.value).expect("cancel contract");
        assert_eq!(intent.actor, "gateway:http");
        for _ in 0..30 {
            if runtime
                .tick("local", run_id, agent_harness::unix_time_ms)
                .await
                .expect("reduce cancel")
                == 0
            {
                break;
            }
        }
        assert!(matches!(
            runtime
                .load("local", run_id)
                .await
                .expect("terminal")
                .reducer
                .outcome(),
            AgentLoopOutcome::Cancelled
        ));
        if !suspend {
            let approval = waiting["approval_id"].as_str().expect("approval");
            assert_eq!(
                client
                    .post(format!(
                        "{address}/api/v1/runs/{run_id}/approvals/{approval}"
                    ))
                    .json(&json!({"decision":ApprovalDecision::AllowOnce}))
                    .send()
                    .await
                    .expect("late approval")
                    .status(),
                StatusCode::CONFLICT
            );
        }
        assert_eq!(model.calls.load(Ordering::SeqCst), 1);
        assert_eq!(tools.calls.load(Ordering::SeqCst), usize::from(suspend));
        server.abort();
        let _ = server.await;
        context_worker.abort();
        let _ = context_worker.await;
    }
}

#[tokio::test]
async fn tools_http_sse_approval_and_wait_live_equal_sqlite_replay_and_last_event_id() {
    for suspend in [false, true] {
        let directory = tempfile::tempdir().expect("temp");
        let release = Arc::new(Notify::new());
        let model = Arc::new(Model {
            calls: AtomicUsize::new(0),
            release: release.clone(),
        });
        let tools = Arc::new(Tools {
            suspend,
            calls: AtomicUsize::new(0),
        });
        let (state, context_worker) = fixture(directory.path(), model.clone(), tools.clone()).await;
        let (address, server) = serve(state.clone()).await;
        let client = reqwest::Client::new();
        let response = client
            .post(format!("{address}/api/v1/runs"))
            .json(&json!({"input":"tools", "stream":true, "max_steps":3}))
            .send()
            .await
            .expect("HTTP");
        assert_eq!(response.status(), StatusCode::OK);
        let mut bytes = response.bytes_stream();
        let mut pending = String::new();
        let mut live = Vec::new();
        tokio::time::timeout(Duration::from_secs(20), async {
            while let Some(chunk) = bytes.next().await {
                pending.push_str(std::str::from_utf8(&chunk.expect("chunk")).expect("UTF8"));
                while let Some(end) = pending.find("\n\n") {
                    let frame = pending[..end].to_owned();
                    pending.drain(..end + 2);
                    let Some(event) = parse_frame(&frame) else {
                        continue;
                    };
                    let run = event["run_id"].as_str().expect("run");
                    if event["type"] == "approval_requested" {
                        assert!(!suspend);
                        let approval = event["approval_id"].as_str().expect("approval");
                        for _ in 0..2 {
                            assert_eq!(
                                client
                                    .post(format!(
                                        "{address}/api/v1/runs/{run}/approvals/{approval}"
                                    ))
                                    .json(&json!({"decision":ApprovalDecision::AllowOnce}))
                                    .send()
                                    .await
                                    .expect("approval HTTP")
                                    .status(),
                                StatusCode::OK
                            );
                        }
                    }
                    if event["type"] == "run_waiting" {
                        assert!(suspend);
                        use agent_core::event_runtime::*;
                        let now = agent_harness::unix_time_ms();
                        state
                            .events
                            .publish(PublishEvent {
                                event_id: EventId::stable("http-external", run),
                                topic: "external.completed".into(),
                                event_type: "external.completed".into(),
                                schema_version: 1,
                                source: EventSource::Gateway,
                                subject: Some(format!("run/{run}")),
                                correlation_id: Some(run.into()),
                                causation_id: None,
                                occurred_at_ms: now,
                                recorded_at_ms: now,
                                payload: json!({"ok":true}),
                            })
                            .await
                            .expect("external");
                        loop {
                            if state
                                .events
                                .dispatch_once("http-events", now)
                                .await
                                .expect("dispatch")
                                .completed
                                > 0
                            {
                                break;
                            }
                            tokio::task::yield_now().await;
                        }
                    }
                    if event["type"] == "output_delta" && event["delta"] == "done" {
                        release.notify_one();
                    }
                    let terminal = event["type"] == "run_completed";
                    live.push(event);
                    if terminal {
                        return;
                    }
                }
            }
            panic!("SSE closed before terminal");
        })
        .await
        .expect("live deadline");
        assert_eq!(live.last().expect("terminal")["type"], "run_completed");
        let run_id = live[0]["run_id"].as_str().expect("run").to_owned();
        assert_eq!(tools.calls.load(Ordering::SeqCst), 1);
        assert_eq!(model.calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            live.iter()
                .filter(|event| event["type"] == "run_resumed")
                .count(),
            usize::from(suspend)
        );
        server.abort();
        let _ = server.await;
        state
            .tools_pipeline
            .as_ref()
            .expect("tools")
            .stop_workers()
            .await;
        context_worker.abort();
        let _ = context_worker.await;
        drop(state);
        let (mut reopened, context_worker) = fixture(directory.path(), model.clone(), tools).await;
        reopened.pipeline_profile = crate::tools_pipeline::EngineProfile::Legacy;
        admission::recover_once(&reopened)
            .await
            .expect("recover pinned profile, never legacy");
        let (address, server) = serve(reopened).await;
        for cutoff in 0..live.len() as u64 {
            let replay = client
                .get(format!(
                    "{address}/api/v1/runs/{run_id}/events?follow=false"
                ))
                .header("Last-Event-ID", cutoff.to_string())
                .send()
                .await
                .expect("replay HTTP")
                .text()
                .await
                .expect("replay");
            let frames: Vec<_> = replay.split("\n\n").filter_map(parse_frame).collect();
            assert_eq!(
                frames,
                live.iter()
                    .filter(|event| event["seq"].as_u64().expect("seq") > cutoff)
                    .cloned()
                    .collect::<Vec<_>>()
            );
        }
        assert_eq!(model.calls.load(Ordering::SeqCst), 2);
        server.abort();
        let _ = server.await;
        context_worker.abort();
        let _ = context_worker.await;
    }
}
