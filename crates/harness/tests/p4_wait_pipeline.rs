use agent_core::{event_runtime::*, harness::*};
use agent_extension::store::SqliteEventStore;
use agent_harness::{
    EventRuntime, EventRuntimeConfig, JobRuntime, JobRuntimeConfig,
    pipeline::{EffectWaitBridge, PipelineStart, ToolsPipelineRuntime},
};
use futures_util::stream;
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct Model(AtomicUsize);
impl ModelPort for Model {
    fn stream(&self, request: ModelRequest) -> ModelEventStream {
        let events = if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
            vec![
                ModelEvent::ToolCallStarted {
                    call_id: "wait-call".into(),
                    name: "wait".into(),
                },
                ModelEvent::ToolCallArgumentsDelta {
                    call_id: "wait-call".into(),
                    delta: "{}".into(),
                },
                ModelEvent::Completed {
                    finish_reason: FinishReason::ToolCall,
                },
            ]
        } else {
            let resumed: serde_json::Value =
                serde_json::from_str(&request.messages.last().expect("tool result").content)
                    .expect("resume JSON");
            assert_eq!(resumed["ok"], true);
            assert_eq!(resumed["events"].as_array().expect("events").len(), 1);
            vec![
                ModelEvent::TextDelta {
                    delta: "resumed".into(),
                },
                ModelEvent::Completed {
                    finish_reason: FinishReason::Stop,
                },
            ]
        };
        Box::pin(stream::iter(events))
    }
}

struct WaitTool {
    mode: &'static str,
    calls: AtomicUsize,
}
impl ToolPort for WaitTool {
    fn definitions(&self) -> Vec<ToolDefinition> {
        vec![
            ToolDefinition::new("wait", "wait", json!({"type":"object"})).with_execution_policy(
                ToolExecutionPolicy::idempotent().with_completion(ToolCompletion::MaySuspend),
            ),
        ]
    }
    fn call(&self, request: ToolCallRequest) -> ToolCallFuture {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mode = self.mode;
        Box::pin(async move {
            let job_id = JobId::stable("p4-wait", &request.run_id.to_string());
            let correlation = if mode == "job" {
                job_id.to_string()
            } else {
                request.run_id.to_string()
            };
            let event = notification(mode, request.run_id, &correlation);
            let effects = match mode {
                "job" => vec![EffectRequest::StartJob {
                    command: StartJob {
                        job_id,
                        run_id: request.run_id,
                        kind: "fake".into(),
                        input: json!({"answer":42}),
                        idempotency_key: "wait-call".into(),
                        requested_at_ms: 0,
                    },
                }],
                "timer" => vec![EffectRequest::ScheduleTimer {
                    command: ScheduleOnce {
                        timer_id: TimerId::stable("p4-wait", &request.run_id.to_string()),
                        fire_at_ms: 10,
                        event: event.clone(),
                        created_at_ms: 0,
                    },
                }],
                _ => vec![],
            };
            Ok(ToolOutput::suspend(
                vec![WaitSpec {
                    wait_key: "wait".into(),
                    subscription: CreateSubscription {
                        subscription_id: SubscriptionId::stable(
                            "p4-wait",
                            &request.run_id.to_string(),
                        ),
                        owner: SubscriptionOwner::Run {
                            run_id: request.run_id,
                        },
                        scope: SubscriptionScope::Run {
                            run_id: request.run_id,
                        },
                        filter: EventFilter {
                            topics: vec![event.topic],
                            sources: vec![event.source],
                            correlation_id: Some(correlation),
                            ..Default::default()
                        },
                        delivery: DeliveryTarget::WakeRun {
                            run_id: request.run_id,
                            wait_key: "wait".into(),
                        },
                        mode: SubscriptionMode::Once,
                        start_position: StartPosition::Now,
                        expires_at_ms: None,
                        max_deliveries: Some(1),
                        created_at_ms: 0,
                    },
                }],
                effects,
            ))
        })
    }
}

fn notification(mode: &str, run_id: RunId, correlation: &str) -> PublishEvent {
    PublishEvent {
        event_id: EventId::stable("p4-wait-event", &run_id.to_string()),
        topic: if mode == "job" {
            "job.completed".into()
        } else {
            format!("{mode}.completed")
        },
        event_type: format!("{mode}.completed"),
        schema_version: 1,
        source: match mode {
            "job" => EventSource::JobWorker,
            "timer" => EventSource::Timer,
            _ => EventSource::Gateway,
        },
        subject: Some(format!("run/{run_id}")),
        correlation_id: Some(correlation.into()),
        causation_id: None,
        occurred_at_ms: 10,
        recorded_at_ms: 10,
        payload: json!({"answer":42}),
    }
}

struct Jobs(Arc<AtomicUsize>);
impl JobRouter for Jobs {
    fn execute(&self, job: JobRecord) -> JobExecutionFuture {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { Ok(job.command.input) })
    }
}

async fn drive_to_wait(
    path: &std::path::Path,
    run_id: RunId,
    model: Arc<Model>,
    tools: Arc<WaitTool>,
) {
    let store = Arc::new(SqliteEventStore::open(path, "p4").await.expect("open"));
    let runtime = ToolsPipelineRuntime::new(store.clone(), model, tools)
        .with_wait_ports(store.clone(), store);
    let mut start = PipelineStart::text(run_id, "wait", "fake").start;
    start.config.allowed_tools = None;
    start.config.max_steps = 3;
    runtime.admit("local", start, 0).await.expect("admit");
    for _ in 0..30 {
        if runtime.tick("local", run_id, || 1).await.expect("tick") == 0 {
            break;
        }
    }
    assert!(matches!(
        runtime
            .load("local", run_id)
            .await
            .expect("load")
            .reducer
            .outcome(),
        AgentLoopOutcome::Suspended { .. }
    ));
}

#[tokio::test]
async fn native_completion_crashes_before_notification_ack_and_reopens_without_reexecution() {
    for mode in ["job", "timer"] {
        let directory = tempfile::tempdir().expect("temp");
        let path = directory.path().join("events.db");
        let run_id = RunId::new();
        let model = Arc::new(Model(AtomicUsize::new(0)));
        let tools = Arc::new(WaitTool {
            mode,
            calls: AtomicUsize::new(0),
        });
        let job_calls = Arc::new(AtomicUsize::new(0));
        drive_to_wait(&path, run_id, model.clone(), tools.clone()).await;
        {
            let store = Arc::new(
                SqliteEventStore::open(&path, "p4")
                    .await
                    .expect("open native"),
            );
            let bridge = Arc::new(EffectWaitBridge::new(
                store.clone(),
                store.clone(),
                store.clone(),
            ));
            let events = Arc::new(EventRuntime::new(
                store.clone(),
                bridge.clone(),
                EventRuntimeConfig::default(),
            ));
            if mode == "job" {
                let jobs = JobRuntime::new(
                    store.clone(),
                    bridge.job_router(Arc::new(Jobs(job_calls.clone()))),
                    events.clone(),
                    JobRuntimeConfig::default(),
                );
                assert_eq!(
                    jobs.execute_once("before-crash", 10)
                        .await
                        .expect("execute")
                        .completed,
                    1
                );
                let pending = store
                    .claim_notifications(ClaimJobNotifications {
                        worker_id: "before-crash".into(),
                        now_ms: 11,
                        lease_until_ms: 12,
                        limit: 1,
                    })
                    .await
                    .expect("claim notification");
                let job = pending.first().expect("notification");
                events.publish(PublishEvent {
                    event_id: EventId::stable("job-result", &job.command.job_id.to_string()), topic: "job.completed".into(), event_type: "job.completed".into(), schema_version: 1,
                    source: EventSource::JobWorker, subject: Some(format!("run/{run_id}")), correlation_id: Some(job.command.job_id.to_string()), causation_id: None,
                    occurred_at_ms: 10, recorded_at_ms: 11, payload: json!({"job_id":job.command.job_id,"kind":job.command.kind,"outcome":job.outcome}),
                }).await.expect("publish before notification ACK");
            } else {
                let timers = store
                    .claim_due_timers(ClaimTimers {
                        worker_id: "before-crash".into(),
                        now_ms: 10,
                        lease_until_ms: 11,
                        limit: 1,
                    })
                    .await
                    .expect("claim");
                assert_eq!(timers.len(), 1);
                events
                    .publish(timers[0].definition.event.clone())
                    .await
                    .expect("publish before timer ACK");
            }
        }
        assert_eq!(model.0.load(Ordering::SeqCst), 1);
        {
            let store = Arc::new(
                SqliteEventStore::open(&path, "p4")
                    .await
                    .expect("reopen notification"),
            );
            let bridge = Arc::new(EffectWaitBridge::new(
                store.clone(),
                store.clone(),
                store.clone(),
            ));
            let events = Arc::new(EventRuntime::new(
                store.clone(),
                bridge.clone(),
                EventRuntimeConfig::default(),
            ));
            if mode == "job" {
                let jobs = JobRuntime::new(
                    store,
                    bridge.job_router(Arc::new(Jobs(job_calls.clone()))),
                    events.clone(),
                    JobRuntimeConfig::default(),
                );
                assert_eq!(
                    jobs.execute_once("after-crash", 12)
                        .await
                        .expect("no duplicate job")
                        .completed,
                    0
                );
                assert_eq!(
                    jobs.publish_notifications_once("after-crash", 12)
                        .await
                        .expect("notification")
                        .published,
                    1
                );
                assert_eq!(job_calls.load(Ordering::SeqCst), 1);
            } else {
                assert_eq!(
                    events
                        .fire_timers_once("after-crash", 12)
                        .await
                        .expect("timer retry")
                        .completed,
                    1
                );
            }
            assert_eq!(
                events
                    .dispatch_once("wake", 13)
                    .await
                    .expect("wake")
                    .completed,
                1
            );
            assert_eq!(model.0.load(Ordering::SeqCst), 1);
        }
        let store = Arc::new(
            SqliteEventStore::open(&path, "p4")
                .await
                .expect("reopen coordinator"),
        );
        let runtime = ToolsPipelineRuntime::new(store.clone(), model.clone(), tools.clone())
            .with_wait_ports(store.clone(), store);
        for _ in 0..30 {
            if runtime.tick("local", run_id, || 14).await.expect("resume") == 0 {
                break;
            }
        }
        assert!(matches!(
            runtime
                .load("local", run_id)
                .await
                .expect("load")
                .reducer
                .outcome(),
            AgentLoopOutcome::Complete { .. }
        ));
        assert_eq!(model.0.load(Ordering::SeqCst), 2);
        assert_eq!(tools.calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn claimed_external_delivery_after_cancel_is_audited_but_does_not_resume() {
    use agent_core::operator_fabric::{FabricCommand, FabricReply, OperatorFabric};
    let directory = tempfile::tempdir().expect("temp");
    let path = directory.path().join("events.db");
    let run_id = RunId::new();
    let model = Arc::new(Model(AtomicUsize::new(0)));
    let tools = Arc::new(WaitTool {
        mode: "external",
        calls: AtomicUsize::new(0),
    });
    drive_to_wait(&path, run_id, model.clone(), tools.clone()).await;
    let (delivery, event, wait_key) = {
        let store = Arc::new(SqliteEventStore::open(&path, "p4").await.expect("open"));
        let runtime = ToolsPipelineRuntime::new(store.clone(), model.clone(), tools.clone())
            .with_wait_ports(store.clone(), store.clone());
        let wait = runtime
            .load("local", run_id)
            .await
            .expect("load")
            .wait
            .expect("wait");
        let event = EventStore::publish(
            store.as_ref(),
            notification("external", run_id, &run_id.to_string()),
        )
        .await
        .expect("publish")
        .event;
        let delivery = store
            .claim_deliveries(ClaimDeliveries {
                worker_id: "inflight".into(),
                now_ms: 10,
                lease_until_ms: 100,
                limit: 1,
            })
            .await
            .expect("claim")
            .pop()
            .expect("delivery");
        runtime
            .request_cancel("local", run_id, "gateway:test", 11)
            .await
            .expect("cancel");
        for _ in 0..20 {
            if runtime
                .tick("local", run_id, || 12)
                .await
                .expect("cancel tick")
                == 0
            {
                break;
            }
        }
        (delivery, event, wait.key())
    };
    let store = Arc::new(
        SqliteEventStore::open(&path, "p4")
            .await
            .expect("reopen late"),
    );
    let bridge = EffectWaitBridge::new(store.clone(), store.clone(), store.clone());
    bridge
        .deliver(delivery.clone(), event.clone())
        .await
        .expect("late fact");
    bridge
        .deliver(delivery, event)
        .await
        .expect("duplicate late fact");
    assert!(matches!(
        store
            .execute(FabricCommand::ReadCheckpoint(format!("{wait_key}:wake")))
            .await
            .expect("audit"),
        FabricReply::Checkpoint(Some(_))
    ));
    let runtime = ToolsPipelineRuntime::new(store.clone(), model.clone(), tools)
        .with_wait_ports(store.clone(), store);
    for _ in 0..20 {
        if runtime
            .tick("local", run_id, || 13)
            .await
            .expect("late tick")
            == 0
        {
            break;
        }
    }
    let run = runtime.load("local", run_id).await.expect("load");
    assert!(matches!(run.reducer.outcome(), AgentLoopOutcome::Cancelled));
    assert!(
        !run.public_events
            .iter()
            .any(|event| matches!(event.kind, RunEventKind::RunResumed { .. }))
    );
    assert_eq!(model.0.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn may_suspend_job_timer_external_reopen_publish_facts_then_coordinator_resumes() {
    use agent_core::operator_fabric::{FabricCommand, FabricReply, OperatorFabric};
    for mode in ["job", "timer", "external"] {
        let directory = tempfile::tempdir().expect("temp");
        let path = directory.path().join("events.db");
        let model = Arc::new(Model(AtomicUsize::new(0)));
        let tools = Arc::new(WaitTool {
            mode,
            calls: AtomicUsize::new(0),
        });
        let run_id = RunId::new();
        {
            let store = Arc::new(SqliteEventStore::open(&path, "p4").await.expect("open"));
            let runtime = ToolsPipelineRuntime::new(store.clone(), model.clone(), tools.clone())
                .with_wait_ports(store.clone(), store);
            let mut start = PipelineStart::text(run_id, "wait", "fake").start;
            start.config.allowed_tools = None;
            start.config.max_steps = 3;
            start.config.tool_timeout_ms = 1000;
            runtime.admit("local", start, 0).await.expect("admit");
            for _ in 0..20 {
                runtime.tick("local", run_id, || 1).await.expect("tick");
                if matches!(
                    runtime
                        .load("local", run_id)
                        .await
                        .expect("state")
                        .reducer
                        .outcome(),
                    AgentLoopOutcome::Suspended { .. }
                ) {
                    break;
                }
            }
            assert!(matches!(
                runtime
                    .load("local", run_id)
                    .await
                    .expect("state")
                    .reducer
                    .outcome(),
                AgentLoopOutcome::Suspended { .. }
            ));
        }
        let store = Arc::new(
            SqliteEventStore::open(&path, "p4")
                .await
                .expect("reopen wait"),
        );
        if mode == "external" {
            EventStore::publish(
                store.as_ref(),
                notification(mode, run_id, &run_id.to_string()),
            )
            .await
            .expect("notification before registration");
        }
        let runtime = ToolsPipelineRuntime::new(store.clone(), model.clone(), tools.clone())
            .with_wait_ports(store.clone(), store.clone());
        for _ in 0..10 {
            if runtime.tick("local", run_id, || 2).await.expect("register") == 0 {
                break;
            }
        }
        assert_eq!(
            runtime
                .tick("local", run_id, || 3)
                .await
                .expect("idle suspended"),
            0
        );
        let bridge = Arc::new(EffectWaitBridge::new(
            store.clone(),
            store.clone(),
            store.clone(),
        ));
        let events = Arc::new(EventRuntime::new(
            store.clone(),
            bridge.clone(),
            EventRuntimeConfig::default(),
        ));
        let job_calls = Arc::new(AtomicUsize::new(0));
        match mode {
            "job" => {
                let jobs = JobRuntime::new(
                    store.clone(),
                    bridge.job_router(Arc::new(Jobs(job_calls.clone()))),
                    events.clone(),
                    JobRuntimeConfig::default(),
                );
                jobs.execute_once("job-worker", 10).await.expect("job");
                jobs.publish_notifications_once("notify-worker", 11)
                    .await
                    .expect("publish");
                jobs.publish_notifications_once("notify-worker", 12)
                    .await
                    .expect("duplicate publish");
                assert_eq!(job_calls.load(Ordering::SeqCst), 1);
            }
            "timer" => {
                assert_eq!(
                    events
                        .fire_timers_once("timer-worker", 10)
                        .await
                        .expect("fire")
                        .completed,
                    1
                );
                assert_eq!(
                    events
                        .fire_timers_once("timer-worker", 11)
                        .await
                        .expect("duplicate")
                        .completed,
                    0
                );
            }
            _ => {
                let event = notification(mode, run_id, &run_id.to_string());
                events.publish(event.clone()).await.expect("external");
                events.publish(event).await.expect("duplicate external");
            }
        }
        assert_eq!(model.0.load(Ordering::SeqCst), 1);
        events
            .dispatch_once("event-worker", 12)
            .await
            .expect("deliver fact");
        assert_eq!(model.0.load(Ordering::SeqCst), 1);
        drop(events);
        drop(bridge);
        drop(runtime);
        drop(store);
        let store = Arc::new(
            SqliteEventStore::open(&path, "p4")
                .await
                .expect("reopen inbox"),
        );
        let runtime = ToolsPipelineRuntime::new(store.clone(), model.clone(), tools.clone())
            .with_wait_ports(store.clone(), store.clone());
        for _ in 0..25 {
            if runtime.tick("local", run_id, || 13).await.expect("resume") == 0 {
                break;
            }
        }
        let state = runtime.load("local", run_id).await.expect("state");
        let route = state
            .routes
            .values()
            .find(|route| route.consumer == "wait")
            .expect("persisted wait route");
        let kinds: &[&str] = match mode {
            "job" => &[
                "job.requested",
                "job.started",
                "job.completed",
                "wait.resolved",
            ],
            "timer" => &[
                "timer.schedule.requested",
                "timer.scheduled",
                "timer.fired",
                "wait.resolved",
            ],
            _ => &["wait.resolved"],
        };
        for kind in kinds {
            let FabricReply::Replay(events) = store
                .execute(FabricCommand::ScanEvents {
                    topic: route.topic.clone(),
                    event_type: (*kind).into(),
                    after: 0,
                    limit: 10,
                })
                .await
                .expect("facts")
            else {
                panic!("scan reply");
            };
            assert_eq!(events.len(), 1, "{kind}");
            assert_eq!(events[0].event.envelope.operator.name, "wait_bridge");
        }
        assert!(matches!(
            state.reducer.outcome(),
            AgentLoopOutcome::Complete { .. }
        ));
        assert_eq!(model.0.load(Ordering::SeqCst), 2);
        assert_eq!(tools.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            state
                .public_events
                .iter()
                .filter(|event| matches!(event.kind, RunEventKind::RunWaiting { .. }))
                .count(),
            1
        );
        assert_eq!(
            state
                .public_events
                .iter()
                .filter(|event| matches!(event.kind, RunEventKind::RunResumed { .. }))
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn cancelled_suspended_run_reopens_and_cancels_native_work_without_resuming() {
    for mode in ["job", "timer", "external"] {
        let directory = tempfile::tempdir().expect("temp");
        let path = directory.path().join("events.db");
        let model = Arc::new(Model(AtomicUsize::new(0)));
        let tools = Arc::new(WaitTool {
            mode,
            calls: AtomicUsize::new(0),
        });
        let run_id = RunId::new();
        {
            let store = Arc::new(SqliteEventStore::open(&path, "p4").await.expect("open"));
            let runtime = ToolsPipelineRuntime::new(store.clone(), model.clone(), tools.clone())
                .with_wait_ports(store.clone(), store.clone());
            let mut start = PipelineStart::text(run_id, "wait", "fake").start;
            start.config.allowed_tools = None;
            start.config.max_steps = 3;
            runtime.admit("local", start, 0).await.expect("admit");
            for _ in 0..30 {
                if runtime.tick("local", run_id, || 1).await.expect("tick") == 0 {
                    break;
                }
            }
            assert!(matches!(
                runtime
                    .load("local", run_id)
                    .await
                    .expect("load")
                    .reducer
                    .outcome(),
                AgentLoopOutcome::Suspended { .. }
            ));
            runtime
                .request_cancel("local", run_id, "gateway:test", 2)
                .await
                .expect("durable cancel");
        }
        let store = Arc::new(SqliteEventStore::open(&path, "p4").await.expect("reopen"));
        let runtime = ToolsPipelineRuntime::new(store.clone(), model.clone(), tools.clone())
            .with_wait_ports(store.clone(), store.clone());
        for _ in 0..30 {
            if runtime
                .tick("local", run_id, || 3)
                .await
                .expect("cancel tick")
                == 0
            {
                break;
            }
        }
        let subscription = store
            .get_subscription(SubscriptionId::stable("p4-wait", &run_id.to_string()))
            .await
            .expect("subscription")
            .expect("exists");
        assert_eq!(subscription.status, SubscriptionStatus::Cancelled);
        if mode == "job" {
            assert_eq!(
                JobStore::get(
                    store.as_ref(),
                    JobId::stable("p4-wait", &run_id.to_string())
                )
                .await
                .expect("job")
                .expect("exists")
                .status,
                JobStatus::Cancelled
            );
        }
        let events = EventRuntime::new(
            store.clone(),
            Arc::new(EffectWaitBridge::new(
                store.clone(),
                store.clone(),
                store.clone(),
            )),
            EventRuntimeConfig::default(),
        );
        assert_eq!(
            events
                .fire_timers_once("timer", 100)
                .await
                .expect("timers")
                .completed,
            0
        );
        let event = notification(mode, run_id, &run_id.to_string());
        events.publish(event).await.expect("late");
        events.dispatch_once("late", 100).await.expect("dispatch");
        for _ in 0..10 {
            if runtime
                .tick("local", run_id, || 101)
                .await
                .expect("late tick")
                == 0
            {
                break;
            }
        }
        assert!(matches!(
            runtime
                .load("local", run_id)
                .await
                .expect("load")
                .reducer
                .outcome(),
            AgentLoopOutcome::Cancelled
        ));
        assert_eq!(model.0.load(Ordering::SeqCst), 1);
    }
}
