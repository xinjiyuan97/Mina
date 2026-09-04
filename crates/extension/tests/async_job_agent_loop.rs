use std::{sync::Arc, time::SystemTime};

use agent_core::{
    event_runtime::{
        Delivery, DeliveryRouter, DeliveryTarget, EventEnvelope, EventError, EventFuture, EventId,
        EventSource, EventStore, PublishEvent,
    },
    harness::{
        ActivationId, AgentEvent, AgentLoop, AgentMachine, ApprovalResolution, CompleteFlowRun,
        ContinueFlowRun, EffectRequest, FinishReason, FlowEffect, FlowEffectRouter, FlowError,
        FlowRunState, FlowRunStatus, FlowStore, JobExecutionFuture, JobRecord, JobRouter, JobStore,
        MachineOutput, MachineResumeRequest, MachineStartRequest, ModelEvent, ModelEventStream,
        ModelPort, ModelRequest, RunCancellation, RunId, StartJob, StepOutcome, SuspendFlowRun,
        ToolRegistry, WakeFlowRun,
    },
};
use agent_extension::{store::SqliteEventStore, tool::AsyncJobTool};
use agent_harness::{
    EventRuntime, EventRuntimeConfig, FlowEffectRuntime, FlowEffectRuntimeConfig, JobRuntime,
    JobRuntimeConfig,
};
use futures_util::{StreamExt, stream};
use serde_json::{Value, json};
use tempfile::tempdir;

struct JobProvider {
    calls: Arc<std::sync::atomic::AtomicUsize>,
}

impl ModelPort for JobProvider {
    fn stream(&self, request: ModelRequest) -> ModelEventStream {
        let call = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if call == 0 {
            return Box::pin(stream::iter(vec![
                ModelEvent::Accepted {
                    provider_request_id: None,
                },
                ModelEvent::ToolCallStarted {
                    call_id: "call_async_job".into(),
                    name: "async_job".into(),
                },
                ModelEvent::ToolCallArgumentsDelta {
                    call_id: "call_async_job".into(),
                    delta: json!({
                        "kind": "builtin.delay",
                        "input": {"value": {"answer": 42}},
                        "idempotency_key": "restart-safe-job"
                    })
                    .to_string(),
                },
                ModelEvent::Completed {
                    finish_reason: FinishReason::ToolCall,
                },
            ]));
        }

        assert_eq!(call, 1);
        let result: Value = serde_json::from_str(
            &request
                .messages
                .last()
                .expect("tool result should be present")
                .content,
        )
        .expect("tool result should be JSON");
        assert_eq!(result["events"][0]["topic"], "job.completed");
        assert_eq!(
            result["events"][0]["payload"]["outcome"]["value"]["answer"],
            42
        );
        Box::pin(stream::iter(vec![
            ModelEvent::Accepted {
                provider_request_id: None,
            },
            ModelEvent::TextDelta {
                delta: "job finished after restart".into(),
            },
            ModelEvent::Completed {
                finish_reason: FinishReason::Stop,
            },
        ]))
    }
}

struct WakeRouter {
    flows: Arc<dyn FlowStore>,
}

impl DeliveryRouter for WakeRouter {
    fn deliver(&self, delivery: Delivery, event: EventEnvelope) -> EventFuture<'_, ()> {
        Box::pin(async move {
            let DeliveryTarget::WakeRun { run_id, .. } = delivery.target else {
                return Err(EventError::invalid("test router only supports WakeRun"));
            };
            self.flows
                .wake(WakeFlowRun {
                    run_id,
                    subscription_id: delivery.delivery_id.subscription_id,
                    event_id: delivery.delivery_id.event_id,
                    woken_at_ms: event.event.recorded_at_ms,
                })
                .await
                .map(|_| ())
                .map_err(flow_to_event)
        })
    }
}

struct EchoJobRouter {
    calls: Arc<std::sync::atomic::AtomicUsize>,
}

impl JobRouter for EchoJobRouter {
    fn execute(&self, job: JobRecord) -> JobExecutionFuture {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::pin(async move { Ok(job.command.input["value"].clone()) })
    }
}

struct TestFlowEffectRouter {
    events: Arc<EventRuntime>,
    jobs: Arc<JobRuntime>,
}

impl FlowEffectRouter for TestFlowEffectRouter {
    fn execute(&self, effect: FlowEffect) -> agent_core::harness::FlowFuture<'_, ()> {
        Box::pin(async move {
            match effect.request {
                EffectRequest::PublishEvent { command } => self
                    .events
                    .publish(command)
                    .await
                    .map(|_| ())
                    .map_err(event_to_flow),
                EffectRequest::ScheduleTimer { command } => self
                    .events
                    .schedule_once(command)
                    .await
                    .map(|_| ())
                    .map_err(event_to_flow),
                EffectRequest::StartJob { command } => self
                    .jobs
                    .submit(command)
                    .await
                    .map(|_| ())
                    .map_err(|error| FlowError::Backend(error.to_string())),
            }
        })
    }
}

fn build_agent(
    provider_calls: Arc<std::sync::atomic::AtomicUsize>,
) -> AgentLoop<JobProvider, ToolRegistry> {
    let mut tools = ToolRegistry::new();
    tools
        .register(AsyncJobTool::new(["builtin.delay".into()]))
        .expect("async job tool should register");
    AgentLoop::new(
        JobProvider {
            calls: provider_calls,
        },
        tools,
        "test-model",
        "system",
        None,
        4,
    )
}

fn build_runtimes(
    adapter: &Arc<SqliteEventStore>,
    job_calls: Arc<std::sync::atomic::AtomicUsize>,
) -> (
    Arc<dyn FlowStore>,
    Arc<EventRuntime>,
    Arc<JobRuntime>,
    Arc<FlowEffectRuntime>,
) {
    let flows: Arc<dyn FlowStore> = adapter.clone();
    let event_store: Arc<dyn EventStore> = adapter.clone();
    let job_store: Arc<dyn JobStore> = adapter.clone();
    let events = Arc::new(EventRuntime::new(
        event_store,
        Arc::new(WakeRouter {
            flows: Arc::clone(&flows),
        }),
        EventRuntimeConfig::default(),
    ));
    let jobs = Arc::new(JobRuntime::new(
        job_store,
        Arc::new(EchoJobRouter { calls: job_calls }),
        Arc::clone(&events),
        JobRuntimeConfig::default(),
    ));
    let effects = Arc::new(FlowEffectRuntime::new(
        Arc::clone(&flows),
        Arc::new(TestFlowEffectRouter {
            events: Arc::clone(&events),
            jobs: Arc::clone(&jobs),
        }),
        FlowEffectRuntimeConfig::default(),
    ));
    (flows, events, jobs, effects)
}

async fn collect_machine(
    mut stream: agent_core::harness::MachineStream,
) -> (Vec<AgentEvent>, StepOutcome) {
    let mut events = Vec::new();
    let mut outcome = None;
    while let Some(output) = stream.next().await {
        match output {
            MachineOutput::Event(event) => events.push(event),
            MachineOutput::Yield(step) => outcome = Some(step),
            _ => panic!("unsupported machine output"),
        }
    }
    (
        events,
        outcome.expect("machine activation should yield an outcome"),
    )
}

#[tokio::test]
async fn async_job_survives_store_and_agent_reconstruction_then_resumes_once() {
    let directory = tempdir().expect("temporary directory should exist");
    let path = directory.path().join("async-job-agent.sqlite3");
    let provider_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let job_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let run_id = RunId::new();
    let cancellation = RunCancellation::new();
    let base = unix_time_ms();

    let adapter = Arc::new(
        SqliteEventStore::open(&path, "async-job-agent:test")
            .await
            .expect("event store should open"),
    );
    let (flows, events, _jobs, effects) = build_runtimes(&adapter, Arc::clone(&job_calls));
    let agent = build_agent(Arc::clone(&provider_calls));
    let start = MachineStartRequest {
        run_id,
        input: "run a background job".into(),
        attachments: Vec::new(),
        prior_messages: Vec::new(),
        allowed_tools: None,
        allow_run_adf: false,
        max_steps: None,
        context_fingerprint: None,
        activated_at_ms: base,
        cancellation: cancellation.clone(),
    };
    flows
        .create(FlowRunState {
            run_id,
            revision: 0,
            status: FlowRunStatus::Runnable,
            activation_id: None,
            checkpoint: agent
                .initial_checkpoint(&start)
                .expect("initial checkpoint should encode"),
            wait_subscription_ids: Vec::new(),
            lease_owner: None,
            lease_until_ms: None,
            updated_at_ms: base,
        })
        .await
        .expect("flow should be created");

    let first = flows
        .claim_runnable("agent-1".into(), base, base + 10_000, 1)
        .await
        .expect("flow should claim")
        .pop()
        .expect("one activation should exist");
    let (first_events, first_outcome) = collect_machine(agent.start(start)).await;
    let approval_id = first_events
        .iter()
        .find_map(|event| match event {
            AgentEvent::ApprovalRequested { approval_id, .. } => Some(*approval_id),
            _ => None,
        })
        .expect("medium-risk async job should request approval");
    let StepOutcome::Suspend {
        checkpoint,
        waits,
        effects: requested_effects,
    } = first_outcome
    else {
        panic!("approval should suspend the agent");
    };
    flows
        .suspend(SuspendFlowRun {
            run_id,
            activation_id: first
                .state
                .activation_id
                .expect("activation id should exist"),
            expected_revision: first.state.revision,
            checkpoint,
            waits,
            effects: requested_effects,
            suspended_at_ms: base + 1,
        })
        .await
        .expect("approval wait should persist");
    effects
        .dispatch_once("effect-approval", base + 2)
        .await
        .expect("approval request effect should dispatch");

    events
        .publish(PublishEvent {
            event_id: EventId::stable("test-approval", &approval_id.to_string()),
            topic: "tool.approval.resolved".into(),
            event_type: "tool.approval.resolved".into(),
            schema_version: 1,
            source: EventSource::Gateway,
            subject: Some(format!("run/{run_id}")),
            correlation_id: Some(approval_id.to_string()),
            causation_id: None,
            occurred_at_ms: base + 3,
            recorded_at_ms: base + 3,
            payload: serde_json::to_value(ApprovalResolution::allow_once())
                .expect("approval should encode"),
        })
        .await
        .expect("approval should publish");
    assert_eq!(
        events
            .dispatch_once("event-approval", base + 4)
            .await
            .expect("approval should deliver")
            .completed,
        1
    );

    let approved = flows
        .claim_runnable("agent-2".into(), base + 5, base + 10_005, 1)
        .await
        .expect("approved flow should claim")
        .pop()
        .expect("approved activation should exist");
    let (_, approved_outcome) = collect_machine(
        agent.resume(MachineResumeRequest {
            run_id,
            activation_id: approved
                .state
                .activation_id
                .unwrap_or_else(ActivationId::new),
            checkpoint: approved.state.checkpoint.clone(),
            inbox: approved.inbox,
            activated_at_ms: base + 5,
            cancellation: cancellation.clone(),
        }),
    )
    .await;
    let StepOutcome::Suspend {
        checkpoint,
        waits,
        effects: job_effects,
    } = approved_outcome
    else {
        panic!("async job tool should suspend after approval");
    };
    assert!(matches!(
        job_effects.as_slice(),
        [EffectRequest::StartJob {
            command: StartJob { kind, .. }
        }] if kind == "builtin.delay"
    ));
    flows
        .suspend(SuspendFlowRun {
            run_id,
            activation_id: approved
                .state
                .activation_id
                .expect("activation id should exist"),
            expected_revision: approved.state.revision,
            checkpoint,
            waits,
            effects: job_effects,
            suspended_at_ms: base + 6,
        })
        .await
        .expect("job suspension should persist");

    // Simulate a process restart after the checkpoint/outbox commit but before
    // the StartJob effect has been dispatched.
    drop(agent);
    drop(effects);
    drop(events);
    drop(flows);
    drop(adapter);

    let adapter = Arc::new(
        SqliteEventStore::open(&path, "async-job-agent:reopened")
            .await
            .expect("event store should reopen"),
    );
    let (flows, events, jobs, effects) = build_runtimes(&adapter, Arc::clone(&job_calls));
    let agent = build_agent(Arc::clone(&provider_calls));
    let now = unix_time_ms().saturating_add(10);
    assert_eq!(
        effects
            .dispatch_once("effect-job", now)
            .await
            .expect("recovered StartJob effect should dispatch")
            .completed,
        1
    );
    assert_eq!(
        jobs.execute_once("job-worker", now + 1)
            .await
            .expect("job should execute")
            .completed,
        1
    );
    assert_eq!(
        jobs.publish_notifications_once("job-notifier", now + 2)
            .await
            .expect("job completion should publish")
            .published,
        1
    );
    assert_eq!(
        events
            .dispatch_once("event-job", now + 3)
            .await
            .expect("job event should wake the flow")
            .completed,
        1
    );

    let resumed = flows
        .claim_runnable("agent-3".into(), now + 4, now + 10_004, 1)
        .await
        .expect("job-completed flow should claim")
        .pop()
        .expect("resumed activation should exist");
    let (resume_events, resume_outcome) = collect_machine(
        agent.resume(MachineResumeRequest {
            run_id,
            activation_id: resumed
                .state
                .activation_id
                .expect("activation id should exist"),
            checkpoint: resumed.state.checkpoint.clone(),
            inbox: resumed.inbox,
            activated_at_ms: now + 4,
            cancellation: cancellation.clone(),
        }),
    )
    .await;
    assert!(resume_events.iter().any(|event| matches!(
        event,
        AgentEvent::ToolExecutionCompleted { call_id, .. } if call_id == "call_async_job"
    )));
    let StepOutcome::Continue {
        checkpoint,
        effects: continue_effects,
    } = resume_outcome
    else {
        panic!("tool result should continue the model loop");
    };
    assert!(continue_effects.is_empty());
    flows
        .continue_run(ContinueFlowRun {
            run_id,
            activation_id: resumed
                .state
                .activation_id
                .expect("activation id should exist"),
            expected_revision: resumed.state.revision,
            checkpoint,
            effects: continue_effects,
            continued_at_ms: now + 5,
        })
        .await
        .expect("continued checkpoint should persist");

    let final_activation = flows
        .claim_runnable("agent-4".into(), now + 6, now + 10_006, 1)
        .await
        .expect("continued flow should claim")
        .pop()
        .expect("final activation should exist");
    let (final_events, final_outcome) = collect_machine(
        agent.resume(MachineResumeRequest {
            run_id,
            activation_id: final_activation
                .state
                .activation_id
                .expect("activation id should exist"),
            checkpoint: final_activation.state.checkpoint,
            inbox: final_activation.inbox,
            activated_at_ms: now + 6,
            cancellation,
        }),
    )
    .await;
    assert!(final_events.iter().any(|event| matches!(
        event,
        AgentEvent::OutputDelta { delta, .. } if delta == "job finished after restart"
    )));
    assert!(matches!(
        final_outcome,
        StepOutcome::Complete {
            finish_reason: FinishReason::Stop
        }
    ));
    let completed = flows
        .complete(CompleteFlowRun {
            run_id,
            activation_id: final_activation
                .state
                .activation_id
                .expect("activation id should exist"),
            expected_revision: final_activation.state.revision,
            status: FlowRunStatus::Completed,
            completed_at_ms: now + 7,
        })
        .await
        .expect("flow should complete");
    assert_eq!(completed.status, FlowRunStatus::Completed);
    assert_eq!(provider_calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert_eq!(job_calls.load(std::sync::atomic::Ordering::SeqCst), 1);
}

fn unix_time_ms() -> i64 {
    SystemTime::UNIX_EPOCH
        .elapsed()
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or_default()
}

fn flow_to_event(error: FlowError) -> EventError {
    match error {
        FlowError::Invalid(message) => EventError::Invalid(message),
        FlowError::NotFound => EventError::NotFound,
        FlowError::Conflict(message) => EventError::Conflict(message),
        FlowError::Backend(message) => EventError::Backend(message),
    }
}

fn event_to_flow(error: EventError) -> FlowError {
    match error {
        EventError::Invalid(message) => FlowError::Invalid(message),
        EventError::NotFound => FlowError::NotFound,
        EventError::Conflict(message) => FlowError::Conflict(message),
        EventError::Backend(message) => FlowError::Backend(message),
    }
}
