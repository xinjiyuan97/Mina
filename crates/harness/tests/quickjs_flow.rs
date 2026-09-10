use std::{sync::Arc, time::SystemTime};

use agent_core::{
    event_runtime::{
        Delivery, DeliveryRouter, DeliveryTarget, EventEnvelope, EventError, EventFuture,
        EventStore,
    },
    harness::{
        AgentEvent, AgentMachine, CompleteFlowRun, EffectRequest, FinishReason, FlowEffect,
        FlowEffectRouter, FlowError, FlowRunState, FlowRunStatus, FlowStore, JobExecutionFuture,
        JobRecord, JobRouter, JobStore, MachineOutput, MachineResumeRequest, MachineStartRequest,
        RunCancellation, RunId, StepOutcome, SuspendFlowRun, WakeFlowRun,
    },
    script::{ScriptLimits, ScriptModuleRef, ScriptRuntime, ScriptSource},
};
use agent_extension::store::SqliteEventStore;
use agent_harness::{
    EventRuntime, EventRuntimeConfig, FlowEffectRuntime, FlowEffectRuntimeConfig, JobRuntime,
    JobRuntimeConfig,
    script::{JavaScriptAgentMachine, JavaScriptFlowPolicy, QuickJsRuntime},
};
use futures_util::StreamExt;
use sha2::{Digest, Sha256};
use tempfile::tempdir;

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

type TestRuntimes = (
    Arc<dyn FlowStore>,
    Arc<EventRuntime>,
    Arc<JobRuntime>,
    Arc<FlowEffectRuntime>,
);

fn build_runtimes(
    adapter: &Arc<SqliteEventStore>,
    job_calls: Arc<std::sync::atomic::AtomicUsize>,
) -> TestRuntimes {
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

fn machine(source: &str, policy: JavaScriptFlowPolicy) -> JavaScriptAgentMachine {
    let mut hasher = Sha256::new();
    hasher.update(source.as_bytes());
    let source = ScriptSource::ResolvedArtifact {
        reference: ScriptModuleRef {
            module_id: "sqlite-restart-flow".into(),
            revision: 1,
            digest: format!("sha256:{:x}", hasher.finalize()),
        },
        source: source.into(),
    };
    let runtime: Arc<dyn ScriptRuntime> = Arc::new(QuickJsRuntime::default());
    JavaScriptAgentMachine::new(runtime, source, ScriptLimits::default(), policy)
        .expect("JavaScript machine should construct")
}

async fn collect(mut stream: agent_core::harness::MachineStream) -> (Vec<AgentEvent>, StepOutcome) {
    let mut events = Vec::new();
    let mut outcome = None;
    while let Some(output) = stream.next().await {
        match output {
            MachineOutput::Event(event) => events.push(event),
            MachineOutput::Yield(step) => outcome = Some(step),
            _ => panic!("unsupported machine output"),
        }
    }
    (events, outcome.expect("JavaScript activation should yield"))
}

async fn persist_initial_suspension(
    machine: &JavaScriptAgentMachine,
    flows: &Arc<dyn FlowStore>,
    run_id: RunId,
    base: i64,
) {
    let request = MachineStartRequest {
        run_id,
        input: "start".into(),
        attachments: Vec::new(),
        prior_messages: Vec::new(),
        allowed_tools: None,
        allow_run_adf: false,
        max_steps: None,
        context_fingerprint: None,
        activated_at_ms: base,
        cancellation: RunCancellation::new(),
    };
    flows
        .create(FlowRunState {
            run_id,
            revision: 0,
            status: FlowRunStatus::Runnable,
            activation_id: None,
            checkpoint: machine
                .initial_checkpoint(&request)
                .expect("initial checkpoint should encode"),
            wait_subscription_ids: Vec::new(),
            lease_owner: None,
            lease_until_ms: None,
            updated_at_ms: base,
        })
        .await
        .expect("flow should create");
    let activation = flows
        .claim_runnable("script-start".into(), base, base + 10_000, 1)
        .await
        .expect("flow should claim")
        .pop()
        .expect("one activation should exist");
    let (_, outcome) = collect(machine.start(request)).await;
    let StepOutcome::Suspend {
        checkpoint,
        waits,
        effects,
    } = outcome
    else {
        panic!("JavaScript flow should suspend");
    };
    flows
        .suspend(SuspendFlowRun {
            run_id,
            activation_id: activation
                .state
                .activation_id
                .expect("activation id should exist"),
            expected_revision: activation.state.revision,
            checkpoint,
            waits,
            effects,
            suspended_at_ms: base + 1,
        })
        .await
        .expect("suspension should persist");
}

async fn resume_and_complete(
    machine: &JavaScriptAgentMachine,
    flows: &Arc<dyn FlowStore>,
    run_id: RunId,
    now: i64,
) -> Vec<AgentEvent> {
    let activation = flows
        .claim_runnable("script-resume".into(), now, now + 10_000, 1)
        .await
        .expect("woken flow should claim")
        .pop()
        .expect("one resumed activation should exist");
    let (events, outcome) = collect(
        machine.resume(MachineResumeRequest {
            run_id,
            activation_id: activation
                .state
                .activation_id
                .expect("activation id should exist"),
            checkpoint: activation.state.checkpoint.clone(),
            inbox: activation.inbox,
            activated_at_ms: now,
            cancellation: RunCancellation::new(),
        }),
    )
    .await;
    assert!(matches!(
        outcome,
        StepOutcome::Complete {
            finish_reason: FinishReason::Stop
        }
    ));
    let completed = flows
        .complete(CompleteFlowRun {
            run_id,
            activation_id: activation
                .state
                .activation_id
                .expect("activation id should exist"),
            expected_revision: activation.state.revision,
            status: FlowRunStatus::Completed,
            completed_at_ms: now + 1,
        })
        .await
        .expect("flow should complete");
    assert_eq!(completed.status, FlowRunStatus::Completed);
    events
}

#[tokio::test]
async fn javascript_timer_flow_reopens_fires_and_resumes() {
    let source = r#"
        export function start(context, input) {
            return {
                type: "suspend",
                checkpoint: { phase: "timer" },
                effects: [{
                    type: "schedule_timer",
                    effect_key: "wake",
                    fire_at_ms: context.activatedAtMs,
                    topic: "flow.timer.ready",
                    payload: { value: 11 },
                }],
                waits: [{ type: "timer", wait_key: "wake", effect_key: "wake" }],
            };
        }
        export function resume(context, checkpoint, events) {
            if (checkpoint.phase !== "timer" || events[0].topic !== "flow.timer.ready") {
                return { type: "failed", code: "timer_resume_failed", message: "bad resume" };
            }
            return {
                events: [{
                    type: "output_delta",
                    channel: "assistant_text",
                    delta: `timer:${events[0].payload.value}`,
                }],
                outcome: { type: "complete" },
            };
        }
    "#;
    let policy = JavaScriptFlowPolicy::default().with_publish_topics(["flow.timer.ready".into()]);
    let directory = tempdir().expect("temporary directory should exist");
    let path = directory.path().join("quickjs-timer.sqlite3");
    let run_id = RunId::new();
    let base = unix_time_ms();
    let job_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let adapter = Arc::new(
        SqliteEventStore::open(&path, "quickjs-timer:first")
            .await
            .expect("store should open"),
    );
    let (flows, events, jobs, effects) = build_runtimes(&adapter, Arc::clone(&job_calls));
    persist_initial_suspension(&machine(source, policy.clone()), &flows, run_id, base).await;
    drop(effects);
    drop(jobs);
    drop(events);
    drop(flows);
    drop(adapter);

    let adapter = Arc::new(
        SqliteEventStore::open(&path, "quickjs-timer:reopened")
            .await
            .expect("store should reopen"),
    );
    let (flows, events, _jobs, effects) = build_runtimes(&adapter, Arc::clone(&job_calls));
    let now = unix_time_ms().saturating_add(5);
    assert_eq!(
        effects
            .dispatch_once("timer-effect", now)
            .await
            .expect("timer effect should dispatch")
            .completed,
        1
    );
    assert_eq!(
        events
            .fire_timers_once("timer-worker", now + 1)
            .await
            .expect("timer should fire")
            .completed,
        1
    );
    assert_eq!(
        events
            .dispatch_once("timer-delivery", now + 2)
            .await
            .expect("timer event should wake flow")
            .completed,
        1
    );
    let output = resume_and_complete(&machine(source, policy), &flows, run_id, now + 3).await;
    assert!(output.iter().any(|event| matches!(
        event,
        AgentEvent::OutputDelta { delta, .. } if delta == "timer:11"
    )));
    assert_eq!(job_calls.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[tokio::test]
async fn javascript_job_flow_reopens_executes_once_and_resumes() {
    let source = r#"
        export function start(context, input) {
            return {
                type: "suspend",
                checkpoint: { phase: "job" },
                effects: [{
                    type: "start_job",
                    effect_key: "work",
                    kind: "test.echo",
                    input: { value: { answer: 42 } },
                    idempotency_key: "quickjs-job-one",
                }],
                waits: [{ type: "job", wait_key: "work", effect_key: "work" }],
            };
        }
        export function resume(context, checkpoint, events) {
            const answer = events[0].payload.outcome.value.answer;
            if (checkpoint.phase !== "job" || answer !== 42) {
                return { type: "failed", code: "job_resume_failed", message: "bad resume" };
            }
            return {
                events: [{
                    type: "output_delta",
                    channel: "assistant_text",
                    delta: `job:${answer}`,
                }],
                outcome: { type: "complete" },
            };
        }
    "#;
    let policy = JavaScriptFlowPolicy::default().with_job_kinds(["test.echo".into()]);
    let directory = tempdir().expect("temporary directory should exist");
    let path = directory.path().join("quickjs-job.sqlite3");
    let run_id = RunId::new();
    let base = unix_time_ms();
    let job_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let adapter = Arc::new(
        SqliteEventStore::open(&path, "quickjs-job:first")
            .await
            .expect("store should open"),
    );
    let (flows, events, jobs, effects) = build_runtimes(&adapter, Arc::clone(&job_calls));
    persist_initial_suspension(&machine(source, policy.clone()), &flows, run_id, base).await;
    drop(effects);
    drop(jobs);
    drop(events);
    drop(flows);
    drop(adapter);

    let adapter = Arc::new(
        SqliteEventStore::open(&path, "quickjs-job:reopened")
            .await
            .expect("store should reopen"),
    );
    let (flows, events, jobs, effects) = build_runtimes(&adapter, Arc::clone(&job_calls));
    let now = unix_time_ms().saturating_add(5);
    assert_eq!(
        effects
            .dispatch_once("job-effect", now)
            .await
            .expect("job effect should dispatch")
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
            .expect("job should publish completion")
            .published,
        1
    );
    assert_eq!(
        events
            .dispatch_once("job-delivery", now + 3)
            .await
            .expect("job completion should wake flow")
            .completed,
        1
    );
    let output = resume_and_complete(&machine(source, policy), &flows, run_id, now + 4).await;
    assert!(output.iter().any(|event| matches!(
        event,
        AgentEvent::OutputDelta { delta, .. } if delta == "job:42"
    )));
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
