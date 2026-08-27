use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use async_stream::stream;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use agent_core::{
    event_runtime::{
        CreateSubscription, DeliveryTarget, EventFilter, EventId, EventSource, PublishEvent,
        ScheduleOnce, StartPosition, SubscriptionId, SubscriptionMode, SubscriptionOwner,
        SubscriptionScope, TimerId,
    },
    harness::{
        Agent, AgentEvent, AgentEventStream, AgentMachine, AgentMetadata, CheckpointCodec,
        CheckpointEnvelope, EffectRequest, FinishReason, FlowInboxItem, JobId, MachineError,
        MachineOutput, MachineResumeRequest, MachineStartRequest, MachineStream, OutputChannel,
        RunRequest, StartJob, StepOutcome, WaitSpec,
    },
};

use agent_core::script::{
    SCRIPT_ABI_VERSION, ScriptErrorKind, ScriptExecutionRequest, ScriptLanguage, ScriptLimits,
    ScriptModuleRef, ScriptPurpose, ScriptRuntime, ScriptSource,
};

const CHECKPOINT_KIND: &str = "javascript-flow";
const CHECKPOINT_VERSION: u32 = 1;
const MAX_FLOW_EVENTS: usize = 256;
const MAX_FLOW_WAITS: usize = 16;
const MAX_FLOW_EFFECTS: usize = 16;

/// Host-owned capability policy for declarative commands returned by a
/// JavaScript flow. QuickJS itself receives no host APIs; Rust validates and
/// materializes every wait and effect after the VM has been destroyed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct JavaScriptFlowPolicy {
    allowed_wait_topics: BTreeSet<String>,
    allowed_publish_topics: BTreeSet<String>,
    allowed_job_kinds: BTreeSet<String>,
}

impl JavaScriptFlowPolicy {
    #[must_use]
    pub fn with_wait_topics(mut self, topics: impl IntoIterator<Item = String>) -> Self {
        self.allowed_wait_topics.extend(topics);
        self
    }

    #[must_use]
    pub fn with_publish_topics(mut self, topics: impl IntoIterator<Item = String>) -> Self {
        self.allowed_publish_topics.extend(topics);
        self
    }

    #[must_use]
    pub fn with_job_kinds(mut self, kinds: impl IntoIterator<Item = String>) -> Self {
        self.allowed_job_kinds.extend(kinds);
        self
    }

    #[must_use]
    pub const fn allowed_wait_topics(&self) -> &BTreeSet<String> {
        &self.allowed_wait_topics
    }

    #[must_use]
    pub const fn allowed_publish_topics(&self) -> &BTreeSet<String> {
        &self.allowed_publish_topics
    }

    #[must_use]
    pub const fn allowed_job_kinds(&self) -> &BTreeSet<String> {
        &self.allowed_job_kinds
    }
}

/// A durable AgentMachine backed by a content-addressed JavaScript module.
/// Every activation creates a fresh ScriptRuntime execution and only the JSON
/// checkpoint below crosses process restarts.
#[derive(Clone)]
pub struct JavaScriptAgentMachine {
    runtime: Arc<dyn ScriptRuntime>,
    source: ScriptSource,
    module: ScriptModuleRef,
    limits: ScriptLimits,
    policy: JavaScriptFlowPolicy,
}

impl std::fmt::Debug for JavaScriptAgentMachine {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("JavaScriptAgentMachine")
            .field("runtime", &self.runtime.descriptor())
            .field("module", &self.module)
            .field("limits", &self.limits)
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}

impl JavaScriptAgentMachine {
    pub fn new(
        runtime: Arc<dyn ScriptRuntime>,
        source: ScriptSource,
        limits: ScriptLimits,
        policy: JavaScriptFlowPolicy,
    ) -> Result<Self, MachineError> {
        let module = source.module_ref().cloned().ok_or_else(|| {
            machine_error(
                "script_flow_artifact_required",
                "JavaScript flow handlers must use a resolved content-addressed artifact",
                false,
            )
        })?;
        validate_module(&module, source.source())?;
        if runtime.descriptor().abi_version != SCRIPT_ABI_VERSION {
            return Err(machine_error(
                "script_flow_abi_incompatible",
                "the script runtime ABI is incompatible with this JavaScript flow",
                false,
            ));
        }
        validate_policy(&policy)?;
        Ok(Self {
            runtime,
            source,
            module,
            limits,
            policy,
        })
    }

    fn metadata_value(&self) -> AgentMetadata {
        AgentMetadata::new("javascript-flow", env!("CARGO_PKG_VERSION"))
            .with_capability("event_stream")
            .with_capability("durable_checkpoint")
            .with_capability("script_flow")
            .with_capability("event_wait")
            .with_capability("durable_job")
    }

    fn initial_state(&self) -> JavaScriptFlowCheckpoint {
        JavaScriptFlowCheckpoint {
            module: self.module.clone(),
            state: Value::Null,
            next_step: 0,
        }
    }
}

impl Agent for JavaScriptAgentMachine {
    fn metadata(&self) -> AgentMetadata {
        self.metadata_value()
    }

    fn run(&self, _request: RunRequest) -> AgentEventStream {
        Box::pin(futures_util::stream::once(async {
            AgentEvent::failed(
                "durable_runtime_required",
                "JavaScript flow handlers require the durable AgentMachine runtime",
                false,
            )
        }))
    }
}

impl AgentMachine for JavaScriptAgentMachine {
    fn metadata(&self) -> AgentMetadata {
        self.metadata_value()
    }

    fn initial_checkpoint(
        &self,
        _request: &MachineStartRequest,
    ) -> Result<CheckpointEnvelope, MachineError> {
        encode_checkpoint(self.initial_state())
    }

    fn start(&self, request: MachineStartRequest) -> MachineStream {
        drive_javascript_activation(
            Arc::clone(&self.runtime),
            self.source.clone(),
            self.module.clone(),
            self.limits,
            self.policy.clone(),
            request.run_id,
            self.initial_state(),
            ScriptPurpose::FlowStart,
            serde_json::json!({
                "value": request.input,
                "prior_messages": request.prior_messages,
                "allowed_tools": request.allowed_tools,
                "allow_run_adf": request.allow_run_adf,
                "max_steps": request.max_steps,
                "context_fingerprint": request.context_fingerprint,
                "activated_at_ms": request.activated_at_ms,
            }),
            request.activated_at_ms,
            request.cancellation,
        )
    }

    fn resume(&self, request: MachineResumeRequest) -> MachineStream {
        let checkpoint = match decode_checkpoint(request.checkpoint, &self.module) {
            Ok(checkpoint) => checkpoint,
            Err(error) => return failure_stream(error),
        };
        let events = request
            .inbox
            .iter()
            .map(flow_inbox_json)
            .collect::<Vec<_>>();
        drive_javascript_activation(
            Arc::clone(&self.runtime),
            self.source.clone(),
            self.module.clone(),
            self.limits,
            self.policy.clone(),
            request.run_id,
            checkpoint.clone(),
            ScriptPurpose::FlowResume,
            serde_json::json!({
                "checkpoint": checkpoint.state,
                "events": events,
                "activated_at_ms": request.activated_at_ms,
            }),
            request.activated_at_ms,
            request.cancellation,
        )
    }
}

#[allow(clippy::too_many_arguments)]
fn drive_javascript_activation(
    runtime: Arc<dyn ScriptRuntime>,
    source: ScriptSource,
    module: ScriptModuleRef,
    limits: ScriptLimits,
    policy: JavaScriptFlowPolicy,
    run_id: agent_core::harness::RunId,
    checkpoint: JavaScriptFlowCheckpoint,
    purpose: ScriptPurpose,
    input: Value,
    activated_at_ms: i64,
    cancellation: agent_core::harness::RunCancellation,
) -> MachineStream {
    Box::pin(stream! {
        if cancellation.is_cancelled() {
            yield MachineOutput::Yield(StepOutcome::Cancelled);
            return;
        }
        let export = match purpose {
            ScriptPurpose::FlowStart => "start",
            ScriptPurpose::FlowResume => "resume",
            ScriptPurpose::Eval | ScriptPurpose::Tool => unreachable!("flow machine only executes flow purposes"),
        };
        let execution = runtime.execute(ScriptExecutionRequest {
            execution_id: format!("flow:{run_id}:{}:{export}", checkpoint.next_step),
            run_id: Some(run_id),
            language: ScriptLanguage::JavaScript,
            purpose,
            source,
            export: export.into(),
            input,
            granted_capabilities: Vec::new(),
            limits,
            cancellation: cancellation.clone(),
        });
        let output = tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                yield MachineOutput::Yield(StepOutcome::Cancelled);
                return;
            }
            result = execution => match result {
                Ok(output) => output,
                Err(error) if error.kind() == ScriptErrorKind::Cancelled => {
                    yield MachineOutput::Yield(StepOutcome::Cancelled);
                    return;
                }
                Err(error) => {
                    yield MachineOutput::Yield(StepOutcome::Failed {
                        error: machine_error(error.code(), error.safe_message(), error.retryable()),
                    });
                    return;
                }
            },
        };
        if output.module_digest != module.digest {
            yield MachineOutput::Yield(StepOutcome::Failed {
                error: machine_error(
                    "script_digest_mismatch",
                    "the executed JavaScript flow module does not match its checkpoint",
                    false,
                ),
            });
            return;
        }
        let response: JavaScriptFlowResponse = match serde_json::from_value(output.value) {
            Ok(response) => response,
            Err(_) => {
                yield MachineOutput::Yield(StepOutcome::Failed {
                    error: machine_error(
                        "script_flow_invalid_result",
                        "the JavaScript flow returned an invalid outcome",
                        false,
                    ),
                });
                return;
            }
        };
        let (events, outcome) = response.into_parts();
        if events.len() > MAX_FLOW_EVENTS {
            yield MachineOutput::Yield(StepOutcome::Failed {
                error: machine_error(
                    "script_flow_too_many_events",
                    format!("a JavaScript activation cannot emit more than {MAX_FLOW_EVENTS} events"),
                    false,
                ),
            });
            return;
        }
        for event in events {
            match event {
                JavaScriptFlowEvent::OutputDelta { channel, delta } if !delta.is_empty() => {
                    yield MachineOutput::Event(AgentEvent::OutputDelta { channel, delta });
                }
                JavaScriptFlowEvent::OutputDelta { .. } => {}
            }
        }
        let outcome = match materialize_outcome(
            run_id,
            checkpoint,
            outcome,
            activated_at_ms,
            &policy,
        ) {
            Ok(outcome) => outcome,
            Err(error) => {
                yield MachineOutput::Yield(StepOutcome::Failed { error });
                return;
            }
        };
        yield MachineOutput::Yield(outcome);
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct JavaScriptFlowCheckpoint {
    module: ScriptModuleRef,
    state: Value,
    next_step: u64,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum JavaScriptFlowResponse {
    Wrapped {
        #[serde(default)]
        events: Vec<JavaScriptFlowEvent>,
        outcome: JavaScriptFlowOutcome,
    },
    Direct(JavaScriptFlowOutcome),
}

impl JavaScriptFlowResponse {
    fn into_parts(self) -> (Vec<JavaScriptFlowEvent>, JavaScriptFlowOutcome) {
        match self {
            Self::Wrapped { events, outcome } => (events, outcome),
            Self::Direct(outcome) => (Vec::new(), outcome),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum JavaScriptFlowEvent {
    OutputDelta {
        channel: OutputChannel,
        delta: String,
    },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum JavaScriptFlowOutcome {
    Continue {
        checkpoint: Value,
        #[serde(default)]
        effects: Vec<JavaScriptFlowEffect>,
    },
    Suspend {
        checkpoint: Value,
        waits: Vec<JavaScriptFlowWait>,
        #[serde(default)]
        effects: Vec<JavaScriptFlowEffect>,
    },
    Complete {
        #[serde(default = "default_finish_reason")]
        finish_reason: FinishReason,
    },
    Failed {
        code: String,
        message: String,
        #[serde(default)]
        retryable: bool,
    },
    Cancelled,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum JavaScriptFlowEffect {
    PublishEvent {
        effect_key: String,
        topic: String,
        #[serde(default)]
        event_type: Option<String>,
        #[serde(default)]
        correlation_id: Option<String>,
        #[serde(default)]
        payload: Value,
    },
    ScheduleTimer {
        effect_key: String,
        fire_at_ms: i64,
        topic: String,
        #[serde(default)]
        event_type: Option<String>,
        #[serde(default)]
        payload: Value,
    },
    StartJob {
        effect_key: String,
        kind: String,
        input: Value,
        idempotency_key: String,
    },
}

impl JavaScriptFlowEffect {
    fn key(&self) -> &str {
        match self {
            Self::PublishEvent { effect_key, .. }
            | Self::ScheduleTimer { effect_key, .. }
            | Self::StartJob { effect_key, .. } => effect_key,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum JavaScriptFlowWait {
    Event {
        wait_key: String,
        filter: EventFilter,
        #[serde(default)]
        expires_at_ms: Option<i64>,
    },
    Timer {
        wait_key: String,
        effect_key: String,
        #[serde(default)]
        expires_at_ms: Option<i64>,
    },
    Job {
        wait_key: String,
        effect_key: String,
        #[serde(default)]
        expires_at_ms: Option<i64>,
    },
}

impl JavaScriptFlowWait {
    fn key(&self) -> &str {
        match self {
            Self::Event { wait_key, .. }
            | Self::Timer { wait_key, .. }
            | Self::Job { wait_key, .. } => wait_key,
        }
    }
}

enum EffectReference {
    Timer {
        topic: String,
        correlation_id: String,
    },
    Job {
        correlation_id: String,
    },
}

fn materialize_outcome(
    run_id: agent_core::harness::RunId,
    checkpoint: JavaScriptFlowCheckpoint,
    outcome: JavaScriptFlowOutcome,
    activated_at_ms: i64,
    policy: &JavaScriptFlowPolicy,
) -> Result<StepOutcome, MachineError> {
    match outcome {
        JavaScriptFlowOutcome::Continue {
            checkpoint: state,
            effects,
        } => {
            let (effects, _) = materialize_effects(
                run_id,
                checkpoint.next_step,
                effects,
                activated_at_ms,
                policy,
            )?;
            Ok(StepOutcome::Continue {
                checkpoint: encode_checkpoint(JavaScriptFlowCheckpoint {
                    module: checkpoint.module,
                    state,
                    next_step: checkpoint.next_step.saturating_add(1),
                })?,
                effects,
            })
        }
        JavaScriptFlowOutcome::Suspend {
            checkpoint: state,
            waits,
            effects,
        } => {
            if waits.is_empty() || waits.len() > MAX_FLOW_WAITS {
                return Err(machine_error(
                    "script_flow_invalid_waits",
                    format!("a suspended JavaScript flow requires 1 to {MAX_FLOW_WAITS} waits"),
                    false,
                ));
            }
            let (effects, references) = materialize_effects(
                run_id,
                checkpoint.next_step,
                effects,
                activated_at_ms,
                policy,
            )?;
            let waits = materialize_waits(
                run_id,
                checkpoint.next_step,
                waits,
                activated_at_ms,
                policy,
                &references,
            )?;
            Ok(StepOutcome::Suspend {
                checkpoint: encode_checkpoint(JavaScriptFlowCheckpoint {
                    module: checkpoint.module,
                    state,
                    next_step: checkpoint.next_step.saturating_add(1),
                })?,
                waits,
                effects,
            })
        }
        JavaScriptFlowOutcome::Complete { finish_reason } => {
            Ok(StepOutcome::Complete { finish_reason })
        }
        JavaScriptFlowOutcome::Failed {
            code,
            message,
            retryable,
        } => {
            validate_error(&code, &message)?;
            Ok(StepOutcome::Failed {
                error: machine_error(code, message, retryable),
            })
        }
        JavaScriptFlowOutcome::Cancelled => Ok(StepOutcome::Cancelled),
    }
}

fn materialize_effects(
    run_id: agent_core::harness::RunId,
    step: u64,
    effects: Vec<JavaScriptFlowEffect>,
    activated_at_ms: i64,
    policy: &JavaScriptFlowPolicy,
) -> Result<(Vec<EffectRequest>, BTreeMap<String, EffectReference>), MachineError> {
    if effects.len() > MAX_FLOW_EFFECTS {
        return Err(machine_error(
            "script_flow_too_many_effects",
            format!("a JavaScript activation cannot request more than {MAX_FLOW_EFFECTS} effects"),
            false,
        ));
    }
    let mut output = Vec::with_capacity(effects.len());
    let mut references = BTreeMap::new();
    let mut keys = BTreeSet::new();
    for effect in effects {
        let key = effect.key().to_owned();
        validate_key(&key, "effect_key")?;
        if !keys.insert(key.clone()) {
            return Err(machine_error(
                "script_flow_duplicate_effect_key",
                "JavaScript flow effect keys must be unique within one activation",
                false,
            ));
        }
        let stable_key = format!("{run_id}:{step}:{key}");
        match effect {
            JavaScriptFlowEffect::PublishEvent {
                topic,
                event_type,
                correlation_id,
                payload,
                ..
            } => {
                require_publish_topic(policy, &topic)?;
                let command = PublishEvent {
                    event_id: EventId::stable("javascript-flow-event", &stable_key),
                    event_type: event_type.unwrap_or_else(|| topic.clone()),
                    topic,
                    schema_version: 1,
                    source: EventSource::Agent,
                    subject: Some(format!("run/{run_id}")),
                    correlation_id,
                    causation_id: None,
                    occurred_at_ms: activated_at_ms,
                    recorded_at_ms: activated_at_ms,
                    payload,
                };
                agent_core::event_runtime::validate_publish(&command).map_err(|error| {
                    machine_error("script_flow_invalid_effect", error.to_string(), false)
                })?;
                output.push(EffectRequest::PublishEvent { command });
            }
            JavaScriptFlowEffect::ScheduleTimer {
                fire_at_ms,
                topic,
                event_type,
                payload,
                ..
            } => {
                require_publish_topic(policy, &topic)?;
                if fire_at_ms < activated_at_ms {
                    return Err(machine_error(
                        "script_flow_invalid_timer",
                        "a JavaScript flow timer cannot be scheduled in the past",
                        false,
                    ));
                }
                let timer_id = TimerId::stable("javascript-flow-timer", &stable_key);
                let correlation_id = timer_id.to_string();
                let command = ScheduleOnce {
                    timer_id,
                    fire_at_ms,
                    event: PublishEvent {
                        event_id: EventId::stable("javascript-flow-timer-event", &stable_key),
                        event_type: event_type.unwrap_or_else(|| topic.clone()),
                        topic: topic.clone(),
                        schema_version: 1,
                        source: EventSource::Timer,
                        subject: Some(format!("run/{run_id}")),
                        correlation_id: Some(correlation_id.clone()),
                        causation_id: None,
                        occurred_at_ms: fire_at_ms,
                        recorded_at_ms: fire_at_ms,
                        payload,
                    },
                    created_at_ms: activated_at_ms,
                };
                command.validate().map_err(|error| {
                    machine_error("script_flow_invalid_effect", error.to_string(), false)
                })?;
                references.insert(
                    key,
                    EffectReference::Timer {
                        topic,
                        correlation_id,
                    },
                );
                output.push(EffectRequest::ScheduleTimer { command });
            }
            JavaScriptFlowEffect::StartJob {
                kind,
                input,
                idempotency_key,
                ..
            } => {
                if !policy.allowed_job_kinds.contains(&kind) {
                    return Err(machine_error(
                        "script_flow_job_kind_denied",
                        "the JavaScript flow requested a job kind outside the host allowlist",
                        false,
                    ));
                }
                let job_key = format!("{run_id}:{kind}:{idempotency_key}");
                let job_id = JobId::stable("javascript-flow-job", &job_key);
                let command = StartJob {
                    job_id,
                    run_id,
                    kind,
                    input,
                    idempotency_key,
                    requested_at_ms: activated_at_ms,
                };
                command.validate().map_err(|error| {
                    machine_error("script_flow_invalid_effect", error.to_string(), false)
                })?;
                references.insert(
                    key,
                    EffectReference::Job {
                        correlation_id: job_id.to_string(),
                    },
                );
                output.push(EffectRequest::StartJob { command });
            }
        }
    }
    Ok((output, references))
}

fn materialize_waits(
    run_id: agent_core::harness::RunId,
    step: u64,
    waits: Vec<JavaScriptFlowWait>,
    activated_at_ms: i64,
    policy: &JavaScriptFlowPolicy,
    references: &BTreeMap<String, EffectReference>,
) -> Result<Vec<WaitSpec>, MachineError> {
    let mut output = Vec::with_capacity(waits.len());
    let mut keys = BTreeSet::new();
    for wait in waits {
        let wait_key = wait.key().to_owned();
        validate_key(&wait_key, "wait_key")?;
        if !keys.insert(wait_key.clone()) {
            return Err(machine_error(
                "script_flow_duplicate_wait_key",
                "JavaScript flow wait keys must be unique within one activation",
                false,
            ));
        }
        let (filter, expires_at_ms) = match wait {
            JavaScriptFlowWait::Event {
                filter,
                expires_at_ms,
                ..
            } => {
                if filter.topics.is_empty()
                    || filter
                        .topics
                        .iter()
                        .any(|topic| !policy.allowed_wait_topics.contains(topic))
                {
                    return Err(machine_error(
                        "script_flow_wait_topic_denied",
                        "the JavaScript flow requested an event wait outside the host allowlist",
                        false,
                    ));
                }
                filter.validate().map_err(|error| {
                    machine_error("script_flow_invalid_wait", error.to_string(), false)
                })?;
                (filter, expires_at_ms)
            }
            JavaScriptFlowWait::Timer {
                effect_key,
                expires_at_ms,
                ..
            } => {
                let Some(EffectReference::Timer {
                    topic,
                    correlation_id,
                }) = references.get(&effect_key)
                else {
                    return Err(machine_error(
                        "script_flow_wait_effect_missing",
                        "a JavaScript timer wait must reference a timer effect from the same activation",
                        false,
                    ));
                };
                (
                    EventFilter {
                        topics: vec![topic.clone()],
                        sources: vec![EventSource::Timer],
                        subject: Some(format!("run/{run_id}")),
                        correlation_id: Some(correlation_id.clone()),
                        ..EventFilter::default()
                    },
                    expires_at_ms,
                )
            }
            JavaScriptFlowWait::Job {
                effect_key,
                expires_at_ms,
                ..
            } => {
                let Some(EffectReference::Job { correlation_id }) = references.get(&effect_key)
                else {
                    return Err(machine_error(
                        "script_flow_wait_effect_missing",
                        "a JavaScript job wait must reference a job effect from the same activation",
                        false,
                    ));
                };
                (
                    EventFilter {
                        topics: vec!["job.completed".into(), "job.failed".into()],
                        sources: vec![EventSource::JobWorker],
                        subject: Some(format!("run/{run_id}")),
                        correlation_id: Some(correlation_id.clone()),
                        ..EventFilter::default()
                    },
                    expires_at_ms,
                )
            }
        };
        let stable_key = format!("{run_id}:{step}:{wait_key}");
        output.push(WaitSpec {
            wait_key: wait_key.clone(),
            subscription: CreateSubscription {
                subscription_id: SubscriptionId::stable("javascript-flow-wait", &stable_key),
                owner: SubscriptionOwner::Run { run_id },
                scope: SubscriptionScope::Run { run_id },
                filter,
                delivery: DeliveryTarget::WakeRun { run_id, wait_key },
                mode: SubscriptionMode::Once,
                start_position: StartPosition::Now,
                expires_at_ms,
                max_deliveries: Some(1),
                created_at_ms: activated_at_ms,
            },
        });
    }
    Ok(output)
}

fn encode_checkpoint(
    checkpoint: JavaScriptFlowCheckpoint,
) -> Result<CheckpointEnvelope, MachineError> {
    let payload = serde_json::to_value(checkpoint).map_err(|_| {
        machine_error(
            "script_flow_checkpoint_encode_failed",
            "the JavaScript flow checkpoint could not be encoded",
            false,
        )
    })?;
    let envelope = CheckpointEnvelope {
        agent_kind: CHECKPOINT_KIND.into(),
        schema_version: CHECKPOINT_VERSION,
        codec: CheckpointCodec::Json,
        payload,
    };
    envelope.validate().map_err(|error| {
        machine_error("script_flow_checkpoint_invalid", error.to_string(), false)
    })?;
    Ok(envelope)
}

fn decode_checkpoint(
    checkpoint: CheckpointEnvelope,
    expected_module: &ScriptModuleRef,
) -> Result<JavaScriptFlowCheckpoint, MachineError> {
    checkpoint.validate().map_err(|error| {
        machine_error("script_flow_checkpoint_invalid", error.to_string(), false)
    })?;
    if checkpoint.agent_kind != CHECKPOINT_KIND
        || checkpoint.schema_version != CHECKPOINT_VERSION
        || checkpoint.codec != CheckpointCodec::Json
    {
        return Err(machine_error(
            "script_flow_checkpoint_incompatible",
            "the checkpoint is not compatible with this JavaScript flow",
            false,
        ));
    }
    let decoded: JavaScriptFlowCheckpoint =
        serde_json::from_value(checkpoint.payload).map_err(|_| {
            machine_error(
                "script_flow_checkpoint_decode_failed",
                "the JavaScript flow checkpoint payload is invalid",
                false,
            )
        })?;
    if &decoded.module != expected_module {
        return Err(machine_error(
            "script_flow_artifact_changed",
            "the JavaScript flow artifact changed after the checkpoint was created",
            false,
        ));
    }
    Ok(decoded)
}

fn validate_module(module: &ScriptModuleRef, source: &str) -> Result<(), MachineError> {
    if module.module_id.trim().is_empty() || module.module_id.len() > 256 || module.revision == 0 {
        return Err(machine_error(
            "script_flow_artifact_invalid",
            "the JavaScript flow artifact id and revision are invalid",
            false,
        ));
    }
    let mut hasher = Sha256::new();
    hasher.update(source.as_bytes());
    let digest = format!("sha256:{:x}", hasher.finalize());
    if module.digest != digest {
        return Err(machine_error(
            "script_digest_mismatch",
            "the JavaScript flow source does not match its artifact digest",
            false,
        ));
    }
    Ok(())
}

fn validate_policy(policy: &JavaScriptFlowPolicy) -> Result<(), MachineError> {
    for value in policy
        .allowed_wait_topics
        .iter()
        .chain(&policy.allowed_publish_topics)
        .chain(&policy.allowed_job_kinds)
    {
        if !valid_name(value) {
            return Err(machine_error(
                "script_flow_policy_invalid",
                "JavaScript flow policy names must use lowercase ASCII name characters",
                false,
            ));
        }
    }
    Ok(())
}

fn require_publish_topic(policy: &JavaScriptFlowPolicy, topic: &str) -> Result<(), MachineError> {
    if policy.allowed_publish_topics.contains(topic) {
        Ok(())
    } else {
        Err(machine_error(
            "script_flow_publish_topic_denied",
            "the JavaScript flow requested an event topic outside the host allowlist",
            false,
        ))
    }
}

fn validate_key(value: &str, field: &str) -> Result<(), MachineError> {
    if value.is_empty() || value.len() > 128 || !valid_name(value) {
        return Err(machine_error(
            "script_flow_invalid_key",
            format!("JavaScript flow {field} must use 1 to 128 lowercase ASCII name characters"),
            false,
        ));
    }
    Ok(())
}

fn valid_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        })
}

fn validate_error(code: &str, message: &str) -> Result<(), MachineError> {
    if !valid_name(code) || message.trim().is_empty() || message.len() > 4_096 {
        return Err(machine_error(
            "script_flow_invalid_error",
            "the JavaScript flow returned an invalid failure",
            false,
        ));
    }
    Ok(())
}

fn flow_inbox_json(item: &FlowInboxItem) -> Value {
    let event = &item.event;
    serde_json::json!({
        "sequence": event.sequence,
        "event_id": event.event.event_id,
        "topic": event.event.topic,
        "event_type": event.event.event_type,
        "schema_version": event.event.schema_version,
        "source": event.event.source,
        "subject": event.event.subject,
        "correlation_id": event.event.correlation_id,
        "causation_id": event.event.causation_id,
        "occurred_at_ms": event.event.occurred_at_ms,
        "payload": event.event.payload,
    })
}

const fn default_finish_reason() -> FinishReason {
    FinishReason::Stop
}

fn failure_stream(error: MachineError) -> MachineStream {
    Box::pin(futures_util::stream::once(async move {
        MachineOutput::Yield(StepOutcome::Failed { error })
    }))
}

fn machine_error(
    code: impl Into<String>,
    message: impl Into<String>,
    retryable: bool,
) -> MachineError {
    MachineError {
        code: code.into(),
        message: message.into(),
        retryable,
    }
}

#[cfg(all(test, feature = "quickjs"))]
mod tests {
    use futures_util::StreamExt;
    use serde_json::json;

    use super::*;
    use crate::script::QuickJsRuntime;
    use agent_core::{
        event_runtime::EventEnvelope,
        harness::{ActivationId, FlowInboxItem, RunCancellation, RunId},
    };

    fn resolved_source(source: &str) -> ScriptSource {
        let mut hasher = Sha256::new();
        hasher.update(source.as_bytes());
        ScriptSource::ResolvedArtifact {
            reference: ScriptModuleRef {
                module_id: "test-flow".into(),
                revision: 1,
                digest: format!("sha256:{:x}", hasher.finalize()),
            },
            source: source.into(),
        }
    }

    async fn collect(stream: MachineStream) -> (Vec<AgentEvent>, StepOutcome) {
        let mut stream = stream;
        let mut events = Vec::new();
        let mut outcome = None;
        while let Some(output) = stream.next().await {
            match output {
                MachineOutput::Event(event) => events.push(event),
                MachineOutput::Yield(step) => outcome = Some(step),
                _ => {}
            }
        }
        (events, outcome.expect("JavaScript activation should yield"))
    }

    #[tokio::test]
    async fn timer_flow_uses_fresh_context_and_resumes_from_json_checkpoint() {
        let source = r#"
            export function start(context, input) {
                globalThis.transientState = 99;
                return {
                    events: [{
                        type: "output_delta",
                        channel: "assistant_reasoning",
                        delta: `waiting:${input.value}`,
                    }],
                    outcome: {
                        type: "suspend",
                        checkpoint: { phase: "timer" },
                        effects: [{
                            type: "schedule_timer",
                            effect_key: "wake_timer",
                            fire_at_ms: context.activatedAtMs + 1,
                            topic: "flow.timer.ready",
                            payload: { answer: 42 },
                        }],
                        waits: [{
                            type: "timer",
                            wait_key: "wake_timer",
                            effect_key: "wake_timer",
                        }],
                    },
                };
            }

            export function resume(context, checkpoint, events) {
                if (globalThis.transientState !== undefined) {
                    return {
                        type: "failed",
                        code: "vm_state_leaked",
                        message: "QuickJS global state crossed activations",
                    };
                }
                if (checkpoint.phase !== "timer" || events[0].topic !== "flow.timer.ready") {
                    return {
                        type: "failed",
                        code: "resume_input_invalid",
                        message: "checkpoint or event was not restored",
                    };
                }
                return {
                    events: [{
                        type: "output_delta",
                        channel: "assistant_text",
                        delta: String(events[0].payload.answer),
                    }],
                    outcome: { type: "complete" },
                };
            }
        "#;
        let machine = JavaScriptAgentMachine::new(
            Arc::new(QuickJsRuntime::default()),
            resolved_source(source),
            ScriptLimits::default(),
            JavaScriptFlowPolicy::default().with_publish_topics(["flow.timer.ready".into()]),
        )
        .expect("flow should construct");
        let run_id = RunId::new();
        let cancellation = RunCancellation::new();
        let (events, outcome) = collect(machine.start(MachineStartRequest {
            run_id,
            input: "hello".into(),
            prior_messages: Vec::new(),
            allowed_tools: None,
            allow_run_adf: false,
            max_steps: None,
            context_fingerprint: None,
            activated_at_ms: 100,
            cancellation: cancellation.clone(),
        }))
        .await;
        assert!(events.iter().any(|event| matches!(
            event,
            AgentEvent::OutputDelta {
                channel: OutputChannel::AssistantReasoning,
                delta,
            } if delta == "waiting:hello"
        )));
        let StepOutcome::Suspend {
            checkpoint,
            waits,
            effects,
        } = outcome
        else {
            panic!("flow should suspend for its timer");
        };
        assert_eq!(waits.len(), 1);
        let EffectRequest::ScheduleTimer { command } = effects
            .into_iter()
            .next()
            .expect("timer effect should exist")
        else {
            panic!("expected timer effect");
        };
        assert_eq!(command.fire_at_ms, 101);
        assert_eq!(
            waits[0].subscription.filter.correlation_id,
            command.event.correlation_id
        );

        let (events, outcome) = collect(machine.resume(MachineResumeRequest {
            run_id,
            activation_id: ActivationId::new(),
            checkpoint,
            inbox: vec![FlowInboxItem {
                event: EventEnvelope {
                    sequence: 1,
                    event: command.event,
                },
                consumed_revision: None,
            }],
            activated_at_ms: 102,
            cancellation,
        }))
        .await;
        assert!(events.iter().any(|event| matches!(
            event,
            AgentEvent::OutputDelta {
                channel: OutputChannel::AssistantText,
                delta,
            } if delta == "42"
        )));
        assert!(matches!(
            outcome,
            StepOutcome::Complete {
                finish_reason: FinishReason::Stop
            }
        ));
    }

    #[tokio::test]
    async fn job_flow_materializes_only_allowlisted_job_kinds() {
        let source = r#"
            export function start(context, input) {
                return {
                    type: "suspend",
                    checkpoint: { phase: "job" },
                    effects: [{
                        type: "start_job",
                        effect_key: "work",
                        kind: "denied.echo",
                        input: { value: 7 },
                        idempotency_key: "job-one",
                    }],
                    waits: [{ type: "job", wait_key: "work", effect_key: "work" }],
                };
            }
            export function resume(context, checkpoint, events) {
                return { type: "complete" };
            }
        "#;
        let machine = JavaScriptAgentMachine::new(
            Arc::new(QuickJsRuntime::default()),
            resolved_source(source),
            ScriptLimits::default(),
            JavaScriptFlowPolicy::default().with_job_kinds(["test.echo".into()]),
        )
        .expect("flow should construct");
        let (_, outcome) = collect(machine.start(MachineStartRequest {
            run_id: RunId::new(),
            input: json!({"kind": "test.echo"}).to_string(),
            prior_messages: Vec::new(),
            allowed_tools: None,
            allow_run_adf: false,
            max_steps: None,
            context_fingerprint: None,
            activated_at_ms: 100,
            cancellation: RunCancellation::new(),
        }))
        .await;
        let StepOutcome::Failed { error } = outcome else {
            panic!("the plain string input must not bypass the job allowlist");
        };
        assert_eq!(error.code, "script_flow_job_kind_denied");
    }
}
