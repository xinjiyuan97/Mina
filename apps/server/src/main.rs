use std::{
    collections::HashMap,
    convert::Infallible,
    env,
    sync::{Arc, Mutex},
    time::Duration,
};

use agent_core::adf::RunScopedJavaScriptPolicy;
use agent_core::context::{
    ContextArtifactStore, ContextBudget, ContextBuildRequest, ContextCompressor, ContextEngine,
    DeterministicSummaryGenerator, FallbackSummaryGenerator, HeuristicTokenEstimator,
    HybridCompressor, NoopFailCompressor, SlidingWindowCompressor, SummaryGenerator,
    TokenEstimator,
};
use agent_core::event_runtime::{
    CreateSubscription, Delivery, DeliveryRouter, DeliveryTarget, EventEnvelope, EventError,
    EventFilter, EventFuture, EventId, EventSource, EventStore, PublishEvent, StartPosition,
    SubscriptionId, SubscriptionMode, SubscriptionOwner, SubscriptionScope,
};
use agent_core::harness::{
    Agent, AgentEvent, AgentEventStream, AgentLoop, AgentMetadata, ApprovalDecision, ApprovalError,
    ApprovalFuture, ApprovalId, ApprovalPort, ApprovalRequest, ApprovalResolution, ArchiveSession,
    BeginSessionRun, CheckpointCodec, CheckpointEnvelope, ContentPart, CreateSession,
    EffectRequest, FinalizeSessionRun, FinishReason, FlowEffect, FlowEffectRouter, FlowError,
    FlowStore, JobExecutionError, JobExecutionFuture, JobRecord, JobRouter, JobStore,
    MAX_SESSION_PAGE_SIZE, MachineError, MachineOutput, MachineResumeRequest, MachineStartRequest,
    MachineStream, ModelMessage, RunEvent, RunFailure, RunId, RunRequest, RunResponse, RunSnapshot,
    RunStatus, RunStore, RunStoreError, SessionId, SessionStatus, SessionStore, SessionStoreError,
    SingleTurnAgent, StepOutcome, ToolApprovalPolicy, ToolDefinition, ToolPort, ToolRegistry,
    ToolRiskLevel, WakeFlowRun,
};
use agent_core::memory::{
    CreateMemoryWriteProposal, HostMemoryWritePolicy, MemoryApprovalId, MemoryApprovalStatus,
    MemoryComponentDescriptor, MemoryExtractionRequest, MemoryExtractor, MemoryListQuery,
    MemoryPage, MemoryProposalStore, MemoryRecord, MemoryRetriever, MemoryScope, MemorySourceRef,
    MemoryStore, MemoryWriteOutcome, MemoryWritePolicy, MemoryWriteProposal, MemoryWriter,
    PutMemory, ResolveMemoryWriteProposal, RuleMemoryExtractor,
};
use agent_core::script::{ScriptLimits, ScriptRuntime, ScriptRuntimeDescriptor};
use agent_core::skill::{
    ComponentDescriptor, ResolveSkillsRequest, ResolvedSkill, SkillDescriptor, SkillId,
    SkillOrchestrator, SkillStore,
};
use agent_extension::adf::{InMemoryAdfArtifactStore, RunAdfToolSession};
use agent_extension::context::ModelSummaryGenerator;
use agent_extension::observability::{
    AsyncObservationConfig, AsyncObservationHook, HookObservationExporter, ObservationHook,
    ObservedMachine, ObservedModel, ObservedTools, TracingObservationHook,
};
use agent_extension::provider::OpenAiCompatibleProvider;
use agent_extension::sandbox::{HostProcessSandbox, ProcessSandbox, ProcessSandboxDescriptor};
use agent_extension::store::{
    FilesystemSkillStore, SqliteEventStore, SqliteMemoryStore, SqliteRunStore,
};
use agent_extension::tool::{
    AsyncJobTool, BuiltinToolCatalog, JavaScriptEvalTool, SearchBackend, SearchBackendDescriptor,
    WorkspaceSearchBackend,
};
use agent_harness::{
    AgentKind, ContextStrategy, EventRuntime, EventRuntimeConfig, FlowEffectRuntime,
    FlowEffectRuntimeConfig, Harness, HarnessConfig, HarnessError, JobRuntime, JobRuntimeConfig,
    MAX_RUN_STEPS, Modality, OrchestrationConfig, PlannedRun, QuickJsConfig, RunRuntime,
    RunRuntimeError, StartedRun,
    adf::JavaScriptAdfExecutor,
    script::{QuickJsRuntime, QuickJsRuntimeConfig},
};
use async_stream::stream;
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{get, post},
};
use futures_util::{StreamExt, stream as futures_stream};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{
    net::TcpListener,
    sync::{broadcast, oneshot},
};
use tracing_subscriber::EnvFilter;

use agent_harness as run_runtime;

#[derive(Debug, Default)]
struct EchoAgent;

impl Agent for EchoAgent {
    fn metadata(&self) -> AgentMetadata {
        AgentMetadata::new("echo", env!("CARGO_PKG_VERSION"))
            .with_capability("request_response")
            .with_capability("event_stream")
    }

    fn run(&self, request: RunRequest) -> AgentEventStream {
        Box::pin(futures_stream::iter(vec![
            AgentEvent::text_delta(format!("Echo: {}", request.input)),
            AgentEvent::completed(FinishReason::Stop),
        ]))
    }
}

enum RuntimeAgent {
    Echo(EchoAgent),
    SingleTurn(Box<SingleTurnAgent<ObservedModel<OpenAiCompatibleProvider>>>),
    AgentLoop(
        Box<
            AgentLoop<
                ObservedModel<OpenAiCompatibleProvider>,
                ObservedTools<RunAdfToolSession<ToolRegistry>>,
            >,
        >,
    ),
}

type AppRuntimeAgent = ObservedMachine<RuntimeAgent>;

impl Agent for RuntimeAgent {
    fn metadata(&self) -> AgentMetadata {
        match self {
            Self::Echo(agent) => agent.metadata(),
            Self::SingleTurn(agent) => agent.metadata(),
            Self::AgentLoop(agent) => agent.metadata(),
        }
    }

    fn run(&self, request: RunRequest) -> AgentEventStream {
        match self {
            Self::Echo(agent) => agent.run(request),
            Self::SingleTurn(agent) => agent.run(request),
            Self::AgentLoop(agent) => agent.run(request),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LegacyMachineCheckpoint {
    input: String,
    prior_messages: Vec<ModelMessage>,
    allowed_tools: Option<Vec<String>>,
    allow_run_adf: bool,
    max_steps: Option<u32>,
}

impl agent_core::harness::AgentMachine for RuntimeAgent {
    fn metadata(&self) -> AgentMetadata {
        Agent::metadata(self).with_capability("durable_checkpoint")
    }

    fn initial_checkpoint(
        &self,
        request: &MachineStartRequest,
    ) -> Result<CheckpointEnvelope, MachineError> {
        match self {
            Self::AgentLoop(agent) => agent.initial_checkpoint(request),
            Self::Echo(_) | Self::SingleTurn(_) => encode_legacy_checkpoint(request),
        }
    }

    fn start(&self, request: MachineStartRequest) -> MachineStream {
        match self {
            Self::AgentLoop(agent) => agent.start(request),
            Self::Echo(agent) => legacy_machine_stream(agent.run(machine_run_request(request))),
            Self::SingleTurn(agent) => {
                legacy_machine_stream(agent.run(machine_run_request(request)))
            }
        }
    }

    fn resume(&self, request: MachineResumeRequest) -> MachineStream {
        match self {
            Self::AgentLoop(agent) => agent.resume(request),
            Self::Echo(agent) => legacy_resume(agent, request),
            Self::SingleTurn(agent) => legacy_resume(agent.as_ref(), request),
        }
    }
}

fn encode_legacy_checkpoint(
    request: &MachineStartRequest,
) -> Result<CheckpointEnvelope, MachineError> {
    let payload = serde_json::to_value(LegacyMachineCheckpoint {
        input: request.input.clone(),
        prior_messages: request.prior_messages.clone(),
        allowed_tools: request.allowed_tools.clone(),
        allow_run_adf: request.allow_run_adf,
        max_steps: request.max_steps,
    })
    .map_err(|_| MachineError {
        code: "checkpoint_encode_failed".into(),
        message: "the legacy agent checkpoint could not be encoded".into(),
        retryable: false,
    })?;
    let checkpoint = CheckpointEnvelope {
        agent_kind: "legacy-agent".into(),
        schema_version: 1,
        codec: CheckpointCodec::Json,
        payload,
    };
    checkpoint.validate().map_err(|error| MachineError {
        code: "checkpoint_invalid".into(),
        message: error.to_string(),
        retryable: false,
    })?;
    Ok(checkpoint)
}

fn machine_run_request(request: MachineStartRequest) -> RunRequest {
    RunRequest {
        run_id: request.run_id,
        input: request.input,
        prior_messages: request.prior_messages,
        allowed_tools: request.allowed_tools,
        allow_run_adf: request.allow_run_adf,
        max_steps: request.max_steps,
        cancellation: request.cancellation,
    }
}

fn legacy_resume<A: Agent>(agent: &A, request: MachineResumeRequest) -> MachineStream {
    if request.checkpoint.agent_kind != "legacy-agent"
        || request.checkpoint.schema_version != 1
        || request.checkpoint.codec != CheckpointCodec::Json
    {
        return machine_failure_stream(
            "checkpoint_incompatible",
            "the checkpoint is not compatible with the legacy agent adapter",
            false,
        );
    }
    let checkpoint: LegacyMachineCheckpoint =
        match serde_json::from_value(request.checkpoint.payload) {
            Ok(checkpoint) => checkpoint,
            Err(_) => {
                return machine_failure_stream(
                    "checkpoint_decode_failed",
                    "the legacy agent checkpoint payload is invalid",
                    false,
                );
            }
        };
    legacy_machine_stream(agent.run(RunRequest {
        run_id: request.run_id,
        input: checkpoint.input,
        prior_messages: checkpoint.prior_messages,
        allowed_tools: checkpoint.allowed_tools,
        allow_run_adf: checkpoint.allow_run_adf,
        max_steps: checkpoint.max_steps,
        cancellation: request.cancellation,
    }))
}

fn legacy_machine_stream(mut events: AgentEventStream) -> MachineStream {
    Box::pin(stream! {
        while let Some(event) = events.next().await {
            match event {
                AgentEvent::Completed { finish_reason } => {
                    yield MachineOutput::Yield(StepOutcome::Complete { finish_reason });
                    return;
                }
                AgentEvent::Failed { code, message, retryable } => {
                    yield MachineOutput::Yield(StepOutcome::Failed {
                        error: MachineError { code, message, retryable },
                    });
                    return;
                }
                AgentEvent::Cancelled => {
                    yield MachineOutput::Yield(StepOutcome::Cancelled);
                    return;
                }
                event => yield MachineOutput::Event(event),
            }
        }
        yield MachineOutput::Yield(StepOutcome::Failed {
            error: MachineError {
                code: "agent_protocol_violation".into(),
                message: "legacy agent event stream ended without a terminal event".into(),
                retryable: false,
            },
        });
    })
}

fn machine_failure_stream(code: &str, message: &str, retryable: bool) -> MachineStream {
    let error = MachineError {
        code: code.into(),
        message: message.into(),
        retryable,
    };
    Box::pin(futures_stream::once(async move {
        MachineOutput::Yield(StepOutcome::Failed { error })
    }))
}

#[derive(Clone)]
struct AppState {
    runtime: RunRuntime<AppRuntimeAgent>,
    config: Option<Arc<HarnessConfig>>,
    approvals: InMemoryApprovalBroker,
    events: Arc<EventRuntime>,
    orchestration: Arc<OrchestrationRuntime>,
}

struct OrchestrationRuntime {
    sessions: Arc<dyn SessionStore>,
    context: ContextEngine,
    skills: SkillOrchestrator,
    memory_store: Arc<dyn MemoryStore>,
    memory_proposals: Arc<dyn MemoryProposalStore>,
    memory_extractor: Arc<dyn MemoryExtractor>,
    memory_writer: MemoryWriter,
    config: OrchestrationConfig,
    agent_instruction: String,
    agent_profile: String,
    model_profile: String,
    model_context_tokens: u64,
    reserved_output_tokens: u64,
    tools: Vec<ToolDefinition>,
    tool_runtime: Option<ToolRuntimeInspection>,
}

struct ServerDeliveryRouter {
    memories: Arc<dyn MemoryStore>,
    proposals: Arc<dyn MemoryProposalStore>,
    flows: Option<Arc<dyn FlowStore>>,
}

impl DeliveryRouter for ServerDeliveryRouter {
    fn deliver(&self, delivery: Delivery, event: EventEnvelope) -> EventFuture<'_, ()> {
        Box::pin(async move {
            match &delivery.target {
                DeliveryTarget::RustHandler { handler } if handler == "memory_write_approval" => {
                    self.handle_memory_approval(event).await
                }
                DeliveryTarget::RustHandler { .. } => Err(EventError::invalid(
                    "delivery references an unknown Rust handler",
                )),
                DeliveryTarget::WakeRun { run_id, .. } => {
                    let flows = self.flows.as_ref().ok_or_else(|| {
                        EventError::backend("durable flow store is not configured")
                    })?;
                    flows
                        .wake(WakeFlowRun {
                            run_id: *run_id,
                            subscription_id: delivery.delivery_id.subscription_id,
                            event_id: delivery.delivery_id.event_id,
                            woken_at_ms: event.event.recorded_at_ms,
                        })
                        .await
                        .map(|_| ())
                        .map_err(flow_event_error)
                }
            }
        })
    }
}

struct ServerFlowEffectRouter {
    events: Arc<EventRuntime>,
    jobs: Arc<JobRuntime>,
}

impl FlowEffectRouter for ServerFlowEffectRouter {
    fn execute(&self, effect: FlowEffect) -> agent_core::harness::FlowFuture<'_, ()> {
        Box::pin(async move {
            match effect.request {
                EffectRequest::PublishEvent { command } => self
                    .events
                    .publish(command)
                    .await
                    .map(|_| ())
                    .map_err(event_flow_error),
                EffectRequest::ScheduleTimer { command } => self
                    .events
                    .schedule_once(command)
                    .await
                    .map(|_| ())
                    .map_err(event_flow_error),
                EffectRequest::StartJob { command } => self
                    .jobs
                    .submit(command)
                    .await
                    .map(|_| ())
                    .map_err(job_flow_error),
            }
        })
    }
}

struct ServerJobRouter;

impl JobRouter for ServerJobRouter {
    fn execute(&self, job: JobRecord) -> JobExecutionFuture {
        Box::pin(async move {
            match job.command.kind.as_str() {
                "builtin.delay" => {
                    let delay_ms = job
                        .command
                        .input
                        .get("delay_ms")
                        .and_then(serde_json::Value::as_u64)
                        .ok_or_else(|| JobExecutionError {
                            code: "invalid_job_input".into(),
                            message: "builtin.delay requires an integer delay_ms".into(),
                            retryable: false,
                            retry_after_ms: None,
                        })?;
                    if delay_ms > 60_000 {
                        return Err(JobExecutionError {
                            code: "job_delay_too_large".into(),
                            message: "builtin.delay cannot exceed 60000 milliseconds".into(),
                            retryable: false,
                            retry_after_ms: None,
                        });
                    }
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    Ok(job
                        .command
                        .input
                        .get("value")
                        .cloned()
                        .unwrap_or(serde_json::Value::Null))
                }
                _ => Err(JobExecutionError {
                    code: "job_kind_not_found".into(),
                    message: "no worker is registered for the requested job kind".into(),
                    retryable: false,
                    retry_after_ms: None,
                }),
            }
        })
    }
}

impl ServerDeliveryRouter {
    async fn handle_memory_approval(&self, event: EventEnvelope) -> Result<(), EventError> {
        if event.event.topic != "memory.approval.resolved" {
            return Err(EventError::invalid(
                "memory approval handler received the wrong topic",
            ));
        }
        let approval_id = event
            .event
            .correlation_id
            .as_deref()
            .ok_or_else(|| EventError::invalid("memory approval event has no correlation id"))?
            .parse::<MemoryApprovalId>()
            .map_err(|_| EventError::invalid("memory approval correlation id is invalid"))?;
        let decision = event
            .event
            .payload
            .get("decision")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| EventError::invalid("memory approval decision is missing"))?;
        let reason = event
            .event
            .payload
            .get("reason")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned);
        let proposal = self
            .proposals
            .get_proposal(approval_id)
            .await
            .map_err(memory_event_error)?
            .ok_or(EventError::NotFound)?;
        let requested_status = match decision {
            "approve" => MemoryApprovalStatus::Approved,
            "deny" => MemoryApprovalStatus::Denied,
            _ => {
                return Err(EventError::invalid(
                    "memory approval decision must be approve or deny",
                ));
            }
        };
        if proposal.status != MemoryApprovalStatus::Pending {
            return if proposal.status == requested_status {
                Ok(())
            } else {
                Err(EventError::Conflict(
                    "memory approval was already resolved differently".into(),
                ))
            };
        }
        let status = match decision {
            "approve" => {
                self.memories
                    .put(PutMemory {
                        record: proposal.candidate.proposed.clone(),
                        expected_absent: true,
                    })
                    .await
                    .map_err(memory_event_error)?;
                MemoryApprovalStatus::Approved
            }
            "deny" => MemoryApprovalStatus::Denied,
            _ => unreachable!("validated memory approval decision"),
        };
        self.proposals
            .resolve_proposal(ResolveMemoryWriteProposal {
                approval_id,
                decision: status,
                reason,
                resolved_at_ms: event.event.recorded_at_ms,
            })
            .await
            .map_err(memory_event_error)?;
        Ok(())
    }
}

fn memory_event_error(error: agent_core::memory::MemoryError) -> EventError {
    EventError::backend(format!("memory approval handler failed: {error}"))
}

fn flow_event_error(error: FlowError) -> EventError {
    match error {
        FlowError::Invalid(message) => EventError::invalid(message),
        FlowError::NotFound => EventError::NotFound,
        FlowError::Conflict(message) => EventError::Conflict(message),
        FlowError::Backend(message) => EventError::backend(message),
    }
}

fn event_flow_error(error: EventError) -> FlowError {
    match error {
        EventError::Invalid(message) => FlowError::Invalid(message),
        EventError::NotFound => FlowError::NotFound,
        EventError::Conflict(message) => FlowError::Conflict(message),
        EventError::Backend(message) => FlowError::Backend(message),
    }
}

fn job_flow_error(error: agent_core::harness::JobStoreError) -> FlowError {
    match error {
        agent_core::harness::JobStoreError::Invalid(message) => FlowError::Invalid(message),
        agent_core::harness::JobStoreError::NotFound => FlowError::NotFound,
        agent_core::harness::JobStoreError::Conflict(message) => FlowError::Conflict(message),
        agent_core::harness::JobStoreError::Backend(message) => FlowError::Backend(message),
    }
}

#[derive(Clone, Default)]
struct InMemoryApprovalBroker {
    records: Arc<Mutex<HashMap<ApprovalId, ApprovalRecord>>>,
}

struct ApprovalRecord {
    request: ApprovalRequest,
    sender: Option<oneshot::Sender<ApprovalResolution>>,
    resolution: Option<ApprovalResolution>,
}

struct ApprovalWaitGuard {
    approval_id: ApprovalId,
    records: Arc<Mutex<HashMap<ApprovalId, ApprovalRecord>>>,
}

impl Drop for ApprovalWaitGuard {
    fn drop(&mut self) {
        let mut records = self
            .records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if records
            .get(&self.approval_id)
            .is_some_and(|record| record.resolution.is_none())
        {
            records.remove(&self.approval_id);
        }
    }
}

impl ApprovalPort for InMemoryApprovalBroker {
    fn request(&self, request: ApprovalRequest) -> ApprovalFuture {
        let approval_id = request.approval_id;
        let (sender, receiver) = oneshot::channel();
        let mut records = self
            .records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if records.contains_key(&approval_id) {
            return Box::pin(async {
                Err(ApprovalError::new(
                    "approval_id_conflict",
                    "approval id is already registered",
                ))
            });
        }
        records.insert(
            approval_id,
            ApprovalRecord {
                request,
                sender: Some(sender),
                resolution: None,
            },
        );
        drop(records);

        let guard = ApprovalWaitGuard {
            approval_id,
            records: Arc::clone(&self.records),
        };
        Box::pin(async move {
            let _guard = guard;
            receiver.await.map_err(|_| {
                ApprovalError::new(
                    "approval_unavailable",
                    "approval request closed before it was resolved",
                )
            })
        })
    }
}

impl InMemoryApprovalBroker {
    fn resolve(
        &self,
        run_id: RunId,
        approval_id: ApprovalId,
        resolution: ApprovalResolution,
    ) -> Result<ResolveApprovalOutcome, ResolveApprovalError> {
        let sender = {
            let mut records = self
                .records
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let Some(record) = records.get_mut(&approval_id) else {
                return Err(ResolveApprovalError::NotPending);
            };
            if record.request.run_id != run_id {
                return Err(ResolveApprovalError::NotPending);
            }
            if let Some(existing) = &record.resolution {
                return if existing == &resolution {
                    Ok(ResolveApprovalOutcome::AlreadyResolved)
                } else {
                    Err(ResolveApprovalError::ConflictingDecision)
                };
            }

            record.resolution = Some(resolution.clone());
            record.sender.take()
        };

        let Some(sender) = sender else {
            return Err(ResolveApprovalError::NotPending);
        };
        sender
            .send(resolution)
            .map_err(|_| ResolveApprovalError::NotPending)?;
        Ok(ResolveApprovalOutcome::Accepted)
    }

    fn remove_run(&self, run_id: RunId) {
        self.records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .retain(|_, record| record.request.run_id != run_id);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResolveApprovalOutcome {
    Accepted,
    AlreadyResolved,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResolveApprovalError {
    NotPending,
    ConflictingDecision,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
}

#[derive(Debug, Serialize)]
struct InfoResponse {
    service: &'static str,
    version: &'static str,
    agent: AgentMetadata,
    model: Option<ModelInfo>,
}

#[derive(Debug, Serialize)]
struct AgentInspectionResponse {
    service: &'static str,
    version: &'static str,
    agent: AgentMetadata,
    model: Option<ModelInfo>,
    system: SystemInspection,
    skills: SkillInspection,
    tools: Vec<ToolDefinition>,
    approval: ApprovalInspection,
    tool_runtime: Option<ToolRuntimeInspection>,
    memory: MemoryInspection,
}

#[derive(Debug, Clone, Serialize)]
struct ApprovalInspection {
    review_level: u8,
    risk_levels: ToolRiskLevelInspection,
}

#[derive(Debug, Clone, Serialize)]
struct ToolRiskLevelInspection {
    low: u8,
    medium: u8,
    high: u8,
}

#[derive(Debug, Clone, Serialize)]
struct ToolRuntimeInspection {
    process_sandbox: ProcessSandboxDescriptor,
    search_backend: SearchBackendDescriptor,
    #[serde(skip_serializing_if = "Option::is_none")]
    script_runtime: Option<ScriptRuntimeDescriptor>,
    adf_enabled: bool,
}

#[derive(Debug, Serialize)]
struct SkillInspection {
    store: ComponentDescriptor,
    packages: Vec<SkillDescriptor>,
}

#[derive(Debug, Serialize)]
struct SystemInspection {
    source: &'static str,
    content: String,
}

#[derive(Debug, Serialize)]
struct MemoryInspection {
    enabled: bool,
    scopes: Vec<String>,
    store: MemoryComponentDescriptor,
    records: Vec<MemoryRecord>,
    pending_approvals: Vec<MemoryWriteProposal>,
    has_more: bool,
}

#[derive(Debug, Serialize)]
struct ModelInfo {
    profile: String,
    provider: &'static str,
    model: String,
    input_modalities: Vec<Modality>,
    output_modalities: Vec<Modality>,
}

#[derive(Debug, Deserialize)]
struct CreateRunRequest {
    input: String,
    #[serde(default)]
    stream: bool,
    #[serde(default)]
    max_steps: Option<u32>,
    #[serde(default)]
    skills: Vec<SkillChoice>,
}

#[derive(Debug, Clone, Deserialize)]
struct SkillChoice {
    skill_id: String,
    version: String,
}

#[derive(Debug, Deserialize)]
struct CreateSessionRequest {
    #[serde(default = "default_agent_profile")]
    agent_profile: String,
    #[serde(default)]
    title: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct ListSessionsQuery {
    #[serde(default)]
    status: Option<SessionStatus>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct SubmitSessionRunRequest {
    input: String,
    expected_revision: u64,
    idempotency_key: String,
    #[serde(default)]
    max_steps: Option<u32>,
    #[serde(default)]
    skills: Vec<SkillChoice>,
}

#[derive(Debug, Serialize)]
struct RunAcceptedResponse {
    session_id: SessionId,
    run_id: RunId,
    session_revision: u64,
    replayed: bool,
    context_fingerprint: Option<String>,
    max_steps: u32,
}

#[derive(Debug, Deserialize)]
struct ArchiveSessionRequest {
    expected_revision: u64,
}

#[derive(Debug, Default, Deserialize)]
struct SessionMessagesQuery {
    before: Option<u64>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
struct RunEventsQuery {
    #[serde(default)]
    after_seq: Option<u64>,
    #[serde(default)]
    follow: Option<bool>,
}

#[derive(Debug, Serialize)]
struct ApiError {
    code: String,
    message: String,
    retryable: bool,
}

#[derive(Debug, Serialize)]
struct CancelRunResponse {
    run_id: RunId,
    status: &'static str,
}

#[derive(Debug, Deserialize)]
struct ResolveApprovalRequest {
    decision: ApprovalDecision,
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Debug, Serialize)]
struct ResolveApprovalResponse {
    run_id: RunId,
    approval_id: ApprovalId,
    resolution: ApprovalResolution,
    replayed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum MemoryApprovalDecision {
    Approve,
    Deny,
}

#[derive(Debug, Deserialize)]
struct ResolveMemoryApprovalRequest {
    decision: MemoryApprovalDecision,
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Debug, Serialize)]
struct ResolveMemoryApprovalResponse {
    approval_id: MemoryApprovalId,
    event_id: EventId,
    replayed: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("mina_server=info,mina::observation=info")),
        )
        .init();

    let config = env::var_os("MINA_CONFIG")
        .map(HarnessConfig::load)
        .transpose()?
        .map(Arc::new);

    if let Some(config) = &config {
        let model = config.default_model();
        tracing::info!(
            profile = config.default_model_name(),
            provider = model.provider.kind(),
            model = model.model,
            "loaded model configuration"
        );
    }

    let orchestration_config = config
        .as_deref()
        .map(|config| config.orchestration().clone())
        .unwrap_or_default();
    let estimator: Arc<dyn TokenEstimator> = Arc::new(HeuristicTokenEstimator);
    let approvals = InMemoryApprovalBroker::default();
    let observation_worker = Arc::new(AsyncObservationHook::new(
        Arc::new(HookObservationExporter::new(Arc::new(
            TracingObservationHook,
        ))),
        AsyncObservationConfig::default(),
    )?);
    let observation_hook: Arc<dyn ObservationHook> = observation_worker.clone();
    let (agent, tools, tool_runtime) = build_agent(
        config.as_deref(),
        approvals.clone(),
        Arc::clone(&estimator),
        Arc::clone(&observation_hook),
    )?;
    tracing::info!(agent = agent.metadata().name, "configured agent runtime");

    let mut harness = Harness::new(ObservedMachine::new(agent, Arc::clone(&observation_hook)));
    if let Some(config) = &config {
        harness = harness.with_run_timeout(Duration::from_secs(config.agent().run_timeout_seconds));
    }
    let store_path = env::var_os("MINA_RUN_STORE_PATH")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from("data/runs.sqlite3"));
    let sqlite_store = Arc::new(SqliteRunStore::open(&store_path).await?);
    tracing::info!(path = %sqlite_store.path().display(), "opened durable run store");
    let store: Arc<dyn RunStore> = sqlite_store.clone();
    let sessions: Arc<dyn SessionStore> = sqlite_store.clone();
    let artifacts: Arc<dyn ContextArtifactStore> = sqlite_store.clone();
    let memory_adapter =
        Arc::new(SqliteMemoryStore::open(&store_path, "memory:sqlite-primary").await?);
    let memory_store: Arc<dyn MemoryStore> = memory_adapter.clone();
    let memory_proposals: Arc<dyn MemoryProposalStore> = memory_adapter.clone();
    let memory_retriever: Arc<dyn MemoryRetriever> = memory_adapter;
    let memory_policy: Arc<dyn MemoryWritePolicy> = Arc::new(HostMemoryWritePolicy);
    let memory_extractor: Arc<dyn MemoryExtractor> = Arc::new(RuleMemoryExtractor);
    let memory_writer = MemoryWriter::new(Arc::clone(&memory_store), memory_policy);
    let deterministic_summary: Arc<dyn SummaryGenerator> = Arc::new(DeterministicSummaryGenerator);
    let summary: Arc<dyn SummaryGenerator> = if let Some(config) = config.as_deref() {
        let model = config.default_model();
        let provider = OpenAiCompatibleProvider::from_model_config(model)?
            .with_token_estimator(Arc::clone(&estimator));
        let provider = ObservedModel::new(provider, Arc::clone(&observation_hook));
        let primary: Arc<dyn SummaryGenerator> = Arc::new(ModelSummaryGenerator::new(
            provider,
            Duration::from_secs(config.agent().model_timeout_seconds),
        ));
        Arc::new(FallbackSummaryGenerator::new(
            primary,
            deterministic_summary,
        ))
    } else {
        deterministic_summary
    };
    let compressor: Arc<dyn ContextCompressor> = match orchestration_config.compressor {
        ContextStrategy::NoopFail => Arc::new(NoopFailCompressor::new(Arc::clone(&estimator))),
        ContextStrategy::SlidingWindow => {
            Arc::new(SlidingWindowCompressor::new(Arc::clone(&estimator)))
        }
        ContextStrategy::Hybrid => Arc::new(HybridCompressor::new(Arc::clone(&estimator), summary)),
    };
    let context = ContextEngine::new(
        Arc::clone(&sessions),
        memory_retriever,
        compressor,
        artifacts,
    );
    let skill_store: Arc<dyn SkillStore> = Arc::new(FilesystemSkillStore::new(
        orchestration_config.skill_directory.clone(),
        "skills:filesystem-primary",
    ));
    let skills = SkillOrchestrator::new(skill_store);
    let event_adapter =
        Arc::new(SqliteEventStore::open(&store_path, "events:sqlite-primary").await?);
    let event_store: Arc<dyn EventStore> = event_adapter.clone();
    let flow_store: Arc<dyn FlowStore> = event_adapter.clone();
    let job_store: Arc<dyn JobStore> = event_adapter;
    let delivery_router: Arc<dyn DeliveryRouter> = Arc::new(ServerDeliveryRouter {
        memories: Arc::clone(&memory_store),
        proposals: Arc::clone(&memory_proposals),
        flows: Some(Arc::clone(&flow_store)),
    });
    let events = Arc::new(EventRuntime::new(
        event_store,
        delivery_router,
        EventRuntimeConfig::default(),
    ));
    let jobs = Arc::new(JobRuntime::new(
        job_store,
        Arc::new(ServerJobRouter),
        Arc::clone(&events),
        JobRuntimeConfig::default(),
    ));
    let flow_effects = Arc::new(FlowEffectRuntime::new(
        Arc::clone(&flow_store),
        Arc::new(ServerFlowEffectRouter {
            events: Arc::clone(&events),
            jobs: Arc::clone(&jobs),
        }),
        FlowEffectRuntimeConfig::default(),
    ));
    for proposal in memory_proposals.pending_proposals(1_000).await? {
        ensure_memory_approval(&events, &proposal).await?;
    }
    let approval_cleanup = approvals.clone();
    let runtime = RunRuntime::new_durable(harness, store, flow_store, move |run_id| {
        approval_cleanup.remove_run(run_id);
    });
    let recovered = runtime.recover_interrupted().await?;
    if recovered > 0 {
        tracing::warn!(
            recovered,
            "closed runs interrupted by the previous server process"
        );
    }
    runtime.start_flow_worker();
    let pending_session_finalizations = sessions.pending_finalizations().await?;
    let reconciled_sessions = pending_session_finalizations.len();
    for (session_id, run) in pending_session_finalizations {
        sessions
            .finalize_run(FinalizeSessionRun {
                session_id,
                run,
                finalized_at_ms: run_runtime::unix_time_ms(),
            })
            .await?;
    }
    if reconciled_sessions > 0 {
        tracing::warn!(
            reconciled_sessions,
            "reconciled terminal session runs after restart"
        );
    }
    start_session_finalization_worker(Arc::clone(&sessions));

    let state = AppState {
        runtime,
        config: config.clone(),
        approvals,
        events: Arc::clone(&events),
        orchestration: Arc::new(OrchestrationRuntime {
            sessions,
            context,
            skills,
            memory_store,
            memory_proposals,
            memory_extractor,
            memory_writer,
            config: orchestration_config,
            agent_instruction: config
                .as_deref()
                .map(|config| config.agent().system_prompt.clone())
                .unwrap_or_else(|| "You are a helpful agent.".into()),
            agent_profile: "default".into(),
            model_profile: config
                .as_deref()
                .map(|config| config.default_model_name().to_owned())
                .unwrap_or_else(|| "echo".into()),
            model_context_tokens: config
                .as_deref()
                .and_then(|config| config.default_model().context_window)
                .map(u64::from)
                .unwrap_or(32_768),
            reserved_output_tokens: config
                .as_deref()
                .and_then(|config| config.default_model().max_output_tokens)
                .map(u64::from)
                .unwrap_or(2_048),
            tools,
            tool_runtime,
        }),
    };
    start_event_workers(events, flow_effects, jobs);
    let app = Router::new()
        .route("/healthz", get(health))
        .route("/api/v1/info", get(info))
        .route("/api/v1/debug/agent", get(inspect_agent))
        .route("/api/v1/runs", post(create_run))
        .route("/api/v1/runs/{run_id}", get(get_run))
        .route("/api/v1/runs/{run_id}/events", get(get_run_events))
        .route("/api/v1/runs/{run_id}/cancel", post(cancel_run))
        .route("/api/v1/sessions", get(list_sessions).post(create_session))
        .route("/api/v1/sessions/{session_id}", get(get_session))
        .route(
            "/api/v1/sessions/{session_id}/messages",
            get(get_session_messages),
        )
        .route(
            "/api/v1/sessions/{session_id}/runs",
            post(submit_session_run),
        )
        .route(
            "/api/v1/sessions/{session_id}/archive",
            post(archive_session),
        )
        .route(
            "/api/v1/runs/{run_id}/approvals/{approval_id}",
            post(resolve_approval),
        )
        .route("/api/v1/memories/approvals", get(list_memory_approvals))
        .route(
            "/api/v1/memories/approvals/{approval_id}",
            post(resolve_memory_approval),
        )
        .with_state(state);

    let address = env::var("MINA_SERVER_ADDR").unwrap_or_else(|_| "127.0.0.1:8787".into());
    let listener = TcpListener::bind(&address).await?;
    let local_address = listener.local_addr()?;
    tracing::info!(address = %local_address, "mina server listening");

    let serve_result = axum::serve(listener, app).await;
    let observation_result = observation_worker.shutdown().await;
    serve_result?;
    observation_result?;
    Ok(())
}

fn build_agent(
    config: Option<&HarnessConfig>,
    approvals: InMemoryApprovalBroker,
    estimator: Arc<dyn TokenEstimator>,
    hook: Arc<dyn ObservationHook>,
) -> AgentBuildResult {
    let Some(config) = config else {
        return Ok((RuntimeAgent::Echo(EchoAgent), Vec::new(), None));
    };

    match config.agent().kind {
        AgentKind::Echo => Ok((RuntimeAgent::Echo(EchoAgent), Vec::new(), None)),
        AgentKind::SingleTurn => {
            let model = config.default_model();
            let provider =
                OpenAiCompatibleProvider::from_model_config(model)?.with_token_estimator(estimator);
            let provider = ObservedModel::new(provider, hook);
            Ok((
                RuntimeAgent::SingleTurn(Box::new(
                    SingleTurnAgent::new(
                        provider,
                        model.model.clone(),
                        "",
                        model.max_output_tokens,
                    )
                    .with_model_timeout(Duration::from_secs(config.agent().model_timeout_seconds)),
                )),
                Vec::new(),
                None,
            ))
        }
        AgentKind::AgentLoop => {
            let model = config.default_model();
            let provider =
                OpenAiCompatibleProvider::from_model_config(model)?.with_token_estimator(estimator);
            let provider = ObservedModel::new(provider, Arc::clone(&hook));
            let workspace = env::current_dir()?;
            let process_sandbox: Arc<dyn ProcessSandbox> = Arc::new(HostProcessSandbox::default());
            let search_backend: Arc<dyn SearchBackend> =
                Arc::new(WorkspaceSearchBackend::new(&workspace)?);
            let tool_runtime = ToolRuntimeInspection {
                process_sandbox: process_sandbox.descriptor(),
                search_backend: search_backend.descriptor(),
                script_runtime: None,
                adf_enabled: config.adf().enabled,
            };
            let mut tools = BuiltinToolCatalog::new(workspace)
                .with_process_sandbox(process_sandbox)
                .with_search_backend(search_backend)
                .enable_terminal_tools()
                .build()?;
            if config.jobs().enabled {
                tools.register(AsyncJobTool::new(
                    config.jobs().allowed_kinds.iter().cloned(),
                ))?;
            }
            let quickjs = if config.script().quickjs.enabled {
                let limits = script_limits(&config.script().quickjs);
                let runtime: Arc<dyn ScriptRuntime> =
                    Arc::new(QuickJsRuntime::new(QuickJsRuntimeConfig {
                        max_source_bytes: config.script().quickjs.max_source_bytes,
                        max_limits: limits,
                        max_concurrent_executions: config
                            .script()
                            .quickjs
                            .max_concurrent_executions,
                    })?);
                tools.register(JavaScriptEvalTool::new(Arc::clone(&runtime), limits))?;
                Some((runtime, limits))
            } else {
                None
            };
            let mut tools = RunAdfToolSession::new(tools);
            if config.adf().enabled {
                let (runtime, limits) = quickjs
                    .as_ref()
                    .expect("validated configuration enables QuickJS for ADF");
                tools = tools.with_adf(
                    Arc::new(InMemoryAdfArtifactStore::new()),
                    Arc::new(RunScopedJavaScriptPolicy::new(runtime.descriptor())),
                    Arc::new(JavaScriptAdfExecutor::new(Arc::clone(runtime), *limits)),
                    config.adf().max_active_per_run,
                )?;
            }
            let mut tool_runtime = tool_runtime;
            tool_runtime.script_runtime = quickjs.as_ref().map(|(runtime, _)| runtime.descriptor());
            let definitions = tools.definitions();
            let tools = ObservedTools::new(tools, hook);
            Ok((
                RuntimeAgent::AgentLoop(Box::new(
                    AgentLoop::new(
                        provider,
                        tools,
                        model.model.clone(),
                        "",
                        model.max_output_tokens,
                        config.agent().max_steps,
                    )
                    .with_approval_policy(config.approval().policy())
                    .with_timeouts(
                        Duration::from_secs(config.agent().model_timeout_seconds),
                        Duration::from_secs(config.agent().tool_timeout_seconds),
                    )
                    .with_tool_call_strategy(config.agent().tool_call_strategy)
                    .with_approval_port(approvals),
                )),
                definitions,
                Some(tool_runtime),
            ))
        }
    }
}

fn script_limits(config: &QuickJsConfig) -> ScriptLimits {
    ScriptLimits {
        timeout_ms: config.timeout_ms,
        memory_bytes: config.memory_bytes,
        max_stack_bytes: config.max_stack_bytes,
        max_output_bytes: config.max_output_bytes,
        ..ScriptLimits::default()
    }
}

type AgentBuildResult = Result<
    (
        RuntimeAgent,
        Vec<ToolDefinition>,
        Option<ToolRuntimeInspection>,
    ),
    Box<dyn std::error::Error>,
>;

async fn ensure_memory_approval(
    events: &EventRuntime,
    proposal: &MemoryWriteProposal,
) -> Result<(), EventError> {
    let approval_key = proposal.approval_id.to_string();
    events
        .subscribe(CreateSubscription {
            subscription_id: SubscriptionId::stable("memory-approval", &approval_key),
            owner: SubscriptionOwner::System {
                component: "memory_writer".into(),
            },
            scope: SubscriptionScope::Global,
            filter: EventFilter {
                topics: vec!["memory.approval.resolved".into()],
                correlation_id: Some(approval_key.clone()),
                ..EventFilter::default()
            },
            delivery: DeliveryTarget::RustHandler {
                handler: "memory_write_approval".into(),
            },
            mode: SubscriptionMode::Once,
            start_position: StartPosition::Beginning,
            expires_at_ms: None,
            max_deliveries: Some(1),
            created_at_ms: proposal.created_at_ms,
        })
        .await?;
    events
        .publish(PublishEvent {
            event_id: EventId::stable("memory-approval-requested", &approval_key),
            topic: "memory.approval.requested".into(),
            event_type: "memory.approval.requested".into(),
            schema_version: 1,
            source: EventSource::System,
            subject: Some(format!("memory/{}", proposal.candidate.proposed.memory_id)),
            correlation_id: Some(approval_key),
            causation_id: None,
            occurred_at_ms: proposal.created_at_ms,
            recorded_at_ms: proposal.created_at_ms,
            payload: serde_json::json!({
                "approval_id": proposal.approval_id,
                "memory_id": proposal.candidate.proposed.memory_id,
                "sensitivity": proposal.candidate.sensitivity,
                "reason": proposal.reason,
            }),
        })
        .await?;
    Ok(())
}

fn start_event_workers(
    events: Arc<EventRuntime>,
    flow_effects: Arc<FlowEffectRuntime>,
    jobs: Arc<JobRuntime>,
) {
    tokio::spawn(async move {
        let worker_id = format!("server-event-worker-{}", std::process::id());
        let mut interval = tokio::time::interval(Duration::from_millis(250));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let now_ms = run_runtime::unix_time_ms();
            if let Err(error) = events.fire_timers_once(&worker_id, now_ms).await {
                tracing::warn!(%error, "event timer tick failed");
            }
            if let Err(error) = events.dispatch_once(&worker_id, now_ms).await {
                tracing::warn!(%error, "event delivery tick failed");
            }
            if let Err(error) = flow_effects.dispatch_once(&worker_id, now_ms).await {
                tracing::warn!(%error, "flow effect tick failed");
            }
            if let Err(error) = jobs.execute_once(&worker_id, now_ms).await {
                tracing::warn!(%error, "job execution tick failed");
            }
            if let Err(error) = jobs.publish_notifications_once(&worker_id, now_ms).await {
                tracing::warn!(%error, "job notification tick failed");
            }
        }
    });
}

fn start_session_finalization_worker(sessions: Arc<dyn SessionStore>) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(250));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let pending = match sessions.pending_finalizations().await {
                Ok(pending) => pending,
                Err(error) => {
                    tracing::warn!(%error, "session finalization scan failed");
                    continue;
                }
            };
            for (session_id, run) in pending {
                let run_id = run.run_id;
                if let Err(error) = sessions
                    .finalize_run(FinalizeSessionRun {
                        session_id,
                        run,
                        finalized_at_ms: run_runtime::unix_time_ms(),
                    })
                    .await
                {
                    tracing::warn!(%session_id, %run_id, %error, "session run finalization failed");
                }
            }
        }
    });
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

async fn info(State(state): State<AppState>) -> Json<InfoResponse> {
    Json(InfoResponse {
        service: "mina-server",
        version: env!("CARGO_PKG_VERSION"),
        agent: state.runtime.metadata(),
        model: state.config.as_deref().map(model_info),
    })
}

async fn inspect_agent(State(state): State<AppState>) -> Response {
    let skills = match state.orchestration.skills.list().await {
        Ok(skills) => skills,
        Err(error) => {
            return api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "skill_inspection_failed",
                error.to_string(),
                true,
            );
        }
    };
    let memory_enabled = state.orchestration.config.memory_enabled;
    let memory_scopes = if memory_enabled {
        vec![state.orchestration.config.memory_scope.clone()]
    } else {
        Vec::new()
    };
    let memory_page = if memory_enabled {
        match state
            .orchestration
            .memory_store
            .list(MemoryListQuery {
                scopes: memory_scopes.iter().cloned().map(MemoryScope).collect(),
                limit: 100,
                ..MemoryListQuery::default()
            })
            .await
        {
            Ok(page) => page,
            Err(error) => {
                return api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "memory_inspection_failed",
                    error.to_string(),
                    true,
                );
            }
        }
    } else {
        MemoryPage {
            records: Vec::new(),
            has_more: false,
        }
    };
    let pending_memory_approvals = if memory_enabled {
        match state
            .orchestration
            .memory_proposals
            .pending_proposals(100)
            .await
        {
            Ok(proposals) => proposals,
            Err(error) => {
                return api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "memory_approval_inspection_failed",
                    error.to_string(),
                    true,
                );
            }
        }
    } else {
        Vec::new()
    };

    let review_level = state
        .config
        .as_deref()
        .map_or(ToolApprovalPolicy::DEFAULT_REVIEW_LEVEL, |config| {
            config.approval().review_level
        });

    Json(AgentInspectionResponse {
        service: "mina-server",
        version: env!("CARGO_PKG_VERSION"),
        agent: state.runtime.metadata(),
        model: state.config.as_deref().map(model_info),
        system: SystemInspection {
            source: "agent_config",
            content: state.orchestration.agent_instruction.clone(),
        },
        skills: SkillInspection {
            store: state.orchestration.skills.descriptor(),
            packages: skills,
        },
        tools: state.orchestration.tools.clone(),
        approval: ApprovalInspection {
            review_level,
            risk_levels: ToolRiskLevelInspection {
                low: ToolRiskLevel::Low.review_level(),
                medium: ToolRiskLevel::Medium.review_level(),
                high: ToolRiskLevel::High.review_level(),
            },
        },
        tool_runtime: state.orchestration.tool_runtime.clone(),
        memory: MemoryInspection {
            enabled: memory_enabled,
            scopes: memory_scopes,
            store: state.orchestration.memory_store.descriptor(),
            records: memory_page.records,
            pending_approvals: pending_memory_approvals,
            has_more: memory_page.has_more,
        },
    })
    .into_response()
}

fn model_info(config: &HarnessConfig) -> ModelInfo {
    let model = config.default_model();
    ModelInfo {
        profile: config.default_model_name().into(),
        provider: model.provider.kind(),
        model: model.model.clone(),
        input_modalities: model.modalities.input.iter().copied().collect(),
        output_modalities: model.modalities.output.iter().copied().collect(),
    }
}

async fn create_run(
    State(state): State<AppState>,
    Json(request): Json<CreateRunRequest>,
) -> Response {
    let run_id = RunId::new();
    let max_steps = match effective_run_max_steps(&state, request.max_steps) {
        Ok(max_steps) => max_steps,
        Err(error) => return map_run_max_steps_error(error),
    };
    let prepared = match prepare_run_plan(
        &state,
        run_id,
        request.input,
        request.skills,
        max_steps,
        None,
        None,
    )
    .await
    {
        Ok(prepared) => prepared,
        Err(error) => return error,
    };
    let started = match state.runtime.start_planned(prepared.plan).await {
        Ok(started) => started,
        Err(error) => return map_runtime_error(error),
    };

    if request.stream {
        return run_event_response(state.runtime, started.run_id, 0, true, Some(started.events));
    }

    match wait_for_terminal(&state.runtime, started).await {
        Ok(response) => Json(response).into_response(),
        Err(error) => map_harness_error(error).into_response(),
    }
}

struct PreparedRun {
    plan: PlannedRun,
    context_fingerprint: String,
    max_steps: u32,
}

async fn prepare_run_plan(
    state: &AppState,
    run_id: RunId,
    input: String,
    explicit_skills: Vec<SkillChoice>,
    max_steps: u32,
    session_id: Option<SessionId>,
    through_message_ordinal: Option<u64>,
) -> Result<PreparedRun, Response> {
    if input.trim().is_empty() {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "invalid_input",
            "input must not be empty",
            false,
        ));
    }
    let defaults = state
        .orchestration
        .config
        .default_skills
        .iter()
        .map(|skill| (SkillId(skill.skill_id.clone()), skill.version.clone()))
        .collect();
    let explicit = explicit_skills
        .into_iter()
        .map(|skill| (SkillId(skill.skill_id), skill.version))
        .collect();
    let resolved_skills = state
        .orchestration
        .skills
        .resolve(ResolveSkillsRequest {
            explicit,
            profile_defaults: defaults,
            current_input: input.clone(),
            max_skills: state.orchestration.config.max_skills,
        })
        .await
        .map_err(|error| {
            api_error(
                StatusCode::BAD_REQUEST,
                "skill_resolution_failed",
                error.to_string(),
                false,
            )
        })?;
    let request_digest = request_digest(&input, &resolved_skills);
    let memory_scopes = if state.orchestration.config.memory_enabled {
        let mut scopes = vec![MemoryScope(state.orchestration.config.memory_scope.clone())];
        if let Some(session_id) = session_id {
            scopes.push(MemoryScope(format!("session:{session_id}")));
        }
        scopes
    } else {
        vec![MemoryScope("__memory_disabled__".into())]
    };
    let pack = state
        .orchestration
        .context
        .build(ContextBuildRequest {
            run_id,
            session_id,
            through_message_ordinal,
            current_input: input.clone(),
            request_digest: request_digest.clone(),
            agent_profile: state.orchestration.agent_profile.clone(),
            agent_instruction: state.orchestration.agent_instruction.clone(),
            model_profile: state.orchestration.model_profile.clone(),
            resolved_skills,
            memory_scopes,
            available_tools: state
                .orchestration
                .tools
                .iter()
                .map(|tool| tool.name.clone())
                .collect(),
            max_memories: state.orchestration.config.max_memories,
            budget: ContextBudget {
                model_context_tokens: state.orchestration.model_context_tokens,
                reserved_output_tokens: state.orchestration.reserved_output_tokens,
                reserved_tool_schema_tokens: state.orchestration.config.reserved_tool_schema_tokens,
                max_skill_tokens: state.orchestration.config.max_skill_tokens,
                max_memory_tokens: state.orchestration.config.max_memory_tokens,
                max_history_tokens: state.orchestration.config.max_history_tokens,
            },
            policy_version: state.orchestration.config.context_policy_version,
            now_ms: run_runtime::unix_time_ms(),
        })
        .await
        .map_err(|error| {
            api_error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "context_build_failed",
                error.to_string(),
                false,
            )
        })?;
    let context_fingerprint = pack.fingerprint.clone();
    let allow_run_adf = state
        .config
        .as_deref()
        .is_some_and(|config| config.adf().enabled);
    let manifest = serde_json::json!({
        "schema_version": 1,
        "context_fingerprint": pack.fingerprint,
        "context_budget": pack.budget,
        "skill_refs": pack.skill_refs,
        "memory_refs": pack.memory_refs,
        "artifact_refs": pack.artifact_refs,
        "components": pack.component_descriptors,
        "token_accounting": {
            "provider_usage_preferred": true,
            "fallback": "token_estimator"
        },
        "step_budget": {
            "max_steps": max_steps,
            "final_summary_step": true
        },
        "tool_grants": {
            "static_tools": &pack.effective_tools,
            "allow_run_adf": allow_run_adf,
        },
        "tool_runtime": {
            "script": state
                .orchestration
                .tool_runtime
                .as_ref()
                .and_then(|runtime| runtime.script_runtime.as_ref()),
            "adf_enabled": allow_run_adf,
        }
    });
    Ok(PreparedRun {
        plan: PlannedRun {
            run_id,
            input,
            prior_messages: pack.messages,
            allowed_tools: Some(pack.effective_tools),
            allow_run_adf,
            max_steps: Some(max_steps),
            context_fingerprint: Some(context_fingerprint.clone()),
            execution_manifest: Some(manifest),
        },
        context_fingerprint,
        max_steps,
    })
}

async fn create_session(
    State(state): State<AppState>,
    Json(request): Json<CreateSessionRequest>,
) -> Response {
    let agent_profile = request.agent_profile.trim();
    if agent_profile.is_empty() {
        return api_error(
            StatusCode::BAD_REQUEST,
            "invalid_agent_profile",
            "agent_profile must not be empty",
            false,
        );
    }
    let now_ms = run_runtime::unix_time_ms();
    match state
        .orchestration
        .sessions
        .create_session(CreateSession {
            session_id: SessionId::new(),
            agent_profile: agent_profile.into(),
            title: request
                .title
                .map(|title| title.trim().to_owned())
                .filter(|title| !title.is_empty()),
            created_at_ms: now_ms,
        })
        .await
    {
        Ok(session) => (StatusCode::CREATED, Json(session)).into_response(),
        Err(error) => map_session_error(error),
    }
}

async fn list_sessions(
    State(state): State<AppState>,
    Query(query): Query<ListSessionsQuery>,
) -> Response {
    match state
        .orchestration
        .sessions
        .list_sessions(
            query.status,
            query.limit.unwrap_or(100).clamp(1, MAX_SESSION_PAGE_SIZE),
        )
        .await
    {
        Ok(sessions) => Json(sessions).into_response(),
        Err(error) => map_session_error(error),
    }
}

async fn get_session(State(state): State<AppState>, Path(session_id): Path<String>) -> Response {
    let Some(session_id) = parse_session_id(&session_id) else {
        return invalid_session_id();
    };
    match state.orchestration.sessions.get_session(session_id).await {
        Ok(Some(session)) => Json(session).into_response(),
        Ok(None) => session_not_found(),
        Err(error) => map_session_error(error),
    }
}

async fn get_session_messages(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    Query(query): Query<SessionMessagesQuery>,
) -> Response {
    let Some(session_id) = parse_session_id(&session_id) else {
        return invalid_session_id();
    };
    match state
        .orchestration
        .sessions
        .messages(
            session_id,
            query.before,
            query.limit.unwrap_or(100).clamp(1, 1_000),
        )
        .await
    {
        Ok(messages) => Json(messages).into_response(),
        Err(error) => map_session_error(error),
    }
}

async fn submit_session_run(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    Json(request): Json<SubmitSessionRunRequest>,
) -> Response {
    let Some(session_id) = parse_session_id(&session_id) else {
        return invalid_session_id();
    };
    if request.idempotency_key.trim().is_empty() || request.idempotency_key.len() > 256 {
        return api_error(
            StatusCode::BAD_REQUEST,
            "invalid_idempotency_key",
            "idempotency_key must contain 1 to 256 bytes",
            false,
        );
    }
    let max_steps = match effective_run_max_steps(&state, request.max_steps) {
        Ok(max_steps) => max_steps,
        Err(error) => return map_run_max_steps_error(error),
    };
    let run_id = RunId::new();
    let now_ms = run_runtime::unix_time_ms();
    let hash = raw_request_hash(&request.input, &request.skills, max_steps);
    let begin = match state
        .orchestration
        .sessions
        .begin_run(
            BeginSessionRun {
                session_id,
                run_id,
                expected_revision: request.expected_revision,
                idempotency_key: request.idempotency_key,
                request_hash: hash,
                input: vec![ContentPart::text(request.input.clone())],
                created_at_ms: now_ms,
            },
            RunSnapshot::new(run_id, request.input.clone(), now_ms),
        )
        .await
    {
        Ok(begin) => begin,
        Err(error) => return map_session_error(error),
    };
    if begin.replayed {
        let fingerprint = state
            .runtime
            .get_run(begin.run_id)
            .await
            .ok()
            .flatten()
            .and_then(|run| run.execution_manifest)
            .and_then(|manifest| {
                manifest
                    .get("context_fingerprint")
                    .and_then(|value| value.as_str())
                    .map(str::to_owned)
            });
        return (
            StatusCode::ACCEPTED,
            Json(RunAcceptedResponse {
                session_id,
                run_id: begin.run_id,
                session_revision: begin.session.revision,
                replayed: true,
                context_fingerprint: fingerprint,
                max_steps,
            }),
        )
            .into_response();
    }

    let prepared = match prepare_run_plan(
        &state,
        run_id,
        request.input.clone(),
        request.skills,
        max_steps,
        Some(session_id),
        Some(begin.context_through_ordinal),
    )
    .await
    {
        Ok(prepared) => prepared,
        Err(response) => {
            if let Ok(snapshot) = state
                .runtime
                .fail_persisted(
                    run_id,
                    "context_build_failed",
                    "context orchestration failed",
                )
                .await
            {
                let _ = state
                    .orchestration
                    .sessions
                    .finalize_run(FinalizeSessionRun {
                        session_id,
                        run: snapshot,
                        finalized_at_ms: run_runtime::unix_time_ms(),
                    })
                    .await;
            }
            return response;
        }
    };
    let context_fingerprint = prepared.context_fingerprint.clone();
    let started = match state.runtime.start_persisted(prepared.plan).await {
        Ok(started) => started,
        Err(error) => {
            if let Ok(snapshot) = state
                .runtime
                .fail_persisted(run_id, "run_start_failed", "run executor could not start")
                .await
            {
                let _ = state
                    .orchestration
                    .sessions
                    .finalize_run(FinalizeSessionRun {
                        session_id,
                        run: snapshot,
                        finalized_at_ms: run_runtime::unix_time_ms(),
                    })
                    .await;
            }
            return map_runtime_error(error);
        }
    };
    let terminal_state = state.clone();
    let extraction_input = request.input;
    tokio::spawn(async move {
        let _ = wait_for_terminal(&terminal_state.runtime, started).await;
        let Ok(Some(snapshot)) = terminal_state.runtime.get_run(run_id).await else {
            return;
        };
        if !snapshot.is_terminal() {
            return;
        }
        if let Err(error) = terminal_state
            .orchestration
            .sessions
            .finalize_run(FinalizeSessionRun {
                session_id,
                run: snapshot.clone(),
                finalized_at_ms: run_runtime::unix_time_ms(),
            })
            .await
        {
            tracing::error!(%session_id, %run_id, %error, "failed to finalize session run");
            return;
        }
        if snapshot.status == RunStatus::Completed
            && terminal_state.orchestration.config.memory_enabled
        {
            let extraction = terminal_state
                .orchestration
                .memory_extractor
                .extract(MemoryExtractionRequest {
                    scope: MemoryScope(terminal_state.orchestration.config.memory_scope.clone()),
                    content: vec![ContentPart::text(extraction_input)],
                    source_refs: vec![MemorySourceRef::ExplicitUserInput {
                        run_id: Some(run_id),
                    }],
                    now_ms: run_runtime::unix_time_ms(),
                })
                .await;
            if let Ok(candidates) = extraction {
                match terminal_state
                    .orchestration
                    .memory_writer
                    .process(candidates)
                    .await
                {
                    Ok(outcomes) => {
                        for outcome in outcomes {
                            if let MemoryWriteOutcome::ApprovalRequired { candidate, reason } =
                                outcome
                            {
                                let proposal = MemoryWriteProposal {
                                    approval_id: MemoryApprovalId::new(),
                                    candidate,
                                    reason,
                                    status: MemoryApprovalStatus::Pending,
                                    created_at_ms: run_runtime::unix_time_ms(),
                                    resolved_at_ms: None,
                                    resolution_reason: None,
                                };
                                match terminal_state
                                    .orchestration
                                    .memory_proposals
                                    .create(CreateMemoryWriteProposal {
                                        proposal: proposal.clone(),
                                    })
                                    .await
                                {
                                    Ok(proposal) => {
                                        if let Err(error) = ensure_memory_approval(
                                            &terminal_state.events,
                                            &proposal,
                                        )
                                        .await
                                        {
                                            tracing::warn!(%run_id, %error, "failed to publish memory approval request");
                                        }
                                    }
                                    Err(error) => {
                                        tracing::warn!(%run_id, %error, "failed to persist memory approval request");
                                    }
                                }
                            }
                        }
                    }
                    Err(error) => {
                        tracing::warn!(%run_id, %error, "post-run memory extraction failed");
                    }
                }
            }
        }
    });

    (
        StatusCode::ACCEPTED,
        Json(RunAcceptedResponse {
            session_id,
            run_id,
            session_revision: begin.session.revision,
            replayed: false,
            context_fingerprint: Some(context_fingerprint),
            max_steps: prepared.max_steps,
        }),
    )
        .into_response()
}

async fn archive_session(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    Json(request): Json<ArchiveSessionRequest>,
) -> Response {
    let Some(session_id) = parse_session_id(&session_id) else {
        return invalid_session_id();
    };
    match state
        .orchestration
        .sessions
        .archive_session(ArchiveSession {
            session_id,
            expected_revision: request.expected_revision,
            archived_at_ms: run_runtime::unix_time_ms(),
        })
        .await
    {
        Ok(session) => Json(session).into_response(),
        Err(error) => map_session_error(error),
    }
}

async fn get_run(State(state): State<AppState>, Path(run_id): Path<String>) -> Response {
    let Some(run_id) = parse_run_id(&run_id) else {
        return invalid_run_id();
    };
    match state.runtime.get_run(run_id).await {
        Ok(Some(snapshot)) => Json(snapshot).into_response(),
        Ok(None) => run_not_found(),
        Err(error) => map_store_error(error),
    }
}

async fn get_run_events(
    State(state): State<AppState>,
    Path(run_id): Path<String>,
    Query(query): Query<RunEventsQuery>,
    headers: HeaderMap,
) -> Response {
    let Some(run_id) = parse_run_id(&run_id) else {
        return invalid_run_id();
    };
    let after_seq = query
        .after_seq
        .or_else(|| {
            headers
                .get("last-event-id")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse().ok())
        })
        .unwrap_or(0);

    match state.runtime.get_run(run_id).await {
        Ok(Some(_)) => run_event_response(
            state.runtime.clone(),
            run_id,
            after_seq,
            query.follow.unwrap_or(true),
            state.runtime.subscribe(run_id),
        ),
        Ok(None) => run_not_found(),
        Err(error) => map_store_error(error),
    }
}

fn run_event_response(
    runtime: RunRuntime<AppRuntimeAgent>,
    run_id: RunId,
    after_seq: u64,
    follow: bool,
    mut live_events: Option<broadcast::Receiver<RunEvent>>,
) -> Response {
    const PAGE_SIZE: usize = 512;

    let events = stream! {
        let mut cursor = after_seq;
        let mut replay_required = true;

        loop {
            if replay_required {
                loop {
                    let page = match runtime.events_after(run_id, cursor, PAGE_SIZE).await {
                        Ok(page) => page,
                        Err(error) => {
                            yield Ok::<Event, Infallible>(stream_error_event(error.to_string()));
                            return;
                        }
                    };
                    if page.is_empty() {
                        break;
                    }
                    let page_is_full = page.len() == PAGE_SIZE;
                    for event in page {
                        if event.seq <= cursor {
                            continue;
                        }
                        cursor = event.seq;
                        let terminal = event.kind.is_terminal();
                        yield Ok::<Event, Infallible>(sse_event(event));
                        if terminal {
                            return;
                        }
                    }
                    if !page_is_full {
                        break;
                    }
                }

                let snapshot = match runtime.get_run(run_id).await {
                    Ok(Some(snapshot)) => snapshot,
                    Ok(None) => {
                        yield Ok::<Event, Infallible>(stream_error_event("run no longer exists"));
                        return;
                    }
                    Err(error) => {
                        yield Ok::<Event, Infallible>(stream_error_event(error.to_string()));
                        return;
                    }
                };
                if snapshot.is_terminal() {
                    return;
                }
                replay_required = false;

                if !follow {
                    return;
                }
            }

            if live_events.is_none() {
                live_events = runtime.subscribe(run_id);
            }
            let Some(receiver) = live_events.as_mut() else {
                // A durable machine deliberately releases its executor while it is
                // waiting for an approval, timer, job or external event. Keep the
                // SSE stream alive and periodically reattach when the flow wakes.
                tokio::time::sleep(Duration::from_millis(200)).await;
                replay_required = true;
                continue;
            };
            match receiver.recv().await {
                Ok(event) if event.seq <= cursor => {}
                Ok(event) if event.seq == cursor.saturating_add(1) => {
                    cursor = event.seq;
                    let terminal = event.kind.is_terminal();
                    yield Ok::<Event, Infallible>(sse_event(event));
                    if terminal {
                        return;
                    }
                }
                Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {
                    replay_required = true;
                }
                Err(broadcast::error::RecvError::Closed) => {
                    live_events = None;
                    replay_required = true;
                }
            }
        }
    };

    (
        [
            ("cache-control", "no-cache, no-transform"),
            ("x-accel-buffering", "no"),
        ],
        Sse::new(events).keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(15))
                .text("keep-alive"),
        ),
    )
        .into_response()
}

fn sse_event(event: RunEvent) -> Event {
    let event_name = event.kind.event_name();
    let event_id = event.seq.to_string();
    match Event::default()
        .id(event_id)
        .event(event_name)
        .json_data(event)
    {
        Ok(event) => event,
        Err(error) => stream_error_event(error.to_string()),
    }
}

fn stream_error_event(message: impl Into<String>) -> Event {
    let payload = ApiError {
        code: "run_stream_error".into(),
        message: message.into(),
        retryable: true,
    };
    match Event::default()
        .event("run_stream_error")
        .json_data(payload)
    {
        Ok(event) => event,
        Err(_) => Event::default()
            .event("run_stream_error")
            .data("run event stream failed"),
    }
}

async fn wait_for_terminal(
    runtime: &RunRuntime<AppRuntimeAgent>,
    mut started: StartedRun,
) -> Result<RunResponse, HarnessError> {
    loop {
        let snapshot = runtime
            .get_run(started.run_id)
            .await
            .map_err(|error| HarnessError::agent("run_store_error", error.to_string(), true))?
            .ok_or_else(|| {
                HarnessError::agent("run_not_found", "persisted run disappeared", false)
            })?;
        if snapshot.is_terminal() {
            return response_from_snapshot(snapshot);
        }

        match started.events.recv().await {
            Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {}
            Err(broadcast::error::RecvError::Closed) => {
                let snapshot = runtime
                    .get_run(started.run_id)
                    .await
                    .map_err(|error| {
                        HarnessError::agent("run_store_error", error.to_string(), true)
                    })?
                    .ok_or_else(|| {
                        HarnessError::agent("run_not_found", "persisted run disappeared", false)
                    })?;
                if snapshot.is_terminal() {
                    return response_from_snapshot(snapshot);
                }
                return Err(HarnessError::agent(
                    "run_executor_unavailable",
                    "run executor stopped before persisting a terminal event",
                    true,
                ));
            }
        }
    }
}

fn response_from_snapshot(snapshot: RunSnapshot) -> Result<RunResponse, HarnessError> {
    match snapshot.status {
        RunStatus::Completed => Ok(RunResponse {
            run_id: snapshot.run_id,
            output: snapshot.output,
            finish_reason: snapshot.finish_reason.unwrap_or(FinishReason::Stop),
            usage: snapshot.usage,
        }),
        RunStatus::Cancelled => Err(HarnessError::agent(
            "run_cancelled",
            "run was cancelled",
            false,
        )),
        RunStatus::Failed => {
            let failure = snapshot.failure.unwrap_or(RunFailure {
                code: "run_failed".into(),
                message: "run failed without details".into(),
                retryable: false,
            });
            Err(HarnessError::agent(
                failure.code,
                failure.message,
                failure.retryable,
            ))
        }
        _ => Err(HarnessError::agent(
            "run_not_terminal",
            "run has not reached a terminal state",
            true,
        )),
    }
}

async fn list_memory_approvals(State(state): State<AppState>) -> Response {
    match state
        .orchestration
        .memory_proposals
        .pending_proposals(100)
        .await
    {
        Ok(proposals) => Json(proposals).into_response(),
        Err(error) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "memory_approval_store_error",
            error.to_string(),
            true,
        ),
    }
}

async fn resolve_memory_approval(
    State(state): State<AppState>,
    Path(approval_id): Path<String>,
    Json(request): Json<ResolveMemoryApprovalRequest>,
) -> Response {
    let Ok(approval_id) = approval_id.parse::<MemoryApprovalId>() else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "invalid_memory_approval_id",
            "approval_id must be a valid UUID",
            false,
        );
    };
    let reason = request
        .reason
        .map(|reason| reason.trim().to_owned())
        .filter(|reason| !reason.is_empty());
    if reason.as_ref().is_some_and(|reason| reason.len() > 2_000) {
        return api_error(
            StatusCode::BAD_REQUEST,
            "invalid_memory_approval_reason",
            "memory approval reason must not exceed 2000 bytes",
            false,
        );
    }
    let proposal = match state
        .orchestration
        .memory_proposals
        .get_proposal(approval_id)
        .await
    {
        Ok(Some(proposal)) => proposal,
        Ok(None) => {
            return api_error(
                StatusCode::NOT_FOUND,
                "memory_approval_not_found",
                "the memory approval does not exist",
                false,
            );
        }
        Err(error) => {
            return api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "memory_approval_store_error",
                error.to_string(),
                true,
            );
        }
    };
    let requested_status = match request.decision {
        MemoryApprovalDecision::Approve => MemoryApprovalStatus::Approved,
        MemoryApprovalDecision::Deny => MemoryApprovalStatus::Denied,
    };
    let event_id = EventId::stable("memory-approval-resolved", &approval_id.to_string());
    if proposal.status != MemoryApprovalStatus::Pending {
        if proposal.status == requested_status {
            return Json(ResolveMemoryApprovalResponse {
                approval_id,
                event_id,
                replayed: true,
            })
            .into_response();
        }
        return api_error(
            StatusCode::CONFLICT,
            "memory_approval_already_resolved",
            "the memory approval was already resolved with a different decision",
            false,
        );
    }
    let now_ms = run_runtime::unix_time_ms();
    let decision = match request.decision {
        MemoryApprovalDecision::Approve => "approve",
        MemoryApprovalDecision::Deny => "deny",
    };
    match state
        .events
        .publish(PublishEvent {
            event_id,
            topic: "memory.approval.resolved".into(),
            event_type: "memory.approval.resolved".into(),
            schema_version: 1,
            source: EventSource::Gateway,
            subject: Some(format!("memory/{}", proposal.candidate.proposed.memory_id)),
            correlation_id: Some(approval_id.to_string()),
            causation_id: None,
            occurred_at_ms: now_ms,
            recorded_at_ms: now_ms,
            payload: serde_json::json!({
                "decision": decision,
                "reason": reason,
            }),
        })
        .await
    {
        Ok(result) => (
            StatusCode::ACCEPTED,
            Json(ResolveMemoryApprovalResponse {
                approval_id,
                event_id,
                replayed: result.replayed,
            }),
        )
            .into_response(),
        Err(EventError::Conflict(message)) => api_error(
            StatusCode::CONFLICT,
            "memory_approval_already_resolved",
            message,
            false,
        ),
        Err(EventError::Invalid(message)) => api_error(
            StatusCode::BAD_REQUEST,
            "invalid_memory_approval",
            message,
            false,
        ),
        Err(error) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "event_runtime_error",
            error.to_string(),
            true,
        ),
    }
}

async fn resolve_approval(
    State(state): State<AppState>,
    Path((run_id, approval_id)): Path<(String, String)>,
    Json(request): Json<ResolveApprovalRequest>,
) -> Response {
    let Ok(run_id) = run_id.parse::<RunId>() else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "invalid_run_id",
            "run_id must be a valid UUID",
            false,
        );
    };
    let Ok(approval_id) = approval_id.parse::<ApprovalId>() else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "invalid_approval_id",
            "approval_id must be a valid UUID",
            false,
        );
    };
    let reason = request
        .reason
        .map(|reason| reason.trim().to_owned())
        .filter(|reason| !reason.is_empty());
    if reason.as_ref().is_some_and(|reason| reason.len() > 2_000) {
        return api_error(
            StatusCode::BAD_REQUEST,
            "invalid_approval_reason",
            "approval reason must not exceed 2000 bytes",
            false,
        );
    }
    let resolution = ApprovalResolution {
        decision: request.decision,
        reason,
    };

    match state
        .approvals
        .resolve(run_id, approval_id, resolution.clone())
    {
        Ok(outcome) => Json(ResolveApprovalResponse {
            run_id,
            approval_id,
            resolution,
            replayed: outcome == ResolveApprovalOutcome::AlreadyResolved,
        })
        .into_response(),
        Err(ResolveApprovalError::NotPending) => {
            resolve_persisted_approval(&state, run_id, approval_id, resolution).await
        }
        Err(ResolveApprovalError::ConflictingDecision) => api_error(
            StatusCode::CONFLICT,
            "approval_already_resolved",
            "the approval was already resolved with a different decision",
            false,
        ),
    }
}

async fn resolve_persisted_approval(
    state: &AppState,
    run_id: RunId,
    approval_id: ApprovalId,
    requested: ApprovalResolution,
) -> Response {
    let snapshot = match state.runtime.get_run(run_id).await {
        Ok(Some(snapshot)) => snapshot,
        Ok(None) => return run_not_found(),
        Err(error) => return map_store_error(error),
    };
    let Some(approval) = snapshot
        .approvals
        .iter()
        .find(|approval| approval.approval_id == approval_id)
    else {
        return api_error(
            StatusCode::NOT_FOUND,
            "approval_not_found",
            "the approval does not exist for this run",
            false,
        );
    };

    match &approval.resolution {
        Some(existing) if existing == &requested => Json(ResolveApprovalResponse {
            run_id,
            approval_id,
            resolution: requested,
            replayed: true,
        })
        .into_response(),
        Some(_) => api_error(
            StatusCode::CONFLICT,
            "approval_already_resolved",
            "the approval was already resolved with a different decision",
            false,
        ),
        None if snapshot.is_terminal() => api_error(
            StatusCode::CONFLICT,
            "approval_run_terminated",
            "the run terminated before this approval was resolved",
            false,
        ),
        None => {
            let now_ms = run_runtime::unix_time_ms();
            let event_id = EventId::stable("tool-approval-resolved", &approval_id.to_string());
            let decision = match requested.decision {
                ApprovalDecision::AllowOnce => "allow-once",
                ApprovalDecision::Deny => "deny",
            };
            match state
                .events
                .publish(PublishEvent {
                    event_id,
                    topic: "tool.approval.resolved".into(),
                    event_type: "tool.approval.resolved".into(),
                    schema_version: 1,
                    source: EventSource::Gateway,
                    subject: Some(format!("run/{run_id}")),
                    correlation_id: Some(approval_id.to_string()),
                    causation_id: None,
                    occurred_at_ms: now_ms,
                    recorded_at_ms: now_ms,
                    payload: serde_json::json!({
                        "decision": decision,
                        "reason": requested.reason.clone(),
                    }),
                })
                .await
            {
                Ok(result) => (
                    StatusCode::ACCEPTED,
                    Json(ResolveApprovalResponse {
                        run_id,
                        approval_id,
                        resolution: requested,
                        replayed: result.replayed,
                    }),
                )
                    .into_response(),
                Err(EventError::Conflict(message)) => api_error(
                    StatusCode::CONFLICT,
                    "approval_already_resolved",
                    message,
                    false,
                ),
                Err(EventError::Invalid(message)) => api_error(
                    StatusCode::BAD_REQUEST,
                    "invalid_approval_event",
                    message,
                    false,
                ),
                Err(error) => api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "event_runtime_error",
                    error.to_string(),
                    true,
                ),
            }
        }
    }
}

async fn cancel_run(State(state): State<AppState>, Path(run_id): Path<String>) -> Response {
    let Some(run_id) = parse_run_id(&run_id) else {
        return invalid_run_id();
    };

    match state.runtime.cancel_durable(run_id).await {
        Ok(true) => {
            return (
                StatusCode::ACCEPTED,
                Json(CancelRunResponse {
                    run_id,
                    status: "cancellation_requested",
                }),
            )
                .into_response();
        }
        Ok(false) => {}
        Err(error) => return map_runtime_error(error),
    }

    if state.runtime.cancel(run_id) {
        return (
            StatusCode::ACCEPTED,
            Json(CancelRunResponse {
                run_id,
                status: "cancellation_requested",
            }),
        )
            .into_response();
    }

    match state.runtime.get_run(run_id).await {
        Ok(Some(_)) => api_error(
            StatusCode::CONFLICT,
            "run_not_active",
            "the run is persisted but no longer active",
            false,
        ),
        Ok(None) => run_not_found(),
        Err(error) => map_store_error(error),
    }
}

fn parse_run_id(value: &str) -> Option<RunId> {
    value.parse().ok()
}

fn parse_session_id(value: &str) -> Option<SessionId> {
    value.parse().ok()
}

fn default_agent_profile() -> String {
    "default".into()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RunMaxStepsError {
    host_max: u32,
}

fn effective_run_max_steps(
    state: &AppState,
    requested: Option<u32>,
) -> Result<u32, RunMaxStepsError> {
    let host_max = state
        .config
        .as_deref()
        .map_or(MAX_RUN_STEPS, |config| config.agent().max_steps);
    let effective = requested.unwrap_or(host_max);
    if effective == 0 || effective > host_max {
        return Err(RunMaxStepsError { host_max });
    }
    Ok(effective)
}

fn map_run_max_steps_error(error: RunMaxStepsError) -> Response {
    api_error(
        StatusCode::BAD_REQUEST,
        "invalid_max_steps",
        format!(
            "max_steps must be between 1 and the host limit of {}",
            error.host_max
        ),
        false,
    )
}

fn request_digest(input: &str, skills: &[ResolvedSkill]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    for skill in skills {
        hasher.update((skill.locked.skill_id).0.as_bytes());
        hasher.update(skill.locked.version.as_bytes());
        hasher.update(skill.locked.digest.as_bytes());
    }
    format!("sha256:{:x}", hasher.finalize())
}

fn raw_request_hash(input: &str, skills: &[SkillChoice], max_steps: u32) -> String {
    let mut skills = skills.to_vec();
    skills.sort_by(|left, right| {
        (&left.skill_id, &left.version).cmp(&(&right.skill_id, &right.version))
    });
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    hasher.update(max_steps.to_le_bytes());
    for skill in skills {
        hasher.update(skill.skill_id.as_bytes());
        hasher.update(skill.version.as_bytes());
    }
    format!("sha256:{:x}", hasher.finalize())
}

fn invalid_run_id() -> Response {
    api_error(
        StatusCode::BAD_REQUEST,
        "invalid_run_id",
        "run_id must be a valid UUID",
        false,
    )
}

fn run_not_found() -> Response {
    api_error(
        StatusCode::NOT_FOUND,
        "run_not_found",
        "the run does not exist",
        false,
    )
}

fn invalid_session_id() -> Response {
    api_error(
        StatusCode::BAD_REQUEST,
        "invalid_session_id",
        "session_id must be a valid UUID",
        false,
    )
}

fn session_not_found() -> Response {
    api_error(
        StatusCode::NOT_FOUND,
        "session_not_found",
        "the session does not exist",
        false,
    )
}

fn api_error(
    status: StatusCode,
    code: impl Into<String>,
    message: impl Into<String>,
    retryable: bool,
) -> Response {
    (
        status,
        Json(ApiError {
            code: code.into(),
            message: message.into(),
            retryable,
        }),
    )
        .into_response()
}

fn map_harness_error(error: HarnessError) -> (StatusCode, Json<ApiError>) {
    let status = match error.code() {
        "invalid_input" | "invalid_request" | "invalid_max_steps" => StatusCode::BAD_REQUEST,
        "rate_limited" | "upstream_unavailable" => StatusCode::SERVICE_UNAVAILABLE,
        "upstream_timeout" | "model_timeout" | "run_timeout" => StatusCode::GATEWAY_TIMEOUT,
        "upstream_authentication"
        | "upstream_permission_denied"
        | "upstream_protocol_violation" => StatusCode::BAD_GATEWAY,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };

    (
        status,
        Json(ApiError {
            code: error.code().into(),
            message: error.to_string(),
            retryable: error.retryable(),
        }),
    )
}

fn map_runtime_error(error: RunRuntimeError) -> Response {
    match error {
        RunRuntimeError::Harness(error) => map_harness_error(error).into_response(),
        RunRuntimeError::Store(error) => map_store_error(error),
        RunRuntimeError::Flow(error) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "flow_runtime_error",
            error.to_string(),
            true,
        ),
        RunRuntimeError::Machine(message) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "machine_runtime_error",
            message,
            false,
        ),
        RunRuntimeError::Persistence(message) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "run_persistence_error",
            message,
            true,
        ),
    }
}

fn map_store_error(error: RunStoreError) -> Response {
    match error {
        RunStoreError::NotFound(_) => run_not_found(),
        RunStoreError::AlreadyExists(_) | RunStoreError::EventConflict { .. } => api_error(
            StatusCode::CONFLICT,
            "run_store_conflict",
            error.to_string(),
            false,
        ),
        RunStoreError::State(_) | RunStoreError::Backend(_) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "run_store_error",
            error.to_string(),
            true,
        ),
        _ => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "run_store_error",
            error.to_string(),
            true,
        ),
    }
}

fn map_session_error(error: SessionStoreError) -> Response {
    match error {
        SessionStoreError::NotFound(_) => session_not_found(),
        SessionStoreError::AlreadyExists(_)
        | SessionStoreError::RevisionConflict { .. }
        | SessionStoreError::Busy(_)
        | SessionStoreError::IdempotencyConflict
        | SessionStoreError::RunMismatch(_) => api_error(
            StatusCode::CONFLICT,
            "session_conflict",
            error.to_string(),
            false,
        ),
        SessionStoreError::Archived => api_error(
            StatusCode::CONFLICT,
            "session_archived",
            error.to_string(),
            false,
        ),
        SessionStoreError::RunNotTerminal | SessionStoreError::Backend(_) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "session_store_error",
            error.to_string(),
            true,
        ),
    }
}

#[cfg(test)]
mod tests {
    use agent_core::context::InMemoryContextArtifactStore;
    use agent_core::event_runtime::{
        ClaimDeliveries, ClaimTimers, CompleteDelivery, EventComponentDescriptor, PublishResult,
        RetryDelivery, ScheduleOnce, Subscription, Timer, TimerId,
    };
    use agent_core::tool::ToolRiskLevel;
    use agent_extension::store::{InMemoryMemoryStore, InMemoryRunStore};
    use tempfile::tempdir;

    use super::*;

    struct UnavailableEventStore;

    fn unavailable<T>() -> EventFuture<'static, T> {
        Box::pin(async { Err(EventError::backend("test event store is unavailable")) })
    }

    impl EventStore for UnavailableEventStore {
        fn descriptor(&self) -> EventComponentDescriptor {
            EventComponentDescriptor {
                identity: "events:test-unavailable".into(),
                kind: "test".into(),
                version: "1".into(),
            }
        }

        fn publish(&self, _command: PublishEvent) -> EventFuture<'_, PublishResult> {
            unavailable()
        }

        fn get_event(&self, _event_id: EventId) -> EventFuture<'_, Option<EventEnvelope>> {
            unavailable()
        }

        fn subscribe(&self, _command: CreateSubscription) -> EventFuture<'_, Subscription> {
            unavailable()
        }

        fn get_subscription(
            &self,
            _subscription_id: SubscriptionId,
        ) -> EventFuture<'_, Option<Subscription>> {
            unavailable()
        }

        fn cancel_subscription(
            &self,
            _subscription_id: SubscriptionId,
            _cancelled_at_ms: i64,
        ) -> EventFuture<'_, Subscription> {
            unavailable()
        }

        fn claim_deliveries(&self, _command: ClaimDeliveries) -> EventFuture<'_, Vec<Delivery>> {
            unavailable()
        }

        fn complete_delivery(&self, _command: CompleteDelivery) -> EventFuture<'_, Delivery> {
            unavailable()
        }

        fn retry_delivery(&self, _command: RetryDelivery) -> EventFuture<'_, Delivery> {
            unavailable()
        }

        fn schedule_once(&self, _command: ScheduleOnce) -> EventFuture<'_, Timer> {
            unavailable()
        }

        fn claim_due_timers(&self, _command: ClaimTimers) -> EventFuture<'_, Vec<Timer>> {
            unavailable()
        }

        fn complete_timer(
            &self,
            _timer_id: TimerId,
            _worker_id: String,
            _event_id: EventId,
        ) -> EventFuture<'_, Timer> {
            unavailable()
        }

        fn cancel_timer(
            &self,
            _timer_id: TimerId,
            _cancelled_at_ms: i64,
        ) -> EventFuture<'_, Timer> {
            unavailable()
        }
    }

    fn test_state() -> AppState {
        test_state_with_approvals(InMemoryApprovalBroker::default())
    }

    fn test_state_with_approvals(approvals: InMemoryApprovalBroker) -> AppState {
        let state_store = Arc::new(InMemoryRunStore::default());
        let store: Arc<dyn RunStore> = state_store.clone();
        let sessions: Arc<dyn SessionStore> = state_store;
        let memory_adapter = Arc::new(InMemoryMemoryStore::default());
        let memory_store: Arc<dyn MemoryStore> = memory_adapter.clone();
        let memory_proposals: Arc<dyn MemoryProposalStore> = memory_adapter.clone();
        let memory_retriever: Arc<dyn MemoryRetriever> = memory_adapter;
        let estimator: Arc<dyn TokenEstimator> = Arc::new(HeuristicTokenEstimator);
        let compressor: Arc<dyn ContextCompressor> =
            Arc::new(SlidingWindowCompressor::new(Arc::clone(&estimator)));
        let artifacts: Arc<dyn ContextArtifactStore> =
            Arc::new(InMemoryContextArtifactStore::default());
        let context = ContextEngine::new(
            Arc::clone(&sessions),
            memory_retriever,
            compressor,
            artifacts,
        );
        let skill_store: Arc<dyn SkillStore> = Arc::new(FilesystemSkillStore::new(
            "target/nonexistent-test-skills",
            "test-skills",
        ));
        let approval_cleanup = approvals.clone();
        let events = Arc::new(EventRuntime::new(
            Arc::new(UnavailableEventStore),
            Arc::new(ServerDeliveryRouter {
                memories: Arc::clone(&memory_store),
                proposals: Arc::clone(&memory_proposals),
                flows: None,
            }),
            EventRuntimeConfig::default(),
        ));
        AppState {
            runtime: RunRuntime::new(
                Harness::new(ObservedMachine::new(
                    RuntimeAgent::Echo(EchoAgent),
                    Arc::new(TracingObservationHook),
                )),
                store,
                move |run_id| approval_cleanup.remove_run(run_id),
            ),
            config: None,
            approvals,
            events,
            orchestration: Arc::new(OrchestrationRuntime {
                sessions,
                context,
                skills: SkillOrchestrator::new(skill_store),
                memory_store: Arc::clone(&memory_store),
                memory_proposals,
                memory_extractor: Arc::new(RuleMemoryExtractor),
                memory_writer: MemoryWriter::new(memory_store, Arc::new(HostMemoryWritePolicy)),
                config: OrchestrationConfig::default(),
                agent_instruction: "You are helpful.".into(),
                agent_profile: "test".into(),
                model_profile: "echo".into(),
                model_context_tokens: 8_192,
                reserved_output_tokens: 1_024,
                tools: Vec::new(),
                tool_runtime: None,
            }),
        }
    }

    #[tokio::test]
    async fn durable_memory_approval_event_writes_the_memory() {
        let directory = tempdir().expect("temporary directory should exist");
        let path = directory.path().join("state.sqlite3");
        let memory_adapter = Arc::new(
            SqliteMemoryStore::open(&path, "memory:test")
                .await
                .expect("memory store should open"),
        );
        let memories: Arc<dyn MemoryStore> = memory_adapter.clone();
        let proposals: Arc<dyn MemoryProposalStore> = memory_adapter;
        let event_store: Arc<dyn EventStore> = Arc::new(
            SqliteEventStore::open(&path, "events:test")
                .await
                .expect("event store should open"),
        );
        let runtime = EventRuntime::new(
            event_store,
            Arc::new(ServerDeliveryRouter {
                memories: Arc::clone(&memories),
                proposals: Arc::clone(&proposals),
                flows: None,
            }),
            EventRuntimeConfig::default(),
        );
        let record = MemoryRecord {
            memory_id: agent_core::memory::MemoryId::new(),
            scope: MemoryScope("global".into()),
            kind: agent_core::memory::MemoryKind::Semantic,
            content: vec![ContentPart::text("private preference")],
            source_refs: vec![MemorySourceRef::ExplicitUserInput { run_id: None }],
            confidence: 1.0,
            salience: 0.8,
            version: 1,
            expires_at_ms: None,
            supersedes: None,
            created_at_ms: 1,
        };
        let proposal = MemoryWriteProposal {
            approval_id: MemoryApprovalId::new(),
            candidate: agent_core::memory::MemoryCandidate {
                proposed: record.clone(),
                sensitivity: agent_core::memory::MemorySensitivity::Private,
                extraction_reason: "explicit".into(),
            },
            reason: "private content requires approval".into(),
            status: MemoryApprovalStatus::Pending,
            created_at_ms: 1,
            resolved_at_ms: None,
            resolution_reason: None,
        };
        proposals
            .create(CreateMemoryWriteProposal {
                proposal: proposal.clone(),
            })
            .await
            .expect("proposal should persist");
        ensure_memory_approval(&runtime, &proposal)
            .await
            .expect("approval event should be prepared");
        runtime
            .publish(PublishEvent {
                event_id: EventId::stable(
                    "memory-approval-resolved",
                    &proposal.approval_id.to_string(),
                ),
                topic: "memory.approval.resolved".into(),
                event_type: "memory.approval.resolved".into(),
                schema_version: 1,
                source: EventSource::Gateway,
                subject: Some(format!("memory/{}", record.memory_id)),
                correlation_id: Some(proposal.approval_id.to_string()),
                causation_id: None,
                occurred_at_ms: 2,
                recorded_at_ms: 2,
                payload: serde_json::json!({"decision": "approve"}),
            })
            .await
            .expect("resolution event should publish");
        let report = runtime
            .dispatch_once("test-worker", 3)
            .await
            .expect("resolution should dispatch");
        assert_eq!(report.completed, 1);
        assert!(
            memories
                .get(agent_core::memory::MemoryLocator {
                    memory_id: record.memory_id,
                    version: Some(1),
                })
                .await
                .expect("memory query should work")
                .is_some()
        );
        assert_eq!(
            proposals
                .get_proposal(proposal.approval_id)
                .await
                .expect("proposal query should work")
                .expect("proposal should exist")
                .status,
            MemoryApprovalStatus::Approved
        );
    }

    #[tokio::test]
    async fn durable_job_completion_event_wakes_a_suspended_flow() {
        use agent_core::harness::{
            CheckpointCodec, CheckpointEnvelope, FlowRunState, FlowRunStatus, SuspendFlowRun,
            WaitSpec,
        };

        let directory = tempdir().expect("temporary directory should exist");
        let path = directory.path().join("job-flow.sqlite3");
        let adapter = Arc::new(
            SqliteEventStore::open(&path, "job-flow:test")
                .await
                .expect("event store should open"),
        );
        let event_store: Arc<dyn EventStore> = adapter.clone();
        let flows: Arc<dyn FlowStore> = adapter.clone();
        let job_store: Arc<dyn JobStore> = adapter;
        let memories = Arc::new(InMemoryMemoryStore::default());
        let memory_store: Arc<dyn MemoryStore> = memories.clone();
        let proposals: Arc<dyn MemoryProposalStore> = memories;
        let events = Arc::new(EventRuntime::new(
            event_store,
            Arc::new(ServerDeliveryRouter {
                memories: memory_store,
                proposals,
                flows: Some(Arc::clone(&flows)),
            }),
            EventRuntimeConfig::default(),
        ));
        let jobs = JobRuntime::new(
            job_store,
            Arc::new(ServerJobRouter),
            Arc::clone(&events),
            JobRuntimeConfig::default(),
        );
        let run_id = RunId::new();
        let job_id = agent_core::harness::JobId::new();
        let checkpoint = CheckpointEnvelope {
            agent_kind: "test".into(),
            schema_version: 1,
            codec: CheckpointCodec::Json,
            payload: serde_json::json!({"waiting": true}),
        };
        flows
            .create(FlowRunState {
                run_id,
                revision: 0,
                status: FlowRunStatus::Runnable,
                activation_id: None,
                checkpoint: checkpoint.clone(),
                wait_subscription_ids: Vec::new(),
                lease_owner: None,
                lease_until_ms: None,
                updated_at_ms: 1,
            })
            .await
            .expect("flow should persist");
        let activation = flows
            .claim_runnable("flow-worker".into(), 1, 1_001, 1)
            .await
            .expect("flow should claim")
            .into_iter()
            .next()
            .expect("one flow should claim");
        flows
            .suspend(SuspendFlowRun {
                run_id,
                activation_id: activation
                    .state
                    .activation_id
                    .expect("activation should exist"),
                expected_revision: activation.state.revision,
                checkpoint,
                waits: vec![WaitSpec {
                    wait_key: "job".into(),
                    subscription: CreateSubscription {
                        subscription_id: SubscriptionId::new(),
                        owner: SubscriptionOwner::Run { run_id },
                        scope: SubscriptionScope::Run { run_id },
                        filter: EventFilter {
                            topics: vec!["job.*".into()],
                            sources: vec![EventSource::JobWorker],
                            correlation_id: Some(job_id.to_string()),
                            ..EventFilter::default()
                        },
                        delivery: DeliveryTarget::WakeRun {
                            run_id,
                            wait_key: "job".into(),
                        },
                        mode: SubscriptionMode::Once,
                        start_position: StartPosition::Now,
                        expires_at_ms: None,
                        max_deliveries: Some(1),
                        created_at_ms: 1,
                    },
                }],
                effects: Vec::new(),
                suspended_at_ms: 1,
            })
            .await
            .expect("flow should suspend");
        jobs.submit(agent_core::harness::StartJob {
            job_id,
            run_id,
            kind: "builtin.delay".into(),
            input: serde_json::json!({"delay_ms": 0, "value": {"answer": 42}}),
            idempotency_key: "job-flow-test".into(),
            requested_at_ms: 2,
        })
        .await
        .expect("job should submit");
        let execution = jobs
            .execute_once("job-worker", 2)
            .await
            .expect("job should execute");
        assert_eq!(execution.completed, 1);
        let notification = jobs
            .publish_notifications_once("job-worker", 3)
            .await
            .expect("job notification should publish");
        assert_eq!(notification.published, 1);
        let delivery = events
            .dispatch_once("event-worker", 3)
            .await
            .expect("job event should dispatch");
        assert_eq!(delivery.completed, 1);
        let resumed = flows
            .claim_runnable("resume-worker".into(), 4, 1_004, 1)
            .await
            .expect("woken flow should claim")
            .into_iter()
            .next()
            .expect("one woken flow should exist");
        assert_eq!(resumed.inbox.len(), 1);
        assert_eq!(resumed.inbox[0].event.event.topic, "job.completed");
        assert_eq!(
            resumed.inbox[0]
                .event
                .event
                .payload
                .pointer("/outcome/value/answer"),
            Some(&serde_json::json!(42))
        );
    }

    #[test]
    fn composes_the_single_turn_agent_from_toml() {
        let config = HarnessConfig::from_toml_str(
            r#"
default_model = "primary"

[agent]
kind = "single-turn"
system_prompt = "Complete one task."

[models.primary]
model = "mock-model"

[models.primary.provider]
type = "openai-compatible"
base_url = "http://127.0.0.1:9999/v1"
api_key = "test-key"
"#,
        )
        .expect("config should parse");

        let (agent, _, _) = build_agent(
            Some(&config),
            InMemoryApprovalBroker::default(),
            Arc::new(HeuristicTokenEstimator),
            Arc::new(TracingObservationHook),
        )
        .expect("agent should compose");

        assert_eq!(agent.metadata().name, "single-turn");
        assert!(
            agent
                .metadata()
                .capabilities
                .iter()
                .any(|capability| capability == "model_completion")
        );
    }

    #[test]
    fn composes_the_agent_loop_with_tools_from_toml() {
        let config = HarnessConfig::from_toml_str(
            r#"
default_model = "primary"

[agent]
kind = "agent-loop"
system_prompt = "Use tools when needed."
max_steps = 6

[models.primary]
model = "mock-model"

[models.primary.provider]
type = "openai-compatible"
base_url = "http://127.0.0.1:9999/v1"
api_key = "test-key"
"#,
        )
        .expect("config should parse");

        let (agent, definitions, runtime) = build_agent(
            Some(&config),
            InMemoryApprovalBroker::default(),
            Arc::new(HeuristicTokenEstimator),
            Arc::new(TracingObservationHook),
        )
        .expect("agent should compose");

        assert_eq!(agent.metadata().name, "agent-loop");
        assert!(
            agent
                .metadata()
                .capabilities
                .iter()
                .any(|capability| capability == "tool_calling")
        );
        for expected in [
            "async_job",
            "javascript_eval",
            "adf_define",
            "adf_list",
            "adf_remove",
        ] {
            assert!(
                definitions.iter().any(|tool| tool.name == expected),
                "{expected} should be composed"
            );
        }
        let runtime = runtime.expect("tool runtime inspection should exist");
        assert!(runtime.script_runtime.is_some());
        assert!(runtime.adf_enabled);
    }

    #[test]
    fn per_run_step_limit_defaults_to_and_cannot_exceed_the_host_limit() {
        let config = HarnessConfig::from_toml_str(
            r#"
default_model = "primary"

[agent]
kind = "agent-loop"
system_prompt = "Use tools when needed."
max_steps = 6

[models.primary]
model = "mock-model"

[models.primary.provider]
type = "openai-compatible"
base_url = "http://127.0.0.1:9999/v1"
api_key = "test-key"
"#,
        )
        .expect("config should parse");
        let mut state = test_state();
        state.config = Some(Arc::new(config));

        assert_eq!(effective_run_max_steps(&state, None).ok(), Some(6));
        assert_eq!(effective_run_max_steps(&state, Some(3)).ok(), Some(3));
        assert!(effective_run_max_steps(&state, Some(0)).is_err());
        assert!(effective_run_max_steps(&state, Some(7)).is_err());
        assert_ne!(
            raw_request_hash("hello", &[], 3),
            raw_request_hash("hello", &[], 4),
            "step limits must participate in session idempotency"
        );
    }

    #[tokio::test]
    async fn cancel_endpoint_rejects_invalid_or_inactive_run_ids() {
        let invalid = cancel_run(State(test_state()), Path("not-a-uuid".into())).await;
        let missing = cancel_run(State(test_state()), Path(RunId::new().to_string())).await;

        assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn approval_broker_resolves_and_replays_the_same_decision() {
        let broker = InMemoryApprovalBroker::default();
        let run_id = RunId::new();
        let approval_id = ApprovalId::new();
        let waiting = broker.request(ApprovalRequest {
            approval_id,
            run_id,
            call_id: "call_1".into(),
            tool_name: "risky_tool".into(),
            risk_level: ToolRiskLevel::High,
            arguments: serde_json::json!({"target": "demo"}),
        });
        let resolution = ApprovalResolution::allow_once();

        assert_eq!(
            broker.resolve(run_id, approval_id, resolution.clone()),
            Ok(ResolveApprovalOutcome::Accepted)
        );
        assert_eq!(waiting.await.expect("approval should resolve"), resolution);
        assert_eq!(
            broker.resolve(run_id, approval_id, resolution),
            Ok(ResolveApprovalOutcome::AlreadyResolved)
        );
        assert_eq!(
            broker.resolve(
                run_id,
                approval_id,
                ApprovalResolution::deny(Some("no".into()))
            ),
            Err(ResolveApprovalError::ConflictingDecision)
        );
    }

    #[tokio::test]
    async fn approval_endpoint_resumes_a_pending_request() {
        let broker = InMemoryApprovalBroker::default();
        let run_id = RunId::new();
        let approval_id = ApprovalId::new();
        let waiting = broker.request(ApprovalRequest {
            approval_id,
            run_id,
            call_id: "call_1".into(),
            tool_name: "risky_tool".into(),
            risk_level: ToolRiskLevel::Medium,
            arguments: serde_json::json!({}),
        });

        let response = resolve_approval(
            State(test_state_with_approvals(broker)),
            Path((run_id.to_string(), approval_id.to_string())),
            Json(ResolveApprovalRequest {
                decision: ApprovalDecision::Deny,
                reason: Some("not now".into()),
            }),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            waiting.await.expect("approval should resolve"),
            ApprovalResolution::deny(Some("not now".into()))
        );
    }
}
