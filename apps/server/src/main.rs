use std::{
    collections::HashMap,
    convert::Infallible,
    env,
    sync::{Arc, Mutex},
    time::Duration,
};

use agent_core::context::{
    ContextArtifactStore, ContextBudget, ContextBuildRequest, ContextCompressor, ContextEngine,
    DeterministicSummaryGenerator, HeuristicTokenEstimator, HybridCompressor, NoopFailCompressor,
    SlidingWindowCompressor, TokenEstimator,
};
use agent_core::harness::{
    Agent, AgentEvent, AgentEventStream, AgentKind, AgentLoop, AgentMetadata, ApprovalDecision,
    ApprovalError, ApprovalFuture, ApprovalId, ApprovalPort, ApprovalRequest, ApprovalResolution,
    ArchiveSession, BeginSessionRun, ContentPart, ContextStrategy, CreateSession,
    FinalizeSessionRun, FinishReason, Harness, HarnessConfig, HarnessError, Modality,
    OrchestrationConfig, RunEvent, RunFailure, RunId, RunRequest, RunResponse, RunSnapshot,
    RunStatus, RunStore, RunStoreError, SessionId, SessionStore, SessionStoreError,
    SingleTurnAgent, ToolDefinition, ToolPort, ToolRegistry,
};
use agent_core::memory::{
    HostMemoryWritePolicy, MemoryComponentDescriptor, MemoryExtractionRequest, MemoryExtractor,
    MemoryListQuery, MemoryPage, MemoryRecord, MemoryRetriever, MemoryScope, MemorySourceRef,
    MemoryStore, MemoryWritePolicy, MemoryWriter, RuleMemoryExtractor,
};
use agent_core::skill::{
    ComponentDescriptor, ResolveSkillsRequest, ResolvedSkill, SkillDescriptor, SkillId,
    SkillOrchestrator, SkillStore,
};
use agent_extension::observability::{
    ObservationHook, ObservedModel, ObservedTools, TracingObservationHook,
};
use agent_extension::provider::OpenAiCompatibleProvider;
use agent_extension::sandbox::{HostProcessSandbox, ProcessSandbox, ProcessSandboxDescriptor};
use agent_extension::store::{FilesystemSkillStore, SqliteMemoryStore, SqliteRunStore};
use agent_extension::tool::{
    BuiltinToolCatalog, SearchBackend, SearchBackendDescriptor, WorkspaceSearchBackend,
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
use futures_util::stream as futures_stream;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{
    net::TcpListener,
    sync::{broadcast, oneshot},
};
use tracing_subscriber::EnvFilter;

mod run_runtime;

use run_runtime::{PlannedRun, RunRuntime, RunRuntimeError, StartedRun};

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
    AgentLoop(Box<AgentLoop<ObservedModel<OpenAiCompatibleProvider>, ObservedTools<ToolRegistry>>>),
}

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

#[derive(Clone)]
struct AppState {
    runtime: RunRuntime<RuntimeAgent>,
    config: Option<Arc<HarnessConfig>>,
    approvals: InMemoryApprovalBroker,
    orchestration: Arc<OrchestrationRuntime>,
}

struct OrchestrationRuntime {
    sessions: Arc<dyn SessionStore>,
    context: ContextEngine,
    skills: SkillOrchestrator,
    memory_store: Arc<dyn MemoryStore>,
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
    tool_runtime: Option<ToolRuntimeInspection>,
    memory: MemoryInspection,
}

#[derive(Debug, Clone, Serialize)]
struct ToolRuntimeInspection {
    process_sandbox: ProcessSandboxDescriptor,
    search_backend: SearchBackendDescriptor,
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

#[derive(Debug, Deserialize)]
struct SubmitSessionRunRequest {
    input: String,
    expected_revision: u64,
    idempotency_key: String,
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
    let (agent, tools, tool_runtime) =
        build_agent(config.as_deref(), approvals.clone(), Arc::clone(&estimator))?;
    tracing::info!(agent = agent.metadata().name, "configured agent runtime");

    let mut harness = Harness::new(agent);
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
    let memory_retriever: Arc<dyn MemoryRetriever> = memory_adapter;
    let memory_policy: Arc<dyn MemoryWritePolicy> = Arc::new(HostMemoryWritePolicy);
    let memory_extractor: Arc<dyn MemoryExtractor> = Arc::new(RuleMemoryExtractor);
    let memory_writer = MemoryWriter::new(Arc::clone(&memory_store), memory_policy);
    let summary = Arc::new(DeterministicSummaryGenerator);
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
    let approval_cleanup = approvals.clone();
    let runtime = RunRuntime::new(harness, store, move |run_id| {
        approval_cleanup.remove_run(run_id);
    });
    let recovered = runtime.recover_interrupted().await?;
    if recovered > 0 {
        tracing::warn!(
            recovered,
            "closed runs interrupted by the previous server process"
        );
    }
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

    let state = AppState {
        runtime,
        config: config.clone(),
        approvals,
        orchestration: Arc::new(OrchestrationRuntime {
            sessions,
            context,
            skills,
            memory_store,
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
    let app = Router::new()
        .route("/healthz", get(health))
        .route("/api/v1/info", get(info))
        .route("/api/v1/debug/agent", get(inspect_agent))
        .route("/api/v1/runs", post(create_run))
        .route("/api/v1/runs/{run_id}", get(get_run))
        .route("/api/v1/runs/{run_id}/events", get(get_run_events))
        .route("/api/v1/runs/{run_id}/cancel", post(cancel_run))
        .route("/api/v1/sessions", post(create_session))
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
        .with_state(state);

    let address = env::var("MINA_SERVER_ADDR").unwrap_or_else(|_| "127.0.0.1:8787".into());
    let listener = TcpListener::bind(&address).await?;
    let local_address = listener.local_addr()?;
    tracing::info!(address = %local_address, "mina server listening");

    axum::serve(listener, app).await?;
    Ok(())
}

fn build_agent(
    config: Option<&HarnessConfig>,
    approvals: InMemoryApprovalBroker,
    estimator: Arc<dyn TokenEstimator>,
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
            let hook: Arc<dyn ObservationHook> = Arc::new(TracingObservationHook);
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
            let hook: Arc<dyn ObservationHook> = Arc::new(TracingObservationHook);
            let provider = ObservedModel::new(provider, Arc::clone(&hook));
            let workspace = env::current_dir()?;
            let process_sandbox: Arc<dyn ProcessSandbox> = Arc::new(HostProcessSandbox::default());
            let search_backend: Arc<dyn SearchBackend> =
                Arc::new(WorkspaceSearchBackend::new(&workspace)?);
            let tool_runtime = ToolRuntimeInspection {
                process_sandbox: process_sandbox.descriptor(),
                search_backend: search_backend.descriptor(),
            };
            let tools = BuiltinToolCatalog::new(workspace)
                .with_process_sandbox(process_sandbox)
                .with_search_backend(search_backend)
                .enable_terminal_tools()
                .build()?;
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
                    .with_timeouts(
                        Duration::from_secs(config.agent().model_timeout_seconds),
                        Duration::from_secs(config.agent().tool_timeout_seconds),
                    )
                    .with_approval_port(approvals),
                )),
                definitions,
                Some(tool_runtime),
            ))
        }
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
        tool_runtime: state.orchestration.tool_runtime.clone(),
        memory: MemoryInspection {
            enabled: memory_enabled,
            scopes: memory_scopes,
            store: state.orchestration.memory_store.descriptor(),
            records: memory_page.records,
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
    let prepared =
        match prepare_run_plan(&state, run_id, request.input, request.skills, None, None).await {
            Ok(prepared) => prepared,
            Err(error) => return error,
        };
    let started = match state.runtime.start_planned(prepared.plan).await {
        Ok(started) => started,
        Err(error) => return map_runtime_error(error),
    };

    if request.stream {
        return run_event_response(state.runtime, started.run_id, 0, Some(started.events));
    }

    match wait_for_terminal(&state.runtime, started).await {
        Ok(response) => Json(response).into_response(),
        Err(error) => map_harness_error(error).into_response(),
    }
}

struct PreparedRun {
    plan: PlannedRun,
    context_fingerprint: String,
}

async fn prepare_run_plan(
    state: &AppState,
    run_id: RunId,
    input: String,
    explicit_skills: Vec<SkillChoice>,
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
        }
    });
    Ok(PreparedRun {
        plan: PlannedRun {
            run_id,
            input,
            prior_messages: pack.messages,
            allowed_tools: Some(pack.effective_tools),
            execution_manifest: Some(manifest),
        },
        context_fingerprint,
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
    let run_id = RunId::new();
    let now_ms = run_runtime::unix_time_ms();
    let hash = raw_request_hash(&request.input, &request.skills);
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
            }),
        )
            .into_response();
    }

    let prepared = match prepare_run_plan(
        &state,
        run_id,
        request.input.clone(),
        request.skills,
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
            if let Ok(candidates) = extraction
                && let Err(error) = terminal_state
                    .orchestration
                    .memory_writer
                    .process(candidates)
                    .await
            {
                tracing::warn!(%run_id, %error, "post-run memory extraction failed");
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
            state.runtime.subscribe(run_id),
        ),
        Ok(None) => run_not_found(),
        Err(error) => map_store_error(error),
    }
}

fn run_event_response(
    runtime: RunRuntime<RuntimeAgent>,
    run_id: RunId,
    after_seq: u64,
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
            }

            let Some(receiver) = live_events.as_mut() else {
                yield Ok::<Event, Infallible>(stream_error_event(
                    "run is non-terminal but has no active executor",
                ));
                return;
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
    runtime: &RunRuntime<RuntimeAgent>,
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
        None => api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "approval_runtime_unavailable",
            "the approval is persisted but its active executor is unavailable",
            true,
        ),
    }
}

async fn cancel_run(State(state): State<AppState>, Path(run_id): Path<String>) -> Response {
    let Some(run_id) = parse_run_id(&run_id) else {
        return invalid_run_id();
    };

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

fn raw_request_hash(input: &str, skills: &[SkillChoice]) -> String {
    let mut skills = skills.to_vec();
    skills.sort_by(|left, right| {
        (&left.skill_id, &left.version).cmp(&(&right.skill_id, &right.version))
    });
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
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
        "invalid_input" | "invalid_request" => StatusCode::BAD_REQUEST,
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
    use agent_core::tool::ToolRiskLevel;
    use agent_extension::store::{InMemoryMemoryStore, InMemoryRunStore};

    use super::*;

    fn test_state() -> AppState {
        test_state_with_approvals(InMemoryApprovalBroker::default())
    }

    fn test_state_with_approvals(approvals: InMemoryApprovalBroker) -> AppState {
        let state_store = Arc::new(InMemoryRunStore::default());
        let store: Arc<dyn RunStore> = state_store.clone();
        let sessions: Arc<dyn SessionStore> = state_store;
        let memory_adapter = Arc::new(InMemoryMemoryStore::default());
        let memory_store: Arc<dyn MemoryStore> = memory_adapter.clone();
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
        AppState {
            runtime: RunRuntime::new(
                Harness::new(RuntimeAgent::Echo(EchoAgent)),
                store,
                move |run_id| approval_cleanup.remove_run(run_id),
            ),
            config: None,
            approvals,
            orchestration: Arc::new(OrchestrationRuntime {
                sessions,
                context,
                skills: SkillOrchestrator::new(skill_store),
                memory_store: Arc::clone(&memory_store),
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

        let (agent, _, _) = build_agent(
            Some(&config),
            InMemoryApprovalBroker::default(),
            Arc::new(HeuristicTokenEstimator),
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
