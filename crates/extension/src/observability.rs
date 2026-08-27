use std::{
    collections::VecDeque,
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use agent_core::harness::{
    Agent, AgentEvent, AgentEventStream, AgentMachine, AgentMetadata, ApprovalDecision,
    CheckpointEnvelope, MachineError, MachineOutput, MachineResumeRequest, MachineStartRequest,
    MachineStream, ModelEvent, ModelEventStream, ModelPort, ModelRequest, RunRequest, StepOutcome,
    TokenUsage, ToolArgumentVisibility, ToolCallFuture, ToolCallRequest, ToolDefinition, ToolError,
    ToolPort, ToolRiskLevel, ToolSetSnapshot,
};
pub use agent_core::observability::{
    NoopObservationHook, ObservationEvent, ObservationHook, ObservationKind, ObservationStatus,
};
use async_stream::stream;
use futures_util::StreamExt;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug, Default)]
pub struct TracingObservationHook;

impl ObservationHook for TracingObservationHook {
    fn record(&self, event: ObservationEvent) {
        let event_type = event_type(&event.kind);
        let payload = serde_json::to_string(&event.kind)
            .unwrap_or_else(|_| "{\"type\":\"serialization_failed\"}".to_owned());
        tracing::info!(
            target: "mina::observation",
            trace_id = %event.trace_id,
            observation_id = %event.observation_id,
            observation_type = event_type,
            timestamp_ms = event.timestamp_ms,
            payload = %payload,
            "agent observation"
        );
    }
}

#[derive(Default)]
pub struct CompositeObservationHook {
    hooks: Vec<Arc<dyn ObservationHook>>,
}

impl CompositeObservationHook {
    #[must_use]
    pub fn new(hooks: Vec<Arc<dyn ObservationHook>>) -> Self {
        Self { hooks }
    }
}

impl ObservationHook for CompositeObservationHook {
    fn record(&self, event: ObservationEvent) {
        for hook in &self.hooks {
            emit(hook, event.clone());
        }
    }
}

/// AgentMachine middleware that observes activation outcomes and approval
/// lifecycle events without changing checkpoint or scheduling semantics.
pub struct ObservedMachine<M> {
    inner: Arc<M>,
    hook: Arc<dyn ObservationHook>,
}

impl<M> ObservedMachine<M> {
    pub fn new(inner: M, hook: Arc<dyn ObservationHook>) -> Self {
        Self {
            inner: Arc::new(inner),
            hook,
        }
    }
}

impl<M> Agent for ObservedMachine<M>
where
    M: Agent,
{
    fn metadata(&self) -> AgentMetadata {
        self.inner.metadata()
    }

    fn run(&self, request: RunRequest) -> AgentEventStream {
        let run_id = request.run_id.to_string();
        let agent_kind = self.inner.metadata().name;
        let stream = self.inner.run(request);
        observe_agent_stream(stream, Arc::clone(&self.hook), run_id, agent_kind)
    }
}

fn observe_agent_stream(
    mut inner: AgentEventStream,
    hook: Arc<dyn ObservationHook>,
    trace_id: String,
    agent_kind: String,
) -> AgentEventStream {
    let observation_id = format!("activation:{trace_id}:legacy");
    emit(
        &hook,
        observation(
            &trace_id,
            &observation_id,
            ObservationKind::FlowActivationStarted {
                agent_kind: agent_kind.clone(),
                activation_id: None,
                resumed: false,
                inbox_count: 0,
            },
        ),
    );
    Box::pin(stream! {
        let started = Instant::now();
        let mut terminal = false;
        while let Some(event) = inner.next().await {
            let (outcome, error_code, retryable) = match &event {
                AgentEvent::Completed { .. } => (Some("complete"), None, None),
                AgentEvent::Failed { code, retryable, .. } => {
                    (Some("failed"), Some(code.clone()), Some(*retryable))
                }
                AgentEvent::Cancelled => (Some("cancelled"), None, None),
                _ => (None, None, None),
            };
            if let Some(outcome) = outcome {
                terminal = true;
                emit(
                    &hook,
                    observation(
                        &trace_id,
                        &observation_id,
                        ObservationKind::FlowActivationFinished {
                            agent_kind: agent_kind.clone(),
                            activation_id: None,
                            outcome: outcome.into(),
                            duration_ms: duration_ms(started.elapsed()),
                            wait_count: 0,
                            effect_count: 0,
                            error_code,
                            retryable,
                        },
                    ),
                );
            }
            yield event;
        }
        if !terminal {
            emit(
                &hook,
                observation(
                    &trace_id,
                    &observation_id,
                    ObservationKind::FlowActivationFinished {
                        agent_kind,
                        activation_id: None,
                        outcome: "incomplete".into(),
                        duration_ms: duration_ms(started.elapsed()),
                        wait_count: 0,
                        effect_count: 0,
                        error_code: Some("agent_stream_incomplete".into()),
                        retryable: Some(false),
                    },
                ),
            );
        }
    })
}

impl<M> AgentMachine for ObservedMachine<M>
where
    M: AgentMachine,
{
    fn metadata(&self) -> AgentMetadata {
        self.inner.metadata()
    }

    fn initial_checkpoint(
        &self,
        request: &MachineStartRequest,
    ) -> Result<CheckpointEnvelope, MachineError> {
        self.inner.initial_checkpoint(request)
    }

    fn start(&self, request: MachineStartRequest) -> MachineStream {
        let run_id = request.run_id;
        let agent_kind = self.inner.metadata().name;
        let stream = self.inner.start(request);
        observe_machine_stream(
            stream,
            Arc::clone(&self.hook),
            run_id.to_string(),
            agent_kind,
            None,
            false,
            0,
        )
    }

    fn resume(&self, request: MachineResumeRequest) -> MachineStream {
        let run_id = request.run_id;
        let activation_id = request.activation_id.to_string();
        let inbox_count = request.inbox.len();
        let agent_kind = self.inner.metadata().name;
        let stream = self.inner.resume(request);
        observe_machine_stream(
            stream,
            Arc::clone(&self.hook),
            run_id.to_string(),
            agent_kind,
            Some(activation_id),
            true,
            inbox_count,
        )
    }
}

#[allow(clippy::too_many_arguments)]
fn observe_machine_stream(
    mut inner: MachineStream,
    hook: Arc<dyn ObservationHook>,
    trace_id: String,
    agent_kind: String,
    activation_id: Option<String>,
    resumed: bool,
    inbox_count: usize,
) -> MachineStream {
    let observation_id = format!(
        "activation:{}:{}",
        trace_id,
        activation_id.as_deref().unwrap_or("start")
    );
    emit(
        &hook,
        observation(
            &trace_id,
            &observation_id,
            ObservationKind::FlowActivationStarted {
                agent_kind: agent_kind.clone(),
                activation_id: activation_id.clone(),
                resumed,
                inbox_count,
            },
        ),
    );
    Box::pin(stream! {
        let started = Instant::now();
        let mut yielded = false;
        while let Some(output) = inner.next().await {
            match &output {
                MachineOutput::Event(AgentEvent::ApprovalRequested {
                    approval_id,
                    call_id,
                    tool_name,
                    risk_level,
                    ..
                }) => emit(
                    &hook,
                    observation(
                        &trace_id,
                        &format!("approval:{approval_id}"),
                        ObservationKind::ApprovalRequested {
                            approval_id: approval_id.to_string(),
                            call_id: call_id.clone(),
                            tool_name: tool_name.clone(),
                            risk_level: risk_level_name(*risk_level).into(),
                        },
                    ),
                ),
                MachineOutput::Event(AgentEvent::ApprovalResolved {
                    approval_id,
                    call_id,
                    resolution,
                }) => emit(
                    &hook,
                    observation(
                        &trace_id,
                        &format!("approval:{approval_id}"),
                        ObservationKind::ApprovalResolved {
                            approval_id: approval_id.to_string(),
                            call_id: call_id.clone(),
                            decision: approval_decision_name(resolution.decision).into(),
                        },
                    ),
                ),
                MachineOutput::Yield(outcome) => {
                    yielded = true;
                    let projection = flow_outcome_projection(outcome);
                    emit(
                        &hook,
                        observation(
                            &trace_id,
                            &observation_id,
                            ObservationKind::FlowActivationFinished {
                                agent_kind: agent_kind.clone(),
                                activation_id: activation_id.clone(),
                                outcome: projection.outcome.into(),
                                duration_ms: duration_ms(started.elapsed()),
                                wait_count: projection.wait_count,
                                effect_count: projection.effect_count,
                                error_code: projection.error_code,
                                retryable: projection.retryable,
                            },
                        ),
                    );
                }
                _ => {}
            }
            yield output;
        }
        if !yielded {
            emit(
                &hook,
                observation(
                    &trace_id,
                    &observation_id,
                    ObservationKind::FlowActivationFinished {
                        agent_kind,
                        activation_id,
                        outcome: "incomplete".into(),
                        duration_ms: duration_ms(started.elapsed()),
                        wait_count: 0,
                        effect_count: 0,
                        error_code: Some("machine_stream_incomplete".into()),
                        retryable: Some(false),
                    },
                ),
            );
        }
    })
}

struct FlowOutcomeProjection {
    outcome: &'static str,
    wait_count: usize,
    effect_count: usize,
    error_code: Option<String>,
    retryable: Option<bool>,
}

fn flow_outcome_projection(outcome: &StepOutcome) -> FlowOutcomeProjection {
    match outcome {
        StepOutcome::Continue { effects, .. } => FlowOutcomeProjection {
            outcome: "continue",
            wait_count: 0,
            effect_count: effects.len(),
            error_code: None,
            retryable: None,
        },
        StepOutcome::Suspend { waits, effects, .. } => FlowOutcomeProjection {
            outcome: "suspend",
            wait_count: waits.len(),
            effect_count: effects.len(),
            error_code: None,
            retryable: None,
        },
        StepOutcome::Complete { .. } => FlowOutcomeProjection {
            outcome: "complete",
            wait_count: 0,
            effect_count: 0,
            error_code: None,
            retryable: None,
        },
        StepOutcome::Failed { error } => FlowOutcomeProjection {
            outcome: "failed",
            wait_count: 0,
            effect_count: 0,
            error_code: Some(error.code.clone()),
            retryable: Some(error.retryable),
        },
        StepOutcome::Cancelled => FlowOutcomeProjection {
            outcome: "cancelled",
            wait_count: 0,
            effect_count: 0,
            error_code: None,
            retryable: None,
        },
    }
}

pub struct ObservedModel<P> {
    inner: Arc<P>,
    hook: Arc<dyn ObservationHook>,
    sequence: AtomicU64,
}

impl<P> ObservedModel<P> {
    pub fn new(inner: P, hook: Arc<dyn ObservationHook>) -> Self {
        Self {
            inner: Arc::new(inner),
            hook,
            sequence: AtomicU64::new(1),
        }
    }
}

impl<P> ModelPort for ObservedModel<P>
where
    P: ModelPort,
{
    fn stream(&self, request: ModelRequest) -> ModelEventStream {
        let inner = Arc::clone(&self.inner);
        let hook = Arc::clone(&self.hook);
        let trace_id = request.run_id.to_string();
        let observation_id = format!(
            "model:{}:{}",
            request.run_id,
            self.sequence.fetch_add(1, Ordering::Relaxed)
        );
        let model = request.model.clone();
        let message_count = request.messages.len();
        let tool_count = request.tools.len();
        let requested_at = Instant::now();
        emit(
            &hook,
            observation(
                &trace_id,
                &observation_id,
                ObservationKind::ModelStarted {
                    model: model.clone(),
                    message_count,
                    tool_count,
                },
            ),
        );
        let mut events = inner.stream(request);
        Box::pin(stream! {
            let mut usage = None;
            let mut ttft_ms = None;
            let mut terminal = false;
            while let Some(event) = futures_next(&mut events).await {
                match &event {
                    ModelEvent::ReasoningDelta { delta } | ModelEvent::TextDelta { delta }
                        if ttft_ms.is_none() && !delta.is_empty() =>
                    {
                        let elapsed = duration_ms(requested_at.elapsed());
                        ttft_ms = Some(elapsed);
                        emit(
                            &hook,
                            observation(
                                &trace_id,
                                &observation_id,
                                ObservationKind::ModelFirstToken {
                                    model: model.clone(),
                                    ttft_ms: elapsed,
                                },
                            ),
                        );
                    }
                    ModelEvent::Usage { usage: next } => usage = Some(*next),
                    ModelEvent::Completed { finish_reason } => {
                        terminal = true;
                        emit_model_finished(
                            &hook,
                            ModelFinish {
                                trace_id: &trace_id,
                                observation_id: &observation_id,
                                model: &model,
                                duration: requested_at.elapsed(),
                                ttft_ms,
                                status: ObservationStatus::Completed,
                                finish_reason: Some(finish_reason_name(*finish_reason).into()),
                                error_code: None,
                                retryable: None,
                                usage,
                            },
                        );
                    }
                    ModelEvent::Failed { error } => {
                        terminal = true;
                        emit_model_finished(
                            &hook,
                            ModelFinish {
                                trace_id: &trace_id,
                                observation_id: &observation_id,
                                model: &model,
                                duration: requested_at.elapsed(),
                                ttft_ms,
                                status: ObservationStatus::Failed,
                                finish_reason: None,
                                error_code: Some(error.kind().code().to_owned()),
                                retryable: Some(error.retryable()),
                                usage,
                            },
                        );
                    }
                    _ => {}
                }
                yield event;
            }
            if !terminal {
                emit_model_finished(
                    &hook,
                    ModelFinish {
                        trace_id: &trace_id,
                        observation_id: &observation_id,
                        model: &model,
                        duration: requested_at.elapsed(),
                        ttft_ms,
                        status: ObservationStatus::Failed,
                        finish_reason: None,
                        error_code: Some("model_stream_incomplete".into()),
                        retryable: Some(false),
                        usage,
                    },
                );
            }
        })
    }
}

pub struct ObservedTools<T> {
    inner: Arc<T>,
    hook: Arc<dyn ObservationHook>,
    sequence: AtomicU64,
}

impl<T> ObservedTools<T> {
    pub fn new(inner: T, hook: Arc<dyn ObservationHook>) -> Self {
        Self {
            inner: Arc::new(inner),
            hook,
            sequence: AtomicU64::new(1),
        }
    }
}

impl<T> ToolPort for ObservedTools<T>
where
    T: ToolPort,
{
    fn definitions(&self) -> Vec<ToolDefinition> {
        self.inner.definitions()
    }

    fn tool_set_snapshot(
        &self,
        run_id: agent_core::harness::RunId,
    ) -> Result<ToolSetSnapshot, ToolError> {
        self.inner.tool_set_snapshot(run_id)
    }

    fn argument_visibility(&self, name: &str) -> ToolArgumentVisibility {
        self.inner.argument_visibility(name)
    }

    fn validate(
        &self,
        name: &str,
        arguments: &serde_json::Value,
    ) -> Result<(), agent_core::harness::ToolError> {
        self.inner.validate(name, arguments)
    }

    fn validate_at(
        &self,
        run_id: agent_core::harness::RunId,
        tool_set_revision: u64,
        name: &str,
        arguments: &serde_json::Value,
    ) -> Result<(), ToolError> {
        self.inner
            .validate_at(run_id, tool_set_revision, name, arguments)
    }

    fn call(&self, request: ToolCallRequest) -> ToolCallFuture {
        let future = self.inner.call(request.clone());
        self.observe_call(request, future)
    }

    fn call_at(&self, tool_set_revision: u64, request: ToolCallRequest) -> ToolCallFuture {
        let future = self.inner.call_at(tool_set_revision, request.clone());
        self.observe_call(request, future)
    }

    fn close_run(&self, run_id: agent_core::harness::RunId) {
        self.inner.close_run(run_id);
    }
}

impl<T> ObservedTools<T>
where
    T: ToolPort,
{
    fn observe_call(&self, request: ToolCallRequest, future: ToolCallFuture) -> ToolCallFuture {
        let trace_id = request.run_id.to_string();
        let call_id = request.call_id.clone();
        let tool_name = request.name.clone();
        let observation_id = format!(
            "tool:{}:{}:{}",
            request.run_id,
            call_id,
            self.sequence.fetch_add(1, Ordering::Relaxed)
        );
        let hook = Arc::clone(&self.hook);
        emit(
            &hook,
            observation(
                &trace_id,
                &observation_id,
                ObservationKind::ToolStarted {
                    call_id: call_id.clone(),
                    tool_name: tool_name.clone(),
                },
            ),
        );
        Box::pin(async move {
            let started = Instant::now();
            let result = future.await;
            let kind = match &result {
                Ok(_) => ObservationKind::ToolFinished {
                    call_id,
                    tool_name,
                    status: ObservationStatus::Completed,
                    duration_ms: duration_ms(started.elapsed()),
                    error_code: None,
                    error_category: None,
                    retryable: None,
                    retry_after_ms: None,
                },
                Err(error) => ObservationKind::ToolFinished {
                    call_id,
                    tool_name,
                    status: ObservationStatus::Failed,
                    duration_ms: duration_ms(started.elapsed()),
                    error_code: Some(error.code().to_owned()),
                    error_category: Some(error.category().as_str().to_owned()),
                    retryable: Some(error.retryable()),
                    retry_after_ms: error.retry_after_ms(),
                },
            };
            emit(&hook, observation(&trace_id, &observation_id, kind));
            result
        })
    }
}

pub type ObservationExportFuture =
    Pin<Box<dyn Future<Output = Result<(), ObservationExportError>> + Send + 'static>>;

/// Host-side adapter contract for OTLP, Langfuse, or another monitoring sink.
/// Implementations receive already-redacted, bounded batches.
pub trait ObservationExporter: Send + Sync + 'static {
    fn export(&self, events: Vec<ObservationEvent>) -> ObservationExportFuture;
}

/// Adapts an existing synchronous hook (for example tracing) to the bounded
/// async worker so Agent threads never perform sink I/O directly.
pub struct HookObservationExporter {
    hook: Arc<dyn ObservationHook>,
}

impl HookObservationExporter {
    #[must_use]
    pub fn new(hook: Arc<dyn ObservationHook>) -> Self {
        Self { hook }
    }
}

impl ObservationExporter for HookObservationExporter {
    fn export(&self, events: Vec<ObservationEvent>) -> ObservationExportFuture {
        let hook = Arc::clone(&self.hook);
        Box::pin(async move {
            for event in events {
                emit(&hook, event);
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
#[error("{message}")]
pub struct ObservationExportError {
    message: String,
}

impl ObservationExportError {
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AsyncObservationConfig {
    pub capacity: usize,
    pub batch_size: usize,
    pub flush_interval: Duration,
    pub max_attempts: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl Default for AsyncObservationConfig {
    fn default() -> Self {
        Self {
            capacity: 2_048,
            batch_size: 64,
            flush_interval: Duration::from_secs(1),
            max_attempts: 3,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(2),
        }
    }
}

impl AsyncObservationConfig {
    fn validate(self) -> Result<Self, AsyncObservationError> {
        if self.capacity == 0 || self.batch_size == 0 || self.batch_size > self.capacity {
            return Err(AsyncObservationError::InvalidConfig(
                "capacity and batch_size must be positive, with batch_size no greater than capacity"
                    .into(),
            ));
        }
        if self.flush_interval.is_zero()
            || self.max_attempts == 0
            || self.max_attempts > 10
            || self.initial_backoff.is_zero()
            || self.max_backoff < self.initial_backoff
        {
            return Err(AsyncObservationError::InvalidConfig(
                "flush/retry timing and max_attempts are invalid".into(),
            ));
        }
        Ok(self)
    }
}

#[derive(Debug, Error)]
pub enum AsyncObservationError {
    #[error("invalid async observation config: {0}")]
    InvalidConfig(String),
    #[error("a Tokio runtime is required for the async observation worker")]
    RuntimeUnavailable,
    #[error("the async observation worker is unavailable")]
    WorkerUnavailable,
    #[error("observation export failed: {0}")]
    Export(#[from] ObservationExportError),
    #[error("the async observation worker failed: {0}")]
    WorkerFailed(String),
}

enum ObservationCommand {
    Event(ObservationEvent),
    Flush(oneshot::Sender<Result<(), ObservationExportError>>),
    Shutdown(oneshot::Sender<Result<(), ObservationExportError>>),
}

/// Non-blocking hook backed by a bounded queue. `record` only attempts an
/// enqueue; network I/O, batching, retry, and shutdown flush happen on a Host
/// worker and can never delay Agent execution.
pub struct AsyncObservationHook {
    sender: mpsc::Sender<ObservationCommand>,
    dropped: Arc<AtomicU64>,
    worker: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl AsyncObservationHook {
    pub fn new(
        exporter: Arc<dyn ObservationExporter>,
        config: AsyncObservationConfig,
    ) -> Result<Self, AsyncObservationError> {
        let config = config.validate()?;
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| AsyncObservationError::RuntimeUnavailable)?;
        let (sender, receiver) = mpsc::channel(config.capacity);
        let dropped = Arc::new(AtomicU64::new(0));
        let worker_dropped = Arc::clone(&dropped);
        let worker = runtime.spawn(observation_worker(
            receiver,
            exporter,
            config,
            worker_dropped,
        ));
        Ok(Self {
            sender,
            dropped,
            worker: Mutex::new(Some(worker)),
        })
    }

    #[must_use]
    pub fn dropped_events(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    pub async fn flush(&self) -> Result<(), AsyncObservationError> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(ObservationCommand::Flush(sender))
            .await
            .map_err(|_| AsyncObservationError::WorkerUnavailable)?;
        receiver
            .await
            .map_err(|_| AsyncObservationError::WorkerUnavailable)??;
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<(), AsyncObservationError> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(ObservationCommand::Shutdown(sender))
            .await
            .map_err(|_| AsyncObservationError::WorkerUnavailable)?;
        receiver
            .await
            .map_err(|_| AsyncObservationError::WorkerUnavailable)??;
        let worker = self
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(worker) = worker {
            worker
                .await
                .map_err(|error| AsyncObservationError::WorkerFailed(error.to_string()))?;
        }
        Ok(())
    }
}

impl ObservationHook for AsyncObservationHook {
    fn record(&self, event: ObservationEvent) {
        if self
            .sender
            .try_send(ObservationCommand::Event(event))
            .is_err()
        {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

async fn observation_worker(
    mut receiver: mpsc::Receiver<ObservationCommand>,
    exporter: Arc<dyn ObservationExporter>,
    config: AsyncObservationConfig,
    dropped: Arc<AtomicU64>,
) {
    let mut batch = VecDeque::with_capacity(config.batch_size);
    let mut interval = tokio::time::interval(config.flush_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval.tick().await;
    loop {
        tokio::select! {
            biased;
            command = receiver.recv() => match command {
                Some(ObservationCommand::Event(event)) => {
                    batch.push_back(event);
                    if batch.len() >= config.batch_size {
                        let _ = export_observation_batch(
                            &exporter,
                            &mut batch,
                            config,
                            &dropped,
                        ).await;
                    }
                }
                Some(ObservationCommand::Flush(reply)) => {
                    let result = export_observation_batch(
                        &exporter,
                        &mut batch,
                        config,
                        &dropped,
                    ).await;
                    let _ = reply.send(result);
                }
                Some(ObservationCommand::Shutdown(reply)) => {
                    while let Ok(command) = receiver.try_recv() {
                        if let ObservationCommand::Event(event) = command {
                            batch.push_back(event);
                        }
                    }
                    let result = export_observation_batch(
                        &exporter,
                        &mut batch,
                        config,
                        &dropped,
                    ).await;
                    let _ = reply.send(result);
                    return;
                }
                None => {
                    let _ = export_observation_batch(
                        &exporter,
                        &mut batch,
                        config,
                        &dropped,
                    ).await;
                    return;
                }
            },
            _ = interval.tick() => {
                let _ = export_observation_batch(
                    &exporter,
                    &mut batch,
                    config,
                    &dropped,
                ).await;
            }
        }
    }
}

async fn export_observation_batch(
    exporter: &Arc<dyn ObservationExporter>,
    pending: &mut VecDeque<ObservationEvent>,
    config: AsyncObservationConfig,
    dropped: &AtomicU64,
) -> Result<(), ObservationExportError> {
    if pending.is_empty() {
        return Ok(());
    }
    let events = pending.drain(..).collect::<Vec<_>>();
    let mut attempts = 0_u32;
    loop {
        attempts = attempts.saturating_add(1);
        match exporter.export(events.clone()).await {
            Ok(()) => return Ok(()),
            Err(_) if attempts < config.max_attempts => {
                let exponent = attempts.saturating_sub(1).min(20);
                let multiplier = 1_u32 << exponent;
                let delay = config
                    .initial_backoff
                    .saturating_mul(multiplier)
                    .min(config.max_backoff);
                tokio::time::sleep(delay).await;
            }
            Err(error) => {
                dropped.fetch_add(events.len() as u64, Ordering::Relaxed);
                return Err(error);
            }
        }
    }
}

async fn futures_next(stream: &mut ModelEventStream) -> Option<ModelEvent> {
    stream.next().await
}

struct ModelFinish<'a> {
    trace_id: &'a str,
    observation_id: &'a str,
    model: &'a str,
    duration: Duration,
    ttft_ms: Option<u64>,
    status: ObservationStatus,
    finish_reason: Option<String>,
    error_code: Option<String>,
    retryable: Option<bool>,
    usage: Option<TokenUsage>,
}

fn emit_model_finished(hook: &Arc<dyn ObservationHook>, finished: ModelFinish<'_>) {
    emit(
        hook,
        observation(
            finished.trace_id,
            finished.observation_id,
            ObservationKind::ModelFinished {
                model: finished.model.to_owned(),
                status: finished.status,
                duration_ms: duration_ms(finished.duration),
                ttft_ms: finished.ttft_ms,
                finish_reason: finished.finish_reason,
                error_code: finished.error_code,
                retryable: finished.retryable,
                input_tokens: finished.usage.map(|value| value.input_tokens),
                output_tokens: finished.usage.map(|value| value.output_tokens),
            },
        ),
    );
}

fn observation(trace_id: &str, observation_id: &str, kind: ObservationKind) -> ObservationEvent {
    ObservationEvent {
        trace_id: trace_id.to_owned(),
        observation_id: observation_id.to_owned(),
        timestamp_ms: unix_time_ms(),
        kind,
    }
}

fn emit(hook: &Arc<dyn ObservationHook>, event: ObservationEvent) {
    let _ = catch_unwind(AssertUnwindSafe(|| hook.record(event)));
}

fn unix_time_ms() -> i64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    i64::try_from(millis).unwrap_or(i64::MAX)
}

fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

const fn event_type(kind: &ObservationKind) -> &'static str {
    match kind {
        ObservationKind::ModelStarted { .. } => "model_started",
        ObservationKind::ModelFirstToken { .. } => "model_first_token",
        ObservationKind::ModelFinished { .. } => "model_finished",
        ObservationKind::ToolStarted { .. } => "tool_started",
        ObservationKind::ToolFinished { .. } => "tool_finished",
        ObservationKind::FlowActivationStarted { .. } => "flow_activation_started",
        ObservationKind::FlowActivationFinished { .. } => "flow_activation_finished",
        ObservationKind::ApprovalRequested { .. } => "approval_requested",
        ObservationKind::ApprovalResolved { .. } => "approval_resolved",
    }
}

const fn risk_level_name(risk_level: ToolRiskLevel) -> &'static str {
    match risk_level {
        ToolRiskLevel::Low => "low",
        ToolRiskLevel::Medium => "medium",
        ToolRiskLevel::High => "high",
    }
}

const fn approval_decision_name(decision: ApprovalDecision) -> &'static str {
    match decision {
        ApprovalDecision::AllowOnce => "allow_once",
        ApprovalDecision::Deny => "deny",
    }
}

const fn finish_reason_name(reason: agent_core::harness::FinishReason) -> &'static str {
    match reason {
        agent_core::harness::FinishReason::Stop => "stop",
        agent_core::harness::FinishReason::Length => "length",
        agent_core::harness::FinishReason::ToolCall => "tool_call",
        agent_core::harness::FinishReason::ContentFilter => "content_filter",
        agent_core::harness::FinishReason::Unknown => "unknown",
        _ => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use agent_core::harness::{
        ApprovalId, ApprovalResolution, FinishReason, ModelEvent, ModelEventStream, ModelMessage,
        ModelRequest, RunId, TokenUsage, TokenUsageSource,
    };
    use futures_util::{StreamExt, stream};

    use super::*;

    #[derive(Default)]
    struct RecordingHook {
        events: Mutex<Vec<ObservationEvent>>,
    }

    impl ObservationHook for RecordingHook {
        fn record(&self, event: ObservationEvent) {
            self.events
                .lock()
                .expect("recording hook lock should be available")
                .push(event);
        }
    }

    struct FakeModel;

    impl ModelPort for FakeModel {
        fn stream(&self, _request: ModelRequest) -> ModelEventStream {
            Box::pin(stream::iter([
                ModelEvent::Usage {
                    usage: TokenUsage {
                        input_tokens: 12,
                        output_tokens: 3,
                        total_tokens: 15,
                        source: TokenUsageSource::ProviderReported,
                    },
                },
                ModelEvent::Completed {
                    finish_reason: FinishReason::Stop,
                },
            ]))
        }
    }

    #[tokio::test]
    async fn model_middleware_records_one_generation_without_content() {
        let hook = Arc::new(RecordingHook::default());
        let observed = ObservedModel::new(FakeModel, hook.clone());
        let mut events = observed.stream(ModelRequest {
            run_id: RunId::new(),
            model: "test-model".into(),
            messages: vec![ModelMessage::user("secret prompt")],
            tools: Vec::new(),
            max_output_tokens: None,
        });
        while events.next().await.is_some() {}

        let recorded = hook
            .events
            .lock()
            .expect("recording hook lock should be available");
        assert_eq!(recorded.len(), 2);
        assert!(matches!(
            recorded[0].kind,
            ObservationKind::ModelStarted { .. }
        ));
        assert!(matches!(
            recorded[1].kind,
            ObservationKind::ModelFinished {
                input_tokens: Some(12),
                output_tokens: Some(3),
                ..
            }
        ));
        let serialized = serde_json::to_string(&*recorded).expect("events should serialize");
        assert!(!serialized.contains("secret prompt"));
    }

    struct TokenModel;

    impl ModelPort for TokenModel {
        fn stream(&self, _request: ModelRequest) -> ModelEventStream {
            Box::pin(stream::iter([
                ModelEvent::TextDelta {
                    delta: "hello".into(),
                },
                ModelEvent::Completed {
                    finish_reason: FinishReason::Stop,
                },
            ]))
        }
    }

    #[tokio::test]
    async fn model_middleware_records_time_to_first_token() {
        let hook = Arc::new(RecordingHook::default());
        let observed = ObservedModel::new(TokenModel, hook.clone());
        let mut events = observed.stream(ModelRequest {
            run_id: RunId::new(),
            model: "test-model".into(),
            messages: vec![ModelMessage::user("hello")],
            tools: Vec::new(),
            max_output_tokens: None,
        });
        while events.next().await.is_some() {}

        let recorded = hook
            .events
            .lock()
            .expect("recording hook lock should be available");
        assert!(matches!(
            recorded[1].kind,
            ObservationKind::ModelFirstToken { .. }
        ));
        assert!(matches!(
            recorded[2].kind,
            ObservationKind::ModelFinished {
                ttft_ms: Some(_),
                ..
            }
        ));
    }

    struct RecordingExporter {
        attempts: Arc<AtomicUsize>,
        failures_before_success: usize,
        batches: Arc<Mutex<Vec<Vec<ObservationEvent>>>>,
    }

    impl ObservationExporter for RecordingExporter {
        fn export(&self, events: Vec<ObservationEvent>) -> ObservationExportFuture {
            let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
            let failures_before_success = self.failures_before_success;
            let batches = Arc::clone(&self.batches);
            Box::pin(async move {
                if attempt < failures_before_success {
                    return Err(ObservationExportError::new("temporary failure"));
                }
                batches
                    .lock()
                    .expect("recording exporter lock should be available")
                    .push(events);
                Ok(())
            })
        }
    }

    #[tokio::test]
    async fn async_hook_batches_retries_and_flushes_on_shutdown() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let batches = Arc::new(Mutex::new(Vec::new()));
        let hook = AsyncObservationHook::new(
            Arc::new(RecordingExporter {
                attempts: Arc::clone(&attempts),
                failures_before_success: 1,
                batches: Arc::clone(&batches),
            }),
            AsyncObservationConfig {
                capacity: 8,
                batch_size: 8,
                flush_interval: Duration::from_secs(60),
                max_attempts: 2,
                initial_backoff: Duration::from_millis(1),
                max_backoff: Duration::from_millis(1),
            },
        )
        .expect("worker should start");
        hook.record(observation(
            "run",
            "one",
            ObservationKind::ModelStarted {
                model: "test".into(),
                message_count: 1,
                tool_count: 0,
            },
        ));
        hook.record(observation(
            "run",
            "two",
            ObservationKind::ToolStarted {
                call_id: "call".into(),
                tool_name: "read".into(),
            },
        ));

        hook.shutdown().await.expect("shutdown should flush");

        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_eq!(hook.dropped_events(), 0);
        let batches = batches
            .lock()
            .expect("recording exporter lock should be available");
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 2);
    }

    #[tokio::test]
    async fn machine_middleware_records_approval_and_activation_outcome() {
        let hook = Arc::new(RecordingHook::default());
        let approval_id = ApprovalId::new();
        let outputs = vec![
            MachineOutput::Event(AgentEvent::ApprovalRequested {
                approval_id,
                call_id: "call".into(),
                tool_name: "write".into(),
                risk_level: ToolRiskLevel::Medium,
                arguments: serde_json::json!({}),
            }),
            MachineOutput::Event(AgentEvent::ApprovalResolved {
                approval_id,
                call_id: "call".into(),
                resolution: ApprovalResolution::allow_once(),
            }),
            MachineOutput::Yield(StepOutcome::Complete {
                finish_reason: FinishReason::Stop,
            }),
        ];
        let mut observed = observe_machine_stream(
            Box::pin(stream::iter(outputs)),
            hook.clone(),
            RunId::new().to_string(),
            "test-agent".into(),
            None,
            false,
            0,
        );
        while observed.next().await.is_some() {}

        let recorded = hook
            .events
            .lock()
            .expect("recording hook lock should be available");
        assert!(
            recorded
                .iter()
                .any(|event| matches!(event.kind, ObservationKind::ApprovalRequested { .. }))
        );
        assert!(
            recorded
                .iter()
                .any(|event| matches!(event.kind, ObservationKind::ApprovalResolved { .. }))
        );
        assert!(recorded.iter().any(|event| matches!(
            &event.kind,
            ObservationKind::FlowActivationFinished { outcome, .. }
                if outcome == "complete"
        )));
    }
}
