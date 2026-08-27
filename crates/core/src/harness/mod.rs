//! Transport-agnostic Agent contracts and decision loops.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[cfg(test)]
use futures_util::StreamExt;
#[cfg(test)]
use std::{sync::Arc, time::Duration};
#[cfg(test)]
use thiserror::Error;

mod agent_loop;
mod approval;
mod control;
mod event;
mod job;
mod machine;
mod model;
mod run_state;
mod session;
mod single_turn;
mod tool;

pub use agent_loop::{AgentLoop, ToolCallStrategy};
pub use approval::{
    ApprovalDecision, ApprovalError, ApprovalFuture, ApprovalId, ApprovalPort, ApprovalRequest,
    ApprovalResolution, RejectAllApprovals, ToolApprovalPolicy,
};
pub use control::RunCancellation;
pub use event::{
    AgentEvent, AgentEventStream, OutputChannel, RunEvent, RunEventKind, RunEventStream,
    run_event_stream,
};
pub use job::{
    ClaimJobNotifications, ClaimJobs, CompleteJob, CompleteJobNotification, JobComponentDescriptor,
    JobExecutionError, JobExecutionFuture, JobFuture, JobNotificationStatus, JobOutcome, JobRecord,
    JobRouter, JobStatus, JobStore, JobStoreError, RetryJob, RetryJobNotification, SubmitJobResult,
};
pub use machine::{
    ActivationId, AgentMachine, CheckpointCodec, CheckpointEnvelope, ClaimedActivation,
    CompleteFlowEffect, CompleteFlowRun, ContinueFlowRun, EffectRequest, FlowEffect, FlowEffectId,
    FlowEffectRouter, FlowEffectStatus, FlowError, FlowFuture, FlowInboxItem, FlowRunState,
    FlowRunStatus, FlowStore, JobId, MachineError, MachineOutput, MachineResumeRequest,
    MachineStartRequest, MachineStream, RetryFlowEffect, StartJob, StepOutcome, SuspendFlowRun,
    WaitSpec, WakeFlowRun,
};
pub use model::{
    FinishReason, ModelError, ModelErrorKind, ModelEvent, ModelEventStream, ModelMessage,
    ModelPort, ModelRequest, ModelRole, ModelToolCall, TokenUsage, TokenUsageSource,
};
pub use run_state::{
    ObservedRunEvent, RUN_STATE_SCHEMA_VERSION, RunApprovalState, RunFailure, RunSnapshot,
    RunStateError, RunStatus, RunStore, RunStoreError, RunStoreFuture, RunToolFailure,
    RunToolSetState, RunToolState, RunToolStatus,
};
pub use session::{
    ArchiveSession, BeginRunResult, BeginSessionRun, ContentPart, ConversationRole, CreateSession,
    FinalizeSessionRun, MAX_SESSION_PAGE_SIZE, MessageId, SessionId, SessionMessage,
    SessionSnapshot, SessionStatus, SessionStore, SessionStoreError, SessionStoreFuture,
};
pub use single_turn::SingleTurnAgent;
pub use tool::{
    RunToolSession, Tool, ToolArgumentVisibility, ToolBinding, ToolBindingKind, ToolCallFuture,
    ToolCallRequest, ToolCompletion, ToolConcurrency, ToolDefinition, ToolError, ToolErrorCategory,
    ToolExecutionPolicy, ToolIdempotency, ToolOutput, ToolPort, ToolRegistrationError,
    ToolRegistry, ToolRetryPolicy, ToolRiskLevel, ToolSetSnapshot, ToolSuspension,
};

/// Current semantic protocol exposed by the harness library.
pub const PROTOCOL_VERSION: &str = "0.1";

/// Stable identifier for one agent run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RunId(Uuid);

impl RunId {
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for RunId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for RunId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for RunId {
    type Err = uuid::Error;

    fn from_str(source: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(source).map(Self)
    }
}

/// Discoverable information about an agent implementation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentMetadata {
    pub name: String,
    pub version: String,
    pub protocol_version: String,
    pub capabilities: Vec<String>,
}

impl AgentMetadata {
    #[must_use]
    pub fn new(name: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            version: version.into(),
            protocol_version: PROTOCOL_VERSION.into(),
            capabilities: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_capability(mut self, capability: impl Into<String>) -> Self {
        self.capabilities.push(capability.into());
        self
    }
}

/// Input passed across the harness/agent boundary.
#[derive(Debug, Clone)]
pub struct RunRequest {
    pub run_id: RunId,
    pub input: String,
    pub prior_messages: Vec<ModelMessage>,
    /// `None` exposes all host-registered tools; `Some` is the per-run
    /// capability intersection produced by orchestration.
    pub allowed_tools: Option<Vec<String>>,
    /// Allows bindings created by a policy-approved Run-scoped ADF even though
    /// their content-derived names cannot exist in the initial allow-list.
    pub allow_run_adf: bool,
    /// Optional per-run model-step budget. Agent implementations may apply a
    /// stricter host-level ceiling, but must never exceed this value.
    pub max_steps: Option<u32>,
    pub cancellation: RunCancellation,
}

/// Host-supplied policy for one run.
///
/// This is deliberately separate from conversation context so callers can
/// vary budgets and capabilities without rewriting model messages.
#[cfg(test)]
#[derive(Debug, Clone, Default)]
pub struct RunOptions {
    pub allowed_tools: Option<Vec<String>>,
    pub allow_run_adf: bool,
    pub max_steps: Option<u32>,
}

/// Portable upper bound accepted by the Harness contract. Individual hosts or
/// agents may enforce a smaller ceiling.
#[cfg(test)]
pub const MAX_RUN_STEPS: u32 = 100;

/// Stable response returned by the harness to a host application.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunResponse {
    pub run_id: RunId,
    pub output: String,
    pub finish_reason: FinishReason,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<TokenUsage>,
}

#[cfg(test)]
#[derive(Debug, Error)]
pub enum HarnessError {
    #[error("input must not be empty")]
    InvalidInput,

    #[error("max_steps must be between 1 and {MAX_RUN_STEPS}, got {requested}")]
    InvalidMaxSteps { requested: u32 },

    #[error("agent failed: {message}")]
    Agent {
        code: String,
        message: String,
        retryable: bool,
    },
}

#[cfg(test)]
impl HarnessError {
    #[must_use]
    pub fn code(&self) -> &str {
        match self {
            Self::InvalidInput => "invalid_input",
            Self::InvalidMaxSteps { .. } => "invalid_max_steps",
            Self::Agent { code, .. } => code,
        }
    }

    #[must_use]
    pub fn agent(code: impl Into<String>, message: impl Into<String>, retryable: bool) -> Self {
        Self::Agent {
            code: code.into(),
            message: message.into(),
            retryable,
        }
    }

    #[must_use]
    pub const fn retryable(&self) -> bool {
        match self {
            Self::InvalidInput | Self::InvalidMaxSteps { .. } => false,
            Self::Agent { retryable, .. } => *retryable,
        }
    }
}

/// The narrow contract implemented by an agent.
///
/// It deliberately has no HTTP, CLI, desktop, or provider-specific types.
pub trait Agent: Send + Sync + 'static {
    fn metadata(&self) -> AgentMetadata;

    fn run(&self, request: RunRequest) -> AgentEventStream;
}

/// Owns an agent implementation and exposes the host-facing execution API.
#[cfg(test)]
#[derive(Debug)]
pub struct Harness<A> {
    agent: Arc<A>,
    run_timeout: Duration,
}

#[cfg(test)]
const DEFAULT_RUN_TIMEOUT: Duration = Duration::from_secs(300);

/// A newly accepted run plus the host-owned cancellation handle.
#[cfg(test)]
pub struct RunExecution {
    pub run_id: RunId,
    pub cancellation: RunCancellation,
    pub events: RunEventStream,
}

#[cfg(test)]
impl<A> Clone for Harness<A> {
    fn clone(&self) -> Self {
        Self {
            agent: Arc::clone(&self.agent),
            run_timeout: self.run_timeout,
        }
    }
}

#[cfg(test)]
impl<A> Harness<A>
where
    A: Agent,
{
    #[must_use]
    pub fn new(agent: A) -> Self {
        Self {
            agent: Arc::new(agent),
            run_timeout: DEFAULT_RUN_TIMEOUT,
        }
    }

    #[must_use]
    pub const fn with_run_timeout(mut self, timeout: Duration) -> Self {
        self.run_timeout = timeout;
        self
    }

    #[must_use]
    pub fn metadata(&self) -> AgentMetadata {
        self.agent.metadata()
    }

    /// Shared handle used by hosts that expose both the legacy one-Future
    /// `Agent` adapter and the durable `AgentMachine` coordinator.
    #[must_use]
    pub fn agent_handle(&self) -> Arc<A> {
        Arc::clone(&self.agent)
    }

    pub fn start(&self, input: impl Into<String>) -> Result<RunExecution, HarnessError> {
        self.start_with_context(RunId::new(), input, Vec::new(), None)
    }

    pub fn start_with_run_id(
        &self,
        run_id: RunId,
        input: impl Into<String>,
        prior_messages: Vec<ModelMessage>,
    ) -> Result<RunExecution, HarnessError> {
        self.start_with_context(run_id, input, prior_messages, None)
    }

    pub fn start_with_context(
        &self,
        run_id: RunId,
        input: impl Into<String>,
        prior_messages: Vec<ModelMessage>,
        allowed_tools: Option<Vec<String>>,
    ) -> Result<RunExecution, HarnessError> {
        self.start_with_options(
            run_id,
            input,
            prior_messages,
            RunOptions {
                allowed_tools,
                allow_run_adf: false,
                max_steps: None,
            },
        )
    }

    pub fn start_with_options(
        &self,
        run_id: RunId,
        input: impl Into<String>,
        prior_messages: Vec<ModelMessage>,
        options: RunOptions,
    ) -> Result<RunExecution, HarnessError> {
        let input = input.into();
        if input.trim().is_empty() {
            return Err(HarnessError::InvalidInput);
        }
        if let Some(requested) = options.max_steps
            && !(1..=MAX_RUN_STEPS).contains(&requested)
        {
            return Err(HarnessError::InvalidMaxSteps { requested });
        }

        let cancellation = RunCancellation::new();
        let events = self.agent.run(RunRequest {
            run_id,
            input,
            prior_messages,
            allowed_tools: options.allowed_tools,
            allow_run_adf: options.allow_run_adf,
            max_steps: options.max_steps,
            cancellation: cancellation.clone(),
        });
        Ok(RunExecution {
            run_id,
            cancellation: cancellation.clone(),
            events: event::run_event_stream(run_id, events, cancellation, self.run_timeout),
        })
    }

    pub fn stream(&self, input: impl Into<String>) -> Result<RunEventStream, HarnessError> {
        self.start(input).map(|execution| execution.events)
    }

    pub async fn execute(&self, input: impl Into<String>) -> Result<RunResponse, HarnessError> {
        self.execute_with_options(input, RunOptions::default())
            .await
    }

    pub async fn execute_with_options(
        &self,
        input: impl Into<String>,
        options: RunOptions,
    ) -> Result<RunResponse, HarnessError> {
        let mut events = self
            .start_with_options(RunId::new(), input, Vec::new(), options)?
            .events;
        let mut output = String::new();
        let mut usage = None;

        while let Some(event) = events.next().await {
            match event.kind {
                RunEventKind::RunStarted
                | RunEventKind::RunWaiting { .. }
                | RunEventKind::RunResumed { .. } => {}
                RunEventKind::OutputDelta {
                    channel: OutputChannel::AssistantText,
                    delta,
                } => output.push_str(&delta),
                RunEventKind::OutputDelta { .. } => {}
                RunEventKind::UsageUpdated { usage: next } => usage = Some(next),
                RunEventKind::ToolSetUpdated { .. }
                | RunEventKind::ToolCallStarted { .. }
                | RunEventKind::ToolCallArgumentsDelta { .. }
                | RunEventKind::ApprovalRequested { .. }
                | RunEventKind::ApprovalResolved { .. }
                | RunEventKind::ToolExecutionStarted { .. }
                | RunEventKind::ToolExecutionCompleted { .. }
                | RunEventKind::ToolExecutionFailed { .. } => {}
                RunEventKind::RunCancelled => {
                    return Err(HarnessError::agent(
                        "run_cancelled",
                        "run was cancelled",
                        false,
                    ));
                }
                RunEventKind::RunCompleted { finish_reason } => {
                    return Ok(RunResponse {
                        run_id: event.run_id,
                        output,
                        finish_reason,
                        usage,
                    });
                }
                RunEventKind::RunFailed {
                    code,
                    message,
                    retryable,
                } => return Err(HarnessError::agent(code, message, retryable)),
            }
        }

        Err(HarnessError::agent(
            "agent_protocol_violation",
            "run event stream ended without a terminal event",
            false,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct TestAgent;

    impl Agent for TestAgent {
        fn metadata(&self) -> AgentMetadata {
            AgentMetadata::new("test", "0.1.0")
        }

        fn run(&self, request: RunRequest) -> AgentEventStream {
            Box::pin(futures_util::stream::iter(vec![
                AgentEvent::text_delta(request.input.to_uppercase()),
                AgentEvent::completed(FinishReason::Stop),
            ]))
        }
    }

    #[tokio::test]
    async fn harness_executes_an_agent() {
        let harness = Harness::new(TestAgent);

        let response = harness.execute("hello").await.expect("run should succeed");

        assert_eq!(response.output, "HELLO");
    }

    #[tokio::test]
    async fn harness_rejects_empty_input() {
        let harness = Harness::new(TestAgent);

        let error = harness
            .execute("  ")
            .await
            .expect_err("empty input must fail");

        assert_eq!(error.code(), "invalid_input");
    }

    #[test]
    fn harness_validates_per_run_step_limits() {
        let harness = Harness::new(TestAgent);

        for requested in [0, MAX_RUN_STEPS + 1] {
            let result = harness.start_with_options(
                RunId::new(),
                "hello",
                Vec::new(),
                RunOptions {
                    allowed_tools: None,
                    allow_run_adf: false,
                    max_steps: Some(requested),
                },
            );
            assert!(matches!(
                result,
                Err(HarnessError::InvalidMaxSteps { requested: actual }) if actual == requested
            ));
        }
    }

    #[tokio::test]
    async fn harness_streams_sequenced_events() {
        let harness = Harness::new(TestAgent);
        let events: Vec<_> = harness
            .stream("hello")
            .expect("stream should start")
            .collect()
            .await;

        assert!(matches!(events[0].kind, RunEventKind::RunStarted));
        assert!(matches!(events[1].kind, RunEventKind::OutputDelta { .. }));
        assert!(matches!(events[2].kind, RunEventKind::RunCompleted { .. }));
    }
}
