//! Provider-neutral Agent, model, tool, storage and lifecycle contracts.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[cfg(test)]
use futures_util::StreamExt;

mod agent_loop;
mod approval;
mod blob;
mod control;
mod event;
mod job;
mod machine;
mod model;
mod run_state;
mod session;
mod title;
mod tool;

pub(crate) use tool::digest_only_arguments;

pub use agent_loop::{
    AGENT_LOOP_CHECKPOINT_SCHEMA_VERSION, AGENT_LOOP_REDUCER_PROTOCOL_VERSION, AgentLoopDispatch,
    AgentLoopEffect, AgentLoopEffectKind, AgentLoopInput, AgentLoopOutcome, AgentLoopReducer,
    AgentLoopReducerConfig, AgentLoopReducerState, AgentLoopStart, AgentLoopStartDispatch,
    AgentLoopTransition, ModelInvocationPurpose, PortableToolError, ToolCallStrategy,
    ToolInvocation, ToolInvocationResult, ToolValidation, ToolValidationRequest,
    resumed_tool_content,
};
pub use approval::{
    ApprovalDecision, ApprovalError, ApprovalFuture, ApprovalId, ApprovalPort, ApprovalRequest,
    ApprovalResolution, RejectAllApprovals, ToolApprovalPolicy,
};
pub use blob::{
    BlobId, BlobMetadata, BlobObject, BlobStore, BlobStoreError, BlobStoreFuture, PutBlob,
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
    MachineStartRequest, MachineStream, RenewFlowLease, RetryFlowEffect, StartJob, StepOutcome,
    SuspendFlowRun, WaitSpec, WakeFlowRun,
};
pub use model::{
    FinishReason, ModelAttachment, ModelError, ModelErrorKind, ModelEvent, ModelEventStream,
    ModelMessage, ModelPort, ModelRequest, ModelRole, ModelToolCall, TokenUsage, TokenUsageSource,
};
pub use run_state::{
    ObservedRunEvent, RUN_STATE_SCHEMA_VERSION, RunApprovalState, RunFailure, RunSnapshot,
    RunStateError, RunStatus, RunStore, RunStoreError, RunStoreFuture, RunToolFailure,
    RunToolSetState, RunToolState, RunToolStatus, project_run_output,
};
pub use session::{
    ArchiveSession, BeginRunResult, BeginSessionRun, ContentPart, ConversationRole, CreateSession,
    FinalizeSessionRun, MAX_SESSION_PAGE_SIZE, MessageId, SessionId, SessionMessage,
    SessionSnapshot, SessionStatus, SessionStore, SessionStoreError, SessionStoreFuture,
    SessionToolCallState, project_run_content,
};
pub use title::{
    TITLE_REDUCER_PROTOCOL_VERSION, TitleGenerationDispatch, TitleGenerationInput,
    TitleGenerationOutcome, TitleGenerationReducer, TitleGenerationStart,
    TitleGenerationStartDispatch, TitleGenerationState, TitleGenerationTransition,
};
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

    /// Builds a deterministic run id for replayable auxiliary model effects.
    #[must_use]
    pub fn stable(namespace: &str, key: &str) -> Self {
        let name = format!("RunId:{namespace}:{key}");
        Self(Uuid::new_v5(&Uuid::NAMESPACE_URL, name.as_bytes()))
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
    pub attachments: Vec<ModelAttachment>,
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

/// Stable response returned by the harness to a host application.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunResponse {
    pub run_id: RunId,
    pub output: String,
    pub finish_reason: FinishReason,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<TokenUsage>,
}

/// The narrow contract implemented by an agent.
///
/// It deliberately has no HTTP, CLI, desktop, or provider-specific types.
pub trait Agent: Send + Sync + 'static {
    fn metadata(&self) -> AgentMetadata;

    fn run(&self, request: RunRequest) -> AgentEventStream;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::{Harness, HarnessError, MAX_RUN_STEPS, RunOptions};

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

pub mod events;
/// Durable execution, external event and context admission protocols.
pub mod fabric;
pub mod preparation;
