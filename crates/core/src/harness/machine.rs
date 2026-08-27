use std::{future::Future, pin::Pin};

use futures_core::Stream;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

use crate::{
    event_runtime::{
        CreateSubscription, EventEnvelope, EventId, PublishEvent, ScheduleOnce, SubscriptionId,
    },
    harness::{AgentEvent, AgentMetadata, FinishReason, ModelMessage, RunCancellation, RunId},
};

macro_rules! machine_uuid_id {
    ($name:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(Uuid);

        impl $name {
            #[must_use]
            pub fn new() -> Self {
                Self(Uuid::new_v4())
            }

            #[must_use]
            pub fn stable(namespace: &str, key: &str) -> Self {
                let name = format!("{}:{namespace}:{key}", stringify!($name));
                Self(Uuid::new_v5(&Uuid::NAMESPACE_URL, name.as_bytes()))
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(formatter)
            }
        }

        impl std::str::FromStr for $name {
            type Err = uuid::Error;

            fn from_str(source: &str) -> Result<Self, Self::Err> {
                Uuid::parse_str(source).map(Self)
            }
        }
    };
}

machine_uuid_id!(ActivationId);
machine_uuid_id!(JobId);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointCodec {
    Json,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CheckpointEnvelope {
    pub agent_kind: String,
    pub schema_version: u32,
    pub codec: CheckpointCodec,
    pub payload: Value,
}

impl CheckpointEnvelope {
    pub fn validate(&self) -> Result<(), FlowError> {
        if self.agent_kind.trim().is_empty() || self.agent_kind.len() > 128 {
            return Err(FlowError::Invalid(
                "checkpoint agent_kind must contain 1 to 128 bytes".into(),
            ));
        }
        if self.schema_version == 0 {
            return Err(FlowError::Invalid(
                "checkpoint schema_version must be greater than zero".into(),
            ));
        }
        let bytes = serde_json::to_vec(&self.payload)
            .map_err(|_| FlowError::Invalid("checkpoint payload could not be encoded".into()))?;
        if bytes.len() > 4 * 1_048_576 {
            return Err(FlowError::Invalid(
                "checkpoint exceeds the 4 MiB portable limit".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct MachineStartRequest {
    pub run_id: RunId,
    pub input: String,
    pub prior_messages: Vec<ModelMessage>,
    pub allowed_tools: Option<Vec<String>>,
    pub allow_run_adf: bool,
    pub max_steps: Option<u32>,
    pub context_fingerprint: Option<String>,
    pub activated_at_ms: i64,
    pub cancellation: RunCancellation,
}

#[derive(Debug, Clone)]
pub struct MachineResumeRequest {
    pub run_id: RunId,
    pub activation_id: ActivationId,
    pub checkpoint: CheckpointEnvelope,
    pub inbox: Vec<FlowInboxItem>,
    pub activated_at_ms: i64,
    pub cancellation: RunCancellation,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WaitSpec {
    pub wait_key: String,
    pub subscription: CreateSubscription,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StartJob {
    pub job_id: JobId,
    pub run_id: RunId,
    pub kind: String,
    pub input: Value,
    pub idempotency_key: String,
    pub requested_at_ms: i64,
}

impl StartJob {
    pub fn validate(&self) -> Result<(), FlowError> {
        if self.kind.trim().is_empty() || self.kind.len() > 128 {
            return Err(FlowError::Invalid(
                "job kind must contain 1 to 128 bytes".into(),
            ));
        }
        if self.idempotency_key.trim().is_empty() || self.idempotency_key.len() > 256 {
            return Err(FlowError::Invalid(
                "job idempotency_key must contain 1 to 256 bytes".into(),
            ));
        }
        let bytes = serde_json::to_vec(&self.input)
            .map_err(|_| FlowError::Invalid("job input could not be encoded".into()))?;
        if bytes.len() > 4 * 1_048_576 {
            return Err(FlowError::Invalid(
                "job input exceeds the 4 MiB portable limit".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EffectRequest {
    PublishEvent { command: PublishEvent },
    ScheduleTimer { command: ScheduleOnce },
    StartJob { command: StartJob },
}

impl EffectRequest {
    pub fn validate(&self) -> Result<(), FlowError> {
        match self {
            Self::PublishEvent { command } => {
                crate::event_runtime::validate_publish(command).map_err(flow_from_event)
            }
            Self::ScheduleTimer { command } => command.validate().map_err(flow_from_event),
            Self::StartJob { command } => command.validate(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StepOutcome {
    Continue {
        checkpoint: CheckpointEnvelope,
        effects: Vec<EffectRequest>,
    },
    Suspend {
        checkpoint: CheckpointEnvelope,
        waits: Vec<WaitSpec>,
        effects: Vec<EffectRequest>,
    },
    Complete {
        finish_reason: FinishReason,
    },
    Failed {
        error: MachineError,
    },
    Cancelled,
}

#[derive(Debug)]
#[non_exhaustive]
pub enum MachineOutput {
    Event(AgentEvent),
    Yield(StepOutcome),
}

pub type MachineStream = Pin<Box<dyn Stream<Item = MachineOutput> + Send + 'static>>;

pub trait AgentMachine: Send + Sync + 'static {
    fn metadata(&self) -> AgentMetadata;
    fn initial_checkpoint(
        &self,
        request: &MachineStartRequest,
    ) -> Result<CheckpointEnvelope, MachineError>;
    fn start(&self, request: MachineStartRequest) -> MachineStream;
    fn resume(&self, request: MachineResumeRequest) -> MachineStream;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MachineError {
    pub code: String,
    pub message: String,
    pub retryable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FlowRunStatus {
    Runnable,
    Running,
    WaitingEvent,
    Completed,
    Failed,
    Cancelled,
}

impl FlowRunStatus {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FlowRunState {
    pub run_id: RunId,
    pub revision: u64,
    pub status: FlowRunStatus,
    pub activation_id: Option<ActivationId>,
    pub checkpoint: CheckpointEnvelope,
    pub wait_subscription_ids: Vec<SubscriptionId>,
    pub lease_owner: Option<String>,
    pub lease_until_ms: Option<i64>,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FlowInboxItem {
    pub event: EventEnvelope,
    pub consumed_revision: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SuspendFlowRun {
    pub run_id: RunId,
    pub activation_id: ActivationId,
    pub expected_revision: u64,
    pub checkpoint: CheckpointEnvelope,
    pub waits: Vec<WaitSpec>,
    pub effects: Vec<EffectRequest>,
    pub suspended_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ContinueFlowRun {
    pub run_id: RunId,
    pub activation_id: ActivationId,
    pub expected_revision: u64,
    pub checkpoint: CheckpointEnvelope,
    pub effects: Vec<EffectRequest>,
    pub continued_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WakeFlowRun {
    pub run_id: RunId,
    pub subscription_id: SubscriptionId,
    pub event_id: EventId,
    pub woken_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClaimedActivation {
    pub state: FlowRunState,
    pub inbox: Vec<FlowInboxItem>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompleteFlowRun {
    pub run_id: RunId,
    pub activation_id: ActivationId,
    pub expected_revision: u64,
    pub status: FlowRunStatus,
    pub completed_at_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FlowEffectId {
    pub run_id: RunId,
    pub revision: u64,
    pub effect_index: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FlowEffectStatus {
    Pending,
    Executing,
    RetryPending,
    Completed,
    DeadLettered,
    Cancelled,
}

impl FlowEffectStatus {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::DeadLettered | Self::Cancelled)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FlowEffect {
    pub effect_id: FlowEffectId,
    pub request: EffectRequest,
    pub status: FlowEffectStatus,
    pub attempts: u32,
    pub next_attempt_at_ms: i64,
    pub lease_owner: Option<String>,
    pub lease_until_ms: Option<i64>,
    pub last_error: Option<String>,
    pub completed_at_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompleteFlowEffect {
    pub effect_id: FlowEffectId,
    pub worker_id: String,
    pub completed_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryFlowEffect {
    pub effect_id: FlowEffectId,
    pub worker_id: String,
    pub next_attempt_at_ms: i64,
    pub error: String,
    pub dead_letter: bool,
}

pub type FlowFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, FlowError>> + Send + 'a>>;

/// Transactional persistence boundary for checkpoints, waits, effects, inbox,
/// and activation leases. Concrete stores must implement suspend and wake as
/// single transactions with their event subscription/outbox tables.
pub trait FlowStore: Send + Sync + 'static {
    fn create(&self, state: FlowRunState) -> FlowFuture<'_, FlowRunState>;
    fn get(&self, run_id: RunId) -> FlowFuture<'_, Option<FlowRunState>>;
    fn continue_run(&self, command: ContinueFlowRun) -> FlowFuture<'_, FlowRunState>;
    fn suspend(&self, command: SuspendFlowRun) -> FlowFuture<'_, FlowRunState>;
    fn wake(&self, command: WakeFlowRun) -> FlowFuture<'_, FlowRunState>;
    fn claim_runnable(
        &self,
        worker_id: String,
        now_ms: i64,
        lease_until_ms: i64,
        limit: usize,
    ) -> FlowFuture<'_, Vec<ClaimedActivation>>;
    fn claim_effects(
        &self,
        worker_id: String,
        now_ms: i64,
        lease_until_ms: i64,
        limit: usize,
    ) -> FlowFuture<'_, Vec<FlowEffect>>;
    fn complete_effect(&self, command: CompleteFlowEffect) -> FlowFuture<'_, FlowEffect>;
    fn retry_effect(&self, command: RetryFlowEffect) -> FlowFuture<'_, FlowEffect>;
    fn complete(&self, command: CompleteFlowRun) -> FlowFuture<'_, FlowRunState>;
    fn cancel(&self, run_id: RunId, cancelled_at_ms: i64) -> FlowFuture<'_, FlowRunState>;
}

pub trait FlowEffectRouter: Send + Sync + 'static {
    fn execute(&self, effect: FlowEffect) -> FlowFuture<'_, ()>;
}

#[derive(Debug, Error)]
pub enum FlowError {
    #[error("flow contract validation failed: {0}")]
    Invalid(String),
    #[error("flow run was not found")]
    NotFound,
    #[error("flow state conflict: {0}")]
    Conflict(String),
    #[error("flow store failed: {0}")]
    Backend(String),
}

fn flow_from_event(error: crate::event_runtime::EventError) -> FlowError {
    match error {
        crate::event_runtime::EventError::Invalid(message) => FlowError::Invalid(message),
        crate::event_runtime::EventError::NotFound => FlowError::NotFound,
        crate::event_runtime::EventError::Conflict(message) => FlowError::Conflict(message),
        crate::event_runtime::EventError::Backend(message) => FlowError::Backend(message),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn checkpoint_requires_a_version_and_has_a_size_limit() {
        let valid = CheckpointEnvelope {
            agent_kind: "agent-loop".into(),
            schema_version: 1,
            codec: CheckpointCodec::Json,
            payload: json!({"step": 2}),
        };
        valid.validate().expect("checkpoint should be valid");
        let invalid = CheckpointEnvelope {
            schema_version: 0,
            ..valid
        };
        assert!(matches!(invalid.validate(), Err(FlowError::Invalid(_))));
    }
}
