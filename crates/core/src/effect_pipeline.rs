use crate::{
    event_runtime::EventId,
    harness::*,
    operator::*,
    operator_fabric::FabricEvent,
    tool_authorization::{BoundApproval, ValidatedToolCall},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;

pub const TOOLS_PROFILE: &str = "operator_v1_tools";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum RunControlPhase {
    #[default]
    Running,
    Paused,
    Terminal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunControlAction {
    Pause,
    Resume,
    Cancel,
    Complete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RunControl {
    pub revision: u64,
    pub phase: RunControlPhase,
}

impl RunControl {
    pub fn apply(&mut self, expected_revision: u64, action: RunControlAction) -> bool {
        if self.revision != expected_revision || self.revision == u64::MAX {
            return false;
        }
        self.phase = match (self.phase, action) {
            (RunControlPhase::Running, RunControlAction::Pause) => RunControlPhase::Paused,
            (RunControlPhase::Paused, RunControlAction::Resume) => RunControlPhase::Running,
            (
                RunControlPhase::Running | RunControlPhase::Paused,
                RunControlAction::Cancel | RunControlAction::Complete,
            ) => RunControlPhase::Terminal,
            _ => return false,
        };
        self.revision += 1;
        true
    }
}

pub fn control_route(tenant: &str, run_id: RunId) -> EffectRoute {
    EffectRoute {
        topic: format!(
            "agent.control.{}",
            EventId::stable("run-control", &run_key(tenant, run_id))
        ),
        event_type: "run.cancel.requested".into(),
        consumer: "coordinator".into(),
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunCancelRequest {
    pub tenant_id: String,
    pub run_id: RunId,
    pub actor: String,
    pub requested_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EffectRoute {
    pub topic: String,
    pub event_type: String,
    pub consumer: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EffectRun {
    pub tenant_id: String,
    pub profile: String,
    pub start: AgentLoopStart,
    pub reducer: AgentLoopReducerState,
    pub public_events: Vec<RunEvent>,
    pub routes: BTreeMap<String, EffectRoute>,
    pub toolset_effect: Option<String>,
    pub validated: BTreeMap<String, ValidatedToolCall>,
    pub approvals: BTreeMap<String, BoundApproval>,
    #[serde(default)]
    pub wait: Option<EffectWaitRequest>,
    #[serde(default)]
    pub output_through: BTreeMap<String, u64>,
    #[serde(default)]
    pub admission_failure: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EffectWaitRequest {
    pub tenant_id: String,
    pub run_id: RunId,
    pub effect_id: String,
    pub call_id: String,
    pub waits: Vec<WaitSpec>,
    pub effects: Vec<EffectRequest>,
    pub route: EffectRoute,
}

impl EffectWaitRequest {
    pub fn key(&self) -> String {
        format!(
            "{}:wait:{}",
            run_key(&self.tenant_id, self.run_id),
            self.effect_id
        )
    }
}

pub fn wait_binding_key(subscription: crate::event_runtime::SubscriptionId) -> String {
    format!("effect-wait-binding:{subscription}")
}

pub fn run_key(tenant: &str, run_id: RunId) -> String {
    format!(
        "effect-run:{}",
        EventId::stable(
            "effect-run",
            &serde_json::to_string(&(tenant, run_id)).expect("identity")
        )
    )
}

pub fn model_key(tenant: &str, run_id: RunId, effect: &str) -> String {
    format!(
        "effect-model:{}",
        EventId::stable(
            "effect-model",
            &serde_json::to_string(&(tenant, run_id, effect)).expect("identity")
        )
    )
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EffectModelRequest {
    pub tenant_id: String,
    pub run_id: RunId,
    pub effect_id: String,
    pub request: ModelRequest,
    pub timeout_ms: u64,
    pub request_ref: BlobId,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EffectModelExecution {
    pub lease: crate::operator_fabric::DeliveryLease,
    pub events: Option<Vec<ModelEvent>>,
    #[serde(default)]
    pub final_ref: Option<crate::pipeline::FinalRef>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct EffectOutputState {
    pub batches: Vec<crate::pipeline::OutputBatch>,
    pub durable_through: u64,
    pub coverage_digest: String,
    pub sealed: bool,
    pub journal_ref: Option<BlobId>,
}

pub fn output_key(tenant: &str, run_id: RunId, effect: &str) -> String {
    format!("{}:output", model_key(tenant, run_id, effect))
}

pub fn reference_schema() -> Value {
    json!({"type":"object","required":["checkpoint_key"],"additionalProperties":false,"properties":{"checkpoint_key":{"type":"string","minLength":1}}})
}

pub fn fact(
    tenant: &str,
    run_id: RunId,
    effect: &str,
    route: &EffectRoute,
    payload: Value,
    schema: Value,
    now_ms: i64,
) -> FabricEvent {
    FabricEvent {
        topic: route.topic.clone(),
        payload_schema: schema,
        envelope: OperatorEnvelope {
            schema_version: 1,
            engine_version: EngineVersion::OperatorV1,
            projection_version: 1,
            event_id: EventId::stable(
                "effect-pipeline",
                &serde_json::to_string(&(tenant, run_id, effect, &route.event_type))
                    .expect("identity"),
            ),
            event_type: route.event_type.clone(),
            tenant_id: tenant.into(),
            run_id,
            session_id: None,
            effect_id: effect.into(),
            invocation_id: effect.into(),
            attempt: 1,
            issued_revision: RunRevision(1),
            lease_epoch: LeaseEpoch(1),
            correlation_id: run_id.to_string(),
            causation_id: None,
            operator: OperatorIdentity {
                name: "coordinator".into(),
                version: 1,
            },
            operator_seq: OperatorSequence(0),
            delivery_class: DeliveryClass::Durable,
            occurred_at_ms: now_ms,
            payload,
        },
    }
}
