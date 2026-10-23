//! Versioned target contracts only; no scheduler, transport or execution wiring.
//!
//! Sequence domains require explicit boundary conversion, never implicit reuse.
//! ```compile_fail,E0308
//! use agent_core::operator::{OperatorSequence, DeltaSequence};
//! let sequence: OperatorSequence = DeltaSequence(1);
//! ```
//! ```compile_fail,E0308
//! use agent_core::operator::{OperatorSequence, PublicRunSequence};
//! let sequence: PublicRunSequence = OperatorSequence(1);
//! ```
//! ```compile_fail,E0308
//! use agent_core::operator::{DeltaSequence, PublicRunSequence};
//! let sequence: DeltaSequence = PublicRunSequence(1);
//! ```
//! ```compile_fail,E0308
//! use agent_core::operator::{LogSequence, RunRevision};
//! let sequence: RunRevision = LogSequence(1);
//! ```

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    event_runtime::EventId,
    harness::{RunId, SessionId},
};

macro_rules! counter {
    ($($name:ident),+ $(,)?) => {$ (
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub u64);
    )+};
}

counter!(
    OperatorSequence,
    DeltaSequence,
    PublicRunSequence,
    LogSequence,
    RunRevision,
    LeaseEpoch
);

pub const ENVELOPE_SCHEMA_VERSION: u32 = 1;
pub const PROJECTION_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EngineVersion {
    #[default]
    LegacyMachine,
    OperatorV1,
}

impl EngineVersion {
    pub fn is_legacy(&self) -> bool {
        *self == Self::LegacyMachine
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryClass {
    Durable,
    Transient,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperatorIdentity {
    pub name: String,
    pub version: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OperatorEnvelope {
    pub schema_version: u32,
    pub engine_version: EngineVersion,
    pub projection_version: u32,
    pub event_id: EventId,
    pub event_type: String,
    pub tenant_id: String,
    pub run_id: RunId,
    pub session_id: Option<SessionId>,
    pub effect_id: String,
    pub invocation_id: String,
    pub attempt: u32,
    pub issued_revision: RunRevision,
    pub lease_epoch: LeaseEpoch,
    pub correlation_id: String,
    pub causation_id: Option<EventId>,
    pub operator: OperatorIdentity,
    pub operator_seq: OperatorSequence,
    pub delivery_class: DeliveryClass,
    pub occurred_at_ms: i64,
    pub payload: Value,
}

pub struct PayloadContract {
    pub event_type: String,
    pub delivery_class: DeliveryClass,
    pub schema: Value,
    pub max_payload_bytes: usize,
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum ContractError {
    #[error("unsupported contract version")]
    UnsupportedVersion,
    #[error("invalid identity or sequence")]
    InvalidIdentity,
    #[error("event type or delivery class does not match payload contract")]
    ContractMismatch,
    #[error("payload exceeds byte limit")]
    PayloadTooLarge,
    #[error("invalid payload schema")]
    InvalidSchema,
    #[error("payload does not satisfy schema")]
    InvalidPayload,
}

impl OperatorEnvelope {
    pub fn validate(&self, contract: &PayloadContract) -> Result<(), ContractError> {
        if self.schema_version != ENVELOPE_SCHEMA_VERSION
            || self.projection_version != PROJECTION_VERSION
        {
            return Err(ContractError::UnsupportedVersion);
        }
        if [
            &self.tenant_id,
            &self.effect_id,
            &self.invocation_id,
            &self.correlation_id,
            &self.operator.name,
        ]
        .iter()
        .any(|value| value.trim().is_empty())
            || self.operator.version == 0
            || self.attempt == 0
            || self.lease_epoch.0 == 0
            || self.operator_seq.0 == 0
        {
            return Err(ContractError::InvalidIdentity);
        }
        if self.event_type != contract.event_type || self.delivery_class != contract.delivery_class
        {
            return Err(ContractError::ContractMismatch);
        }
        let bytes = serde_json::to_vec(&self.payload).map_err(|_| ContractError::InvalidPayload)?;
        if bytes.len() > contract.max_payload_bytes {
            return Err(ContractError::PayloadTooLarge);
        }
        let schema = jsonschema::JSONSchema::compile(&contract.schema)
            .map_err(|_| ContractError::InvalidSchema)?;
        if !schema.is_valid(&self.payload) {
            return Err(ContractError::InvalidPayload);
        }
        Ok(())
    }
}
