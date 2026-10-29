//! Stable, storage-independent host records and terminal run facts.
use super::{RunId, fabric::*};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostRecord {
    pub tenant: String,
    pub namespace: String,
    #[serde(default)]
    pub run_id: Option<RunId>,
    pub operation_id: String,
    pub schema_version: u32,
    pub payload: Value,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunCompletion {
    pub schema_version: u32,
    pub tenant: String,
    pub run_id: RunId,
    pub completion_id: String,
    pub status: String,
    pub output: Option<Value>,
    pub error: Option<String>,
    pub usage: Option<Value>,
    pub completed_at_ms: i64,
    pub definition_digest: String,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompletionPage {
    pub through: u64,
    pub items: Vec<(u64, RunCompletion)>,
}
/// Stable internal logical identity; the SQL layout is never part of this contract.
pub fn host_record_key(tenant: &str, namespace: &str, operation: &str) -> String {
    format!(
        "host-record:{}",
        super::events::EventId::stable(
            "host-record",
            &serde_json::json!([tenant, namespace, operation]).to_string()
        )
    )
}
pub fn completion_log(tenant: &str) -> String {
    format!(
        "run-completions:{}",
        super::events::EventId::stable("run-completions", tenant)
    )
}
impl HostRecord {
    pub fn validate(&self) -> Result<(), FabricError> {
        self.validate_with_limits(FabricLimits::default())
    }
    pub fn validate_with_limits(&self, limits: FabricLimits) -> Result<(), FabricError> {
        limits.validate()?;
        if self.schema_version == 0
            || [&self.tenant, &self.namespace, &self.operation_id]
                .iter()
                .any(|s| s.is_empty() || s.len() > MAX_KEY_BYTES)
            || serde_json::to_vec(&self.payload)
                .map_err(|e| FabricError::Invalid(e.to_string()))?
                .len()
                > limits.max_event_bytes
        {
            return Err(FabricError::Invalid("host record bounds".into()));
        }
        Ok(())
    }
}
