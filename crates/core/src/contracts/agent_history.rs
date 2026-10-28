//! Historical archives are not executable recovery bundles.
use super::{RunId, blob::BlobMetadata, fabric::FabricError};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryBlob {
    pub metadata: BlobMetadata,
    pub base64: String,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunHistory {
    pub run_id: RunId,
    /// The final graph, bindings, input, output, error and operator state.
    pub final_state: Value,
    pub events_through: u64,
    pub events: Vec<Value>,
    /// Durable routed facts preserve timestamps, causation and payload schemas.
    pub routed_events: Vec<super::fabric::FabricEvent>,
    pub completion: Option<super::host::RunCompletion>,
    /// Public output facts persisted at the export boundary (a derived projection).
    pub public_events_through: u64,
    pub public_events: Vec<Value>,
    pub blobs: Vec<HistoryBlob>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHistory {
    pub format_version: u32,
    pub purpose: String,
    pub tenant: String,
    pub agent_id: String,
    /// All registered terminal Runs visible in one consistent store snapshot.
    pub runs: Vec<RunHistory>,
    pub digest: String,
}
impl AgentHistory {
    pub(crate) fn new(
        tenant: &str,
        agent_id: &str,
        runs: Vec<RunHistory>,
    ) -> Result<Self, FabricError> {
        let mut archive = Self {
            format_version: 1,
            purpose: "agent_history".into(),
            tenant: tenant.into(),
            agent_id: agent_id.into(),
            runs,
            digest: String::new(),
        };
        archive.digest = archive.content_digest()?;
        Ok(archive)
    }
    fn content_digest(&self) -> Result<String, FabricError> {
        let value = (
            &self.format_version,
            &self.purpose,
            &self.tenant,
            &self.agent_id,
            &self.runs,
        );
        if serde_json::to_vec(&value)
            .map_err(|e| FabricError::Invalid(e.to_string()))?
            .len()
            > super::run_archive::MAX_BUNDLE_BYTES
        {
            return Err(FabricError::Capacity);
        }
        crate::agent::bindings::digest(&value)
    }
    pub fn verify(&self) -> Result<(), FabricError> {
        if self.format_version != 1
            || self.purpose != "agent_history"
            || self.digest != self.content_digest()?
        {
            return Err(FabricError::Invalid(
                "history format, purpose or checksum".into(),
            ));
        }
        Ok(())
    }
}

pub(crate) fn membership(tenant: &str, agent_id: &str, run_id: RunId) -> super::host::HostRecord {
    super::host::HostRecord {
        tenant: tenant.into(),
        namespace: "$agent-run".into(),
        run_id: Some(run_id),
        operation_id: run_id.to_string(),
        schema_version: 1,
        payload: serde_json::json!({"agent_id":agent_id}),
    }
}
