//! Versioned logical Run snapshots. These formats do not contain SQLite table names.
use super::{RunId, fabric::FabricError};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

pub const MAX_BUNDLE_BYTES: usize = 128 * 1024 * 1024;
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunBundle {
    pub format_version: u32,
    pub purpose: String,
    pub tenant: String,
    pub run_id: RunId,
    pub digest: String,
    /// Versioned logical adapter records, opaque to hosts. Not a SQL dump.
    pub(crate) data: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dependencies: Option<Value>,
}
impl RunBundle {
    pub(crate) fn new(tenant: String, run_id: RunId, data: Value) -> Result<Self, FabricError> {
        let mut result = Self {
            format_version: 1,
            purpose: "recovery".into(),
            tenant,
            run_id,
            digest: String::new(),
            data,
            dependencies: None,
        };
        result.digest = result.content_digest()?;
        Ok(result)
    }
    fn content_digest(&self) -> Result<String, FabricError> {
        let bytes = serde_json::to_vec(&(
            self.format_version,
            &self.purpose,
            &self.tenant,
            self.run_id,
            &self.data,
            &self.dependencies,
        ))
        .map_err(|e| FabricError::Invalid(e.to_string()))?;
        if bytes.len() > MAX_BUNDLE_BYTES {
            return Err(FabricError::Invalid("bundle byte limit".into()));
        }
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }
    pub fn with_dependencies(mut self, dependencies: Value) -> Result<Self, FabricError> {
        self.dependencies = Some(dependencies);
        self.digest = self.content_digest()?;
        Ok(self)
    }
    pub fn verify(&self) -> Result<(), FabricError> {
        if self.format_version != 1
            || self.purpose != "recovery"
            || self.content_digest()? != self.digest
        {
            return Err(FabricError::Invalid(
                "bundle format, purpose or checksum".into(),
            ));
        }
        Ok(())
    }
    pub fn graph_run(&self) -> Result<crate::agent::GraphRun, FabricError> {
        self.verify()?;
        let key = crate::agent::graph::run_checkpoint_key(&self.tenant, self.run_id);
        serde_json::from_value(self.data["checkpoints"][key]["value"].clone())
            .map_err(|e| FabricError::Invalid(e.to_string()))
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunInspectionItem {
    pub logical_id: String,
    pub kind: String,
    pub schema_version: u32,
    pub revision: Option<u64>,
    pub payload: Value,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunInspectionSnapshot {
    pub api_version: u32,
    pub tenant: String,
    pub run_id: RunId,
    pub snapshot_id: String,
    pub status: Value,
    pub diagnostic_only: bool,
    pub items: Vec<RunInspectionItem>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunInspectionPage {
    pub api_version: u32,
    pub snapshot_id: String,
    pub status: Value,
    pub items: Vec<RunInspectionItem>,
    pub next_cursor: Option<String>,
}

#[doc(hidden)]
pub fn import_marker(tenant: &str, run: RunId) -> String {
    format!(
        "run-import:{}",
        super::events::EventId::stable("run-import", &serde_json::json!([tenant, run]).to_string())
    )
}
