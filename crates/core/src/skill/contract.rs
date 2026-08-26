use std::{collections::BTreeMap, future::Future, pin::Pin};

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SkillId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComponentDescriptor {
    pub identity: String,
    pub kind: String,
    pub version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillCompatibility {
    #[serde(default = "default_contract_version")]
    pub contract_version: u32,
    #[serde(default)]
    pub minimum_harness_version: Option<String>,
}

impl Default for SkillCompatibility {
    fn default() -> Self {
        Self {
            contract_version: default_contract_version(),
            minimum_harness_version: None,
        }
    }
}

const fn default_contract_version() -> u32 {
    1
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SkillActivation {
    #[default]
    ExplicitOnly,
    ProfileDefault,
    Routable {
        hints: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillResourceRef {
    pub path: String,
    #[serde(default)]
    pub media_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillManifest {
    pub skill_id: SkillId,
    pub version: String,
    pub description: String,
    #[serde(default)]
    pub compatibility: SkillCompatibility,
    #[serde(default)]
    pub activation: SkillActivation,
    #[serde(default)]
    pub required_tools: Vec<String>,
    #[serde(default)]
    pub optional_tools: Vec<String>,
    #[serde(default)]
    pub required_capabilities: Vec<String>,
    #[serde(default)]
    pub resources: Vec<SkillResourceRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillPackage {
    pub manifest: SkillManifest,
    pub instructions: String,
    pub resources: BTreeMap<String, Vec<u8>>,
    pub digest: String,
    pub store_identity: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillDescriptor {
    pub skill_id: SkillId,
    pub version: String,
    pub description: String,
    pub digest: String,
    pub store_identity: String,
    pub activation: SkillActivation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SkillLocator {
    IdVersion {
        skill_id: SkillId,
        version: String,
    },
    Locked {
        skill_id: SkillId,
        version: String,
        digest: String,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SkillStoreQuery {
    pub skill_ids: Vec<SkillId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutSkillPackage {
    pub package: SkillPackage,
    pub expected_digest: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteSkillPackage {
    pub locator: SkillLocator,
    pub expected_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillPackageRef {
    pub skill_id: SkillId,
    pub version: String,
    pub digest: String,
    pub store_identity: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SkillStoreCapabilities {
    pub readable: bool,
    pub writable: bool,
    pub deletable: bool,
}

pub type SkillFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, SkillStoreError>> + Send + 'a>>;

pub trait SkillStore: Send + Sync + 'static {
    fn descriptor(&self) -> ComponentDescriptor;
    fn capabilities(&self) -> SkillStoreCapabilities;
    fn list(&self, query: SkillStoreQuery) -> SkillFuture<'_, Vec<SkillDescriptor>>;
    fn get(&self, locator: SkillLocator) -> SkillFuture<'_, Option<SkillPackage>>;
    fn put(&self, command: PutSkillPackage) -> SkillFuture<'_, SkillPackageRef>;
    fn delete(&self, command: DeleteSkillPackage) -> SkillFuture<'_, ()>;
}

#[derive(Debug, Error)]
pub enum SkillStoreError {
    #[error("skill store is read only")]
    ReadOnly,
    #[error("skill package conflicts with existing digest")]
    DigestConflict,
    #[error("skill package is invalid: {0}")]
    InvalidPackage(String),
    #[error("skill package path is unsafe")]
    UnsafePath,
    #[error("skill store failed: {0}")]
    Backend(String),
}

impl SkillStoreError {
    #[must_use]
    pub fn backend(message: impl Into<String>) -> Self {
        Self::Backend(message.into())
    }
}
