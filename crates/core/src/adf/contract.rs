use std::{fmt, future::Future, pin::Pin, str::FromStr};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

use crate::{
    harness::{RunCancellation, RunId, SessionId},
    script::{ScriptCapability, ScriptRuntimeDescriptor},
    skill::ComponentDescriptor,
    tool::{ToolConcurrency, ToolDefinition, ToolExecutionPolicy, ToolRiskLevel},
};

pub const ADF_CONTRACT_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AdfId(Uuid);

impl AdfId {
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for AdfId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for AdfId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for AdfId {
    type Err = uuid::Error;

    fn from_str(source: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(source).map(Self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AdfRuntimeRequest {
    #[serde(rename = "javascript")]
    JavaScript,
    Python {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        interpreter_profile: Option<String>,
    },
    Shell {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        shell_profile: Option<String>,
    },
}

impl AdfRuntimeRequest {
    #[must_use]
    pub const fn kind(&self) -> AdfRuntimeKind {
        match self {
            Self::JavaScript => AdfRuntimeKind::JavaScript,
            Self::Python { .. } => AdfRuntimeKind::Python,
            Self::Shell { .. } => AdfRuntimeKind::Shell,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdfRuntimeKind {
    #[serde(rename = "javascript")]
    JavaScript,
    Python,
    Shell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdfScope {
    Run,
    Session,
    Workspace,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdfExecutionMode {
    Sync,
    JobCapable,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AdfOwner {
    Run { run_id: RunId },
    Session { session_id: SessionId },
    Workspace { workspace_id: String },
}

impl AdfOwner {
    #[must_use]
    pub const fn scope(&self) -> AdfScope {
        match self {
            Self::Run { .. } => AdfScope::Run,
            Self::Session { .. } => AdfScope::Session,
            Self::Workspace { .. } => AdfScope::Workspace,
        }
    }
}

pub type AdfCapability = ScriptCapability;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdfDefinitionRequest {
    pub requested_name: String,
    pub description: String,
    pub input_schema: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<Value>,
    pub runtime: AdfRuntimeRequest,
    pub source: String,
    #[serde(default = "default_entrypoint")]
    pub entrypoint: String,
    #[serde(default)]
    pub requested_capabilities: Vec<AdfCapability>,
    pub requested_scope: AdfScope,
    pub execution_mode: AdfExecutionMode,
    pub idempotency_key: String,
}

fn default_entrypoint() -> String {
    "main".into()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdfRuntimeDescriptor {
    pub identity: String,
    pub kind: AdfRuntimeKind,
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub script_runtime: Option<ScriptRuntimeDescriptor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdfArtifactRef {
    pub adf_id: AdfId,
    pub revision: u64,
    pub digest: String,
    pub store_identity: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LockedAdfDefinition {
    pub contract_version: u32,
    pub adf_id: AdfId,
    pub revision: u64,
    pub canonical_name: String,
    pub description: String,
    pub input_schema: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<Value>,
    pub entrypoint: String,
    pub runtime: AdfRuntimeDescriptor,
    pub source_ref: AdfArtifactRef,
    pub source_digest: String,
    pub manifest_digest: String,
    pub effective_risk: ToolRiskLevel,
    #[serde(default)]
    pub granted_capabilities: Vec<AdfCapability>,
    pub owner: AdfOwner,
    pub execution_mode: AdfExecutionMode,
    pub created_by_run_id: RunId,
    pub created_at_ms: i64,
}

impl LockedAdfDefinition {
    #[must_use]
    pub fn tool_definition(&self) -> ToolDefinition {
        ToolDefinition::new(
            self.canonical_name.clone(),
            self.description.clone(),
            self.input_schema.clone(),
        )
        .with_risk_level(self.effective_risk)
        .with_execution_policy(
            ToolExecutionPolicy::read_only().with_concurrency(ToolConcurrency::ParallelSafe),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdfArtifact {
    pub definition: LockedAdfDefinition,
    pub source: String,
    pub active: bool,
}

#[derive(Debug, Clone)]
pub struct PutAdfArtifact {
    pub artifact: AdfArtifact,
    pub idempotency_key: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AdfArtifactQuery {
    pub owner: Option<AdfOwner>,
    pub active_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdfArtifactDescriptor {
    pub reference: AdfArtifactRef,
    pub canonical_name: String,
    pub runtime: AdfRuntimeKind,
    pub owner: AdfOwner,
    pub effective_risk: ToolRiskLevel,
    pub active: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdfStoreCapabilities {
    pub readable: bool,
    pub writable: bool,
    pub deactivatable: bool,
}

pub type AdfFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, AdfStoreError>> + Send + 'a>>;

pub trait AdfArtifactStore: Send + Sync + 'static {
    fn descriptor(&self) -> ComponentDescriptor;
    fn capabilities(&self) -> AdfStoreCapabilities;
    fn put(&self, command: PutAdfArtifact) -> AdfFuture<'_, AdfArtifactRef>;
    fn get(&self, reference: AdfArtifactRef) -> AdfFuture<'_, Option<AdfArtifact>>;
    fn list(&self, query: AdfArtifactQuery) -> AdfFuture<'_, Vec<AdfArtifactDescriptor>>;
    fn deactivate(&self, reference: AdfArtifactRef) -> AdfFuture<'_, ()>;
}

#[derive(Debug, Error)]
pub enum AdfStoreError {
    #[error("ADF artifact store is read only")]
    ReadOnly,
    #[error("ADF artifact conflicts with an existing idempotency key")]
    IdempotencyConflict,
    #[error("ADF artifact conflicts with an existing revision or digest")]
    ArtifactConflict,
    #[error("ADF artifact is invalid: {0}")]
    InvalidArtifact(String),
    #[error("ADF artifact was not found")]
    NotFound,
    #[error("ADF artifact store failed: {0}")]
    Backend(String),
}

impl AdfStoreError {
    #[must_use]
    pub fn backend(message: impl Into<String>) -> Self {
        Self::Backend(message.into())
    }
}

#[derive(Debug, Clone)]
pub struct AdfPolicyRequest {
    pub run_id: RunId,
    pub definition: AdfDefinitionRequest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdfPolicyDecision {
    pub runtime: AdfRuntimeDescriptor,
    pub effective_risk: ToolRiskLevel,
    pub granted_capabilities: Vec<AdfCapability>,
    pub scope: AdfScope,
    pub execution_mode: AdfExecutionMode,
}

pub trait AdfPolicy: Send + Sync + 'static {
    fn evaluate(&self, request: &AdfPolicyRequest) -> Result<AdfPolicyDecision, AdfError>;
}

#[derive(Debug, Clone)]
pub struct AdfExecutionRequest {
    pub run_id: RunId,
    pub call_id: String,
    pub artifact: AdfArtifact,
    pub arguments: Value,
    pub cancellation: RunCancellation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdfExecutionOutput {
    pub value: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
}

pub type AdfExecutionFuture =
    Pin<Box<dyn Future<Output = Result<AdfExecutionOutput, AdfError>> + Send + 'static>>;
pub type AdfValidationFuture = Pin<Box<dyn Future<Output = Result<(), AdfError>> + Send + 'static>>;

pub trait AdfExecutor: Send + Sync + 'static {
    fn descriptor(&self) -> AdfRuntimeDescriptor;
    fn validate(&self, artifact: AdfArtifact) -> AdfValidationFuture;
    fn execute(&self, request: AdfExecutionRequest) -> AdfExecutionFuture;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdfErrorKind {
    InvalidRequest,
    Conflict,
    PermissionDenied,
    NotFound,
    ResourceExhausted,
    Timeout,
    Cancelled,
    Unavailable,
    Internal,
}

#[derive(Debug, Error)]
#[error("{message}")]
pub struct AdfError {
    kind: AdfErrorKind,
    code: String,
    message: String,
    retryable: bool,
}

impl AdfError {
    #[must_use]
    pub fn new(
        kind: AdfErrorKind,
        code: impl Into<String>,
        message: impl Into<String>,
        retryable: bool,
    ) -> Self {
        Self {
            kind,
            code: code.into(),
            message: message.into(),
            retryable,
        }
    }

    #[must_use]
    pub const fn kind(&self) -> AdfErrorKind {
        self.kind
    }

    #[must_use]
    pub fn code(&self) -> &str {
        &self.code
    }

    #[must_use]
    pub fn safe_message(&self) -> &str {
        &self.message
    }

    #[must_use]
    pub const fn retryable(&self) -> bool {
        self.retryable
    }
}
