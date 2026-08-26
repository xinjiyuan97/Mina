use std::{future::Future, pin::Pin};

use crate::harness::{ModelMessage, ModelRole};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextComponentDescriptor {
    pub identity: String,
    pub kind: String,
    pub version: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextPriority {
    Low,
    Normal,
    High,
    Required,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextSourceRef {
    pub kind: String,
    pub identity: String,
    pub version: String,
    pub digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextItem {
    pub item_id: String,
    pub role: ModelRole,
    pub content: String,
    pub priority: ContextPriority,
    pub source: ContextSourceRef,
}

impl ContextItem {
    #[must_use]
    pub fn into_model_message(self) -> ModelMessage {
        match self.role {
            ModelRole::System => ModelMessage::system(self.content),
            ModelRole::User => ModelMessage::user(self.content),
            ModelRole::Assistant => ModelMessage {
                role: ModelRole::Assistant,
                content: self.content,
                reasoning: String::new(),
                tool_calls: Vec::new(),
                tool_call_id: None,
            },
            ModelRole::Tool => ModelMessage::tool_result("context-artifact", self.content),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenSegment {
    pub identity: String,
    pub content: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenEstimateRequest {
    pub model_profile: String,
    pub segments: Vec<TokenSegment>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TokenEstimate {
    pub tokens: u64,
    pub per_segment: Vec<u64>,
    pub confidence: f32,
    pub estimator: ContextComponentDescriptor,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextBudget {
    pub model_context_tokens: u64,
    pub reserved_output_tokens: u64,
    pub reserved_tool_schema_tokens: u64,
    pub max_skill_tokens: u64,
    pub max_memory_tokens: u64,
    pub max_history_tokens: u64,
}

impl ContextBudget {
    #[must_use]
    pub fn available_input_tokens(&self) -> u64 {
        self.model_context_tokens
            .saturating_sub(self.reserved_output_tokens)
            .saturating_sub(self.reserved_tool_schema_tokens)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompressionRequest {
    pub candidates: Vec<ContextItem>,
    pub source_digest: String,
    pub budget: ContextBudget,
    pub reusable_artifacts: Vec<ContextArtifact>,
    pub model_profile: String,
    pub policy_version: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DroppedContextItem {
    pub item: ContextItem,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextArtifactCandidate {
    pub kind: ContextArtifactKind,
    pub source_refs: Vec<ContextSourceRef>,
    pub content: String,
    pub source_digest: String,
    pub content_digest: String,
    pub policy_version: u32,
    pub generator: ContextComponentDescriptor,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompressionResult {
    pub retained: Vec<ContextItem>,
    pub dropped: Vec<DroppedContextItem>,
    pub artifact_candidates: Vec<ContextArtifactCandidate>,
    pub estimated_input_tokens: u64,
    pub estimator_confidence: f32,
    pub compressor: ContextComponentDescriptor,
    pub result_fingerprint: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextArtifactKind {
    Summary,
    ExtractedFacts,
    ReducedToolResult,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ContextArtifactId(Uuid);

impl ContextArtifactId {
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for ContextArtifactId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for ContextArtifactId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::str::FromStr for ContextArtifactId {
    type Err = uuid::Error;

    fn from_str(source: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(source).map(Self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextArtifactRef {
    pub artifact_id: ContextArtifactId,
    pub content_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextArtifact {
    pub artifact_id: ContextArtifactId,
    pub kind: ContextArtifactKind,
    pub source_refs: Vec<ContextSourceRef>,
    pub policy_version: u32,
    pub generator: ContextComponentDescriptor,
    pub content: String,
    pub source_digest: String,
    pub content_digest: String,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReusableArtifactQuery {
    pub source_digest: String,
    pub policy_version: u32,
    pub kinds: Vec<ContextArtifactKind>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutContextArtifact {
    pub candidate: ContextArtifactCandidate,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidateContextArtifacts {
    pub source_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SummaryRequest {
    pub model_profile: String,
    pub items: Vec<ContextItem>,
    pub maximum_tokens: u64,
    pub source_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SummaryResult {
    pub content: String,
    pub input_digest: String,
    pub output_digest: String,
    pub generator: ContextComponentDescriptor,
}

pub type ContextFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, ContextError>> + Send + 'a>>;

pub trait TokenEstimator: Send + Sync + 'static {
    fn descriptor(&self) -> ContextComponentDescriptor;
    fn estimate(&self, request: TokenEstimateRequest) -> ContextFuture<'_, TokenEstimate>;
}

pub trait ContextCompressor: Send + Sync + 'static {
    fn descriptor(&self) -> ContextComponentDescriptor;
    fn compress(&self, request: CompressionRequest) -> ContextFuture<'_, CompressionResult>;
}

pub trait SummaryGenerator: Send + Sync + 'static {
    fn descriptor(&self) -> ContextComponentDescriptor;
    fn summarize(&self, request: SummaryRequest) -> ContextFuture<'_, SummaryResult>;
}

pub trait ContextArtifactStore: Send + Sync + 'static {
    fn descriptor(&self) -> ContextComponentDescriptor;
    fn find_reusable(
        &self,
        query: ReusableArtifactQuery,
    ) -> ContextFuture<'_, Vec<ContextArtifactRef>>;
    fn get(&self, artifact: ContextArtifactRef) -> ContextFuture<'_, Option<ContextArtifact>>;
    fn put(&self, command: PutContextArtifact) -> ContextFuture<'_, ContextArtifactRef>;
    fn invalidate(&self, command: InvalidateContextArtifacts) -> ContextFuture<'_, u64>;
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextBudgetReport {
    pub available_input_tokens: u64,
    pub estimated_input_tokens: u64,
    pub estimator_confidence: f32,
    pub retained_item_ids: Vec<String>,
    pub dropped: Vec<DroppedContextItem>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextPack {
    pub messages: Vec<ModelMessage>,
    pub memory_refs: Vec<String>,
    pub artifact_refs: Vec<ContextArtifactRef>,
    pub skill_refs: Vec<String>,
    pub effective_tools: Vec<String>,
    pub budget: ContextBudgetReport,
    pub component_descriptors: Vec<ContextComponentDescriptor>,
    pub fingerprint: String,
}

#[derive(Debug, Error)]
pub enum ContextError {
    #[error("required context exceeds the model budget")]
    BudgetExceeded,
    #[error("context contract validation failed: {0}")]
    Invalid(String),
    #[error("context component failed: {0}")]
    Component(String),
}
