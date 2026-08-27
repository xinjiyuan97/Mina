use std::{future::Future, pin::Pin};

use crate::harness::{ContentPart, RunId, SessionId};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MemoryId(Uuid);

impl MemoryId {
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MemoryApprovalId(Uuid);

impl MemoryApprovalId {
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for MemoryApprovalId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for MemoryApprovalId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::str::FromStr for MemoryApprovalId {
    type Err = uuid::Error;

    fn from_str(source: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(source).map(Self)
    }
}

impl Default for MemoryId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for MemoryId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::str::FromStr for MemoryId {
    type Err = uuid::Error;

    fn from_str(source: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(source).map(Self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MemoryScope(pub String);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryKind {
    Semantic,
    Episodic,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MemorySourceRef {
    Session { session_id: SessionId, ordinal: u64 },
    Run { run_id: RunId, seq: Option<u64> },
    ExplicitUserInput { run_id: Option<RunId> },
    Artifact { artifact_id: String, digest: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemoryRecord {
    pub memory_id: MemoryId,
    pub scope: MemoryScope,
    pub kind: MemoryKind,
    pub content: Vec<ContentPart>,
    pub source_refs: Vec<MemorySourceRef>,
    pub confidence: f32,
    pub salience: f32,
    pub version: u64,
    pub expires_at_ms: Option<i64>,
    pub supersedes: Option<MemoryId>,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PutMemory {
    pub record: MemoryRecord,
    pub expected_absent: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryLocator {
    pub memory_id: MemoryId,
    pub version: Option<u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MemoryListQuery {
    pub scopes: Vec<MemoryScope>,
    pub kinds: Vec<MemoryKind>,
    pub after_created_at_ms: Option<i64>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MemoryPage {
    pub records: Vec<MemoryRecord>,
    pub has_more: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SupersedeMemory {
    pub existing_id: MemoryId,
    pub expected_version: u64,
    pub replacement: MemoryRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForgetMemory {
    pub memory_id: MemoryId,
    pub expected_version: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryComponentDescriptor {
    pub identity: String,
    pub kind: String,
    pub version: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryRetrieveRequest {
    pub query: String,
    pub scopes: Vec<MemoryScope>,
    pub kinds: Vec<MemoryKind>,
    pub top_k: usize,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RankedMemory {
    pub record: MemoryRecord,
    pub score: f32,
    pub score_explanation: String,
    pub strategy: MemoryComponentDescriptor,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MemoryExtractionRequest {
    pub scope: MemoryScope,
    pub content: Vec<ContentPart>,
    pub source_refs: Vec<MemorySourceRef>,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemoryCandidate {
    pub proposed: MemoryRecord,
    pub sensitivity: MemorySensitivity,
    pub extraction_reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemorySensitivity {
    Public,
    Private,
    Secret,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MemoryWriteDecision {
    Accept { normalized: PutMemory },
    Reject { reason: String },
    RequireApproval { reason: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryApprovalStatus {
    Pending,
    Approved,
    Denied,
    Expired,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemoryWriteProposal {
    pub approval_id: MemoryApprovalId,
    pub candidate: MemoryCandidate,
    pub reason: String,
    pub status: MemoryApprovalStatus,
    pub created_at_ms: i64,
    pub resolved_at_ms: Option<i64>,
    pub resolution_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateMemoryWriteProposal {
    pub proposal: MemoryWriteProposal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolveMemoryWriteProposal {
    pub approval_id: MemoryApprovalId,
    pub decision: MemoryApprovalStatus,
    pub reason: Option<String>,
    pub resolved_at_ms: i64,
}

pub type MemoryFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, MemoryError>> + Send + 'a>>;

pub trait MemoryStore: Send + Sync + 'static {
    fn descriptor(&self) -> MemoryComponentDescriptor;
    fn put(&self, command: PutMemory) -> MemoryFuture<'_, MemoryRecord>;
    fn get(&self, locator: MemoryLocator) -> MemoryFuture<'_, Option<MemoryRecord>>;
    fn list(&self, query: MemoryListQuery) -> MemoryFuture<'_, MemoryPage>;
    fn supersede(&self, command: SupersedeMemory) -> MemoryFuture<'_, MemoryRecord>;
    fn forget(&self, command: ForgetMemory) -> MemoryFuture<'_, ()>;
}

pub trait MemoryRetriever: Send + Sync + 'static {
    fn descriptor(&self) -> MemoryComponentDescriptor;
    fn retrieve(&self, request: MemoryRetrieveRequest) -> MemoryFuture<'_, Vec<RankedMemory>>;
}

pub trait MemoryExtractor: Send + Sync + 'static {
    fn descriptor(&self) -> MemoryComponentDescriptor;
    fn extract(&self, request: MemoryExtractionRequest) -> MemoryFuture<'_, Vec<MemoryCandidate>>;
}

pub trait MemoryWritePolicy: Send + Sync + 'static {
    fn descriptor(&self) -> MemoryComponentDescriptor;
    fn decide(&self, candidate: MemoryCandidate) -> MemoryFuture<'_, MemoryWriteDecision>;
}

pub trait MemoryProposalStore: Send + Sync + 'static {
    fn descriptor(&self) -> MemoryComponentDescriptor;
    fn create(&self, command: CreateMemoryWriteProposal) -> MemoryFuture<'_, MemoryWriteProposal>;
    fn get_proposal(
        &self,
        approval_id: MemoryApprovalId,
    ) -> MemoryFuture<'_, Option<MemoryWriteProposal>>;
    fn resolve_proposal(
        &self,
        command: ResolveMemoryWriteProposal,
    ) -> MemoryFuture<'_, MemoryWriteProposal>;
    fn pending_proposals(&self, limit: usize) -> MemoryFuture<'_, Vec<MemoryWriteProposal>>;
}

#[derive(Debug, Error)]
pub enum MemoryError {
    #[error("memory {0} was not found")]
    NotFound(MemoryId),
    #[error("memory {0} already exists")]
    AlreadyExists(MemoryId),
    #[error("memory version conflict: expected {expected}, actual {actual}")]
    VersionConflict { expected: u64, actual: u64 },
    #[error("memory contract validation failed: {0}")]
    Invalid(String),
    #[error("memory adapter failed: {0}")]
    Backend(String),
}

impl MemoryError {
    #[must_use]
    pub fn backend(message: impl Into<String>) -> Self {
        Self::Backend(message.into())
    }
}

#[must_use]
pub fn content_text(parts: &[ContentPart]) -> String {
    parts
        .iter()
        .filter_map(ContentPart::as_text)
        .collect::<Vec<_>>()
        .join("\n")
}
