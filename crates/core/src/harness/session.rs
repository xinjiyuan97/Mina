use std::{fmt, future::Future, pin::Pin, str::FromStr};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::harness::{RunId, RunSnapshot};

pub const MAX_SESSION_PAGE_SIZE: usize = 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SessionId(Uuid);

impl SessionId {
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for SessionId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for SessionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for SessionId {
    type Err = uuid::Error;

    fn from_str(source: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(source).map(Self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MessageId(Uuid);

impl MessageId {
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for MessageId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for MessageId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for MessageId {
    type Err = uuid::Error;

    fn from_str(source: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(source).map(Self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentPart {
    Text { text: String },
    BlobRef { blob_id: String, media_type: String },
}

impl ContentPart {
    #[must_use]
    pub fn text(text: impl Into<String>) -> Self {
        Self::Text { text: text.into() }
    }

    #[must_use]
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Self::Text { text } => Some(text),
            Self::BlobRef { .. } => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConversationRole {
    User,
    Assistant,
    SystemNote,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionMessage {
    pub message_id: MessageId,
    pub session_id: SessionId,
    pub ordinal: u64,
    pub role: ConversationRole,
    pub content: Vec<ContentPart>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_run_id: Option<RunId>,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionStatus {
    Active,
    Archived,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionSnapshot {
    pub session_id: SessionId,
    pub agent_profile: String,
    pub status: SessionStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    pub revision: u64,
    pub next_message_ordinal: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_run_id: Option<RunId>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

impl SessionSnapshot {
    #[must_use]
    pub fn new(command: &CreateSession) -> Self {
        Self {
            session_id: command.session_id,
            agent_profile: command.agent_profile.clone(),
            status: SessionStatus::Active,
            title: command.title.clone(),
            revision: 0,
            next_message_ordinal: 1,
            active_run_id: None,
            created_at_ms: command.created_at_ms,
            updated_at_ms: command.created_at_ms,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateSession {
    pub session_id: SessionId,
    pub agent_profile: String,
    pub title: Option<String>,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BeginSessionRun {
    pub session_id: SessionId,
    pub run_id: RunId,
    pub expected_revision: u64,
    pub idempotency_key: String,
    pub request_hash: String,
    pub input: Vec<ContentPart>,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeginRunResult {
    pub session: SessionSnapshot,
    pub run_id: RunId,
    pub context_through_ordinal: u64,
    pub replayed: bool,
}

#[derive(Debug, Clone)]
pub struct FinalizeSessionRun {
    pub session_id: SessionId,
    pub run: RunSnapshot,
    pub finalized_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveSession {
    pub session_id: SessionId,
    pub expected_revision: u64,
    pub archived_at_ms: i64,
}

pub type SessionStoreFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, SessionStoreError>> + Send + 'a>>;

pub trait SessionStore: Send + Sync + 'static {
    fn create_session(&self, command: CreateSession) -> SessionStoreFuture<'_, SessionSnapshot>;

    fn list_sessions(
        &self,
        status: Option<SessionStatus>,
        limit: usize,
    ) -> SessionStoreFuture<'_, Vec<SessionSnapshot>>;

    fn begin_run(
        &self,
        command: BeginSessionRun,
        initial_run: RunSnapshot,
    ) -> SessionStoreFuture<'_, BeginRunResult>;

    fn finalize_run(&self, command: FinalizeSessionRun) -> SessionStoreFuture<'_, SessionSnapshot>;

    fn archive_session(&self, command: ArchiveSession) -> SessionStoreFuture<'_, SessionSnapshot>;

    fn get_session(&self, session_id: SessionId)
    -> SessionStoreFuture<'_, Option<SessionSnapshot>>;

    fn messages(
        &self,
        session_id: SessionId,
        before: Option<u64>,
        limit: usize,
    ) -> SessionStoreFuture<'_, Vec<SessionMessage>>;

    fn pending_finalizations(&self) -> SessionStoreFuture<'_, Vec<(SessionId, RunSnapshot)>>;
}

#[derive(Debug, Error)]
pub enum SessionStoreError {
    #[error("session {0} does not exist")]
    NotFound(SessionId),
    #[error("session {0} already exists")]
    AlreadyExists(SessionId),
    #[error("session revision conflict: expected {expected}, actual {actual}")]
    RevisionConflict { expected: u64, actual: u64 },
    #[error("session already has active run {0}")]
    Busy(RunId),
    #[error("session is archived")]
    Archived,
    #[error("idempotency key was reused with different input")]
    IdempotencyConflict,
    #[error("run {0} is not the active run for the session")]
    RunMismatch(RunId),
    #[error("run must be terminal before session finalization")]
    RunNotTerminal,
    #[error("session store backend failed: {0}")]
    Backend(String),
}

impl SessionStoreError {
    #[must_use]
    pub fn backend(message: impl Into<String>) -> Self {
        Self::Backend(message.into())
    }
}
