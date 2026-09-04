use std::{collections::HashMap, fmt, future::Future, pin::Pin, str::FromStr};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

use crate::harness::{
    ApprovalId, ApprovalResolution, BlobId, ObservedRunEvent, OutputChannel, RunEventKind, RunId,
    RunSnapshot, ToolRiskLevel,
};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionToolCallState {
    InputStreaming,
    InputAvailable,
    Executing,
    OutputAvailable,
    OutputError,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentPart {
    Text {
        text: String,
    },
    Reasoning {
        text: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        duration_ms: Option<u64>,
    },
    ToolCall {
        tool_call_id: String,
        name: String,
        state: SessionToolCallState,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        input: Option<Value>,
        #[serde(default, skip_serializing_if = "String::is_empty")]
        input_text: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        output: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        duration_ms: Option<u64>,
    },
    Permission {
        approval_id: ApprovalId,
        call_id: String,
        tool_name: String,
        risk_level: ToolRiskLevel,
        arguments: Value,
        requested_at_ms: i64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        resolution: Option<ApprovalResolution>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        resolved_at_ms: Option<i64>,
    },
    BlobRef {
        blob_id: BlobId,
        media_type: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        size_bytes: Option<u64>,
    },
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
            Self::Reasoning { .. }
            | Self::ToolCall { .. }
            | Self::Permission { .. }
            | Self::BlobRef { .. } => None,
        }
    }
}

/// Builds the compact, ordered Session replay projection from a run's event log.
///
/// The append-only run log remains authoritative. This projection deliberately
/// keeps message-level UI semantics (reasoning blocks, tool state and approvals)
/// without requiring clients to replay every token event after a refresh.
#[must_use]
pub fn project_run_content(events: &[ObservedRunEvent]) -> Vec<ContentPart> {
    #[derive(Debug, Clone, Copy)]
    enum ActiveOutput {
        Text(usize),
        Reasoning { index: usize, started_at_ms: i64 },
    }

    fn elapsed_ms(started_at_ms: i64, ended_at_ms: i64) -> u64 {
        u64::try_from(ended_at_ms.saturating_sub(started_at_ms).max(0)).unwrap_or(u64::MAX)
    }

    fn close_output(
        content: &mut [ContentPart],
        active: &mut Option<ActiveOutput>,
        ended_at_ms: i64,
    ) {
        let Some(active_output) = active.take() else {
            return;
        };
        if let ActiveOutput::Reasoning {
            index,
            started_at_ms,
        } = active_output
            && let Some(ContentPart::Reasoning { duration_ms, .. }) = content.get_mut(index)
        {
            *duration_ms = Some(elapsed_ms(started_at_ms, ended_at_ms));
        }
    }

    let mut content = Vec::new();
    let mut active_output = None;
    let mut tool_indices = HashMap::<String, usize>::new();
    let mut tool_started_at = HashMap::<String, i64>::new();
    let mut approval_indices = HashMap::<ApprovalId, usize>::new();

    for observed in events {
        let observed_at_ms = observed.observed_at_ms;
        match &observed.event.kind {
            RunEventKind::OutputDelta { channel, delta } if !delta.is_empty() => match channel {
                OutputChannel::AssistantText => {
                    let index = match active_output {
                        Some(ActiveOutput::Text(index)) => index,
                        _ => {
                            close_output(&mut content, &mut active_output, observed_at_ms);
                            content.push(ContentPart::Text {
                                text: String::new(),
                            });
                            let index = content.len().saturating_sub(1);
                            active_output = Some(ActiveOutput::Text(index));
                            index
                        }
                    };
                    if let Some(ContentPart::Text { text }) = content.get_mut(index) {
                        text.push_str(delta);
                    }
                }
                OutputChannel::AssistantReasoning => {
                    let index = match active_output {
                        Some(ActiveOutput::Reasoning { index, .. }) => index,
                        _ => {
                            close_output(&mut content, &mut active_output, observed_at_ms);
                            content.push(ContentPart::Reasoning {
                                text: String::new(),
                                duration_ms: None,
                            });
                            let index = content.len().saturating_sub(1);
                            active_output = Some(ActiveOutput::Reasoning {
                                index,
                                started_at_ms: observed_at_ms,
                            });
                            index
                        }
                    };
                    if let Some(ContentPart::Reasoning { text, .. }) = content.get_mut(index) {
                        text.push_str(delta);
                    }
                }
            },
            RunEventKind::ToolCallStarted { call_id, name } => {
                close_output(&mut content, &mut active_output, observed_at_ms);
                content.push(ContentPart::ToolCall {
                    tool_call_id: call_id.clone(),
                    name: name.clone(),
                    state: SessionToolCallState::InputStreaming,
                    input: None,
                    input_text: String::new(),
                    output: None,
                    error: None,
                    duration_ms: None,
                });
                tool_indices.insert(call_id.clone(), content.len().saturating_sub(1));
            }
            RunEventKind::ToolCallArgumentsDelta { call_id, delta } => {
                if let Some(index) = tool_indices.get(call_id).copied()
                    && let Some(ContentPart::ToolCall { input_text, .. }) = content.get_mut(index)
                {
                    input_text.push_str(delta);
                }
            }
            RunEventKind::ApprovalRequested {
                approval_id,
                call_id,
                tool_name,
                risk_level,
                arguments,
            } => {
                if let Some(index) = tool_indices.get(call_id).copied()
                    && let Some(ContentPart::ToolCall { state, input, .. }) = content.get_mut(index)
                {
                    *state = SessionToolCallState::InputAvailable;
                    *input = Some(arguments.clone());
                }
                content.push(ContentPart::Permission {
                    approval_id: *approval_id,
                    call_id: call_id.clone(),
                    tool_name: tool_name.clone(),
                    risk_level: *risk_level,
                    arguments: arguments.clone(),
                    requested_at_ms: observed_at_ms,
                    resolution: None,
                    resolved_at_ms: None,
                });
                approval_indices.insert(*approval_id, content.len().saturating_sub(1));
            }
            RunEventKind::ApprovalResolved {
                approval_id,
                resolution,
                ..
            } => {
                if let Some(index) = approval_indices.get(approval_id).copied()
                    && let Some(ContentPart::Permission {
                        resolution: stored_resolution,
                        resolved_at_ms,
                        ..
                    }) = content.get_mut(index)
                {
                    *stored_resolution = Some(resolution.clone());
                    *resolved_at_ms = Some(observed_at_ms);
                }
            }
            RunEventKind::ToolExecutionStarted { call_id, arguments } => {
                if let Some(index) = tool_indices.get(call_id).copied()
                    && let Some(ContentPart::ToolCall { state, input, .. }) = content.get_mut(index)
                {
                    *state = SessionToolCallState::Executing;
                    *input = Some(arguments.clone());
                    tool_started_at.insert(call_id.clone(), observed_at_ms);
                }
            }
            RunEventKind::ToolExecutionCompleted { call_id, output } => {
                if let Some(index) = tool_indices.get(call_id).copied()
                    && let Some(ContentPart::ToolCall {
                        state,
                        output: stored_output,
                        duration_ms,
                        ..
                    }) = content.get_mut(index)
                {
                    *state = SessionToolCallState::OutputAvailable;
                    *stored_output = Some(output.clone());
                    *duration_ms = tool_started_at
                        .get(call_id)
                        .map(|started_at_ms| elapsed_ms(*started_at_ms, observed_at_ms));
                }
            }
            RunEventKind::ToolExecutionFailed {
                call_id, message, ..
            } => {
                if let Some(index) = tool_indices.get(call_id).copied()
                    && let Some(ContentPart::ToolCall {
                        state,
                        error,
                        duration_ms,
                        ..
                    }) = content.get_mut(index)
                {
                    *state = SessionToolCallState::OutputError;
                    *error = Some(message.clone());
                    *duration_ms = tool_started_at
                        .get(call_id)
                        .map(|started_at_ms| elapsed_ms(*started_at_ms, observed_at_ms));
                }
            }
            RunEventKind::RunWaiting { .. }
            | RunEventKind::RunCompleted { .. }
            | RunEventKind::RunFailed { .. }
            | RunEventKind::RunCancelled => {
                close_output(&mut content, &mut active_output, observed_at_ms);
            }
            _ => {}
        }
    }

    if let Some(last) = events.last() {
        close_output(&mut content, &mut active_output, last.observed_at_ms);
    }
    content
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::harness::{ApprovalDecision, FinishReason, RunEvent, ToolErrorCategory};

    #[test]
    fn projects_ordered_reasoning_tools_approvals_and_text() {
        let run_id = RunId::new();
        let approval_id = ApprovalId::new();
        let events = vec![
            observed(run_id, 1, RunEventKind::RunStarted, 10),
            observed(
                run_id,
                2,
                RunEventKind::OutputDelta {
                    channel: OutputChannel::AssistantReasoning,
                    delta: "inspect".into(),
                },
                20,
            ),
            observed(
                run_id,
                3,
                RunEventKind::OutputDelta {
                    channel: OutputChannel::AssistantText,
                    delta: "checking".into(),
                },
                35,
            ),
            observed(
                run_id,
                4,
                RunEventKind::ToolCallStarted {
                    call_id: "call_read".into(),
                    name: "read".into(),
                },
                40,
            ),
            observed(
                run_id,
                5,
                RunEventKind::ToolCallArgumentsDelta {
                    call_id: "call_read".into(),
                    delta: r#"{"path":"README.md"}"#.into(),
                },
                41,
            ),
            observed(
                run_id,
                6,
                RunEventKind::ApprovalRequested {
                    approval_id,
                    call_id: "call_read".into(),
                    tool_name: "read".into(),
                    risk_level: ToolRiskLevel::Medium,
                    arguments: json!({"path": "README.md"}),
                },
                45,
            ),
            observed(
                run_id,
                7,
                RunEventKind::ApprovalResolved {
                    approval_id,
                    call_id: "call_read".into(),
                    resolution: ApprovalResolution::allow_once(),
                },
                50,
            ),
            observed(
                run_id,
                8,
                RunEventKind::ToolExecutionStarted {
                    call_id: "call_read".into(),
                    arguments: json!({"path": "README.md"}),
                },
                60,
            ),
            observed(
                run_id,
                9,
                RunEventKind::ToolExecutionCompleted {
                    call_id: "call_read".into(),
                    output: r#"{"content":"mina"}"#.into(),
                },
                85,
            ),
            observed(
                run_id,
                10,
                RunEventKind::ToolCallStarted {
                    call_id: "call_search".into(),
                    name: "search".into(),
                },
                90,
            ),
            observed(
                run_id,
                11,
                RunEventKind::ToolExecutionStarted {
                    call_id: "call_search".into(),
                    arguments: json!({"query": "missing"}),
                },
                95,
            ),
            observed(
                run_id,
                12,
                RunEventKind::ToolExecutionFailed {
                    call_id: "call_search".into(),
                    code: "not_found".into(),
                    message: "no result".into(),
                    category: ToolErrorCategory::NotFound,
                    retryable: false,
                    retry_after_ms: None,
                },
                107,
            ),
            observed(
                run_id,
                13,
                RunEventKind::OutputDelta {
                    channel: OutputChannel::AssistantText,
                    delta: "done".into(),
                },
                110,
            ),
            observed(
                run_id,
                14,
                RunEventKind::RunCompleted {
                    finish_reason: FinishReason::Stop,
                },
                120,
            ),
        ];

        let content = project_run_content(&events);

        assert_eq!(content.len(), 6);
        assert_eq!(
            content[0],
            ContentPart::Reasoning {
                text: "inspect".into(),
                duration_ms: Some(15),
            }
        );
        assert_eq!(content[1], ContentPart::text("checking"));
        assert_eq!(
            content[2],
            ContentPart::ToolCall {
                tool_call_id: "call_read".into(),
                name: "read".into(),
                state: SessionToolCallState::OutputAvailable,
                input: Some(json!({"path": "README.md"})),
                input_text: r#"{"path":"README.md"}"#.into(),
                output: Some(r#"{"content":"mina"}"#.into()),
                error: None,
                duration_ms: Some(25),
            }
        );
        assert_eq!(
            content[3],
            ContentPart::Permission {
                approval_id,
                call_id: "call_read".into(),
                tool_name: "read".into(),
                risk_level: ToolRiskLevel::Medium,
                arguments: json!({"path": "README.md"}),
                requested_at_ms: 45,
                resolution: Some(ApprovalResolution {
                    decision: ApprovalDecision::AllowOnce,
                    reason: None,
                }),
                resolved_at_ms: Some(50),
            }
        );
        assert_eq!(
            content[4],
            ContentPart::ToolCall {
                tool_call_id: "call_search".into(),
                name: "search".into(),
                state: SessionToolCallState::OutputError,
                input: Some(json!({"query": "missing"})),
                input_text: String::new(),
                output: None,
                error: Some("no result".into()),
                duration_ms: Some(12),
            }
        );
        assert_eq!(content[5], ContentPart::text("done"));
        assert!(content[2..5].iter().all(|part| part.as_text().is_none()));
    }

    fn observed(
        run_id: RunId,
        seq: u64,
        kind: RunEventKind,
        observed_at_ms: i64,
    ) -> ObservedRunEvent {
        ObservedRunEvent::new(RunEvent::new(run_id, seq, kind), observed_at_ms)
    }
}
