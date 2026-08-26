use std::pin::Pin;

use futures_core::Stream;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::harness::{RunId, ToolDefinition};

/// Provider-neutral role used at the Agent -> model boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ModelRole {
    System,
    User,
    Assistant,
    Tool,
}

impl ModelRole {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::System => "system",
            Self::User => "user",
            Self::Assistant => "assistant",
            Self::Tool => "tool",
        }
    }
}

/// A completed model-requested function call stored in conversation history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelToolCall {
    pub id: String,
    pub name: String,
    /// Complete JSON arguments as produced by the model.
    pub arguments: String,
}

/// One normalized conversation message. Provider adapters map this shape to
/// their own assistant tool-call and tool-result wire formats.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelMessage {
    pub role: ModelRole,
    pub content: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reasoning: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<ModelToolCall>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
}

impl ModelMessage {
    #[must_use]
    pub fn system(content: impl Into<String>) -> Self {
        Self {
            role: ModelRole::System,
            content: content.into(),
            reasoning: String::new(),
            tool_calls: Vec::new(),
            tool_call_id: None,
        }
    }

    #[must_use]
    pub fn user(content: impl Into<String>) -> Self {
        Self {
            role: ModelRole::User,
            content: content.into(),
            reasoning: String::new(),
            tool_calls: Vec::new(),
            tool_call_id: None,
        }
    }

    #[must_use]
    pub fn assistant_tool_calls(
        content: impl Into<String>,
        reasoning: impl Into<String>,
        tool_calls: Vec<ModelToolCall>,
    ) -> Self {
        Self {
            role: ModelRole::Assistant,
            content: content.into(),
            reasoning: reasoning.into(),
            tool_calls,
            tool_call_id: None,
        }
    }

    #[must_use]
    pub fn tool_result(call_id: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            role: ModelRole::Tool,
            content: content.into(),
            reasoning: String::new(),
            tool_calls: Vec::new(),
            tool_call_id: Some(call_id.into()),
        }
    }
}

/// A single normalized model invocation produced by an Agent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelRequest {
    pub run_id: RunId,
    pub model: String,
    pub messages: Vec<ModelMessage>,
    pub tools: Vec<ToolDefinition>,
    pub max_output_tokens: Option<u32>,
}

/// Provider-independent completion reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum FinishReason {
    Stop,
    Length,
    ToolCall,
    ContentFilter,
    Unknown,
}

/// Normalized token accounting returned by a provider when available.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_tokens: u64,
    #[serde(default)]
    pub source: TokenUsageSource,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TokenUsageSource {
    #[default]
    ProviderReported,
    EstimatorFallback,
    Mixed,
}

/// Stable model error categories understood by the Agent and Gateway.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ModelErrorKind {
    InvalidRequest,
    Authentication,
    PermissionDenied,
    RateLimited,
    UpstreamUnavailable,
    Timeout,
    ProtocolViolation,
    Internal,
}

/// Provider-neutral streaming events. The adapter must emit exactly one
/// `Completed` or `Failed` event and then end the stream.
#[derive(Debug)]
#[non_exhaustive]
pub enum ModelEvent {
    Accepted { provider_request_id: Option<String> },
    ReasoningDelta { delta: String },
    TextDelta { delta: String },
    ToolCallStarted { call_id: String, name: String },
    ToolCallArgumentsDelta { call_id: String, delta: String },
    Usage { usage: TokenUsage },
    Completed { finish_reason: FinishReason },
    Failed { error: ModelError },
}

/// Pull-based model stream. Polling provides natural backpressure and dropping
/// the stream cancels the local HTTP response body.
pub type ModelEventStream = Pin<Box<dyn Stream<Item = ModelEvent> + Send + 'static>>;

impl ModelErrorKind {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidRequest => "invalid_request",
            Self::Authentication => "upstream_authentication",
            Self::PermissionDenied => "upstream_permission_denied",
            Self::RateLimited => "rate_limited",
            Self::UpstreamUnavailable => "upstream_unavailable",
            Self::Timeout => "upstream_timeout",
            Self::ProtocolViolation => "upstream_protocol_violation",
            Self::Internal => "internal",
        }
    }
}

/// Safe, normalized provider error. Raw response bodies and credentials stay in
/// the adapter and never cross into Agent events or public HTTP responses.
#[derive(Debug, Error)]
#[error("{message}")]
pub struct ModelError {
    kind: ModelErrorKind,
    message: String,
    retryable: bool,
}

impl ModelError {
    #[must_use]
    pub fn new(kind: ModelErrorKind, message: impl Into<String>, retryable: bool) -> Self {
        Self {
            kind,
            message: message.into(),
            retryable,
        }
    }

    #[must_use]
    pub const fn kind(&self) -> ModelErrorKind {
        self.kind
    }

    #[must_use]
    pub const fn retryable(&self) -> bool {
        self.retryable
    }

    #[must_use]
    pub fn safe_message(&self) -> &str {
        &self.message
    }
}

/// Hexagonal port used by Agents. Implementations may use OpenAI-compatible
/// HTTP, another vendor SDK, a local model, or a deterministic fake.
pub trait ModelPort: Send + Sync + 'static {
    fn stream(&self, request: ModelRequest) -> ModelEventStream;
}
