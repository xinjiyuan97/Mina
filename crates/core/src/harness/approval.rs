use std::{fmt, future::Future, pin::Pin, str::FromStr};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

use crate::harness::{RunId, ToolRiskLevel};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ApprovalId(Uuid);

impl ApprovalId {
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for ApprovalId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for ApprovalId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for ApprovalId {
    type Err = uuid::Error;

    fn from_str(source: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(source).map(Self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ApprovalDecision {
    AllowOnce,
    Deny,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalResolution {
    pub decision: ApprovalDecision,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl ApprovalResolution {
    #[must_use]
    pub const fn allow_once() -> Self {
        Self {
            decision: ApprovalDecision::AllowOnce,
            reason: None,
        }
    }

    #[must_use]
    pub fn deny(reason: Option<String>) -> Self {
        Self {
            decision: ApprovalDecision::Deny,
            reason,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ApprovalRequest {
    pub approval_id: ApprovalId,
    pub run_id: RunId,
    pub call_id: String,
    pub tool_name: String,
    pub risk_level: ToolRiskLevel,
    pub arguments: Value,
}

#[derive(Debug, Error)]
#[error("{message}")]
pub struct ApprovalError {
    code: String,
    message: String,
}

impl ApprovalError {
    #[must_use]
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }

    #[must_use]
    pub fn code(&self) -> &str {
        &self.code
    }

    #[must_use]
    pub fn safe_message(&self) -> &str {
        &self.message
    }
}

pub type ApprovalFuture =
    Pin<Box<dyn Future<Output = Result<ApprovalResolution, ApprovalError>> + Send + 'static>>;

/// Host boundary for one pending approval. Durable event-driven implementations
/// can replace the in-memory Gateway adapter without changing AgentLoop.
pub trait ApprovalPort: Send + Sync + 'static {
    fn request(&self, request: ApprovalRequest) -> ApprovalFuture;
}

/// Safe fallback: risky tools are rejected when the host has no approval adapter.
#[derive(Debug, Default)]
pub struct RejectAllApprovals;

impl ApprovalPort for RejectAllApprovals {
    fn request(&self, _request: ApprovalRequest) -> ApprovalFuture {
        Box::pin(async { Ok(ApprovalResolution::deny(None)) })
    }
}
