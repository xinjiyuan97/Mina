use std::{fmt, future::Future, pin::Pin, str::FromStr};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

use crate::harness::{RunId, ToolRiskLevel};

/// Numeric host filter for Tool review, analogous to a logging threshold.
/// Tool severities are Low=10, Medium=50 and High=90. A threshold of 100
/// disables Tool review; 0 reviews every Tool.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ToolApprovalPolicy {
    review_level: u8,
}

impl ToolApprovalPolicy {
    pub const DEFAULT_REVIEW_LEVEL: u8 = 50;
    pub const DISABLED_REVIEW_LEVEL: u8 = 100;

    #[must_use]
    pub const fn new(review_level: u8) -> Self {
        Self { review_level }
    }

    #[must_use]
    pub const fn review_level(self) -> u8 {
        self.review_level
    }

    #[must_use]
    pub const fn requires_review(self, risk_level: ToolRiskLevel) -> bool {
        self.review_level < Self::DISABLED_REVIEW_LEVEL
            && risk_level.review_level() >= self.review_level
    }
}

impl Default for ToolApprovalPolicy {
    fn default() -> Self {
        Self::new(Self::DEFAULT_REVIEW_LEVEL)
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numeric_review_threshold_filters_tool_risk() {
        let default = ToolApprovalPolicy::default();
        assert!(!default.requires_review(ToolRiskLevel::Low));
        assert!(default.requires_review(ToolRiskLevel::Medium));
        assert!(default.requires_review(ToolRiskLevel::High));

        let high_only = ToolApprovalPolicy::new(90);
        assert!(!high_only.requires_review(ToolRiskLevel::Medium));
        assert!(high_only.requires_review(ToolRiskLevel::High));

        let all = ToolApprovalPolicy::new(0);
        assert!(all.requires_review(ToolRiskLevel::Low));

        let disabled = ToolApprovalPolicy::new(100);
        assert!(!disabled.requires_review(ToolRiskLevel::High));
    }
}
