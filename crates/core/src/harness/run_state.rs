use std::{future::Future, pin::Pin};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::harness::{
    ApprovalId, ApprovalResolution, FinishReason, OutputChannel, RunEvent, RunEventKind, RunId,
    TokenUsage, ToolErrorCategory, ToolRiskLevel,
};

/// Schema version of the persisted single-run projection.
pub const RUN_STATE_SCHEMA_VERSION: u32 = 1;

/// Durable lifecycle of one independent run.
///
/// A run deliberately has no session or conversation identity. Cross-run state
/// belongs to the future session layer rather than this contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Accepted,
    Running,
    WaitingApproval,
    ExecutingTool,
    Completed,
    Failed,
    Cancelled,
}

impl RunStatus {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

/// Stable failure details retained after a run reaches `failed`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunFailure {
    pub code: String,
    pub message: String,
    pub retryable: bool,
}

/// Approval state projected from approval events for one run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunApprovalState {
    pub approval_id: ApprovalId,
    pub call_id: String,
    pub tool_name: String,
    pub risk_level: ToolRiskLevel,
    pub arguments: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolution: Option<ApprovalResolution>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunToolStatus {
    Preparing,
    WaitingApproval,
    Ready,
    Executing,
    Completed,
    Failed,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunToolFailure {
    pub code: String,
    pub message: String,
    #[serde(default)]
    pub category: ToolErrorCategory,
    pub retryable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
}

/// Query-friendly state of a tool call within one run. Raw argument deltas
/// remain in the event log while this projection captures its current phase.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunToolState {
    pub call_id: String,
    pub name: String,
    pub arguments_json: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub arguments: Option<Value>,
    pub status: RunToolStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure: Option<RunToolFailure>,
}

/// Durable, query-friendly projection of a single run's event stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunSnapshot {
    pub schema_version: u32,
    pub run_id: RunId,
    pub status: RunStatus,
    pub input: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_manifest: Option<Value>,
    pub output: String,
    pub reasoning: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<TokenUsage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finish_reason: Option<FinishReason>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure: Option<RunFailure>,
    pub approvals: Vec<RunApprovalState>,
    #[serde(default)]
    pub tools: Vec<RunToolState>,
    pub last_seq: u64,
    pub revision: u64,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

impl RunSnapshot {
    #[must_use]
    pub fn new(run_id: RunId, input: impl Into<String>, now_ms: i64) -> Self {
        Self {
            schema_version: RUN_STATE_SCHEMA_VERSION,
            run_id,
            status: RunStatus::Accepted,
            input: input.into(),
            execution_manifest: None,
            output: String::new(),
            reasoning: String::new(),
            usage: None,
            finish_reason: None,
            failure: None,
            approvals: Vec::new(),
            tools: Vec::new(),
            last_seq: 0,
            revision: 0,
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
        }
    }

    #[must_use]
    pub fn with_execution_manifest(mut self, manifest: Value) -> Self {
        self.execution_manifest = Some(manifest);
        self
    }

    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        self.status.is_terminal()
    }

    /// Applies exactly one next event to the projection.
    ///
    /// Sequence validation lives here so every store adapter implements the
    /// same state machine. Store implementations are additionally responsible
    /// for making event insertion and snapshot replacement atomic.
    pub fn apply(&mut self, event: &RunEvent, now_ms: i64) -> Result<(), RunStateError> {
        if event.run_id != self.run_id {
            return Err(RunStateError::RunIdMismatch {
                expected: self.run_id,
                actual: event.run_id,
            });
        }
        if self.is_terminal() {
            return Err(RunStateError::AlreadyTerminal(self.status));
        }

        let expected = self.last_seq.saturating_add(1);
        if event.seq != expected {
            return Err(RunStateError::SequenceConflict {
                expected,
                actual: event.seq,
            });
        }

        match &event.kind {
            RunEventKind::RunStarted => {
                if self.last_seq != 0 || self.status != RunStatus::Accepted {
                    return Err(RunStateError::InvalidTransition {
                        status: self.status,
                        event: event.kind.event_name(),
                    });
                }
                self.status = RunStatus::Running;
            }
            RunEventKind::OutputDelta { channel, delta } => {
                self.require_started(event)?;
                match channel {
                    OutputChannel::AssistantText => self.output.push_str(delta),
                    OutputChannel::AssistantReasoning => self.reasoning.push_str(delta),
                }
            }
            RunEventKind::UsageUpdated { usage } => {
                self.require_started(event)?;
                self.usage = Some(*usage);
            }
            RunEventKind::ToolCallStarted { call_id, name } => {
                self.require_started(event)?;
                if self.tools.iter().any(|tool| tool.call_id == *call_id) {
                    return Err(RunStateError::DuplicateToolCall(call_id.clone()));
                }
                self.tools.push(RunToolState {
                    call_id: call_id.clone(),
                    name: name.clone(),
                    arguments_json: String::new(),
                    arguments: None,
                    status: RunToolStatus::Preparing,
                    output: None,
                    failure: None,
                });
                self.status = RunStatus::Running;
            }
            RunEventKind::ToolCallArgumentsDelta { call_id, delta } => {
                self.require_started(event)?;
                self.tool_mut(call_id)?.arguments_json.push_str(delta);
                self.status = RunStatus::Running;
            }
            RunEventKind::ApprovalRequested {
                approval_id,
                call_id,
                tool_name,
                risk_level,
                arguments,
            } => {
                self.require_started(event)?;
                if self
                    .approvals
                    .iter()
                    .any(|approval| approval.approval_id == *approval_id)
                {
                    return Err(RunStateError::DuplicateApproval(*approval_id));
                }
                self.approvals.push(RunApprovalState {
                    approval_id: *approval_id,
                    call_id: call_id.clone(),
                    tool_name: tool_name.clone(),
                    risk_level: *risk_level,
                    arguments: arguments.clone(),
                    resolution: None,
                });
                let tool = self.tool_mut(call_id)?;
                tool.arguments = Some(arguments.clone());
                tool.status = RunToolStatus::WaitingApproval;
                self.status = RunStatus::WaitingApproval;
            }
            RunEventKind::ApprovalResolved {
                approval_id,
                call_id,
                resolution,
            } => {
                self.require_started(event)?;
                let Some(approval) = self
                    .approvals
                    .iter_mut()
                    .find(|approval| approval.approval_id == *approval_id)
                else {
                    return Err(RunStateError::ApprovalNotFound(*approval_id));
                };
                if approval.resolution.is_some() {
                    return Err(RunStateError::ApprovalAlreadyResolved(*approval_id));
                }
                approval.resolution = Some(resolution.clone());
                self.tool_mut(call_id)?.status =
                    if resolution.decision == crate::harness::ApprovalDecision::Deny {
                        RunToolStatus::Rejected
                    } else {
                        RunToolStatus::Ready
                    };
                self.status = if self
                    .approvals
                    .iter()
                    .any(|approval| approval.resolution.is_none())
                {
                    RunStatus::WaitingApproval
                } else {
                    RunStatus::Running
                };
            }
            RunEventKind::ToolExecutionStarted { call_id, arguments } => {
                self.require_started(event)?;
                let tool = self.tool_mut(call_id)?;
                tool.arguments = Some(arguments.clone());
                tool.status = RunToolStatus::Executing;
                self.status = RunStatus::ExecutingTool;
            }
            RunEventKind::ToolExecutionCompleted { call_id, output } => {
                self.require_started(event)?;
                let tool = self.tool_mut(call_id)?;
                tool.output = Some(output.clone());
                tool.status = RunToolStatus::Completed;
                self.status = RunStatus::Running;
            }
            RunEventKind::ToolExecutionFailed {
                call_id,
                code,
                message,
                category,
                retryable,
                retry_after_ms,
            } => {
                self.require_started(event)?;
                let tool = self.tool_mut(call_id)?;
                tool.failure = Some(RunToolFailure {
                    code: code.clone(),
                    message: message.clone(),
                    category: *category,
                    retryable: *retryable,
                    retry_after_ms: *retry_after_ms,
                });
                tool.status = RunToolStatus::Failed;
                self.status = RunStatus::Running;
            }
            RunEventKind::RunCancelled => {
                self.status = RunStatus::Cancelled;
            }
            RunEventKind::RunCompleted { finish_reason } => {
                self.require_started(event)?;
                self.finish_reason = Some(*finish_reason);
                self.status = RunStatus::Completed;
            }
            RunEventKind::RunFailed {
                code,
                message,
                retryable,
            } => {
                // A host may persist an accepted run and then terminate before
                // polling RunStarted. Recovery must still be able to close it.
                self.failure = Some(RunFailure {
                    code: code.clone(),
                    message: message.clone(),
                    retryable: *retryable,
                });
                self.status = RunStatus::Failed;
            }
        }

        self.last_seq = event.seq;
        self.revision = self.revision.saturating_add(1);
        self.updated_at_ms = now_ms.max(self.updated_at_ms);
        Ok(())
    }

    fn require_started(&self, event: &RunEvent) -> Result<(), RunStateError> {
        if self.status == RunStatus::Accepted {
            return Err(RunStateError::InvalidTransition {
                status: self.status,
                event: event.kind.event_name(),
            });
        }
        Ok(())
    }

    fn tool_mut(&mut self, call_id: &str) -> Result<&mut RunToolState, RunStateError> {
        self.tools
            .iter_mut()
            .find(|tool| tool.call_id == call_id)
            .ok_or_else(|| RunStateError::ToolCallNotFound(call_id.into()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum RunStateError {
    #[error("run id mismatch: expected {expected}, got {actual}")]
    RunIdMismatch { expected: RunId, actual: RunId },
    #[error("run is already terminal with status {0:?}")]
    AlreadyTerminal(RunStatus),
    #[error("event sequence conflict: expected {expected}, got {actual}")]
    SequenceConflict { expected: u64, actual: u64 },
    #[error("event {event} is invalid while run is {status:?}")]
    InvalidTransition {
        status: RunStatus,
        event: &'static str,
    },
    #[error("approval {0} already exists")]
    DuplicateApproval(ApprovalId),
    #[error("approval {0} does not exist")]
    ApprovalNotFound(ApprovalId),
    #[error("approval {0} is already resolved")]
    ApprovalAlreadyResolved(ApprovalId),
    #[error("tool call {0} already exists")]
    DuplicateToolCall(String),
    #[error("tool call {0} does not exist")]
    ToolCallNotFound(String),
}

/// Error vocabulary shared by all durable run store adapters.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum RunStoreError {
    #[error("run {0} was not found")]
    NotFound(RunId),
    #[error("run {0} already exists")]
    AlreadyExists(RunId),
    #[error("event {seq} for run {run_id} conflicts with the persisted event")]
    EventConflict { run_id: RunId, seq: u64 },
    #[error(transparent)]
    State(#[from] RunStateError),
    #[error("run store backend error: {0}")]
    Backend(String),
}

impl RunStoreError {
    #[must_use]
    pub fn backend(message: impl Into<String>) -> Self {
        Self::Backend(message.into())
    }
}

pub type RunStoreFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, RunStoreError>> + Send + 'a>>;

/// Persistence port for one-run snapshots and their append-only event logs.
///
/// `append_event` must atomically persist the event and its projected snapshot.
/// Re-appending an identical `(run_id, seq)` is idempotent; different payloads
/// at that key must return `EventConflict`.
pub trait RunStore: Send + Sync + 'static {
    fn create_run(&self, snapshot: RunSnapshot) -> RunStoreFuture<'_, RunSnapshot>;

    fn get_run(&self, run_id: RunId) -> RunStoreFuture<'_, Option<RunSnapshot>>;

    fn set_execution_manifest(
        &self,
        run_id: RunId,
        manifest: Value,
    ) -> RunStoreFuture<'_, RunSnapshot>;

    fn append_event(&self, event: RunEvent, observed_at_ms: i64)
    -> RunStoreFuture<'_, RunSnapshot>;

    fn events_after(
        &self,
        run_id: RunId,
        after_seq: u64,
        limit: usize,
    ) -> RunStoreFuture<'_, Vec<RunEvent>>;

    fn unfinished_runs(&self) -> RunStoreFuture<'_, Vec<RunSnapshot>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn projects_a_complete_run() {
        let run_id = RunId::new();
        let mut snapshot = RunSnapshot::new(run_id, "hello", 10);
        snapshot
            .apply(&RunEvent::new(run_id, 1, RunEventKind::RunStarted), 11)
            .expect("run should start");
        snapshot
            .apply(
                &RunEvent::new(
                    run_id,
                    2,
                    RunEventKind::OutputDelta {
                        channel: OutputChannel::AssistantReasoning,
                        delta: "think".into(),
                    },
                ),
                12,
            )
            .expect("reasoning should apply");
        snapshot
            .apply(
                &RunEvent::new(
                    run_id,
                    3,
                    RunEventKind::OutputDelta {
                        channel: OutputChannel::AssistantText,
                        delta: "answer".into(),
                    },
                ),
                13,
            )
            .expect("output should apply");
        snapshot
            .apply(
                &RunEvent::new(
                    run_id,
                    4,
                    RunEventKind::RunCompleted {
                        finish_reason: FinishReason::Stop,
                    },
                ),
                14,
            )
            .expect("run should complete");

        assert_eq!(snapshot.status, RunStatus::Completed);
        assert_eq!(snapshot.output, "answer");
        assert_eq!(snapshot.reasoning, "think");
        assert_eq!(snapshot.last_seq, 4);
        assert_eq!(snapshot.revision, 4);
        assert!(snapshot.is_terminal());
    }

    #[test]
    fn allows_recovery_to_fail_an_unstarted_persisted_run() {
        let run_id = RunId::new();
        let mut snapshot = RunSnapshot::new(run_id, "hello", 10);
        snapshot
            .apply(
                &RunEvent::new(
                    run_id,
                    1,
                    RunEventKind::RunFailed {
                        code: "run_interrupted".into(),
                        message: "server restarted".into(),
                        retryable: true,
                    },
                ),
                20,
            )
            .expect("accepted run should be recoverable");

        assert_eq!(snapshot.status, RunStatus::Failed);
        assert_eq!(
            snapshot
                .failure
                .as_ref()
                .map(|failure| failure.code.as_str()),
            Some("run_interrupted")
        );
    }

    #[test]
    fn rejects_sequence_gaps() {
        let run_id = RunId::new();
        let mut snapshot = RunSnapshot::new(run_id, "hello", 10);
        let error = snapshot
            .apply(&RunEvent::new(run_id, 2, RunEventKind::RunStarted), 11)
            .expect_err("sequence gap must fail");

        assert!(matches!(
            error,
            RunStateError::SequenceConflict {
                expected: 1,
                actual: 2
            }
        ));
    }

    #[test]
    fn projects_tool_and_approval_lifecycle() {
        let run_id = RunId::new();
        let approval_id = ApprovalId::new();
        let mut snapshot = RunSnapshot::new(run_id, "use a tool", 1);
        let events = [
            RunEvent::new(run_id, 1, RunEventKind::RunStarted),
            RunEvent::new(
                run_id,
                2,
                RunEventKind::ToolCallStarted {
                    call_id: "call_1".into(),
                    name: "demo".into(),
                },
            ),
            RunEvent::new(
                run_id,
                3,
                RunEventKind::ToolCallArgumentsDelta {
                    call_id: "call_1".into(),
                    delta: "{\"value\":1}".into(),
                },
            ),
            RunEvent::new(
                run_id,
                4,
                RunEventKind::ApprovalRequested {
                    approval_id,
                    call_id: "call_1".into(),
                    tool_name: "demo".into(),
                    risk_level: ToolRiskLevel::High,
                    arguments: serde_json::json!({"value": 1}),
                },
            ),
            RunEvent::new(
                run_id,
                5,
                RunEventKind::ApprovalResolved {
                    approval_id,
                    call_id: "call_1".into(),
                    resolution: ApprovalResolution::allow_once(),
                },
            ),
            RunEvent::new(
                run_id,
                6,
                RunEventKind::ToolExecutionStarted {
                    call_id: "call_1".into(),
                    arguments: serde_json::json!({"value": 1}),
                },
            ),
            RunEvent::new(
                run_id,
                7,
                RunEventKind::ToolExecutionCompleted {
                    call_id: "call_1".into(),
                    output: "done".into(),
                },
            ),
        ];
        for (index, event) in events.iter().enumerate() {
            snapshot
                .apply(event, i64::try_from(index + 2).expect("small timestamp"))
                .expect("tool event should apply");
        }

        assert_eq!(snapshot.status, RunStatus::Running);
        assert_eq!(snapshot.approvals.len(), 1);
        assert_eq!(
            snapshot.approvals[0]
                .resolution
                .as_ref()
                .map(|resolution| resolution.decision),
            Some(crate::harness::ApprovalDecision::AllowOnce)
        );
        assert_eq!(snapshot.tools.len(), 1);
        assert_eq!(snapshot.tools[0].status, RunToolStatus::Completed);
        assert_eq!(snapshot.tools[0].output.as_deref(), Some("done"));
    }
}
