use std::{pin::Pin, time::Duration};

use async_stream::stream;
use futures_core::Stream;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::harness::{
    ApprovalId, ApprovalResolution, FinishReason, RunCancellation, RunId, TokenUsage,
    ToolErrorCategory, ToolRiskLevel,
};

/// Pull-based event stream returned by an Agent implementation.
pub type AgentEventStream = Pin<Box<dyn Stream<Item = AgentEvent> + Send + 'static>>;

/// Validated, sequenced event stream exposed by the Harness to a host.
pub type RunEventStream = Pin<Box<dyn Stream<Item = RunEvent> + Send + 'static>>;

/// Semantic events produced by an Agent. Run identity, sequence numbers, and
/// terminal validation are owned by the Harness event engine.
#[derive(Debug)]
#[non_exhaustive]
pub enum AgentEvent {
    OutputDelta {
        channel: OutputChannel,
        delta: String,
    },
    UsageUpdated {
        usage: TokenUsage,
    },
    ToolCallStarted {
        call_id: String,
        name: String,
    },
    ToolCallArgumentsDelta {
        call_id: String,
        delta: String,
    },
    ApprovalRequested {
        approval_id: ApprovalId,
        call_id: String,
        tool_name: String,
        risk_level: ToolRiskLevel,
        arguments: Value,
    },
    ApprovalResolved {
        approval_id: ApprovalId,
        call_id: String,
        resolution: ApprovalResolution,
    },
    ToolExecutionStarted {
        call_id: String,
        arguments: Value,
    },
    ToolExecutionCompleted {
        call_id: String,
        output: String,
    },
    ToolExecutionFailed {
        call_id: String,
        code: String,
        message: String,
        category: ToolErrorCategory,
        retryable: bool,
        retry_after_ms: Option<u64>,
    },
    Cancelled,
    Completed {
        finish_reason: FinishReason,
    },
    Failed {
        code: String,
        message: String,
        retryable: bool,
    },
}

impl AgentEvent {
    #[must_use]
    pub fn text_delta(delta: impl Into<String>) -> Self {
        Self::OutputDelta {
            channel: OutputChannel::AssistantText,
            delta: delta.into(),
        }
    }

    #[must_use]
    pub const fn completed(finish_reason: FinishReason) -> Self {
        Self::Completed { finish_reason }
    }

    #[must_use]
    pub fn failed(code: impl Into<String>, message: impl Into<String>, retryable: bool) -> Self {
        Self::Failed {
            code: code.into(),
            message: message.into(),
            retryable,
        }
    }

    const fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Cancelled | Self::Completed { .. } | Self::Failed { .. }
        )
    }
}

/// Stable output channels. Provider-specific fields never become channel names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum OutputChannel {
    AssistantText,
    AssistantReasoning,
}

/// Public event envelope shared by HTTP, CLI, desktop, persistence, and future
/// cross-process transports.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunEvent {
    pub run_id: RunId,
    pub seq: u64,
    #[serde(flatten)]
    pub kind: RunEventKind,
}

/// Facts emitted during one run. This is transport-neutral; SSE event names are
/// derived at the Gateway boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[non_exhaustive]
pub enum RunEventKind {
    RunStarted,
    OutputDelta {
        channel: OutputChannel,
        delta: String,
    },
    UsageUpdated {
        usage: TokenUsage,
    },
    ToolCallStarted {
        call_id: String,
        name: String,
    },
    ToolCallArgumentsDelta {
        call_id: String,
        delta: String,
    },
    ApprovalRequested {
        approval_id: ApprovalId,
        call_id: String,
        tool_name: String,
        risk_level: ToolRiskLevel,
        arguments: Value,
    },
    ApprovalResolved {
        approval_id: ApprovalId,
        call_id: String,
        resolution: ApprovalResolution,
    },
    ToolExecutionStarted {
        call_id: String,
        arguments: Value,
    },
    ToolExecutionCompleted {
        call_id: String,
        output: String,
    },
    ToolExecutionFailed {
        call_id: String,
        code: String,
        message: String,
        #[serde(default)]
        category: ToolErrorCategory,
        retryable: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        retry_after_ms: Option<u64>,
    },
    RunCancelled,
    RunCompleted {
        finish_reason: FinishReason,
    },
    RunFailed {
        code: String,
        message: String,
        retryable: bool,
    },
}

impl RunEventKind {
    #[must_use]
    pub const fn event_name(&self) -> &'static str {
        match self {
            Self::RunStarted => "run_started",
            Self::OutputDelta { .. } => "output_delta",
            Self::UsageUpdated { .. } => "usage_updated",
            Self::ToolCallStarted { .. } => "tool_call_started",
            Self::ToolCallArgumentsDelta { .. } => "tool_call_arguments_delta",
            Self::ApprovalRequested { .. } => "approval_requested",
            Self::ApprovalResolved { .. } => "approval_resolved",
            Self::ToolExecutionStarted { .. } => "tool_execution_started",
            Self::ToolExecutionCompleted { .. } => "tool_execution_completed",
            Self::ToolExecutionFailed { .. } => "tool_execution_failed",
            Self::RunCancelled => "run_cancelled",
            Self::RunCompleted { .. } => "run_completed",
            Self::RunFailed { .. } => "run_failed",
        }
    }

    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::RunCancelled | Self::RunCompleted { .. } | Self::RunFailed { .. }
        )
    }
}

impl RunEvent {
    /// Creates an event envelope. This is public so hosts can append lifecycle
    /// events such as `run_interrupted` during durable startup recovery.
    #[must_use]
    pub const fn new(run_id: RunId, seq: u64, kind: RunEventKind) -> Self {
        Self { run_id, seq, kind }
    }
}

/// Adds the public envelope, assigns strictly increasing sequence numbers, and
/// guarantees exactly one terminal event even when an Agent ends unexpectedly.
pub(crate) fn run_event_stream(
    run_id: RunId,
    mut events: AgentEventStream,
    cancellation: RunCancellation,
    run_timeout: Duration,
) -> RunEventStream {
    Box::pin(stream! {
        let mut seq = 1_u64;
        yield RunEvent::new(run_id, seq, RunEventKind::RunStarted);
        let deadline = tokio::time::Instant::now() + run_timeout;

        loop {
            let poll = tokio::select! {
                biased;
                () = cancellation.cancelled() => RunPoll::Cancelled,
                result = tokio::time::timeout_at(deadline, events.next()) => match result {
                    Ok(event) => RunPoll::Event(event),
                    Err(_) => RunPoll::TimedOut,
                },
            };
            let event = match poll {
                RunPoll::Cancelled => {
                    seq += 1;
                    yield RunEvent::new(run_id, seq, RunEventKind::RunCancelled);
                    return;
                }
                RunPoll::TimedOut => {
                    cancellation.cancel();
                    seq += 1;
                    yield RunEvent::new(
                        run_id,
                        seq,
                        RunEventKind::RunFailed {
                            code: "run_timeout".into(),
                            message: "run exceeded the configured deadline".into(),
                            retryable: false,
                        },
                    );
                    return;
                }
                RunPoll::Event(Some(event)) => event,
                RunPoll::Event(None) => break,
            };

            seq += 1;
            let terminal = event.is_terminal();
            let kind = match event {
                AgentEvent::OutputDelta { channel, delta } => {
                    RunEventKind::OutputDelta { channel, delta }
                }
                AgentEvent::UsageUpdated { usage } => RunEventKind::UsageUpdated { usage },
                AgentEvent::ToolCallStarted { call_id, name } => {
                    RunEventKind::ToolCallStarted { call_id, name }
                }
                AgentEvent::ToolCallArgumentsDelta { call_id, delta } => {
                    RunEventKind::ToolCallArgumentsDelta { call_id, delta }
                }
                AgentEvent::ApprovalRequested {
                    approval_id,
                    call_id,
                    tool_name,
                    risk_level,
                    arguments,
                } => RunEventKind::ApprovalRequested {
                    approval_id,
                    call_id,
                    tool_name,
                    risk_level,
                    arguments,
                },
                AgentEvent::ApprovalResolved {
                    approval_id,
                    call_id,
                    resolution,
                } => RunEventKind::ApprovalResolved {
                    approval_id,
                    call_id,
                    resolution,
                },
                AgentEvent::ToolExecutionStarted { call_id, arguments } => {
                    RunEventKind::ToolExecutionStarted { call_id, arguments }
                }
                AgentEvent::ToolExecutionCompleted { call_id, output } => {
                    RunEventKind::ToolExecutionCompleted { call_id, output }
                }
                AgentEvent::ToolExecutionFailed {
                    call_id,
                    code,
                    message,
                    category,
                    retryable,
                    retry_after_ms,
                } => RunEventKind::ToolExecutionFailed {
                    call_id,
                    code,
                    message,
                    category,
                    retryable,
                    retry_after_ms,
                },
                AgentEvent::Cancelled => RunEventKind::RunCancelled,
                AgentEvent::Completed { finish_reason } => {
                    RunEventKind::RunCompleted { finish_reason }
                }
                AgentEvent::Failed { code, message, retryable } => {
                    RunEventKind::RunFailed { code, message, retryable }
                }
            };
            yield RunEvent::new(run_id, seq, kind);

            if terminal {
                return;
            }
        }

        seq += 1;
        yield RunEvent::new(
            run_id,
            seq,
            RunEventKind::RunFailed {
                code: "agent_protocol_violation".into(),
                message: "agent event stream ended without a terminal event".into(),
                retryable: false,
            },
        );
    })
}

enum RunPoll {
    Cancelled,
    TimedOut,
    Event(Option<AgentEvent>),
}

#[cfg(test)]
mod tests {
    use futures_util::stream;

    use super::*;

    #[tokio::test]
    async fn sequences_events_and_preserves_one_terminal_event() {
        let run_id = RunId::new();
        let agent_events: AgentEventStream = Box::pin(stream::iter(vec![
            AgentEvent::text_delta("hello"),
            AgentEvent::completed(FinishReason::Stop),
            AgentEvent::text_delta("ignored"),
        ]));

        let events: Vec<_> = run_event_stream(
            run_id,
            agent_events,
            RunCancellation::new(),
            Duration::from_secs(5),
        )
        .collect()
        .await;

        assert_eq!(events.len(), 3);
        assert_eq!(events[0].seq, 1);
        assert_eq!(events[1].seq, 2);
        assert_eq!(events[2].seq, 3);
        assert!(events[2].kind.is_terminal());
        assert_eq!(
            events
                .iter()
                .filter(|event| event.kind.is_terminal())
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn synthesizes_a_failure_when_agent_omits_a_terminal_event() {
        let run_id = RunId::new();
        let agent_events: AgentEventStream =
            Box::pin(stream::iter(vec![AgentEvent::text_delta("partial")]));

        let events: Vec<_> = run_event_stream(
            run_id,
            agent_events,
            RunCancellation::new(),
            Duration::from_secs(5),
        )
        .collect()
        .await;

        assert!(matches!(
            &events.last().expect("event should exist").kind,
            RunEventKind::RunFailed { code, .. } if code == "agent_protocol_violation"
        ));
    }

    #[tokio::test]
    async fn cancellation_produces_a_unique_cancelled_terminal_event() {
        let run_id = RunId::new();
        let cancellation = RunCancellation::new();
        let agent_events: AgentEventStream = Box::pin(stream::pending());
        let mut events = run_event_stream(
            run_id,
            agent_events,
            cancellation.clone(),
            Duration::from_secs(5),
        );

        assert!(matches!(
            events.next().await.map(|event| event.kind),
            Some(RunEventKind::RunStarted)
        ));
        cancellation.cancel();
        assert!(matches!(
            events.next().await.map(|event| event.kind),
            Some(RunEventKind::RunCancelled)
        ));
        assert!(events.next().await.is_none());
    }

    #[tokio::test]
    async fn run_deadline_produces_a_unique_timeout_failure() {
        let events: Vec<_> = run_event_stream(
            RunId::new(),
            Box::pin(stream::pending()),
            RunCancellation::new(),
            Duration::from_millis(10),
        )
        .collect()
        .await;

        assert!(matches!(
            events.last().map(|event| &event.kind),
            Some(RunEventKind::RunFailed { code, .. }) if code == "run_timeout"
        ));
        assert_eq!(
            events
                .iter()
                .filter(|event| event.kind.is_terminal())
                .count(),
            1
        );
    }
}
