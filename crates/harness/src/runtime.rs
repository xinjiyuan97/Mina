use std::{sync::Arc, time::Duration};

use agent_core::harness::{
    Agent, AgentMetadata, ModelAttachment, ModelMessage, OutputChannel, RunCancellation,
    RunEventKind, RunEventStream, RunId, RunRequest, RunResponse,
};
use futures_util::StreamExt;
use thiserror::Error;

/// Host-supplied policy for one run.
#[derive(Debug, Clone, Default)]
pub struct RunOptions {
    pub allowed_tools: Option<Vec<String>>,
    pub allow_run_adf: bool,
    pub max_steps: Option<u32>,
}

/// Portable upper bound accepted by the Harness contract.
pub const MAX_RUN_STEPS: u32 = 100;

#[derive(Debug, Error)]
pub enum HarnessError {
    #[error("input must not be empty")]
    InvalidInput,

    #[error("max_steps must be between 1 and {MAX_RUN_STEPS}, got {requested}")]
    InvalidMaxSteps { requested: u32 },

    #[error("agent failed: {message}")]
    Agent {
        code: String,
        message: String,
        retryable: bool,
    },
}

impl HarnessError {
    #[must_use]
    pub fn code(&self) -> &str {
        match self {
            Self::InvalidInput => "invalid_input",
            Self::InvalidMaxSteps { .. } => "invalid_max_steps",
            Self::Agent { code, .. } => code,
        }
    }

    #[must_use]
    pub fn agent(code: impl Into<String>, message: impl Into<String>, retryable: bool) -> Self {
        Self::Agent {
            code: code.into(),
            message: message.into(),
            retryable,
        }
    }

    #[must_use]
    pub const fn retryable(&self) -> bool {
        match self {
            Self::InvalidInput | Self::InvalidMaxSteps { .. } => false,
            Self::Agent { retryable, .. } => *retryable,
        }
    }
}

/// Owns an agent implementation and exposes the host-facing execution API.
#[derive(Debug)]
pub struct Harness<A> {
    agent: Arc<A>,
    run_timeout: Duration,
}

const DEFAULT_RUN_TIMEOUT: Duration = Duration::from_secs(300);

/// A newly accepted run plus the host-owned cancellation handle.
pub struct RunExecution {
    pub run_id: RunId,
    pub cancellation: RunCancellation,
    pub events: RunEventStream,
}

impl<A> Clone for Harness<A> {
    fn clone(&self) -> Self {
        Self {
            agent: Arc::clone(&self.agent),
            run_timeout: self.run_timeout,
        }
    }
}

impl<A> Harness<A>
where
    A: Agent,
{
    #[must_use]
    pub fn new(agent: A) -> Self {
        Self {
            agent: Arc::new(agent),
            run_timeout: DEFAULT_RUN_TIMEOUT,
        }
    }

    #[must_use]
    pub const fn with_run_timeout(mut self, timeout: Duration) -> Self {
        self.run_timeout = timeout;
        self
    }

    #[must_use]
    pub fn metadata(&self) -> AgentMetadata {
        self.agent.metadata()
    }

    #[must_use]
    pub fn agent_handle(&self) -> Arc<A> {
        Arc::clone(&self.agent)
    }

    pub fn start(&self, input: impl Into<String>) -> Result<RunExecution, HarnessError> {
        self.start_with_context(RunId::new(), input, Vec::new(), None)
    }

    pub fn start_with_run_id(
        &self,
        run_id: RunId,
        input: impl Into<String>,
        prior_messages: Vec<ModelMessage>,
    ) -> Result<RunExecution, HarnessError> {
        self.start_with_context(run_id, input, prior_messages, None)
    }

    pub fn start_with_context(
        &self,
        run_id: RunId,
        input: impl Into<String>,
        prior_messages: Vec<ModelMessage>,
        allowed_tools: Option<Vec<String>>,
    ) -> Result<RunExecution, HarnessError> {
        self.start_with_options(
            run_id,
            input,
            prior_messages,
            RunOptions {
                allowed_tools,
                allow_run_adf: false,
                max_steps: None,
            },
        )
    }

    pub fn start_with_options(
        &self,
        run_id: RunId,
        input: impl Into<String>,
        prior_messages: Vec<ModelMessage>,
        options: RunOptions,
    ) -> Result<RunExecution, HarnessError> {
        self.start_with_options_and_attachments(run_id, input, Vec::new(), prior_messages, options)
    }

    pub fn start_with_options_and_attachments(
        &self,
        run_id: RunId,
        input: impl Into<String>,
        attachments: Vec<ModelAttachment>,
        prior_messages: Vec<ModelMessage>,
        options: RunOptions,
    ) -> Result<RunExecution, HarnessError> {
        let input = input.into();
        if input.trim().is_empty() && attachments.is_empty() {
            return Err(HarnessError::InvalidInput);
        }
        if let Some(requested) = options.max_steps
            && !(1..=MAX_RUN_STEPS).contains(&requested)
        {
            return Err(HarnessError::InvalidMaxSteps { requested });
        }

        let cancellation = RunCancellation::new();
        let events = self.agent.run(RunRequest {
            run_id,
            input,
            attachments,
            prior_messages,
            allowed_tools: options.allowed_tools,
            allow_run_adf: options.allow_run_adf,
            max_steps: options.max_steps,
            cancellation: cancellation.clone(),
        });
        Ok(RunExecution {
            run_id,
            cancellation: cancellation.clone(),
            events: agent_core::harness::run_event_stream(
                run_id,
                events,
                cancellation,
                self.run_timeout,
            ),
        })
    }

    pub fn stream(&self, input: impl Into<String>) -> Result<RunEventStream, HarnessError> {
        self.start(input).map(|execution| execution.events)
    }

    pub async fn execute(&self, input: impl Into<String>) -> Result<RunResponse, HarnessError> {
        self.execute_with_options(input, RunOptions::default())
            .await
    }

    pub async fn execute_with_options(
        &self,
        input: impl Into<String>,
        options: RunOptions,
    ) -> Result<RunResponse, HarnessError> {
        let mut events = self
            .start_with_options(RunId::new(), input, Vec::new(), options)?
            .events;
        let mut output = String::new();
        let mut usage = None;

        while let Some(event) = events.next().await {
            match event.kind {
                RunEventKind::RunStarted
                | RunEventKind::RunWaiting { .. }
                | RunEventKind::RunResumed { .. } => {}
                RunEventKind::OutputDelta {
                    channel: OutputChannel::AssistantText,
                    delta,
                } => output.push_str(&delta),
                RunEventKind::OutputDelta { .. } => {}
                RunEventKind::UsageUpdated { usage: next } => usage = Some(next),
                RunEventKind::ToolSetUpdated { .. }
                | RunEventKind::ToolCallStarted { .. }
                | RunEventKind::ToolCallArgumentsDelta { .. }
                | RunEventKind::ApprovalRequested { .. }
                | RunEventKind::ApprovalResolved { .. }
                | RunEventKind::ToolExecutionStarted { .. }
                | RunEventKind::ToolExecutionCompleted { .. }
                | RunEventKind::ToolExecutionFailed { .. } => {}
                RunEventKind::RunCancelled => {
                    return Err(HarnessError::agent(
                        "run_cancelled",
                        "run was cancelled",
                        false,
                    ));
                }
                RunEventKind::RunCompleted { finish_reason } => {
                    return Ok(RunResponse {
                        run_id: event.run_id,
                        output,
                        finish_reason,
                        usage,
                    });
                }
                RunEventKind::RunFailed {
                    code,
                    message,
                    retryable,
                } => return Err(HarnessError::agent(code, message, retryable)),
                _ => {}
            }
        }

        Err(HarnessError::agent(
            "agent_protocol_violation",
            "run event stream ended without a terminal event",
            false,
        ))
    }
}

#[cfg(test)]
mod tests {
    use agent_core::harness::{
        AgentEvent, AgentEventStream, AgentMetadata, FinishReason, RunRequest,
    };

    use super::*;

    #[derive(Debug)]
    struct TestAgent;

    impl Agent for TestAgent {
        fn metadata(&self) -> AgentMetadata {
            AgentMetadata::new("test", "0.1.0")
        }

        fn run(&self, request: RunRequest) -> AgentEventStream {
            Box::pin(futures_util::stream::iter(vec![
                AgentEvent::text_delta(request.input.to_uppercase()),
                AgentEvent::completed(FinishReason::Stop),
            ]))
        }
    }

    #[tokio::test]
    async fn executes_an_agent() {
        let response = Harness::new(TestAgent)
            .execute("hello")
            .await
            .expect("run should succeed");
        assert_eq!(response.output, "HELLO");
    }
}
