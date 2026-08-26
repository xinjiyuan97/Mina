use std::time::Duration;

use async_stream::stream;
use futures_util::StreamExt;

use crate::harness::{
    Agent, AgentEvent, AgentEventStream, AgentMetadata, ModelEvent, ModelMessage, ModelPort,
    ModelRequest, RunRequest,
};

/// Minimal Agent implementation for one independent user task.
///
/// It deliberately performs exactly one model invocation and has no session,
/// tool loop, memory, or persistence. Those capabilities can be added around
/// the same `ModelPort` without leaking provider details into the Agent.
pub struct SingleTurnAgent<P> {
    provider: P,
    model: String,
    system_prompt: String,
    max_output_tokens: Option<u32>,
    model_timeout: Duration,
}

const DEFAULT_MODEL_TIMEOUT: Duration = Duration::from_secs(120);

impl<P> SingleTurnAgent<P> {
    #[must_use]
    pub fn new(
        provider: P,
        model: impl Into<String>,
        system_prompt: impl Into<String>,
        max_output_tokens: Option<u32>,
    ) -> Self {
        Self {
            provider,
            model: model.into(),
            system_prompt: system_prompt.into(),
            max_output_tokens,
            model_timeout: DEFAULT_MODEL_TIMEOUT,
        }
    }

    #[must_use]
    pub const fn with_model_timeout(mut self, timeout: Duration) -> Self {
        self.model_timeout = timeout;
        self
    }
}

impl<P> Agent for SingleTurnAgent<P>
where
    P: ModelPort,
{
    fn metadata(&self) -> AgentMetadata {
        AgentMetadata::new("single-turn", env!("CARGO_PKG_VERSION"))
            .with_capability("request_response")
            .with_capability("event_stream")
            .with_capability("model_completion")
    }

    fn run(&self, request: RunRequest) -> AgentEventStream {
        let mut messages = Vec::with_capacity(request.prior_messages.len() + 2);
        if !self.system_prompt.trim().is_empty() {
            messages.push(ModelMessage::system(self.system_prompt.clone()));
        }
        messages.extend(request.prior_messages);
        messages.push(ModelMessage::user(request.input));

        let mut model_events = self.provider.stream(ModelRequest {
            run_id: request.run_id,
            model: self.model.clone(),
            messages,
            tools: Vec::new(),
            max_output_tokens: self.max_output_tokens,
        });
        let cancellation = request.cancellation;
        let model_timeout = self.model_timeout;

        Box::pin(stream! {
            let deadline = tokio::time::Instant::now() + model_timeout;
            loop {
                let poll = tokio::select! {
                    biased;
                    () = cancellation.cancelled() => ModelPoll::Cancelled,
                    result = tokio::time::timeout_at(deadline, model_events.next()) => match result {
                        Ok(event) => ModelPoll::Event(event),
                        Err(_) => ModelPoll::TimedOut,
                    },
                };
                let event = match poll {
                    ModelPoll::Cancelled => {
                        yield AgentEvent::Cancelled;
                        return;
                    }
                    ModelPoll::TimedOut => {
                        yield AgentEvent::failed(
                            "model_timeout",
                            "model invocation exceeded the configured timeout",
                            true,
                        );
                        return;
                    }
                    ModelPoll::Event(Some(event)) => event,
                    ModelPoll::Event(None) => break,
                };
                match event {
                    ModelEvent::Accepted { .. } => {}
                    ModelEvent::ReasoningDelta { delta } => {
                        if !delta.is_empty() {
                            yield AgentEvent::OutputDelta {
                                channel: crate::harness::OutputChannel::AssistantReasoning,
                                delta,
                            };
                        }
                    }
                    ModelEvent::TextDelta { delta } => {
                        if !delta.is_empty() {
                            yield AgentEvent::text_delta(delta);
                        }
                    }
                    ModelEvent::ToolCallStarted { .. }
                    | ModelEvent::ToolCallArgumentsDelta { .. } => {
                        yield AgentEvent::failed(
                            "upstream_protocol_violation",
                            "model requested a tool from an agent without tools",
                            false,
                        );
                        return;
                    }
                    ModelEvent::Usage { usage } => yield AgentEvent::UsageUpdated { usage },
                    ModelEvent::Completed { finish_reason } => {
                        yield AgentEvent::completed(finish_reason);
                        return;
                    }
                    ModelEvent::Failed { error } => {
                        yield AgentEvent::failed(
                            error.kind().code(),
                            error.safe_message(),
                            error.retryable(),
                        );
                        return;
                    }
                }
            }

            yield AgentEvent::failed(
                "upstream_protocol_violation",
                "model event stream ended without a terminal event",
                false,
            );
        })
    }
}

enum ModelPoll {
    Cancelled,
    TimedOut,
    Event(Option<ModelEvent>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::stream;

    use crate::harness::{FinishReason, Harness, ModelEventStream, ModelRole, TokenUsage};

    struct FakeProvider;

    impl ModelPort for FakeProvider {
        fn stream(&self, request: ModelRequest) -> ModelEventStream {
            assert_eq!(request.model, "test-model");
            assert_eq!(request.messages.len(), 2);
            assert_eq!(request.messages[0].role, ModelRole::System);
            assert_eq!(request.messages[1].role, ModelRole::User);
            assert_eq!(request.messages[1].content, "ship it");
            Box::pin(stream::iter(vec![
                ModelEvent::Accepted {
                    provider_request_id: Some("provider-request".into()),
                },
                ModelEvent::ReasoningDelta {
                    delta: "think".into(),
                },
                ModelEvent::TextDelta { delta: "do".into() },
                ModelEvent::TextDelta { delta: "ne".into() },
                ModelEvent::Usage {
                    usage: TokenUsage {
                        input_tokens: 2,
                        output_tokens: 1,
                        total_tokens: 3,
                        source: crate::harness::TokenUsageSource::ProviderReported,
                    },
                },
                ModelEvent::Completed {
                    finish_reason: FinishReason::Stop,
                },
            ]))
        }
    }

    struct PendingProvider;

    impl ModelPort for PendingProvider {
        fn stream(&self, _request: ModelRequest) -> ModelEventStream {
            Box::pin(stream::pending())
        }
    }

    #[tokio::test]
    async fn executes_one_task_through_the_model_port() {
        let agent = SingleTurnAgent::new(
            FakeProvider,
            "test-model",
            "Complete the requested task.",
            Some(1024),
        );
        let harness = Harness::new(agent);

        let response = harness
            .execute("ship it")
            .await
            .expect("single task should complete");

        assert_eq!(response.output, "done");
        assert_eq!(response.finish_reason, crate::harness::FinishReason::Stop);
        assert_eq!(response.usage.expect("usage should exist").total_tokens, 3);
    }

    #[tokio::test]
    async fn keeps_reasoning_separate_from_assistant_text() {
        let harness = Harness::new(SingleTurnAgent::new(
            FakeProvider,
            "test-model",
            "Complete the requested task.",
            Some(1024),
        ));

        let events: Vec<_> = harness
            .stream("ship it")
            .expect("stream should start")
            .collect()
            .await;

        assert!(events.iter().any(|event| matches!(
            &event.kind,
            crate::harness::RunEventKind::OutputDelta {
                channel: crate::harness::OutputChannel::AssistantReasoning,
                delta,
            } if delta == "think"
        )));
    }

    #[tokio::test]
    async fn fails_when_the_model_exceeds_its_deadline() {
        let harness = Harness::new(
            SingleTurnAgent::new(PendingProvider, "test-model", "Complete the task.", None)
                .with_model_timeout(Duration::from_millis(10)),
        );

        let events: Vec<_> = harness
            .stream("ship it")
            .expect("stream should start")
            .collect()
            .await;

        assert!(matches!(
            events.last().map(|event| &event.kind),
            Some(crate::harness::RunEventKind::RunFailed { code, .. }) if code == "model_timeout"
        ));
    }
}
