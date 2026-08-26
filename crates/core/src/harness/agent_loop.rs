use std::{collections::HashSet, sync::Arc, time::Duration};

use async_stream::stream;
use futures_util::StreamExt;

use crate::harness::{
    Agent, AgentEvent, AgentEventStream, AgentMetadata, ApprovalDecision, ApprovalError,
    ApprovalId, ApprovalPort, ApprovalRequest, ApprovalResolution, FinishReason, ModelEvent,
    ModelMessage, ModelPort, ModelRequest, ModelToolCall, OutputChannel, RejectAllApprovals,
    RunRequest, TokenUsage, TokenUsageSource, ToolCallRequest, ToolError, ToolErrorCategory,
    ToolOutput, ToolPort, ToolRiskLevel,
};

/// A bounded model/tool loop for one independent run.
///
/// Every model invocation has its own terminal `ModelEvent`, while the Agent
/// continues across tool results and emits exactly one terminal `AgentEvent`.
pub struct AgentLoop<P, T> {
    provider: Arc<P>,
    tools: Arc<T>,
    approvals: Arc<dyn ApprovalPort>,
    model: String,
    system_prompt: String,
    max_output_tokens: Option<u32>,
    max_steps: u32,
    model_timeout: Duration,
    tool_timeout: Duration,
}

const DEFAULT_MODEL_TIMEOUT: Duration = Duration::from_secs(120);
const DEFAULT_TOOL_TIMEOUT: Duration = Duration::from_secs(30);

impl<P, T> AgentLoop<P, T> {
    #[must_use]
    pub fn new(
        provider: P,
        tools: T,
        model: impl Into<String>,
        system_prompt: impl Into<String>,
        max_output_tokens: Option<u32>,
        max_steps: u32,
    ) -> Self {
        Self {
            provider: Arc::new(provider),
            tools: Arc::new(tools),
            approvals: Arc::new(RejectAllApprovals),
            model: model.into(),
            system_prompt: system_prompt.into(),
            max_output_tokens,
            max_steps,
            model_timeout: DEFAULT_MODEL_TIMEOUT,
            tool_timeout: DEFAULT_TOOL_TIMEOUT,
        }
    }

    #[must_use]
    pub const fn with_timeouts(mut self, model_timeout: Duration, tool_timeout: Duration) -> Self {
        self.model_timeout = model_timeout;
        self.tool_timeout = tool_timeout;
        self
    }

    #[must_use]
    pub fn with_approval_port<A>(mut self, approvals: A) -> Self
    where
        A: ApprovalPort,
    {
        self.approvals = Arc::new(approvals);
        self
    }
}

impl<P, T> Agent for AgentLoop<P, T>
where
    P: ModelPort,
    T: ToolPort,
{
    fn metadata(&self) -> AgentMetadata {
        AgentMetadata::new("agent-loop", env!("CARGO_PKG_VERSION"))
            .with_capability("request_response")
            .with_capability("event_stream")
            .with_capability("model_completion")
            .with_capability("tool_calling")
            .with_capability("tool_approval")
    }

    fn run(&self, request: RunRequest) -> AgentEventStream {
        let provider = Arc::clone(&self.provider);
        let tools = Arc::clone(&self.tools);
        let approvals = Arc::clone(&self.approvals);
        let model = self.model.clone();
        let max_output_tokens = self.max_output_tokens;
        let max_steps = self.max_steps;
        let model_timeout = self.model_timeout;
        let tool_timeout = self.tool_timeout;
        let mut tool_definitions = tools.definitions();
        if let Some(allowed) = &request.allowed_tools {
            tool_definitions.retain(|tool| allowed.contains(&tool.name));
        }
        let allowed_tool_names: HashSet<_> = tool_definitions
            .iter()
            .map(|tool| tool.name.clone())
            .collect();
        let cancellation = request.cancellation.clone();

        let mut messages = Vec::with_capacity(request.prior_messages.len() + 4);
        if !self.system_prompt.trim().is_empty() {
            messages.push(ModelMessage::system(self.system_prompt.clone()));
        }
        messages.extend(request.prior_messages);
        messages.push(ModelMessage::user(request.input));

        Box::pin(stream! {
            let mut completed_usage = TokenUsage {
                input_tokens: 0,
                output_tokens: 0,
                total_tokens: 0,
                source: TokenUsageSource::ProviderReported,
            };

            for step in 0..max_steps {
                let mut model_events = provider.stream(ModelRequest {
                    run_id: request.run_id,
                    model: model.clone(),
                    messages: messages.clone(),
                    tools: tool_definitions.clone(),
                    max_output_tokens,
                });
                let mut pending_calls = Vec::<ModelToolCall>::new();
                let mut seen_call_ids = HashSet::new();
                let mut assistant_text = String::new();
                let mut assistant_reasoning = String::new();
                let mut finish_reason = None;
                let mut round_usage = None;
                let model_deadline = tokio::time::Instant::now() + model_timeout;

                loop {
                    let poll = tokio::select! {
                        biased;
                        () = cancellation.cancelled() => ModelPoll::Cancelled,
                        result = tokio::time::timeout_at(model_deadline, model_events.next()) => {
                            match result {
                                Ok(event) => ModelPoll::Event(event),
                                Err(_) => ModelPoll::TimedOut,
                            }
                        }
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
                                assistant_reasoning.push_str(&delta);
                                yield AgentEvent::OutputDelta {
                                    channel: OutputChannel::AssistantReasoning,
                                    delta,
                                };
                            }
                        }
                        ModelEvent::TextDelta { delta } => {
                            if !delta.is_empty() {
                                assistant_text.push_str(&delta);
                                yield AgentEvent::text_delta(delta);
                            }
                        }
                        ModelEvent::ToolCallStarted { call_id, name } => {
                            if !seen_call_ids.insert(call_id.clone()) {
                                yield protocol_failure("model emitted a duplicate tool call id");
                                return;
                            }
                            pending_calls.push(ModelToolCall {
                                id: call_id.clone(),
                                name: name.clone(),
                                arguments: String::new(),
                            });
                            yield AgentEvent::ToolCallStarted { call_id, name };
                        }
                        ModelEvent::ToolCallArgumentsDelta { call_id, delta } => {
                            let Some(call) = pending_calls.iter_mut().find(|call| call.id == call_id) else {
                                yield protocol_failure("model emitted tool arguments before the tool call started");
                                return;
                            };
                            call.arguments.push_str(&delta);
                            if !delta.is_empty() {
                                yield AgentEvent::ToolCallArgumentsDelta { call_id, delta };
                            }
                        }
                        ModelEvent::Usage { usage } => {
                            round_usage = Some(usage);
                            yield AgentEvent::UsageUpdated {
                                usage: add_usage(completed_usage, usage),
                            };
                        }
                        ModelEvent::Completed { finish_reason: reason } => {
                            finish_reason = Some(reason);
                            break;
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

                let Some(finish_reason) = finish_reason else {
                    yield protocol_failure("model event stream ended without a terminal event");
                    return;
                };
                if let Some(usage) = round_usage {
                    completed_usage = add_usage(completed_usage, usage);
                }

                if finish_reason != FinishReason::ToolCall {
                    if !pending_calls.is_empty() {
                        yield protocol_failure("model emitted tool calls without a tool-call finish reason");
                        return;
                    }
                    yield AgentEvent::completed(finish_reason);
                    return;
                }

                if pending_calls.is_empty() {
                    yield protocol_failure("model ended for tool calls without requesting a tool");
                    return;
                }
                if step + 1 >= max_steps {
                    yield AgentEvent::failed(
                        "agent_step_limit_exceeded",
                        "agent reached the configured model step limit",
                        false,
                    );
                    return;
                }

                messages.push(ModelMessage::assistant_tool_calls(
                    assistant_text,
                    assistant_reasoning,
                    pending_calls.clone(),
                ));

                for call in pending_calls {
                    if !allowed_tool_names.contains(&call.name) {
                        let code = "tool_not_allowed";
                        let message = "model requested a tool outside this run's capability set";
                        yield AgentEvent::ToolExecutionFailed {
                            call_id: call.id.clone(),
                            code: code.into(),
                            message: message.into(),
                            category: ToolErrorCategory::PermissionDenied,
                            retryable: false,
                            retry_after_ms: None,
                        };
                        messages.push(ModelMessage::tool_result(
                            call.id,
                            tool_error_content(
                                code,
                                message,
                                ToolErrorCategory::PermissionDenied,
                                false,
                                None,
                            ),
                        ));
                        continue;
                    }
                    let arguments: serde_json::Value = match serde_json::from_str(&call.arguments) {
                        Ok(arguments) => arguments,
                        Err(_) => {
                            let code = "invalid_tool_arguments";
                            let message = "model produced invalid JSON tool arguments";
                            yield AgentEvent::ToolExecutionFailed {
                                call_id: call.id.clone(),
                                code: code.into(),
                                message: message.into(),
                                category: ToolErrorCategory::InvalidRequest,
                                retryable: false,
                                retry_after_ms: None,
                            };
                            messages.push(ModelMessage::tool_result(
                                call.id,
                                tool_error_content(
                                    code,
                                    message,
                                    ToolErrorCategory::InvalidRequest,
                                    false,
                                    None,
                                ),
                            ));
                            continue;
                        }
                    };

                    if let Err(error) = tools.validate(&call.name, &arguments) {
                        yield AgentEvent::ToolExecutionFailed {
                            call_id: call.id.clone(),
                            code: error.code().into(),
                            message: error.safe_message().into(),
                            category: error.category(),
                            retryable: error.retryable(),
                            retry_after_ms: error.retry_after_ms(),
                        };
                        messages.push(ModelMessage::tool_result(
                            call.id,
                            tool_error_content(
                                error.code(),
                                error.safe_message(),
                                error.category(),
                                error.retryable(),
                                error.retry_after_ms(),
                            ),
                        ));
                        continue;
                    }

                    let risk_level = tool_definitions
                        .iter()
                        .find(|definition| definition.name == call.name)
                        .map_or(ToolRiskLevel::Low, |definition| definition.risk_level);
                    if risk_level.requires_approval() {
                        let approval_id = ApprovalId::new();
                        let approval = approvals.request(ApprovalRequest {
                            approval_id,
                            run_id: request.run_id,
                            call_id: call.id.clone(),
                            tool_name: call.name.clone(),
                            risk_level,
                            arguments: arguments.clone(),
                        });
                        yield AgentEvent::ApprovalRequested {
                            approval_id,
                            call_id: call.id.clone(),
                            tool_name: call.name.clone(),
                            risk_level,
                            arguments: arguments.clone(),
                        };

                        let approval_poll = tokio::select! {
                            biased;
                            () = cancellation.cancelled() => ApprovalPoll::Cancelled,
                            result = approval => ApprovalPoll::Resolved(result),
                        };
                        let resolution = match approval_poll {
                            ApprovalPoll::Cancelled => {
                                yield AgentEvent::Cancelled;
                                return;
                            }
                            ApprovalPoll::Resolved(Ok(resolution)) => resolution,
                            ApprovalPoll::Resolved(Err(error)) => {
                                yield AgentEvent::failed(
                                    error.code(),
                                    error.safe_message(),
                                    false,
                                );
                                return;
                            }
                        };
                        yield AgentEvent::ApprovalResolved {
                            approval_id,
                            call_id: call.id.clone(),
                            resolution: resolution.clone(),
                        };

                        if resolution.decision == ApprovalDecision::Deny {
                            let code = "tool_rejected";
                            let message = rejection_message(&resolution);
                            yield AgentEvent::ToolExecutionFailed {
                                call_id: call.id.clone(),
                                code: code.into(),
                                message: message.clone(),
                                category: ToolErrorCategory::PermissionDenied,
                                retryable: false,
                                retry_after_ms: None,
                            };
                            messages.push(ModelMessage::tool_result(
                                call.id,
                                tool_error_content(
                                    code,
                                    &message,
                                    ToolErrorCategory::PermissionDenied,
                                    false,
                                    None,
                                ),
                            ));
                            continue;
                        }
                    }

                    yield AgentEvent::ToolExecutionStarted {
                        call_id: call.id.clone(),
                        arguments: arguments.clone(),
                    };

                    let tool_request = ToolCallRequest {
                        run_id: request.run_id,
                        call_id: call.id.clone(),
                        name: call.name,
                        arguments,
                        cancellation: cancellation.clone(),
                    };
                    let tool_poll = tokio::select! {
                        biased;
                        () = cancellation.cancelled() => ToolPoll::Cancelled,
                        result = tokio::time::timeout(tool_timeout, tools.call(tool_request)) => {
                            match result {
                                Ok(result) => ToolPoll::Finished(result),
                                Err(_) => ToolPoll::TimedOut,
                            }
                        }
                    };
                    match tool_poll {
                        ToolPoll::Cancelled => {
                            yield AgentEvent::Cancelled;
                            return;
                        }
                        ToolPoll::TimedOut => {
                            let code = "tool_timeout";
                            let message = "tool execution exceeded the configured timeout";
                            yield AgentEvent::ToolExecutionFailed {
                                call_id: call.id.clone(),
                                code: code.into(),
                                message: message.into(),
                                category: ToolErrorCategory::Timeout,
                                retryable: true,
                                retry_after_ms: None,
                            };
                            messages.push(ModelMessage::tool_result(
                                call.id,
                                tool_error_content(
                                    code,
                                    message,
                                    ToolErrorCategory::Timeout,
                                    true,
                                    None,
                                ),
                            ));
                        }
                        ToolPoll::Finished(Ok(output)) => {
                            yield AgentEvent::ToolExecutionCompleted {
                                call_id: call.id.clone(),
                                output: output.content.clone(),
                            };
                            messages.push(ModelMessage::tool_result(call.id, output.content));
                        }
                        ToolPoll::Finished(Err(error)) => {
                            yield AgentEvent::ToolExecutionFailed {
                                call_id: call.id.clone(),
                                code: error.code().into(),
                                message: error.safe_message().into(),
                                category: error.category(),
                                retryable: error.retryable(),
                                retry_after_ms: error.retry_after_ms(),
                            };
                            messages.push(ModelMessage::tool_result(
                                call.id,
                                tool_error_content(
                                    error.code(),
                                    error.safe_message(),
                                    error.category(),
                                    error.retryable(),
                                    error.retry_after_ms(),
                                ),
                            ));
                        }
                    }
                }
            }

            yield AgentEvent::failed(
                "agent_step_limit_exceeded",
                "agent reached the configured model step limit",
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

enum ToolPoll {
    Cancelled,
    TimedOut,
    Finished(Result<ToolOutput, ToolError>),
}

enum ApprovalPoll {
    Cancelled,
    Resolved(Result<ApprovalResolution, ApprovalError>),
}

fn add_usage(left: TokenUsage, right: TokenUsage) -> TokenUsage {
    let source = if left.input_tokens == 0 && left.output_tokens == 0 && left.total_tokens == 0 {
        right.source
    } else if left.source == right.source {
        left.source
    } else {
        TokenUsageSource::Mixed
    };
    TokenUsage {
        input_tokens: left.input_tokens.saturating_add(right.input_tokens),
        output_tokens: left.output_tokens.saturating_add(right.output_tokens),
        total_tokens: left.total_tokens.saturating_add(right.total_tokens),
        source,
    }
}

fn protocol_failure(message: &str) -> AgentEvent {
    AgentEvent::failed("upstream_protocol_violation", message, false)
}

fn tool_error_content(
    code: &str,
    message: &str,
    category: ToolErrorCategory,
    retryable: bool,
    retry_after_ms: Option<u64>,
) -> String {
    serde_json::json!({
        "ok": false,
        "error": {
            "code": code,
            "message": message,
            "category": category,
            "retryable": retryable,
            "retry_after_ms": retry_after_ms,
        }
    })
    .to_string()
}

fn rejection_message(resolution: &ApprovalResolution) -> String {
    resolution.reason.as_deref().map_or_else(
        || "the user rejected this tool call".into(),
        |reason| format!("the user rejected this tool call: {reason}"),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures_util::stream;
    use serde_json::json;

    use super::*;
    use crate::harness::{
        Harness, ModelEventStream, RunEventKind, ToolCallFuture, ToolDefinition, ToolError,
        ToolOutput,
    };

    struct FakeProvider {
        calls: AtomicUsize,
    }

    impl ModelPort for FakeProvider {
        fn stream(&self, request: ModelRequest) -> ModelEventStream {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            assert_eq!(request.tools[0].name, "get_current_time");

            if call == 0 {
                assert_eq!(request.messages.len(), 2);
                return Box::pin(stream::iter(vec![
                    ModelEvent::Accepted {
                        provider_request_id: None,
                    },
                    ModelEvent::ToolCallStarted {
                        call_id: "call_1".into(),
                        name: "get_current_time".into(),
                    },
                    ModelEvent::ToolCallArgumentsDelta {
                        call_id: "call_1".into(),
                        delta: "{}".into(),
                    },
                    ModelEvent::Usage {
                        usage: TokenUsage {
                            input_tokens: 10,
                            output_tokens: 2,
                            total_tokens: 12,
                            source: TokenUsageSource::ProviderReported,
                        },
                    },
                    ModelEvent::Completed {
                        finish_reason: FinishReason::ToolCall,
                    },
                ]));
            }

            assert_eq!(request.messages.len(), 4);
            assert_eq!(request.messages[2].tool_calls[0].id, "call_1");
            assert_eq!(request.messages[3].role, crate::harness::ModelRole::Tool);
            assert_eq!(request.messages[3].tool_call_id.as_deref(), Some("call_1"));
            Box::pin(stream::iter(vec![
                ModelEvent::Accepted {
                    provider_request_id: None,
                },
                ModelEvent::TextDelta {
                    delta: "It is noon.".into(),
                },
                ModelEvent::Usage {
                    usage: TokenUsage {
                        input_tokens: 20,
                        output_tokens: 4,
                        total_tokens: 24,
                        source: TokenUsageSource::ProviderReported,
                    },
                },
                ModelEvent::Completed {
                    finish_reason: FinishReason::Stop,
                },
            ]))
        }
    }

    struct FakeTools;

    impl ToolPort for FakeTools {
        fn definitions(&self) -> Vec<ToolDefinition> {
            vec![ToolDefinition::new(
                "get_current_time",
                "Return the current time.",
                json!({"type": "object", "properties": {}}),
            )]
        }

        fn call(&self, request: ToolCallRequest) -> ToolCallFuture {
            Box::pin(async move {
                if request.name != "get_current_time" {
                    return Err(ToolError::new("tool_not_found", "unknown tool", false));
                }
                assert_eq!(request.arguments, json!({}));
                Ok(ToolOutput::text(r#"{"utc":"2026-08-25T00:00:00Z"}"#))
            })
        }
    }

    struct PendingProvider;

    impl ModelPort for PendingProvider {
        fn stream(&self, _request: ModelRequest) -> ModelEventStream {
            Box::pin(stream::pending())
        }
    }

    struct ToolTimeoutProvider {
        calls: AtomicUsize,
    }

    impl ModelPort for ToolTimeoutProvider {
        fn stream(&self, request: ModelRequest) -> ModelEventStream {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                return Box::pin(stream::iter(vec![
                    ModelEvent::ToolCallStarted {
                        call_id: "call_slow".into(),
                        name: "get_current_time".into(),
                    },
                    ModelEvent::ToolCallArgumentsDelta {
                        call_id: "call_slow".into(),
                        delta: "{}".into(),
                    },
                    ModelEvent::Completed {
                        finish_reason: FinishReason::ToolCall,
                    },
                ]));
            }

            assert!(request.messages[3].content.contains("tool_timeout"));
            Box::pin(stream::iter(vec![
                ModelEvent::TextDelta {
                    delta: "The tool timed out.".into(),
                },
                ModelEvent::Completed {
                    finish_reason: FinishReason::Stop,
                },
            ]))
        }
    }

    struct SlowTools;

    impl ToolPort for SlowTools {
        fn definitions(&self) -> Vec<ToolDefinition> {
            FakeTools.definitions()
        }

        fn call(&self, _request: ToolCallRequest) -> ToolCallFuture {
            Box::pin(std::future::pending())
        }
    }

    struct RiskyTools {
        calls: Arc<AtomicUsize>,
    }

    impl ToolPort for RiskyTools {
        fn definitions(&self) -> Vec<ToolDefinition> {
            vec![
                ToolDefinition::new(
                    "get_current_time",
                    "Return the current time.",
                    json!({"type": "object", "properties": {}}),
                )
                .with_risk_level(ToolRiskLevel::High),
            ]
        }

        fn call(&self, _request: ToolCallRequest) -> ToolCallFuture {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(ToolOutput::text(r#"{"utc":"now"}"#)) })
        }
    }

    struct FixedApproval {
        resolution: ApprovalResolution,
    }

    impl ApprovalPort for FixedApproval {
        fn request(&self, request: ApprovalRequest) -> crate::harness::ApprovalFuture {
            assert_eq!(request.tool_name, "get_current_time");
            assert_eq!(request.risk_level, ToolRiskLevel::High);
            assert_eq!(request.arguments, json!({}));
            let resolution = self.resolution.clone();
            Box::pin(async move { Ok(resolution) })
        }
    }

    #[tokio::test]
    async fn executes_a_tool_and_continues_until_the_model_finishes() {
        let harness = Harness::new(AgentLoop::new(
            FakeProvider {
                calls: AtomicUsize::new(0),
            },
            FakeTools,
            "test-model",
            "Use tools when needed.",
            Some(1024),
            4,
        ));

        let events: Vec<_> = harness
            .stream("What time is it?")
            .expect("stream should start")
            .collect()
            .await;

        assert!(events.iter().any(|event| matches!(
            &event.kind,
            RunEventKind::ToolExecutionCompleted { call_id, .. } if call_id == "call_1"
        )));
        assert!(events.iter().any(|event| matches!(
            event.kind,
            RunEventKind::UsageUpdated {
                usage: TokenUsage {
                    total_tokens: 36,
                    ..
                }
            }
        )));
        assert!(matches!(
            events.last().map(|event| &event.kind),
            Some(RunEventKind::RunCompleted {
                finish_reason: FinishReason::Stop
            })
        ));
    }

    #[tokio::test]
    async fn fails_when_a_model_round_exceeds_its_deadline() {
        let harness = Harness::new(
            AgentLoop::new(
                PendingProvider,
                FakeTools,
                "test-model",
                "Use tools when needed.",
                None,
                4,
            )
            .with_timeouts(Duration::from_millis(10), Duration::from_secs(1)),
        );

        let events: Vec<_> = harness
            .stream("Wait forever")
            .expect("stream should start")
            .collect()
            .await;

        assert!(matches!(
            events.last().map(|event| &event.kind),
            Some(RunEventKind::RunFailed { code, .. }) if code == "model_timeout"
        ));
    }

    #[tokio::test]
    async fn returns_tool_timeouts_to_the_model_and_continues() {
        let harness = Harness::new(
            AgentLoop::new(
                ToolTimeoutProvider {
                    calls: AtomicUsize::new(0),
                },
                SlowTools,
                "test-model",
                "Use tools when needed.",
                None,
                4,
            )
            .with_timeouts(Duration::from_secs(1), Duration::from_millis(10)),
        );

        let events: Vec<_> = harness
            .stream("Use the slow tool")
            .expect("stream should start")
            .collect()
            .await;

        assert!(events.iter().any(|event| matches!(
            &event.kind,
            RunEventKind::ToolExecutionFailed { code, .. } if code == "tool_timeout"
        )));
        assert!(matches!(
            events.last().map(|event| &event.kind),
            Some(RunEventKind::RunCompleted { .. })
        ));
    }

    #[tokio::test]
    async fn executes_a_risky_tool_only_after_approval() {
        let calls = Arc::new(AtomicUsize::new(0));
        let harness = Harness::new(
            AgentLoop::new(
                FakeProvider {
                    calls: AtomicUsize::new(0),
                },
                RiskyTools {
                    calls: Arc::clone(&calls),
                },
                "test-model",
                "Use tools when needed.",
                None,
                4,
            )
            .with_approval_port(FixedApproval {
                resolution: ApprovalResolution::allow_once(),
            }),
        );

        let events: Vec<_> = harness
            .stream("Use the risky tool")
            .expect("stream should start")
            .collect()
            .await;

        let approval_index = events
            .iter()
            .position(|event| matches!(event.kind, RunEventKind::ApprovalRequested { .. }))
            .expect("approval should be requested");
        let execution_index = events
            .iter()
            .position(|event| matches!(event.kind, RunEventKind::ToolExecutionStarted { .. }))
            .expect("tool should execute");
        assert!(approval_index < execution_index);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(events.iter().any(|event| matches!(
            &event.kind,
            RunEventKind::ApprovalResolved { resolution, .. }
                if resolution.decision == ApprovalDecision::AllowOnce
        )));
    }

    #[tokio::test]
    async fn returns_a_rejected_risky_tool_to_the_model_without_executing_it() {
        let calls = Arc::new(AtomicUsize::new(0));
        let harness = Harness::new(
            AgentLoop::new(
                FakeProvider {
                    calls: AtomicUsize::new(0),
                },
                RiskyTools {
                    calls: Arc::clone(&calls),
                },
                "test-model",
                "Use tools when needed.",
                None,
                4,
            )
            .with_approval_port(FixedApproval {
                resolution: ApprovalResolution::deny(Some("not allowed".into())),
            }),
        );

        let events: Vec<_> = harness
            .stream("Use the risky tool")
            .expect("stream should start")
            .collect()
            .await;

        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(events.iter().any(|event| matches!(
            &event.kind,
            RunEventKind::ToolExecutionFailed { code, message, .. }
                if code == "tool_rejected" && message.contains("not allowed")
        )));
        assert!(matches!(
            events.last().map(|event| &event.kind),
            Some(RunEventKind::RunCompleted { .. })
        ));
    }
}
