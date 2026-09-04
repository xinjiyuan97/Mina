use super::{AgentLoop, native_reducer};
use crate::harness::{
    Agent, AgentMachine, CheckpointEnvelope, MachineError, MachineResumeRequest,
    MachineStartRequest, MachineStream, ModelPort, ToolPort,
};

#[cfg(test)]
use super::{ToolCallStrategy, tool_error_content};
#[cfg(test)]
use crate::{
    event_runtime::{
        CreateSubscription, DeliveryTarget, EventFilter, EventId, EventSource, PublishEvent,
        StartPosition, SubscriptionId, SubscriptionMode, SubscriptionOwner, SubscriptionScope,
    },
    harness::{
        AgentEvent, ApprovalResolution, EffectRequest, FinishReason, FlowInboxItem, MachineOutput,
        ModelEvent, ModelMessage, ModelRequest, OutputChannel, RunCancellation, RunId, StepOutcome,
        ToolCallRequest, ToolCompletion, ToolConcurrency, ToolErrorCategory, ToolExecutionPolicy,
        ToolRiskLevel, ToolSuspension, WaitSpec,
    },
};
#[cfg(test)]
use async_stream::stream;
#[cfg(test)]
use futures_core::Stream;
#[cfg(test)]
use serde_json::Value;
#[cfg(test)]
use std::{collections::HashSet, sync::Arc, time::Duration};

#[cfg(test)]
#[allow(dead_code)]
#[derive(Debug, Clone)]
struct PreparedToolCall {
    call_id: String,
    name: String,
    arguments: Value,
    public_arguments: Value,
    risk_level: ToolRiskLevel,
    execution: ToolExecutionPolicy,
}

#[cfg(test)]
#[allow(dead_code)]
enum ToolExecutionOutput {
    Event(AgentEvent),
    Result(ModelMessage),
    Suspended {
        call_id: String,
        suspension: ToolSuspension,
    },
    Cancelled,
}

impl<P, T> AgentMachine for AgentLoop<P, T>
where
    P: ModelPort,
    T: ToolPort,
{
    fn metadata(&self) -> crate::harness::AgentMetadata {
        <Self as Agent>::metadata(self).with_capability("durable_checkpoint")
    }

    fn initial_checkpoint(
        &self,
        request: &MachineStartRequest,
    ) -> Result<CheckpointEnvelope, MachineError> {
        native_reducer::initial_checkpoint(self, request)
    }

    fn start(&self, request: MachineStartRequest) -> MachineStream {
        native_reducer::start(self, request)
    }

    fn resume(&self, request: MachineResumeRequest) -> MachineStream {
        native_reducer::resume(self, request)
    }
}

#[cfg(test)]
fn execute_tool<T: ToolPort>(
    tools: Arc<T>,
    run_id: RunId,
    tool_set_revision: u64,
    call: PreparedToolCall,
    cancellation: RunCancellation,
    timeout: Duration,
) -> std::pin::Pin<Box<dyn Stream<Item = ToolExecutionOutput> + Send + 'static>> {
    Box::pin(stream! {
        yield ToolExecutionOutput::Event(AgentEvent::ToolExecutionStarted {
            call_id: call.call_id.clone(),
            arguments: call.public_arguments,
        });
        let mut attempts = 0_u32;
        loop {
            attempts = attempts.saturating_add(1);
            let request = ToolCallRequest {
                run_id,
                call_id: call.call_id.clone(),
                name: call.name.clone(),
                arguments: call.arguments.clone(),
                cancellation: cancellation.clone(),
            };
            let result = tokio::select! {
                biased;
                () = cancellation.cancelled() => {
                    yield ToolExecutionOutput::Cancelled;
                    return;
                }
                result = tokio::time::timeout(timeout, tools.call_at(tool_set_revision, request)) => result,
            };
            match result {
            Err(_) => {
                if should_retry(call.execution, attempts, true) {
                    if !wait_for_retry(
                        &cancellation,
                        call.execution.retry.delay_for_attempt(attempts),
                    )
                    .await
                    {
                        yield ToolExecutionOutput::Cancelled;
                        return;
                    }
                    continue;
                }
                let code = "tool_timeout";
                let message = "tool execution exceeded the configured timeout";
                yield ToolExecutionOutput::Event(AgentEvent::ToolExecutionFailed {
                    call_id: call.call_id.clone(),
                    code: code.into(),
                    message: message.into(),
                    category: ToolErrorCategory::Timeout,
                    retryable: true,
                    retry_after_ms: None,
                });
                yield ToolExecutionOutput::Result(ModelMessage::tool_result(
                    call.call_id,
                    tool_error_content(code, message, ToolErrorCategory::Timeout, true, None),
                ));
            }
            Ok(Ok(output)) => {
                if let Some(suspension) = output.suspension {
                    if call.execution.completion != ToolCompletion::MaySuspend {
                        let code = "unexpected_tool_suspension";
                        let message = "the tool suspended without declaring may_suspend execution semantics";
                        yield ToolExecutionOutput::Event(AgentEvent::ToolExecutionFailed {
                            call_id: call.call_id.clone(),
                            code: code.into(),
                            message: message.into(),
                            category: ToolErrorCategory::Internal,
                            retryable: false,
                            retry_after_ms: None,
                        });
                        yield ToolExecutionOutput::Result(ModelMessage::tool_result(
                            call.call_id,
                            tool_error_content(
                                code,
                                message,
                                ToolErrorCategory::Internal,
                                false,
                                None,
                            ),
                        ));
                    } else if !output.content.is_empty() {
                        let code = "invalid_tool_suspension";
                        let message = "a suspended tool cannot also return immediate content";
                        yield ToolExecutionOutput::Event(AgentEvent::ToolExecutionFailed {
                            call_id: call.call_id.clone(),
                            code: code.into(),
                            message: message.into(),
                            category: ToolErrorCategory::Internal,
                            retryable: false,
                            retry_after_ms: None,
                        });
                        yield ToolExecutionOutput::Result(ModelMessage::tool_result(
                            call.call_id,
                            tool_error_content(
                                code,
                                message,
                                ToolErrorCategory::Internal,
                                false,
                                None,
                            ),
                        ));
                    } else if let Err(message) = validate_tool_suspension(run_id, &suspension) {
                        let code = "invalid_tool_suspension";
                        yield ToolExecutionOutput::Event(AgentEvent::ToolExecutionFailed {
                            call_id: call.call_id.clone(),
                            code: code.into(),
                            message: message.clone(),
                            category: ToolErrorCategory::Internal,
                            retryable: false,
                            retry_after_ms: None,
                        });
                        yield ToolExecutionOutput::Result(ModelMessage::tool_result(
                            call.call_id,
                            tool_error_content(
                                code,
                                &message,
                                ToolErrorCategory::Internal,
                                false,
                                None,
                            ),
                        ));
                    } else {
                        yield ToolExecutionOutput::Suspended {
                            call_id: call.call_id,
                            suspension,
                        };
                    }
                } else {
                    yield ToolExecutionOutput::Event(AgentEvent::ToolExecutionCompleted {
                        call_id: call.call_id.clone(),
                        output: output.content.clone(),
                    });
                    yield ToolExecutionOutput::Result(ModelMessage::tool_result(call.call_id, output.content));
                }
            }
            Ok(Err(error)) => {
                if should_retry(call.execution, attempts, error.retryable()) {
                    let delay = error.retry_after_ms().map_or_else(
                        || call.execution.retry.delay_for_attempt(attempts),
                        |requested| requested.min(call.execution.retry.max_backoff_ms),
                    );
                    if !wait_for_retry(&cancellation, delay).await {
                        yield ToolExecutionOutput::Cancelled;
                        return;
                    }
                    continue;
                }
                yield ToolExecutionOutput::Event(AgentEvent::ToolExecutionFailed {
                    call_id: call.call_id.clone(),
                    code: error.code().into(),
                    message: error.safe_message().into(),
                    category: error.category(),
                    retryable: error.retryable(),
                    retry_after_ms: error.retry_after_ms(),
                });
                yield ToolExecutionOutput::Result(ModelMessage::tool_result(
                    call.call_id,
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
            break;
        }
    })
}

#[cfg(test)]
fn should_retry(policy: ToolExecutionPolicy, attempts: u32, retryable: bool) -> bool {
    retryable
        && policy.idempotency.permits_automatic_retry()
        && attempts < policy.retry.max_attempts
}

#[cfg(test)]
async fn wait_for_retry(cancellation: &RunCancellation, delay_ms: u64) -> bool {
    tokio::select! {
        biased;
        () = cancellation.cancelled() => false,
        () = tokio::time::sleep(Duration::from_millis(delay_ms)) => true,
    }
}

#[cfg(test)]
fn validate_tool_suspension(run_id: RunId, suspension: &ToolSuspension) -> Result<(), String> {
    const MAX_WAITS: usize = 16;
    const MAX_EFFECTS: usize = 16;

    if suspension.waits.is_empty() {
        return Err("a suspended tool requires at least one wait".into());
    }
    if suspension.waits.len() > MAX_WAITS {
        return Err(format!(
            "a suspended tool cannot register more than {MAX_WAITS} waits"
        ));
    }
    if suspension.effects.len() > MAX_EFFECTS {
        return Err(format!(
            "a suspended tool cannot submit more than {MAX_EFFECTS} effects"
        ));
    }

    let mut wait_keys = HashSet::new();
    let mut subscription_ids = HashSet::new();
    for wait in &suspension.waits {
        if !wait_keys.insert(wait.wait_key.as_str()) {
            return Err("a suspended tool cannot register duplicate wait keys".into());
        }
        if !subscription_ids.insert(wait.subscription.subscription_id) {
            return Err("a suspended tool cannot register duplicate subscription ids".into());
        }
        wait.subscription
            .validate()
            .map_err(|error| error.to_string())?;
        if wait.subscription.owner != (SubscriptionOwner::Run { run_id })
            || wait.subscription.scope != (SubscriptionScope::Run { run_id })
        {
            return Err("tool waits must be owned and scoped by the current run".into());
        }
        match &wait.subscription.delivery {
            DeliveryTarget::WakeRun {
                run_id: target_run_id,
                wait_key,
            } if *target_run_id == run_id && wait_key == &wait.wait_key => {}
            _ => {
                return Err(
                    "tool waits must wake the current run using the declared wait key".into(),
                );
            }
        }
    }
    for effect in &suspension.effects {
        effect.validate().map_err(|error| error.to_string())?;
        if let EffectRequest::StartJob { command } = effect
            && command.run_id != run_id
        {
            return Err("tool job effects must belong to the current run".into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures_util::{StreamExt, stream};
    use serde_json::json;

    use super::*;
    use crate::harness::{
        ActivationId, ModelEventStream, ToolCallFuture, ToolDefinition, ToolOutput,
    };

    struct ApprovalProvider {
        calls: AtomicUsize,
    }

    impl ModelPort for ApprovalProvider {
        fn stream(&self, request: ModelRequest) -> ModelEventStream {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                assert_eq!(request.messages.len(), 2);
                return Box::pin(stream::iter(vec![
                    ModelEvent::Accepted {
                        provider_request_id: None,
                    },
                    ModelEvent::ToolCallStarted {
                        call_id: "call_approval".into(),
                        name: "dangerous_tool".into(),
                    },
                    ModelEvent::ToolCallArgumentsDelta {
                        call_id: "call_approval".into(),
                        delta: r#"{"value":1}"#.into(),
                    },
                    ModelEvent::Completed {
                        finish_reason: FinishReason::ToolCall,
                    },
                ]));
            }
            assert_eq!(call, 1);
            assert_eq!(request.messages.len(), 4);
            assert_eq!(request.messages[3].content, r#"{"ok":true}"#);
            Box::pin(stream::iter(vec![
                ModelEvent::Accepted {
                    provider_request_id: None,
                },
                ModelEvent::TextDelta {
                    delta: "finished".into(),
                },
                ModelEvent::Completed {
                    finish_reason: FinishReason::Stop,
                },
            ]))
        }
    }

    struct ApprovalTools {
        calls: AtomicUsize,
    }

    impl ToolPort for ApprovalTools {
        fn definitions(&self) -> Vec<ToolDefinition> {
            vec![
                ToolDefinition::new(
                    "dangerous_tool",
                    "A tool requiring approval.",
                    json!({
                        "type": "object",
                        "properties": {"value": {"type": "integer"}},
                        "required": ["value"],
                        "additionalProperties": false
                    }),
                )
                .with_risk_level(ToolRiskLevel::High),
            ]
        }

        fn call(&self, request: ToolCallRequest) -> ToolCallFuture {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                assert_eq!(request.arguments, json!({"value": 1}));
                Ok(ToolOutput::text(r#"{"ok":true}"#))
            })
        }
    }

    #[tokio::test]
    async fn approval_suspends_serializes_resumes_and_completes() {
        let agent = AgentLoop::new(
            ApprovalProvider {
                calls: AtomicUsize::new(0),
            },
            ApprovalTools {
                calls: AtomicUsize::new(0),
            },
            "test-model",
            "system",
            None,
            4,
        );
        let run_id = RunId::new();
        let cancellation = RunCancellation::new();
        let start = MachineStartRequest {
            run_id,
            input: "perform the task".into(),
            attachments: Vec::new(),
            prior_messages: Vec::new(),
            allowed_tools: None,
            allow_run_adf: false,
            max_steps: None,
            context_fingerprint: Some("sha256:test".into()),
            activated_at_ms: 10,
            cancellation: cancellation.clone(),
        };
        agent
            .initial_checkpoint(&start)
            .expect("initial checkpoint should encode");
        let mut first = agent.start(start);
        let mut approval_id = None;
        let mut suspended = None;
        while let Some(output) = first.next().await {
            match output {
                MachineOutput::Event(AgentEvent::ApprovalRequested {
                    approval_id: requested,
                    ..
                }) => approval_id = Some(requested),
                MachineOutput::Yield(StepOutcome::Suspend {
                    checkpoint,
                    waits,
                    effects,
                }) => suspended = Some((checkpoint, waits, effects)),
                _ => {}
            }
        }
        let approval_id = approval_id.expect("approval should be requested");
        let (checkpoint, waits, effects) = suspended.expect("machine should suspend");
        assert_eq!(waits.len(), 1);
        assert_eq!(effects.len(), 1);

        let resolution = ApprovalResolution::allow_once();
        let event = crate::event_runtime::EventEnvelope {
            sequence: 1,
            event: PublishEvent {
                event_id: EventId::new(),
                topic: "tool.approval.resolved".into(),
                event_type: "tool.approval.resolved".into(),
                schema_version: 1,
                source: EventSource::Gateway,
                subject: Some(format!("run/{run_id}")),
                correlation_id: Some(approval_id.to_string()),
                causation_id: None,
                occurred_at_ms: 20,
                recorded_at_ms: 20,
                payload: serde_json::to_value(resolution).expect("resolution should encode"),
            },
        };
        let mut resumed = agent.resume(MachineResumeRequest {
            run_id,
            activation_id: ActivationId::new(),
            checkpoint,
            inbox: vec![FlowInboxItem {
                event,
                consumed_revision: None,
            }],
            activated_at_ms: 21,
            cancellation: cancellation.clone(),
        });
        let mut continued = None;
        let mut completed_tool = false;
        while let Some(output) = resumed.next().await {
            match output {
                MachineOutput::Event(AgentEvent::ToolExecutionCompleted { .. }) => {
                    completed_tool = true;
                }
                MachineOutput::Yield(StepOutcome::Continue { checkpoint, .. }) => {
                    continued = Some(checkpoint);
                }
                _ => {}
            }
        }
        assert!(completed_tool);
        let checkpoint = continued.expect("approved tool should checkpoint and continue");

        let mut final_activation = agent.resume(MachineResumeRequest {
            run_id,
            activation_id: ActivationId::new(),
            checkpoint,
            inbox: Vec::new(),
            activated_at_ms: 22,
            cancellation,
        });
        let mut text = String::new();
        let mut completed = false;
        while let Some(output) = final_activation.next().await {
            match output {
                MachineOutput::Event(AgentEvent::OutputDelta {
                    channel: OutputChannel::AssistantText,
                    delta,
                }) => text.push_str(&delta),
                MachineOutput::Yield(StepOutcome::Complete {
                    finish_reason: FinishReason::Stop,
                }) => completed = true,
                _ => {}
            }
        }
        assert_eq!(text, "finished");
        assert!(completed);
        assert_eq!(agent.provider.calls.load(Ordering::SeqCst), 2);
        assert_eq!(agent.tools.calls.load(Ordering::SeqCst), 1);
    }

    struct SuspensionProvider {
        calls: AtomicUsize,
    }

    impl ModelPort for SuspensionProvider {
        fn stream(&self, request: ModelRequest) -> ModelEventStream {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                return Box::pin(stream::iter(vec![
                    ModelEvent::Accepted {
                        provider_request_id: None,
                    },
                    ModelEvent::ToolCallStarted {
                        call_id: "call_wait".into(),
                        name: "waiting_tool".into(),
                    },
                    ModelEvent::ToolCallArgumentsDelta {
                        call_id: "call_wait".into(),
                        delta: "{}".into(),
                    },
                    ModelEvent::ToolCallStarted {
                        call_id: "call_deferred".into(),
                        name: "side_effect_tool".into(),
                    },
                    ModelEvent::ToolCallArgumentsDelta {
                        call_id: "call_deferred".into(),
                        delta: "{}".into(),
                    },
                    ModelEvent::Completed {
                        finish_reason: FinishReason::ToolCall,
                    },
                ]));
            }

            assert_eq!(call, 1);
            assert_eq!(request.messages.len(), 5);
            let resumed: Value = serde_json::from_str(&request.messages[3].content)
                .expect("resumed tool result should be JSON");
            assert_eq!(resumed["ok"], true);
            assert_eq!(resumed["events"][0]["topic"], "external.work.completed");
            assert_eq!(resumed["events"][0]["payload"], json!({"value": 42}));
            let deferred: Value = serde_json::from_str(&request.messages[4].content)
                .expect("deferred tool result should be JSON");
            assert_eq!(deferred["error"]["code"], "tool_deferred_by_suspension");
            Box::pin(stream::iter(vec![
                ModelEvent::Accepted {
                    provider_request_id: None,
                },
                ModelEvent::TextDelta {
                    delta: "resumed".into(),
                },
                ModelEvent::Completed {
                    finish_reason: FinishReason::Stop,
                },
            ]))
        }
    }

    struct SuspensionTools {
        calls: AtomicUsize,
    }

    impl ToolPort for SuspensionTools {
        fn definitions(&self) -> Vec<ToolDefinition> {
            vec![
                ToolDefinition::new(
                    "waiting_tool",
                    "Suspend until an external event arrives.",
                    json!({"type": "object", "additionalProperties": false}),
                )
                .with_execution_policy(
                    ToolExecutionPolicy::default().with_completion(ToolCompletion::MaySuspend),
                ),
                ToolDefinition::new(
                    "side_effect_tool",
                    "Must not execute after another call suspends.",
                    json!({"type": "object", "additionalProperties": false}),
                ),
            ]
        }

        fn call(&self, request: ToolCallRequest) -> ToolCallFuture {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                assert_eq!(request.name, "waiting_tool");
                let correlation_id = request.call_id.clone();
                let wait_key = format!("external-work:{correlation_id}");
                Ok(ToolOutput::suspend(
                    vec![WaitSpec {
                        wait_key: wait_key.clone(),
                        subscription: CreateSubscription {
                            subscription_id: SubscriptionId::stable(
                                "external-work",
                                &correlation_id,
                            ),
                            owner: SubscriptionOwner::Run {
                                run_id: request.run_id,
                            },
                            scope: SubscriptionScope::Run {
                                run_id: request.run_id,
                            },
                            filter: EventFilter {
                                topics: vec!["external.work.completed".into()],
                                sources: vec![EventSource::ToolWorker],
                                correlation_id: Some(correlation_id),
                                ..EventFilter::default()
                            },
                            delivery: DeliveryTarget::WakeRun {
                                run_id: request.run_id,
                                wait_key,
                            },
                            mode: SubscriptionMode::Once,
                            start_position: StartPosition::Now,
                            expires_at_ms: None,
                            max_deliveries: Some(1),
                            created_at_ms: 10,
                        },
                    }],
                    Vec::new(),
                ))
            })
        }
    }

    #[tokio::test]
    async fn tool_suspension_resumes_from_inbox_and_defers_remaining_calls() {
        let agent = AgentLoop::new(
            SuspensionProvider {
                calls: AtomicUsize::new(0),
            },
            SuspensionTools {
                calls: AtomicUsize::new(0),
            },
            "test-model",
            "system",
            None,
            4,
        );
        let run_id = RunId::new();
        let cancellation = RunCancellation::new();
        let mut first = agent.start(MachineStartRequest {
            run_id,
            input: "wait for work".into(),
            attachments: Vec::new(),
            prior_messages: Vec::new(),
            allowed_tools: None,
            allow_run_adf: false,
            max_steps: None,
            context_fingerprint: None,
            activated_at_ms: 10,
            cancellation: cancellation.clone(),
        });
        let mut suspended = None;
        while let Some(output) = first.next().await {
            if let MachineOutput::Yield(StepOutcome::Suspend {
                checkpoint,
                waits,
                effects,
            }) = output
            {
                suspended = Some((checkpoint, waits, effects));
            }
        }
        let (checkpoint, waits, effects) = suspended.expect("tool should suspend");
        assert_eq!(waits.len(), 1);
        assert!(effects.is_empty());
        assert_eq!(agent.tools.calls.load(Ordering::SeqCst), 1);

        let event = crate::event_runtime::EventEnvelope {
            sequence: 7,
            event: PublishEvent {
                event_id: EventId::new(),
                topic: "external.work.completed".into(),
                event_type: "external.work.completed".into(),
                schema_version: 1,
                source: EventSource::ToolWorker,
                subject: Some(format!("run/{run_id}")),
                correlation_id: Some("call_wait".into()),
                causation_id: None,
                occurred_at_ms: 20,
                recorded_at_ms: 20,
                payload: json!({"value": 42}),
            },
        };
        let mut resumed = agent.resume(MachineResumeRequest {
            run_id,
            activation_id: ActivationId::new(),
            checkpoint,
            inbox: vec![FlowInboxItem {
                event,
                consumed_revision: None,
            }],
            activated_at_ms: 21,
            cancellation: cancellation.clone(),
        });
        let mut continued = None;
        let mut completed_call = None;
        let mut deferred_call = None;
        while let Some(output) = resumed.next().await {
            match output {
                MachineOutput::Event(AgentEvent::ToolExecutionCompleted { call_id, .. }) => {
                    completed_call = Some(call_id);
                }
                MachineOutput::Event(AgentEvent::ToolExecutionFailed { call_id, code, .. })
                    if code == "tool_deferred_by_suspension" =>
                {
                    deferred_call = Some(call_id);
                }
                MachineOutput::Yield(StepOutcome::Continue { checkpoint, .. }) => {
                    continued = Some(checkpoint);
                }
                _ => {}
            }
        }
        assert_eq!(completed_call.as_deref(), Some("call_wait"));
        assert_eq!(deferred_call.as_deref(), Some("call_deferred"));

        let mut final_activation = agent.resume(MachineResumeRequest {
            run_id,
            activation_id: ActivationId::new(),
            checkpoint: continued.expect("resume should checkpoint and continue"),
            inbox: Vec::new(),
            activated_at_ms: 22,
            cancellation,
        });
        let mut completed = false;
        while let Some(output) = final_activation.next().await {
            if let MachineOutput::Yield(StepOutcome::Complete {
                finish_reason: FinishReason::Stop,
            }) = output
            {
                completed = true;
            }
        }
        assert!(completed);
        assert_eq!(agent.provider.calls.load(Ordering::SeqCst), 2);
        assert_eq!(agent.tools.calls.load(Ordering::SeqCst), 1);
    }

    struct RetryTools {
        calls: AtomicUsize,
        failures_before_success: usize,
    }

    impl ToolPort for RetryTools {
        fn definitions(&self) -> Vec<ToolDefinition> {
            Vec::new()
        }

        fn call(&self, _request: ToolCallRequest) -> ToolCallFuture {
            let attempt = self.calls.fetch_add(1, Ordering::SeqCst);
            let failures_before_success = self.failures_before_success;
            Box::pin(async move {
                if attempt < failures_before_success {
                    Err(crate::harness::ToolError::new(
                        "backend_unavailable",
                        "try again",
                        true,
                    ))
                } else {
                    Ok(ToolOutput::text("done"))
                }
            })
        }
    }

    fn prepared_call(execution: ToolExecutionPolicy) -> PreparedToolCall {
        PreparedToolCall {
            call_id: "call_retry".into(),
            name: "retry_tool".into(),
            arguments: json!({}),
            public_arguments: json!({}),
            risk_level: ToolRiskLevel::Low,
            execution,
        }
    }

    #[tokio::test]
    async fn retries_retryable_read_only_tool_within_its_bound() {
        let tools = Arc::new(RetryTools {
            calls: AtomicUsize::new(0),
            failures_before_success: 2,
        });
        let policy = ToolExecutionPolicy::read_only()
            .with_retry(crate::harness::ToolRetryPolicy::bounded(3, 1, 1));
        let outputs = execute_tool(
            Arc::clone(&tools),
            RunId::new(),
            0,
            prepared_call(policy),
            RunCancellation::new(),
            Duration::from_secs(1),
        )
        .collect::<Vec<_>>()
        .await;

        assert_eq!(tools.calls.load(Ordering::SeqCst), 3);
        assert!(outputs.iter().any(|output| matches!(
            output,
            ToolExecutionOutput::Event(AgentEvent::ToolExecutionCompleted { output, .. })
                if output == "done"
        )));
    }

    #[tokio::test]
    async fn does_not_retry_tool_with_unknown_idempotency() {
        let tools = Arc::new(RetryTools {
            calls: AtomicUsize::new(0),
            failures_before_success: usize::MAX,
        });
        let outputs = execute_tool(
            Arc::clone(&tools),
            RunId::new(),
            0,
            prepared_call(ToolExecutionPolicy::default()),
            RunCancellation::new(),
            Duration::from_secs(1),
        )
        .collect::<Vec<_>>()
        .await;

        assert_eq!(tools.calls.load(Ordering::SeqCst), 1);
        assert!(outputs.iter().any(|output| matches!(
            output,
            ToolExecutionOutput::Event(AgentEvent::ToolExecutionFailed { code, .. })
                if code == "backend_unavailable"
        )));
    }

    struct ParallelProvider {
        calls: AtomicUsize,
    }

    impl ModelPort for ParallelProvider {
        fn stream(&self, request: ModelRequest) -> ModelEventStream {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                return Box::pin(stream::iter(vec![
                    ModelEvent::ToolCallStarted {
                        call_id: "call_slow".into(),
                        name: "slow".into(),
                    },
                    ModelEvent::ToolCallArgumentsDelta {
                        call_id: "call_slow".into(),
                        delta: "{}".into(),
                    },
                    ModelEvent::ToolCallStarted {
                        call_id: "call_fast".into(),
                        name: "fast".into(),
                    },
                    ModelEvent::ToolCallArgumentsDelta {
                        call_id: "call_fast".into(),
                        delta: "{}".into(),
                    },
                    ModelEvent::Completed {
                        finish_reason: FinishReason::ToolCall,
                    },
                ]));
            }

            assert_eq!(
                request.messages[3].tool_call_id.as_deref(),
                Some("call_slow")
            );
            assert_eq!(request.messages[3].content, "slow-result");
            assert_eq!(
                request.messages[4].tool_call_id.as_deref(),
                Some("call_fast")
            );
            assert_eq!(request.messages[4].content, "fast-result");
            Box::pin(stream::iter(vec![ModelEvent::Completed {
                finish_reason: FinishReason::Stop,
            }]))
        }
    }

    struct ConcurrencyTools {
        active: Arc<AtomicUsize>,
        max_active: Arc<AtomicUsize>,
        second_parallel_safe: bool,
    }

    impl ToolPort for ConcurrencyTools {
        fn definitions(&self) -> Vec<ToolDefinition> {
            let parallel =
                ToolExecutionPolicy::read_only().with_concurrency(ToolConcurrency::ParallelSafe);
            vec![
                ToolDefinition::new("slow", "Slow read.", json!({"type": "object"}))
                    .with_execution_policy(parallel),
                ToolDefinition::new("fast", "Fast read.", json!({"type": "object"}))
                    .with_execution_policy(if self.second_parallel_safe {
                        parallel
                    } else {
                        ToolExecutionPolicy::read_only()
                    }),
            ]
        }

        fn call(&self, request: ToolCallRequest) -> ToolCallFuture {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active.fetch_max(active, Ordering::SeqCst);
            let delay = if request.name == "slow" { 30 } else { 5 };
            let content = format!("{}-result", request.name);
            let active_counter = Arc::clone(&self.active);
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(delay)).await;
                active_counter.fetch_sub(1, Ordering::SeqCst);
                Ok(ToolOutput::text(content))
            })
        }
    }

    async fn run_parallel_case(second_parallel_safe: bool) -> usize {
        let agent = AgentLoop::new(
            ParallelProvider {
                calls: AtomicUsize::new(0),
            },
            ConcurrencyTools {
                active: Arc::new(AtomicUsize::new(0)),
                max_active: Arc::new(AtomicUsize::new(0)),
                second_parallel_safe,
            },
            "test-model",
            "system",
            None,
            4,
        )
        .with_tool_call_strategy(ToolCallStrategy::ParallelSafe);
        let run_id = RunId::new();
        let cancellation = RunCancellation::new();
        let mut first = agent.start(MachineStartRequest {
            run_id,
            input: "run both".into(),
            attachments: Vec::new(),
            prior_messages: Vec::new(),
            allowed_tools: None,
            allow_run_adf: false,
            max_steps: None,
            context_fingerprint: None,
            activated_at_ms: 10,
            cancellation: cancellation.clone(),
        });
        let mut checkpoint = None;
        while let Some(output) = first.next().await {
            if let MachineOutput::Yield(StepOutcome::Continue {
                checkpoint: next, ..
            }) = output
            {
                checkpoint = Some(next);
            }
        }
        let mut resumed = agent.resume(MachineResumeRequest {
            run_id,
            activation_id: ActivationId::new(),
            checkpoint: checkpoint.expect("tool round should continue"),
            inbox: Vec::new(),
            activated_at_ms: 20,
            cancellation,
        });
        while resumed.next().await.is_some() {}
        agent.tools.max_active.load(Ordering::SeqCst)
    }

    #[tokio::test]
    async fn overlaps_parallel_safe_tools_but_preserves_model_result_order() {
        assert_eq!(run_parallel_case(true).await, 2);
    }

    #[tokio::test]
    async fn keeps_exclusive_tools_out_of_parallel_batches() {
        assert_eq!(run_parallel_case(false).await, 1);
    }
}
