use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
    time::Duration,
};

use async_stream::stream;
use futures_core::Stream;
use futures_util::{StreamExt, stream::FuturesUnordered};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{
    AgentLoop, ToolCallStrategy, add_usage, final_summary_stream, rejection_message,
    step_limit_tool_error_content, tool_error_content,
};
use crate::{
    event_runtime::{
        CreateSubscription, DeliveryTarget, EventFilter, EventId, EventSource, PublishEvent,
        StartPosition, SubscriptionId, SubscriptionMode, SubscriptionOwner, SubscriptionScope,
    },
    harness::{
        Agent, AgentEvent, AgentMachine, ApprovalDecision, ApprovalId, ApprovalResolution,
        CheckpointCodec, CheckpointEnvelope, EffectRequest, FinishReason, FlowInboxItem,
        MachineError, MachineOutput, MachineResumeRequest, MachineStartRequest, MachineStream,
        ModelEvent, ModelMessage, ModelPort, ModelRequest, ModelToolCall, OutputChannel,
        RunCancellation, RunId, StepOutcome, TokenUsage, TokenUsageSource, ToolApprovalPolicy,
        ToolArgumentVisibility, ToolBindingKind, ToolCallRequest, ToolCompletion, ToolConcurrency,
        ToolErrorCategory, ToolExecutionPolicy, ToolPort, ToolRiskLevel, ToolSetSnapshot,
        ToolSuspension, WaitSpec,
    },
};

const CHECKPOINT_KIND: &str = "agent-loop";
const CHECKPOINT_VERSION: u32 = 2;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AgentLoopCheckpoint {
    messages: Vec<ModelMessage>,
    completed_usage: TokenUsage,
    previous_tool_set: Option<ToolSetIdentity>,
    next_step: u32,
    max_steps: u32,
    allowed_tools: Option<Vec<String>>,
    allow_run_adf: bool,
    context_fingerprint: Option<String>,
    pending_approval: Option<PendingApproval>,
    pending_tool: Option<PendingToolWait>,
    tool_call_strategy: ToolCallStrategy,
    #[serde(default)]
    approval_policy: ToolApprovalPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ToolSetIdentity {
    revision: u64,
    digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PendingApproval {
    approval_id: ApprovalId,
    call: PreparedToolCall,
    remaining_calls: Vec<ModelToolCall>,
    tool_set: ToolSetSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PendingToolWait {
    call_id: String,
    remaining_calls: Vec<ModelToolCall>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PreparedToolCall {
    call_id: String,
    name: String,
    arguments: Value,
    public_arguments: Value,
    risk_level: ToolRiskLevel,
    execution: ToolExecutionPolicy,
}

enum PreparedCall {
    Ready {
        call: PreparedToolCall,
        events: Vec<AgentEvent>,
    },
    Rejected {
        events: Vec<AgentEvent>,
        result: ModelMessage,
    },
}

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
        encode_checkpoint(initial_state(self, request))
    }

    fn start(&self, request: MachineStartRequest) -> MachineStream {
        let state = initial_state(self, &request);
        drive_activation(
            Arc::clone(&self.provider),
            Arc::clone(&self.tools),
            self.model.clone(),
            self.max_output_tokens,
            self.model_timeout,
            self.tool_timeout,
            request.run_id,
            state,
            Vec::new(),
            request.activated_at_ms,
            request.cancellation,
        )
    }

    fn resume(&self, request: MachineResumeRequest) -> MachineStream {
        let state = match decode_checkpoint(request.checkpoint) {
            Ok(state) => state,
            Err(error) => return failed_stream(error),
        };
        drive_activation(
            Arc::clone(&self.provider),
            Arc::clone(&self.tools),
            self.model.clone(),
            self.max_output_tokens,
            self.model_timeout,
            self.tool_timeout,
            request.run_id,
            state,
            request.inbox,
            request.activated_at_ms,
            request.cancellation,
        )
    }
}

fn initial_state<P, T>(
    loop_agent: &AgentLoop<P, T>,
    request: &MachineStartRequest,
) -> AgentLoopCheckpoint {
    let mut messages = Vec::with_capacity(request.prior_messages.len() + 2);
    if !loop_agent.system_prompt.trim().is_empty() {
        messages.push(ModelMessage::system(loop_agent.system_prompt.clone()));
    }
    messages.extend(request.prior_messages.clone());
    messages.push(ModelMessage::user(request.input.clone()));
    let host_max_steps = loop_agent.max_steps.max(1);
    let max_steps = request.max_steps.map_or(host_max_steps, |requested| {
        requested.clamp(1, host_max_steps)
    });
    AgentLoopCheckpoint {
        messages,
        completed_usage: TokenUsage {
            input_tokens: 0,
            output_tokens: 0,
            total_tokens: 0,
            source: TokenUsageSource::ProviderReported,
        },
        previous_tool_set: None,
        next_step: 0,
        max_steps,
        allowed_tools: request.allowed_tools.clone(),
        allow_run_adf: request.allow_run_adf,
        context_fingerprint: request.context_fingerprint.clone(),
        pending_approval: None,
        pending_tool: None,
        tool_call_strategy: loop_agent.tool_call_strategy,
        approval_policy: loop_agent.approval_policy,
    }
}

#[allow(clippy::too_many_arguments)]
fn drive_activation<P, T>(
    provider: Arc<P>,
    tools: Arc<T>,
    model: String,
    max_output_tokens: Option<u32>,
    model_timeout: Duration,
    tool_timeout: Duration,
    run_id: RunId,
    mut state: AgentLoopCheckpoint,
    inbox: Vec<FlowInboxItem>,
    activated_at_ms: i64,
    cancellation: RunCancellation,
) -> MachineStream
where
    P: ModelPort,
    T: ToolPort,
{
    Box::pin(stream! {
        if cancellation.is_cancelled() {
            tools.close_run(run_id);
            yield MachineOutput::Yield(StepOutcome::Cancelled);
            return;
        }

        if state.pending_approval.is_some() && state.pending_tool.is_some() {
            yield MachineOutput::Yield(StepOutcome::Failed {
                error: machine_error(
                    "checkpoint_invalid",
                    "the agent checkpoint cannot contain both a pending approval and a pending tool wait",
                    false,
                ),
            });
            return;
        }

        if let Some(pending) = state.pending_tool.take() {
            let content = match resumed_tool_content(&inbox) {
                Ok(content) => content,
                Err(error) => {
                    yield MachineOutput::Yield(StepOutcome::Failed { error });
                    return;
                }
            };
            yield MachineOutput::Event(AgentEvent::ToolExecutionCompleted {
                call_id: pending.call_id.clone(),
                output: content.clone(),
            });
            state
                .messages
                .push(ModelMessage::tool_result(pending.call_id, content));
            for call in pending.remaining_calls {
                let (event, result) = deferred_tool_call(call);
                yield MachineOutput::Event(event);
                state.messages.push(result);
            }
            let checkpoint = match encode_checkpoint(state) {
                Ok(checkpoint) => checkpoint,
                Err(error) => {
                    yield MachineOutput::Yield(StepOutcome::Failed { error });
                    return;
                }
            };
            yield MachineOutput::Yield(StepOutcome::Continue {
                checkpoint,
                effects: Vec::new(),
            });
            return;
        }

        if let Some(pending) = state.pending_approval.take() {
            let resolution = match resolve_approval(run_id, pending.approval_id, &inbox) {
                Ok(resolution) => resolution,
                Err(error) => {
                    yield MachineOutput::Yield(StepOutcome::Failed { error });
                    return;
                }
            };
            yield MachineOutput::Event(AgentEvent::ApprovalResolved {
                approval_id: pending.approval_id,
                call_id: pending.call.call_id.clone(),
                resolution: resolution.clone(),
            });

            if resolution.decision == ApprovalDecision::Deny {
                let code = "tool_rejected";
                let message = rejection_message(&resolution);
                yield MachineOutput::Event(AgentEvent::ToolExecutionFailed {
                    call_id: pending.call.call_id.clone(),
                    code: code.into(),
                    message: message.clone(),
                    category: ToolErrorCategory::PermissionDenied,
                    retryable: false,
                    retry_after_ms: None,
                });
                state.messages.push(ModelMessage::tool_result(
                    pending.call.call_id,
                    tool_error_content(
                        code,
                        &message,
                        ToolErrorCategory::PermissionDenied,
                        false,
                        None,
                    ),
                ));
            } else {
                let mut execution = execute_tool(
                    Arc::clone(&tools),
                    run_id,
                    pending.tool_set.revision,
                    pending.call,
                    cancellation.clone(),
                    tool_timeout,
                );
                while let Some(output) = execution.next().await {
                    match output {
                        ToolExecutionOutput::Event(event) => yield MachineOutput::Event(event),
                        ToolExecutionOutput::Result(result) => state.messages.push(result),
                        ToolExecutionOutput::Cancelled => {
                            tools.close_run(run_id);
                            yield MachineOutput::Yield(StepOutcome::Cancelled);
                            return;
                        }
                        ToolExecutionOutput::Suspended { call_id, suspension } => {
                            state.pending_tool = Some(PendingToolWait {
                                call_id,
                                remaining_calls: pending.remaining_calls.clone(),
                            });
                            let checkpoint = match encode_checkpoint(state) {
                                Ok(checkpoint) => checkpoint,
                                Err(error) => {
                                    yield MachineOutput::Yield(StepOutcome::Failed { error });
                                    return;
                                }
                            };
                            yield MachineOutput::Yield(StepOutcome::Suspend {
                                checkpoint,
                                waits: suspension.waits,
                                effects: suspension.effects,
                            });
                            return;
                        }
                    }
                }
            }

            let tool_set = pending.tool_set;
            let calls = pending.remaining_calls;
            for (index, call) in calls.iter().enumerate() {
                let prepared = prepare_call(&tools, run_id, &tool_set, call);
                let prepared = match prepared {
                    PreparedCall::Ready { call, events } => {
                        for event in events {
                            yield MachineOutput::Event(event);
                        }
                        call
                    }
                    PreparedCall::Rejected { events, result } => {
                        for event in events {
                            yield MachineOutput::Event(event);
                        }
                        state.messages.push(result);
                        continue;
                    }
                };
                if state.approval_policy.requires_review(prepared.risk_level) {
                    let approval_id = ApprovalId::new();
                    yield MachineOutput::Event(AgentEvent::ApprovalRequested {
                        approval_id,
                        call_id: prepared.call_id.clone(),
                        tool_name: prepared.name.clone(),
                        risk_level: prepared.risk_level,
                        arguments: prepared.public_arguments.clone(),
                    });
                    state.pending_approval = Some(PendingApproval {
                        approval_id,
                        call: prepared.clone(),
                        remaining_calls: calls[index + 1..].to_vec(),
                        tool_set: tool_set.clone(),
                    });
                    let checkpoint = match encode_checkpoint(state) {
                        Ok(checkpoint) => checkpoint,
                        Err(error) => {
                            yield MachineOutput::Yield(StepOutcome::Failed { error });
                            return;
                        }
                    };
                    let (waits, effects) = approval_wait(
                        run_id,
                        approval_id,
                        &prepared,
                        activated_at_ms,
                    );
                    yield MachineOutput::Yield(StepOutcome::Suspend {
                        checkpoint,
                        waits,
                        effects,
                    });
                    return;
                }
                let mut execution = execute_tool(
                    Arc::clone(&tools),
                    run_id,
                    tool_set.revision,
                    prepared,
                    cancellation.clone(),
                    tool_timeout,
                );
                while let Some(output) = execution.next().await {
                    match output {
                        ToolExecutionOutput::Event(event) => yield MachineOutput::Event(event),
                        ToolExecutionOutput::Result(result) => state.messages.push(result),
                        ToolExecutionOutput::Cancelled => {
                            tools.close_run(run_id);
                            yield MachineOutput::Yield(StepOutcome::Cancelled);
                            return;
                        }
                        ToolExecutionOutput::Suspended { call_id, suspension } => {
                            state.pending_tool = Some(PendingToolWait {
                                call_id,
                                remaining_calls: calls[index + 1..].to_vec(),
                            });
                            let checkpoint = match encode_checkpoint(state) {
                                Ok(checkpoint) => checkpoint,
                                Err(error) => {
                                    yield MachineOutput::Yield(StepOutcome::Failed { error });
                                    return;
                                }
                            };
                            yield MachineOutput::Yield(StepOutcome::Suspend {
                                checkpoint,
                                waits: suspension.waits,
                                effects: suspension.effects,
                            });
                            return;
                        }
                    }
                }
            }

            let checkpoint = match encode_checkpoint(state) {
                Ok(checkpoint) => checkpoint,
                Err(error) => {
                    yield MachineOutput::Yield(StepOutcome::Failed { error });
                    return;
                }
            };
            yield MachineOutput::Yield(StepOutcome::Continue {
                checkpoint,
                effects: Vec::new(),
            });
            return;
        }

        if !inbox.is_empty() {
            yield MachineOutput::Yield(StepOutcome::Failed {
                error: machine_error(
                    "unexpected_machine_event",
                    "the agent checkpoint has no handler for the delivered event",
                    false,
                ),
            });
            return;
        }

        let tool_set = match filtered_tool_set(&tools, run_id, &state) {
            Ok(tool_set) => tool_set,
            Err(error) => {
                tools.close_run(run_id);
                yield MachineOutput::Yield(StepOutcome::Failed { error });
                return;
            }
        };
        let identity = ToolSetIdentity {
            revision: tool_set.revision,
            digest: tool_set.digest.clone(),
        };
        if state.previous_tool_set.as_ref() != Some(&identity) {
            yield MachineOutput::Event(AgentEvent::ToolSetUpdated {
                revision: tool_set.revision,
                digest: tool_set.digest.clone(),
                tools: tool_set
                    .definitions
                    .iter()
                    .map(|definition| definition.name.clone())
                    .collect(),
                dynamic_tools: tool_set
                    .bindings
                    .iter()
                    .filter(|binding| binding.kind == ToolBindingKind::AgentDefined)
                    .map(|binding| binding.name.clone())
                    .collect(),
            });
            state.previous_tool_set = Some(identity);
        }

        let mut model_events = provider.stream(ModelRequest {
            run_id,
            model: model.clone(),
            messages: state.messages.clone(),
            tools: tool_set.definitions.clone(),
            max_output_tokens,
        });
        let mut pending_calls = Vec::<ModelToolCall>::new();
        let mut seen_call_ids = HashSet::new();
        let mut assistant_text = String::new();
        let mut assistant_reasoning = String::new();
        let mut round_usage = None;
        let model_deadline = tokio::time::Instant::now() + model_timeout;

        let finish_reason = loop {
            let event = tokio::select! {
                biased;
                () = cancellation.cancelled() => {
                    tools.close_run(run_id);
                    yield MachineOutput::Yield(StepOutcome::Cancelled);
                    return;
                }
                result = tokio::time::timeout_at(model_deadline, model_events.next()) => match result {
                    Ok(Some(event)) => event,
                    Ok(None) => {
                        yield MachineOutput::Yield(StepOutcome::Failed {
                            error: machine_error(
                                "upstream_protocol_violation",
                                "model event stream ended without a terminal event",
                                false,
                            ),
                        });
                        return;
                    }
                    Err(_) => {
                        yield MachineOutput::Yield(StepOutcome::Failed {
                            error: machine_error(
                                "model_timeout",
                                "model invocation exceeded the configured timeout",
                                true,
                            ),
                        });
                        return;
                    }
                },
            };
            match event {
                ModelEvent::Accepted { .. } => {}
                ModelEvent::ReasoningDelta { delta } => {
                    if !delta.is_empty() {
                        assistant_reasoning.push_str(&delta);
                        yield MachineOutput::Event(AgentEvent::OutputDelta {
                            channel: OutputChannel::AssistantReasoning,
                            delta,
                        });
                    }
                }
                ModelEvent::TextDelta { delta } => {
                    if !delta.is_empty() {
                        assistant_text.push_str(&delta);
                        yield MachineOutput::Event(AgentEvent::text_delta(delta));
                    }
                }
                ModelEvent::ToolCallStarted { call_id, name } => {
                    if !seen_call_ids.insert(call_id.clone()) {
                        yield MachineOutput::Yield(StepOutcome::Failed {
                            error: machine_error(
                                "upstream_protocol_violation",
                                "model emitted a duplicate tool call id",
                                false,
                            ),
                        });
                        return;
                    }
                    pending_calls.push(ModelToolCall {
                        id: call_id.clone(),
                        name: name.clone(),
                        arguments: String::new(),
                    });
                    yield MachineOutput::Event(AgentEvent::ToolCallStarted { call_id, name });
                }
                ModelEvent::ToolCallArgumentsDelta { call_id, delta } => {
                    let Some(call) = pending_calls.iter_mut().find(|call| call.id == call_id) else {
                        yield MachineOutput::Yield(StepOutcome::Failed {
                            error: machine_error(
                                "upstream_protocol_violation",
                                "model emitted tool arguments before the tool call started",
                                false,
                            ),
                        });
                        return;
                    };
                    call.arguments.push_str(&delta);
                    let visibility = tool_set
                        .binding(&call.name)
                        .map_or(ToolArgumentVisibility::DigestOnly, |binding| binding.argument_visibility);
                    if !delta.is_empty() && visibility == ToolArgumentVisibility::Full {
                        yield MachineOutput::Event(AgentEvent::ToolCallArgumentsDelta { call_id, delta });
                    }
                }
                ModelEvent::Usage { usage } => {
                    round_usage = Some(usage);
                    yield MachineOutput::Event(AgentEvent::UsageUpdated {
                        usage: add_usage(state.completed_usage, usage),
                    });
                }
                ModelEvent::Completed { finish_reason: reason } => {
                    break reason;
                }
                ModelEvent::Failed { error } => {
                    tools.close_run(run_id);
                    yield MachineOutput::Yield(StepOutcome::Failed {
                        error: machine_error(
                            error.kind().code(),
                            error.safe_message(),
                            error.retryable(),
                        ),
                    });
                    return;
                }
            }
        };

        if let Some(usage) = round_usage {
            state.completed_usage = add_usage(state.completed_usage, usage);
        }
        if finish_reason != FinishReason::ToolCall {
            if !pending_calls.is_empty() {
                yield MachineOutput::Yield(StepOutcome::Failed {
                    error: machine_error(
                        "upstream_protocol_violation",
                        "model emitted tool calls without a tool-call finish reason",
                        false,
                    ),
                });
                return;
            }
            tools.close_run(run_id);
            yield MachineOutput::Yield(StepOutcome::Complete { finish_reason });
            return;
        }
        if pending_calls.is_empty() {
            yield MachineOutput::Yield(StepOutcome::Failed {
                error: machine_error(
                    "upstream_protocol_violation",
                    "model ended for tool calls without requesting a tool",
                    false,
                ),
            });
            return;
        }

        state.messages.push(ModelMessage::assistant_tool_calls(
            assistant_text,
            assistant_reasoning,
            pending_calls.clone(),
        ));
        if state.next_step.saturating_add(1) >= state.max_steps {
            for call in pending_calls {
                let code = "agent_step_limit_exceeded";
                let message = "the model step limit was reached; this tool was not executed";
                yield MachineOutput::Event(AgentEvent::ToolExecutionFailed {
                    call_id: call.id.clone(),
                    code: code.into(),
                    message: message.into(),
                    category: ToolErrorCategory::ResourceExhausted,
                    retryable: false,
                    retry_after_ms: None,
                });
                state.messages.push(ModelMessage::tool_result(
                    call.id,
                    step_limit_tool_error_content(code, message),
                ));
            }
            let mut final_events = final_summary_stream(
                Arc::clone(&provider),
                ModelRequest {
                    run_id,
                    model,
                    messages: state.messages,
                    tools: Vec::new(),
                    max_output_tokens,
                },
                cancellation.clone(),
                model_timeout,
                state.completed_usage,
            );
            while let Some(event) = final_events.next().await {
                match event {
                    AgentEvent::Completed { finish_reason } => {
                        tools.close_run(run_id);
                        yield MachineOutput::Yield(StepOutcome::Complete { finish_reason });
                        return;
                    }
                    AgentEvent::Failed { code, message, retryable } => {
                        tools.close_run(run_id);
                        yield MachineOutput::Yield(StepOutcome::Failed {
                            error: machine_error(code, message, retryable),
                        });
                        return;
                    }
                    AgentEvent::Cancelled => {
                        tools.close_run(run_id);
                        yield MachineOutput::Yield(StepOutcome::Cancelled);
                        return;
                    }
                    event => yield MachineOutput::Event(event),
                }
            }
            yield MachineOutput::Yield(StepOutcome::Failed {
                error: machine_error(
                    "upstream_protocol_violation",
                    "final model summary ended without an outcome",
                    false,
                ),
            });
            return;
        }

        state.next_step = state.next_step.saturating_add(1);
        let mut prepared_calls = pending_calls
            .iter()
            .map(|call| Some(prepare_call(&tools, run_id, &tool_set, call)))
            .collect::<Vec<_>>();
        let mut index = 0_usize;
        while index < pending_calls.len() {
            let prepared = prepared_calls[index]
                .take()
                .expect("each prepared call is consumed once");
            let prepared = match prepared {
                PreparedCall::Ready { call, events } => {
                    for event in events {
                        yield MachineOutput::Event(event);
                    }
                    call
                }
                PreparedCall::Rejected { events, result } => {
                    for event in events {
                        yield MachineOutput::Event(event);
                    }
                    state.messages.push(result);
                    index = index.saturating_add(1);
                    continue;
                }
            };
            if state.approval_policy.requires_review(prepared.risk_level) {
                let approval_id = ApprovalId::new();
                yield MachineOutput::Event(AgentEvent::ApprovalRequested {
                    approval_id,
                    call_id: prepared.call_id.clone(),
                    tool_name: prepared.name.clone(),
                    risk_level: prepared.risk_level,
                    arguments: prepared.public_arguments.clone(),
                });
                state.pending_approval = Some(PendingApproval {
                    approval_id,
                    call: prepared.clone(),
                    remaining_calls: pending_calls[index + 1..].to_vec(),
                    tool_set: tool_set.clone(),
                });
                let checkpoint = match encode_checkpoint(state) {
                    Ok(checkpoint) => checkpoint,
                    Err(error) => {
                        yield MachineOutput::Yield(StepOutcome::Failed { error });
                        return;
                    }
                };
                let (waits, effects) = approval_wait(
                    run_id,
                    approval_id,
                    &prepared,
                    activated_at_ms,
                );
                yield MachineOutput::Yield(StepOutcome::Suspend {
                    checkpoint,
                    waits,
                    effects,
                });
                return;
            }

            if is_parallel_candidate(state.tool_call_strategy, state.approval_policy, &prepared) {
                let mut batch = vec![(index, prepared.clone())];
                let mut next = index.saturating_add(1);
                while next < prepared_calls.len()
                    && prepared_calls[next]
                        .as_ref()
                        .is_some_and(|call| {
                            prepared_call_is_parallel_candidate(state.approval_policy, call)
                        })
                {
                    let PreparedCall::Ready { call, events } = prepared_calls[next]
                        .take()
                        .expect("parallel candidate should exist")
                    else {
                        unreachable!("parallel candidate must be ready");
                    };
                    for event in events {
                        yield MachineOutput::Event(event);
                    }
                    batch.push((next, call));
                    next = next.saturating_add(1);
                }
                if batch.len() > 1 {
                    let batch_indices = batch.iter().map(|(index, _)| *index).collect::<Vec<_>>();
                    let mut running = FuturesUnordered::new();
                    for (call_index, call) in batch {
                        let tools = Arc::clone(&tools);
                        let cancellation = cancellation.clone();
                        running.push(async move {
                            let mut execution = execute_tool(
                                tools,
                                run_id,
                                tool_set.revision,
                                call,
                                cancellation,
                                tool_timeout,
                            );
                            let mut outputs = Vec::new();
                            while let Some(output) = execution.next().await {
                                outputs.push(output);
                            }
                            (call_index, outputs)
                        });
                    }
                    let mut results = BTreeMap::new();
                    while let Some((call_index, outputs)) = running.next().await {
                        for output in outputs {
                            match output {
                                ToolExecutionOutput::Event(event) => {
                                    yield MachineOutput::Event(event);
                                }
                                ToolExecutionOutput::Result(result) => {
                                    results.insert(call_index, result);
                                }
                                ToolExecutionOutput::Cancelled => {
                                    tools.close_run(run_id);
                                    yield MachineOutput::Yield(StepOutcome::Cancelled);
                                    return;
                                }
                                ToolExecutionOutput::Suspended { .. } => {
                                    tools.close_run(run_id);
                                    yield MachineOutput::Yield(StepOutcome::Failed {
                                        error: machine_error(
                                            "parallel_tool_suspended",
                                            "a parallel-safe tool unexpectedly suspended",
                                            false,
                                        ),
                                    });
                                    return;
                                }
                            }
                        }
                    }
                    for call_index in batch_indices {
                        let Some(result) = results.remove(&call_index) else {
                            tools.close_run(run_id);
                            yield MachineOutput::Yield(StepOutcome::Failed {
                                error: machine_error(
                                    "parallel_tool_result_missing",
                                    "a parallel tool execution ended without a result",
                                    false,
                                ),
                            });
                            return;
                        };
                        state.messages.push(result);
                    }
                    index = next;
                    continue;
                }
            }

            let mut execution = execute_tool(
                Arc::clone(&tools),
                run_id,
                tool_set.revision,
                prepared,
                cancellation.clone(),
                tool_timeout,
            );
            while let Some(output) = execution.next().await {
                match output {
                    ToolExecutionOutput::Event(event) => yield MachineOutput::Event(event),
                    ToolExecutionOutput::Result(result) => state.messages.push(result),
                    ToolExecutionOutput::Cancelled => {
                        tools.close_run(run_id);
                        yield MachineOutput::Yield(StepOutcome::Cancelled);
                        return;
                    }
                    ToolExecutionOutput::Suspended { call_id, suspension } => {
                        state.pending_tool = Some(PendingToolWait {
                            call_id,
                            remaining_calls: pending_calls[index + 1..].to_vec(),
                        });
                        let checkpoint = match encode_checkpoint(state) {
                            Ok(checkpoint) => checkpoint,
                            Err(error) => {
                                yield MachineOutput::Yield(StepOutcome::Failed { error });
                                return;
                            }
                        };
                        yield MachineOutput::Yield(StepOutcome::Suspend {
                            checkpoint,
                            waits: suspension.waits,
                            effects: suspension.effects,
                        });
                        return;
                    }
                }
            }
            index = index.saturating_add(1);
        }

        let checkpoint = match encode_checkpoint(state) {
            Ok(checkpoint) => checkpoint,
            Err(error) => {
                yield MachineOutput::Yield(StepOutcome::Failed { error });
                return;
            }
        };
        yield MachineOutput::Yield(StepOutcome::Continue {
            checkpoint,
            effects: Vec::new(),
        });
    })
}

fn is_parallel_candidate(
    strategy: ToolCallStrategy,
    approval_policy: ToolApprovalPolicy,
    call: &PreparedToolCall,
) -> bool {
    strategy == ToolCallStrategy::ParallelSafe
        && !approval_policy.requires_review(call.risk_level)
        && call.execution.concurrency == ToolConcurrency::ParallelSafe
        && call.execution.completion == ToolCompletion::Immediate
}

fn prepared_call_is_parallel_candidate(
    approval_policy: ToolApprovalPolicy,
    call: &PreparedCall,
) -> bool {
    matches!(
        call,
        PreparedCall::Ready { call, .. }
            if is_parallel_candidate(ToolCallStrategy::ParallelSafe, approval_policy, call)
    )
}

fn filtered_tool_set<T: ToolPort>(
    tools: &Arc<T>,
    run_id: RunId,
    state: &AgentLoopCheckpoint,
) -> Result<ToolSetSnapshot, MachineError> {
    tools
        .tool_set_snapshot(run_id)
        .and_then(|snapshot| {
            snapshot.retain(|binding| {
                let statically_allowed = state
                    .allowed_tools
                    .as_ref()
                    .is_none_or(|allowed| allowed.contains(&binding.name));
                match binding.kind {
                    ToolBindingKind::Static => statically_allowed,
                    ToolBindingKind::AdfManagement => state.allow_run_adf && statically_allowed,
                    ToolBindingKind::AgentDefined => state.allow_run_adf,
                }
            })
        })
        .map_err(|error| machine_error(error.code(), error.safe_message(), error.retryable()))
}

fn prepare_call<T: ToolPort>(
    tools: &Arc<T>,
    run_id: RunId,
    tool_set: &ToolSetSnapshot,
    call: &ModelToolCall,
) -> PreparedCall {
    if tool_set.binding(&call.name).is_none() {
        let code = "tool_not_allowed";
        let message = "model requested a tool outside this run's capability set";
        return rejected_call(
            call,
            code,
            message,
            ToolErrorCategory::PermissionDenied,
            false,
            None,
            Vec::new(),
        );
    }
    let visibility = tool_set
        .binding(&call.name)
        .map_or(ToolArgumentVisibility::DigestOnly, |binding| {
            binding.argument_visibility
        });
    let arguments: Value = match serde_json::from_str(&call.arguments) {
        Ok(arguments) => arguments,
        Err(_) => {
            let events = if visibility == ToolArgumentVisibility::DigestOnly {
                vec![AgentEvent::ToolCallArgumentsDelta {
                    call_id: call.id.clone(),
                    delta: super::digest_only_bytes(call.arguments.as_bytes()).to_string(),
                }]
            } else {
                Vec::new()
            };
            return rejected_call(
                call,
                "invalid_tool_arguments",
                "model produced invalid JSON tool arguments",
                ToolErrorCategory::InvalidRequest,
                false,
                None,
                events,
            );
        }
    };
    let public_arguments = match visibility {
        ToolArgumentVisibility::Full => arguments.clone(),
        ToolArgumentVisibility::DigestOnly => super::digest_only_arguments(&arguments),
    };
    let mut events = Vec::new();
    if visibility == ToolArgumentVisibility::DigestOnly {
        events.push(AgentEvent::ToolCallArgumentsDelta {
            call_id: call.id.clone(),
            delta: public_arguments.to_string(),
        });
    }
    if let Err(error) = tools.validate_at(run_id, tool_set.revision, &call.name, &arguments) {
        return rejected_call(
            call,
            error.code(),
            error.safe_message(),
            error.category(),
            error.retryable(),
            error.retry_after_ms(),
            events,
        );
    }
    let definition = tool_set
        .definitions
        .iter()
        .find(|definition| definition.name == call.name);
    let risk_level = definition.map_or(ToolRiskLevel::Low, |definition| definition.risk_level);
    let execution = definition.map_or_else(ToolExecutionPolicy::default, |definition| {
        definition.execution
    });
    PreparedCall::Ready {
        call: PreparedToolCall {
            call_id: call.id.clone(),
            name: call.name.clone(),
            arguments,
            public_arguments,
            risk_level,
            execution,
        },
        events,
    }
}

fn rejected_call(
    call: &ModelToolCall,
    code: &str,
    message: &str,
    category: ToolErrorCategory,
    retryable: bool,
    retry_after_ms: Option<u64>,
    mut events: Vec<AgentEvent>,
) -> PreparedCall {
    events.push(AgentEvent::ToolExecutionFailed {
        call_id: call.id.clone(),
        code: code.into(),
        message: message.into(),
        category,
        retryable,
        retry_after_ms,
    });
    PreparedCall::Rejected {
        events,
        result: ModelMessage::tool_result(
            call.id.clone(),
            tool_error_content(code, message, category, retryable, retry_after_ms),
        ),
    }
}

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

fn should_retry(policy: ToolExecutionPolicy, attempts: u32, retryable: bool) -> bool {
    retryable
        && policy.idempotency.permits_automatic_retry()
        && attempts < policy.retry.max_attempts
}

async fn wait_for_retry(cancellation: &RunCancellation, delay_ms: u64) -> bool {
    tokio::select! {
        biased;
        () = cancellation.cancelled() => false,
        () = tokio::time::sleep(Duration::from_millis(delay_ms)) => true,
    }
}

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

fn resumed_tool_content(inbox: &[FlowInboxItem]) -> Result<String, MachineError> {
    if inbox.is_empty() {
        return Err(machine_error(
            "tool_resume_event_missing",
            "the suspended tool resumed without a matching event",
            true,
        ));
    }
    let events = inbox
        .iter()
        .map(|item| {
            let event = &item.event;
            serde_json::json!({
                "sequence": event.sequence,
                "event_id": event.event.event_id,
                "topic": event.event.topic,
                "event_type": event.event.event_type,
                "schema_version": event.event.schema_version,
                "source": event.event.source,
                "subject": event.event.subject,
                "correlation_id": event.event.correlation_id,
                "causation_id": event.event.causation_id,
                "occurred_at_ms": event.event.occurred_at_ms,
                "payload": event.event.payload,
            })
        })
        .collect::<Vec<_>>();
    serde_json::to_string(&serde_json::json!({
        "ok": true,
        "events": events,
    }))
    .map_err(|_| {
        machine_error(
            "tool_resume_event_invalid",
            "the suspended tool event could not be encoded",
            false,
        )
    })
}

fn deferred_tool_call(call: ModelToolCall) -> (AgentEvent, ModelMessage) {
    let code = "tool_deferred_by_suspension";
    let message = "another tool call suspended this model step; request this tool again if it is still needed";
    (
        AgentEvent::ToolExecutionFailed {
            call_id: call.id.clone(),
            code: code.into(),
            message: message.into(),
            category: ToolErrorCategory::Conflict,
            retryable: true,
            retry_after_ms: None,
        },
        ModelMessage::tool_result(
            call.id,
            tool_error_content(code, message, ToolErrorCategory::Conflict, true, None),
        ),
    )
}

fn approval_wait(
    run_id: RunId,
    approval_id: ApprovalId,
    call: &PreparedToolCall,
    now_ms: i64,
) -> (Vec<WaitSpec>, Vec<EffectRequest>) {
    let approval_key = approval_id.to_string();
    let subscription_id = SubscriptionId::stable("tool-approval", &approval_key);
    let waits = vec![WaitSpec {
        wait_key: format!("tool-approval:{approval_key}"),
        subscription: CreateSubscription {
            subscription_id,
            owner: SubscriptionOwner::Run { run_id },
            scope: SubscriptionScope::Run { run_id },
            filter: EventFilter {
                topics: vec!["tool.approval.resolved".into()],
                sources: vec![EventSource::Gateway],
                correlation_id: Some(approval_key.clone()),
                ..EventFilter::default()
            },
            delivery: DeliveryTarget::WakeRun {
                run_id,
                wait_key: format!("tool-approval:{approval_key}"),
            },
            mode: SubscriptionMode::Once,
            start_position: StartPosition::Now,
            expires_at_ms: None,
            max_deliveries: Some(1),
            created_at_ms: now_ms,
        },
    }];
    let effects = vec![EffectRequest::PublishEvent {
        command: PublishEvent {
            event_id: EventId::stable("tool-approval-requested", &approval_key),
            topic: "tool.approval.requested".into(),
            event_type: "tool.approval.requested".into(),
            schema_version: 1,
            source: EventSource::Agent,
            subject: Some(format!("run/{run_id}")),
            correlation_id: Some(approval_key),
            causation_id: None,
            occurred_at_ms: now_ms,
            recorded_at_ms: now_ms,
            payload: serde_json::json!({
                "approval_id": approval_id,
                "call_id": call.call_id,
                "tool_name": call.name,
                "risk_level": call.risk_level,
                "arguments": call.public_arguments,
            }),
        },
    }];
    (waits, effects)
}

fn resolve_approval(
    run_id: RunId,
    approval_id: ApprovalId,
    inbox: &[FlowInboxItem],
) -> Result<ApprovalResolution, MachineError> {
    let approval_key = approval_id.to_string();
    let subject = format!("run/{run_id}");
    let mut matches = inbox.iter().filter(|item| {
        item.event.event.topic == "tool.approval.resolved"
            && item.event.event.correlation_id.as_deref() == Some(approval_key.as_str())
            && item.event.event.subject.as_deref() == Some(subject.as_str())
    });
    let Some(item) = matches.next() else {
        return Err(machine_error(
            "approval_event_missing",
            "the approval checkpoint resumed without its resolution event",
            true,
        ));
    };
    if matches.next().is_some() {
        return Err(machine_error(
            "approval_event_conflict",
            "the approval checkpoint received more than one resolution event",
            false,
        ));
    }
    serde_json::from_value(item.event.event.payload.clone()).map_err(|_| {
        machine_error(
            "approval_event_invalid",
            "the approval resolution event payload is invalid",
            false,
        )
    })
}

fn encode_checkpoint(state: AgentLoopCheckpoint) -> Result<CheckpointEnvelope, MachineError> {
    let payload = serde_json::to_value(state).map_err(|_| {
        machine_error(
            "checkpoint_encode_failed",
            "the agent checkpoint could not be encoded",
            false,
        )
    })?;
    let checkpoint = CheckpointEnvelope {
        agent_kind: CHECKPOINT_KIND.into(),
        schema_version: CHECKPOINT_VERSION,
        codec: CheckpointCodec::Json,
        payload,
    };
    checkpoint
        .validate()
        .map_err(|error| machine_error("checkpoint_invalid", error.to_string(), false))?;
    Ok(checkpoint)
}

fn decode_checkpoint(checkpoint: CheckpointEnvelope) -> Result<AgentLoopCheckpoint, MachineError> {
    checkpoint
        .validate()
        .map_err(|error| machine_error("checkpoint_invalid", error.to_string(), false))?;
    if checkpoint.agent_kind != CHECKPOINT_KIND
        || checkpoint.schema_version != CHECKPOINT_VERSION
        || checkpoint.codec != CheckpointCodec::Json
    {
        return Err(machine_error(
            "checkpoint_incompatible",
            "the checkpoint is not compatible with this agent machine",
            false,
        ));
    }
    serde_json::from_value(checkpoint.payload).map_err(|_| {
        machine_error(
            "checkpoint_decode_failed",
            "the agent checkpoint payload is invalid",
            false,
        )
    })
}

fn failed_stream(error: MachineError) -> MachineStream {
    Box::pin(futures_util::stream::once(async move {
        MachineOutput::Yield(StepOutcome::Failed { error })
    }))
}

fn machine_error(
    code: impl Into<String>,
    message: impl Into<String>,
    retryable: bool,
) -> MachineError {
    MachineError {
        code: code.into(),
        message: message.into(),
        retryable,
    }
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
