//! Native effect runner for the pure Agent loop reducer.
//!
//! This is intentionally an adapter: all decisions remain in `reducer.rs`.
//! Tokio, ModelPort, ToolPort, timers, and durable inbox translation live here.

use std::{collections::HashSet, sync::Arc, time::Duration};

use async_stream::stream;
use futures_util::{StreamExt, stream::FuturesUnordered};

use super::{
    AGENT_LOOP_CHECKPOINT_SCHEMA_VERSION, AgentLoop, AgentLoopEffect, AgentLoopEffectKind,
    AgentLoopInput, AgentLoopOutcome, AgentLoopReducer, AgentLoopReducerConfig,
    AgentLoopReducerState, AgentLoopStart, PortableToolError, ToolInvocation, ToolInvocationResult,
    ToolValidation,
};
use crate::{
    event_runtime::{
        CreateSubscription, DeliveryTarget, EventFilter, EventId, EventSource, PublishEvent,
        StartPosition, SubscriptionId, SubscriptionMode, SubscriptionOwner, SubscriptionScope,
    },
    harness::{
        ApprovalId, ApprovalResolution, CheckpointCodec, CheckpointEnvelope, EffectRequest,
        FlowInboxItem, MachineError, MachineOutput, MachineResumeRequest, MachineStartRequest,
        MachineStream, ModelPort, RunCancellation, RunId, StartJob, StepOutcome, ToolCallRequest,
        ToolCompletion, ToolErrorCategory, ToolExecutionPolicy, ToolPort, ToolSuspension, WaitSpec,
    },
};

const CHECKPOINT_KIND: &str = "agent-loop";

pub(super) fn initial_checkpoint<P, T>(
    agent: &AgentLoop<P, T>,
    request: &MachineStartRequest,
) -> Result<CheckpointEnvelope, MachineError> {
    encode_checkpoint(initial_transition(agent, request).state)
}

pub(super) fn start<P, T>(agent: &AgentLoop<P, T>, request: MachineStartRequest) -> MachineStream
where
    P: ModelPort,
    T: ToolPort,
{
    let state = initial_transition(agent, &request).state;
    drive_activation(
        Arc::clone(&agent.provider),
        Arc::clone(&agent.tools),
        state,
        Vec::new(),
        request.activated_at_ms,
        request.cancellation,
    )
}

pub(super) fn resume<P, T>(agent: &AgentLoop<P, T>, request: MachineResumeRequest) -> MachineStream
where
    P: ModelPort,
    T: ToolPort,
{
    let state = match decode_checkpoint(request.checkpoint) {
        Ok(state) if state.run_id == request.run_id => state,
        Ok(_) => {
            return failed_stream(machine_error(
                "checkpoint_run_mismatch",
                "the checkpoint belongs to a different run",
                false,
            ));
        }
        Err(error) => return failed_stream(error),
    };
    drive_activation(
        Arc::clone(&agent.provider),
        Arc::clone(&agent.tools),
        state,
        request.inbox,
        request.activated_at_ms,
        request.cancellation,
    )
}

fn initial_transition<P, T>(
    agent: &AgentLoop<P, T>,
    request: &MachineStartRequest,
) -> super::AgentLoopTransition {
    let host_max_steps = agent.max_steps.max(1);
    let max_steps = request.max_steps.map_or(host_max_steps, |requested| {
        requested.clamp(1, host_max_steps)
    });
    AgentLoopReducer::start(AgentLoopStart {
        run_id: request.run_id,
        input: request.input.clone(),
        attachments: request.attachments.clone(),
        prior_messages: request.prior_messages.clone(),
        context_fingerprint: request.context_fingerprint.clone(),
        config: AgentLoopReducerConfig {
            model: agent.model.clone(),
            system_prompt: agent.system_prompt.clone(),
            max_output_tokens: agent.max_output_tokens,
            max_steps,
            allowed_tools: request.allowed_tools.clone(),
            allow_run_adf: request.allow_run_adf,
            approval_policy: agent.approval_policy,
            tool_call_strategy: agent.tool_call_strategy,
            model_timeout_ms: duration_ms(agent.model_timeout),
            tool_timeout_ms: duration_ms(agent.tool_timeout),
        },
    })
}

fn drive_activation<P, T>(
    provider: Arc<P>,
    tools: Arc<T>,
    mut state: AgentLoopReducerState,
    inbox: Vec<FlowInboxItem>,
    activated_at_ms: i64,
    cancellation: RunCancellation,
) -> MachineStream
where
    P: ModelPort,
    T: ToolPort,
{
    Box::pin(stream! {
        let mut checkpoint_after_tool_round = false;
        if cancellation.is_cancelled() {
            let transition = AgentLoopReducer::dispatch(state, AgentLoopInput::Cancelled);
            state = transition.state;
        } else if state.is_waiting_for_approval() {
            let Some(approval_effect) = state.pending_effects().into_iter().next() else {
                yield MachineOutput::Yield(StepOutcome::Failed {
                    error: machine_error(
                        "checkpoint_invalid",
                        "the approval checkpoint has no matching effect",
                        false,
                    ),
                });
                return;
            };
            let AgentLoopEffectKind::RequestApproval {
                approval_id,
                run_id,
                ..
            } = approval_effect.kind.clone()
            else {
                yield MachineOutput::Yield(StepOutcome::Failed {
                    error: machine_error(
                        "checkpoint_invalid",
                        "the approval checkpoint contains the wrong effect",
                        false,
                    ),
                });
                return;
            };
            let effect_id = approval_effect.effect_id.clone();
            if inbox.is_empty() {
                let (waits, effects) =
                    approval_wait_from_effect(&approval_effect, activated_at_ms);
                let checkpoint = match encode_checkpoint(state) {
                    Ok(checkpoint) => checkpoint,
                    Err(error) => {
                        yield MachineOutput::Yield(StepOutcome::Failed { error });
                        return;
                    }
                };
                yield MachineOutput::Yield(StepOutcome::Suspend {
                    checkpoint,
                    waits,
                    effects,
                });
                return;
            }
            let resolution = match resolve_approval(run_id, approval_id, &inbox) {
                Ok(resolution) => resolution,
                Err(error) => {
                    yield MachineOutput::Yield(StepOutcome::Failed { error });
                    return;
                }
            };
            let transition = AgentLoopReducer::dispatch(
                state,
                AgentLoopInput::ApprovalResolved {
                    effect_id,
                    resolution,
                },
            );
            for event in transition.events {
                yield MachineOutput::Event(event);
            }
            state = transition.state;
            checkpoint_after_tool_round = true;
        } else if state.is_waiting_for_tool_resume() {
            let Some(call_id) = state.pending_tool_resume_call_id().map(str::to_owned) else {
                yield MachineOutput::Yield(StepOutcome::Failed {
                    error: machine_error(
                        "checkpoint_invalid",
                        "the suspended tool checkpoint has no call id",
                        false,
                    ),
                });
                return;
            };
            let content = match resumed_tool_content(&inbox) {
                Ok(content) => content,
                Err(error) => {
                    yield MachineOutput::Yield(StepOutcome::Failed { error });
                    return;
                }
            };
            let transition = AgentLoopReducer::dispatch(
                state,
                AgentLoopInput::ToolResumed { call_id, content },
            );
            for event in transition.events {
                yield MachineOutput::Event(event);
            }
            state = transition.state;
            checkpoint_after_tool_round = true;
        } else if !inbox.is_empty() {
            yield MachineOutput::Yield(StepOutcome::Failed {
                error: machine_error(
                    "unexpected_machine_event",
                    "the agent checkpoint has no handler for the delivered event",
                    false,
                ),
            });
            return;
        }

        loop {
            match state.outcome() {
                AgentLoopOutcome::Complete { finish_reason } => {
                    tools.close_run(state.run_id);
                    yield MachineOutput::Yield(StepOutcome::Complete { finish_reason });
                    return;
                }
                AgentLoopOutcome::Failed { error } => {
                    tools.close_run(state.run_id);
                    yield MachineOutput::Yield(StepOutcome::Failed { error });
                    return;
                }
                AgentLoopOutcome::Cancelled => {
                    tools.close_run(state.run_id);
                    yield MachineOutput::Yield(StepOutcome::Cancelled);
                    return;
                }
                AgentLoopOutcome::Suspended { waits, effects } => {
                    let checkpoint = match encode_checkpoint(state) {
                        Ok(checkpoint) => checkpoint,
                        Err(error) => {
                            yield MachineOutput::Yield(StepOutcome::Failed { error });
                            return;
                        }
                    };
                    yield MachineOutput::Yield(StepOutcome::Suspend {
                        checkpoint,
                        waits,
                        effects,
                    });
                    return;
                }
                AgentLoopOutcome::Running => {}
            }

            if cancellation.is_cancelled() {
                let transition = AgentLoopReducer::dispatch(state, AgentLoopInput::Cancelled);
                state = transition.state;
                continue;
            }

            let pending = state.pending_effects();
            let Some(first) = pending.first().cloned() else {
                yield MachineOutput::Yield(StepOutcome::Failed {
                    error: machine_error(
                        "reducer_stalled",
                        "the reducer is running without a pending effect",
                        false,
                    ),
                });
                return;
            };
            if checkpoint_after_tool_round
                && matches!(first.kind, AgentLoopEffectKind::LoadToolSet { .. })
            {
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
            match first.kind.clone() {
                AgentLoopEffectKind::LoadToolSet { run_id, .. } => {
                    let input = match tools.tool_set_snapshot(run_id) {
                        Ok(tool_set) => AgentLoopInput::ToolSetLoaded {
                            effect_id: first.effect_id,
                            tool_set,
                        },
                        Err(error) => AgentLoopInput::ToolSetFailed {
                            effect_id: first.effect_id,
                            error: machine_error(
                                error.code(),
                                error.safe_message(),
                                error.retryable(),
                            ),
                        },
                    };
                    let transition = AgentLoopReducer::dispatch(state, input);
                    for event in transition.events {
                        yield MachineOutput::Event(event);
                    }
                    state = transition.state;
                }
                AgentLoopEffectKind::InvokeModel {
                    request,
                    timeout_ms,
                    ..
                } => {
                    let effect_id = first.effect_id;
                    let mut model_events = provider.stream(request);
                    let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
                    loop {
                        let input = tokio::select! {
                            biased;
                            () = cancellation.cancelled() => AgentLoopInput::Cancelled,
                            result = tokio::time::timeout_at(deadline, model_events.next()) => match result {
                                Ok(Some(event)) => AgentLoopInput::ModelEvent {
                                    effect_id: effect_id.clone(),
                                    event,
                                },
                                Ok(None) => AgentLoopInput::ModelStreamEnded {
                                    effect_id: effect_id.clone(),
                                },
                                Err(_) => AgentLoopInput::ModelTimedOut {
                                    effect_id: effect_id.clone(),
                                },
                            },
                        };
                        let transition = AgentLoopReducer::dispatch(state, input);
                        for event in transition.events {
                            yield MachineOutput::Event(event);
                        }
                        state = transition.state;
                        if !state
                            .pending_effects()
                            .iter()
                            .any(|effect| effect.effect_id == effect_id)
                        {
                            checkpoint_after_tool_round = state
                                .pending_effects()
                                .first()
                                .is_some_and(|effect| {
                                    !matches!(
                                        effect.kind,
                                        AgentLoopEffectKind::InvokeModel { .. }
                                    )
                                });
                            break;
                        }
                    }
                }
                AgentLoopEffectKind::ValidateTools {
                    run_id,
                    tool_set_revision,
                    calls,
                } => {
                    let results = calls
                        .into_iter()
                        .map(|call| {
                            let error = tools
                                .validate_at(
                                    run_id,
                                    tool_set_revision,
                                    &call.name,
                                    &call.arguments,
                                )
                                .err()
                                .map(|error| PortableToolError {
                                    code: error.code().into(),
                                    message: error.safe_message().into(),
                                    category: error.category(),
                                    retryable: error.retryable(),
                                    retry_after_ms: error.retry_after_ms(),
                                });
                            ToolValidation {
                                call_id: call.call_id,
                                error,
                            }
                        })
                        .collect();
                    let transition = AgentLoopReducer::dispatch(
                        state,
                        AgentLoopInput::ToolsValidated {
                            effect_id: first.effect_id,
                            results,
                        },
                    );
                    for event in transition.events {
                        yield MachineOutput::Event(event);
                    }
                    state = transition.state;
                }
                AgentLoopEffectKind::RequestApproval { .. } => {
                    let checkpoint = match encode_checkpoint(state) {
                        Ok(checkpoint) => checkpoint,
                        Err(error) => {
                            yield MachineOutput::Yield(StepOutcome::Failed { error });
                            return;
                        }
                    };
                    let (waits, effects) = approval_wait_from_effect(&first, activated_at_ms);
                    yield MachineOutput::Yield(StepOutcome::Suspend {
                        checkpoint,
                        waits,
                        effects,
                    });
                    return;
                }
                AgentLoopEffectKind::InvokeTool { .. } => {
                    let mut running = FuturesUnordered::new();
                    for effect in pending {
                        let AgentLoopEffectKind::InvokeTool {
                            run_id,
                            tool_set_revision,
                            call,
                            timeout_ms,
                        } = effect.kind
                        else {
                            yield MachineOutput::Yield(StepOutcome::Failed {
                                error: machine_error(
                                    "checkpoint_invalid",
                                    "a tool batch contains a non-tool effect",
                                    false,
                                ),
                            });
                            return;
                        };
                        let tools = Arc::clone(&tools);
                        let cancellation = cancellation.clone();
                        running.push(async move {
                            let result = execute_tool_effect(
                                tools,
                                run_id,
                                tool_set_revision,
                                call,
                                cancellation,
                                Duration::from_millis(timeout_ms),
                            )
                            .await;
                            (effect.effect_id, result)
                        });
                    }
                    while let Some((effect_id, result)) = running.next().await {
                        let result = match result {
                            HostToolResult::Completed(result) => result,
                            HostToolResult::Cancelled => {
                                let transition = AgentLoopReducer::dispatch(
                                    state,
                                    AgentLoopInput::Cancelled,
                                );
                                state = transition.state;
                                break;
                            }
                        };
                        let transition = AgentLoopReducer::dispatch(
                            state,
                            AgentLoopInput::ToolFinished { effect_id, result },
                        );
                        for event in transition.events {
                            yield MachineOutput::Event(event);
                        }
                        state = transition.state;
                    }
                }
            }
        }
    })
}

enum HostToolResult {
    Completed(ToolInvocationResult),
    Cancelled,
}

async fn execute_tool_effect<T: ToolPort>(
    tools: Arc<T>,
    run_id: RunId,
    tool_set_revision: u64,
    call: ToolInvocation,
    cancellation: RunCancellation,
    timeout: Duration,
) -> HostToolResult {
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
            () = cancellation.cancelled() => return HostToolResult::Cancelled,
            result = tokio::time::timeout(timeout, tools.call_at(tool_set_revision, request)) => result,
        };
        match result {
            Err(_) if should_retry(call.execution, attempts, true) => {
                if !wait_for_retry(
                    &cancellation,
                    call.execution.retry.delay_for_attempt(attempts),
                )
                .await
                {
                    return HostToolResult::Cancelled;
                }
            }
            Err(_) => {
                return HostToolResult::Completed(ToolInvocationResult::Failed {
                    error: PortableToolError {
                        code: "tool_timeout".into(),
                        message: "tool execution exceeded the configured timeout".into(),
                        category: ToolErrorCategory::Timeout,
                        retryable: true,
                        retry_after_ms: None,
                    },
                });
            }
            Ok(Err(error)) if should_retry(call.execution, attempts, error.retryable()) => {
                let delay = error.retry_after_ms().map_or_else(
                    || call.execution.retry.delay_for_attempt(attempts),
                    |requested| requested.min(call.execution.retry.max_backoff_ms),
                );
                if !wait_for_retry(&cancellation, delay).await {
                    return HostToolResult::Cancelled;
                }
            }
            Ok(Err(error)) => {
                return HostToolResult::Completed(ToolInvocationResult::Failed {
                    error: PortableToolError {
                        code: error.code().into(),
                        message: error.safe_message().into(),
                        category: error.category(),
                        retryable: error.retryable(),
                        retry_after_ms: error.retry_after_ms(),
                    },
                });
            }
            Ok(Ok(output)) => {
                let Some(suspension) = output.suspension else {
                    return HostToolResult::Completed(ToolInvocationResult::Completed {
                        content: output.content,
                    });
                };
                let validation_error = if call.execution.completion != ToolCompletion::MaySuspend {
                    Some((
                        "unexpected_tool_suspension",
                        "the tool suspended without declaring may_suspend execution semantics"
                            .into(),
                    ))
                } else if !output.content.is_empty() {
                    Some((
                        "invalid_tool_suspension",
                        "a suspended tool cannot also return immediate content".into(),
                    ))
                } else {
                    validate_tool_suspension(run_id, &suspension)
                        .err()
                        .map(|message| ("invalid_tool_suspension", message))
                };
                if let Some((code, message)) = validation_error {
                    return HostToolResult::Completed(ToolInvocationResult::Failed {
                        error: PortableToolError {
                            code: code.into(),
                            message,
                            category: ToolErrorCategory::Internal,
                            retryable: false,
                            retry_after_ms: None,
                        },
                    });
                }
                return HostToolResult::Completed(ToolInvocationResult::Suspended { suspension });
            }
        }
    }
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
    if suspension.waits.len() > MAX_WAITS || suspension.effects.len() > MAX_EFFECTS {
        return Err("a suspended tool exceeded the portable wait/effect limit".into());
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
        if let EffectRequest::StartJob {
            command:
                StartJob {
                    run_id: effect_run_id,
                    ..
                },
        } = effect
            && *effect_run_id != run_id
        {
            return Err("tool job effects must belong to the current run".into());
        }
    }
    Ok(())
}

fn approval_wait_from_effect(
    effect: &AgentLoopEffect,
    now_ms: i64,
) -> (Vec<WaitSpec>, Vec<EffectRequest>) {
    let AgentLoopEffectKind::RequestApproval {
        approval_id,
        run_id,
        call,
    } = &effect.kind
    else {
        return (Vec::new(), Vec::new());
    };
    let approval_key = approval_id.to_string();
    let subscription_id = SubscriptionId::stable("tool-approval", &approval_key);
    let waits = vec![WaitSpec {
        wait_key: format!("tool-approval:{approval_key}"),
        subscription: CreateSubscription {
            subscription_id,
            owner: SubscriptionOwner::Run { run_id: *run_id },
            scope: SubscriptionScope::Run { run_id: *run_id },
            filter: EventFilter {
                topics: vec!["tool.approval.resolved".into()],
                sources: vec![EventSource::Gateway],
                correlation_id: Some(approval_key.clone()),
                ..EventFilter::default()
            },
            delivery: DeliveryTarget::WakeRun {
                run_id: *run_id,
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

fn encode_checkpoint(state: AgentLoopReducerState) -> Result<CheckpointEnvelope, MachineError> {
    let payload = serde_json::to_value(state).map_err(|_| {
        machine_error(
            "checkpoint_encode_failed",
            "the agent reducer checkpoint could not be encoded",
            false,
        )
    })?;
    let checkpoint = CheckpointEnvelope {
        agent_kind: CHECKPOINT_KIND.into(),
        schema_version: AGENT_LOOP_CHECKPOINT_SCHEMA_VERSION,
        codec: CheckpointCodec::Json,
        payload,
    };
    checkpoint
        .validate()
        .map_err(|error| machine_error("checkpoint_invalid", error.to_string(), false))?;
    Ok(checkpoint)
}

fn decode_checkpoint(
    checkpoint: CheckpointEnvelope,
) -> Result<AgentLoopReducerState, MachineError> {
    checkpoint
        .validate()
        .map_err(|error| machine_error("checkpoint_invalid", error.to_string(), false))?;
    if checkpoint.agent_kind != CHECKPOINT_KIND
        || checkpoint.schema_version != AGENT_LOOP_CHECKPOINT_SCHEMA_VERSION
        || checkpoint.codec != CheckpointCodec::Json
    {
        return Err(machine_error(
            "checkpoint_incompatible",
            "the checkpoint is not compatible with the Agent loop reducer",
            false,
        ));
    }
    serde_json::from_value(checkpoint.payload).map_err(|_| {
        machine_error(
            "checkpoint_decode_failed",
            "the Agent loop reducer checkpoint payload is invalid",
            false,
        )
    })
}

fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
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
