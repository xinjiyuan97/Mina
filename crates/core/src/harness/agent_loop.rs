use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
    time::Duration,
};

use async_stream::stream;
use futures_util::{StreamExt, stream::FuturesUnordered};
use serde::{Deserialize, Serialize};

use crate::harness::tool::{digest_only_arguments, digest_only_bytes};
use crate::harness::{
    Agent, AgentEvent, AgentEventStream, AgentMetadata, ApprovalDecision, ApprovalError,
    ApprovalId, ApprovalPort, ApprovalRequest, ApprovalResolution, FinishReason, ModelEvent,
    ModelMessage, ModelPort, ModelRequest, ModelToolCall, OutputChannel, RejectAllApprovals,
    RunCancellation, RunId, RunRequest, TokenUsage, TokenUsageSource, ToolApprovalPolicy,
    ToolArgumentVisibility, ToolBindingKind, ToolCallRequest, ToolCompletion, ToolConcurrency,
    ToolError, ToolErrorCategory, ToolExecutionPolicy, ToolOutput, ToolPort, ToolRiskLevel,
    ToolSetSnapshot,
};

mod machine;

/// A bounded model/tool loop for one independent run.
///
/// Every model invocation has its own terminal `ModelEvent`, while the Agent
/// continues across tool results and emits exactly one terminal `AgentEvent`.
pub struct AgentLoop<P, T> {
    provider: Arc<P>,
    tools: Arc<T>,
    approvals: Arc<dyn ApprovalPort>,
    approval_policy: ToolApprovalPolicy,
    model: String,
    system_prompt: String,
    max_output_tokens: Option<u32>,
    max_steps: u32,
    model_timeout: Duration,
    tool_timeout: Duration,
    tool_call_strategy: ToolCallStrategy,
}

/// Host-selected scheduling for one model response containing multiple tool
/// calls. ParallelSafe only overlaps tools that explicitly opt in and cannot
/// suspend; every other call remains ordered and exclusive.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ToolCallStrategy {
    #[default]
    Sequential,
    ParallelSafe,
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
            approval_policy: ToolApprovalPolicy::default(),
            model: model.into(),
            system_prompt: system_prompt.into(),
            max_output_tokens,
            max_steps,
            model_timeout: DEFAULT_MODEL_TIMEOUT,
            tool_timeout: DEFAULT_TOOL_TIMEOUT,
            tool_call_strategy: ToolCallStrategy::Sequential,
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

    #[must_use]
    pub const fn with_approval_policy(mut self, policy: ToolApprovalPolicy) -> Self {
        self.approval_policy = policy;
        self
    }

    #[must_use]
    pub const fn with_tool_call_strategy(mut self, strategy: ToolCallStrategy) -> Self {
        self.tool_call_strategy = strategy;
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
            .with_capability("tool_set_snapshots")
            .with_capability("run_scoped_dynamic_tools")
            .with_capability("tool_approval")
    }

    fn run(&self, request: RunRequest) -> AgentEventStream {
        let provider = Arc::clone(&self.provider);
        let tools = Arc::clone(&self.tools);
        let approvals = Arc::clone(&self.approvals);
        let approval_policy = self.approval_policy;
        let model = self.model.clone();
        let max_output_tokens = self.max_output_tokens;
        let host_max_steps = self.max_steps.max(1);
        let max_steps = request.max_steps.map_or(host_max_steps, |requested| {
            requested.clamp(1, host_max_steps)
        });
        let model_timeout = self.model_timeout;
        let tool_timeout = self.tool_timeout;
        let tool_call_strategy = self.tool_call_strategy;
        let allowed_tools = request.allowed_tools.clone();
        let allow_run_adf = request.allow_run_adf;
        let cancellation = request.cancellation.clone();

        let mut messages = Vec::with_capacity(request.prior_messages.len() + 4);
        if !self.system_prompt.trim().is_empty() {
            messages.push(ModelMessage::system(self.system_prompt.clone()));
        }
        messages.extend(request.prior_messages);
        messages.push(ModelMessage::user(request.input));

        Box::pin(stream! {
            let _tool_session_guard = RunToolSessionGuard {
                tools: Arc::clone(&tools),
                run_id: request.run_id,
            };
            let mut completed_usage = TokenUsage {
                input_tokens: 0,
                output_tokens: 0,
                total_tokens: 0,
                source: TokenUsageSource::ProviderReported,
            };
            let mut previous_tool_set = None::<(u64, String)>;

            for step in 0..max_steps {
                let tool_set = match tools.tool_set_snapshot(request.run_id).and_then(|snapshot| {
                    snapshot.retain(|binding| {
                        let statically_allowed = allowed_tools
                            .as_ref()
                            .is_none_or(|allowed| allowed.contains(&binding.name));
                        match binding.kind {
                            ToolBindingKind::Static => statically_allowed,
                            ToolBindingKind::AdfManagement => {
                                allow_run_adf && statically_allowed
                            }
                            ToolBindingKind::AgentDefined => allow_run_adf,
                        }
                    })
                }) {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        yield AgentEvent::failed(
                            error.code(),
                            error.safe_message(),
                            error.retryable(),
                        );
                        return;
                    }
                };
                let tool_set_identity = (tool_set.revision, tool_set.digest.clone());
                if previous_tool_set.as_ref() != Some(&tool_set_identity) {
                    yield AgentEvent::ToolSetUpdated {
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
                    };
                    previous_tool_set = Some(tool_set_identity);
                }
                let tool_definitions = tool_set.definitions.clone();
                let allowed_tool_names: HashSet<_> = tool_definitions
                    .iter()
                    .map(|tool| tool.name.clone())
                    .collect();
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
                            let visibility = tool_set
                                .binding(&call.name)
                                .map_or(ToolArgumentVisibility::DigestOnly, |binding| {
                                    binding.argument_visibility
                                });
                            if !delta.is_empty() && visibility == ToolArgumentVisibility::Full {
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
                    messages.push(ModelMessage::assistant_tool_calls(
                        assistant_text,
                        assistant_reasoning,
                        pending_calls.clone(),
                    ));
                    for call in pending_calls {
                        let code = "agent_step_limit_exceeded";
                        let message = "the model step limit was reached; this tool was not executed";
                        yield AgentEvent::ToolExecutionFailed {
                            call_id: call.id.clone(),
                            code: code.into(),
                            message: message.into(),
                            category: ToolErrorCategory::ResourceExhausted,
                            retryable: false,
                            retry_after_ms: None,
                        };
                        messages.push(ModelMessage::tool_result(
                            call.id,
                            step_limit_tool_error_content(code, message),
                        ));
                    }

                    let mut final_events = final_summary_stream(
                        Arc::clone(&provider),
                        ModelRequest {
                            run_id: request.run_id,
                            model: model.clone(),
                            messages,
                            tools: Vec::new(),
                            max_output_tokens,
                        },
                        cancellation.clone(),
                        model_timeout,
                        completed_usage,
                    );
                    while let Some(event) = final_events.next().await {
                        yield event;
                    }
                    return;
                }

                messages.push(ModelMessage::assistant_tool_calls(
                    assistant_text,
                    assistant_reasoning,
                    pending_calls.clone(),
                ));

                let mut prepared_calls = pending_calls
                    .into_iter()
                    .map(|call| {
                        Some(prepare_legacy_call(
                            &tools,
                            request.run_id,
                            &tool_set,
                            &allowed_tool_names,
                            call,
                        ))
                    })
                    .collect::<Vec<_>>();
                let mut index = 0_usize;
                while index < prepared_calls.len() {
                    let prepared = prepared_calls[index]
                        .take()
                        .expect("each legacy tool call is consumed once");
                    let call = match prepared {
                        LegacyPreparedCall::Ready { call, events } => {
                            for event in events {
                                yield event;
                            }
                            call
                        }
                        LegacyPreparedCall::Rejected { events, result } => {
                            for event in events {
                                yield event;
                            }
                            messages.push(result);
                            index = index.saturating_add(1);
                            continue;
                        }
                    };

                    if legacy_parallel_candidate(tool_call_strategy, approval_policy, &call) {
                        let mut batch = vec![(index, call.clone())];
                        let mut next = index.saturating_add(1);
                        while next < prepared_calls.len()
                            && prepared_calls[next]
                                .as_ref()
                                .is_some_and(|call| {
                                    legacy_prepared_is_parallel_candidate(approval_policy, call)
                                })
                        {
                            let LegacyPreparedCall::Ready { call, events } = prepared_calls[next]
                                .take()
                                .expect("parallel candidate should exist")
                            else {
                                unreachable!("parallel candidate must be ready");
                            };
                            for event in events {
                                yield event;
                            }
                            batch.push((next, call));
                            next = next.saturating_add(1);
                        }

                        if batch.len() > 1 {
                            let order = batch.iter().map(|(position, _)| *position).collect::<Vec<_>>();
                            for (_, call) in &batch {
                                yield AgentEvent::ToolExecutionStarted {
                                    call_id: call.call_id.clone(),
                                    arguments: call.public_arguments.clone(),
                                };
                            }
                            let mut running = FuturesUnordered::new();
                            for (position, call) in batch {
                                let tools = Arc::clone(&tools);
                                let cancellation = cancellation.clone();
                                running.push(async move {
                                    let poll = legacy_execute_tool(
                                        tools,
                                        request.run_id,
                                        tool_set.revision,
                                        &call,
                                        cancellation,
                                        tool_timeout,
                                    )
                                    .await;
                                    (position, call, poll)
                                });
                            }
                            let mut results = BTreeMap::new();
                            while let Some((position, call, poll)) = running.next().await {
                                match project_legacy_tool_result(call, poll) {
                                    LegacyToolResult::Cancelled => {
                                        yield AgentEvent::Cancelled;
                                        return;
                                    }
                                    LegacyToolResult::Finished { event, result } => {
                                        yield event;
                                        results.insert(position, result);
                                    }
                                }
                            }
                            for position in order {
                                let result = results
                                    .remove(&position)
                                    .expect("each parallel legacy tool must produce a result");
                                messages.push(*result);
                            }
                            index = next;
                            continue;
                        }
                    }

                    if approval_policy.requires_review(call.risk_level) {
                        let approval_id = ApprovalId::new();
                        let approval = approvals.request(ApprovalRequest {
                            approval_id,
                            run_id: request.run_id,
                            call_id: call.call_id.clone(),
                            tool_name: call.name.clone(),
                            risk_level: call.risk_level,
                            arguments: call.public_arguments.clone(),
                        });
                        yield AgentEvent::ApprovalRequested {
                            approval_id,
                            call_id: call.call_id.clone(),
                            tool_name: call.name.clone(),
                            risk_level: call.risk_level,
                            arguments: call.public_arguments.clone(),
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
                            call_id: call.call_id.clone(),
                            resolution: resolution.clone(),
                        };

                        if resolution.decision == ApprovalDecision::Deny {
                            let code = "tool_rejected";
                            let message = rejection_message(&resolution);
                            yield AgentEvent::ToolExecutionFailed {
                                call_id: call.call_id.clone(),
                                code: code.into(),
                                message: message.clone(),
                                category: ToolErrorCategory::PermissionDenied,
                                retryable: false,
                                retry_after_ms: None,
                            };
                            messages.push(ModelMessage::tool_result(
                                call.call_id,
                                tool_error_content(
                                    code,
                                    &message,
                                    ToolErrorCategory::PermissionDenied,
                                    false,
                                    None,
                                ),
                            ));
                            index = index.saturating_add(1);
                            continue;
                        }
                    }

                    yield AgentEvent::ToolExecutionStarted {
                        call_id: call.call_id.clone(),
                        arguments: call.public_arguments.clone(),
                    };

                    let tool_poll = legacy_execute_tool(
                        Arc::clone(&tools),
                        request.run_id,
                        tool_set.revision,
                        &call,
                        cancellation.clone(),
                        tool_timeout,
                    )
                    .await;
                    match project_legacy_tool_result(call, tool_poll) {
                        LegacyToolResult::Cancelled => {
                            yield AgentEvent::Cancelled;
                            return;
                        }
                        LegacyToolResult::Finished { event, result } => {
                            yield event;
                            messages.push(*result);
                        }
                    }
                    index = index.saturating_add(1);
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

struct RunToolSessionGuard<T>
where
    T: ToolPort,
{
    tools: Arc<T>,
    run_id: RunId,
}

impl<T> Drop for RunToolSessionGuard<T>
where
    T: ToolPort,
{
    fn drop(&mut self) {
        self.tools.close_run(self.run_id);
    }
}

#[derive(Clone)]
struct LegacyPreparedToolCall {
    call_id: String,
    name: String,
    arguments: serde_json::Value,
    public_arguments: serde_json::Value,
    risk_level: ToolRiskLevel,
    execution: ToolExecutionPolicy,
}

enum LegacyPreparedCall {
    Ready {
        call: LegacyPreparedToolCall,
        events: Vec<AgentEvent>,
    },
    Rejected {
        events: Vec<AgentEvent>,
        result: ModelMessage,
    },
}

enum LegacyToolResult {
    Cancelled,
    Finished {
        event: AgentEvent,
        result: Box<ModelMessage>,
    },
}

fn prepare_legacy_call<T: ToolPort>(
    tools: &Arc<T>,
    run_id: RunId,
    tool_set: &ToolSetSnapshot,
    allowed_tool_names: &HashSet<String>,
    call: ModelToolCall,
) -> LegacyPreparedCall {
    if !allowed_tool_names.contains(&call.name) {
        return reject_legacy_call(
            call,
            "tool_not_allowed",
            "model requested a tool outside this run's capability set",
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
    let arguments: serde_json::Value = match serde_json::from_str(&call.arguments) {
        Ok(arguments) => arguments,
        Err(_) => {
            let events = if visibility == ToolArgumentVisibility::DigestOnly {
                vec![AgentEvent::ToolCallArgumentsDelta {
                    call_id: call.id.clone(),
                    delta: digest_only_bytes(call.arguments.as_bytes()).to_string(),
                }]
            } else {
                Vec::new()
            };
            return reject_legacy_call(
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
        ToolArgumentVisibility::DigestOnly => digest_only_arguments(&arguments),
    };
    let mut events = Vec::new();
    if visibility == ToolArgumentVisibility::DigestOnly {
        events.push(AgentEvent::ToolCallArgumentsDelta {
            call_id: call.id.clone(),
            delta: public_arguments.to_string(),
        });
    }
    if let Err(error) = tools.validate_at(run_id, tool_set.revision, &call.name, &arguments) {
        return reject_legacy_call(
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
    LegacyPreparedCall::Ready {
        call: LegacyPreparedToolCall {
            call_id: call.id,
            name: call.name,
            arguments,
            public_arguments,
            risk_level: definition.map_or(ToolRiskLevel::Low, |value| value.risk_level),
            execution: definition
                .map_or_else(ToolExecutionPolicy::default, |value| value.execution),
        },
        events,
    }
}

#[allow(clippy::too_many_arguments)]
fn reject_legacy_call(
    call: ModelToolCall,
    code: &str,
    message: &str,
    category: ToolErrorCategory,
    retryable: bool,
    retry_after_ms: Option<u64>,
    mut events: Vec<AgentEvent>,
) -> LegacyPreparedCall {
    events.push(AgentEvent::ToolExecutionFailed {
        call_id: call.id.clone(),
        code: code.into(),
        message: message.into(),
        category,
        retryable,
        retry_after_ms,
    });
    LegacyPreparedCall::Rejected {
        events,
        result: ModelMessage::tool_result(
            call.id,
            tool_error_content(code, message, category, retryable, retry_after_ms),
        ),
    }
}

fn legacy_parallel_candidate(
    strategy: ToolCallStrategy,
    approval_policy: ToolApprovalPolicy,
    call: &LegacyPreparedToolCall,
) -> bool {
    strategy == ToolCallStrategy::ParallelSafe
        && !approval_policy.requires_review(call.risk_level)
        && call.execution.concurrency == ToolConcurrency::ParallelSafe
        && call.execution.completion == ToolCompletion::Immediate
}

fn legacy_prepared_is_parallel_candidate(
    approval_policy: ToolApprovalPolicy,
    call: &LegacyPreparedCall,
) -> bool {
    matches!(
        call,
        LegacyPreparedCall::Ready { call, .. }
            if legacy_parallel_candidate(ToolCallStrategy::ParallelSafe, approval_policy, call)
    )
}

async fn legacy_execute_tool<T: ToolPort>(
    tools: Arc<T>,
    run_id: RunId,
    tool_set_revision: u64,
    call: &LegacyPreparedToolCall,
    cancellation: RunCancellation,
    timeout: Duration,
) -> ToolPoll {
    let mut attempts = 0_u32;
    loop {
        attempts = attempts.saturating_add(1);
        let tool_request = ToolCallRequest {
            run_id,
            call_id: call.call_id.clone(),
            name: call.name.clone(),
            arguments: call.arguments.clone(),
            cancellation: cancellation.clone(),
        };
        let poll = tokio::select! {
            biased;
            () = cancellation.cancelled() => ToolPoll::Cancelled,
            result = tokio::time::timeout(
                timeout,
                tools.call_at(tool_set_revision, tool_request),
            ) => {
                match result {
                    Ok(result) => ToolPoll::Finished(result),
                    Err(_) => ToolPoll::TimedOut,
                }
            }
        };
        let retry_delay = match &poll {
            ToolPoll::TimedOut if legacy_should_retry(call.execution, attempts, true) => {
                Some(call.execution.retry.delay_for_attempt(attempts))
            }
            ToolPoll::Finished(Err(error))
                if legacy_should_retry(call.execution, attempts, error.retryable()) =>
            {
                Some(error.retry_after_ms().map_or_else(
                    || call.execution.retry.delay_for_attempt(attempts),
                    |requested| requested.min(call.execution.retry.max_backoff_ms),
                ))
            }
            _ => None,
        };
        let Some(delay) = retry_delay else {
            return poll;
        };
        if !legacy_wait_for_retry(&cancellation, delay).await {
            return ToolPoll::Cancelled;
        }
    }
}

fn project_legacy_tool_result(call: LegacyPreparedToolCall, poll: ToolPoll) -> LegacyToolResult {
    let call_id = call.call_id;
    match poll {
        ToolPoll::Cancelled => LegacyToolResult::Cancelled,
        ToolPoll::TimedOut => {
            let code = "tool_timeout";
            let message = "tool execution exceeded the configured timeout";
            LegacyToolResult::Finished {
                event: AgentEvent::ToolExecutionFailed {
                    call_id: call_id.clone(),
                    code: code.into(),
                    message: message.into(),
                    category: ToolErrorCategory::Timeout,
                    retryable: true,
                    retry_after_ms: None,
                },
                result: Box::new(ModelMessage::tool_result(
                    call_id,
                    tool_error_content(code, message, ToolErrorCategory::Timeout, true, None),
                )),
            }
        }
        ToolPoll::Finished(Ok(output)) if output.suspension.is_some() => {
            let code = "durable_runtime_required";
            let message =
                "this tool requires the durable AgentMachine runtime to suspend and resume";
            LegacyToolResult::Finished {
                event: AgentEvent::ToolExecutionFailed {
                    call_id: call_id.clone(),
                    code: code.into(),
                    message: message.into(),
                    category: ToolErrorCategory::Conflict,
                    retryable: false,
                    retry_after_ms: None,
                },
                result: Box::new(ModelMessage::tool_result(
                    call_id,
                    tool_error_content(code, message, ToolErrorCategory::Conflict, false, None),
                )),
            }
        }
        ToolPoll::Finished(Ok(output)) => LegacyToolResult::Finished {
            event: AgentEvent::ToolExecutionCompleted {
                call_id: call_id.clone(),
                output: output.content.clone(),
            },
            result: Box::new(ModelMessage::tool_result(call_id, output.content)),
        },
        ToolPoll::Finished(Err(error)) => LegacyToolResult::Finished {
            event: AgentEvent::ToolExecutionFailed {
                call_id: call_id.clone(),
                code: error.code().into(),
                message: error.safe_message().into(),
                category: error.category(),
                retryable: error.retryable(),
                retry_after_ms: error.retry_after_ms(),
            },
            result: Box::new(ModelMessage::tool_result(
                call_id,
                tool_error_content(
                    error.code(),
                    error.safe_message(),
                    error.category(),
                    error.retryable(),
                    error.retry_after_ms(),
                ),
            )),
        },
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

fn legacy_should_retry(policy: ToolExecutionPolicy, attempts: u32, retryable: bool) -> bool {
    retryable
        && policy.idempotency.permits_automatic_retry()
        && attempts < policy.retry.max_attempts
}

async fn legacy_wait_for_retry(cancellation: &RunCancellation, delay_ms: u64) -> bool {
    tokio::select! {
        biased;
        () = cancellation.cancelled() => false,
        () = tokio::time::sleep(Duration::from_millis(delay_ms)) => true,
    }
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

fn final_summary_stream<P>(
    provider: Arc<P>,
    request: ModelRequest,
    cancellation: crate::harness::RunCancellation,
    model_timeout: Duration,
    completed_usage: TokenUsage,
) -> AgentEventStream
where
    P: ModelPort,
{
    Box::pin(stream! {
        let mut model_events = provider.stream(request);
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
                        "final model summary exceeded the configured timeout",
                        true,
                    );
                    return;
                }
                ModelPoll::Event(Some(event)) => event,
                ModelPoll::Event(None) => {
                    yield protocol_failure("final model summary ended without a terminal event");
                    return;
                }
            };

            match event {
                ModelEvent::Accepted { .. } => {}
                ModelEvent::ReasoningDelta { delta } => {
                    if !delta.is_empty() {
                        yield AgentEvent::OutputDelta {
                            channel: OutputChannel::AssistantReasoning,
                            delta,
                        };
                    }
                }
                ModelEvent::TextDelta { delta } => {
                    if !delta.is_empty() {
                        yield AgentEvent::text_delta(delta);
                    }
                }
                ModelEvent::Usage { usage } => {
                    yield AgentEvent::UsageUpdated {
                        usage: add_usage(completed_usage, usage),
                    };
                }
                ModelEvent::Completed { finish_reason } if finish_reason != FinishReason::ToolCall => {
                    yield AgentEvent::completed(finish_reason);
                    return;
                }
                ModelEvent::Completed { .. }
                | ModelEvent::ToolCallStarted { .. }
                | ModelEvent::ToolCallArgumentsDelta { .. } => {
                    yield protocol_failure(
                        "model requested a tool during the tool-disabled final summary",
                    );
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
    })
}

fn step_limit_tool_error_content(code: &str, message: &str) -> String {
    serde_json::json!({
        "ok": false,
        "error": {
            "code": code,
            "message": message,
            "category": ToolErrorCategory::ResourceExhausted,
            "retryable": false,
            "retry_after_ms": null,
        },
        "instruction": "Do not request more tools. Give the user a final answer using the information already available and clearly state any incomplete work."
    })
    .to_string()
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
        Harness, ModelEventStream, RunEventKind, RunOptions, ToolCallFuture, ToolDefinition,
        ToolError, ToolOutput,
    };

    struct FakeProvider {
        calls: AtomicUsize,
        tool_name: &'static str,
    }

    impl ModelPort for FakeProvider {
        fn stream(&self, request: ModelRequest) -> ModelEventStream {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            assert_eq!(request.tools[0].name, self.tool_name);

            if call == 0 {
                assert_eq!(request.messages.len(), 2);
                return Box::pin(stream::iter(vec![
                    ModelEvent::Accepted {
                        provider_request_id: None,
                    },
                    ModelEvent::ToolCallStarted {
                        call_id: "call_1".into(),
                        name: self.tool_name.into(),
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

    struct StepLimitProvider {
        calls: Arc<AtomicUsize>,
    }

    impl ModelPort for StepLimitProvider {
        fn stream(&self, request: ModelRequest) -> ModelEventStream {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                assert_eq!(request.tools.len(), 1);
                return Box::pin(stream::iter(vec![
                    ModelEvent::ToolCallStarted {
                        call_id: "call_after_limit".into(),
                        name: "get_current_time".into(),
                    },
                    ModelEvent::ToolCallArgumentsDelta {
                        call_id: "call_after_limit".into(),
                        delta: "{}".into(),
                    },
                    ModelEvent::Completed {
                        finish_reason: FinishReason::ToolCall,
                    },
                ]));
            }

            assert!(request.tools.is_empty());
            assert_eq!(request.messages.len(), 4);
            assert_eq!(request.messages[2].tool_calls[0].id, "call_after_limit");
            assert!(
                request.messages[3]
                    .content
                    .contains("agent_step_limit_exceeded")
            );
            assert!(
                request.messages[3]
                    .content
                    .contains("Do not request more tools")
            );
            Box::pin(stream::iter(vec![
                ModelEvent::TextDelta {
                    delta: "I could not run the final tool, so here is the partial result.".into(),
                },
                ModelEvent::Completed {
                    finish_reason: FinishReason::Stop,
                },
            ]))
        }
    }

    struct CountingTools {
        calls: Arc<AtomicUsize>,
    }

    impl ToolPort for CountingTools {
        fn definitions(&self) -> Vec<ToolDefinition> {
            FakeTools.definitions()
        }

        fn call(&self, _request: ToolCallRequest) -> ToolCallFuture {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(ToolOutput::text("should not execute")) })
        }
    }

    struct RiskyTools {
        calls: Arc<AtomicUsize>,
    }

    impl ToolPort for RiskyTools {
        fn definitions(&self) -> Vec<ToolDefinition> {
            vec![
                ToolDefinition::new(
                    "exec_command",
                    "Run a managed terminal command.",
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
            assert_eq!(request.tool_name, "exec_command");
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
                tool_name: "get_current_time",
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
    async fn returns_step_limit_as_tool_error_then_runs_a_tool_free_final_summary() {
        let provider_calls = Arc::new(AtomicUsize::new(0));
        let tool_calls = Arc::new(AtomicUsize::new(0));
        let harness = Harness::new(AgentLoop::new(
            StepLimitProvider {
                calls: Arc::clone(&provider_calls),
            },
            CountingTools {
                calls: Arc::clone(&tool_calls),
            },
            "test-model",
            "Use tools when needed.",
            None,
            4,
        ));

        let execution = harness
            .start_with_options(
                crate::harness::RunId::new(),
                "Use one more tool",
                Vec::new(),
                RunOptions {
                    allowed_tools: None,
                    allow_run_adf: false,
                    max_steps: Some(1),
                },
            )
            .expect("run should start");
        let events: Vec<_> = execution.events.collect().await;

        assert_eq!(provider_calls.load(Ordering::SeqCst), 2);
        assert_eq!(tool_calls.load(Ordering::SeqCst), 0);
        assert!(events.iter().any(|event| matches!(
            &event.kind,
            RunEventKind::ToolExecutionFailed {
                call_id,
                code,
                category: ToolErrorCategory::ResourceExhausted,
                retryable: false,
                ..
            } if call_id == "call_after_limit" && code == "agent_step_limit_exceeded"
        )));
        assert!(events.iter().any(|event| matches!(
            &event.kind,
            RunEventKind::OutputDelta {
                channel: OutputChannel::AssistantText,
                delta,
            } if delta.contains("partial result")
        )));
        assert!(matches!(
            events.last().map(|event| &event.kind),
            Some(RunEventKind::RunCompleted {
                finish_reason: FinishReason::Stop
            })
        ));
    }

    #[tokio::test]
    async fn executes_a_risky_tool_only_after_approval() {
        let calls = Arc::new(AtomicUsize::new(0));
        let harness = Harness::new(
            AgentLoop::new(
                FakeProvider {
                    calls: AtomicUsize::new(0),
                    tool_name: "exec_command",
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
    async fn numeric_review_level_can_filter_high_risk_tools() {
        let calls = Arc::new(AtomicUsize::new(0));
        let harness = Harness::new(
            AgentLoop::new(
                FakeProvider {
                    calls: AtomicUsize::new(0),
                    tool_name: "exec_command",
                },
                RiskyTools {
                    calls: Arc::clone(&calls),
                },
                "test-model",
                "Use tools when needed.",
                None,
                4,
            )
            .with_approval_policy(ToolApprovalPolicy::new(100)),
        );

        let events: Vec<_> = harness
            .stream("Use the risky tool without review")
            .expect("stream should start")
            .collect()
            .await;

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(
            !events
                .iter()
                .any(|event| matches!(event.kind, RunEventKind::ApprovalRequested { .. }))
        );
    }

    #[tokio::test]
    async fn returns_a_rejected_risky_tool_to_the_model_without_executing_it() {
        let calls = Arc::new(AtomicUsize::new(0));
        let harness = Harness::new(
            AgentLoop::new(
                FakeProvider {
                    calls: AtomicUsize::new(0),
                    tool_name: "exec_command",
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

    struct LegacySuspensionProvider {
        calls: AtomicUsize,
    }

    impl ModelPort for LegacySuspensionProvider {
        fn stream(&self, request: ModelRequest) -> ModelEventStream {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                return Box::pin(stream::iter(vec![
                    ModelEvent::Accepted {
                        provider_request_id: None,
                    },
                    ModelEvent::ToolCallStarted {
                        call_id: "call_suspend".into(),
                        name: "suspending_tool".into(),
                    },
                    ModelEvent::ToolCallArgumentsDelta {
                        call_id: "call_suspend".into(),
                        delta: "{}".into(),
                    },
                    ModelEvent::Completed {
                        finish_reason: FinishReason::ToolCall,
                    },
                ]));
            }
            let result: serde_json::Value = serde_json::from_str(&request.messages[3].content)
                .expect("legacy suspension failure should be JSON");
            assert_eq!(result["error"]["code"], "durable_runtime_required");
            Box::pin(stream::iter(vec![
                ModelEvent::Accepted {
                    provider_request_id: None,
                },
                ModelEvent::TextDelta {
                    delta: "durable runtime required".into(),
                },
                ModelEvent::Completed {
                    finish_reason: FinishReason::Stop,
                },
            ]))
        }
    }

    struct LegacySuspendingTool;

    impl ToolPort for LegacySuspendingTool {
        fn definitions(&self) -> Vec<ToolDefinition> {
            vec![ToolDefinition::new(
                "suspending_tool",
                "Requires durable suspension.",
                json!({"type": "object", "additionalProperties": false}),
            )]
        }

        fn call(&self, _request: ToolCallRequest) -> ToolCallFuture {
            Box::pin(async { Ok(ToolOutput::suspend(Vec::new(), Vec::new())) })
        }
    }

    #[tokio::test]
    async fn legacy_agent_run_returns_suspension_as_a_structured_tool_error() {
        let harness = Harness::new(AgentLoop::new(
            LegacySuspensionProvider {
                calls: AtomicUsize::new(0),
            },
            LegacySuspendingTool,
            "test-model",
            "Use tools when needed.",
            None,
            4,
        ));

        let events: Vec<_> = harness
            .stream("Suspend")
            .expect("stream should start")
            .collect()
            .await;
        assert!(events.iter().any(|event| matches!(
            &event.kind,
            RunEventKind::ToolExecutionFailed { code, .. }
                if code == "durable_runtime_required"
        )));
        assert!(!events.iter().any(|event| matches!(
            &event.kind,
            RunEventKind::ToolExecutionCompleted { call_id, .. }
                if call_id == "call_suspend"
        )));
        assert!(matches!(
            events.last().map(|event| &event.kind),
            Some(RunEventKind::RunCompleted { .. })
        ));
    }

    struct LegacyParallelProvider {
        calls: AtomicUsize,
    }

    impl ModelPort for LegacyParallelProvider {
        fn stream(&self, request: ModelRequest) -> ModelEventStream {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                return Box::pin(stream::iter(vec![
                    ModelEvent::ToolCallStarted {
                        call_id: "call_slow".into(),
                        name: "slow_read".into(),
                    },
                    ModelEvent::ToolCallArgumentsDelta {
                        call_id: "call_slow".into(),
                        delta: "{}".into(),
                    },
                    ModelEvent::ToolCallStarted {
                        call_id: "call_fast".into(),
                        name: "fast_read".into(),
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
            assert_eq!(request.messages[3].content, "slow_read-result");
            assert_eq!(
                request.messages[4].tool_call_id.as_deref(),
                Some("call_fast")
            );
            assert_eq!(request.messages[4].content, "fast_read-result");
            Box::pin(stream::iter(vec![ModelEvent::Completed {
                finish_reason: FinishReason::Stop,
            }]))
        }
    }

    struct LegacyParallelTools {
        active: Arc<AtomicUsize>,
        max_active: Arc<AtomicUsize>,
    }

    impl ToolPort for LegacyParallelTools {
        fn definitions(&self) -> Vec<ToolDefinition> {
            let policy =
                ToolExecutionPolicy::read_only().with_concurrency(ToolConcurrency::ParallelSafe);
            vec![
                ToolDefinition::new("slow_read", "Slow read.", json!({"type": "object"}))
                    .with_execution_policy(policy),
                ToolDefinition::new("fast_read", "Fast read.", json!({"type": "object"}))
                    .with_execution_policy(policy),
            ]
        }

        fn call(&self, request: ToolCallRequest) -> ToolCallFuture {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active.fetch_max(active, Ordering::SeqCst);
            let active_counter = Arc::clone(&self.active);
            let delay = if request.name == "slow_read" { 30 } else { 5 };
            let content = format!("{}-result", request.name);
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(delay)).await;
                active_counter.fetch_sub(1, Ordering::SeqCst);
                Ok(ToolOutput::text(content))
            })
        }
    }

    #[tokio::test]
    async fn legacy_agent_overlaps_parallel_safe_tools_and_preserves_result_order() {
        let max_active = Arc::new(AtomicUsize::new(0));
        let harness = Harness::new(
            AgentLoop::new(
                LegacyParallelProvider {
                    calls: AtomicUsize::new(0),
                },
                LegacyParallelTools {
                    active: Arc::new(AtomicUsize::new(0)),
                    max_active: Arc::clone(&max_active),
                },
                "test-model",
                "Use tools when needed.",
                None,
                4,
            )
            .with_tool_call_strategy(ToolCallStrategy::ParallelSafe),
        );

        let events: Vec<_> = harness
            .stream("Read both")
            .expect("stream should start")
            .collect()
            .await;

        assert_eq!(max_active.load(Ordering::SeqCst), 2);
        assert!(matches!(
            events.last().map(|event| &event.kind),
            Some(RunEventKind::RunCompleted { .. })
        ));
    }
}
