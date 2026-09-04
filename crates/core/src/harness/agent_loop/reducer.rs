//! Pure, replayable Agent loop decisions.
//!
//! This module deliberately performs no HTTP, timers, storage, random-number
//! generation, or tool execution. A native harness or a browser Worker persists
//! each [`AgentLoopTransition`] and then executes the returned effects. Results
//! are fed back through [`AgentLoopInput`].

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{
    ToolCallStrategy, add_usage, digest_only_arguments, digest_only_bytes, rejection_message,
    step_limit_tool_error_content, tool_error_content,
};
use crate::harness::{
    AgentEvent, ApprovalDecision, ApprovalId, ApprovalResolution, EffectRequest, FinishReason,
    MachineError, ModelAttachment, ModelEvent, ModelMessage, ModelRequest, ModelToolCall,
    OutputChannel, RunId, TokenUsage, TokenUsageSource, ToolApprovalPolicy, ToolArgumentVisibility,
    ToolBindingKind, ToolCompletion, ToolConcurrency, ToolErrorCategory, ToolExecutionPolicy,
    ToolRiskLevel, ToolSetSnapshot, ToolSuspension, WaitSpec,
};

/// Version of the JSON request/response boundary exposed by the reducer.
pub const AGENT_LOOP_REDUCER_PROTOCOL_VERSION: u32 = 1;

/// Host-independent settings captured in every reducer checkpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentLoopReducerConfig {
    pub model: String,
    #[serde(default)]
    pub system_prompt: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u32>,
    pub max_steps: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allowed_tools: Option<Vec<String>>,
    #[serde(default)]
    pub allow_run_adf: bool,
    #[serde(default)]
    pub approval_policy: ToolApprovalPolicy,
    #[serde(default)]
    pub tool_call_strategy: ToolCallStrategy,
    pub model_timeout_ms: u64,
    pub tool_timeout_ms: u64,
}

/// Inputs required to create a portable Agent loop checkpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentLoopStart {
    pub run_id: RunId,
    pub input: String,
    #[serde(default)]
    pub attachments: Vec<ModelAttachment>,
    #[serde(default)]
    pub prior_messages: Vec<ModelMessage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_fingerprint: Option<String>,
    pub config: AgentLoopReducerConfig,
}

/// Versioned JSON ABI request for starting a reducer run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentLoopStartDispatch {
    pub protocol_version: u32,
    pub start: AgentLoopStart,
}

/// Why a model is being invoked. Hosts may route, meter, or authorize each
/// purpose differently without changing the model request itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelInvocationPurpose {
    AgentStep,
    FinalSummary,
    SessionTitle,
}

/// Safe failure data that can cross the native/WASM JSON boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortableToolError {
    pub code: String,
    pub message: String,
    #[serde(default)]
    pub category: ToolErrorCategory,
    pub retryable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
}

/// A tool call after the reducer has checked the advertised capability,
/// decoded its JSON arguments, and applied public-argument redaction.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolInvocation {
    pub call_id: String,
    pub name: String,
    pub arguments: Value,
    pub public_arguments: Value,
    pub risk_level: ToolRiskLevel,
    pub execution: ToolExecutionPolicy,
}

/// One host-side validation request. Browser and native hosts both validate
/// immediately before execution; the reducer additionally checks the returned
/// call identity so a compromised or stale response cannot be applied.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolValidationRequest {
    pub call_id: String,
    pub name: String,
    pub arguments: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolValidation {
    pub call_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<PortableToolError>,
}

/// Result returned by a native ToolPort or a sealed browser JS handler.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ToolInvocationResult {
    Completed { content: String },
    Failed { error: PortableToolError },
    Suspended { suspension: ToolSuspension },
}

/// Side effects requested by the pure reducer. The effect id is stable across
/// checkpoint replay and is the host's idempotency/deduplication key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentLoopEffect {
    pub effect_id: String,
    #[serde(flatten)]
    pub kind: AgentLoopEffectKind,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AgentLoopEffectKind {
    LoadToolSet {
        run_id: RunId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        allowed_tools: Option<Vec<String>>,
        allow_run_adf: bool,
    },
    InvokeModel {
        purpose: ModelInvocationPurpose,
        request: ModelRequest,
        timeout_ms: u64,
    },
    ValidateTools {
        run_id: RunId,
        tool_set_revision: u64,
        calls: Vec<ToolValidationRequest>,
    },
    RequestApproval {
        approval_id: ApprovalId,
        run_id: RunId,
        call: ToolInvocation,
    },
    InvokeTool {
        run_id: RunId,
        tool_set_revision: u64,
        call: ToolInvocation,
        timeout_ms: u64,
    },
}

/// Facts supplied to the reducer by its host. Every effect result carries the
/// matching id, preventing late results from a cancelled/replayed effect from
/// mutating the current checkpoint.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AgentLoopInput {
    ToolSetLoaded {
        effect_id: String,
        tool_set: ToolSetSnapshot,
    },
    ToolSetFailed {
        effect_id: String,
        error: MachineError,
    },
    ModelEvent {
        effect_id: String,
        event: ModelEvent,
    },
    ModelStreamEnded {
        effect_id: String,
    },
    ModelTimedOut {
        effect_id: String,
    },
    ToolsValidated {
        effect_id: String,
        results: Vec<ToolValidation>,
    },
    ApprovalResolved {
        effect_id: String,
        resolution: ApprovalResolution,
    },
    ToolFinished {
        effect_id: String,
        result: ToolInvocationResult,
    },
    ToolResumed {
        call_id: String,
        content: String,
    },
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum AgentLoopOutcome {
    Running,
    Suspended {
        waits: Vec<WaitSpec>,
        effects: Vec<EffectRequest>,
    },
    Complete {
        finish_reason: FinishReason,
    },
    Failed {
        error: MachineError,
    },
    Cancelled,
}

/// Atomic reducer output. Persist `state` and `events` before executing any
/// returned effect.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentLoopTransition {
    pub protocol_version: u32,
    pub state: AgentLoopReducerState,
    pub events: Vec<AgentEvent>,
    pub effects: Vec<AgentLoopEffect>,
    pub outcome: AgentLoopOutcome,
}

/// JSON ABI request used by a thin wasm-bindgen wrapper.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentLoopDispatch {
    pub protocol_version: u32,
    pub state: AgentLoopReducerState,
    pub input: AgentLoopInput,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentLoopReducerState {
    pub protocol_version: u32,
    pub run_id: RunId,
    pub config: AgentLoopReducerConfig,
    pub messages: Vec<ModelMessage>,
    pub completed_usage: TokenUsage,
    pub next_step: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_fingerprint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    previous_tool_set: Option<ToolSetIdentity>,
    next_effect_sequence: u32,
    phase: AgentLoopPhase,
}

impl AgentLoopReducerState {
    /// Effects that must be replayed when a host restores this checkpoint.
    #[must_use]
    pub fn pending_effects(&self) -> Vec<AgentLoopEffect> {
        self.phase.pending_effects()
    }

    #[must_use]
    pub fn outcome(&self) -> AgentLoopOutcome {
        self.phase.outcome()
    }

    #[must_use]
    pub fn is_waiting_for_approval(&self) -> bool {
        matches!(self.phase, AgentLoopPhase::AwaitingApproval { .. })
    }

    #[must_use]
    pub fn is_waiting_for_tool_resume(&self) -> bool {
        matches!(self.phase, AgentLoopPhase::AwaitingToolResume { .. })
    }

    #[must_use]
    pub fn pending_tool_resume_call_id(&self) -> Option<&str> {
        match &self.phase {
            AgentLoopPhase::AwaitingToolResume { call_id, .. } => Some(call_id),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ToolSetIdentity {
    revision: u64,
    digest: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum AgentLoopPhase {
    AwaitingToolSet {
        effect: AgentLoopEffect,
    },
    AwaitingModel {
        effect: AgentLoopEffect,
        tool_set: ToolSetSnapshot,
        turn: ModelTurnState,
    },
    AwaitingValidation {
        effect: AgentLoopEffect,
        tool_set: ToolSetSnapshot,
        calls: Vec<PlannedCall>,
    },
    AwaitingApproval {
        effect: AgentLoopEffect,
        tool_set_revision: u64,
        call: ToolInvocation,
        remaining: Vec<PlannedCall>,
    },
    AwaitingTools {
        effects: Vec<AgentLoopEffect>,
        calls: Vec<ToolInvocation>,
        results: BTreeMap<String, ToolInvocationResult>,
        remaining: Vec<PlannedCall>,
    },
    AwaitingToolResume {
        call_id: String,
        remaining: Vec<PlannedCall>,
        suspension: ToolSuspension,
    },
    Terminal {
        outcome: AgentLoopOutcome,
    },
}

impl AgentLoopPhase {
    fn pending_effects(&self) -> Vec<AgentLoopEffect> {
        match self {
            Self::AwaitingToolSet { effect }
            | Self::AwaitingModel { effect, .. }
            | Self::AwaitingValidation { effect, .. }
            | Self::AwaitingApproval { effect, .. } => vec![effect.clone()],
            Self::AwaitingTools {
                effects, results, ..
            } => effects
                .iter()
                .filter(|effect| !results.contains_key(&effect.effect_id))
                .cloned()
                .collect(),
            Self::AwaitingToolResume { .. } | Self::Terminal { .. } => Vec::new(),
        }
    }

    fn outcome(&self) -> AgentLoopOutcome {
        match self {
            Self::AwaitingToolResume { suspension, .. } => AgentLoopOutcome::Suspended {
                waits: suspension.waits.clone(),
                effects: suspension.effects.clone(),
            },
            Self::Terminal { outcome } => outcome.clone(),
            _ => AgentLoopOutcome::Running,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct ModelTurnState {
    purpose: ModelInvocationPurpose,
    assistant_text: String,
    assistant_reasoning: String,
    pending_calls: Vec<ModelToolCall>,
    seen_call_ids: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    round_usage: Option<TokenUsage>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum PlannedCall {
    Ready(ToolInvocation),
    Rejected {
        call_id: String,
        error: PortableToolError,
    },
}

type PreparedCall = (ToolInvocation, Option<AgentEvent>);
type RejectedPreparedCall = (PortableToolError, Option<AgentEvent>);

/// Stateless entry point for the portable Agent loop.
#[derive(Debug, Default, Clone, Copy)]
pub struct AgentLoopReducer;

impl AgentLoopReducer {
    #[must_use]
    pub fn start(start: AgentLoopStart) -> AgentLoopTransition {
        let mut messages = Vec::with_capacity(start.prior_messages.len() + 2);
        if !start.config.system_prompt.trim().is_empty() {
            messages.push(ModelMessage::system(start.config.system_prompt.clone()));
        }
        messages.extend(start.prior_messages);
        messages.push(ModelMessage::user_with_attachments(
            start.input,
            start.attachments,
        ));
        let mut state = AgentLoopReducerState {
            protocol_version: AGENT_LOOP_REDUCER_PROTOCOL_VERSION,
            run_id: start.run_id,
            config: AgentLoopReducerConfig {
                max_steps: start.config.max_steps.max(1),
                ..start.config
            },
            messages,
            completed_usage: zero_usage(),
            next_step: 0,
            context_fingerprint: start.context_fingerprint,
            previous_tool_set: None,
            next_effect_sequence: 0,
            phase: AgentLoopPhase::Terminal {
                outcome: AgentLoopOutcome::Running,
            },
        };
        let effect = load_tool_set_effect(&mut state);
        state.phase = AgentLoopPhase::AwaitingToolSet {
            effect: effect.clone(),
        };
        transition(state, Vec::new(), vec![effect])
    }

    /// Starts a run through the same string-only boundary used by a thin WASM
    /// export. Unsupported versions produce a normal failed transition.
    pub fn start_json(json: &str) -> Result<String, serde_json::Error> {
        let request: AgentLoopStartDispatch = serde_json::from_str(json)?;
        let response = if request.protocol_version == AGENT_LOOP_REDUCER_PROTOCOL_VERSION {
            Self::start(request.start)
        } else {
            let mut response = Self::start(request.start);
            response.state.phase = AgentLoopPhase::Terminal {
                outcome: AgentLoopOutcome::Failed {
                    error: MachineError {
                        code: "reducer_protocol_incompatible".into(),
                        message: "the start protocol version is not supported".into(),
                        retryable: false,
                    },
                },
            };
            response.effects.clear();
            response.outcome = response.state.outcome();
            response
        };
        serde_json::to_string(&response)
    }

    #[must_use]
    pub fn dispatch(
        mut state: AgentLoopReducerState,
        input: AgentLoopInput,
    ) -> AgentLoopTransition {
        if state.protocol_version != AGENT_LOOP_REDUCER_PROTOCOL_VERSION {
            return fail(
                state,
                "reducer_protocol_incompatible",
                "the reducer checkpoint protocol version is not supported",
                false,
            );
        }
        if matches!(input, AgentLoopInput::Cancelled) {
            state.phase = AgentLoopPhase::Terminal {
                outcome: AgentLoopOutcome::Cancelled,
            };
            return transition(state, Vec::new(), Vec::new());
        }

        match (state.phase.clone(), input) {
            (
                AgentLoopPhase::AwaitingToolSet { effect },
                AgentLoopInput::ToolSetLoaded {
                    effect_id,
                    tool_set,
                },
            ) if effect.effect_id == effect_id => tool_set_loaded(state, tool_set),
            (
                AgentLoopPhase::AwaitingToolSet { effect },
                AgentLoopInput::ToolSetFailed { effect_id, error },
            ) if effect.effect_id == effect_id => terminal_error(state, error),
            (
                AgentLoopPhase::AwaitingModel {
                    effect,
                    tool_set,
                    turn,
                },
                AgentLoopInput::ModelEvent { effect_id, event },
            ) if effect.effect_id == effect_id => {
                reduce_model_event(state, effect, tool_set, turn, event)
            }
            (
                AgentLoopPhase::AwaitingModel { effect, .. },
                AgentLoopInput::ModelStreamEnded { effect_id },
            ) if effect.effect_id == effect_id => fail(
                state,
                "upstream_protocol_violation",
                "model event stream ended without a terminal event",
                false,
            ),
            (
                AgentLoopPhase::AwaitingModel { effect, .. },
                AgentLoopInput::ModelTimedOut { effect_id },
            ) if effect.effect_id == effect_id => fail(
                state,
                "model_timeout",
                "model invocation exceeded the configured timeout",
                true,
            ),
            (
                AgentLoopPhase::AwaitingValidation {
                    effect,
                    tool_set,
                    calls,
                },
                AgentLoopInput::ToolsValidated { effect_id, results },
            ) if effect.effect_id == effect_id => {
                validation_completed(state, tool_set, calls, results)
            }
            (
                AgentLoopPhase::AwaitingApproval {
                    effect,
                    tool_set_revision,
                    call,
                    remaining,
                },
                AgentLoopInput::ApprovalResolved {
                    effect_id,
                    resolution,
                },
            ) if effect.effect_id == effect_id => {
                approval_resolved(state, tool_set_revision, call, remaining, resolution)
            }
            (
                AgentLoopPhase::AwaitingTools {
                    effects,
                    calls,
                    results,
                    remaining,
                },
                AgentLoopInput::ToolFinished { effect_id, result },
            ) => tool_finished(state, effects, calls, results, remaining, effect_id, result),
            (
                AgentLoopPhase::AwaitingToolResume {
                    call_id, remaining, ..
                },
                AgentLoopInput::ToolResumed {
                    call_id: resumed_id,
                    content,
                },
            ) if call_id == resumed_id => tool_resumed(state, call_id, remaining, content),
            _ => fail(
                state,
                "reducer_input_unexpected",
                "the input does not match the reducer's pending effect",
                false,
            ),
        }
    }

    /// Convenience entry point for a thin WASM export. It does not depend on
    /// wasm-bindgen and is equally usable by a native JSON host.
    pub fn dispatch_json(json: &str) -> Result<String, serde_json::Error> {
        let request: AgentLoopDispatch = serde_json::from_str(json)?;
        let response = if request.protocol_version == AGENT_LOOP_REDUCER_PROTOCOL_VERSION {
            Self::dispatch(request.state, request.input)
        } else {
            fail(
                request.state,
                "reducer_protocol_incompatible",
                "the dispatch protocol version is not supported",
                false,
            )
        };
        serde_json::to_string(&response)
    }
}

fn tool_set_loaded(
    mut state: AgentLoopReducerState,
    tool_set: ToolSetSnapshot,
) -> AgentLoopTransition {
    let tool_set = match tool_set.retain(|binding| {
        let statically_allowed = state
            .config
            .allowed_tools
            .as_ref()
            .is_none_or(|allowed| allowed.contains(&binding.name));
        match binding.kind {
            ToolBindingKind::Static => statically_allowed,
            ToolBindingKind::AdfManagement => state.config.allow_run_adf && statically_allowed,
            ToolBindingKind::AgentDefined => state.config.allow_run_adf,
        }
    }) {
        Ok(tool_set) => tool_set,
        Err(error) => {
            return fail(state, error.code(), error.safe_message(), error.retryable());
        }
    };
    let identity = ToolSetIdentity {
        revision: tool_set.revision,
        digest: tool_set.digest.clone(),
    };
    let mut events = Vec::new();
    if state.previous_tool_set.as_ref() != Some(&identity) {
        events.push(AgentEvent::ToolSetUpdated {
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
    invoke_model(state, tool_set, ModelInvocationPurpose::AgentStep, events)
}

fn invoke_model(
    mut state: AgentLoopReducerState,
    tool_set: ToolSetSnapshot,
    purpose: ModelInvocationPurpose,
    events: Vec<AgentEvent>,
) -> AgentLoopTransition {
    let tools = if purpose == ModelInvocationPurpose::FinalSummary {
        Vec::new()
    } else {
        tool_set.definitions.clone()
    };
    let effect_id = next_effect_id(&mut state, "model");
    let effect = AgentLoopEffect {
        effect_id,
        kind: AgentLoopEffectKind::InvokeModel {
            purpose,
            request: ModelRequest {
                run_id: state.run_id,
                model: state.config.model.clone(),
                messages: state.messages.clone(),
                tools,
                max_output_tokens: state.config.max_output_tokens,
            },
            timeout_ms: state.config.model_timeout_ms,
        },
    };
    state.phase = AgentLoopPhase::AwaitingModel {
        effect: effect.clone(),
        tool_set,
        turn: ModelTurnState {
            purpose,
            assistant_text: String::new(),
            assistant_reasoning: String::new(),
            pending_calls: Vec::new(),
            seen_call_ids: BTreeSet::new(),
            round_usage: None,
        },
    };
    transition(state, events, vec![effect])
}

fn reduce_model_event(
    mut state: AgentLoopReducerState,
    effect: AgentLoopEffect,
    tool_set: ToolSetSnapshot,
    mut turn: ModelTurnState,
    event: ModelEvent,
) -> AgentLoopTransition {
    let mut events = Vec::new();
    match event {
        ModelEvent::Accepted { .. } => {}
        ModelEvent::ReasoningDelta { delta } => {
            if !delta.is_empty() {
                turn.assistant_reasoning.push_str(&delta);
                events.push(AgentEvent::OutputDelta {
                    channel: OutputChannel::AssistantReasoning,
                    delta,
                });
            }
        }
        ModelEvent::TextDelta { delta } => {
            if !delta.is_empty() {
                turn.assistant_text.push_str(&delta);
                events.push(AgentEvent::text_delta(delta));
            }
        }
        ModelEvent::ToolCallStarted { call_id, name } => {
            if !turn.seen_call_ids.insert(call_id.clone()) {
                return fail(
                    state,
                    "upstream_protocol_violation",
                    "model emitted a duplicate tool call id",
                    false,
                );
            }
            turn.pending_calls.push(ModelToolCall {
                id: call_id.clone(),
                name: name.clone(),
                arguments: String::new(),
            });
            events.push(AgentEvent::ToolCallStarted { call_id, name });
        }
        ModelEvent::ToolCallArgumentsDelta { call_id, delta } => {
            let Some(call) = turn
                .pending_calls
                .iter_mut()
                .find(|call| call.id == call_id)
            else {
                return fail(
                    state,
                    "upstream_protocol_violation",
                    "model emitted tool arguments before the tool call started",
                    false,
                );
            };
            call.arguments.push_str(&delta);
            let visibility = tool_set
                .binding(&call.name)
                .map_or(ToolArgumentVisibility::DigestOnly, |binding| {
                    binding.argument_visibility
                });
            if !delta.is_empty() && visibility == ToolArgumentVisibility::Full {
                events.push(AgentEvent::ToolCallArgumentsDelta { call_id, delta });
            }
        }
        ModelEvent::Usage { usage } => {
            turn.round_usage = Some(usage);
            events.push(AgentEvent::UsageUpdated {
                usage: add_usage(state.completed_usage, usage),
            });
        }
        ModelEvent::Failed { error } => {
            return fail(
                state,
                error.kind().code(),
                error.safe_message(),
                error.retryable(),
            );
        }
        ModelEvent::Completed { finish_reason } => {
            if let Some(usage) = turn.round_usage {
                state.completed_usage = add_usage(state.completed_usage, usage);
            }
            return model_completed(state, tool_set, turn, finish_reason, events);
        }
    }
    state.phase = AgentLoopPhase::AwaitingModel {
        effect,
        tool_set,
        turn,
    };
    transition(state, events, Vec::new())
}

fn model_completed(
    mut state: AgentLoopReducerState,
    tool_set: ToolSetSnapshot,
    turn: ModelTurnState,
    finish_reason: FinishReason,
    events: Vec<AgentEvent>,
) -> AgentLoopTransition {
    if finish_reason != FinishReason::ToolCall {
        if !turn.pending_calls.is_empty() {
            return fail_with_events(
                state,
                events,
                "upstream_protocol_violation",
                "model emitted tool calls without a tool-call finish reason",
                false,
            );
        }
        state.phase = AgentLoopPhase::Terminal {
            outcome: AgentLoopOutcome::Complete { finish_reason },
        };
        return transition(state, events, Vec::new());
    }
    if turn.purpose == ModelInvocationPurpose::FinalSummary {
        return fail_with_events(
            state,
            events,
            "upstream_protocol_violation",
            "the tool-free final summary attempted to call a tool",
            false,
        );
    }
    if turn.pending_calls.is_empty() {
        return fail_with_events(
            state,
            events,
            "upstream_protocol_violation",
            "model ended for tool calls without requesting a tool",
            false,
        );
    }

    state.messages.push(ModelMessage::assistant_tool_calls(
        turn.assistant_text,
        turn.assistant_reasoning,
        turn.pending_calls.clone(),
    ));
    if state.next_step.saturating_add(1) >= state.config.max_steps {
        let mut events = events;
        for call in turn.pending_calls {
            let code = "agent_step_limit_exceeded";
            let message = "the model step limit was reached; this tool was not executed";
            events.push(AgentEvent::ToolExecutionFailed {
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
        return invoke_model(
            state,
            empty_tool_set(tool_set.revision),
            ModelInvocationPurpose::FinalSummary,
            events,
        );
    }

    state.next_step = state.next_step.saturating_add(1);
    prepare_and_validate(state, tool_set, turn.pending_calls, events)
}

fn prepare_and_validate(
    mut state: AgentLoopReducerState,
    tool_set: ToolSetSnapshot,
    calls: Vec<ModelToolCall>,
    mut events: Vec<AgentEvent>,
) -> AgentLoopTransition {
    let mut planned = Vec::with_capacity(calls.len());
    let mut validation = Vec::new();
    for call in calls {
        match prepare_call(&tool_set, &call) {
            Ok((invocation, event)) => {
                if let Some(event) = event {
                    events.push(event);
                }
                validation.push(ToolValidationRequest {
                    call_id: invocation.call_id.clone(),
                    name: invocation.name.clone(),
                    arguments: invocation.arguments.clone(),
                });
                planned.push(PlannedCall::Ready(invocation));
            }
            Err(error) => {
                let (error, event) = *error;
                if let Some(event) = event {
                    events.push(event);
                }
                planned.push(PlannedCall::Rejected {
                    call_id: call.id,
                    error,
                });
            }
        }
    }
    if validation.is_empty() {
        return schedule_calls(state, tool_set.revision, planned, events);
    }
    let effect_id = next_effect_id(&mut state, "validate-tools");
    let effect = AgentLoopEffect {
        effect_id,
        kind: AgentLoopEffectKind::ValidateTools {
            run_id: state.run_id,
            tool_set_revision: tool_set.revision,
            calls: validation,
        },
    };
    state.phase = AgentLoopPhase::AwaitingValidation {
        effect: effect.clone(),
        tool_set,
        calls: planned,
    };
    transition(state, events, vec![effect])
}

fn prepare_call(
    tool_set: &ToolSetSnapshot,
    call: &ModelToolCall,
) -> Result<PreparedCall, Box<RejectedPreparedCall>> {
    let Some(binding) = tool_set.binding(&call.name) else {
        return Err(Box::new((
            portable_error(
                "tool_not_allowed",
                "model requested a tool outside this run's capability set",
                ToolErrorCategory::PermissionDenied,
                false,
                None,
            ),
            None,
        )));
    };
    let arguments: Value =
        match serde_json::from_str(&call.arguments) {
            Ok(arguments) => arguments,
            Err(_) => {
                let event = (binding.argument_visibility == ToolArgumentVisibility::DigestOnly)
                    .then(|| AgentEvent::ToolCallArgumentsDelta {
                        call_id: call.id.clone(),
                        delta: digest_only_bytes(call.arguments.as_bytes()).to_string(),
                    });
                return Err(Box::new((
                    portable_error(
                        "invalid_tool_arguments",
                        "model produced invalid JSON tool arguments",
                        ToolErrorCategory::InvalidRequest,
                        false,
                        None,
                    ),
                    event,
                )));
            }
        };
    let public_arguments = match binding.argument_visibility {
        ToolArgumentVisibility::Full => arguments.clone(),
        ToolArgumentVisibility::DigestOnly => digest_only_arguments(&arguments),
    };
    let event = (binding.argument_visibility == ToolArgumentVisibility::DigestOnly).then(|| {
        AgentEvent::ToolCallArgumentsDelta {
            call_id: call.id.clone(),
            delta: public_arguments.to_string(),
        }
    });
    let Some(definition) = tool_set
        .definitions
        .iter()
        .find(|definition| definition.name == call.name)
    else {
        return Err(Box::new((
            portable_error(
                "tool_set_invalid",
                "the tool binding has no matching definition",
                ToolErrorCategory::Internal,
                false,
                None,
            ),
            event,
        )));
    };
    Ok((
        ToolInvocation {
            call_id: call.id.clone(),
            name: call.name.clone(),
            arguments,
            public_arguments,
            risk_level: definition.risk_level,
            execution: definition.execution,
        },
        event,
    ))
}

fn validation_completed(
    state: AgentLoopReducerState,
    tool_set: ToolSetSnapshot,
    mut calls: Vec<PlannedCall>,
    results: Vec<ToolValidation>,
) -> AgentLoopTransition {
    let expected = calls
        .iter()
        .filter_map(|call| match call {
            PlannedCall::Ready(call) => Some(call.call_id.clone()),
            PlannedCall::Rejected { .. } => None,
        })
        .collect::<BTreeSet<_>>();
    let actual = results
        .iter()
        .map(|result| result.call_id.clone())
        .collect::<BTreeSet<_>>();
    if expected != actual || actual.len() != results.len() {
        return fail(
            state,
            "tool_validation_protocol_violation",
            "tool validation results do not match the pending calls",
            false,
        );
    }
    let errors = results
        .into_iter()
        .filter_map(|result| result.error.map(|error| (result.call_id, error)))
        .collect::<BTreeMap<_, _>>();
    for call in &mut calls {
        let PlannedCall::Ready(invocation) = call else {
            continue;
        };
        if let Some(error) = errors.get(&invocation.call_id) {
            *call = PlannedCall::Rejected {
                call_id: invocation.call_id.clone(),
                error: error.clone(),
            };
        }
    }
    schedule_calls(state, tool_set.revision, calls, Vec::new())
}

fn schedule_calls(
    mut state: AgentLoopReducerState,
    tool_set_revision: u64,
    mut calls: Vec<PlannedCall>,
    mut events: Vec<AgentEvent>,
) -> AgentLoopTransition {
    while let Some(PlannedCall::Rejected { .. }) = calls.first() {
        let PlannedCall::Rejected { call_id, error } = calls.remove(0) else {
            unreachable!();
        };
        events.push(tool_failed_event(&call_id, &error));
        state.messages.push(ModelMessage::tool_result(
            call_id,
            tool_error_content(
                &error.code,
                &error.message,
                error.category,
                error.retryable,
                error.retry_after_ms,
            ),
        ));
    }
    let Some(PlannedCall::Ready(first)) = calls.first().cloned() else {
        let effect = load_tool_set_effect(&mut state);
        state.phase = AgentLoopPhase::AwaitingToolSet {
            effect: effect.clone(),
        };
        return transition(state, events, vec![effect]);
    };

    if state
        .config
        .approval_policy
        .requires_review(first.risk_level)
    {
        calls.remove(0);
        let approval_key = format!("{}:{}:{}", state.run_id, state.next_step, first.call_id);
        let approval_id = ApprovalId::stable("agent-loop", &approval_key);
        let effect_id = next_effect_id(&mut state, "approval");
        let effect = AgentLoopEffect {
            effect_id,
            kind: AgentLoopEffectKind::RequestApproval {
                approval_id,
                run_id: state.run_id,
                call: first.clone(),
            },
        };
        events.push(AgentEvent::ApprovalRequested {
            approval_id,
            call_id: first.call_id.clone(),
            tool_name: first.name.clone(),
            risk_level: first.risk_level,
            arguments: first.public_arguments.clone(),
        });
        state.phase = AgentLoopPhase::AwaitingApproval {
            effect: effect.clone(),
            tool_set_revision,
            call: first,
            remaining: calls,
        };
        return transition(state, events, vec![effect]);
    }

    let mut batch = vec![first];
    calls.remove(0);
    if state.config.tool_call_strategy == ToolCallStrategy::ParallelSafe
        && is_parallel_candidate(state.config.approval_policy, &batch[0])
    {
        while let Some(PlannedCall::Ready(candidate)) = calls.first() {
            if !is_parallel_candidate(state.config.approval_policy, candidate) {
                break;
            }
            let PlannedCall::Ready(candidate) = calls.remove(0) else {
                unreachable!();
            };
            batch.push(candidate);
        }
    }

    let mut effects = Vec::with_capacity(batch.len());
    for call in &batch {
        events.push(AgentEvent::ToolExecutionStarted {
            call_id: call.call_id.clone(),
            arguments: call.public_arguments.clone(),
        });
        let effect_id = next_effect_id(&mut state, "tool");
        effects.push(AgentLoopEffect {
            effect_id,
            kind: AgentLoopEffectKind::InvokeTool {
                run_id: state.run_id,
                tool_set_revision,
                call: call.clone(),
                timeout_ms: state.config.tool_timeout_ms,
            },
        });
    }
    state.phase = AgentLoopPhase::AwaitingTools {
        effects: effects.clone(),
        calls: batch,
        results: BTreeMap::new(),
        remaining: calls,
    };
    transition(state, events, effects)
}

fn approval_resolved(
    mut state: AgentLoopReducerState,
    tool_set_revision: u64,
    call: ToolInvocation,
    remaining: Vec<PlannedCall>,
    resolution: ApprovalResolution,
) -> AgentLoopTransition {
    let approval_key = format!("{}:{}:{}", state.run_id, state.next_step, call.call_id);
    let approval_id = ApprovalId::stable("agent-loop", &approval_key);
    let mut events = vec![AgentEvent::ApprovalResolved {
        approval_id,
        call_id: call.call_id.clone(),
        resolution: resolution.clone(),
    }];
    if resolution.decision == ApprovalDecision::Deny {
        let error = portable_error(
            "tool_rejected",
            rejection_message(&resolution),
            ToolErrorCategory::PermissionDenied,
            false,
            None,
        );
        events.push(tool_failed_event(&call.call_id, &error));
        state.messages.push(ModelMessage::tool_result(
            call.call_id,
            tool_error_content(
                &error.code,
                &error.message,
                error.category,
                error.retryable,
                error.retry_after_ms,
            ),
        ));
        return schedule_calls(state, tool_set_revision, remaining, events);
    }

    events.push(AgentEvent::ToolExecutionStarted {
        call_id: call.call_id.clone(),
        arguments: call.public_arguments.clone(),
    });
    let effect_id = next_effect_id(&mut state, "tool");
    let effect = AgentLoopEffect {
        effect_id,
        kind: AgentLoopEffectKind::InvokeTool {
            run_id: state.run_id,
            tool_set_revision,
            call: call.clone(),
            timeout_ms: state.config.tool_timeout_ms,
        },
    };
    state.phase = AgentLoopPhase::AwaitingTools {
        effects: vec![effect.clone()],
        calls: vec![call],
        results: BTreeMap::new(),
        remaining,
    };
    transition(state, events, vec![effect])
}

#[allow(clippy::too_many_arguments)]
fn tool_finished(
    mut state: AgentLoopReducerState,
    effects: Vec<AgentLoopEffect>,
    calls: Vec<ToolInvocation>,
    mut results: BTreeMap<String, ToolInvocationResult>,
    remaining: Vec<PlannedCall>,
    effect_id: String,
    result: ToolInvocationResult,
) -> AgentLoopTransition {
    let Some(index) = effects
        .iter()
        .position(|effect| effect.effect_id == effect_id)
    else {
        return fail(
            state,
            "tool_result_unexpected",
            "the tool result does not match a pending effect",
            false,
        );
    };
    if results.contains_key(&effect_id) {
        return fail(
            state,
            "tool_result_duplicate",
            "the pending tool effect already has a result",
            false,
        );
    }
    if matches!(result, ToolInvocationResult::Suspended { .. }) && calls.len() > 1 {
        return fail(
            state,
            "parallel_tool_suspended",
            "a parallel-safe tool unexpectedly suspended",
            false,
        );
    }
    if let ToolInvocationResult::Suspended { suspension } = result {
        state.phase = AgentLoopPhase::AwaitingToolResume {
            call_id: calls[index].call_id.clone(),
            remaining,
            suspension,
        };
        return transition(state, Vec::new(), Vec::new());
    }
    results.insert(effect_id, result);
    if results.len() != effects.len() {
        state.phase = AgentLoopPhase::AwaitingTools {
            effects,
            calls,
            results,
            remaining,
        };
        return transition(state, Vec::new(), Vec::new());
    }

    let mut events = Vec::new();
    for (effect, call) in effects.iter().zip(&calls) {
        let Some(result) = results.remove(&effect.effect_id) else {
            return fail(
                state,
                "parallel_tool_result_missing",
                "a tool execution ended without a result",
                false,
            );
        };
        match result {
            ToolInvocationResult::Completed { content } => {
                events.push(AgentEvent::ToolExecutionCompleted {
                    call_id: call.call_id.clone(),
                    output: content.clone(),
                });
                state
                    .messages
                    .push(ModelMessage::tool_result(call.call_id.clone(), content));
            }
            ToolInvocationResult::Failed { error } => {
                events.push(tool_failed_event(&call.call_id, &error));
                state.messages.push(ModelMessage::tool_result(
                    call.call_id.clone(),
                    tool_error_content(
                        &error.code,
                        &error.message,
                        error.category,
                        error.retryable,
                        error.retry_after_ms,
                    ),
                ));
            }
            ToolInvocationResult::Suspended { .. } => unreachable!(),
        }
    }
    schedule_calls(state, tool_set_revision(&effects), remaining, events)
}

fn tool_resumed(
    mut state: AgentLoopReducerState,
    call_id: String,
    remaining: Vec<PlannedCall>,
    content: String,
) -> AgentLoopTransition {
    let mut events = vec![AgentEvent::ToolExecutionCompleted {
        call_id: call_id.clone(),
        output: content.clone(),
    }];
    state
        .messages
        .push(ModelMessage::tool_result(call_id, content));
    for call in remaining {
        match call {
            PlannedCall::Ready(call) => {
                let error = portable_error(
                    "tool_deferred_by_suspension",
                    "another tool call suspended this model step; request this tool again if it is still needed",
                    ToolErrorCategory::Conflict,
                    true,
                    None,
                );
                events.push(tool_failed_event(&call.call_id, &error));
                state.messages.push(ModelMessage::tool_result(
                    call.call_id,
                    tool_error_content(
                        &error.code,
                        &error.message,
                        error.category,
                        error.retryable,
                        error.retry_after_ms,
                    ),
                ));
            }
            PlannedCall::Rejected { call_id, error } => {
                events.push(tool_failed_event(&call_id, &error));
                state.messages.push(ModelMessage::tool_result(
                    call_id,
                    tool_error_content(
                        &error.code,
                        &error.message,
                        error.category,
                        error.retryable,
                        error.retry_after_ms,
                    ),
                ));
            }
        }
    }
    let effect = load_tool_set_effect(&mut state);
    state.phase = AgentLoopPhase::AwaitingToolSet {
        effect: effect.clone(),
    };
    transition(state, events, vec![effect])
}

fn load_tool_set_effect(state: &mut AgentLoopReducerState) -> AgentLoopEffect {
    AgentLoopEffect {
        effect_id: next_effect_id(state, "tool-set"),
        kind: AgentLoopEffectKind::LoadToolSet {
            run_id: state.run_id,
            allowed_tools: state.config.allowed_tools.clone(),
            allow_run_adf: state.config.allow_run_adf,
        },
    }
}

fn next_effect_id(state: &mut AgentLoopReducerState, kind: &str) -> String {
    let sequence = state.next_effect_sequence;
    state.next_effect_sequence = state.next_effect_sequence.saturating_add(1);
    format!("{}:{kind}:{sequence}", state.run_id)
}

fn tool_set_revision(effects: &[AgentLoopEffect]) -> u64 {
    effects
        .first()
        .and_then(|effect| match effect.kind {
            AgentLoopEffectKind::InvokeTool {
                tool_set_revision, ..
            } => Some(tool_set_revision),
            _ => None,
        })
        .unwrap_or_default()
}

fn is_parallel_candidate(policy: ToolApprovalPolicy, call: &ToolInvocation) -> bool {
    !policy.requires_review(call.risk_level)
        && call.execution.concurrency == ToolConcurrency::ParallelSafe
        && call.execution.completion == ToolCompletion::Immediate
}

fn empty_tool_set(revision: u64) -> ToolSetSnapshot {
    ToolSetSnapshot::new(revision, Vec::new(), Vec::new())
        .expect("an empty tool set is always valid")
}

fn portable_error(
    code: impl Into<String>,
    message: impl Into<String>,
    category: ToolErrorCategory,
    retryable: bool,
    retry_after_ms: Option<u64>,
) -> PortableToolError {
    PortableToolError {
        code: code.into(),
        message: message.into(),
        category,
        retryable,
        retry_after_ms,
    }
}

fn tool_failed_event(call_id: &str, error: &PortableToolError) -> AgentEvent {
    AgentEvent::ToolExecutionFailed {
        call_id: call_id.into(),
        code: error.code.clone(),
        message: error.message.clone(),
        category: error.category,
        retryable: error.retryable,
        retry_after_ms: error.retry_after_ms,
    }
}

fn zero_usage() -> TokenUsage {
    TokenUsage {
        input_tokens: 0,
        output_tokens: 0,
        total_tokens: 0,
        source: TokenUsageSource::ProviderReported,
    }
}

fn transition(
    state: AgentLoopReducerState,
    events: Vec<AgentEvent>,
    effects: Vec<AgentLoopEffect>,
) -> AgentLoopTransition {
    let outcome = state.phase.outcome();
    AgentLoopTransition {
        protocol_version: AGENT_LOOP_REDUCER_PROTOCOL_VERSION,
        state,
        events,
        effects,
        outcome,
    }
}

fn fail(
    state: AgentLoopReducerState,
    code: impl Into<String>,
    message: impl Into<String>,
    retryable: bool,
) -> AgentLoopTransition {
    fail_with_events(state, Vec::new(), code, message, retryable)
}

fn fail_with_events(
    state: AgentLoopReducerState,
    events: Vec<AgentEvent>,
    code: impl Into<String>,
    message: impl Into<String>,
    retryable: bool,
) -> AgentLoopTransition {
    terminal_error(
        state,
        MachineError {
            code: code.into(),
            message: message.into(),
            retryable,
        },
    )
    .with_events(events)
}

fn terminal_error(mut state: AgentLoopReducerState, error: MachineError) -> AgentLoopTransition {
    state.phase = AgentLoopPhase::Terminal {
        outcome: AgentLoopOutcome::Failed { error },
    };
    transition(state, Vec::new(), Vec::new())
}

trait WithEvents {
    fn with_events(self, events: Vec<AgentEvent>) -> Self;
}

impl WithEvents for AgentLoopTransition {
    fn with_events(mut self, mut events: Vec<AgentEvent>) -> Self {
        events.append(&mut self.events);
        self.events = events;
        self
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::harness::{
        ModelError, ModelErrorKind, ToolBinding, ToolDefinition, ToolIdempotency,
    };

    fn config() -> AgentLoopReducerConfig {
        AgentLoopReducerConfig {
            model: "test-model".into(),
            system_prompt: "system".into(),
            max_output_tokens: Some(256),
            max_steps: 4,
            allowed_tools: None,
            allow_run_adf: false,
            approval_policy: ToolApprovalPolicy::new(ToolApprovalPolicy::DISABLED_REVIEW_LEVEL),
            tool_call_strategy: ToolCallStrategy::Sequential,
            model_timeout_ms: 120_000,
            tool_timeout_ms: 30_000,
        }
    }

    fn tool_set() -> ToolSetSnapshot {
        ToolSetSnapshot::new(
            7,
            vec![
                ToolDefinition::new(
                    "artifact_write",
                    "write an artifact",
                    json!({"type": "object"}),
                )
                .with_execution_policy(ToolExecutionPolicy {
                    idempotency: ToolIdempotency::Idempotent,
                    ..ToolExecutionPolicy::default()
                }),
            ],
            vec![ToolBinding {
                name: "artifact_write".into(),
                kind: ToolBindingKind::Static,
                argument_visibility: ToolArgumentVisibility::Full,
            }],
        )
        .expect("tool set")
    }

    fn effect_id(transition: &AgentLoopTransition) -> String {
        transition.effects[0].effect_id.clone()
    }

    #[test]
    fn drives_model_tool_model_without_host_specific_io() {
        let run_id = RunId::new();
        let started = AgentLoopReducer::start(AgentLoopStart {
            run_id,
            input: "create it".into(),
            attachments: Vec::new(),
            prior_messages: Vec::new(),
            context_fingerprint: None,
            config: config(),
        });
        assert!(matches!(
            started.effects[0].kind,
            AgentLoopEffectKind::LoadToolSet { .. }
        ));

        let load_id = effect_id(&started);
        let loaded = AgentLoopReducer::dispatch(
            started.state,
            AgentLoopInput::ToolSetLoaded {
                effect_id: load_id,
                tool_set: tool_set(),
            },
        );
        let model_id = effect_id(&loaded);
        let mut transition = loaded;
        for event in [
            ModelEvent::ToolCallStarted {
                call_id: "call-1".into(),
                name: "artifact_write".into(),
            },
            ModelEvent::ToolCallArgumentsDelta {
                call_id: "call-1".into(),
                delta: "{\"name\":\"a.md\"}".into(),
            },
            ModelEvent::Completed {
                finish_reason: FinishReason::ToolCall,
            },
        ] {
            transition = AgentLoopReducer::dispatch(
                transition.state,
                AgentLoopInput::ModelEvent {
                    effect_id: model_id.clone(),
                    event,
                },
            );
        }
        assert!(matches!(
            transition.effects[0].kind,
            AgentLoopEffectKind::ValidateTools { .. }
        ));
        let validate_id = effect_id(&transition);
        transition = AgentLoopReducer::dispatch(
            transition.state,
            AgentLoopInput::ToolsValidated {
                effect_id: validate_id,
                results: vec![ToolValidation {
                    call_id: "call-1".into(),
                    error: None,
                }],
            },
        );
        assert!(matches!(
            transition.effects[0].kind,
            AgentLoopEffectKind::InvokeTool { .. }
        ));
        let tool_id = effect_id(&transition);
        transition = AgentLoopReducer::dispatch(
            transition.state,
            AgentLoopInput::ToolFinished {
                effect_id: tool_id,
                result: ToolInvocationResult::Completed {
                    content: "artifact:1".into(),
                },
            },
        );
        assert!(transition.events.iter().any(|event| matches!(
            event,
            AgentEvent::ToolExecutionCompleted { call_id, .. } if call_id == "call-1"
        )));
        assert!(matches!(
            transition.effects[0].kind,
            AgentLoopEffectKind::LoadToolSet { .. }
        ));
    }

    #[test]
    fn rejects_stale_effect_results() {
        let started = AgentLoopReducer::start(AgentLoopStart {
            run_id: RunId::new(),
            input: "hello".into(),
            attachments: Vec::new(),
            prior_messages: Vec::new(),
            context_fingerprint: None,
            config: config(),
        });
        let transition = AgentLoopReducer::dispatch(
            started.state,
            AgentLoopInput::ToolSetLoaded {
                effect_id: "stale".into(),
                tool_set: tool_set(),
            },
        );
        assert!(matches!(
            transition.outcome,
            AgentLoopOutcome::Failed { ref error }
                if error.code == "reducer_input_unexpected"
        ));
    }

    #[test]
    fn json_dispatch_contract_round_trips() {
        let started = AgentLoopReducer::start(AgentLoopStart {
            run_id: RunId::new(),
            input: "hello".into(),
            attachments: Vec::new(),
            prior_messages: Vec::new(),
            context_fingerprint: None,
            config: config(),
        });
        let load_id = effect_id(&started);
        let request = AgentLoopDispatch {
            protocol_version: AGENT_LOOP_REDUCER_PROTOCOL_VERSION,
            state: started.state,
            input: AgentLoopInput::ToolSetFailed {
                effect_id: load_id,
                error: MachineError {
                    code: "host_unavailable".into(),
                    message: "unavailable".into(),
                    retryable: true,
                },
            },
        };
        let json = serde_json::to_string(&request).expect("request JSON");
        let response = AgentLoopReducer::dispatch_json(&json).expect("response JSON");
        let response: AgentLoopTransition =
            serde_json::from_str(&response).expect("transition JSON");
        assert!(matches!(response.outcome, AgentLoopOutcome::Failed { .. }));
    }

    #[test]
    fn model_errors_are_portable_inputs() {
        let event = ModelEvent::Failed {
            error: ModelError::new(ModelErrorKind::RateLimited, "slow down", true),
        };
        let json = serde_json::to_string(&event).expect("model event JSON");
        let decoded: ModelEvent = serde_json::from_str(&json).expect("model event");
        assert_eq!(decoded, event);
    }
}
