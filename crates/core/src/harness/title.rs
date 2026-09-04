//! Pure session-title generation flow.
//!
//! Title generation is deliberately separate from the conversation history.
//! It emits one tool-free model effect and carries the expected session
//! revision so the host can commit the result with compare-and-swap semantics.

use serde::{Deserialize, Serialize};

use crate::harness::{
    AgentLoopEffect, AgentLoopEffectKind, FinishReason, MessageId, ModelEvent,
    ModelInvocationPurpose, ModelMessage, ModelRequest, RunId, SessionId,
};

pub const TITLE_REDUCER_PROTOCOL_VERSION: u32 = 1;
const DEFAULT_MAX_OUTPUT_TOKENS: u32 = 32;
const MAX_TITLE_CHARS: usize = 80;
const FALLBACK_TITLE_CHARS: usize = 48;
const MIN_SOURCE_CHARS: usize = 4;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TitleGenerationStart {
    pub session_id: SessionId,
    pub first_message_id: MessageId,
    pub expected_session_revision: u64,
    pub model: String,
    pub source_text: String,
    #[serde(default = "default_max_output_tokens")]
    pub max_output_tokens: u32,
    pub timeout_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TitleGenerationStartDispatch {
    pub protocol_version: u32,
    pub start: TitleGenerationStart,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TitleGenerationState {
    pub protocol_version: u32,
    pub session_id: SessionId,
    pub first_message_id: MessageId,
    pub expected_session_revision: u64,
    pub fallback_title: String,
    generated: String,
    pending_effect: Option<AgentLoopEffect>,
    outcome: TitleGenerationOutcome,
}

impl TitleGenerationState {
    #[must_use]
    pub fn pending_effect(&self) -> Option<&AgentLoopEffect> {
        self.pending_effect.as_ref()
    }

    #[must_use]
    pub fn outcome(&self) -> &TitleGenerationOutcome {
        &self.outcome
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum TitleGenerationOutcome {
    Running,
    /// The first message is too short; the host may retry after the next
    /// meaningful user message rather than spending a model request now.
    Deferred,
    Ready {
        title: String,
        expected_session_revision: u64,
    },
    Fallback {
        title: String,
        expected_session_revision: u64,
        reason: String,
    },
    /// A manual rename won the race. Any late provider result must be ignored.
    UserLocked,
    Failed {
        code: String,
        message: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TitleGenerationInput {
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
    UserLocked,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TitleGenerationTransition {
    pub protocol_version: u32,
    pub state: TitleGenerationState,
    pub effects: Vec<AgentLoopEffect>,
    pub outcome: TitleGenerationOutcome,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TitleGenerationDispatch {
    pub protocol_version: u32,
    pub state: TitleGenerationState,
    pub input: TitleGenerationInput,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct TitleGenerationReducer;

impl TitleGenerationReducer {
    #[must_use]
    pub fn start(start: TitleGenerationStart) -> TitleGenerationTransition {
        let fallback_title = normalize_title(&start.source_text, FALLBACK_TITLE_CHARS);
        let source_chars = start.source_text.trim().chars().count();
        if source_chars < MIN_SOURCE_CHARS {
            let state = TitleGenerationState {
                protocol_version: TITLE_REDUCER_PROTOCOL_VERSION,
                session_id: start.session_id,
                first_message_id: start.first_message_id,
                expected_session_revision: start.expected_session_revision,
                fallback_title,
                generated: String::new(),
                pending_effect: None,
                outcome: TitleGenerationOutcome::Deferred,
            };
            return transition(state, Vec::new());
        }

        let idempotency_key = format!("title:{}:{}", start.session_id, start.first_message_id);
        let run_id = RunId::stable("session-title", &idempotency_key);
        let effect = AgentLoopEffect {
            effect_id: idempotency_key,
            kind: AgentLoopEffectKind::InvokeModel {
                purpose: ModelInvocationPurpose::SessionTitle,
                request: ModelRequest {
                    run_id,
                    model: start.model,
                    messages: vec![
                        ModelMessage::system(
                            "Generate one concise conversation title. Return only the title, without quotes, markdown, commentary, tools, or search.",
                        ),
                        ModelMessage::user(start.source_text),
                    ],
                    tools: Vec::new(),
                    max_output_tokens: Some(start.max_output_tokens.clamp(1, 64)),
                },
                timeout_ms: start.timeout_ms,
            },
        };
        let state = TitleGenerationState {
            protocol_version: TITLE_REDUCER_PROTOCOL_VERSION,
            session_id: start.session_id,
            first_message_id: start.first_message_id,
            expected_session_revision: start.expected_session_revision,
            fallback_title,
            generated: String::new(),
            pending_effect: Some(effect.clone()),
            outcome: TitleGenerationOutcome::Running,
        };
        transition(state, vec![effect])
    }

    pub fn start_json(json: &str) -> Result<String, serde_json::Error> {
        let request: TitleGenerationStartDispatch = serde_json::from_str(json)?;
        let response = if request.protocol_version == TITLE_REDUCER_PROTOCOL_VERSION {
            Self::start(request.start)
        } else {
            let response = Self::start(request.start);
            failed(
                response.state,
                "title_protocol_incompatible",
                "the title start protocol version is not supported",
            )
        };
        serde_json::to_string(&response)
    }

    #[must_use]
    pub fn dispatch(
        mut state: TitleGenerationState,
        input: TitleGenerationInput,
    ) -> TitleGenerationTransition {
        if matches!(input, TitleGenerationInput::UserLocked) {
            state.pending_effect = None;
            state.outcome = TitleGenerationOutcome::UserLocked;
            return transition(state, Vec::new());
        }
        if state.protocol_version != TITLE_REDUCER_PROTOCOL_VERSION {
            return failed(
                state,
                "title_protocol_incompatible",
                "the title checkpoint protocol version is not supported",
            );
        }
        let Some(effect) = state.pending_effect.clone() else {
            return failed(
                state,
                "title_input_unexpected",
                "the title flow has no pending model effect",
            );
        };
        let input_effect_id = match &input {
            TitleGenerationInput::ModelEvent { effect_id, .. }
            | TitleGenerationInput::ModelStreamEnded { effect_id }
            | TitleGenerationInput::ModelTimedOut { effect_id } => effect_id,
            TitleGenerationInput::UserLocked => unreachable!(),
        };
        if input_effect_id != &effect.effect_id {
            return failed(
                state,
                "title_effect_mismatch",
                "the title input does not match the pending model effect",
            );
        }

        match input {
            TitleGenerationInput::ModelEvent { event, .. } => match event {
                ModelEvent::Accepted { .. }
                | ModelEvent::ReasoningDelta { .. }
                | ModelEvent::Usage { .. } => transition(state, Vec::new()),
                ModelEvent::TextDelta { delta } => {
                    state.generated.push_str(&delta);
                    transition(state, Vec::new())
                }
                ModelEvent::Completed {
                    finish_reason: FinishReason::Stop | FinishReason::Length,
                } => complete(state),
                ModelEvent::Completed { .. } => {
                    fallback(state, "model_title_finish_reason_invalid".into())
                }
                ModelEvent::Failed { error } => {
                    fallback(state, format!("model_failed:{}", error.kind().code()))
                }
                ModelEvent::ToolCallStarted { .. } | ModelEvent::ToolCallArgumentsDelta { .. } => {
                    fallback(state, "model_requested_forbidden_tool".into())
                }
            },
            TitleGenerationInput::ModelStreamEnded { .. } => {
                fallback(state, "model_stream_ended".into())
            }
            TitleGenerationInput::ModelTimedOut { .. } => fallback(state, "model_timeout".into()),
            TitleGenerationInput::UserLocked => unreachable!(),
        }
    }

    pub fn dispatch_json(json: &str) -> Result<String, serde_json::Error> {
        let request: TitleGenerationDispatch = serde_json::from_str(json)?;
        let response = if request.protocol_version == TITLE_REDUCER_PROTOCOL_VERSION {
            Self::dispatch(request.state, request.input)
        } else {
            failed(
                request.state,
                "title_protocol_incompatible",
                "the title dispatch protocol version is not supported",
            )
        };
        serde_json::to_string(&response)
    }
}

fn complete(mut state: TitleGenerationState) -> TitleGenerationTransition {
    let title = normalize_title(&state.generated, MAX_TITLE_CHARS);
    if title.is_empty() {
        return fallback(state, "empty_model_title".into());
    }
    state.pending_effect = None;
    state.outcome = TitleGenerationOutcome::Ready {
        title,
        expected_session_revision: state.expected_session_revision,
    };
    transition(state, Vec::new())
}

fn fallback(mut state: TitleGenerationState, reason: String) -> TitleGenerationTransition {
    state.pending_effect = None;
    state.outcome = TitleGenerationOutcome::Fallback {
        title: state.fallback_title.clone(),
        expected_session_revision: state.expected_session_revision,
        reason,
    };
    transition(state, Vec::new())
}

fn failed(
    mut state: TitleGenerationState,
    code: impl Into<String>,
    message: impl Into<String>,
) -> TitleGenerationTransition {
    state.pending_effect = None;
    state.outcome = TitleGenerationOutcome::Failed {
        code: code.into(),
        message: message.into(),
    };
    transition(state, Vec::new())
}

fn transition(
    state: TitleGenerationState,
    effects: Vec<AgentLoopEffect>,
) -> TitleGenerationTransition {
    TitleGenerationTransition {
        protocol_version: TITLE_REDUCER_PROTOCOL_VERSION,
        outcome: state.outcome.clone(),
        state,
        effects,
    }
}

fn normalize_title(value: &str, max_chars: usize) -> String {
    let collapsed = value.split_whitespace().collect::<Vec<_>>().join(" ");
    let trimmed = collapsed
        .trim_matches(|character: char| {
            character.is_whitespace()
                || matches!(character, '"' | '\'' | '`' | '#' | '*' | '_' | '-')
        })
        .trim();
    trimmed.chars().take(max_chars).collect()
}

const fn default_max_output_tokens() -> u32 {
    DEFAULT_MAX_OUTPUT_TOKENS
}

#[cfg(test)]
mod tests {
    use super::*;

    fn start(source_text: &str) -> TitleGenerationTransition {
        TitleGenerationReducer::start(TitleGenerationStart {
            session_id: SessionId::new(),
            first_message_id: MessageId::new(),
            expected_session_revision: 3,
            model: "title-model".into(),
            source_text: source_text.into(),
            max_output_tokens: 32,
            timeout_ms: 10_000,
        })
    }

    #[test]
    fn title_effect_is_tool_free_and_idempotent() {
        let transition = start("Design a browser WASM agent");
        let effect = &transition.effects[0];
        assert!(effect.effect_id.starts_with("title:"));
        let AgentLoopEffectKind::InvokeModel {
            purpose, request, ..
        } = &effect.kind
        else {
            panic!("title must invoke a model");
        };
        assert_eq!(*purpose, ModelInvocationPurpose::SessionTitle);
        assert!(request.tools.is_empty());
        assert_eq!(request.max_output_tokens, Some(32));
    }

    #[test]
    fn completes_without_touching_conversation_history() {
        let mut transition = start("Design a browser WASM agent");
        let effect_id = transition.effects[0].effect_id.clone();
        for event in [
            ModelEvent::TextDelta {
                delta: " Browser WASM Agent ".into(),
            },
            ModelEvent::Completed {
                finish_reason: FinishReason::Stop,
            },
        ] {
            transition = TitleGenerationReducer::dispatch(
                transition.state,
                TitleGenerationInput::ModelEvent {
                    effect_id: effect_id.clone(),
                    event,
                },
            );
        }
        assert!(matches!(
            transition.outcome,
            TitleGenerationOutcome::Ready { ref title, expected_session_revision: 3 }
                if title == "Browser WASM Agent"
        ));
    }

    #[test]
    fn manual_title_lock_discards_a_late_result() {
        let transition = start("Design a browser WASM agent");
        let transition =
            TitleGenerationReducer::dispatch(transition.state, TitleGenerationInput::UserLocked);
        assert_eq!(transition.outcome, TitleGenerationOutcome::UserLocked);
        assert!(transition.effects.is_empty());
    }

    #[test]
    fn short_source_is_deferred() {
        let transition = start("hi");
        assert_eq!(transition.outcome, TitleGenerationOutcome::Deferred);
        assert!(transition.effects.is_empty());
    }
}
