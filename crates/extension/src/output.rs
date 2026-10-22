//! Output-only adapter from Mina run events to the `agent-chat/1` wire model.

use std::collections::HashSet;

use agent_core::harness::RunId;
use agent_core::harness::{
    ApprovalDecision, FinishReason, OutputChannel, RunEvent, RunEventKind, TokenUsage,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

pub type ChatEvent = Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentChatConfig {
    pub conversation_id: String,
    pub turn_id: String,
    pub message_id: String,
    pub capabilities: Capabilities,
    pub limits: Limits,
}

impl AgentChatConfig {
    #[must_use]
    pub fn new(
        conversation_id: impl Into<String>,
        turn_id: impl Into<String>,
        message_id: impl Into<String>,
    ) -> Self {
        Self {
            conversation_id: conversation_id.into(),
            turn_id: turn_id.into(),
            message_id: message_id.into(),
            capabilities: Capabilities::default(),
            limits: Limits::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capabilities {
    #[serde(rename = "blockIds")]
    pub block_ids: bool,
    pub resume: bool,
    #[serde(rename = "clientTools")]
    pub client_tools: bool,
    pub permissions: bool,
    pub a2ui: bool,
}

impl Default for Capabilities {
    fn default() -> Self {
        Self {
            block_ids: true,
            // Event replay/resume is not implemented by this adapter, so it must not advertise the capability.
            resume: false,
            client_tools: false,
            permissions: true,
            a2ui: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Limits {
    #[serde(rename = "disconnectGracePeriodMs")]
    pub disconnect_grace_period_ms: u64,
    #[serde(rename = "maxAttachmentBytes")]
    pub max_attachment_bytes: u64,
    #[serde(rename = "maxAttachmentCount")]
    pub max_attachment_count: u64,
    #[serde(rename = "resumeWindowEvents")]
    pub resume_window_events: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            disconnect_grace_period_ms: 10_000,
            max_attachment_bytes: 10_485_760,
            max_attachment_count: 10,
            resume_window_events: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChatFrame {
    pub event: ChatEvent,
    pub event_id: String,
}

impl ChatFrame {
    #[must_use]
    pub fn to_sse(&self) -> String {
        let data = serde_json::to_string(&self.event).expect("ChatEvent is always serializable");
        format!("id: {}\ndata: {data}\n\n", self.event_id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActiveBlockKind {
    Text,
    Reasoning,
    Tool,
    Permission,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ActiveBlock {
    kind: ActiveBlockKind,
    id: String,
}

#[derive(Debug, Clone, Default)]
struct State {
    next_event_id: u64,
    seen: HashSet<(RunId, u64)>,
    finished: bool,
    active_block: Option<ActiveBlock>,
    next_text_block: u64,
    next_reasoning_block: u64,
    usage: Option<TokenUsage>,
}

#[derive(Debug, Clone)]
pub struct AgentChatAdapter {
    config: AgentChatConfig,
    state: State,
}

impl AgentChatAdapter {
    #[must_use]
    pub fn new(config: AgentChatConfig) -> Self {
        Self {
            config,
            state: State::default(),
        }
    }

    #[must_use]
    pub fn server_hello(&self) -> ChatEvent {
        json!({
            "type": "server-hello",
            "protocol": "agent-chat/1",
            "capabilities": self.config.capabilities,
            "limits": self.config.limits,
        })
    }

    #[must_use]
    pub fn server_hello_json(&self) -> String {
        format!(
            r#"{{"type":"server-hello","protocol":"agent-chat/1","capabilities":{{"blockIds":{},"resume":{},"clientTools":{},"permissions":{},"a2ui":{}}},"limits":{{"disconnectGracePeriodMs":{},"maxAttachmentBytes":{},"maxAttachmentCount":{},"resumeWindowEvents":{}}}}}"#,
            self.config.capabilities.block_ids,
            self.config.capabilities.resume,
            self.config.capabilities.client_tools,
            self.config.capabilities.permissions,
            self.config.capabilities.a2ui,
            self.config.limits.disconnect_grace_period_ms,
            self.config.limits.max_attachment_bytes,
            self.config.limits.max_attachment_count,
            self.config.limits.resume_window_events,
        )
    }

    #[must_use]
    pub fn server_hello_sse(&self) -> String {
        format!("data: {}\n\n", self.server_hello_json())
    }

    #[must_use]
    pub fn heartbeat() -> &'static str {
        ": heartbeat\n\n"
    }

    #[must_use]
    pub fn done() -> &'static str {
        "data: [DONE]\n\n"
    }

    pub fn adapt(&mut self, run_event: &RunEvent) -> Vec<ChatFrame> {
        if !self.state.seen.insert((run_event.run_id, run_event.seq)) {
            return Vec::new();
        }
        if self.state.finished {
            return vec![self.diagnostic(run_event, "event_after_terminal")];
        }

        let mut payloads = Vec::new();
        match &run_event.kind {
            RunEventKind::RunStarted => {
                payloads.push(self.envelope(
                    run_event,
                    "message-start",
                    None,
                    None,
                    json!({"id": self.config.message_id}),
                ));
            }
            RunEventKind::OutputDelta { channel, delta } => {
                self.map_output(run_event, *channel, delta, &mut payloads);
            }
            RunEventKind::UsageUpdated { usage } => self.state.usage = Some(*usage),
            RunEventKind::ToolCallStarted { call_id, name } => {
                self.close_active(run_event, &mut payloads);
                let block_id = tool_block_id(call_id);
                self.state.active_block = Some(ActiveBlock {
                    kind: ActiveBlockKind::Tool,
                    id: block_id.clone(),
                });
                payloads.push(self.envelope(
                    run_event,
                    "tool-input-start",
                    Some(block_id),
                    None,
                    json!({"toolCallId": call_id, "name": name, "execution": "server"}),
                ));
            }
            RunEventKind::ToolCallArgumentsDelta { call_id, delta } => {
                payloads.push(self.envelope(
                    run_event,
                    "tool-input-delta",
                    Some(tool_block_id(call_id)),
                    None,
                    json!({"toolCallId": call_id, "delta": delta}),
                ));
            }
            RunEventKind::ToolExecutionStarted { call_id, arguments } => {
                payloads.push(self.envelope(
                    run_event,
                    "tool-input-available",
                    Some(tool_block_id(call_id)),
                    None,
                    json!({"toolCallId": call_id, "input": arguments}),
                ));
                payloads.push(self.envelope(
                    run_event,
                    "tool-executing",
                    Some(tool_block_id(call_id)),
                    None,
                    json!({"toolCallId": call_id}),
                ));
            }
            RunEventKind::ToolExecutionCompleted { call_id, output } => {
                payloads.push(self.envelope(
                    run_event,
                    "tool-output",
                    Some(tool_block_id(call_id)),
                    None,
                    json!({"toolCallId": call_id, "output": parse_json_or_string(output)}),
                ));
            }
            RunEventKind::ToolExecutionFailed {
                call_id,
                code,
                message,
                category,
                retryable,
                retry_after_ms,
            } => {
                let mut error = Map::new();
                error.insert("scope".into(), Value::String("tool".into()));
                error.insert("code".into(), Value::String(code.clone()));
                error.insert("message".into(), Value::String(message.clone()));
                error.insert("category".into(), Value::String(category.as_str().into()));
                error.insert("retryable".into(), Value::Bool(*retryable));
                if let Some(value) = retry_after_ms {
                    error.insert("retryAfterMs".into(), Value::from(*value));
                }
                payloads.push(self.envelope(
                    run_event,
                    "tool-error",
                    Some(tool_block_id(call_id)),
                    None,
                    json!({"toolCallId": call_id, "error": message}),
                ));
                payloads.push(self.envelope(run_event, "custom", None, None,
                    json!({"name": "mina-tool-error", "data": {"toolCallId": call_id, "error": error}})));
            }
            RunEventKind::ApprovalRequested {
                approval_id,
                call_id,
                tool_name,
                risk_level,
                arguments,
            } => {
                self.close_active(run_event, &mut payloads);
                let block_id = permission_block_id(approval_id);
                self.state.active_block = Some(ActiveBlock {
                    kind: ActiveBlockKind::Permission,
                    id: block_id.clone(),
                });
                payloads.push(self.envelope(
                    run_event,
                    "permission-request",
                    Some(block_id),
                    None,
                    json!({
                        "request": {
                            "id": approval_id.to_string(),
                            "toolCallId": call_id,
                            "toolName": tool_name,
                            "risk": risk_level,
                            "detail": serde_json::to_string_pretty(arguments).expect("arguments are JSON"),
                            "detailLanguage": "json",
                            "options": [
                                {"value": "allow-once", "decision": "allow-once"},
                                {"value": "deny", "decision": "deny", "promptForReason": true},
                            ],
                        },
                    }),
                ));
                payloads.push(self.end_event(run_event, "awaiting-permission"));
                self.state.finished = true;
            }
            RunEventKind::ApprovalResolved {
                approval_id,
                call_id: _,
                resolution,
            } => {
                let mut decision = json!({
                    "requestId": approval_id.to_string(),
                    "option": approval_decision(resolution.decision),
                    "decision": approval_decision(resolution.decision),
                });
                if let Some(reason) = &resolution.reason {
                    decision["reason"] = json!(reason);
                }
                payloads.push(self.envelope(
                    run_event,
                    "permission-resolved",
                    Some(permission_block_id(approval_id)),
                    None,
                    json!({
                        "requestId": approval_id.to_string(),
                        "resolution": decision,
                    }),
                ));
            }
            RunEventKind::RunCancelled => {
                self.close_active(run_event, &mut payloads);
                payloads.push(self.end_event(run_event, "cancelled"));
                self.state.finished = true;
            }
            RunEventKind::RunCompleted { finish_reason } => {
                self.close_active(run_event, &mut payloads);
                let reason = map_finish_reason(*finish_reason);
                payloads.push(self.end_event(run_event, reason));
                self.state.finished = true;
                if reason == "error" {
                    payloads.push(self.error_event(
                        run_event,
                        "finish_reason_error",
                        "The run completed with an unsupported finish reason",
                        false,
                    ));
                }
            }
            RunEventKind::RunFailed {
                code,
                message,
                retryable,
            } => {
                self.close_active(run_event, &mut payloads);
                payloads.push(self.end_event(run_event, "error"));
                payloads.push(self.error_event(run_event, code, message, *retryable));
                self.state.finished = true;
            }
            _ => payloads.push(self.diagnostic_event(run_event, "unknown_run_event")),
        }

        payloads
            .into_iter()
            .enumerate()
            .map(|(ordinal, mut event)| {
                let event_id = frame_id(run_event.seq, ordinal);
                if let Value::Object(object) = &mut event {
                    self.state.next_event_id += 1;
                    object.insert("eventId".into(), Value::from(self.state.next_event_id));
                }
                ChatFrame { event_id, event }
            })
            .collect()
    }

    fn map_output(
        &mut self,
        run_event: &RunEvent,
        channel: OutputChannel,
        delta: &str,
        output: &mut Vec<ChatEvent>,
    ) {
        let (kind, start_type, delta_type) = match channel {
            OutputChannel::AssistantText => (ActiveBlockKind::Text, "text-start", "text-delta"),
            OutputChannel::AssistantReasoning => (
                ActiveBlockKind::Reasoning,
                "reasoning-start",
                "reasoning-delta",
            ),
            _ => {
                output.push(self.diagnostic_event(run_event, "unknown_output_channel"));
                return;
            }
        };
        let block_id = match self.state.active_block.as_ref() {
            Some(active) if active.kind == kind => active.id.clone(),
            _ => {
                self.close_active(run_event, output);
                let block_id = self.next_output_block_id(kind);
                self.state.active_block = Some(ActiveBlock {
                    kind,
                    id: block_id.clone(),
                });
                output.push(self.envelope(
                    run_event,
                    start_type,
                    Some(block_id.clone()),
                    None,
                    json!({}),
                ));
                block_id
            }
        };
        output.push(self.envelope(
            run_event,
            delta_type,
            Some(block_id),
            None,
            json!({"delta": delta}),
        ));
    }

    fn next_output_block_id(&mut self, kind: ActiveBlockKind) -> String {
        match kind {
            ActiveBlockKind::Text => {
                let index = self.state.next_text_block;
                self.state.next_text_block += 1;
                format!("text:{index}")
            }
            ActiveBlockKind::Reasoning => {
                let index = self.state.next_reasoning_block;
                self.state.next_reasoning_block += 1;
                format!("reasoning:{index}")
            }
            ActiveBlockKind::Tool | ActiveBlockKind::Permission => {
                unreachable!("only text and reasoning blocks use generated ids")
            }
        }
    }

    fn close_active(&mut self, run_event: &RunEvent, output: &mut Vec<ChatEvent>) {
        if let Some(active) = self.state.active_block.take() {
            let end_type = match active.kind {
                ActiveBlockKind::Text => "text-end",
                ActiveBlockKind::Reasoning => "reasoning-end",
                ActiveBlockKind::Tool | ActiveBlockKind::Permission => return,
            };
            output.push(self.envelope(run_event, end_type, Some(active.id), None, json!({})));
        }
    }

    fn end_event(&self, run_event: &RunEvent, finish_reason: &'static str) -> ChatEvent {
        self.envelope(
            run_event,
            "message-end",
            None,
            None,
            json!({
                "finishReason": finish_reason,
                "usage": self.state.usage.map(usage_value),
            }),
        )
    }

    fn error_event(
        &self,
        run_event: &RunEvent,
        code: &str,
        message: &str,
        retryable: bool,
    ) -> ChatEvent {
        self.envelope(
            run_event,
            "error",
            None,
            None,
            json!({
                "error": message,
                "scope": "server",
                "code": code,
                "retryable": retryable,
            }),
        )
    }

    fn diagnostic(&mut self, run_event: &RunEvent, code: &str) -> ChatFrame {
        let mut event = self.diagnostic_event(run_event, code);
        self.state.next_event_id += 1;
        event["eventId"] = json!(self.state.next_event_id);
        ChatFrame {
            event_id: run_event.seq.to_string(),
            event,
        }
    }

    fn diagnostic_event(&self, run_event: &RunEvent, code: &str) -> ChatEvent {
        self.envelope(
            run_event,
            "custom",
            None,
            None,
            json!({"name": "mina-diagnostic", "data": {"code": code}}),
        )
    }

    fn envelope(
        &self,
        run_event: &RunEvent,
        event_type: &str,
        block_id: Option<String>,
        message_id: Option<String>,
        payload: Value,
    ) -> ChatEvent {
        let mut object = Map::new();
        object.insert("type".into(), Value::String(event_type.into()));
        object.insert(
            "conversationId".into(),
            Value::String(self.config.conversation_id.clone()),
        );
        object.insert("turnId".into(), Value::String(self.config.turn_id.clone()));
        object.insert("runId".into(), Value::String(run_event.run_id.to_string()));
        object.insert(
            "messageId".into(),
            Value::String(message_id.unwrap_or_else(|| self.config.message_id.clone())),
        );
        if let Some(block_id) = block_id {
            object.insert("blockId".into(), Value::String(block_id));
        }
        if let Value::Object(payload) = payload {
            object.extend(payload);
        }
        Value::Object(object)
    }
}

#[must_use]
pub fn map_finish_reason(reason: FinishReason) -> &'static str {
    match reason {
        FinishReason::Stop => "stop",
        FinishReason::Length => "length",
        FinishReason::ToolCall => "tool-calls",
        FinishReason::ContentFilter | FinishReason::Unknown => "error",
        _ => "error",
    }
}

fn frame_id(seq: u64, ordinal: usize) -> String {
    if ordinal == 0 {
        seq.to_string()
    } else {
        format!("{seq}:{ordinal:06}")
    }
}

fn tool_block_id(call_id: &str) -> String {
    format!("tool:{call_id}")
}

fn permission_block_id(approval_id: &impl std::fmt::Display) -> String {
    format!("permission:{approval_id}")
}

fn parse_json_or_string(output: &str) -> Value {
    serde_json::from_str(output).unwrap_or_else(|_| Value::String(output.into()))
}

fn usage_value(usage: TokenUsage) -> Value {
    json!({
        "inputTokens": usage.input_tokens,
        "outputTokens": usage.output_tokens,
        "totalTokens": usage.total_tokens,
    })
}

fn approval_decision(decision: ApprovalDecision) -> &'static str {
    match decision {
        ApprovalDecision::AllowOnce => "allow-once",
        ApprovalDecision::Deny => "deny",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_core::harness::{
        AgentEvent, ApprovalId, ApprovalResolution, RunEventKind, ToolErrorCategory, ToolRiskLevel,
    };
    use serde_json::json;
    use std::str::FromStr;

    fn run(seq: u64, kind: RunEventKind) -> RunEvent {
        RunEvent::new(
            RunId::from_str("00000000-0000-0000-0000-000000000001")
                .expect("fixture run id is valid"),
            seq,
            kind,
        )
    }

    fn adapter() -> AgentChatAdapter {
        AgentChatAdapter::new(AgentChatConfig::new("conv", "turn", "msg"))
    }

    fn event_json(adapter: &mut AgentChatAdapter, event: RunEvent) -> Vec<Value> {
        adapter
            .adapt(&event)
            .into_iter()
            .map(|frame| frame.event)
            .collect()
    }

    fn client_reducer_parts(events: &[Value]) -> Vec<String> {
        let mut parts = Vec::new();
        for event in events {
            let (kind, block_id) = match event["type"].as_str() {
                Some("text-start") => ("text", event["blockId"].as_str()),
                Some("tool-input-start") => ("tool", event["blockId"].as_str()),
                _ => continue,
            };
            let Some(block_id) = block_id else {
                continue;
            };
            let part = format!("{kind}:{block_id}");
            if !parts.contains(&part) {
                parts.push(part);
            }
        }
        parts
    }

    #[test]
    fn server_hello_matches_spec_fixture_byte_for_byte() {
        let mut config = AgentChatConfig::new("conv", "turn", "msg");
        config.capabilities = Capabilities {
            block_ids: true,
            resume: true,
            client_tools: true,
            permissions: true,
            a2ui: true,
        };
        let adapter = AgentChatAdapter::new(config);
        let expected = r#"{"type":"server-hello","protocol":"agent-chat/1","capabilities":{"blockIds":true,"resume":true,"clientTools":true,"permissions":true,"a2ui":true},"limits":{"disconnectGracePeriodMs":10000,"maxAttachmentBytes":10485760,"maxAttachmentCount":10,"resumeWindowEvents":0}}"#;
        assert_eq!(adapter.server_hello_json(), expected);
        assert_eq!(adapter.server_hello_sse(), format!("data: {expected}\n\n"));
    }

    #[test]
    fn unimplemented_resume_must_not_be_declared() {
        let adapter = adapter();
        let hello = adapter.server_hello();
        let capabilities = hello
            .get("capabilities")
            .and_then(Value::as_object)
            .expect("server-hello capabilities should be an object");

        assert!(!Capabilities::default().resume);
        assert_eq!(capabilities.get("resume"), Some(&json!(false)));
        assert_eq!(hello["limits"]["resumeWindowEvents"], 0);
    }

    #[test]
    fn maps_text_and_reasoning_with_stable_blocks() {
        let mut adapter = adapter();
        let events = event_json(&mut adapter, run(1, RunEventKind::RunStarted));
        assert_eq!(events[0]["type"], "message-start");
        let events = event_json(
            &mut adapter,
            run(
                2,
                RunEventKind::OutputDelta {
                    channel: OutputChannel::AssistantText,
                    delta: "hi".into(),
                },
            ),
        );
        assert_eq!(events[0]["type"], "text-start");
        assert_eq!(events[1]["type"], "text-delta");
        assert_eq!(events[0]["blockId"], "text:0");
        assert_eq!(events[1]["blockId"], "text:0");
        let events = event_json(
            &mut adapter,
            run(
                3,
                RunEventKind::OutputDelta {
                    channel: OutputChannel::AssistantText,
                    delta: " again".into(),
                },
            ),
        );
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["type"], "text-delta");
        assert_eq!(events[0]["blockId"], "text:0");
        let events = event_json(
            &mut adapter,
            run(
                4,
                RunEventKind::OutputDelta {
                    channel: OutputChannel::AssistantReasoning,
                    delta: "think".into(),
                },
            ),
        );
        assert_eq!(events[0]["type"], "text-end");
        assert_eq!(events[0]["blockId"], "text:0");
        assert_eq!(events[1]["type"], "reasoning-start");
        assert_eq!(events[1]["blockId"], "reasoning:0");
        assert_eq!(events[2]["blockId"], "reasoning:0");
    }

    #[test]
    fn allocates_a_new_text_block_after_a_tool() {
        let mut adapter = adapter();
        let _ = event_json(&mut adapter, run(1, RunEventKind::RunStarted));
        let first = event_json(
            &mut adapter,
            run(
                2,
                RunEventKind::OutputDelta {
                    channel: OutputChannel::AssistantText,
                    delta: "before".into(),
                },
            ),
        );
        assert_eq!(first[0]["blockId"], "text:0");
        let _ = event_json(
            &mut adapter,
            run(
                3,
                RunEventKind::ToolCallStarted {
                    call_id: "call-1".into(),
                    name: "shell".into(),
                },
            ),
        );
        let second = event_json(
            &mut adapter,
            run(
                4,
                RunEventKind::OutputDelta {
                    channel: OutputChannel::AssistantText,
                    delta: "after".into(),
                },
            ),
        );
        assert_eq!(second[0]["type"], "text-start");
        assert_eq!(second[0]["blockId"], "text:1");
        assert_eq!(second[1]["blockId"], "text:1");
    }

    #[test]
    fn client_reducer_replay_preserves_text_and_tool_order() {
        let mut adapter = adapter();
        let mut frames = event_json(&mut adapter, run(1, RunEventKind::RunStarted));
        let events = [
            run(
                2,
                RunEventKind::OutputDelta {
                    channel: OutputChannel::AssistantText,
                    delta: "text-0".into(),
                },
            ),
            run(
                3,
                RunEventKind::OutputDelta {
                    channel: OutputChannel::AssistantText,
                    delta: "-continued".into(),
                },
            ),
            run(
                4,
                RunEventKind::ToolCallStarted {
                    call_id: "call-0".into(),
                    name: "shell".into(),
                },
            ),
            run(
                5,
                RunEventKind::OutputDelta {
                    channel: OutputChannel::AssistantText,
                    delta: "text-1".into(),
                },
            ),
            run(
                6,
                RunEventKind::ToolCallStarted {
                    call_id: "call-1".into(),
                    name: "shell".into(),
                },
            ),
            run(
                7,
                RunEventKind::OutputDelta {
                    channel: OutputChannel::AssistantText,
                    delta: "text-2".into(),
                },
            ),
            run(
                8,
                RunEventKind::ToolCallStarted {
                    call_id: "call-2".into(),
                    name: "shell".into(),
                },
            ),
        ];
        for event in events {
            frames.extend(event_json(&mut adapter, event));
        }

        let starts: Vec<Value> = frames
            .iter()
            .filter(|event| {
                matches!(
                    event["type"].as_str(),
                    Some("text-start") | Some("tool-input-start")
                )
            })
            .cloned()
            .collect();
        let actual_parts = client_reducer_parts(&frames);
        let expected_parts = vec![
            "text:text:0",
            "tool:tool:call-0",
            "text:text:1",
            "tool:tool:call-1",
            "text:text:2",
            "tool:tool:call-2",
        ];
        println!(
            "replayed start frames: {}",
            serde_json::to_string(&starts).expect("start frames are serializable")
        );
        println!("replayed parts: {actual_parts:?}");
        assert_eq!(actual_parts, expected_parts);
    }

    #[test]
    fn maps_tools_permission_usage_and_errors() {
        let mut lifecycle = adapter();
        let _ = event_json(&mut lifecycle, run(1, RunEventKind::RunStarted));
        let events = event_json(
            &mut lifecycle,
            run(
                2,
                RunEventKind::ToolCallStarted {
                    call_id: "call-1".into(),
                    name: "shell".into(),
                },
            ),
        );
        assert_eq!(events[0]["type"], "tool-input-start");
        assert_eq!(events[0]["blockId"], "tool:call-1");
        let events = event_json(
            &mut lifecycle,
            run(
                3,
                RunEventKind::ToolExecutionFailed {
                    call_id: "call-1".into(),
                    code: "timeout".into(),
                    message: "timed out".into(),
                    category: ToolErrorCategory::Timeout,
                    retryable: true,
                    retry_after_ms: Some(50),
                },
            ),
        );
        assert_eq!(events[0]["type"], "tool-error");
        assert_eq!(events[0]["error"], "timed out");
        assert_eq!(events[1]["data"]["error"]["retryAfterMs"], 50);
        let usage = TokenUsage {
            input_tokens: 1,
            output_tokens: 2,
            total_tokens: 3,
            source: Default::default(),
        };
        let _ = event_json(&mut lifecycle, run(4, RunEventKind::UsageUpdated { usage }));
        let events = event_json(
            &mut lifecycle,
            run(
                5,
                RunEventKind::ApprovalRequested {
                    approval_id: ApprovalId::from_str("00000000-0000-0000-0000-000000000002")
                        .expect("fixture approval id is valid"),
                    call_id: "call-1".into(),
                    tool_name: "shell".into(),
                    risk_level: ToolRiskLevel::High,
                    arguments: json!({"cmd":"ls"}),
                },
            ),
        );
        assert_eq!(events[0]["type"], "permission-request");
        assert_eq!(
            events[0]["request"],
            json!({
                "id": "00000000-0000-0000-0000-000000000002",
                "toolCallId": "call-1", "toolName": "shell", "risk": "high",
                "detail": "{\n  \"cmd\": \"ls\"\n}", "detailLanguage": "json",
                "options": [
                    {"value": "allow-once", "decision": "allow-once"},
                    {"value": "deny", "decision": "deny", "promptForReason": true}
                ]
            })
        );
        assert!(events[0].get("requestId").is_none());
        assert_eq!(events[1]["type"], "message-end");
        assert_eq!(events[1]["finishReason"], "awaiting-permission");
        assert_eq!(events[1]["usage"]["totalTokens"], 3);
    }

    #[test]
    fn maps_tool_argument_execution_output_and_permission_resolution() {
        let mut lifecycle = adapter();
        let _ = event_json(&mut lifecycle, run(1, RunEventKind::RunStarted));
        let _ = event_json(
            &mut lifecycle,
            run(
                2,
                RunEventKind::ToolCallStarted {
                    call_id: "call-1".into(),
                    name: "shell".into(),
                },
            ),
        );
        let events = event_json(
            &mut lifecycle,
            run(
                3,
                RunEventKind::ToolCallArgumentsDelta {
                    call_id: "call-1".into(),
                    delta: "{\"cmd\":\"ls\"}".into(),
                },
            ),
        );
        assert_eq!(events[0]["type"], "tool-input-delta");
        assert_eq!(events[0]["toolCallId"], "call-1");

        let events = event_json(
            &mut lifecycle,
            run(
                4,
                RunEventKind::ToolExecutionStarted {
                    call_id: "call-1".into(),
                    arguments: json!({"cmd": "ls"}),
                },
            ),
        );
        assert_eq!(events[0]["type"], "tool-input-available");
        assert_eq!(events[1]["type"], "tool-executing");
        assert_eq!(events[0]["input"]["cmd"], "ls");

        let events = event_json(
            &mut lifecycle,
            run(
                5,
                RunEventKind::ToolExecutionCompleted {
                    call_id: "call-1".into(),
                    output: "{\"ok\":true}".into(),
                },
            ),
        );
        assert_eq!(events[0]["type"], "tool-output");
        assert_eq!(events[0]["output"]["ok"], true);

        let mut resolved = adapter();
        let events = event_json(
            &mut resolved,
            run(
                1,
                RunEventKind::ApprovalResolved {
                    approval_id: ApprovalId::from_str("00000000-0000-0000-0000-000000000002")
                        .expect("fixture approval id is valid"),
                    call_id: "call-1".into(),
                    resolution: ApprovalResolution::allow_once(),
                },
            ),
        );
        assert_eq!(events[0]["type"], "permission-resolved");
        assert_eq!(
            events[0]["resolution"],
            json!({
                "requestId": "00000000-0000-0000-0000-000000000002",
                "option": "allow-once", "decision": "allow-once"
            })
        );
        assert_eq!(events[0]["requestId"], events[0]["resolution"]["requestId"]);
    }

    #[test]
    fn maps_all_terminal_reasons_and_keeps_cancelled_distinct() {
        for (seq, reason, expected) in [
            (2, FinishReason::Stop, "stop"),
            (3, FinishReason::Length, "length"),
            (4, FinishReason::ToolCall, "tool-calls"),
            (5, FinishReason::ContentFilter, "error"),
            (6, FinishReason::Unknown, "error"),
        ] {
            let mut adapter = adapter();
            let _ = event_json(&mut adapter, run(1, RunEventKind::RunStarted));
            let events = event_json(
                &mut adapter,
                run(
                    seq,
                    RunEventKind::RunCompleted {
                        finish_reason: reason,
                    },
                ),
            );
            let message_end = events
                .iter()
                .find(|event| event["type"] == "message-end")
                .expect("terminal message-end");
            assert_eq!(message_end["finishReason"], expected);
        }
        let mut cancelled = adapter();
        let _ = event_json(&mut cancelled, run(1, RunEventKind::RunStarted));
        let cancelled = event_json(&mut cancelled, run(2, RunEventKind::RunCancelled));
        assert_eq!(cancelled[0]["finishReason"], "cancelled");
        assert_ne!(cancelled[0]["finishReason"], "error");
    }

    #[test]
    fn frame_rules_are_monotonic_heartbeat_free_and_idempotent() {
        let mut adapter = adapter();
        let first = adapter.adapt(&run(1, RunEventKind::RunStarted));
        assert_eq!(first[0].event["eventId"], 1);
        let duplicate = adapter.adapt(&run(1, RunEventKind::RunStarted));
        assert!(duplicate.is_empty());
        let next = adapter.adapt(&run(
            2,
            RunEventKind::OutputDelta {
                channel: OutputChannel::AssistantText,
                delta: "x".into(),
            },
        ));
        assert_eq!(next[0].event["eventId"], 2);
        assert_eq!(next[1].event["eventId"], 3);
        assert_eq!(AgentChatAdapter::heartbeat(), ": heartbeat\n\n");
    }

    #[test]
    fn cancelled_and_failed_are_different_wire_events() {
        let mut cancelled = adapter();
        let _ = cancelled.adapt(&run(1, RunEventKind::RunStarted));
        let cancelled = cancelled.adapt(&run(2, RunEventKind::RunCancelled));
        let mut failed = adapter();
        let _ = failed.adapt(&run(1, RunEventKind::RunStarted));
        let failed = failed.adapt(&run(
            2,
            RunEventKind::RunFailed {
                code: "internal".into(),
                message: "boom".into(),
                retryable: false,
            },
        ));
        assert_eq!(cancelled[0].event["finishReason"], "cancelled");
        assert_eq!(failed[0].event["finishReason"], "error");
        assert_eq!(failed[1].event["type"], "error");
    }

    #[test]
    fn unknown_diagnostic_path_is_non_panicking() {
        let adapter = adapter();
        let event = run(1, RunEventKind::RunStarted);
        let diagnostic = adapter.diagnostic_event(&event, "unknown_run_event");
        assert_eq!(diagnostic["type"], "custom");
        assert_eq!(diagnostic["name"], "mina-diagnostic");
        assert_eq!(diagnostic["data"]["code"], "unknown_run_event");
        let _ = AgentEvent::text_delta("future variant remains wildcard-safe");
    }
}
