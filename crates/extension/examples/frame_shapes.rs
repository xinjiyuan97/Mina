use std::str::FromStr;

use agent_core::contracts::{
    ApprovalId, ApprovalResolution, FinishReason, OutputChannel, RunEvent, RunEventKind, RunId,
    ToolRiskLevel,
};
use agent_extension::output::{AgentChatAdapter, AgentChatConfig};
use serde_json::json;

fn event(seq: u64, kind: RunEventKind) -> RunEvent {
    RunEvent::new(
        RunId::from_str("00000000-0000-0000-0000-000000000001").expect("fixture id is valid"),
        seq,
        kind,
    )
}

fn print_events(adapter: &mut AgentChatAdapter, events: impl IntoIterator<Item = RunEvent>) {
    for event in events {
        for frame in adapter.adapt(&event) {
            print!("{}", frame.to_sse());
        }
    }
}

fn main() {
    let config = AgentChatConfig::new("conv", "turn", "msg");
    let approval_id =
        ApprovalId::from_str("00000000-0000-0000-0000-000000000002").expect("fixture id is valid");
    let mut adapter = AgentChatAdapter::new(config.clone());
    print_events(
        &mut adapter,
        [
            event(1, RunEventKind::RunStarted),
            event(
                2,
                RunEventKind::OutputDelta {
                    channel: OutputChannel::AssistantText,
                    delta: "hello".into(),
                },
            ),
            event(
                3,
                RunEventKind::ToolCallStarted {
                    call_id: "call-1".into(),
                    name: "shell".into(),
                },
            ),
            event(
                4,
                RunEventKind::ToolCallArgumentsDelta {
                    call_id: "call-1".into(),
                    delta: "{\"cmd\":\"ls\"}".into(),
                },
            ),
            event(
                5,
                RunEventKind::ToolExecutionStarted {
                    call_id: "call-1".into(),
                    arguments: json!({"cmd":"ls"}),
                },
            ),
            event(
                6,
                RunEventKind::ToolExecutionCompleted {
                    call_id: "call-1".into(),
                    output: "{\"ok\":true}".into(),
                },
            ),
            event(
                7,
                RunEventKind::ToolCallStarted {
                    call_id: "call-2".into(),
                    name: "shell".into(),
                },
            ),
            event(
                8,
                RunEventKind::ToolExecutionFailed {
                    call_id: "call-2".into(),
                    code: "timeout".into(),
                    message: "timed out".into(),
                    category: Default::default(),
                    retryable: true,
                    retry_after_ms: None,
                },
            ),
            event(
                9,
                RunEventKind::ApprovalRequested {
                    approval_id,
                    call_id: "call-3".into(),
                    tool_name: "shell".into(),
                    risk_level: ToolRiskLevel::High,
                    arguments: json!({"cmd":"rm -rf /tmp/x"}),
                },
            ),
        ],
    );
    print!("{}", AgentChatAdapter::done());

    let mut resolved = AgentChatAdapter::new(config);
    print_events(
        &mut resolved,
        [
            event(
                10,
                RunEventKind::ApprovalResolved {
                    approval_id,
                    call_id: "call-3".into(),
                    resolution: ApprovalResolution::deny(Some("no".into())),
                },
            ),
            event(
                11,
                RunEventKind::RunCompleted {
                    finish_reason: FinishReason::Stop,
                },
            ),
        ],
    );
    print!("{}", AgentChatAdapter::done());
}
