#![cfg(feature = "adf")]

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use agent_core::{
    adf::RunScopedJavaScriptPolicy,
    harness::{
        AgentLoop, FinishReason, ModelEvent, ModelEventStream, ModelPort, ModelRequest,
        RunEventKind, RunId, ToolRegistry,
    },
    script::{ScriptLimits, ScriptRuntime},
};
use agent_extension::adf::{InMemoryAdfArtifactStore, RunAdfToolSession};
use agent_harness::{Harness, RunOptions, adf::JavaScriptAdfExecutor, script::QuickJsRuntime};
use futures_util::{StreamExt, stream};
use serde_json::{Value, json};

const SOURCE_MARKER: &str = "ADF_PRIVATE_SOURCE_MARKER";

struct DefineThenInvokeProvider {
    calls: AtomicUsize,
}

struct NoAdfGrantProvider;

impl ModelPort for NoAdfGrantProvider {
    fn stream(&self, request: ModelRequest) -> ModelEventStream {
        assert!(
            request.tools.iter().all(|tool| !matches!(
                tool.name.as_str(),
                "adf_define" | "adf_list" | "adf_remove"
            ))
        );
        Box::pin(stream::iter([ModelEvent::Completed {
            finish_reason: FinishReason::Stop,
        }]))
    }
}

impl ModelPort for DefineThenInvokeProvider {
    fn stream(&self, request: ModelRequest) -> ModelEventStream {
        match self.calls.fetch_add(1, Ordering::SeqCst) {
            0 => {
                assert!(request.tools.iter().any(|tool| tool.name == "adf_define"));
                assert!(
                    request
                        .tools
                        .iter()
                        .all(|tool| !tool.name.starts_with("adf_sum_values_"))
                );
                let arguments = json!({
                    "requested_name": "sum_values",
                    "description": "Sum an array of numbers.",
                    "input_schema": {
                        "type": "object",
                        "properties": {
                            "values": {"type": "array", "items": {"type": "number"}}
                        },
                        "required": ["values"],
                        "additionalProperties": false
                    },
                    "output_schema": {"type": "number"},
                    "runtime": {"type": "javascript"},
                    "source": format!(
                        "// {SOURCE_MARKER}\nexport function main(input) {{ return input.values.reduce((sum, value) => sum + value, 0); }}"
                    ),
                    "entrypoint": "main",
                    "requested_capabilities": [],
                    "requested_scope": "run",
                    "execution_mode": "sync",
                    "idempotency_key": "sum-values-agent-loop-v1"
                })
                .to_string();
                Box::pin(stream::iter([
                    ModelEvent::ToolCallStarted {
                        call_id: "call_define".into(),
                        name: "adf_define".into(),
                    },
                    ModelEvent::ToolCallArgumentsDelta {
                        call_id: "call_define".into(),
                        delta: arguments,
                    },
                    ModelEvent::Completed {
                        finish_reason: FinishReason::ToolCall,
                    },
                ]))
            }
            1 => {
                let definition_result: Value = serde_json::from_str(
                    &request
                        .messages
                        .last()
                        .expect("define tool result should be in context")
                        .content,
                )
                .expect("define result should be JSON");
                let canonical_name = definition_result["canonical_name"]
                    .as_str()
                    .expect("canonical name should exist")
                    .to_owned();
                assert!(request.tools.iter().any(|tool| tool.name == canonical_name));
                Box::pin(stream::iter([
                    ModelEvent::ToolCallStarted {
                        call_id: "call_dynamic".into(),
                        name: canonical_name,
                    },
                    ModelEvent::ToolCallArgumentsDelta {
                        call_id: "call_dynamic".into(),
                        delta: json!({"values": [1, 2, 3]}).to_string(),
                    },
                    ModelEvent::Completed {
                        finish_reason: FinishReason::ToolCall,
                    },
                ]))
            }
            2 => {
                assert_eq!(
                    request
                        .messages
                        .last()
                        .expect("dynamic tool result should be in context")
                        .content,
                    "6"
                );
                Box::pin(stream::iter([
                    ModelEvent::TextDelta {
                        delta: "The generated function returned 6.".into(),
                    },
                    ModelEvent::Completed {
                        finish_reason: FinishReason::Stop,
                    },
                ]))
            }
            _ => panic!("provider received too many model calls"),
        }
    }
}

#[tokio::test]
async fn agent_defines_a_tool_then_sees_and_invokes_it_on_the_next_step() {
    let runtime: Arc<dyn ScriptRuntime> = Arc::new(QuickJsRuntime::default());
    let tools = RunAdfToolSession::new(ToolRegistry::new())
        .with_adf(
            Arc::new(InMemoryAdfArtifactStore::new()),
            Arc::new(RunScopedJavaScriptPolicy::new(runtime.descriptor())),
            Arc::new(JavaScriptAdfExecutor::new(runtime, ScriptLimits::default())),
            8,
        )
        .expect("ADF session should build");
    let harness = Harness::new(AgentLoop::new(
        DefineThenInvokeProvider {
            calls: AtomicUsize::new(0),
        },
        tools,
        "test-model",
        "Define and use an ADF.",
        None,
        4,
    ));
    let execution = harness
        .start_with_options(
            RunId::new(),
            "Sum the values.",
            Vec::new(),
            RunOptions {
                allowed_tools: Some(vec![
                    "adf_define".into(),
                    "adf_list".into(),
                    "adf_remove".into(),
                ]),
                allow_run_adf: true,
                max_steps: None,
            },
        )
        .expect("run should start");
    let events = execution.events.collect::<Vec<_>>().await;

    assert!(matches!(
        events.last().map(|event| &event.kind),
        Some(RunEventKind::RunCompleted {
            finish_reason: FinishReason::Stop
        })
    ));
    assert!(events.iter().any(|event| matches!(
        &event.kind,
        RunEventKind::ToolSetUpdated { dynamic_tools, .. }
            if dynamic_tools.iter().any(|name| name.starts_with("adf_sum_values_"))
    )));
    assert!(events.iter().any(|event| matches!(
        &event.kind,
        RunEventKind::ToolExecutionCompleted { call_id, output }
            if call_id == "call_dynamic" && output == "6"
    )));

    let public_events = serde_json::to_string(&events).expect("events should serialize");
    assert!(!public_events.contains(SOURCE_MARKER));
    assert!(public_events.contains("\"redacted\":true"));
}

#[tokio::test]
async fn run_adf_grant_is_required_to_expose_management_tools() {
    let runtime: Arc<dyn ScriptRuntime> = Arc::new(QuickJsRuntime::default());
    let tools = RunAdfToolSession::new(ToolRegistry::new())
        .with_adf(
            Arc::new(InMemoryAdfArtifactStore::new()),
            Arc::new(RunScopedJavaScriptPolicy::new(runtime.descriptor())),
            Arc::new(JavaScriptAdfExecutor::new(runtime, ScriptLimits::default())),
            8,
        )
        .expect("ADF session should build");
    let harness = Harness::new(AgentLoop::new(
        NoAdfGrantProvider,
        tools,
        "test-model",
        "Do not expose ADF.",
        None,
        2,
    ));
    let events = harness
        .start_with_options(
            RunId::new(),
            "Complete without tools.",
            Vec::new(),
            RunOptions {
                allowed_tools: None,
                allow_run_adf: false,
                max_steps: None,
            },
        )
        .expect("run should start")
        .events
        .collect::<Vec<_>>()
        .await;

    assert!(matches!(
        events.last().map(|event| &event.kind),
        Some(RunEventKind::RunCompleted { .. })
    ));
}
