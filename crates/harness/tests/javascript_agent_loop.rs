use std::sync::atomic::{AtomicUsize, Ordering};

use agent_core::{
    harness::{
        AgentLoop, FinishReason, ModelEvent, ModelEventStream, ModelPort, ModelRequest,
        RunEventKind, RunId, ToolRegistry,
    },
    script::ScriptLimits,
};
use agent_extension::tool::JavaScriptEvalTool;
use agent_harness::{Harness, RunOptions, script::QuickJsRuntime};
use futures_util::{StreamExt, stream};
use serde_json::{Value, json};

const SOURCE_MARKER: &str = "JAVASCRIPT_EVAL_PRIVATE_SOURCE_MARKER";

struct EvalWithoutInputProvider {
    calls: AtomicUsize,
}

impl ModelPort for EvalWithoutInputProvider {
    fn stream(&self, request: ModelRequest) -> ModelEventStream {
        match self.calls.fetch_add(1, Ordering::SeqCst) {
            0 => {
                let tool = request
                    .tools
                    .iter()
                    .find(|tool| tool.name == "javascript_eval")
                    .expect("javascript_eval should be exposed to the model");
                assert_eq!(tool.input_schema["required"], json!(["source"]));
                Box::pin(stream::iter([
                    ModelEvent::ToolCallStarted {
                        call_id: "call_eval".into(),
                        name: "javascript_eval".into(),
                    },
                    ModelEvent::ToolCallArgumentsDelta {
                        call_id: "call_eval".into(),
                        delta: json!({
                            "source": format!("// {SOURCE_MARKER}\nexport function main(input) {{ return {{ inputWasNull: input === null, total: [1, 2, 3].reduce((sum, value) => sum + value, 0) }}; }}")
                        })
                        .to_string(),
                    },
                    ModelEvent::Completed {
                        finish_reason: FinishReason::ToolCall,
                    },
                ]))
            }
            1 => {
                let result: Value = serde_json::from_str(
                    &request
                        .messages
                        .last()
                        .expect("tool result should be present")
                        .content,
                )
                .expect("tool result should be JSON");
                assert_eq!(result["value"], json!({"inputWasNull": true, "total": 6}));
                Box::pin(stream::iter([
                    ModelEvent::TextDelta {
                        delta: "The JavaScript calculation returned 6.".into(),
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
async fn model_generated_eval_without_input_completes_the_agent_loop() {
    let mut tools = ToolRegistry::new();
    tools
        .register(JavaScriptEvalTool::new(
            std::sync::Arc::new(QuickJsRuntime::default()),
            ScriptLimits::default(),
        ))
        .expect("javascript_eval should register");
    let harness = Harness::new(AgentLoop::new(
        EvalWithoutInputProvider {
            calls: AtomicUsize::new(0),
        },
        tools,
        "test-model",
        "Use tools when useful.",
        None,
        3,
    ));

    let events = harness
        .start_with_options(
            RunId::new(),
            "Calculate a small value with JavaScript.",
            Vec::new(),
            RunOptions {
                allowed_tools: Some(vec!["javascript_eval".into()]),
                allow_run_adf: false,
                max_steps: None,
            },
        )
        .expect("run should start")
        .events
        .collect::<Vec<_>>()
        .await;

    assert!(events.iter().any(|event| matches!(
        &event.kind,
        RunEventKind::ToolExecutionCompleted { call_id, .. } if call_id == "call_eval"
    )));
    assert!(matches!(
        events.last().map(|event| &event.kind),
        Some(RunEventKind::RunCompleted {
            finish_reason: FinishReason::Stop
        })
    ));
    let public_events = serde_json::to_string(&events).expect("events should serialize");
    assert!(!public_events.contains(SOURCE_MARKER));
    assert!(public_events.contains("\"redacted\":true"));
}
