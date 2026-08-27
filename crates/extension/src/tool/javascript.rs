use std::sync::Arc;

use agent_core::{
    harness::{
        Tool, ToolArgumentVisibility, ToolCallFuture, ToolCallRequest, ToolConcurrency,
        ToolDefinition, ToolError, ToolErrorCategory, ToolExecutionPolicy, ToolOutput,
        ToolRetryPolicy, ToolRiskLevel,
    },
    script::{
        ScriptError, ScriptErrorKind, ScriptExecutionRequest, ScriptLanguage, ScriptLimits,
        ScriptPurpose, ScriptRuntime, ScriptSource, ScriptValidationRequest,
    },
};
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Clone)]
pub struct JavaScriptEvalTool {
    runtime: Arc<dyn ScriptRuntime>,
    limits: ScriptLimits,
}

impl std::fmt::Debug for JavaScriptEvalTool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("JavaScriptEvalTool")
            .field("runtime", &self.runtime.descriptor())
            .field("limits", &self.limits)
            .finish()
    }
}

impl JavaScriptEvalTool {
    #[must_use]
    pub fn new(runtime: Arc<dyn ScriptRuntime>, limits: ScriptLimits) -> Self {
        Self { runtime, limits }
    }
}

impl Tool for JavaScriptEvalTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition::new(
            "javascript_eval",
            "Run a bounded, capability-free JavaScript ES module for small pure computations over JSON. Export `main(input)`, for example `export function main(input) { return input; }`. `input` is optional and defaults to null. This is not Node.js: require, process, filesystem, network, environment variables, and package imports are unavailable; use the dedicated read, search, or terminal tools for those operations.",
            json!({
                "type": "object",
                "properties": {
                    "source": {
                        "type": "string",
                        "minLength": 1,
                        "description": "A self-contained JavaScript ES module exporting the selected function."
                    },
                    "input": {
                        "description": "Optional JSON value passed to the exported function; defaults to null.",
                        "default": null
                    },
                    "export": {
                        "type": "string",
                        "minLength": 1,
                        "description": "Exported function name.",
                        "default": "main"
                    }
                },
                "required": ["source"],
                "additionalProperties": false
            }),
        )
        .with_risk_level(ToolRiskLevel::Low)
        .with_execution_policy(
            ToolExecutionPolicy::read_only()
                .with_concurrency(ToolConcurrency::ParallelSafe)
                .with_retry(ToolRetryPolicy::bounded(2, 25, 250)),
        )
    }

    fn argument_visibility(&self) -> ToolArgumentVisibility {
        ToolArgumentVisibility::DigestOnly
    }

    fn call(&self, request: ToolCallRequest) -> ToolCallFuture {
        let runtime = Arc::clone(&self.runtime);
        let limits = self.limits;
        Box::pin(async move {
            let arguments: JavaScriptEvalRequest = serde_json::from_value(request.arguments)
                .map_err(|_| {
                    ToolError::new(
                        "javascript_eval_invalid_arguments",
                        "javascript_eval arguments are invalid",
                        false,
                    )
                    .with_category(ToolErrorCategory::InvalidRequest)
                })?;
            let validation = runtime
                .validate(ScriptValidationRequest {
                    language: ScriptLanguage::JavaScript,
                    purpose: ScriptPurpose::Eval,
                    source: ScriptSource::Inline {
                        source: arguments.source.clone(),
                        expected_digest: None,
                    },
                    export: arguments.export.clone(),
                    requested_capabilities: Vec::new(),
                    limits,
                })
                .await
                .map_err(map_script_error)?;
            let output = runtime
                .execute(ScriptExecutionRequest {
                    execution_id: request.call_id,
                    run_id: Some(request.run_id),
                    language: ScriptLanguage::JavaScript,
                    purpose: ScriptPurpose::Eval,
                    source: ScriptSource::Inline {
                        source: arguments.source,
                        expected_digest: Some(validation.source_digest),
                    },
                    export: arguments.export,
                    input: arguments.input,
                    granted_capabilities: Vec::new(),
                    limits,
                    cancellation: request.cancellation,
                })
                .await
                .map_err(map_script_error)?;
            let content = serde_json::to_string(&json!({
                "ok": true,
                "value": output.value,
                "module_digest": output.module_digest,
                "usage": output.usage,
            }))
            .map_err(|_| {
                ToolError::new(
                    "javascript_eval_output_failed",
                    "javascript_eval result could not be encoded",
                    false,
                )
                .with_category(ToolErrorCategory::Internal)
            })?;
            Ok(ToolOutput::text(content))
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct JavaScriptEvalRequest {
    source: String,
    #[serde(default = "default_input")]
    input: Value,
    #[serde(default = "default_export")]
    export: String,
}

fn default_input() -> Value {
    Value::Null
}

fn default_export() -> String {
    "main".into()
}

fn map_script_error(error: ScriptError) -> ToolError {
    let category = match error.kind() {
        ScriptErrorKind::InvalidRequest
        | ScriptErrorKind::InvalidSource
        | ScriptErrorKind::InvalidResult => ToolErrorCategory::InvalidRequest,
        ScriptErrorKind::NotFound => ToolErrorCategory::NotFound,
        ScriptErrorKind::CapabilityDenied => ToolErrorCategory::PermissionDenied,
        ScriptErrorKind::ResourceExhausted => ToolErrorCategory::ResourceExhausted,
        ScriptErrorKind::Timeout => ToolErrorCategory::Timeout,
        ScriptErrorKind::Cancelled => ToolErrorCategory::Cancelled,
        ScriptErrorKind::Unavailable => ToolErrorCategory::Unavailable,
        ScriptErrorKind::Runtime | ScriptErrorKind::Internal => ToolErrorCategory::Internal,
    };
    ToolError::new(error.code(), error.safe_message(), error.retryable()).with_category(category)
}

#[cfg(all(test, feature = "builtin-tools"))]
mod tests {
    use agent_core::harness::{RunCancellation, RunId};
    use agent_harness::script::QuickJsRuntime;
    use serde_json::json;

    use super::*;

    #[tokio::test]
    async fn executes_json_without_host_capabilities() {
        let tool =
            JavaScriptEvalTool::new(Arc::new(QuickJsRuntime::default()), ScriptLimits::default());
        let output = tool
            .call(ToolCallRequest {
                run_id: RunId::new(),
                call_id: "call_js".into(),
                name: "javascript_eval".into(),
                arguments: json!({
                    "source": "export function main(input) { return { total: input.values.reduce((sum, value) => sum + value, 0) }; }",
                    "input": {"values": [1, 2, 3]}
                }),
                cancellation: RunCancellation::new(),
            })
            .await
            .expect("JavaScript should execute");
        let value: Value = serde_json::from_str(&output.content).expect("output should be JSON");
        assert_eq!(value["value"], json!({"total": 6}));
        assert_eq!(
            tool.argument_visibility(),
            ToolArgumentVisibility::DigestOnly
        );
    }

    #[tokio::test]
    async fn defaults_missing_input_to_null() {
        let tool =
            JavaScriptEvalTool::new(Arc::new(QuickJsRuntime::default()), ScriptLimits::default());
        let definition = tool.definition();
        assert_eq!(definition.input_schema["required"], json!(["source"]));

        let output = tool
            .call(ToolCallRequest {
                run_id: RunId::new(),
                call_id: "call_js_without_input".into(),
                name: "javascript_eval".into(),
                arguments: json!({
                    "source": "export function main(input) { return { receivedNull: input === null }; }"
                }),
                cancellation: RunCancellation::new(),
            })
            .await
            .expect("JavaScript without explicit input should execute");
        let value: Value = serde_json::from_str(&output.content).expect("output should be JSON");
        assert_eq!(value["value"], json!({"receivedNull": true}));
    }
}
