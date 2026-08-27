use agent_core::harness::{
    Tool, ToolCallFuture, ToolCallRequest, ToolConcurrency, ToolDefinition, ToolError,
    ToolExecutionPolicy, ToolOutput, ToolRetryPolicy,
};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

#[derive(Debug, Default)]
pub struct GetCurrentTimeTool;

impl Tool for GetCurrentTimeTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition::new(
            "get_current_time",
            "Return the current UTC time and Unix timestamp. Use this whenever the user asks for the current date or time.",
            serde_json::json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }),
        )
        .with_execution_policy(
            ToolExecutionPolicy::read_only()
                .with_concurrency(ToolConcurrency::ParallelSafe)
                .with_retry(ToolRetryPolicy::bounded(2, 25, 100)),
        )
    }

    fn call(&self, request: ToolCallRequest) -> ToolCallFuture {
        Box::pin(async move {
            if request.cancellation.is_cancelled() {
                return Err(cancelled());
            }

            let now = OffsetDateTime::now_utc();
            let formatted = now.format(&Rfc3339).map_err(|_| {
                ToolError::new(
                    "tool_internal",
                    "current time could not be formatted",
                    false,
                )
            })?;
            Ok(ToolOutput::text(
                serde_json::json!({
                    "timezone": "UTC",
                    "iso8601": formatted,
                    "unix_seconds": now.unix_timestamp()
                })
                .to_string(),
            ))
        })
    }
}

fn cancelled() -> ToolError {
    ToolError::new("tool_cancelled", "tool execution was cancelled", false)
}
