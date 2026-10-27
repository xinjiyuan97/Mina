use crate::contracts::tool::{digest_only_arguments, digest_only_bytes};
use crate::contracts::{ApprovalResolution, TokenUsage, TokenUsageSource, ToolErrorCategory};
use serde::{Deserialize, Serialize};

mod reducer;
pub use crate::agent::graph::resumed_tool_content;

pub use reducer::{
    AGENT_LOOP_REDUCER_PROTOCOL_VERSION, AgentLoopDispatch, AgentLoopEffect, AgentLoopEffectKind,
    AgentLoopInput, AgentLoopOutcome, AgentLoopReducer, AgentLoopReducerConfig,
    AgentLoopReducerState, AgentLoopStart, AgentLoopStartDispatch, AgentLoopTransition,
    ModelInvocationPurpose, PortableToolError, ToolInvocation, ToolInvocationResult,
    ToolValidation, ToolValidationRequest,
};

/// Portable reducer checkpoint schema retained for browser and stored-state readers.
/// Operator records additionally carry their own versioned execution manifests.
pub const AGENT_LOOP_CHECKPOINT_SCHEMA_VERSION: u32 = 4;

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
