use crate::{
    event_runtime::EventId,
    harness::{RunId, ToolApprovalPolicy, ToolInvocationResult},
    operator_fabric::DeliveryLease,
    tool_authorization::{BoundApproval, ValidatedToolCall},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub const TOOL_TOPIC: &str = "agent.tools.v1";
pub const MAX_TOOL_OUTPUT_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolRequest {
    pub tenant_id: String,
    pub run_id: RunId,
    pub effect_id: String,
    pub validated: ValidatedToolCall,
    pub timeout_ms: u64,
    pub approval: Option<BoundApproval>,
    pub approval_policy: ToolApprovalPolicy,
}

impl ToolRequest {
    pub fn checkpoint_key(&self) -> String {
        request_key(&self.tenant_id, self.run_id, &self.effect_id)
    }

    pub fn execution_key(&self) -> String {
        format!(
            "tool-execution:{}",
            EventId::stable(
                "tool-execution",
                &serde_json::to_string(&(&self.tenant_id, self.run_id, &self.effect_id))
                    .expect("execution identity")
            )
        )
    }
}

pub fn request_key(tenant_id: &str, run_id: RunId, effect_id: &str) -> String {
    format!(
        "tool-request:{}",
        EventId::stable(
            "tool-request",
            &serde_json::to_string(&(tenant_id, run_id, effect_id)).expect("request identity")
        )
    )
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ToolExecutionResult {
    Finished { result: ToolInvocationResult },
    Uncertain { code: String },
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolExecutionRecord {
    pub request: ToolRequest,
    pub lease: DeliveryLease,
    pub attempts: u32,
    pub result: Option<ToolExecutionResult>,
}

pub fn payload_schema(kind: &str) -> Value {
    let properties = match kind {
        "tool.requested" => json!({"request_key":{"type":"string","minLength":1}}),
        "tool.started"
        | "tool.completed"
        | "tool.failed"
        | "tool.suspended"
        | "tool.cancel.requested"
        | "tool.cancelled"
        | "operator.execution.uncertain"
        | "operator.result.stale" => json!({
            "result_key":{"type":"string","minLength":1},
            "call_id":{"type":"string","minLength":1},
            "code":{"type":["string","null"]}
        }),
        _ => return json!(false),
    };
    let required: Vec<_> = properties
        .as_object()
        .expect("schema object")
        .keys()
        .cloned()
        .collect();
    json!({"type":"object","properties":properties,"required":required,"additionalProperties":false})
}
