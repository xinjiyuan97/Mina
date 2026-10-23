use crate::{
    event_runtime::EventId,
    harness::{RunId, ToolSetSnapshot, ToolValidationRequest},
    tool_authorization::ValidatedToolCall,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub const PREPARATION_TOPIC: &str = "agent.tool-preparation.v1";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PreparationRequest {
    pub tenant_id: String,
    pub run_id: RunId,
    pub effect_id: String,
    pub kind: PreparationKind,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PreparationKind {
    ToolSet,
    Validate {
        toolset_effect_id: String,
        revision: u64,
        calls: Vec<ToolValidationRequest>,
        allowed_tools: Option<Vec<String>>,
        allow_run_adf: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum PreparationResult {
    ToolSet { snapshot: ToolSetSnapshot },
    Validated { calls: Vec<ValidatedToolCall> },
    Failed { code: String },
}

pub fn request_key(tenant: &str, run_id: RunId, effect_id: &str) -> String {
    format!(
        "tool-preparation:{}",
        EventId::stable(
            "tool-preparation",
            &serde_json::to_string(&(tenant, run_id, effect_id)).expect("identity")
        )
    )
}

impl PreparationRequest {
    pub fn checkpoint_key(&self) -> String {
        request_key(&self.tenant_id, self.run_id, &self.effect_id)
    }

    pub fn result_key(&self) -> String {
        format!("{}:result", self.checkpoint_key())
    }

    pub fn event_type(&self) -> &'static str {
        match self.kind {
            PreparationKind::ToolSet => "toolset.requested",
            PreparationKind::Validate { .. } => "tool.validation.requested",
        }
    }
}

pub fn payload_schema(kind: &str) -> Value {
    match kind {
        "toolset.requested" | "tool.validation.requested" => json!({
            "type":"object", "required":["request_key"], "additionalProperties":false,
            "properties":{"request_key":{"type":"string","minLength":1}}
        }),
        "toolset.completed"
        | "toolset.failed"
        | "tool.validation.completed"
        | "tool.validation.failed" => json!({
            "type":"object", "required":["result_key"], "additionalProperties":false,
            "properties":{"result_key":{"type":"string","minLength":1}}
        }),
        _ => json!(false),
    }
}
