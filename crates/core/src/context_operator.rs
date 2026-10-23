//! P2 request/Context contracts. Execution remains pinned to legacy_machine.

use crate::{
    context::{ContextArtifactRef, ContextBudget, ContextPack},
    harness::{BlobId, ModelAttachment, RunId, SessionId},
    skill::{SkillId, SkillLock},
};
use serde::{Deserialize, Serialize};

pub const RUN_REQUESTED: &str = "run.requested";
pub const CONTEXT_REQUESTED: &str = "context.requested";
pub const CONTEXT_COMPLETED: &str = "context.completed";
pub const CONTEXT_FAILED: &str = "context.failed";
pub const CONTEXT_CONSUMER: &str = "agent-context-v1";
pub const CONTEXT_TOPIC: &str = "agent.context.v1";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextPolicy {
    pub default_skills: Vec<(SkillId, String)>,
    pub max_skills: usize,
    pub memory_enabled: bool,
    pub memory_scope: String,
    pub max_memories: usize,
    pub agent_profile: String,
    pub agent_instruction: String,
    pub model_profile: String,
    pub available_tools: Vec<String>,
    pub budget: ContextBudget,
    pub policy_version: u32,
    pub allow_run_adf: bool,
    pub script_runtime: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextRequest {
    pub run_id: RunId,
    pub session_id: Option<SessionId>,
    pub through_message_ordinal: Option<u64>,
    pub request_hash: String,
    pub input: String,
    pub attachments: Vec<ModelAttachment>,
    pub explicit_skills: Vec<(SkillId, String)>,
    pub max_steps: u32,
    pub policy: ContextPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestPayload {
    pub request_hash: String,
    pub request_ref: BlobId,
    pub max_steps: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PreparedContext {
    pub pack: ContextPack,
    pub skill_lock: SkillLock,
    pub request_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextCompleted {
    pub request_hash: String,
    pub prepared_ref: BlobId,
    pub fingerprint: String,
    pub skill_lock: SkillLock,
    pub memory_refs: Vec<String>,
    pub artifact_refs: Vec<ContextArtifactRef>,
    pub max_steps: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("{message}")]
pub struct ContextFailed {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextFailurePayload {
    pub request_hash: String,
    pub max_steps: u32,
    pub code: String,
    pub error_ref: BlobId,
}

pub fn payload_schema(kind: &str) -> serde_json::Value {
    use serde_json::json;
    let properties = match kind {
        RUN_REQUESTED | CONTEXT_REQUESTED => json!({
            "request_hash":{"type":"string", "minLength":1},
            "request_ref":{"type":"string", "format":"uuid"},
            "max_steps":{"type":"integer", "minimum":1}
        }),
        CONTEXT_COMPLETED => json!({
            "request_hash":{"type":"string", "minLength":1},
            "prepared_ref":{"type":"string", "format":"uuid"},
            "fingerprint":{"type":"string"},
            "skill_lock":{"type":"object", "required":["skills", "compiler_version", "compiled_instruction_digest"]},
            "memory_refs":{"type":"array", "items":{"type":"string"}},
            "artifact_refs":{"type":"array", "items":{"type":"object"}},
            "max_steps":{"type":"integer", "minimum":1}
        }),
        CONTEXT_FAILED => json!({
            "request_hash":{"type":"string", "minLength":1},
            "max_steps":{"type":"integer", "minimum":1},
            "code":{"enum":["skill_resolution_failed", "context_build_failed"]},
            "error_ref":{"type":"string", "format":"uuid"}
        }),
        _ => return json!(false),
    };
    let required: Vec<_> = properties
        .as_object()
        .expect("object schema")
        .keys()
        .cloned()
        .collect();
    json!({"type":"object", "additionalProperties":false, "required":required, "properties":properties})
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", content = "result", rename_all = "snake_case")]
pub enum ContextOutcome {
    Completed(ContextCompleted),
    Failed(ContextFailurePayload),
}
