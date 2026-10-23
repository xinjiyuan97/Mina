use crate::harness::{
    ApprovalDecision, ApprovalId, ApprovalResolution, RunId, ToolArgumentVisibility,
    ToolBindingKind, ToolInvocation, ToolSetSnapshot, ToolValidationRequest, digest_only_arguments,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub fn approval_payload_schema(kind: &str) -> serde_json::Value {
    use serde_json::json;
    if !matches!(
        kind,
        "approval.requested" | "approval.resolved" | "approval.expired" | "approval.cancelled"
    ) {
        return json!(false);
    }
    let resolution = if kind == "approval.resolved" {
        json!({"type":"object","required":["decision"],"additionalProperties":false,
            "properties":{"decision":{"enum":["allow-once","deny"]},"reason":{"type":"string"}}})
    } else {
        json!({"type":"null"})
    };
    json!({"type":"object","additionalProperties":false,
    "required":["approval_id","call_id","effect_id","arguments_digest","binding_digest","tool_set_revision","tool_set_digest","actor","resolution","expires_at_ms"],
    "properties":{
        "approval_id":{"type":"string","format":"uuid"},"call_id":{"type":"string","minLength":1},"effect_id":{"type":"string","minLength":1},
        "arguments_digest":{"type":"string","pattern":"^sha256:[0-9a-f]{64}$"},"binding_digest":{"type":"string","pattern":"^sha256:[0-9a-f]{64}$"},
        "tool_set_revision":{"type":"integer","minimum":0},"tool_set_digest":{"type":"string","pattern":"^sha256:[0-9a-f]{64}$"},
        "actor":{"type":"string","minLength":1},"resolution":resolution,"expires_at_ms":{"type":"integer"}
    }})
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(rename_all = "snake_case")]
pub enum AuthorizationError {
    #[error("invalid call, effect or actor identity")]
    InvalidIdentity,
    #[error("tool snapshot metadata does not match its digest")]
    InvalidSnapshot,
    #[error("tool is absent from the locked snapshot")]
    UnknownTool,
    #[error("tool or ADF capability is not allowed")]
    NotAllowed,
    #[error("tool execution policy is invalid")]
    InvalidPolicy,
    #[error("tool arguments do not satisfy the locked schema")]
    InvalidArguments,
    #[error("approval binding changed; validate and approve again")]
    BindingChanged,
    #[error("approval resolution conflicts with the recorded decision")]
    Conflict,
    #[error("approval is pending")]
    Pending,
    #[error("approval was denied")]
    Denied,
    #[error("approval expired")]
    Expired,
    #[error("approval was cancelled")]
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ValidatedToolCall {
    pub tool_set_revision: u64,
    pub tool_set_digest: String,
    pub arguments_digest: String,
    pub binding_digest: String,
    pub invocation: ToolInvocation,
}

fn digest(value: &impl Serialize) -> String {
    format!(
        "sha256:{:x}",
        Sha256::digest(serde_json::to_vec(value).expect("contract data is serializable"))
    )
}

impl ValidatedToolCall {
    fn computed_binding(&self) -> String {
        digest(&(
            self.tool_set_revision,
            &self.tool_set_digest,
            &self.arguments_digest,
            &self.invocation,
        ))
    }

    fn verify(&self) -> Result<(), AuthorizationError> {
        if self.arguments_digest != digest(&self.invocation.arguments)
            || self.binding_digest != self.computed_binding()
        {
            return Err(AuthorizationError::BindingChanged);
        }
        Ok(())
    }
}

pub fn validate_tool_call(
    snapshot: &ToolSetSnapshot,
    request: &ToolValidationRequest,
    allowed_tools: Option<&[String]>,
    allow_run_adf: bool,
) -> Result<ValidatedToolCall, AuthorizationError> {
    if request.call_id.trim().is_empty() || request.name.trim().is_empty() {
        return Err(AuthorizationError::InvalidIdentity);
    }
    let canonical = ToolSetSnapshot::new(
        snapshot.revision,
        snapshot.definitions.clone(),
        snapshot.bindings.clone(),
    )
    .map_err(|_| AuthorizationError::InvalidSnapshot)?;
    if canonical != *snapshot {
        return Err(AuthorizationError::InvalidSnapshot);
    }
    let binding = snapshot
        .binding(&request.name)
        .ok_or(AuthorizationError::UnknownTool)?;
    let statically_allowed = allowed_tools.is_none_or(|allowed| allowed.contains(&request.name));
    let allowed = match binding.kind {
        ToolBindingKind::Static => statically_allowed,
        ToolBindingKind::AdfManagement => allow_run_adf && statically_allowed,
        ToolBindingKind::AgentDefined => allow_run_adf,
    };
    if !allowed {
        return Err(AuthorizationError::NotAllowed);
    }
    let definition = snapshot
        .definitions
        .iter()
        .find(|definition| definition.name == request.name)
        .ok_or(AuthorizationError::UnknownTool)?;
    definition
        .execution
        .validate()
        .map_err(|_| AuthorizationError::InvalidPolicy)?;
    let schema = jsonschema::JSONSchema::compile(&definition.input_schema)
        .map_err(|_| AuthorizationError::InvalidSnapshot)?;
    if !schema.is_valid(&request.arguments) {
        return Err(AuthorizationError::InvalidArguments);
    }
    let arguments_digest = digest(&request.arguments);
    let public_arguments = match binding.argument_visibility {
        ToolArgumentVisibility::Full => request.arguments.clone(),
        ToolArgumentVisibility::DigestOnly => digest_only_arguments(&request.arguments),
    };
    let mut validated = ValidatedToolCall {
        tool_set_revision: snapshot.revision,
        tool_set_digest: snapshot.digest.clone(),
        arguments_digest,
        binding_digest: String::new(),
        invocation: ToolInvocation {
            call_id: request.call_id.clone(),
            name: request.name.clone(),
            arguments: request.arguments.clone(),
            public_arguments,
            risk_level: definition.risk_level,
            execution: definition.execution,
        },
    };
    validated.binding_digest = validated.computed_binding();
    Ok(validated)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ApprovalStatus {
    Pending,
    Expired,
    Resolved {
        actor: String,
        resolution: ApprovalResolution,
        resolved_at_ms: i64,
    },
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BoundApproval {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reducer_step: Option<u32>,
    pub approval_id: ApprovalId,
    pub run_id: RunId,
    pub effect_id: String,
    pub call: ValidatedToolCall,
    pub requested_at_ms: i64,
    pub expires_at_ms: i64,
    pub status: ApprovalStatus,
}

impl BoundApproval {
    pub fn new(
        run_id: RunId,
        effect_id: &str,
        call: ValidatedToolCall,
        requested_at_ms: i64,
        expires_at_ms: i64,
    ) -> Result<Self, AuthorizationError> {
        if effect_id.trim().is_empty() || expires_at_ms <= requested_at_ms {
            return Err(AuthorizationError::InvalidIdentity);
        }
        call.verify()?;
        Ok(Self {
            reducer_step: None,
            approval_id: ApprovalId::stable(
                "operator-tool-approval-v1",
                &digest(&(run_id, effect_id, &call.binding_digest)),
            ),
            run_id,
            effect_id: effect_id.into(),
            call,
            requested_at_ms,
            expires_at_ms,
            status: ApprovalStatus::Pending,
        })
    }

    pub fn for_reducer(
        run_id: RunId,
        effect_id: &str,
        call: ValidatedToolCall,
        requested_at_ms: i64,
        expires_at_ms: i64,
        step: u32,
    ) -> Result<Self, AuthorizationError> {
        let mut approval = Self::new(run_id, effect_id, call, requested_at_ms, expires_at_ms)?;
        approval.reducer_step = Some(step);
        approval.approval_id = ApprovalId::stable(
            "agent-loop",
            &format!("{run_id}:{step}:{}", approval.call.invocation.call_id),
        );
        Ok(approval)
    }

    fn check_binding(&self, call: &ValidatedToolCall) -> Result<(), AuthorizationError> {
        call.verify()?;
        self.call.verify()?;
        if self.call != *call {
            return Err(AuthorizationError::BindingChanged);
        }
        Ok(())
    }

    pub fn resolve(
        &mut self,
        actor: &str,
        call: &ValidatedToolCall,
        resolution: ApprovalResolution,
        now_ms: i64,
    ) -> Result<bool, AuthorizationError> {
        if actor.trim().is_empty() || now_ms < self.requested_at_ms {
            return Err(AuthorizationError::InvalidIdentity);
        }
        self.check_binding(call)?;
        match &self.status {
            ApprovalStatus::Expired => return Err(AuthorizationError::Expired),
            ApprovalStatus::Cancelled => return Err(AuthorizationError::Cancelled),
            ApprovalStatus::Resolved {
                resolution: previous,
                ..
            } => {
                return if previous.decision == resolution.decision {
                    Ok(false)
                } else {
                    Err(AuthorizationError::Conflict)
                };
            }
            ApprovalStatus::Pending => {}
        }
        if now_ms >= self.expires_at_ms {
            return Err(AuthorizationError::Expired);
        }
        self.status = ApprovalStatus::Resolved {
            actor: actor.into(),
            resolution,
            resolved_at_ms: now_ms,
        };
        Ok(true)
    }

    pub fn authorize(
        &self,
        call: &ValidatedToolCall,
        now_ms: i64,
    ) -> Result<(), AuthorizationError> {
        self.check_binding(call)?;
        match &self.status {
            ApprovalStatus::Expired => Err(AuthorizationError::Expired),
            ApprovalStatus::Cancelled => Err(AuthorizationError::Cancelled),
            _ if now_ms >= self.expires_at_ms => Err(AuthorizationError::Expired),
            ApprovalStatus::Pending => Err(AuthorizationError::Pending),
            ApprovalStatus::Resolved {
                resolution,
                resolved_at_ms,
                ..
            } => {
                if now_ms < *resolved_at_ms {
                    return Err(AuthorizationError::InvalidIdentity);
                }
                match resolution.decision {
                    ApprovalDecision::AllowOnce => Ok(()),
                    ApprovalDecision::Deny => Err(AuthorizationError::Denied),
                }
            }
        }
    }

    pub fn cancel(&mut self) -> bool {
        if matches!(self.status, ApprovalStatus::Cancelled) {
            return false;
        }
        self.status = ApprovalStatus::Cancelled;
        true
    }

    pub fn expire(&mut self, now_ms: i64) -> bool {
        if now_ms < self.expires_at_ms || !matches!(self.status, ApprovalStatus::Pending) {
            return false;
        }
        self.status = ApprovalStatus::Expired;
        true
    }
}
