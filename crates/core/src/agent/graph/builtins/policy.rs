//! Durable tool authorization. Decisions are facts delivered through host ingress;
//! this operator never waits on a UI or invokes an external tool.
use super::*;
use crate::tool_authorization::{BoundApproval, ValidatedToolCall, validate_tool_call};

pub(super) fn register(registry: &mut OperatorRegistry) {
    let mut desc = descriptor(
        "agent.tool_policy",
        &[
            "gate.dispatch.v1",
            "approval.decided.v1",
            "approval.expire.v1",
        ],
        &[
            "tool.authorized.v1",
            "approval.requested.v1",
            "approval.finished.v1",
            "gate.result.v1",
        ],
        json!({"type":"object","additionalProperties":false,"required":["toolset","approval_timeout_ms"],"properties":{
            "toolset":{"type":"string","minLength":1},
            "approval_timeout_ms":{"type":"integer","minimum":1,"maximum":86400000},
            "review_level":{"type":"integer","minimum":0,"maximum":100},
            "allowed_tools":{"type":"array","items":{"type":"string"},"uniqueItems":true},
            "allow_run_adf":{"type":"boolean"}}}),
        true,
    );
    desc.resources
        .insert("toolset".into(), ResourceKind::Toolset);
    registry
        .register(desc, |config, resources| {
            let config: PolicyConfig =
                serde_json::from_value(config.clone()).map_err(|e| invalid("config", e))?;
            Ok(Arc::new(Policy {
                port: resources.get_toolset(&config.toolset)?,
                config,
            }))
        })
        .expect("tool policy registration");
}
fn review_default() -> u8 {
    ToolApprovalPolicy::DEFAULT_REVIEW_LEVEL
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicyConfig {
    toolset: String,
    approval_timeout_ms: u64,
    #[serde(default = "review_default")]
    review_level: u8,
    allowed_tools: Option<Vec<String>>,
    #[serde(default)]
    allow_run_adf: bool,
}
struct Policy {
    config: PolicyConfig,
    port: Arc<dyn ToolPort>,
}
#[derive(Default, Serialize, Deserialize)]
struct PolicyState {
    calls: BTreeMap<String, Pending>,
}
#[derive(Serialize, Deserialize)]
struct Pending {
    dispatch: Value,
    call: ValidatedToolCall,
    approval: Option<BoundApproval>,
    finished: bool,
}
pub(super) fn failure(dispatch: &Value, code: &str, message: &str) -> GraphEvent {
    let error = json!({"code":code,"message":message,"category":ToolError::new(code, message, false).category(),"retryable":false,"retry_after_ms":null});
    GraphEvent::new(
        "gate.result.v1",
        json!({"batch_id":dispatch["batch_id"],"id":dispatch["id"],"payload":{"ok":false,"output":json!({"ok":false,"error":error}).to_string(),"error":error}}),
    )
}
pub(super) fn validate(
    dispatch: &Value,
    allowed: Option<&[String]>,
    allow_adf: bool,
) -> Result<ValidatedToolCall, FabricError> {
    let call: ModelToolCall =
        serde_json::from_value(dispatch["payload"].clone()).map_err(|e| invalid("call", e))?;
    if dispatch["id"].as_str() != Some(&call.id) {
        return Err(invalid("id", "tool call ID mismatch"));
    }
    let snapshot: ToolSetSnapshot =
        serde_json::from_value(dispatch["context"]["tool_snapshot"].clone())
            .map_err(|e| invalid("tool_snapshot", e))?;
    validate_tool_call(
        &snapshot,
        &ToolValidationRequest {
            call_id: call.id,
            name: call.name,
            arguments: serde_json::from_str(if call.arguments.trim().is_empty() {
                "{}"
            } else {
                &call.arguments
            })
            .map_err(|e| invalid("arguments", e))?,
        },
        allowed,
        allow_adf,
    )
    .map_err(|e| invalid("authorization", e))
}
fn authorized(pending: &Pending) -> GraphEvent {
    let mut payload = pending.dispatch.clone();
    payload["authorization"] = json!({"call":pending.call,"approval":pending.approval});
    GraphEvent::new("tool.authorized.v1", payload)
}
impl GraphOperator for Policy {
    fn handle(&self, ctx: GraphContext) -> GraphFuture<'_> {
        Box::pin(async move {
            let mut saved: PolicyState = if ctx.state.is_null() {
                PolicyState::default()
            } else {
                serde_json::from_value(ctx.state).map_err(|e| invalid("state", e))?
            };
            let mut events = vec![];
            if ctx.event.kind == "gate.dispatch.v1" {
                let dispatch = &ctx.event.payload;
                // Encode the pair; a separator alone would permit ambiguous keys.
                let key = json!([dispatch["batch_id"], dispatch["id"]]).to_string();
                if let Some(prior) = saved.calls.get(&key) {
                    if prior.dispatch != *dispatch {
                        return Err(invalid("call", "conflicting dispatch"));
                    }
                    return Ok(GraphOutput::new(
                        serde_json::to_value(saved).map_err(|e| invalid("state", e))?,
                        events,
                    ));
                }
                let call = match validate(
                    dispatch,
                    self.config.allowed_tools.as_deref(),
                    self.config.allow_run_adf,
                ) {
                    Ok(call) => call,
                    Err(_) => {
                        return Ok(GraphOutput::new(
                            serde_json::to_value(saved).map_err(|e| invalid("state", e))?,
                            vec![failure(
                                dispatch,
                                "invalid_tool_call",
                                "Tool call does not match the advertised capability",
                            )],
                        ));
                    }
                };
                if self
                    .port
                    .validate_at(
                        ctx.run_id,
                        call.tool_set_revision,
                        &call.invocation.name,
                        &call.invocation.arguments,
                    )
                    .is_err()
                {
                    return Ok(GraphOutput::new(
                        serde_json::to_value(saved).map_err(|e| invalid("state", e))?,
                        vec![failure(
                            dispatch,
                            "invalid_tool_arguments",
                            "Tool validation failed",
                        )],
                    ));
                }
                let review = ToolApprovalPolicy::new(self.config.review_level)
                    .requires_review(call.invocation.risk_level);
                let mut pending = Pending {
                    dispatch: dispatch.clone(),
                    call,
                    approval: None,
                    finished: !review,
                };
                if review {
                    let now = crate::agent::unix_time_ms();
                    let approval = BoundApproval::new(
                        ctx.run_id,
                        &format!("{}:{key}", ctx.instance),
                        pending.call.clone(),
                        now,
                        now.saturating_add(self.config.approval_timeout_ms as i64),
                    )
                    .map_err(|e| invalid("approval", e))?;
                    events.push(GraphEvent::new("approval.requested.v1", json!({
                        "approval_id":approval.approval_id,"run_id":ctx.run_id,
                        "call_id":pending.call.invocation.call_id,"tool_name":pending.call.invocation.name,
                        "risk_level":pending.call.invocation.risk_level,"arguments":pending.call.invocation.public_arguments,
                        "binding_digest":pending.call.binding_digest,"expires_at_ms":approval.expires_at_ms
                    })));
                    pending.approval = Some(approval);
                } else {
                    events.push(authorized(&pending));
                }
                saved.calls.insert(key, pending);
            } else {
                if ctx.event.from != "$external" {
                    return Err(invalid("approval", "host ingress required"));
                }
                let id: ApprovalId =
                    serde_json::from_value(ctx.event.payload["approval_id"].clone())
                        .map_err(|e| invalid("approval_id", e))?;
                if let Some(pending) = saved
                    .calls
                    .values_mut()
                    .find(|p| p.approval.as_ref().is_some_and(|a| a.approval_id == id))
                    && !pending.finished
                {
                    let approval = pending.approval.as_mut().expect("matched approval");
                    let now = crate::agent::unix_time_ms();
                    if ctx.event.kind == "approval.expire.v1" {
                        if !approval.expire(now) {
                            return Ok(GraphOutput::new(
                                serde_json::to_value(saved).map_err(|e| invalid("state", e))?,
                                events,
                            ));
                        }
                    } else {
                        let digest = ctx.event.payload["binding_digest"].as_str();
                        // Invalid/stale decisions cannot consume a pending authorization.
                        if digest != Some(pending.call.binding_digest.as_str()) {
                            return Ok(GraphOutput::new(
                                serde_json::to_value(saved).map_err(|e| invalid("state", e))?,
                                events,
                            ));
                        }
                        let actor = ctx.event.payload["actor"]
                            .as_str()
                            .ok_or_else(|| invalid("actor", "required"))?;
                        let resolution: ApprovalResolution =
                            serde_json::from_value(ctx.event.payload["resolution"].clone())
                                .map_err(|e| invalid("resolution", e))?;
                        if !approval.expire(now) {
                            approval
                                .resolve(actor, &pending.call, resolution, now)
                                .map_err(|e| invalid("approval", e))?;
                        }
                    }
                    let result = approval.authorize(&pending.call, now);
                    events.push(GraphEvent::new("approval.finished.v1", json!({"approval_id":id,"call_id":pending.call.invocation.call_id,"status":approval.status})));
                    pending.finished = true;
                    events.push(match result {
                        Ok(()) => authorized(pending),
                        Err(error) => {
                            let reason = match &pending
                                .approval
                                .as_ref()
                                .expect("matched approval")
                                .status
                            {
                                crate::tool_authorization::ApprovalStatus::Resolved {
                                    resolution,
                                    ..
                                } => resolution
                                    .reason
                                    .clone()
                                    .unwrap_or_else(|| error.to_string()),
                                _ => error.to_string(),
                            };
                            failure(&pending.dispatch, "tool_rejected", &reason)
                        }
                    });
                }
            }
            Ok(GraphOutput::new(
                serde_json::to_value(saved).map_err(|e| invalid("state", e))?,
                events,
            ))
        })
    }
}
