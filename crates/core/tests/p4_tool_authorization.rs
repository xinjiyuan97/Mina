use agent_core::{contracts::*, tool_authorization::*};
use serde_json::json;

fn snapshot(revision: u64, kind: ToolBindingKind) -> ToolSetSnapshot {
    ToolSetSnapshot::new(
        revision,
        vec![ToolDefinition::new(
            "write",
            "Write a value",
            json!({"type":"object","required":["value"],"properties":{"value":{"type":"integer"}},"additionalProperties":false}),
        ).with_risk_level(ToolRiskLevel::High)],
        vec![ToolBinding {
            name: "write".into(),
            kind,
            argument_visibility: ToolArgumentVisibility::DigestOnly,
        }],
    ).expect("snapshot")
}

fn request() -> ToolValidationRequest {
    ToolValidationRequest {
        call_id: "call-1".into(),
        name: "write".into(),
        arguments: json!({"value":1}),
    }
}

fn authorize(snapshot: &ToolSetSnapshot, request: &ToolValidationRequest) -> ValidatedToolCall {
    validate_tool_call(snapshot, request, Some(&["write".into()]), false).expect("valid call")
}

#[test]
fn validation_checks_schema_allowlist_binding_policy_and_snapshot_digest() {
    let tools = snapshot(7, ToolBindingKind::Static);
    let call = authorize(&tools, &request());
    assert_eq!(call.tool_set_revision, 7);
    assert_eq!(call.tool_set_digest, tools.digest);
    assert_eq!(call.invocation.risk_level, ToolRiskLevel::High);
    assert_eq!(
        call.invocation.public_arguments["digest"],
        call.arguments_digest
    );
    assert_ne!(call.invocation.public_arguments, request().arguments);
    let mut invalid = request();
    invalid.arguments = json!({"value":"wrong"});
    assert_eq!(
        validate_tool_call(&tools, &invalid, None, false).expect_err("authorization must reject"),
        AuthorizationError::InvalidArguments
    );
    assert_eq!(
        validate_tool_call(&tools, &request(), Some(&[]), false)
            .expect_err("authorization must reject"),
        AuthorizationError::NotAllowed
    );
    invalid = request();
    invalid.name = "missing".into();
    assert_eq!(
        validate_tool_call(&tools, &invalid, None, false).expect_err("authorization must reject"),
        AuthorizationError::UnknownTool
    );
    let mut tampered = tools.clone();
    tampered.definitions[0].risk_level = ToolRiskLevel::Low;
    assert_eq!(
        validate_tool_call(&tampered, &request(), None, false)
            .expect_err("authorization must reject"),
        AuthorizationError::InvalidSnapshot
    );
    let mut definitions = tools.definitions.clone();
    definitions[0].execution.retry.max_attempts = 0;
    let invalid_policy =
        ToolSetSnapshot::new(7, definitions, tools.bindings.clone()).expect("snapshot");
    assert_eq!(
        validate_tool_call(&invalid_policy, &request(), None, false)
            .expect_err("authorization must reject"),
        AuthorizationError::InvalidPolicy
    );
}

#[test]
fn adf_requires_grant_and_revision_is_bound_even_when_digest_is_unchanged() {
    let tools = snapshot(7, ToolBindingKind::AgentDefined);
    assert_eq!(
        validate_tool_call(&tools, &request(), None, false).expect_err("authorization must reject"),
        AuthorizationError::NotAllowed
    );
    let first = validate_tool_call(&tools, &request(), None, true).expect("ADF grant");
    let second = validate_tool_call(
        &snapshot(8, ToolBindingKind::AgentDefined),
        &request(),
        None,
        true,
    )
    .expect("new revision");
    assert_eq!(first.tool_set_digest, second.tool_set_digest);
    assert_ne!(first.binding_digest, second.binding_digest);
    assert!(validate_tool_call(&tools, &request(), Some(&[]), true).is_ok());
    let management = snapshot(7, ToolBindingKind::AdfManagement);
    assert_eq!(
        validate_tool_call(&management, &request(), Some(&[]), true)
            .expect_err("authorization must reject"),
        AuthorizationError::NotAllowed
    );
    assert!(validate_tool_call(&management, &request(), Some(&["write".into()]), true).is_ok());
}

#[test]
fn approval_roundtrip_duplicates_conflict_expiry_and_parameter_revision_fences() {
    let run_id = RunId::new();
    let call = authorize(&snapshot(7, ToolBindingKind::Static), &request());
    let pending =
        BoundApproval::new(run_id, "approval-effect", call.clone(), 100, 200).expect("pending");
    let mut restored: BoundApproval =
        serde_json::from_str(&serde_json::to_string(&pending).expect("encode")).expect("restore");
    let resolution = ApprovalResolution::allow_once();
    assert!(
        restored
            .resolve("gateway-user", &call, resolution.clone(), 150)
            .expect("resolve")
    );
    assert!(
        !restored
            .resolve("gateway-user", &call, resolution, 250)
            .expect("same decision remains idempotent after expiry")
    );
    assert_eq!(
        restored
            .resolve("gateway-user", &call, ApprovalResolution::deny(None), 160)
            .expect_err("authorization must reject"),
        AuthorizationError::Conflict
    );
    let changed = authorize(&snapshot(8, ToolBindingKind::Static), &request());
    assert_eq!(
        restored
            .authorize(&changed, 160)
            .expect_err("authorization must reject"),
        AuthorizationError::BindingChanged
    );
    let mut changed_request = request();
    changed_request.arguments = json!({"value":2});
    let changed = authorize(&snapshot(7, ToolBindingKind::Static), &changed_request);
    assert_eq!(
        restored
            .authorize(&changed, 160)
            .expect_err("authorization must reject"),
        AuthorizationError::BindingChanged
    );
    restored.authorize(&call, 160).expect("matching grant");
    assert_eq!(
        restored
            .authorize(&call, 200)
            .expect_err("authorization must reject"),
        AuthorizationError::Expired
    );
    let mut expired = pending.clone();
    assert_eq!(
        expired
            .resolve("gateway-user", &call, ApprovalResolution::allow_once(), 200)
            .expect_err("authorization must reject"),
        AuthorizationError::Expired
    );
    let mut cancelled = pending;
    assert!(cancelled.cancel());
    assert!(!cancelled.cancel());
    assert_eq!(
        cancelled
            .authorize(&call, 150)
            .expect_err("authorization must reject"),
        AuthorizationError::Cancelled
    );
    assert_eq!(
        cancelled
            .resolve("gateway-user", &call, ApprovalResolution::allow_once(), 150)
            .expect_err("authorization must reject"),
        AuthorizationError::Cancelled
    );
}

#[test]
fn tampered_validated_call_cannot_reuse_an_approval() {
    let call = authorize(&snapshot(7, ToolBindingKind::Static), &request());
    let mut approval = BoundApproval::new(RunId::new(), "approval-effect", call.clone(), 100, 200)
        .expect("pending");
    approval
        .resolve("actor", &call, ApprovalResolution::allow_once(), 150)
        .expect("allow");
    let mut changed = call.clone();
    changed.invocation.arguments = json!({"value":2});
    assert_eq!(
        approval.authorize(&changed, 151),
        Err(AuthorizationError::BindingChanged)
    );
    changed = call.clone();
    changed.invocation.risk_level = ToolRiskLevel::Low;
    assert_eq!(
        approval.authorize(&changed, 151),
        Err(AuthorizationError::BindingChanged)
    );
    changed = call;
    changed.invocation.execution.idempotency = ToolIdempotency::ReadOnly;
    assert_eq!(
        approval.authorize(&changed, 151),
        Err(AuthorizationError::BindingChanged)
    );
}

#[test]
fn approval_denial_and_invalid_actor_never_authorize() {
    let call = authorize(&snapshot(7, ToolBindingKind::Static), &request());
    let mut approval = BoundApproval::new(RunId::new(), "approval-effect", call.clone(), 100, 200)
        .expect("pending");
    assert_eq!(
        approval
            .authorize(&call, 150)
            .expect_err("authorization must reject"),
        AuthorizationError::Pending
    );
    assert_eq!(
        approval
            .resolve(" ", &call, ApprovalResolution::allow_once(), 150)
            .expect_err("authorization must reject"),
        AuthorizationError::InvalidIdentity
    );
    approval
        .resolve("gateway-user", &call, ApprovalResolution::deny(None), 150)
        .expect("deny");
    assert_eq!(
        approval
            .authorize(&call, 150)
            .expect_err("authorization must reject"),
        AuthorizationError::Denied
    );
}

#[test]
fn repeated_decision_keeps_original_actor_reason_and_timestamp() {
    let call = authorize(&snapshot(7, ToolBindingKind::Static), &request());
    let mut approval = BoundApproval::new(RunId::new(), "approval-effect", call.clone(), 100, 200)
        .expect("pending");
    approval
        .resolve(
            "first-actor",
            &call,
            ApprovalResolution::deny(Some("original".into())),
            150,
        )
        .expect("deny");
    let original_status = approval.status.clone();
    assert_eq!(
        approval.resolve(
            "second-actor",
            &call,
            ApprovalResolution::deny(Some("duplicate".into())),
            250
        ),
        Ok(false)
    );
    assert_eq!(approval.status, original_status);
}
