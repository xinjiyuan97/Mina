use super::*;
use agent_core::{harness::*, tool_authorization::*};
use agent_harness::approval_operator::ApprovalIngress;
use serde_json::json;

#[tokio::test]
async fn operator_approval_endpoint_only_publishes_gateway_fact_without_legacy_double_delivery() {
    let state = tests::test_state();
    let run_id = RunId::new();
    state
        .runtime
        .run_store()
        .create_run(
            RunSnapshot::new(run_id, "input", 1).with_execution_manifest(
                json!({"engine_version":"operator_v1","profile":"operator_v1_tools"}),
            ),
        )
        .await
        .expect("run");
    let snapshot = ToolSetSnapshot::new(
        1,
        vec![
            ToolDefinition::new("write", "write", json!({"type":"object"}))
                .with_risk_level(ToolRiskLevel::High),
        ],
        vec![ToolBinding {
            name: "write".into(),
            kind: ToolBindingKind::Static,
            argument_visibility: ToolArgumentVisibility::DigestOnly,
        }],
    )
    .expect("snapshot");
    let call = validate_tool_call(
        &snapshot,
        &ToolValidationRequest {
            call_id: "call".into(),
            name: "write".into(),
            arguments: json!({"secret":"private"}),
        },
        None,
        false,
    )
    .expect("validate");
    let now_ms = agent_harness::unix_time_ms();
    let pending = BoundApproval::new(run_id, "approval-effect", call, now_ms, now_ms + 60_000)
        .expect("pending");
    let ingress = ApprovalIngress::new(state.admission.fabric());
    ingress
        .request("local", pending.clone())
        .await
        .expect("durable request");
    let mut legacy_waiter = state.approvals.request(ApprovalRequest {
        approval_id: pending.approval_id,
        run_id,
        call_id: "call".into(),
        tool_name: "write".into(),
        risk_level: ToolRiskLevel::High,
        arguments: json!({}),
    });
    for (decision, status) in [
        (ApprovalDecision::AllowOnce, StatusCode::OK),
        (ApprovalDecision::AllowOnce, StatusCode::OK),
        (ApprovalDecision::Deny, StatusCode::CONFLICT),
    ] {
        let response = resolve_approval(
            State(state.clone()),
            Path((run_id.to_string(), pending.approval_id.to_string())),
            Json(ResolveApprovalRequest {
                decision,
                reason: None,
            }),
        )
        .await;
        assert_eq!(response.status(), status);
    }
    assert!(futures_util::poll!(legacy_waiter.as_mut()).is_pending());
    assert!(matches!(
        ingress
            .load("local", run_id, pending.approval_id)
            .await
            .expect("persisted decision")
            .status,
        ApprovalStatus::Resolved {
            resolution: ApprovalResolution {
                decision: ApprovalDecision::AllowOnce,
                ..
            },
            ..
        }
    ));
    assert!(
        state
            .runtime
            .get_run(run_id)
            .await
            .expect("run")
            .expect("run")
            .tools
            .is_empty()
    );
}
