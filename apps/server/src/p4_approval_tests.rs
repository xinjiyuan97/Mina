use super::*;
use agent_core::agent::AgentResources;
use agent_core::harness::*;
use serde_json::json;

struct ApprovalModel;
impl ModelPort for ApprovalModel {
    fn stream(&self, _: ModelRequest) -> ModelEventStream {
        Box::pin(futures_util::stream::iter([
            ModelEvent::ToolCallStarted {
                call_id: "call".into(),
                name: "write".into(),
            },
            ModelEvent::ToolCallArgumentsDelta {
                call_id: "call".into(),
                delta: "{}".into(),
            },
            ModelEvent::Completed {
                finish_reason: FinishReason::ToolCall,
            },
        ]))
    }
}
struct NeverExecute;
impl ToolPort for NeverExecute {
    fn definitions(&self) -> Vec<ToolDefinition> {
        vec![
            ToolDefinition::new("write", "write", json!({"type":"object"}))
                .with_risk_level(ToolRiskLevel::High),
        ]
    }
    fn call(&self, _: ToolCallRequest) -> ToolCallFuture {
        panic!("stopped workers cannot execute tools")
    }
}
#[tokio::test]
async fn graph_approval_endpoint_persists_one_decision_without_broker_double_delivery() {
    let mut state = tests::test_state();
    state.tools_pipeline = Some(Arc::new(crate::tools_pipeline::ServerAgentRuntime::new(
        AgentResources::new(state.admission.fabric())
            .model("primary", "test", Arc::new(ApprovalModel))
            .toolset("workspace", "test", Arc::new(NeverExecute)),
        state.runtime.run_store(),
    )));
    let engine = &state.tools_pipeline.as_ref().unwrap().engine;
    let started = engine
        .run(
            &agent_core::agent::GraphAgentConfig::new("test", ""),
            "approve",
        )
        .await
        .unwrap();
    let run_id = started.run_id;
    let approval_id = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let events = engine.events_after(run_id, 0, 128).await.unwrap();
            if let Some(id) = events.into_iter().find_map(|e| match e.kind {
                RunEventKind::ApprovalRequested { approval_id, .. } => Some(approval_id),
                _ => None,
            }) {
                break id;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    engine.stop_workers().await;
    let mut broker_waiter = state.approvals.request(ApprovalRequest {
        approval_id,
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
            Path((run_id.to_string(), approval_id.to_string())),
            Json(ResolveApprovalRequest {
                decision,
                reason: None,
            }),
        )
        .await;
        assert_eq!(response.status(), status);
    }
    assert!(futures_util::poll!(broker_waiter.as_mut()).is_pending());
    let host = engine.host(run_id).await.unwrap();
    let facts = host
        .engine
        .events_after("local", run_id, 0, 128)
        .await
        .unwrap();
    assert_eq!(
        facts
            .iter()
            .filter(|e| e.kind == "approval.decided.v1")
            .count(),
        1
    );
    assert!(!facts.iter().any(|e| e.kind == "tool.authorized.v1"));
}
