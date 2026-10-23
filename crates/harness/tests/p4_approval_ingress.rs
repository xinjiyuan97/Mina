use agent_core::{harness::*, operator_fabric::*, tool_authorization::*};
use agent_extension::store::SqliteEventStore;
use agent_harness::approval_operator::ApprovalIngress;
use serde_json::json;
use std::sync::Arc;

fn approval() -> BoundApproval {
    let tools = ToolSetSnapshot::new(
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
        &tools,
        &ToolValidationRequest {
            call_id: "call".into(),
            name: "write".into(),
            arguments: json!({"secret":"private"}),
        },
        None,
        false,
    )
    .expect("call");
    BoundApproval::new(RunId::new(), "approval-effect", call, 10, 100).expect("pending")
}

#[tokio::test]
async fn gateway_resolution_reopens_is_idempotent_conflicts_and_never_executes_tools() {
    let directory = tempfile::tempdir().expect("temp");
    let path = directory.path().join("fabric.db");
    let pending = approval();
    {
        let fabric = Arc::new(SqliteEventStore::open(&path, "p4").await.expect("open"));
        ApprovalIngress::new(fabric)
            .request("local", pending.clone())
            .await
            .expect("request");
    }
    let fabric = Arc::new(SqliteEventStore::open(&path, "p4").await.expect("reopen"));
    let ingress = ApprovalIngress::new(fabric.clone());
    assert!(
        ingress
            .resolve(
                "local",
                pending.run_id,
                pending.approval_id,
                "gateway-user",
                ApprovalResolution::allow_once(),
                20
            )
            .await
            .expect("resolve")
    );
    assert!(
        !ingress
            .resolve(
                "local",
                pending.run_id,
                pending.approval_id,
                "gateway-user",
                ApprovalResolution::allow_once(),
                120
            )
            .await
            .expect("duplicate")
    );
    assert!(
        ingress
            .resolve(
                "local",
                pending.run_id,
                pending.approval_id,
                "gateway-user",
                ApprovalResolution::deny(None),
                21
            )
            .await
            .is_err()
    );
    assert!(
        ingress
            .resolve(
                "other-tenant",
                pending.run_id,
                pending.approval_id,
                "gateway-user",
                ApprovalResolution::allow_once(),
                21
            )
            .await
            .is_err()
    );
    assert!(
        ingress
            .resolve(
                "local",
                RunId::new(),
                pending.approval_id,
                "gateway-user",
                ApprovalResolution::allow_once(),
                21
            )
            .await
            .is_err()
    );
    let saved = ingress
        .load("local", pending.run_id, pending.approval_id)
        .await
        .expect("load");
    saved
        .authorize(&pending.call, 21)
        .expect("allow once grant");
    fabric
        .execute(FabricCommand::DispatchOutbox { limit: 10 })
        .await
        .expect("dispatch");
    let FabricReply::Replay(events) = fabric
        .execute(FabricCommand::ScanEvents {
            topic: "agent.approval.v1".into(),
            event_type: "approval.resolved".into(),
            after: 0,
            limit: 10,
        })
        .await
        .expect("scan")
    else {
        panic!("scan")
    };
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event.envelope.payload["actor"], "gateway-user");
    assert!(
        !serde_json::to_string(&events[0].event.envelope.payload)
            .expect("public payload")
            .contains("private")
    );
}

#[tokio::test]
async fn expiry_and_cancel_are_durable_facts_and_prevent_resolution() {
    for cancelled in [false, true] {
        let directory = tempfile::tempdir().expect("temp");
        let path = directory.path().join("fabric.db");
        let pending = approval();
        {
            let fabric = Arc::new(SqliteEventStore::open(&path, "p4").await.expect("open"));
            let ingress = ApprovalIngress::new(fabric);
            ingress
                .request("local", pending.clone())
                .await
                .expect("request");
            if cancelled {
                ingress
                    .cancel(
                        "local",
                        pending.run_id,
                        pending.approval_id,
                        "coordinator",
                        20,
                    )
                    .await
                    .expect("cancel");
            } else {
                ingress
                    .expire("local", pending.run_id, pending.approval_id, 100)
                    .await
                    .expect("expire");
            }
        }
        let fabric = Arc::new(SqliteEventStore::open(&path, "p4").await.expect("reopen"));
        let ingress = ApprovalIngress::new(fabric.clone());
        assert!(
            ingress
                .resolve(
                    "local",
                    pending.run_id,
                    pending.approval_id,
                    "gateway-user",
                    ApprovalResolution::allow_once(),
                    101
                )
                .await
                .is_err()
        );
        fabric
            .execute(FabricCommand::DispatchOutbox { limit: 10 })
            .await
            .expect("dispatch");
        let FabricReply::Replay(events) = fabric
            .execute(FabricCommand::ScanEvents {
                topic: "agent.approval.v1".into(),
                event_type: if cancelled {
                    "approval.cancelled"
                } else {
                    "approval.expired"
                }
                .into(),
                after: 0,
                limit: 10,
            })
            .await
            .expect("scan")
        else {
            panic!("scan")
        };
        assert_eq!(events.len(), 1);
    }
}

struct ConcurrentReadFabric {
    inner: Arc<dyn OperatorFabric>,
    reads: std::sync::atomic::AtomicUsize,
    barrier: tokio::sync::Barrier,
}

impl OperatorFabric for ConcurrentReadFabric {
    fn capabilities(&self) -> FabricCapabilities {
        self.inner.capabilities()
    }
    fn execute(&self, command: FabricCommand) -> FabricFuture<'_> {
        Box::pin(async move {
            let synchronize = matches!(&command, FabricCommand::ReadCheckpoint(_))
                && self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst) < 2;
            let reply = self.inner.execute(command).await?;
            if synchronize {
                self.barrier.wait().await;
            }
            Ok(reply)
        })
    }
}

#[tokio::test]
async fn concurrent_duplicate_request_is_idempotent_and_decisions_have_one_winner() {
    let directory = tempfile::tempdir().expect("temp");
    let inner = Arc::new(
        SqliteEventStore::open(directory.path().join("fabric.db"), "p4")
            .await
            .expect("store"),
    );
    let ingress = ApprovalIngress::new(Arc::new(ConcurrentReadFabric {
        inner: inner.clone(),
        reads: Default::default(),
        barrier: tokio::sync::Barrier::new(2),
    }));
    let pending = approval();
    let (first, second) = tokio::join!(
        ingress.request("local", pending.clone()),
        ingress.request("local", pending.clone())
    );
    first.expect("first duplicate");
    second.expect("second duplicate");
    let (allow, deny) = tokio::join!(
        ingress.resolve(
            "local",
            pending.run_id,
            pending.approval_id,
            "actor-one",
            ApprovalResolution::allow_once(),
            20
        ),
        ingress.resolve(
            "local",
            pending.run_id,
            pending.approval_id,
            "actor-two",
            ApprovalResolution::deny(None),
            20
        ),
    );
    assert!((allow == Ok(true) && deny.is_err()) || (deny == Ok(true) && allow.is_err()));
}

#[tokio::test]
async fn late_gateway_resolution_persists_expiry_without_a_separate_sweeper() {
    let directory = tempfile::tempdir().expect("temp");
    let fabric = Arc::new(
        SqliteEventStore::open(directory.path().join("fabric.db"), "p4")
            .await
            .expect("store"),
    );
    let pending = approval();
    let ingress = ApprovalIngress::new(fabric.clone());
    ingress
        .request("local", pending.clone())
        .await
        .expect("request");
    assert!(
        ingress
            .resolve(
                "local",
                pending.run_id,
                pending.approval_id,
                "actor",
                ApprovalResolution::allow_once(),
                100
            )
            .await
            .is_err()
    );
    assert_eq!(
        ingress
            .load("local", pending.run_id, pending.approval_id)
            .await
            .expect("load")
            .status,
        ApprovalStatus::Expired
    );
}
