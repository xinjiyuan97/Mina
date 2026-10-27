use agent_core::{
    contracts::events::EventId, contracts::fabric::*, contracts::*, operator::*,
    tool_authorization::*,
};
use agent_extension::store::SqliteEventStore;
use serde_json::json;

fn approval() -> BoundApproval {
    let tools = ToolSetSnapshot::new(
        12,
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
            arguments: json!({"secret":"value"}),
        },
        None,
        false,
    )
    .expect("validate");
    BoundApproval::new(RunId::new(), "approval-effect", call, 10, 100).expect("approval")
}

fn event(approval: &BoundApproval) -> FabricEvent {
    FabricEvent {
        topic: "p4-authorization-test".into(),
        envelope: OperatorEnvelope {
            schema_version: 1,
            engine_version: EngineVersion::OperatorV1,
            projection_version: 1,
            event_id: EventId::stable("p4-authorization-test", &approval.approval_id.to_string()),
            event_type: "tool.validation.requested".into(),
            tenant_id: "local".into(),
            run_id: approval.run_id,
            session_id: None,
            effect_id: approval.effect_id.clone(),
            invocation_id: "invocation".into(),
            attempt: 1,
            issued_revision: RunRevision(1),
            lease_epoch: LeaseEpoch(1),
            correlation_id: approval.run_id.to_string(),
            causation_id: None,
            operator: OperatorIdentity {
                name: "test-coordinator".into(),
                version: 1,
            },
            operator_seq: OperatorSequence(1),
            delivery_class: DeliveryClass::Durable,
            occurred_at_ms: 10,
            payload: json!({"binding_digest": approval.call.binding_digest}),
        },
        payload_schema: json!({"type":"object","required":["binding_digest"]}),
    }
}

async fn claim(store: &dyn OperatorFabric, now_ms: i64) -> ClaimedDelivery {
    let FabricReply::Claimed(mut deliveries) = store
        .execute(FabricCommand::Claim {
            worker: "test-worker".into(),
            now_ms,
            lease_ms: 10,
            limit: 1,
            max_attempts: 5,
        })
        .await
        .expect("claim")
    else {
        panic!("claim response")
    };
    assert_eq!(deliveries.len(), 1);
    deliveries.remove(0)
}

#[tokio::test]
async fn bound_approval_receipt_and_outbox_survive_real_sqlite_reopen() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("fabric.db");
    let pending = approval();
    let requested = event(&pending);
    let key = ReceiptKey::for_event("test-validator", &requested);
    let mut outbox = requested.clone();
    outbox.envelope.event_id = EventId::stable(
        "p4-authorization-test-result",
        &pending.approval_id.to_string(),
    );
    outbox.envelope.event_type = "tool.validation.completed".into();
    outbox.envelope.operator_seq = OperatorSequence(2);
    outbox.envelope.causation_id = Some(requested.envelope.event_id);
    {
        let store = SqliteEventStore::open(&path, "p4-test")
            .await
            .expect("open");
        store
            .execute(FabricCommand::Publish(requested.clone()))
            .await
            .expect("publish");
        store
            .execute(FabricCommand::Subscribe(SubscriptionSpec {
                consumer: "test-validator".into(),
                subscription: "validation".into(),
                topic: requested.topic.clone(),
                event_type: Some(requested.envelope.event_type.clone()),
                tenant: Some("local".into()),
            }))
            .await
            .expect("subscribe");
        let delivery = claim(&store, 10).await;
        store
            .execute(FabricCommand::Commit {
                lease: delivery.lease,
                now_ms: 11,
                result: serde_json::to_value(&pending).expect("encode state"),
                outbox: vec![outbox.clone()],
            })
            .await
            .expect("atomic receipt/outbox/ACK");
    }
    let store = SqliteEventStore::open(&path, "p4-test")
        .await
        .expect("reopen");
    let FabricReply::Receipt(Some(receipt)) = store
        .execute(FabricCommand::Receipt(key))
        .await
        .expect("receipt")
    else {
        panic!("receipt missing")
    };
    let mut restored: BoundApproval = serde_json::from_value(receipt.result).expect("decode state");
    assert_eq!(restored, pending);
    assert_eq!(receipt.outbox, vec![outbox.clone()]);
    assert_eq!(
        store
            .execute(FabricCommand::Offset {
                consumer: "test-validator".into(),
                subscription: "validation".into()
            })
            .await
            .expect("offset"),
        FabricReply::Offset(1)
    );
    assert_eq!(
        store
            .execute(FabricCommand::DispatchOutbox { limit: 10 })
            .await
            .expect("dispatch"),
        FabricReply::Dispatched(1)
    );
    assert_eq!(
        store
            .execute(FabricCommand::ReadEvent(outbox.envelope.event_id))
            .await
            .expect("result"),
        FabricReply::Event(Some(outbox))
    );
    let call = restored.call.clone();
    assert!(
        restored
            .resolve(
                "test-gateway-user",
                &call,
                ApprovalResolution::allow_once(),
                20
            )
            .expect("resolution")
    );
    assert!(
        !restored
            .resolve(
                "test-gateway-user",
                &call,
                ApprovalResolution::allow_once(),
                21
            )
            .expect("duplicate")
    );
    assert_eq!(
        restored.resolve(
            "test-gateway-user",
            &call,
            ApprovalResolution::deny(None),
            22
        ),
        Err(AuthorizationError::Conflict)
    );
    assert_eq!(
        restored.authorize(&call, 100),
        Err(AuthorizationError::Expired)
    );
}

#[tokio::test]
async fn uncommitted_authorization_does_not_ack_or_survive_reopen() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("fabric.db");
    let pending = approval();
    let requested = event(&pending);
    let key = ReceiptKey::for_event("test-validator", &requested);
    let original;
    {
        let store = SqliteEventStore::open(&path, "p4-test")
            .await
            .expect("open");
        store
            .execute(FabricCommand::Publish(requested.clone()))
            .await
            .expect("publish");
        store
            .execute(FabricCommand::Subscribe(SubscriptionSpec {
                consumer: "test-validator".into(),
                subscription: "validation".into(),
                topic: requested.topic,
                event_type: None,
                tenant: Some("local".into()),
            }))
            .await
            .expect("subscribe");
        original = claim(&store, 10).await.lease;
    }
    let store = SqliteEventStore::open(&path, "p4-test")
        .await
        .expect("reopen");
    assert_eq!(
        store
            .execute(FabricCommand::Receipt(key))
            .await
            .expect("receipt"),
        FabricReply::Receipt(None)
    );
    assert_eq!(
        store
            .execute(FabricCommand::Offset {
                consumer: "test-validator".into(),
                subscription: "validation".into()
            })
            .await
            .expect("offset"),
        FabricReply::Offset(0)
    );
    let delivery = claim(&store, 21).await;
    assert!(delivery.lease.epoch > original.epoch);
    assert_eq!(
        store
            .execute(FabricCommand::Commit {
                lease: original,
                now_ms: 21,
                result: json!(pending),
                outbox: vec![],
            })
            .await,
        Err(FabricError::Fenced)
    );
}
