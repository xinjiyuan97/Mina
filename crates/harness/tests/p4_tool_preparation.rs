use agent_core::{
    event_runtime::EventId, harness::*, operator::*, operator_fabric::*, tool_preparation::*,
};
use agent_extension::store::SqliteEventStore;
use agent_harness::tool_preparation::ToolPreparationOperator;
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

struct Tools(AtomicU64);

impl ToolPort for Tools {
    fn definitions(&self) -> Vec<ToolDefinition> {
        vec![ToolDefinition::new(
            "echo",
            "echo",
            json!({"type":"object","required":["value"],"properties":{"value":{"type":"integer"}}}),
        )]
    }

    fn tool_set_snapshot(&self, _: RunId) -> Result<ToolSetSnapshot, ToolError> {
        ToolSetSnapshot::new(
            self.0.load(Ordering::SeqCst),
            self.definitions(),
            vec![ToolBinding {
                name: "echo".into(),
                kind: ToolBindingKind::Static,
                argument_visibility: ToolArgumentVisibility::DigestOnly,
            }],
        )
    }

    fn call(&self, _: ToolCallRequest) -> ToolCallFuture {
        panic!("preparation must never execute a tool")
    }
}

async fn request(store: &dyn OperatorFabric, request: PreparationRequest) -> ClaimedDelivery {
    let event = FabricEvent {
        topic: PREPARATION_TOPIC.into(),
        envelope: OperatorEnvelope {
            schema_version: 1,
            engine_version: EngineVersion::OperatorV1,
            projection_version: 1,
            event_id: EventId::stable("prepare-test", &request.checkpoint_key()),
            event_type: request.event_type().into(),
            tenant_id: request.tenant_id.clone(),
            run_id: request.run_id,
            session_id: None,
            effect_id: request.effect_id.clone(),
            invocation_id: request.effect_id.clone(),
            attempt: 1,
            issued_revision: RunRevision(1),
            lease_epoch: LeaseEpoch(1),
            correlation_id: request.run_id.to_string(),
            causation_id: None,
            operator: OperatorIdentity {
                name: "coordinator".into(),
                version: 1,
            },
            operator_seq: OperatorSequence(0),
            delivery_class: DeliveryClass::Durable,
            occurred_at_ms: 0,
            payload: json!({"request_key":request.checkpoint_key()}),
        },
        payload_schema: payload_schema(request.event_type()),
    };
    store
        .execute(FabricCommand::Subscribe(SubscriptionSpec {
            consumer: "prepare".into(),
            subscription: request.event_type().into(),
            topic: PREPARATION_TOPIC.into(),
            event_type: Some(request.event_type().into()),
            tenant: Some("local".into()),
        }))
        .await
        .expect("subscribe");
    store
        .execute(FabricCommand::Transact {
            checkpoint: CheckpointWrite {
                key: request.checkpoint_key(),
                expected: None,
                value: json!(request),
            },
            outbox: vec![event],
            delivery: None,
        })
        .await
        .expect("request");
    store
        .execute(FabricCommand::DispatchOutbox { limit: 100 })
        .await
        .expect("dispatch");
    let FabricReply::Claimed(mut deliveries) = store
        .execute(FabricCommand::Claim {
            worker: "prepare".into(),
            now_ms: 1,
            lease_ms: 100,
            limit: 1,
            max_attempts: 5,
        })
        .await
        .expect("claim")
    else {
        panic!("claim")
    };
    deliveries.remove(0)
}

#[tokio::test]
async fn locked_toolset_and_validation_survive_reopen_without_execution() {
    let directory = tempfile::tempdir().expect("temp");
    let path = directory.path().join("fabric.db");
    let tools = Arc::new(Tools(AtomicU64::new(7)));
    let run_id = RunId::new();
    let snapshot_request = PreparationRequest {
        tenant_id: "local".into(),
        run_id,
        effect_id: "toolset".into(),
        kind: PreparationKind::ToolSet,
    };
    {
        let store = Arc::new(SqliteEventStore::open(&path, "p4").await.expect("open"));
        let delivery = request(store.as_ref(), snapshot_request.clone()).await;
        let operator = ToolPreparationOperator::new(store.clone(), tools.clone());
        operator
            .execute(delivery.clone(), 2)
            .await
            .expect("snapshot");
        operator.execute(delivery, 3).await.expect("duplicate");
    }
    let store = Arc::new(SqliteEventStore::open(&path, "p4").await.expect("reopen"));
    let validation = PreparationRequest {
        tenant_id: "local".into(),
        run_id,
        effect_id: "validation".into(),
        kind: PreparationKind::Validate {
            toolset_effect_id: "toolset".into(),
            revision: 7,
            calls: vec![ToolValidationRequest {
                call_id: "call".into(),
                name: "echo".into(),
                arguments: json!({"value":42}),
            }],
            allowed_tools: None,
            allow_run_adf: false,
        },
    };
    let delivery = request(store.as_ref(), validation.clone()).await;
    ToolPreparationOperator::new(store.clone(), tools.clone())
        .execute(delivery, 4)
        .await
        .expect("validate");
    let FabricReply::Checkpoint(Some(saved)) = store
        .execute(FabricCommand::ReadCheckpoint(validation.result_key()))
        .await
        .expect("read")
    else {
        panic!("result")
    };
    let result: PreparationResult = serde_json::from_value(saved.value).expect("result");
    let PreparationResult::Validated { calls } = result else {
        panic!("validation failed")
    };
    assert_eq!(calls[0].tool_set_revision, 7);
    assert_eq!(calls[0].invocation.arguments, json!({"value":42}));
    assert_ne!(
        calls[0].invocation.public_arguments,
        calls[0].invocation.arguments
    );
    tools.0.store(8, Ordering::SeqCst);
    let mut revised = validation;
    revised.effect_id = "revision-changed".into();
    let delivery = request(store.as_ref(), revised.clone()).await;
    ToolPreparationOperator::new(store.clone(), tools)
        .execute(delivery, 5)
        .await
        .expect("revision fact");
    let FabricReply::Checkpoint(Some(saved)) = store
        .execute(FabricCommand::ReadCheckpoint(revised.result_key()))
        .await
        .expect("read")
    else {
        panic!("result")
    };
    assert_eq!(
        saved.value,
        json!(PreparationResult::Failed {
            code: "toolset_changed".into()
        })
    );
}

#[tokio::test]
async fn expired_validation_commit_rolls_back_then_reopen_redelivers() {
    let directory = tempfile::tempdir().expect("temp");
    let path = directory.path().join("fabric.db");
    let tools = Arc::new(Tools(AtomicU64::new(7)));
    let command = PreparationRequest {
        tenant_id: "local".into(),
        run_id: RunId::new(),
        effect_id: "toolset".into(),
        kind: PreparationKind::ToolSet,
    };
    let receipt_key;
    {
        let store = Arc::new(SqliteEventStore::open(&path, "p4").await.expect("open"));
        let delivery = request(store.as_ref(), command.clone()).await;
        receipt_key = ReceiptKey::for_event("prepare", &delivery.event);
        assert_eq!(
            ToolPreparationOperator::new(store.clone(), tools.clone())
                .execute(delivery, 101)
                .await,
            Err(FabricError::Fenced)
        );
        assert_eq!(
            store
                .execute(FabricCommand::ReadCheckpoint(command.result_key()))
                .await
                .expect("read"),
            FabricReply::Checkpoint(None)
        );
        assert_eq!(
            store
                .execute(FabricCommand::Receipt(receipt_key.clone()))
                .await
                .expect("receipt"),
            FabricReply::Receipt(None)
        );
        assert_eq!(
            store
                .execute(FabricCommand::Offset {
                    consumer: "prepare".into(),
                    subscription: command.event_type().into()
                })
                .await
                .expect("offset"),
            FabricReply::Offset(0)
        );
    }
    let store = Arc::new(SqliteEventStore::open(&path, "p4").await.expect("reopen"));
    let FabricReply::Claimed(mut deliveries) = store
        .execute(FabricCommand::Claim {
            worker: "restart".into(),
            now_ms: 102,
            lease_ms: 100,
            limit: 1,
            max_attempts: 5,
        })
        .await
        .expect("claim")
    else {
        panic!("claim")
    };
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].lease.epoch, 2);
    ToolPreparationOperator::new(store.clone(), tools)
        .execute(deliveries.remove(0), 103)
        .await
        .expect("recover");
    drop(store);
    let store = SqliteEventStore::open(&path, "p4")
        .await
        .expect("reopen result");
    let FabricReply::Receipt(Some(receipt)) = store
        .execute(FabricCommand::Receipt(receipt_key))
        .await
        .expect("receipt")
    else {
        panic!("receipt")
    };
    assert_eq!(receipt.outbox.len(), 1);
    assert_eq!(receipt.outbox[0].envelope.event_type, "toolset.completed");
    assert_eq!(
        store
            .execute(FabricCommand::DispatchOutbox { limit: 100 })
            .await
            .expect("outbox"),
        FabricReply::Dispatched(1)
    );
    assert_eq!(
        store
            .execute(FabricCommand::DispatchOutbox { limit: 100 })
            .await
            .expect("duplicate outbox"),
        FabricReply::Dispatched(0)
    );
}

#[tokio::test]
async fn validation_rejects_arguments_allowlist_and_duplicate_call_ids_without_execution() {
    let directory = tempfile::tempdir().expect("temp");
    let store = Arc::new(
        SqliteEventStore::open(directory.path().join("fabric.db"), "p4")
            .await
            .expect("open"),
    );
    let tools = Arc::new(Tools(AtomicU64::new(7)));
    let operator = ToolPreparationOperator::new(store.clone(), tools);
    let run_id = RunId::new();
    let snapshot = PreparationRequest {
        tenant_id: "local".into(),
        run_id,
        effect_id: "toolset".into(),
        kind: PreparationKind::ToolSet,
    };
    let delivery = request(store.as_ref(), snapshot).await;
    operator.execute(delivery, 2).await.expect("snapshot");
    for (effect, arguments, allowed_tools, duplicate, code) in [
        (
            "arguments",
            json!({"value":"secret"}),
            None,
            false,
            "tool_validation_failed",
        ),
        (
            "allowlist",
            json!({"value":42}),
            Some(vec![]),
            false,
            "tool_validation_failed",
        ),
        (
            "duplicate",
            json!({"value":42}),
            None,
            true,
            "duplicate_call_id",
        ),
    ] {
        let call = ToolValidationRequest {
            call_id: "call".into(),
            name: "echo".into(),
            arguments,
        };
        let command = PreparationRequest {
            tenant_id: "local".into(),
            run_id,
            effect_id: effect.into(),
            kind: PreparationKind::Validate {
                toolset_effect_id: "toolset".into(),
                revision: 7,
                calls: if duplicate {
                    vec![call.clone(), call]
                } else {
                    vec![call]
                },
                allowed_tools,
                allow_run_adf: false,
            },
        };
        let delivery = request(store.as_ref(), command.clone()).await;
        let key = ReceiptKey::for_event("prepare", &delivery.event);
        operator.execute(delivery, 3).await.expect("failure fact");
        let FabricReply::Checkpoint(Some(result)) = store
            .execute(FabricCommand::ReadCheckpoint(command.result_key()))
            .await
            .expect("read")
        else {
            panic!("result")
        };
        assert_eq!(
            result.value,
            json!(PreparationResult::Failed { code: code.into() })
        );
        let FabricReply::Receipt(Some(receipt)) = store
            .execute(FabricCommand::Receipt(key))
            .await
            .expect("receipt")
        else {
            panic!("receipt")
        };
        assert_eq!(
            receipt.outbox[0].envelope.event_type,
            "tool.validation.failed"
        );
        assert_eq!(
            receipt.outbox[0].envelope.payload,
            json!({"result_key":command.result_key()})
        );
    }
}
