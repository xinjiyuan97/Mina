use agent_core::{
    event_runtime::EventId, harness::*, operator::*, operator_fabric::*, tool_authorization::*,
};
use agent_extension::store::SqliteEventStore;
use agent_harness::tool_operator::{ToolOperator, ToolRequest};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct FakeTool {
    calls: AtomicUsize,
    execution: ToolExecutionPolicy,
}
impl ToolPort for FakeTool {
    fn definitions(&self) -> Vec<ToolDefinition> {
        vec![
            ToolDefinition::new("echo", "echo", json!({"type":"object"}))
                .with_execution_policy(self.execution),
        ]
    }
    fn call(&self, request: ToolCallRequest) -> ToolCallFuture {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { Ok(ToolOutput::text(request.arguments.to_string())) })
    }
}

fn event(request: &ToolRequest) -> FabricEvent {
    FabricEvent {
        topic: "agent.tools.v1".into(),
        envelope: OperatorEnvelope {
            schema_version: 1,
            engine_version: EngineVersion::OperatorV1,
            projection_version: 1,
            event_id: EventId::stable("p4-tool-test", &request.effect_id),
            event_type: "tool.requested".into(),
            tenant_id: "local".into(),
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
            operator_seq: OperatorSequence(1),
            delivery_class: DeliveryClass::Durable,
            occurred_at_ms: 0,
            payload: json!({"request_key": request.checkpoint_key()}),
        },
        payload_schema: agent_core::tool_operator::payload_schema("tool.requested"),
    }
}

async fn setup(store: &dyn OperatorFabric, tools: &dyn ToolPort) -> ToolRequest {
    setup_with_timeout(store, tools, 1000).await
}

async fn setup_with_timeout(
    store: &dyn OperatorFabric,
    tools: &dyn ToolPort,
    timeout_ms: u64,
) -> ToolRequest {
    let run_id = RunId::new();
    let snapshot = tools.tool_set_snapshot(run_id).expect("snapshot");
    let validated = validate_tool_call(
        &snapshot,
        &ToolValidationRequest {
            call_id: "call".into(),
            name: "echo".into(),
            arguments: json!({"value":42}),
        },
        None,
        false,
    )
    .expect("validate");
    let request = ToolRequest {
        tenant_id: "local".into(),
        run_id,
        effect_id: format!("{run_id}:tool"),
        validated,
        timeout_ms,
        approval: None,
        approval_policy: ToolApprovalPolicy::default(),
    };
    store
        .execute(FabricCommand::Transact {
            checkpoint: CheckpointWrite {
                key: request.checkpoint_key(),
                expected: None,
                value: json!(request),
            },
            outbox: vec![],
            delivery: None,
        })
        .await
        .expect("persist request");
    store
        .execute(FabricCommand::Publish(event(&request)))
        .await
        .expect("request");
    subscribe(store, "one").await;
    request
}

struct NeverTool(AtomicUsize);
impl ToolPort for NeverTool {
    fn definitions(&self) -> Vec<ToolDefinition> {
        vec![ToolDefinition::new(
            "echo",
            "echo",
            json!({"type":"object"}),
        )]
    }
    fn call(&self, _: ToolCallRequest) -> ToolCallFuture {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(std::future::pending())
    }
}

#[tokio::test]
async fn non_idempotent_timeout_commits_uncertain_and_duplicate_does_not_retry() {
    let directory = tempfile::tempdir().expect("temp");
    let fabric = Arc::new(
        SqliteEventStore::open(directory.path().join("fabric.db"), "p4")
            .await
            .expect("store"),
    );
    let tools = Arc::new(NeverTool(AtomicUsize::new(0)));
    let request = setup_with_timeout(fabric.as_ref(), tools.as_ref(), 0).await;
    let delivery = claim(fabric.as_ref(), 0).await;
    let operator = ToolOperator::new(fabric.clone(), tools.clone());
    operator
        .execute(delivery.clone(), || 1)
        .await
        .expect("timeout");
    operator.execute(delivery, || 2).await.expect("duplicate");
    assert_eq!(tools.0.load(Ordering::SeqCst), 1);
    let FabricReply::Checkpoint(Some(saved)) = fabric
        .execute(FabricCommand::ReadCheckpoint(request.execution_key()))
        .await
        .expect("checkpoint")
    else {
        panic!("checkpoint")
    };
    let saved: agent_core::tool_operator::ToolExecutionRecord =
        serde_json::from_value(saved.value).expect("decode");
    assert_eq!(
        saved.result,
        Some(agent_core::tool_operator::ToolExecutionResult::Uncertain {
            code: "tool_timeout".into()
        })
    );
}

struct CrashBeforeResultCommit(Arc<dyn OperatorFabric>);
impl OperatorFabric for CrashBeforeResultCommit {
    fn capabilities(&self) -> FabricCapabilities {
        self.0.capabilities()
    }
    fn execute(&self, command: FabricCommand) -> FabricFuture<'_> {
        Box::pin(async move {
            if matches!(
                &command,
                FabricCommand::Transact {
                    delivery: Some(_),
                    ..
                }
            ) {
                return Err(FabricError::Storage(
                    "injected crash before result commit".into(),
                ));
            }
            self.0.execute(command).await
        })
    }
}

#[tokio::test]
async fn crash_after_external_success_before_receipt_reopens_without_blind_retry() {
    for retry_safe in [false, true] {
        let directory = tempfile::tempdir().expect("temp");
        let path = directory.path().join("fabric.db");
        let tools = Arc::new(FakeTool {
            calls: AtomicUsize::new(0),
            execution: if retry_safe {
                ToolExecutionPolicy::idempotent().with_retry(ToolRetryPolicy::bounded(2, 1, 1))
            } else {
                ToolExecutionPolicy::default()
            },
        });
        let request;
        {
            let fabric = Arc::new(SqliteEventStore::open(&path, "p4").await.expect("store"));
            request = setup(fabric.as_ref(), tools.as_ref()).await;
            let delivery = claim(fabric.as_ref(), 0).await;
            let operator =
                ToolOperator::new(Arc::new(CrashBeforeResultCommit(fabric)), tools.clone());
            assert!(matches!(
                operator.execute(delivery, || 1).await,
                Err(FabricError::Storage(_))
            ));
            assert_eq!(tools.calls.load(Ordering::SeqCst), 1);
        }
        let fabric = Arc::new(SqliteEventStore::open(&path, "p4").await.expect("reopen"));
        ToolOperator::new(fabric.clone(), tools.clone())
            .execute(claim(fabric.as_ref(), 2001).await, || 2002)
            .await
            .expect("recover");
        assert_eq!(
            tools.calls.load(Ordering::SeqCst),
            if retry_safe { 2 } else { 1 }
        );
        let FabricReply::Receipt(Some(receipt)) = fabric
            .execute(FabricCommand::Receipt(ReceiptKey::for_event(
                "tool-v1",
                &event(&request),
            )))
            .await
            .expect("receipt")
        else {
            panic!("receipt")
        };
        assert_eq!(
            receipt.outbox[0].envelope.event_type,
            if retry_safe {
                "tool.completed"
            } else {
                "operator.execution.uncertain"
            }
        );
    }
}

#[tokio::test]
async fn altered_delivery_is_rejected_before_any_tool_side_effect() {
    let directory = tempfile::tempdir().expect("temp");
    let fabric = Arc::new(
        SqliteEventStore::open(directory.path().join("fabric.db"), "p4")
            .await
            .expect("store"),
    );
    let tools = Arc::new(FakeTool {
        calls: AtomicUsize::new(0),
        execution: ToolExecutionPolicy::default(),
    });
    setup(fabric.as_ref(), tools.as_ref()).await;
    let mut delivery = claim(fabric.as_ref(), 0).await;
    delivery.event.envelope.tenant_id = "another-tenant".into();
    assert!(
        ToolOperator::new(fabric, tools.clone())
            .execute(delivery, || 1)
            .await
            .is_err()
    );
    assert_eq!(tools.calls.load(Ordering::SeqCst), 0);
}

async fn subscribe(store: &dyn OperatorFabric, subscription: &str) {
    store
        .execute(FabricCommand::Subscribe(SubscriptionSpec {
            consumer: "tool-v1".into(),
            subscription: subscription.into(),
            topic: "agent.tools.v1".into(),
            event_type: Some("tool.requested".into()),
            tenant: Some("local".into()),
        }))
        .await
        .expect("subscribe");
}

async fn claim(store: &dyn OperatorFabric, now_ms: i64) -> ClaimedDelivery {
    let FabricReply::Claimed(mut deliveries) = store
        .execute(FabricCommand::Claim {
            worker: "worker".into(),
            now_ms,
            lease_ms: 2000,
            limit: 1,
            max_attempts: 5,
        })
        .await
        .expect("claim")
    else {
        panic!("claim")
    };
    assert_eq!(deliveries.len(), 1);
    deliveries.remove(0)
}

#[tokio::test]
async fn tool_receipt_reopen_duplicate_subscription_does_not_repeat_side_effect() {
    let directory = tempfile::tempdir().expect("temp");
    let path = directory.path().join("fabric.db");
    let tools = Arc::new(FakeTool {
        calls: AtomicUsize::new(0),
        execution: ToolExecutionPolicy::default(),
    });
    let request;
    {
        let fabric = Arc::new(SqliteEventStore::open(&path, "p4").await.expect("store"));
        request = setup(fabric.as_ref(), tools.as_ref()).await;
        let operator = ToolOperator::new(fabric.clone(), tools.clone());
        operator
            .execute(claim(fabric.as_ref(), 0).await, || 1)
            .await
            .expect("execute");
        assert_eq!(tools.calls.load(Ordering::SeqCst), 1);
    }
    let fabric = Arc::new(SqliteEventStore::open(&path, "p4").await.expect("reopen"));
    subscribe(fabric.as_ref(), "duplicate").await;
    let operator = ToolOperator::new(fabric.clone(), tools.clone());
    operator
        .execute(claim(fabric.as_ref(), 10).await, || 11)
        .await
        .expect("duplicate");
    assert_eq!(tools.calls.load(Ordering::SeqCst), 1);
    let FabricReply::Receipt(Some(receipt)) = fabric
        .execute(FabricCommand::Receipt(ReceiptKey::for_event(
            "tool-v1",
            &event(&request),
        )))
        .await
        .expect("receipt")
    else {
        panic!("receipt")
    };
    assert_eq!(receipt.outbox[0].envelope.event_type, "tool.completed");
}

#[tokio::test]
async fn crashed_non_idempotent_execution_is_uncertain_without_retry() {
    let directory = tempfile::tempdir().expect("temp");
    let path = directory.path().join("fabric.db");
    let tools = Arc::new(FakeTool {
        calls: AtomicUsize::new(0),
        execution: ToolExecutionPolicy::default(),
    });
    let request;
    {
        let fabric = Arc::new(SqliteEventStore::open(&path, "p4").await.expect("store"));
        request = setup(fabric.as_ref(), tools.as_ref()).await;
        let operator = ToolOperator::new(fabric.clone(), tools.clone());
        operator
            .begin(&claim(fabric.as_ref(), 0).await, 1)
            .await
            .expect("durable execution intent");
    }
    let fabric = Arc::new(SqliteEventStore::open(&path, "p4").await.expect("reopen"));
    let operator = ToolOperator::new(fabric.clone(), tools.clone());
    operator
        .execute(claim(fabric.as_ref(), 2001).await, || 2002)
        .await
        .expect("uncertain");
    assert_eq!(tools.calls.load(Ordering::SeqCst), 0);
    let FabricReply::Receipt(Some(receipt)) = fabric
        .execute(FabricCommand::Receipt(ReceiptKey::for_event(
            "tool-v1",
            &event(&request),
        )))
        .await
        .expect("receipt")
    else {
        panic!("receipt")
    };
    assert_eq!(
        receipt.outbox[0].envelope.event_type,
        "operator.execution.uncertain"
    );
}

#[tokio::test]
async fn crashed_idempotent_execution_can_retry_within_declared_budget() {
    let directory = tempfile::tempdir().expect("temp");
    let path = directory.path().join("fabric.db");
    let tools = Arc::new(FakeTool {
        calls: AtomicUsize::new(0),
        execution: ToolExecutionPolicy::idempotent().with_retry(ToolRetryPolicy::bounded(2, 1, 1)),
    });
    {
        let fabric = Arc::new(SqliteEventStore::open(&path, "p4").await.expect("store"));
        setup(fabric.as_ref(), tools.as_ref()).await;
        ToolOperator::new(fabric.clone(), tools.clone())
            .begin(&claim(fabric.as_ref(), 0).await, 1)
            .await
            .expect("begin");
    }
    let fabric = Arc::new(SqliteEventStore::open(&path, "p4").await.expect("reopen"));
    ToolOperator::new(fabric.clone(), tools.clone())
        .execute(claim(fabric.as_ref(), 2001).await, || 2002)
        .await
        .expect("safe retry");
    assert_eq!(tools.calls.load(Ordering::SeqCst), 1);
}

struct BlockingTool {
    entered: tokio::sync::Notify,
    release: Arc<tokio::sync::Notify>,
}

impl ToolPort for BlockingTool {
    fn definitions(&self) -> Vec<ToolDefinition> {
        vec![ToolDefinition::new(
            "echo",
            "echo",
            json!({"type":"object"}),
        )]
    }
    fn call(&self, _: ToolCallRequest) -> ToolCallFuture {
        self.entered.notify_one();
        let release = self.release.clone();
        Box::pin(async move {
            release.notified().await;
            Ok(ToolOutput::text("late result"))
        })
    }
}

#[tokio::test]
async fn durable_tool_cancel_fences_late_success_without_erasing_audit() {
    let directory = tempfile::tempdir().expect("temp");
    let path = directory.path().join("fabric.db");
    let fabric = Arc::new(SqliteEventStore::open(&path, "p4").await.expect("store"));
    let tools = Arc::new(BlockingTool {
        entered: tokio::sync::Notify::new(),
        release: Arc::new(tokio::sync::Notify::new()),
    });
    let request = setup(fabric.as_ref(), tools.as_ref()).await;
    let delivery = claim(fabric.as_ref(), 0).await;
    let operator = Arc::new(ToolOperator::new(fabric.clone(), tools.clone()));
    let task = {
        let operator = operator.clone();
        let delivery = delivery.clone();
        tokio::spawn(async move { operator.execute(delivery, || 1).await })
    };
    tools.entered.notified().await;
    assert!(
        operator
            .cancel(&delivery, "coordinator", 2)
            .await
            .expect("cancel")
    );
    assert!(
        !operator
            .cancel(&delivery, "coordinator", 3)
            .await
            .expect("duplicate cancel")
    );
    tools.release.notify_one();
    assert_eq!(task.await.expect("task"), Err(FabricError::Fenced));
    fabric
        .execute(FabricCommand::DispatchOutbox { limit: 10 })
        .await
        .expect("dispatch audit");
    let FabricReply::Replay(audit) = fabric
        .execute(FabricCommand::ScanEvents {
            topic: "agent.tools.v1".into(),
            event_type: "operator.result.stale".into(),
            after: 0,
            limit: 10,
        })
        .await
        .expect("audit")
    else {
        panic!("audit reply")
    };
    assert_eq!(audit.len(), 1);
    let FabricReply::Checkpoint(Some(saved)) = fabric
        .execute(FabricCommand::ReadCheckpoint(request.execution_key()))
        .await
        .expect("load")
    else {
        panic!("execution checkpoint")
    };
    let record: agent_core::tool_operator::ToolExecutionRecord =
        serde_json::from_value(saved.value).expect("record");
    assert_eq!(
        record.result,
        Some(agent_core::tool_operator::ToolExecutionResult::Cancelled)
    );
    drop(operator);
    drop(fabric);
    let fabric = Arc::new(SqliteEventStore::open(&path, "p4").await.expect("reopen"));
    ToolOperator::new(fabric.clone(), tools)
        .execute(claim(fabric.as_ref(), 2001).await, || 2002)
        .await
        .expect("ack cancelled command after reopen");
    assert_eq!(
        fabric
            .execute(FabricCommand::Offset {
                consumer: "tool-v1".into(),
                subscription: "one".into()
            })
            .await
            .expect("offset"),
        FabricReply::Offset(1)
    );
}

#[tokio::test]
async fn tool_reads_durable_approval_instead_of_trusting_a_stale_allow_copy() {
    use agent_harness::approval_operator::ApprovalIngress;
    for cancelled in [false, true] {
        let directory = tempfile::tempdir().expect("temp");
        let fabric = Arc::new(
            SqliteEventStore::open(directory.path().join("fabric.db"), "p4")
                .await
                .expect("store"),
        );
        let tools = Arc::new(FakeTool {
            calls: AtomicUsize::new(0),
            execution: ToolExecutionPolicy::default(),
        });
        let run_id = RunId::new();
        let validated = validate_tool_call(
            &tools.tool_set_snapshot(run_id).expect("snapshot"),
            &ToolValidationRequest {
                call_id: "call".into(),
                name: "echo".into(),
                arguments: json!({"value":42}),
            },
            None,
            false,
        )
        .expect("validated");
        let pending = BoundApproval::new(run_id, "approval-effect", validated.clone(), 10, 100)
            .expect("pending");
        let ingress = ApprovalIngress::new(fabric.clone());
        ingress
            .request("local", pending.clone())
            .await
            .expect("request");
        ingress
            .resolve(
                "local",
                run_id,
                pending.approval_id,
                "actor",
                ApprovalResolution::allow_once(),
                20,
            )
            .await
            .expect("resolve");
        let approved = ingress
            .load("local", run_id, pending.approval_id)
            .await
            .expect("allow copy");
        if cancelled {
            ingress
                .cancel("local", run_id, pending.approval_id, "coordinator", 21)
                .await
                .expect("cancel approval");
        }
        let request = ToolRequest {
            tenant_id: "local".into(),
            run_id,
            effect_id: format!("{run_id}:tool"),
            validated,
            timeout_ms: 1000,
            approval: Some(approved),
            approval_policy: ToolApprovalPolicy::new(0),
        };
        let mut requested = event(&request);
        requested.envelope.operator_seq = OperatorSequence(0);
        fabric
            .execute(FabricCommand::Transact {
                checkpoint: CheckpointWrite {
                    key: request.checkpoint_key(),
                    expected: None,
                    value: json!(request),
                },
                outbox: vec![requested],
                delivery: None,
            })
            .await
            .expect("enqueue");
        fabric
            .execute(FabricCommand::DispatchOutbox { limit: 10 })
            .await
            .expect("dispatch");
        subscribe(fabric.as_ref(), "one").await;
        ToolOperator::new(fabric.clone(), tools.clone())
            .execute(claim(fabric.as_ref(), 22).await, || 23)
            .await
            .expect("execute");
        assert_eq!(tools.calls.load(Ordering::SeqCst), usize::from(!cancelled));
    }
}
