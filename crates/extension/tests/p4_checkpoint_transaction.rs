use agent_core::{contracts::RunId, contracts::events::EventId, contracts::fabric::*, operator::*};
use agent_extension::operator_fabric::MemoryFabric;
use serde_json::json;

fn event(run_id: RunId, identity: &str) -> FabricEvent {
    FabricEvent {
        topic: "p4-test".into(),
        envelope: OperatorEnvelope {
            schema_version: 1,
            engine_version: EngineVersion::OperatorV1,
            projection_version: 1,
            event_id: EventId::stable("p4-transaction", identity),
            event_type: "tool.requested".into(),
            tenant_id: "local".into(),
            run_id,
            session_id: None,
            effect_id: identity.into(),
            invocation_id: identity.into(),
            attempt: 1,
            issued_revision: RunRevision(1),
            lease_epoch: LeaseEpoch(1),
            correlation_id: run_id.to_string(),
            causation_id: None,
            operator: OperatorIdentity {
                name: "test".into(),
                version: 1,
            },
            operator_seq: OperatorSequence(0),
            delivery_class: DeliveryClass::Durable,
            occurred_at_ms: 1,
            payload: json!({"value":1}),
        },
        payload_schema: json!({"type":"object","required":["value"],"properties":{"value":{"type":"integer"}}}),
    }
}

async fn exercise(store: &dyn OperatorFabric) -> String {
    let run_id = RunId::new();
    let key = run_id.to_string();
    let requested = event(run_id, &format!("{run_id}:requested"));
    let result = store
        .execute(FabricCommand::Transact {
            checkpoint: CheckpointWrite {
                key: key.clone(),
                expected: None,
                value: json!({"phase":"pending"}),
            },
            outbox: vec![requested.clone()],
            delivery: None,
        })
        .await
        .expect("create transaction");
    assert_eq!(
        result,
        FabricReply::Checkpoint(Some(FabricCheckpoint {
            version: 0,
            value: json!({"phase":"pending"})
        }))
    );
    store
        .execute(FabricCommand::DispatchOutbox { limit: 10 })
        .await
        .expect("dispatch");
    store
        .execute(FabricCommand::Subscribe(SubscriptionSpec {
            consumer: "tool".into(),
            subscription: "tool".into(),
            topic: "p4-test".into(),
            event_type: None,
            tenant: Some("local".into()),
        }))
        .await
        .expect("subscribe");
    let FabricReply::Claimed(deliveries) = store
        .execute(FabricCommand::Claim {
            worker: "worker".into(),
            now_ms: 10,
            lease_ms: 100,
            limit: 1,
            max_attempts: 3,
        })
        .await
        .expect("claim")
    else {
        panic!("claim reply")
    };
    let lease = deliveries[0].lease.clone();
    let mut completed = event(run_id, &format!("{run_id}:completed"));
    completed.envelope.event_type = "tool.completed".into();
    let write = CheckpointWrite {
        key: key.clone(),
        expected: Some(0),
        value: json!({"phase":"done"}),
    };
    let mut invalid = completed.clone();
    invalid.envelope.payload = json!({"value":"wrong"});
    assert!(
        store
            .execute(FabricCommand::Transact {
                checkpoint: write.clone(),
                outbox: vec![invalid],
                delivery: Some((lease.clone(), 11))
            })
            .await
            .is_err()
    );
    assert_eq!(
        store
            .execute(FabricCommand::ReadCheckpoint(key.clone()))
            .await
            .expect("read"),
        result
    );
    assert_eq!(
        store
            .execute(FabricCommand::Offset {
                consumer: "tool".into(),
                subscription: "tool".into()
            })
            .await
            .expect("offset"),
        FabricReply::Offset(0)
    );
    assert_eq!(
        store
            .execute(FabricCommand::Receipt(ReceiptKey::for_event(
                "tool", &requested
            )))
            .await
            .expect("receipt"),
        FabricReply::Receipt(None)
    );
    store
        .execute(FabricCommand::Transact {
            checkpoint: write.clone(),
            outbox: vec![completed.clone()],
            delivery: Some((lease.clone(), 11)),
        })
        .await
        .expect("commit state receipt outbox ACK");
    assert_eq!(
        store
            .execute(FabricCommand::Transact {
                checkpoint: write,
                outbox: vec![completed],
                delivery: Some((lease, 12))
            })
            .await,
        Err(FabricError::Fenced)
    );
    assert_eq!(
        store
            .execute(FabricCommand::Offset {
                consumer: "tool".into(),
                subscription: "tool".into()
            })
            .await
            .expect("offset"),
        FabricReply::Offset(1)
    );
    let FabricReply::Receipt(Some(receipt)) = store
        .execute(FabricCommand::Receipt(ReceiptKey::for_event(
            "tool", &requested,
        )))
        .await
        .expect("receipt")
    else {
        panic!("receipt missing")
    };
    assert_eq!(receipt.outbox[0].envelope.operator_seq, OperatorSequence(2));
    assert_eq!(
        receipt.result,
        json!({"checkpoint_key":key,"checkpoint_version":1})
    );
    key
}

#[tokio::test]
async fn memory_transaction_rolls_back_state_receipt_outbox_and_ack() {
    exercise(&MemoryFabric::default()).await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn coordinator_and_effect_checkpoints_commit_together_or_not_at_all() {
    use agent_extension::store::SqliteEventStore;
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("fabric.db");
    let writes = vec![
        CheckpointWrite {
            key: "run".into(),
            expected: None,
            value: json!({"phase":"tool"}),
        },
        CheckpointWrite {
            key: "effect".into(),
            expected: None,
            value: json!({"call":"write"}),
        },
    ];
    {
        let store = SqliteEventStore::open(&path, "p4").await.expect("open");
        let mut invalid = writes.clone();
        invalid[1].expected = Some(99);
        assert_eq!(
            store
                .execute(FabricCommand::TransactMany {
                    checkpoints: invalid,
                    outbox: vec![],
                    delivery: None
                })
                .await,
            Err(FabricError::Fenced)
        );
        assert_eq!(
            store
                .execute(FabricCommand::ReadCheckpoint("run".into()))
                .await
                .expect("read"),
            FabricReply::Checkpoint(None)
        );
        store
            .execute(FabricCommand::TransactMany {
                checkpoints: writes,
                outbox: vec![event(RunId::new(), "many")],
                delivery: None,
            })
            .await
            .expect("commit");
    }
    let store = SqliteEventStore::open(&path, "p4").await.expect("reopen");
    for key in ["run", "effect"] {
        assert!(matches!(
            store
                .execute(FabricCommand::ReadCheckpoint(key.into()))
                .await
                .expect("read"),
            FabricReply::Checkpoint(Some(FabricCheckpoint { version: 0, .. }))
        ));
    }
    assert_eq!(
        store
            .execute(FabricCommand::DispatchOutbox { limit: 10 })
            .await
            .expect("dispatch"),
        FabricReply::Dispatched(1)
    );
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn consumer_claim_does_not_lease_another_operators_work_after_reopen() {
    use agent_extension::store::SqliteEventStore;
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("fabric.db");
    {
        let store = SqliteEventStore::open(&path, "p4").await.expect("open");
        let mut requested = event(RunId::new(), "isolated");
        requested.envelope.operator_seq = OperatorSequence(1);
        store
            .execute(FabricCommand::Publish(requested))
            .await
            .expect("publish");
        for consumer in ["context", "tools"] {
            store
                .execute(FabricCommand::Subscribe(SubscriptionSpec {
                    consumer: consumer.into(),
                    subscription: "work".into(),
                    topic: "p4-test".into(),
                    event_type: None,
                    tenant: None,
                }))
                .await
                .expect("subscribe");
        }
        let FabricReply::Claimed(deliveries) = store
            .execute(FabricCommand::ClaimForConsumer {
                consumer: "tools".into(),
                worker: "tools-worker".into(),
                now_ms: 1,
                lease_ms: 100,
                limit: 10,
                max_attempts: 3,
            })
            .await
            .expect("claim tools")
        else {
            panic!("claim")
        };
        assert_eq!(deliveries.len(), 1);
        assert_eq!(deliveries[0].lease.consumer, "tools");
    }
    let store = SqliteEventStore::open(&path, "p4").await.expect("reopen");
    let FabricReply::Claimed(deliveries) = store
        .execute(FabricCommand::ClaimForConsumer {
            consumer: "context".into(),
            worker: "context-worker".into(),
            now_ms: 2,
            lease_ms: 100,
            limit: 10,
            max_attempts: 3,
        })
        .await
        .expect("claim context")
    else {
        panic!("claim")
    };
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].lease.consumer, "context");
    assert_eq!(deliveries[0].lease.epoch, 1);
    assert_eq!(deliveries[0].lease.delivery_attempt, 1);
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_transaction_and_undispatched_outbox_survive_reopen() {
    use agent_extension::store::SqliteEventStore;
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("fabric.db");
    let key = {
        let store = SqliteEventStore::open(&path, "p4").await.expect("open");
        exercise(&store).await
    };
    let store = SqliteEventStore::open(&path, "p4").await.expect("reopen");
    assert_eq!(
        store
            .execute(FabricCommand::ReadCheckpoint(key))
            .await
            .expect("checkpoint"),
        FabricReply::Checkpoint(Some(FabricCheckpoint {
            version: 1,
            value: json!({"phase":"done"})
        }))
    );
    assert_eq!(
        store
            .execute(FabricCommand::DispatchOutbox { limit: 10 })
            .await
            .expect("recover outbox"),
        FabricReply::Dispatched(1)
    );
}

async fn guarded_many_contract(store: &dyn OperatorFabric) {
    let run = RunId::new();
    let mut source = event(run, "guarded-source");
    source.envelope.operator_seq = OperatorSequence(1);
    store.execute(FabricCommand::Publish(source)).await.unwrap();
    store
        .execute(FabricCommand::Subscribe(SubscriptionSpec {
            consumer: "writer".into(),
            subscription: "writer".into(),
            topic: "p4-test".into(),
            event_type: None,
            tenant: Some("local".into()),
        }))
        .await
        .unwrap();
    let FabricReply::Claimed(deliveries) = store
        .execute(FabricCommand::Claim {
            worker: "worker".into(),
            now_ms: 1,
            lease_ms: 100,
            limit: 1,
            max_attempts: 3,
        })
        .await
        .unwrap()
    else {
        panic!("claim")
    };
    let lease = deliveries[0].lease.clone();
    let write = |key: &str, expected| CheckpointWrite {
        key: key.into(),
        expected,
        value: json!({"value": 1}),
    };
    let command = |now_ms, expected, outbox| FabricCommand::GuardedTransactMany {
        lease: lease.clone(),
        now_ms,
        checkpoints: vec![write("batch", None), write("head", expected)],
        outbox,
    };
    // A conflict on the last write must roll back the earlier batch and outbox.
    assert_eq!(
        store
            .execute(command(2, Some(7), vec![event(run, "batch-event")]))
            .await,
        Err(FabricError::Fenced)
    );
    assert_eq!(
        store
            .execute(FabricCommand::ReadCheckpoint("batch".into()))
            .await
            .unwrap(),
        FabricReply::Checkpoint(None)
    );
    assert_eq!(
        store
            .execute(FabricCommand::DispatchOutbox { limit: 10 })
            .await
            .unwrap(),
        FabricReply::Dispatched(0)
    );
    // Expired leases and cross-run outbox writes cannot commit either record.
    assert_eq!(
        store.execute(command(101, None, vec![])).await,
        Err(FabricError::Fenced)
    );
    assert!(
        store
            .execute(command(2, None, vec![event(RunId::new(), "foreign")]))
            .await
            .is_err()
    );
    assert_eq!(
        store
            .execute(FabricCommand::ReadCheckpoint("batch".into()))
            .await
            .unwrap(),
        FabricReply::Checkpoint(None)
    );
    store
        .execute(command(2, None, vec![event(run, "batch-event")]))
        .await
        .unwrap();
    for key in ["head", "batch"] {
        assert!(matches!(
            store
                .execute(FabricCommand::ReadCheckpoint(key.into()))
                .await
                .unwrap(),
            FabricReply::Checkpoint(Some(FabricCheckpoint { version: 0, .. }))
        ));
    }
    assert_eq!(
        store
            .execute(FabricCommand::Offset {
                consumer: "writer".into(),
                subscription: "writer".into(),
            })
            .await
            .unwrap(),
        FabricReply::Offset(0)
    );
    // The same delivery stays active for subsequent flushes.
    store
        .execute(FabricCommand::GuardedTransactMany {
            lease: lease.clone(),
            now_ms: 3,
            checkpoints: vec![write("next-batch", None), write("head", Some(0))],
            outbox: vec![],
        })
        .await
        .unwrap();
    assert_eq!(
        store
            .execute(FabricCommand::DispatchOutbox { limit: 10 })
            .await
            .unwrap(),
        FabricReply::Dispatched(1)
    );
    let stream_checkpoint = |now_ms, revision| FabricCommand::GuardedTransactLog {
        lease: lease.clone(),
        now_ms,
        log: "stream-preview".into(),
        expected: 0,
        items: vec![json!("visible")],
        checkpoints: vec![write("stream-state", revision)],
    };
    assert_eq!(
        store.execute(stream_checkpoint(101, None)).await,
        Err(FabricError::Fenced)
    );
    assert_eq!(
        store.execute(stream_checkpoint(4, Some(99))).await,
        Err(FabricError::Fenced)
    );
    assert_eq!(
        store
            .execute(FabricCommand::ReadLog {
                log: "stream-preview".into(),
                after: 0,
                limit: 1
            })
            .await
            .unwrap(),
        FabricReply::Log(None)
    );
    store.execute(stream_checkpoint(4, None)).await.unwrap();
    assert_eq!(
        store
            .execute(FabricCommand::ReadLog {
                log: "stream-preview".into(),
                after: 0,
                limit: 1
            })
            .await
            .unwrap(),
        FabricReply::Log(Some(LogPage {
            through: 1,
            items: vec![json!("visible")]
        }))
    );
    assert_eq!(
        store
            .execute(FabricCommand::Offset {
                consumer: "writer".into(),
                subscription: "writer".into()
            })
            .await
            .unwrap(),
        FabricReply::Offset(0)
    );
}

#[tokio::test]
async fn memory_guarded_many_is_atomic_and_does_not_acknowledge() {
    guarded_many_contract(&MemoryFabric::default()).await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_guarded_many_is_atomic_and_survives_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("guarded.db");
    {
        let store = agent_extension::store::SqliteEventStore::open(&path, "guarded")
            .await
            .unwrap();
        guarded_many_contract(&store).await;
    }
    let store = agent_extension::store::SqliteEventStore::open(&path, "guarded")
        .await
        .unwrap();
    assert!(matches!(
        store
            .execute(FabricCommand::ReadCheckpoint("head".into()))
            .await
            .unwrap(),
        FabricReply::Checkpoint(Some(FabricCheckpoint { version: 1, .. }))
    ));
    assert_eq!(
        store
            .execute(FabricCommand::Offset {
                consumer: "writer".into(),
                subscription: "writer".into(),
            })
            .await
            .unwrap(),
        FabricReply::Offset(0)
    );
}

async fn log_transaction_contract(store: &dyn OperatorFabric) {
    let command = |expected, revision, items| FabricCommand::TransactLog {
        log: "public-log".into(),
        expected,
        items,
        checkpoints: vec![CheckpointWrite {
            key: "run-state".into(),
            expected: revision,
            value: json!(expected),
        }],
        outbox: vec![],
        delivery: None,
    };
    store
        .execute(command(0, None, vec![json!("one"), json!("two")]))
        .await
        .unwrap();
    assert_eq!(
        store
            .execute(command(2, Some(99), vec![json!("lost")]))
            .await,
        Err(FabricError::Fenced)
    );
    assert_eq!(
        store
            .execute(command(0, Some(0), vec![json!("duplicate")]))
            .await,
        Err(FabricError::Fenced)
    );
    assert_eq!(
        store
            .execute(FabricCommand::ReadLog {
                log: "public-log".into(),
                after: 1,
                limit: 1
            })
            .await
            .unwrap(),
        FabricReply::Log(Some(LogPage {
            through: 2,
            items: vec![json!("two")]
        }))
    );
    store
        .execute(command(2, Some(0), vec![json!("three")]))
        .await
        .unwrap();
    assert_eq!(
        store
            .execute(FabricCommand::ReadLog {
                log: "public-log".into(),
                after: 2,
                limit: 128
            })
            .await
            .unwrap(),
        FabricReply::Log(Some(LogPage {
            through: 3,
            items: vec![json!("three")]
        }))
    );
    assert_eq!(
        store
            .execute(FabricCommand::ReadLog {
                log: "missing".into(),
                after: 0,
                limit: 1
            })
            .await
            .unwrap(),
        FabricReply::Log(None)
    );
    assert!(
        store
            .execute(FabricCommand::ReadLog {
                log: "public-log".into(),
                after: 0,
                limit: 0
            })
            .await
            .is_err()
    );
}

#[tokio::test]
async fn memory_log_append_and_checkpoint_commit_atomically() {
    log_transaction_contract(&MemoryFabric::default()).await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_log_append_and_checkpoint_survive_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("log.db");
    {
        let store = agent_extension::store::SqliteEventStore::open(&path, "log")
            .await
            .unwrap();
        log_transaction_contract(&store).await;
    }
    let store = agent_extension::store::SqliteEventStore::open(&path, "log")
        .await
        .unwrap();
    assert_eq!(
        store
            .execute(FabricCommand::ReadLog {
                log: "public-log".into(),
                after: 0,
                limit: 128
            })
            .await
            .unwrap(),
        FabricReply::Log(Some(LogPage {
            through: 3,
            items: vec![json!("one"), json!("two"), json!("three")]
        }))
    );
}
