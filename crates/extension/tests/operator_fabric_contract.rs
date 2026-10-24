use agent_core::{event_runtime::EventId, operator_fabric::*};
use agent_extension::operator_fabric::MemoryFabric;
use serde_json::json;

#[tokio::test]
#[cfg(feature = "pipeline")]
async fn p5_artifact_reader_preserves_legacy_and_rejects_future_before_decode() {
    use agent_core::harness::{BlobStore, PutBlob};
    let store = std::sync::Arc::new(MemoryFabric::default());
    let blobs = agent_extension::pipeline::FabricBlobStore(store.clone());
    let metadata = blobs
        .put(PutBlob {
            media_type: "text/plain".into(),
            name: None,
            data: b"legacy".to_vec(),
            created_at_ms: 0,
        })
        .await
        .expect("artifact");
    let key = format!("pipeline-artifact:{}", metadata.blob_id);
    let FabricReply::Checkpoint(Some(mut saved)) = store
        .execute(FabricCommand::ReadCheckpoint(key.clone()))
        .await
        .expect("read")
    else {
        panic!("manifest")
    };
    saved
        .value
        .as_object_mut()
        .expect("object")
        .remove("schema_version");
    store
        .execute(FabricCommand::Transact {
            checkpoint: CheckpointWrite {
                key: key.clone(),
                expected: Some(saved.version),
                value: saved.value,
            },
            outbox: vec![],
            delivery: None,
        })
        .await
        .expect("historical manifest");
    assert_eq!(
        blobs
            .get(metadata.blob_id)
            .await
            .expect("read legacy")
            .expect("blob")
            .data,
        b"legacy"
    );
    store
        .execute(FabricCommand::Transact {
            checkpoint: CheckpointWrite {
                key,
                expected: Some(saved.version + 1),
                value: json!({"schema_version":999,"metadata":"future format"}),
            },
            outbox: vec![],
            delivery: None,
        })
        .await
        .expect("future manifest");
    let error = blobs
        .get(metadata.blob_id)
        .await
        .expect_err("isolate future artifact");
    assert!(
        error.to_string().contains("unsupported_artifact_version"),
        "{error}"
    );
}

#[tokio::test]
#[cfg(all(feature = "sqlite", feature = "pipeline"))]
async fn p5_text_ledger_unknown_versions_fail_closed_before_decode() {
    use agent_core::{
        harness::RunId,
        pipeline::{PipelineError, PipelineStore},
    };
    let directory = tempfile::tempdir().expect("temp");
    let path = directory.path().join("text.db");
    let store = agent_extension::pipeline::SqlitePipelineStore::open(&path).expect("open");
    let connection = rusqlite::Connection::open(&path).expect("connection");
    let run_id = RunId::new();
    let encoded = json!({"schema_version":999,"reducer":"future format"}).to_string();
    connection
        .execute(
            "INSERT INTO operator_runs_v1(run_id,generation,record_json) VALUES (?1,0,?2)",
            [run_id.to_string(), encoded.clone()],
        )
        .expect("future record");
    assert_eq!(
        store.load(run_id).await,
        Err(PipelineError::Storage(
            "unsupported_pipeline_version".into()
        ))
    );
    let saved: String = connection
        .query_row("SELECT record_json FROM operator_runs_v1", [], |row| {
            row.get(0)
        })
        .expect("saved");
    assert_eq!(saved, encoded);
}

#[tokio::test]
#[cfg(feature = "pipeline")]
async fn p5_artifact_size_is_rejected_before_any_chunk_is_saved() {
    use agent_core::harness::{BlobStore, PutBlob};
    let store = std::sync::Arc::new(MemoryFabric::default());
    let blobs = agent_extension::pipeline::FabricBlobStore(store.clone());
    assert!(
        blobs
            .put(PutBlob {
                media_type: "application/octet-stream".into(),
                name: None,
                data: vec![0; 16 * 1024 * 1024 + 1],
                created_at_ms: 0
            })
            .await
            .is_err()
    );
    let FabricReply::Retention(report) = store
        .execute(FabricCommand::InspectRetention {
            now_ms: 1,
            grace_ms: 1,
            external_roots: vec![],
        })
        .await
        .expect("inspect")
    else {
        panic!("report")
    };
    assert_eq!(report.checkpoints, 0);
}

#[tokio::test]
#[cfg(feature = "sqlite")]
async fn p5_unknown_ledger_version_is_quarantined_without_rewrite() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("events.db");
    let store = agent_extension::store::SqliteEventStore::open(&path, "p5")
        .await
        .expect("open");
    store
        .execute(FabricCommand::Publish(event(1)))
        .await
        .expect("publish");
    drop(store);
    let connection = rusqlite::Connection::open(&path).expect("connection");
    let original: String = connection
        .query_row(
            "SELECT state_json FROM workflow_operator_fabric_v1",
            [],
            |row| row.get(0),
        )
        .expect("state");
    let mut future: serde_json::Value = serde_json::from_str(&original).expect("json");
    future["schema_version"] = json!(999);
    future["events"] = json!("future format must not be decoded");
    let encoded = future.to_string();
    connection
        .execute(
            "UPDATE workflow_operator_fabric_v1 SET state_json = ?1",
            [&encoded],
        )
        .expect("future fixture");
    let store = agent_extension::store::SqliteEventStore::open(&path, "p5")
        .await
        .expect("reopen");
    assert_eq!(
        store.execute(FabricCommand::Publish(event(2))).await,
        Err(FabricError::Invalid("unsupported_fabric_version".into()))
    );
    let unchanged: String = connection
        .query_row(
            "SELECT state_json FROM workflow_operator_fabric_v1",
            [],
            |row| row.get(0),
        )
        .expect("state");
    assert_eq!(unchanged, encoded);
    let mut historical: serde_json::Value = serde_json::from_str(&original).expect("JSON");
    historical
        .as_object_mut()
        .expect("object")
        .remove("schema_version");
    historical
        .as_object_mut()
        .expect("object")
        .remove("lease_reclaims");
    let mut unsupported = historical.clone();
    unsupported["events"][0]["event"]["envelope"]["projection_version"] = json!(999);
    connection
        .execute(
            "UPDATE workflow_operator_fabric_v1 SET state_json = ?1",
            [unsupported.to_string()],
        )
        .expect("future projection");
    assert_eq!(
        store.execute(FabricCommand::Publish(event(2))).await,
        Err(FabricError::Invalid(
            "unsupported_fabric_event_version".into()
        ))
    );
    connection
        .execute(
            "UPDATE workflow_operator_fabric_v1 SET state_json = ?1",
            [historical.to_string()],
        )
        .expect("rollback fixture");
    assert!(
        store
            .execute(FabricCommand::Publish(event(2)))
            .await
            .is_ok()
    );
}

fn event(sequence: u64) -> FabricEvent {
    FabricEvent {
        topic: "facts".into(),
        envelope: serde_json::from_value(json!({
            "schema_version": 1, "engine_version": "operator_v1", "projection_version": 1,
            "event_id": EventId::stable("p1", &sequence.to_string()), "event_type": "test.fact",
            "tenant_id": "tenant", "run_id": "00000000-0000-0000-0000-000000000002", "session_id": null,
            "effect_id": "effect", "invocation_id": "invocation", "attempt": 1,
            "issued_revision": 0, "lease_epoch": 1, "correlation_id": "correlation", "causation_id": null,
            "operator": {"name": "test", "version": 1}, "operator_seq": sequence,
            "delivery_class": "durable", "occurred_at_ms": 0, "payload": {"value": sequence}
        })).expect("envelope"),
        payload_schema: json!({"type":"object","required":["value"],"properties":{"value":{"type":"integer"}},"additionalProperties":false}),
    }
}

#[tokio::test]
async fn p5_atomic_backlog_limit_allows_retry_and_drain() {
    let store = MemoryFabric::default();
    let publish = |sequence| FabricCommand::PublishBounded {
        events: vec![event(sequence)],
        subscription: subscription("sub"),
        max_pending: 1,
    };
    store.execute(publish(1)).await.expect("first");
    store
        .execute(publish(1))
        .await
        .expect("idempotent at capacity");
    assert_eq!(store.execute(publish(2)).await, Err(FabricError::Capacity));
    assert_eq!(replay(&store).await.len(), 1);
    let delivery = claim(&store, 0).await.remove(0);
    store
        .execute(commit(delivery.lease, 1, vec![]))
        .await
        .expect("drain");
    store.execute(publish(2)).await.expect("capacity released");
}

#[tokio::test]
async fn p5_inventory_counts_expired_lease_reclaims() {
    let store = MemoryFabric::default();
    subscribe(&store).await;
    store
        .execute(FabricCommand::Publish(event(1)))
        .await
        .expect("publish");
    claim(&store, 0).await;
    claim(&store, 10).await;
    let FabricReply::Retention(report) = store
        .execute(FabricCommand::InspectRetention {
            now_ms: 11,
            grace_ms: 1,
            external_roots: vec![],
        })
        .await
        .expect("report")
    else {
        panic!("report")
    };
    assert_eq!(report.lease_reclaims, 1);
    assert_eq!(report.active_leases, 1);
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn p5_sqlite_busy_does_not_ack_or_publish_unsaved_facts() {
    let directory = tempfile::tempdir().expect("temp");
    let path = directory.path().join("busy.db");
    let store = agent_extension::store::SqliteEventStore::open(&path, "p5")
        .await
        .expect("store");
    subscribe(&store).await;
    store
        .execute(FabricCommand::Publish(event(1)))
        .await
        .expect("publish");
    let delivery = claim(&store, 0).await.remove(0);
    let connection = rusqlite::Connection::open(&path).expect("connection");
    connection
        .execute_batch("BEGIN IMMEDIATE")
        .expect("hold writer");
    assert!(matches!(
        store
            .execute(commit(delivery.lease, 1, vec![event(2)]))
            .await,
        Err(FabricError::Storage(_))
    ));
    connection.execute_batch("ROLLBACK").expect("release");
    assert_eq!(offset(&store).await, 0);
    assert_eq!(replay(&store).await.len(), 1);
    let delivery = claim(&store, 10).await.remove(0);
    store
        .execute(commit(delivery.lease, 11, vec![event(2)]))
        .await
        .expect("retry after reclaim");
    assert_eq!(offset(&store).await, 1);
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn p5_seeded_kill_points_reopen_without_duplicate_facts() {
    let seed = 0x5eed_u64;
    let mut random = seed;
    let mut covered = [false; 4];
    for iteration in 0..32 {
        random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
        let point = (random >> 32) as usize % 4;
        covered[point] = true;
        let directory = tempfile::tempdir().expect("temp");
        let path = directory.path().join("kill.db");
        let store = agent_extension::store::SqliteEventStore::open(&path, "p5")
            .await
            .expect("open");
        subscribe(&store).await;
        store
            .execute(FabricCommand::Publish(event(1)))
            .await
            .expect("publish");
        if point > 0 {
            let delivery = claim(&store, 0).await.remove(0);
            if point > 1 {
                store
                    .execute(commit(delivery.lease, 1, vec![event(2)]))
                    .await
                    .expect("commit");
                if point > 2 {
                    store
                        .execute(FabricCommand::DispatchOutbox { limit: 1 })
                        .await
                        .expect("dispatch");
                }
            }
        }
        drop(store);
        let store = agent_extension::store::SqliteEventStore::open(&path, "p5")
            .await
            .expect("reopen");
        if point < 2 {
            let delivery = claim(&store, 10).await.remove(0);
            store
                .execute(commit(delivery.lease, 11, vec![event(2)]))
                .await
                .expect("recover");
        }
        store
            .execute(FabricCommand::DispatchOutbox { limit: 1 })
            .await
            .expect("dispatch");
        store
            .execute(FabricCommand::DispatchOutbox { limit: 1 })
            .await
            .expect("idempotent dispatch");
        assert_eq!(
            offset(&store).await,
            1,
            "seed={seed} iteration={iteration} point={point}"
        );
        assert_eq!(
            replay(&store).await.len(),
            2,
            "seed={seed} iteration={iteration} point={point}"
        );
    }
    assert_eq!(covered, [true; 4]);
}

#[tokio::test]
async fn p5_retention_inventory_protects_active_outbox_and_checkpoint_references() {
    let store = MemoryFabric::default();
    let orphan = agent_core::harness::BlobId::new();
    let active = agent_core::harness::BlobId::new();
    let pending = agent_core::harness::BlobId::new();
    let dead = agent_core::harness::BlobId::new();
    for blob in [orphan, active, pending, dead] {
        store
            .execute(FabricCommand::Transact {
                checkpoint: CheckpointWrite {
                    key: format!("pipeline-artifact:{blob}"),
                    expected: None,
                    value: json!({"metadata":{"blob_id":blob,"created_at_ms":0},"chunks":1}),
                },
                outbox: vec![],
                delivery: None,
            })
            .await
            .expect("manifest");
    }
    store
        .execute(FabricCommand::Transact {
            checkpoint: CheckpointWrite {
                key: "active-run".into(),
                expected: None,
                value: json!({"ref":active}),
            },
            outbox: vec![],
            delivery: None,
        })
        .await
        .expect("active checkpoint");
    subscribe(&store).await;
    let mut source = event(1);
    source.envelope.payload = json!({"ref":dead});
    source.payload_schema = json!(true);
    store
        .execute(FabricCommand::Publish(source))
        .await
        .expect("source");
    let delivery = claim(&store, 0).await.remove(0);
    store
        .execute(FabricCommand::Retry {
            lease: delivery.lease,
            now_ms: 1,
            backoff_ms: 1,
            max_attempts: 1,
            error: "injected".into(),
        })
        .await
        .expect("DLQ");
    let mut outbox = event(2);
    outbox.envelope.operator_seq.0 = 0;
    outbox.envelope.payload = json!({"ref":pending});
    outbox.payload_schema = json!(true);
    store
        .execute(FabricCommand::Transact {
            checkpoint: CheckpointWrite {
                key: "outbox-owner".into(),
                expected: None,
                value: json!({}),
            },
            outbox: vec![outbox],
            delivery: None,
        })
        .await
        .expect("pending outbox");
    let inspect = |now_ms| FabricCommand::InspectRetention {
        now_ms,
        grace_ms: 100,
        external_roots: vec![],
    };
    let FabricReply::Retention(report) = store.execute(inspect(99)).await.expect("inspect") else {
        panic!("report")
    };
    assert!(report.orphan_candidates.is_empty());
    let FabricReply::Retention(report) = store.execute(inspect(100)).await.expect("inspect") else {
        panic!("report")
    };
    assert_eq!(report.orphan_candidates, vec![orphan]);
    assert_eq!(report.protected_artifacts, 3);
    assert_eq!(report.outbox_backlog, 1);
    assert_eq!(report.dead_letters, 1);
    let FabricReply::Retention(report) = store
        .execute(FabricCommand::InspectRetention {
            now_ms: 100,
            grace_ms: 100,
            external_roots: vec![orphan],
        })
        .await
        .expect("external roots")
    else {
        panic!("report")
    };
    assert!(report.orphan_candidates.is_empty());
    assert!(matches!(
        store
            .execute(FabricCommand::ReadCheckpoint(format!(
                "pipeline-artifact:{orphan}"
            )))
            .await,
        Ok(FabricReply::Checkpoint(Some(_)))
    ));
}

#[tokio::test]
async fn p6_retention_protects_attachment_and_session_blob_roots() {
    use agent_core::harness::{BlobId, ContentPart, ModelAttachment};
    let store = MemoryFabric::default();
    let active = BlobId::new();
    let session = BlobId::new();
    for blob in [active, session] {
        store
            .execute(FabricCommand::Transact {
                checkpoint: CheckpointWrite {
                    key: format!("pipeline-artifact:{blob}"),
                    expected: None,
                    value: json!({"metadata":{"blob_id":blob,"created_at_ms":0},"chunks":1}),
                },
                outbox: vec![],
                delivery: None,
            })
            .await
            .expect("manifest");
    }
    store.execute(FabricCommand::Transact {
        checkpoint: CheckpointWrite { key: "p6-active-context".into(), expected: None,
            value: json!({"attachments":[ModelAttachment { blob_id: active, media_type: "image/png".into(), name: None, size_bytes: Some(3) }]}) },
        outbox: vec![], delivery: None,
    }).await.expect("context checkpoint");
    let part = ContentPart::BlobRef {
        blob_id: session,
        media_type: "application/pdf".into(),
        name: None,
        size_bytes: Some(3),
    };
    let ContentPart::BlobRef { blob_id, .. } = part else {
        panic!("blob ref")
    };
    let FabricReply::Retention(report) = store
        .execute(FabricCommand::InspectRetention {
            now_ms: 1000,
            grace_ms: 1,
            external_roots: vec![blob_id],
        })
        .await
        .expect("inspect")
    else {
        panic!("inventory")
    };
    assert_eq!(report.protected_artifacts, 2);
    assert!(report.orphan_candidates.is_empty());
    for blob in [active, session] {
        assert!(matches!(
            store
                .execute(FabricCommand::ReadCheckpoint(format!(
                    "pipeline-artifact:{blob}"
                )))
                .await,
            Ok(FabricReply::Checkpoint(Some(_)))
        ));
    }
}

fn subscription(name: &str) -> SubscriptionSpec {
    SubscriptionSpec {
        consumer: "consumer".into(),
        subscription: name.into(),
        topic: "facts".into(),
        event_type: None,
        tenant: None,
    }
}

async fn subscribe(store: &dyn OperatorFabric) {
    assert_eq!(
        store
            .execute(FabricCommand::Subscribe(subscription("sub")))
            .await
            .expect("subscribe"),
        FabricReply::Subscribed
    );
}

async fn replay(store: &dyn OperatorFabric) -> Vec<ReplayItem> {
    match store
        .execute(FabricCommand::Replay {
            consumer: "consumer".into(),
            subscription: "sub".into(),
            after: 0,
            limit: 128,
        })
        .await
        .expect("replay")
    {
        FabricReply::Replay(items) => items,
        other => panic!("unexpected {other:?}"),
    }
}

async fn offset(store: &dyn OperatorFabric) -> u64 {
    match store
        .execute(FabricCommand::Offset {
            consumer: "consumer".into(),
            subscription: "sub".into(),
        })
        .await
        .expect("offset")
    {
        FabricReply::Offset(value) => value,
        other => panic!("unexpected {other:?}"),
    }
}

async fn claim(store: &dyn OperatorFabric, now_ms: i64) -> Vec<ClaimedDelivery> {
    match store
        .execute(FabricCommand::Claim {
            worker: "worker".into(),
            now_ms,
            lease_ms: 10,
            limit: 128,
            max_attempts: 3,
        })
        .await
        .expect("claim")
    {
        FabricReply::Claimed(items) => items,
        other => panic!("unexpected {other:?}"),
    }
}

fn commit(lease: DeliveryLease, now_ms: i64, outbox: Vec<FabricEvent>) -> FabricCommand {
    FabricCommand::Commit {
        lease,
        now_ms,
        result: json!({"ok":true}),
        outbox,
    }
}

async fn single_publish(store: &dyn OperatorFabric) {
    subscribe(store).await;
    let command = FabricCommand::Publish(event(1));
    let first = store.execute(command.clone()).await.expect("publish");
    assert_eq!(store.execute(command).await.expect("idempotent"), first);
    let mut changed = event(1);
    changed.envelope.payload = json!({"value":99});
    assert_eq!(
        store.execute(FabricCommand::Publish(changed)).await,
        Err(FabricError::Conflict)
    );
    assert_eq!(replay(store).await.len(), 1);
    assert_eq!(replay(store).await[0].offset, 1);
}

async fn batch_atomic(store: &dyn OperatorFabric) {
    subscribe(store).await;
    let mut invalid = event(2);
    invalid.envelope.payload = json!({"value":"invalid"});
    assert!(
        store
            .execute(FabricCommand::PublishBatch(vec![event(1), invalid]))
            .await
            .is_err()
    );
    assert!(replay(store).await.is_empty());
    let mut conflict = event(1);
    conflict.envelope.payload = json!({"value":99});
    assert_eq!(
        store
            .execute(FabricCommand::PublishBatch(vec![
                event(1),
                conflict.clone()
            ]))
            .await,
        Err(FabricError::Conflict)
    );
    assert!(replay(store).await.is_empty());
    let command = FabricCommand::PublishBatch(vec![event(1), event(1), event(2)]);
    let first = store
        .execute(command.clone())
        .await
        .expect("duplicate identical ID collapses");
    assert_eq!(store.execute(command).await.expect("batch retry"), first);
    assert_eq!(replay(store).await.len(), 2);
    assert_eq!(
        store
            .execute(FabricCommand::PublishBatch(vec![event(3), conflict]))
            .await,
        Err(FabricError::Conflict)
    );
    assert_eq!(replay(store).await.len(), 2);
}

async fn holes_and_order(store: &dyn OperatorFabric) {
    subscribe(store).await;
    assert!(
        store
            .execute(FabricCommand::Publish(event(2)))
            .await
            .is_err()
    );
    store
        .execute(FabricCommand::PublishBatch(vec![event(1), event(2)]))
        .await
        .expect("publish");
    let claims = claim(store, 0).await;
    assert_eq!(claims.len(), 1);
    let mut hole = claims[0].lease.clone();
    hole.offset = 2;
    assert_eq!(
        store.execute(commit(hole, 1, vec![])).await,
        Err(FabricError::AckHole)
    );
    assert_eq!(offset(store).await, 0);
    let first = store
        .execute(commit(claims[0].lease.clone(), 1, vec![]))
        .await
        .expect("ack");
    assert_eq!(
        store
            .execute(commit(claims[0].lease.clone(), 2, vec![]))
            .await
            .expect("duplicate ack"),
        first
    );
    assert_eq!(claim(store, 2).await[0].event, event(2));
}

async fn fencing(store: &dyn OperatorFabric) {
    subscribe(store).await;
    store
        .execute(FabricCommand::Publish(event(1)))
        .await
        .expect("publish");
    let old = claim(store, 0).await.remove(0).lease;
    assert_eq!(
        store.execute(commit(old.clone(), 10, vec![event(2)])).await,
        Err(FabricError::Fenced)
    );
    let new = claim(store, 10).await.remove(0).lease;
    assert!(new.epoch > old.epoch);
    assert_eq!(
        store.execute(commit(old.clone(), 11, vec![])).await,
        Err(FabricError::Fenced)
    );
    assert_eq!(
        store
            .execute(FabricCommand::Retry {
                lease: old,
                now_ms: 11,
                backoff_ms: 1,
                max_attempts: 3,
                error: "stale".into()
            })
            .await,
        Err(FabricError::Fenced)
    );
    store
        .execute(commit(new, 11, vec![]))
        .await
        .expect("current worker");
    assert_eq!(replay(store).await.len(), 1);
}

async fn transaction(store: &dyn OperatorFabric) {
    subscribe(store).await;
    store
        .execute(FabricCommand::Publish(event(1)))
        .await
        .expect("publish");
    let lease = claim(store, 0).await.remove(0).lease;
    let key = ReceiptKey::for_event("consumer", &event(1));
    let mut conflict = event(1);
    conflict.envelope.payload = json!({"value":88});
    assert!(
        store
            .execute(commit(lease.clone(), 1, vec![event(2), conflict]))
            .await
            .is_err()
    );
    assert_eq!(offset(store).await, 0);
    assert_eq!(
        store
            .execute(FabricCommand::Receipt(key.clone()))
            .await
            .expect("receipt"),
        FabricReply::Receipt(None)
    );
    assert_eq!(
        store
            .execute(FabricCommand::DispatchOutbox { limit: 128 })
            .await
            .expect("drain"),
        FabricReply::Dispatched(0)
    );
    let receipt = store
        .execute(commit(lease.clone(), 1, vec![event(2)]))
        .await
        .expect("commit");
    assert_eq!(offset(store).await, 1);
    assert_eq!(replay(store).await.len(), 1);
    assert_eq!(
        store
            .execute(commit(lease, 2, vec![event(2)]))
            .await
            .expect("retry commit"),
        receipt
    );
    assert!(matches!(
        store
            .execute(FabricCommand::Receipt(key))
            .await
            .expect("receipt"),
        FabricReply::Receipt(Some(_))
    ));
    store
        .execute(FabricCommand::Publish(event(3)))
        .await
        .expect("later publish");
    assert_eq!(
        replay(store).await.len(),
        1,
        "pending outbox is an ordering barrier"
    );
    assert_eq!(
        store
            .execute(FabricCommand::DispatchOutbox { limit: 1 })
            .await
            .expect("drain"),
        FabricReply::Dispatched(1)
    );
    assert_eq!(
        replay(store)
            .await
            .iter()
            .map(|item| item.event.envelope.operator_seq.0)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(
        store
            .execute(FabricCommand::DispatchOutbox { limit: 1 })
            .await
            .expect("drain twice"),
        FabricReply::Dispatched(0)
    );
}

async fn retry_dlq(store: &dyn OperatorFabric) {
    subscribe(store).await;
    store
        .execute(FabricCommand::PublishBatch(vec![event(1), event(2)]))
        .await
        .expect("publish");
    let first = claim(store, 0).await.remove(0).lease;
    assert_eq!(
        store
            .execute(FabricCommand::Retry {
                lease: first.clone(),
                now_ms: 1,
                backoff_ms: 5,
                max_attempts: 2,
                error: "temporary".into()
            })
            .await
            .expect("retry"),
        FabricReply::Retried {
            dead_lettered: false
        }
    );
    assert_eq!(
        store.execute(commit(first, 2, vec![])).await,
        Err(FabricError::Fenced)
    );
    assert!(claim(store, 5).await.is_empty());
    let second = claim(store, 6).await.remove(0).lease;
    assert_eq!(second.delivery_attempt, 2);
    assert_eq!(
        store
            .execute(FabricCommand::Retry {
                lease: second,
                now_ms: 7,
                backoff_ms: 5,
                max_attempts: 2,
                error: "permanent".into()
            })
            .await
            .expect("dlq"),
        FabricReply::Retried {
            dead_lettered: true
        }
    );
    assert_eq!(offset(store).await, 1);
    match store
        .execute(FabricCommand::DeadLetters {
            after: 0,
            limit: 128,
        })
        .await
        .expect("dlq query")
    {
        FabricReply::DeadLetters(items) => {
            assert_eq!(items.len(), 1);
            assert_eq!(items[0].event_id, event(1).envelope.event_id);
            assert_eq!(items[0].lease.delivery_attempt, 2);
            assert_eq!(items[0].error, "permanent");
            assert_eq!(items[0].dead_lettered_at_ms, 7);
        }
        other => panic!("unexpected {other:?}"),
    }
    assert_eq!(claim(store, 7).await[0].event, event(2));
}

async fn filters_and_receipts(store: &dyn OperatorFabric) {
    subscribe(store).await;
    let mut filtered = subscription("filtered");
    filtered.event_type = Some("test.other".into());
    store
        .execute(FabricCommand::Subscribe(filtered.clone()))
        .await
        .expect("filtered");
    let mut second = event(2);
    second.envelope.event_type = "test.other".into();
    store
        .execute(FabricCommand::PublishBatch(vec![event(1), second.clone()]))
        .await
        .expect("publish");
    let claims = claim(store, 0).await;
    assert_eq!(claims.len(), 2);
    let filter_claim = claims
        .iter()
        .find(|item| item.lease.subscription == "filtered")
        .expect("filtered claim");
    assert_eq!(filter_claim.lease.offset, 1);
    assert_eq!(filter_claim.event, second);
    store
        .execute(commit(filter_claim.lease.clone(), 1, vec![]))
        .await
        .expect("filtered ack");
    let main_claim = claims
        .iter()
        .find(|item| item.lease.subscription == "sub")
        .expect("main claim");
    store
        .execute(commit(main_claim.lease.clone(), 1, vec![]))
        .await
        .expect("main ack");
    let duplicate = claim(store, 2).await.remove(0);
    assert!(
        duplicate.committed.is_some(),
        "consumer receipt spans subscriptions"
    );
    store
        .execute(commit(duplicate.lease, 3, vec![]))
        .await
        .expect("ack already committed");
    filtered.topic = "different".into();
    assert_eq!(
        store.execute(FabricCommand::Subscribe(filtered)).await,
        Err(FabricError::Conflict)
    );
}

async fn bounds(store: &dyn OperatorFabric) {
    subscribe(store).await;
    let mut oversized = event(1);
    oversized.payload_schema = json!({"type":"object"});
    oversized.envelope.payload = json!({"value":"x".repeat(MAX_EVENT_BYTES)});
    assert!(
        store
            .execute(FabricCommand::Publish(oversized))
            .await
            .is_err()
    );
    let mut large = event(1);
    large.payload_schema = json!({"type":"object"});
    large.envelope.payload = json!({"value":"x".repeat(32 * 1024)});
    assert!(
        store
            .execute(FabricCommand::PublishBatch(vec![large; 32]))
            .await
            .is_err()
    );
    let mut cross_partition = event(2);
    cross_partition.envelope.tenant_id = "other".into();
    assert!(
        store
            .execute(FabricCommand::PublishBatch(vec![event(1), cross_partition]))
            .await
            .is_err()
    );
    assert!(
        store
            .execute(FabricCommand::PublishBatch(vec![
                event(1);
                MAX_BATCH_ITEMS + 1
            ]))
            .await
            .is_err()
    );
    assert!(
        store
            .execute(FabricCommand::PublishBatch(vec![]))
            .await
            .is_err()
    );
    assert!(
        store
            .execute(FabricCommand::Claim {
                worker: "worker".into(),
                now_ms: 0,
                lease_ms: 0,
                limit: 1,
                max_attempts: 3
            })
            .await
            .is_err()
    );
    assert!(replay(store).await.is_empty());
    let caps = store.capabilities();
    assert!(
        caps.atomic_batch
            && caps.continuous_offset
            && caps.lease_fencing
            && caps.transactional_outbox
    );
}

async fn stable_offsets_across_partitions(store: &dyn OperatorFabric) {
    subscribe(store).await;
    store
        .execute(FabricCommand::Publish(event(1)))
        .await
        .expect("publish");
    let lease = claim(store, 0).await.remove(0).lease;
    store
        .execute(commit(lease, 1, vec![event(2)]))
        .await
        .expect("pending outbox");
    let mut independent = event(1);
    independent.envelope.event_id = EventId::stable("p1", "independent");
    independent.envelope.tenant_id = "independent".into();
    store
        .execute(FabricCommand::Publish(independent))
        .await
        .expect("other partition");
    let before = replay(store).await;
    store
        .execute(FabricCommand::DispatchOutbox { limit: 1 })
        .await
        .expect("dispatch");
    let after = replay(store).await;
    for item in before {
        assert!(
            after.contains(&item),
            "replay offset must never change after publication"
        );
    }
}

async fn outbox_batch_visibility(store: &dyn OperatorFabric) {
    subscribe(store).await;
    store
        .execute(FabricCommand::Publish(event(1)))
        .await
        .expect("publish");
    let lease = claim(store, 0).await.remove(0).lease;
    store
        .execute(commit(lease, 1, vec![event(2), event(3)]))
        .await
        .expect("outbox batch");
    assert_eq!(replay(store).await.len(), 1);
    assert_eq!(
        store
            .execute(FabricCommand::DispatchOutbox { limit: 1 })
            .await
            .expect("dispatch one atomic batch"),
        FabricReply::Dispatched(2)
    );
    assert_eq!(replay(store).await.len(), 3);
}

async fn crashed_workers_eventually_dlq(store: &dyn OperatorFabric) {
    subscribe(store).await;
    store
        .execute(FabricCommand::Publish(event(1)))
        .await
        .expect("publish");
    for now in [0, 10, 20] {
        assert_eq!(claim(store, now).await.len(), 1);
    }
    assert!(
        claim(store, 30).await.is_empty(),
        "expired workers cannot retry forever"
    );
    assert_eq!(offset(store).await, 1);
    assert!(
        matches!(store.execute(FabricCommand::DeadLetters { after: 0, limit: 1 }).await.expect("crash dlq"), FabricReply::DeadLetters(items) if items.len() == 1 && items[0].lease.delivery_attempt == 3)
    );
}

macro_rules! contracts {
    ($($name:ident),+ $(,)?) => {
        mod memory {
            use super::*;
            $(#[tokio::test] async fn $name() { super::$name(&MemoryFabric::default()).await; })+
        }
        #[cfg(feature = "sqlite")]
        mod sqlite {
            $(#[tokio::test] async fn $name() {
                let directory = tempfile::tempdir().expect("tempdir");
                let store = agent_extension::store::SqliteEventStore::open(directory.path().join("events.db"), "p1").await.expect("open");
                super::$name(&store).await;
            })+
        }
    };
}

contracts!(
    single_publish,
    batch_atomic,
    holes_and_order,
    fencing,
    transaction,
    retry_dlq,
    filters_and_receipts,
    bounds,
    stable_offsets_across_partitions,
    outbox_batch_visibility,
    crashed_workers_eventually_dlq
);

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_reopen_kill_points() {
    use agent_extension::store::SqliteEventStore;
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("events.db");
    let store = SqliteEventStore::open(&path, "p1").await.expect("open");
    subscribe(&store).await;
    store
        .execute(FabricCommand::Publish(event(1)))
        .await
        .expect("publish");
    let old = claim(&store, 0).await.remove(0).lease;
    let mut bad = event(3);
    bad.envelope.payload = json!(null);
    assert!(
        store
            .execute(commit(old.clone(), 1, vec![event(2), bad]))
            .await
            .is_err()
    );
    drop(store);
    let store = SqliteEventStore::open(&path, "p1")
        .await
        .expect("reopen before commit");
    assert_eq!(offset(&store).await, 0);
    assert_eq!(replay(&store).await.len(), 1);
    assert_eq!(
        store
            .execute(FabricCommand::Receipt(ReceiptKey::for_event(
                "consumer",
                &event(1)
            )))
            .await
            .expect("no receipt"),
        FabricReply::Receipt(None)
    );
    let lease = claim(&store, 10).await.remove(0).lease;
    assert!(lease.epoch > old.epoch);
    assert_eq!(
        store.execute(commit(old, 11, vec![])).await,
        Err(FabricError::Fenced)
    );
    store
        .execute(commit(lease, 11, vec![event(2)]))
        .await
        .expect("commit before dispatch");
    drop(store);
    let store = SqliteEventStore::open(&path, "p1")
        .await
        .expect("reopen after commit");
    assert_eq!(offset(&store).await, 1);
    assert_eq!(replay(&store).await.len(), 1);
    assert!(matches!(
        store
            .execute(FabricCommand::Receipt(ReceiptKey::for_event(
                "consumer",
                &event(1)
            )))
            .await
            .expect("receipt survived"),
        FabricReply::Receipt(Some(_))
    ));
    assert_eq!(
        store
            .execute(FabricCommand::DispatchOutbox { limit: 1 })
            .await
            .expect("recover outbox"),
        FabricReply::Dispatched(1)
    );
    drop(store);
    let store = SqliteEventStore::open(&path, "p1")
        .await
        .expect("reopen dispatched");
    assert_eq!(replay(&store).await.len(), 2);
    assert_eq!(
        store
            .execute(FabricCommand::DispatchOutbox { limit: 1 })
            .await
            .expect("no duplicate"),
        FabricReply::Dispatched(0)
    );
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_sql_write_before_commit_failure_rolls_back_everything() {
    use agent_extension::store::SqliteEventStore;
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("events.db");
    let store = SqliteEventStore::open(&path, "p1").await.expect("open");
    subscribe(&store).await;
    store
        .execute(FabricCommand::Publish(event(1)))
        .await
        .expect("publish");
    let lease = claim(&store, 0).await.remove(0).lease;
    let injector = rusqlite::Connection::open(&path).expect("independent connection");
    injector.execute_batch("CREATE TABLE fault_parent (id INTEGER PRIMARY KEY);
        CREATE TABLE fault_child (parent INTEGER REFERENCES fault_parent(id) DEFERRABLE INITIALLY DEFERRED);
        CREATE TRIGGER fault_before_commit AFTER UPDATE ON workflow_operator_fabric_v1
        BEGIN INSERT INTO fault_child(parent) VALUES(1); END;").expect("inject real deferred commit failure");
    assert!(matches!(
        store
            .execute(commit(lease, 1, vec![event(2), event(3)]))
            .await,
        Err(FabricError::Storage(_))
    ));
    let count: i64 = injector
        .query_row("SELECT COUNT(*) FROM fault_child", [], |row| row.get(0))
        .expect("rollback side write");
    assert_eq!(count, 0);
    drop(injector);
    drop(store);
    let store = SqliteEventStore::open(&path, "p1")
        .await
        .expect("reopen after SQL commit failure");
    assert_eq!(offset(&store).await, 0);
    assert_eq!(replay(&store).await.len(), 1);
    assert_eq!(
        store
            .execute(FabricCommand::Receipt(ReceiptKey::for_event(
                "consumer",
                &event(1)
            )))
            .await
            .expect("receipt absent"),
        FabricReply::Receipt(None)
    );
    let injector = rusqlite::Connection::open(&path).expect("remove fault");
    injector
        .execute_batch("DROP TRIGGER fault_before_commit")
        .expect("remove trigger");
    drop(injector);
    assert_eq!(
        store
            .execute(FabricCommand::DispatchOutbox { limit: 1 })
            .await
            .expect("outbox absent"),
        FabricReply::Dispatched(0)
    );
    let lease = claim(&store, 10).await.remove(0).lease;
    store
        .execute(commit(lease, 11, vec![event(2), event(3)]))
        .await
        .expect("retry real commit");
    drop(store);
    let store = SqliteEventStore::open(&path, "p1")
        .await
        .expect("reopen committed transaction");
    assert_eq!(offset(&store).await, 1);
    assert_eq!(replay(&store).await.len(), 1);
    assert_eq!(
        store
            .execute(FabricCommand::DispatchOutbox { limit: 1 })
            .await
            .expect("recover atomic batch"),
        FabricReply::Dispatched(2)
    );
    assert_eq!(replay(&store).await.len(), 3);
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_independent_connections_serialize_claims_and_publish() {
    use agent_extension::store::SqliteEventStore;
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("events.db");
    let first = SqliteEventStore::open(&path, "first").await.expect("open");
    let second = SqliteEventStore::open(&path, "second")
        .await
        .expect("independent open");
    subscribe(&first).await;
    let command = FabricCommand::PublishBatch(vec![event(1), event(2)]);
    let (left, right) = tokio::join!(first.execute(command.clone()), second.execute(command));
    assert_eq!(left.expect("first publish"), right.expect("second publish"));
    let (left, right) = tokio::join!(claim(&first, 0), claim(&second, 0));
    assert_eq!(left.len() + right.len(), 1);
    assert_eq!(replay(&first).await.len(), 2);
    drop(first);
    drop(second);
    let store = SqliteEventStore::open(&path, "reopen")
        .await
        .expect("reopen");
    assert_eq!(replay(&store).await.len(), 2);
    assert_eq!(claim(&store, 10).await[0].lease.epoch, 2);
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_dlq_survives_reopen() {
    use agent_extension::store::SqliteEventStore;
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("events.db");
    let store = SqliteEventStore::open(&path, "p1").await.expect("open");
    retry_dlq(&store).await;
    drop(store);
    let store = SqliteEventStore::open(&path, "p1")
        .await
        .expect("reopen dlq");
    assert_eq!(offset(&store).await, 1);
    assert!(
        matches!(store.execute(FabricCommand::DeadLetters { after: 0, limit: 1 }).await.expect("durable dlq"), FabricReply::DeadLetters(items) if items.len() == 1 && items[0].error == "permanent")
    );
}
