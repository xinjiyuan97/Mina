use agent_core::{
    agent::{artifacts::FabricBlobStore, memory_fabric::MemoryFabric},
    contracts::fabric::*,
    contracts::*,
};
use std::sync::Arc;
async fn put(blobs: &dyn BlobStore, value: serde_json::Value, created_at_ms: i64) -> BlobId {
    blobs
        .put(PutBlob {
            media_type: "application/json".into(),
            name: None,
            data: serde_json::to_vec(&value).unwrap(),
            created_at_ms,
        })
        .await
        .unwrap()
        .blob_id
}
#[tokio::test]
async fn embedded_gc_protects_previous_chains_external_roots_and_young_blobs() {
    let fabric = Arc::new(MemoryFabric::default());
    let blobs = FabricBlobStore(fabric.clone());
    let attachment = put(&blobs, serde_json::json!({"image":"data"}), 0).await;
    let first = put(&blobs, serde_json::json!({"attachment":attachment}), 0).await;
    let head = put(&blobs, serde_json::json!({"previous":first}), 0).await;
    let orphan = put(&blobs, serde_json::json!({"orphan":true}), 0).await;
    let young = put(&blobs, serde_json::json!({"previous":head}), 990).await;
    let command = |dry_run| FabricCommand::CollectArtifacts {
        now_ms: 1000,
        grace_ms: 100,
        external_roots: vec![attachment],
        dry_run,
    };
    let FabricReply::Collected(report) = fabric.execute(command(true)).await.unwrap() else {
        panic!("report");
    };
    assert_eq!(report.artifacts, 1);
    assert!(blobs.get(orphan).await.unwrap().is_some());
    fabric.execute(command(false)).await.unwrap();
    assert!(blobs.get(orphan).await.unwrap().is_none());
    for id in [attachment, first, head, young] {
        assert!(blobs.get(id).await.unwrap().is_some());
    }
    fabric
        .execute(FabricCommand::CollectArtifacts {
            now_ms: 2000,
            grace_ms: 100,
            external_roots: vec![head],
            dry_run: false,
        })
        .await
        .unwrap();
    assert!(blobs.get(young).await.unwrap().is_none());
    for id in [attachment, first, head] {
        assert!(blobs.get(id).await.unwrap().is_some());
    }
}

#[tokio::test]
async fn legacy_orphan_chunks_require_observation_then_grace() {
    let fabric = MemoryFabric::default();
    let id = BlobId::new();
    let key = format!("pipeline-artifact:{id}:0");
    fabric
        .execute(FabricCommand::Transact {
            checkpoint: CheckpointWrite {
                key: key.clone(),
                expected: None,
                value: serde_json::json!("e30="),
            },
            outbox: vec![],
            delivery: None,
        })
        .await
        .unwrap();
    let collect = |now_ms, dry_run| FabricCommand::CollectArtifacts {
        now_ms,
        grace_ms: 100,
        external_roots: vec![],
        dry_run,
    };
    let FabricReply::Collected(preview) = fabric.execute(collect(0, true)).await.unwrap() else {
        panic!("report")
    };
    assert_eq!(preview.quarantined_artifacts, 1);
    // Dry run must not start the quarantine clock.
    fabric.execute(collect(1000, false)).await.unwrap();
    fabric.execute(collect(1099, false)).await.unwrap();
    assert!(matches!(
        fabric
            .execute(FabricCommand::ReadCheckpoint(key.clone()))
            .await
            .unwrap(),
        FabricReply::Checkpoint(Some(_))
    ));
    fabric.execute(collect(1100, false)).await.unwrap();
    assert!(matches!(
        fabric
            .execute(FabricCommand::ReadCheckpoint(key))
            .await
            .unwrap(),
        FabricReply::Checkpoint(None)
    ));
}

#[tokio::test]
async fn incomplete_upload_requires_age_and_no_reference() {
    let fabric = Arc::new(MemoryFabric::default());
    let id = BlobId::new();
    fabric.execute(FabricCommand::Transact {
        checkpoint: CheckpointWrite {
            key: format!("pipeline-artifact:{id}"), expected: None,
            value: serde_json::json!({"complete":false,"schema_version":1,"chunks":1,
                "metadata":{"blob_id":id,"media_type":"application/json","size_bytes":10,"created_at_ms":1000}}),
        }, outbox: vec![], delivery: None,
    }).await.unwrap();
    assert!(FabricBlobStore(fabric.clone()).get(id).await.is_err());
    let collect = |now_ms, external_roots| FabricCommand::CollectArtifacts {
        now_ms,
        grace_ms: 100,
        external_roots,
        dry_run: false,
    };
    assert!(fabric.execute(collect(1050, vec![])).await.is_err());
    assert!(fabric.execute(collect(1200, vec![id])).await.is_err());
    let FabricReply::Collected(report) = fabric.execute(collect(1200, vec![])).await.unwrap()
    else {
        panic!("report")
    };
    assert_eq!(report.artifacts, 1);
    assert!(FabricBlobStore(fabric).get(id).await.unwrap().is_none());
}

#[tokio::test]
async fn malformed_reachable_embedded_json_prevents_sweep() {
    let fabric = Arc::new(MemoryFabric::default());
    let blobs = FabricBlobStore(fabric.clone());
    let broken = blobs
        .put(PutBlob {
            media_type: "application/json".into(),
            name: None,
            data: b"not json".to_vec(),
            created_at_ms: 0,
        })
        .await
        .unwrap()
        .blob_id;
    let orphan = put(&blobs, serde_json::json!({}), 0).await;
    assert!(
        fabric
            .execute(FabricCommand::CollectArtifacts {
                now_ms: 1000,
                grace_ms: 100,
                external_roots: vec![broken],
                dry_run: false
            })
            .await
            .is_err()
    );
    assert!(blobs.get(orphan).await.unwrap().is_some());
}
