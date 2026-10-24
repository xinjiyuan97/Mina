use super::*;

async fn corrupt_recovery(kind: &str) {
    let directory = tempfile::tempdir().expect("temp");
    let (sender, mut received) = tokio::sync::mpsc::unbounded_channel();
    let (state, worker) = text_state(directory.path(), sender).await;
    let bad = RunId::new();
    let good = RunId::new();
    pending(&state, bad, "bad", vec![], false).await;
    pending(&state, good, "good", vec![], false).await;
    worker.abort();
    let _ = worker.await;
    let connection =
        rusqlite::Connection::open(directory.path().join("events.db")).expect("database");
    let encoded: String = connection
        .query_row(
            "SELECT state_json FROM workflow_operator_fabric_v1 WHERE singleton = 1",
            [],
            |row| row.get(0),
        )
        .expect("fabric state");
    let mut fabric: Value = serde_json::from_str(&encoded).expect("state json");
    let event = fabric["events"]
        .as_array_mut()
        .expect("events")
        .iter_mut()
        .find(|item| {
            item["event"]["envelope"]["run_id"] == json!(bad)
                && item["event"]["envelope"]["event_type"] == "run.requested"
        })
        .expect("bad request event");
    let payload = &mut event["event"]["envelope"]["payload"];
    if kind == "payload" {
        *payload = json!({"invalid":"P6_PRIVATE_CORRUPT_SENTINEL"});
        connection
            .execute(
                "UPDATE workflow_operator_fabric_v1 SET state_json = ?1 WHERE singleton = 1",
                [serde_json::to_string(&fabric).expect("serialize")],
            )
            .expect("inject invalid durable payload");
    } else {
        let reference = payload["request_ref"].as_str().expect("reference");
        let path = directory
            .path()
            .join("blobs")
            .join(format!("{reference}.blob"));
        if kind == "missing" {
            std::fs::remove_file(path).expect("inject missing artifact");
        } else {
            let length = std::fs::metadata(&path).expect("metadata").len() as usize;
            std::fs::write(path, vec![b'!'; length])
                .expect("inject same-length invalid artifact JSON");
        }
    }
    drop(connection);
    let error = admission::recover_once(&state)
        .await
        .expect_err("scan must report the corrupt run");
    assert!(!error.to_string().contains("P6_PRIVATE_CORRUPT_SENTINEL"));
    let request = tokio::time::timeout(Duration::from_secs(2), received.recv())
        .await
        .expect("healthy later run must execute despite enumeration corruption")
        .expect("model request");
    assert_eq!(request.run_id, good);
    assert!(received.try_recv().is_err(), "corrupt run must not execute");
    assert!(
        state
            .pipeline
            .as_ref()
            .expect("pipeline")
            .store
            .load(bad)
            .await
            .expect("load")
            .expect("record")
            .lease_owner
            .is_none()
    );
}

#[tokio::test]
async fn p6_second_review_corrupt_payload_isolated() {
    corrupt_recovery("payload").await;
}
#[tokio::test]
async fn p6_second_review_missing_artifact_isolated() {
    corrupt_recovery("missing").await;
}
#[tokio::test]
async fn p6_second_review_corrupt_artifact_isolated() {
    corrupt_recovery("artifact").await;
}

async fn legacy_name(stored_name: bool, explicit_null: bool, offline: bool) {
    let directory = tempfile::tempdir().expect("temp");
    let (mut state, worker, session, _received) = session_fixture(directory.path()).await;
    let template = RunId::new();
    prepare_run_plan(&state, prepare_request(template, "template", vec![]))
        .await
        .expect("template");
    let mut saved = state
        .admission
        .request(template)
        .await
        .expect("read")
        .expect("request");
    let name = "legacy.png";
    let blob = state
        .blobs
        .put(PutBlob {
            media_type: "image/png".into(),
            name: stored_name.then(|| name.into()),
            data: vec![1, 2, 3],
            created_at_ms: 0,
        })
        .await
        .expect("blob");
    saved.run_id = RunId::new();
    saved.session_id = Some(session.session_id);
    saved.input = String::new();
    saved.max_steps = 8;
    saved.attachment_names = None;
    saved.attachments = vec![ModelAttachment {
        blob_id: blob.blob_id,
        media_type: blob.media_type.clone(),
        name: Some(name.into()),
        size_bytes: Some(blob.size_bytes),
    }];
    saved.request_hash = "legacy-name-hash".into();
    state
        .orchestration
        .sessions
        .begin_run(
            BeginSessionRun {
                session_id: session.session_id,
                run_id: saved.run_id,
                expected_revision: session.revision,
                idempotency_key: "old-name".into(),
                request_hash: saved.request_hash.clone(),
                input: vec![ContentPart::BlobRef {
                    blob_id: blob.blob_id,
                    media_type: blob.media_type,
                    name: Some(name.into()),
                    size_bytes: Some(blob.size_bytes),
                }],
                created_at_ms: 0,
            },
            RunSnapshot::new(saved.run_id, "", 0),
        )
        .await
        .expect("legacy session index");
    let encoded = serde_json::to_value(&saved).expect("legacy representation");
    assert!(encoded.get("attachment_names").is_none());
    state
        .admission
        .publish(saved.clone())
        .await
        .expect("legacy admission");
    if offline {
        state.blobs = admission::test_blobs();
    }
    let mut request = json!({"input":"", "attachments":[{"blob_id":blob.blob_id}],
        "expected_revision":session.revision,"idempotency_key":"old-name"});
    if explicit_null {
        request["attachments"][0]["name"] = Value::Null;
    }
    let (status, body) = response(&state, session.session_id, request).await;
    assert_eq!(
        status,
        if stored_name && !offline {
            StatusCode::ACCEPTED
        } else {
            StatusCode::CONFLICT
        }
    );
    if status == StatusCode::ACCEPTED {
        assert_eq!(body["run_id"], json!(saved.run_id));
    }
    worker.abort();
    let _ = worker.await;
}

#[tokio::test]
async fn p6_second_review_legacy_stored_name_null_replays() {
    legacy_name(true, true, false).await;
}
#[tokio::test]
async fn p6_second_review_legacy_stored_name_omitted_replays() {
    legacy_name(true, false, false).await;
}
#[tokio::test]
async fn p6_second_review_legacy_alias_null_conflicts() {
    legacy_name(false, true, false).await;
}
#[tokio::test]
async fn p6_second_review_legacy_alias_omitted_conflicts() {
    legacy_name(false, false, false).await;
}
#[tokio::test]
async fn p6_second_review_legacy_null_source_unavailable_conflicts() {
    legacy_name(true, true, true).await;
}
#[tokio::test]
async fn p6_second_review_legacy_omitted_source_unavailable_conflicts() {
    legacy_name(true, false, true).await;
}
