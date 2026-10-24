use agent_core::{harness::*, pipeline::*};
use agent_extension::{
    pipeline::SqlitePipelineStore,
    store::{FilesystemBlobStore, SqliteRunStore},
};
use agent_harness::pipeline::{PipelineRuntime, PipelineStart};
use std::sync::{Arc, Mutex};

struct Capture(Mutex<Vec<ModelRequest>>);
impl ModelPort for Capture {
    fn stream(&self, request: ModelRequest) -> ModelEventStream {
        self.0.lock().expect("capture").push(request);
        Box::pin(futures_util::stream::iter([ModelEvent::Completed {
            finish_reason: FinishReason::Stop,
        }]))
    }
}

#[tokio::test]
async fn p6_review_actual_p5_ledger_reducer_artifact_digest() {
    let directory = tempfile::tempdir().expect("temp");
    let database = directory.path().join("old.db");
    let connection = rusqlite::Connection::open(&database).expect("raw database");
    connection
        .execute_batch("PRAGMA foreign_keys=OFF;")
        .expect("restore dump ordering");
    connection
        .execute_batch(include_str!("fixtures/p5/pending.sql"))
        .expect("original P5 writer records");
    drop(connection);
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/p5/pending.json")).expect("fixture");
    let blob_path = directory.path().join("blobs");
    std::fs::create_dir(&blob_path).expect("blob directory");
    for object in fixture["blobs"].as_array().expect("blobs") {
        let metadata: BlobMetadata =
            serde_json::from_value(object["metadata"].clone()).expect("metadata");
        std::fs::write(
            blob_path.join(format!("{}.json", metadata.blob_id)),
            serde_json::to_vec(&metadata).expect("metadata"),
        )
        .expect("write metadata");
        std::fs::write(
            blob_path.join(format!("{}.blob", metadata.blob_id)),
            object["text"].as_str().expect("original bytes"),
        )
        .expect("write bytes");
    }
    let blobs = Arc::new(FilesystemBlobStore::new(blob_path));
    let store = Arc::new(SqlitePipelineStore::open(&database).expect("open old ledger"));
    let runs = Arc::new(
        SqliteRunStore::open(&database)
            .await
            .expect("open old runs"),
    );
    let model = Arc::new(Capture(Mutex::new(vec![])));
    let runtime = PipelineRuntime::new(store.clone(), runs, blobs, model.clone());
    for original in fixture["records"].as_array().expect("records") {
        let start: AgentLoopStart =
            serde_json::from_value(original["start"].clone()).expect("old start");
        assert!(
            start
                .attachments
                .iter()
                .all(|attachment| attachment.size_bytes.is_none())
        );
        let record = store
            .load(start.run_id)
            .await
            .expect("load")
            .expect("record");
        assert_eq!(
            record.start_digest,
            original["record"]["start_digest"]
                .as_str()
                .expect("fixed old digest")
        );
        assert_eq!(record.ledger.phase, RunPhase::AwaitingModel);
        let mut replay = PipelineStart::text(start.run_id, "", "");
        replay.start = start.clone();
        runtime
            .admit(replay.clone())
            .await
            .expect("replay original digest without rewriting");
        if !start.attachments.is_empty() {
            for change in 0..4 {
                let mut changed = replay.clone();
                let attachment = &mut changed.start.attachments[0];
                match change {
                    0 => attachment.blob_id = BlobId::new(),
                    1 => attachment.name = None,
                    2 => attachment.media_type = "image/jpeg".into(),
                    _ => attachment.size_bytes = Some(8),
                }
                assert!(matches!(
                    runtime.admit(changed).await,
                    Err(PipelineError::Conflict)
                ));
            }
        }
        runtime
            .drive(start.run_id)
            .await
            .expect("recover original pending artifact");
        {
            let captured = model.0.lock().expect("capture");
            let request = captured.last().expect("request");
            assert_eq!(request.model, "p5-pinned-model");
            assert_eq!(
                request.messages.last().expect("user").attachments,
                start.attachments
            );
        }
        assert_eq!(
            store
                .load(start.run_id)
                .await
                .expect("load")
                .expect("record")
                .start_digest,
            record.start_digest
        );
    }
}
