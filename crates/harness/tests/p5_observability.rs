use agent_core::harness::*;
use agent_extension::{
    pipeline::SqlitePipelineStore,
    store::{FilesystemBlobStore, SqliteRunStore},
};
use agent_harness::pipeline::{PipelineRuntime, PipelineStart};
use futures_util::stream;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use tracing::{
    Event, Subscriber,
    field::{Field, Visit},
    span::{Attributes, Id, Record},
};

#[derive(Clone)]
struct Recorder(Arc<Mutex<Vec<BTreeMap<String, String>>>>);
struct Fields(BTreeMap<String, String>);
impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().into(), format!("{value:?}"));
    }
}
impl Subscriber for Recorder {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }
    fn record(&self, _: &Id, _: &Record<'_>) {}
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn enter(&self, _: &Id) {}
    fn exit(&self, _: &Id) {}
    fn event(&self, event: &Event<'_>) {
        let mut fields = Fields(BTreeMap::new());
        event.record(&mut fields);
        self.0.lock().expect("logs").push(fields.0);
    }
}

struct PrivateModel;
impl ModelPort for PrivateModel {
    fn stream(&self, _: ModelRequest) -> ModelEventStream {
        Box::pin(stream::iter(vec![
            ModelEvent::ReasoningDelta {
                delta: "private-reasoning-marker".into(),
            },
            ModelEvent::TextDelta {
                delta: "private-output-marker".into(),
            },
            ModelEvent::Completed {
                finish_reason: FinishReason::Stop,
            },
        ]))
    }
}

#[test]
fn p5_text_observability_has_latency_batch_and_lag_without_private_content() {
    let logs = Arc::new(Mutex::new(Vec::new()));
    let executor = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    tracing::subscriber::with_default(Recorder(logs.clone()), || {
        executor.block_on(async {
            let directory = tempfile::tempdir().expect("temp");
            let runtime = PipelineRuntime::new(
                Arc::new(
                    SqlitePipelineStore::open(directory.path().join("ledger.db")).expect("ledger"),
                ),
                Arc::new(
                    SqliteRunStore::open(directory.path().join("runs.db"))
                        .await
                        .expect("runs"),
                ),
                Arc::new(FilesystemBlobStore::new(directory.path().join("blobs"))),
                Arc::new(PrivateModel),
            );
            let run_id = RunId::new();
            runtime
                .admit(PipelineStart::text(run_id, "private-input-marker", "fake"))
                .await
                .expect("admit");
            let mut record = runtime
                .store
                .load(run_id)
                .await
                .expect("load")
                .expect("record");
            let expected = record.generation;
            record.generation += 1;
            record.lease_owner = Some("crashed-worker".into());
            record.lease_expires_at_ms = 0;
            runtime
                .store
                .compare_exchange(Some(expected), record)
                .await
                .expect("expired lease");
            runtime.drive(run_id).await.expect("drive");
        })
    });
    let logs = logs.lock().expect("logs");
    for field in [
        "operator_latency_ms",
        "delta_batch_items",
        "delta_batch_bytes",
        "projection_lag_events",
        "lease_reclaim_count",
    ] {
        assert!(
            logs.iter().any(|entry| entry.contains_key(field)),
            "missing metric {field}"
        );
    }
    for entry in logs
        .iter()
        .filter(|entry| entry.contains_key("operator_latency_ms"))
    {
        assert_eq!(
            entry.get("engine").map(String::as_str),
            Some("\"operator_v1\"")
        );
        assert_eq!(
            entry.get("profile").map(String::as_str),
            Some("\"operator_v1_text\"")
        );
    }
    assert!(
        !serde_json::to_string(&*logs)
            .expect("JSON")
            .contains("private-")
    );
}
