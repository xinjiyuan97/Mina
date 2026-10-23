use agent_core::{harness::*, operator_fabric::*};
use agent_extension::store::SqliteEventStore;
use agent_harness::pipeline::{PipelineStart, ToolsPipelineRuntime};
use std::sync::Arc;
use tokio::sync::Notify;

struct StreamingModel {
    started: Arc<Notify>,
    release: Arc<Notify>,
}
impl ModelPort for StreamingModel {
    fn stream(&self, _: ModelRequest) -> ModelEventStream {
        let started = self.started.clone();
        let release = self.release.clone();
        Box::pin(async_stream::stream! {
            yield ModelEvent::TextDelta { delta: "before seal".into() };
            started.notify_one();
            release.notified().await;
            yield ModelEvent::Completed { finish_reason: FinishReason::Stop };
        })
    }
}
struct NoTools;
impl ToolPort for NoTools {
    fn definitions(&self) -> Vec<ToolDefinition> {
        vec![]
    }
    fn call(&self, _: ToolCallRequest) -> ToolCallFuture {
        panic!("no tools expected")
    }
}

struct CorruptFinal(Arc<dyn BlobStore>);
impl BlobStore for CorruptFinal {
    fn put(&self, command: PutBlob) -> BlobStoreFuture<'_, BlobMetadata> {
        self.0.put(command)
    }
    fn get(&self, id: BlobId) -> BlobStoreFuture<'_, Option<BlobObject>> {
        Box::pin(async move {
            let mut blob = self.0.get(id).await?;
            if let Some(blob) = &mut blob
                && let Ok(mut value) = serde_json::from_slice::<serde_json::Value>(&blob.data)
                && value.get("journal").is_some()
            {
                value["coverage_digest"] = serde_json::json!("corrupt");
                blob.data = serde_json::to_vec(&value).expect("JSON");
            }
            Ok(blob)
        })
    }
}

#[tokio::test]
async fn corrupt_final_artifact_produces_durable_output_failed_not_an_unsealed_success() {
    let directory = tempfile::tempdir().expect("temp");
    let store = Arc::new(
        SqliteEventStore::open(directory.path().join("events.db"), "p4")
            .await
            .expect("open"),
    );
    let release = Arc::new(Notify::new());
    release.notify_one();
    let runtime = ToolsPipelineRuntime::new(
        store.clone(),
        Arc::new(StreamingModel {
            started: Arc::new(Notify::new()),
            release,
        }),
        Arc::new(NoTools),
    )
    .with_blobs(Arc::new(CorruptFinal(Arc::new(
        agent_extension::store::FilesystemBlobStore::new(directory.path().join("blobs")),
    ))));
    let run_id = RunId::new();
    runtime
        .admit(
            "local",
            PipelineStart::text(run_id, "corrupt", "fake").start,
            0,
        )
        .await
        .expect("admit");
    for _ in 0..25 {
        if runtime
            .tick("local", run_id, || 1)
            .await
            .expect("output gap becomes durable failure")
            == 0
        {
            break;
        }
    }
    let run = runtime.load("local", run_id).await.expect("run");
    assert!(matches!(
        run.reducer.outcome(),
        AgentLoopOutcome::Failed { .. }
    ));
    let route = run
        .routes
        .values()
        .find(|route| route.consumer == "model")
        .expect("model");
    let FabricReply::Replay(events) = store
        .execute(FabricCommand::ScanEvents {
            topic: route.topic.clone(),
            event_type: "output.failed".into(),
            after: 0,
            limit: 10,
        })
        .await
        .expect("failure")
    else {
        panic!("reply");
    };
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event.envelope.payload["code"], "output_gap");
}

#[tokio::test]
async fn tools_profile_streams_durable_batches_before_final_and_seals_with_p3_coverage() {
    let directory = tempfile::tempdir().expect("temp");
    let store = Arc::new(
        SqliteEventStore::open(directory.path().join("events.db"), "p4")
            .await
            .expect("open"),
    );
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let runtime = Arc::new(ToolsPipelineRuntime::new(
        store.clone(),
        Arc::new(StreamingModel {
            started: started.clone(),
            release: release.clone(),
        }),
        Arc::new(NoTools),
    ));
    let run_id = RunId::new();
    runtime
        .admit(
            "local",
            PipelineStart::text(run_id, "stream", "fake").start,
            0,
        )
        .await
        .expect("admit");
    let worker = tokio::spawn({
        let runtime = runtime.clone();
        async move {
            for _ in 0..30 {
                if runtime.tick("local", run_id, || 1).await.expect("tick") == 0 {
                    break;
                }
            }
        }
    });
    started.notified().await;
    let streaming = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let run = runtime.load("local", run_id).await.expect("load");
            if run.public_events.iter().any(|event| matches!(&event.kind, RunEventKind::OutputDelta { delta, .. } if delta == "before seal")) { break run; }
            tokio::task::yield_now().await;
        }
    }).await;
    release.notify_one();
    worker.await.expect("worker");
    let streaming = streaming.expect("durable projection must advance while provider is blocked");
    assert!(
        !streaming
            .public_events
            .iter()
            .any(|event| event.kind.is_terminal())
    );
    let run = runtime.load("local", run_id).await.expect("terminal");
    assert!(run.public_events.last().expect("last").kind.is_terminal());
    let route = run
        .routes
        .values()
        .find(|route| route.consumer == "model")
        .expect("model route");
    let FabricReply::Replay(batches) = store
        .execute(FabricCommand::ScanEvents {
            topic: route.topic.clone(),
            event_type: "output.batch".into(),
            after: 0,
            limit: 20,
        })
        .await
        .expect("batches")
    else {
        panic!("reply")
    };
    assert!(!batches.is_empty());
    let batch: agent_core::pipeline::OutputBatch = serde_json::from_value(
        batches
            .last()
            .expect("batch")
            .event
            .envelope
            .payload
            .clone(),
    )
    .expect("P3 batch contract");
    let FabricReply::Replay(seals) = store
        .execute(FabricCommand::ScanEvents {
            topic: route.topic.clone(),
            event_type: "output.sealed".into(),
            after: 0,
            limit: 20,
        })
        .await
        .expect("seal")
    else {
        panic!("reply")
    };
    assert_eq!(seals.len(), 1);
    assert_eq!(
        seals[0].event.envelope.payload["coverage_digest"],
        batch.coverage_digest
    );
    assert_eq!(
        seals[0].event.envelope.payload["final_delta_seq"],
        batch.last_seq
    );
    runtime
        .ensure_deadline("local", run_id, 2)
        .await
        .expect("deadline");
    runtime
        .reconcile_control("local", run_id, 3)
        .await
        .expect("late timeout");
    assert_eq!(
        runtime
            .load("local", run_id)
            .await
            .expect("unchanged")
            .public_events,
        run.public_events
    );
}

#[tokio::test]
async fn deadline_fences_blocked_stream_then_reopen_does_not_restart_provider() {
    let directory = tempfile::tempdir().expect("temp");
    let path = directory.path().join("stream.db");
    let run_id = RunId::new();
    let terminal;
    {
        let store = Arc::new(
            SqliteEventStore::open(&path, "stream")
                .await
                .expect("fixture operation"),
        );
        let started = Arc::new(Notify::new());
        let runtime = Arc::new(ToolsPipelineRuntime::new(
            store,
            Arc::new(StreamingModel {
                started: started.clone(),
                release: Arc::new(Notify::new()),
            }),
            Arc::new(NoTools),
        ));
        runtime
            .admit(
                "local",
                PipelineStart::text(run_id, "blocked", "fake").start,
                0,
            )
            .await
            .expect("fixture operation");
        runtime
            .ensure_deadline("local", run_id, 10)
            .await
            .expect("fixture operation");
        let worker = tokio::spawn({
            let runtime = runtime.clone();
            async move {
                for _ in 0..30 {
                    runtime
                        .tick("local", run_id, || 1)
                        .await
                        .expect("fixture operation");
                }
            }
        });
        started.notified().await;
        runtime
            .reconcile_control("local", run_id, 10)
            .await
            .expect("fixture operation");
        let run = runtime
            .load("local", run_id)
            .await
            .expect("fixture operation");
        assert!(
            matches!(run.reducer.outcome(), AgentLoopOutcome::Failed { ref error } if error.code == "run_timeout")
        );
        terminal = run.public_events;
        worker.abort();
        let _ = worker.await;
    }
    struct MustNotRestart;
    impl ModelPort for MustNotRestart {
        fn stream(&self, _: ModelRequest) -> ModelEventStream {
            panic!("terminal run restarted model")
        }
    }
    let store = Arc::new(
        SqliteEventStore::open(&path, "stream")
            .await
            .expect("fixture operation"),
    );
    let runtime = ToolsPipelineRuntime::new(store, Arc::new(MustNotRestart), Arc::new(NoTools));
    for _ in 0..20 {
        runtime
            .tick("local", run_id, || 200_000)
            .await
            .expect("fixture operation");
    }
    assert_eq!(
        runtime
            .load("local", run_id)
            .await
            .expect("fixture operation")
            .public_events,
        terminal
    );
}

#[tokio::test]
async fn crash_after_durable_batch_recovers_prefix_without_repeating_provider() {
    let directory = tempfile::tempdir().expect("fixture operation");
    let path = directory.path().join("interrupted.db");
    let run_id = RunId::new();
    let prefix;
    {
        let store = Arc::new(
            SqliteEventStore::open(&path, "interrupted")
                .await
                .expect("fixture operation"),
        );
        let started = Arc::new(Notify::new());
        let runtime = Arc::new(ToolsPipelineRuntime::new(
            store,
            Arc::new(StreamingModel {
                started: started.clone(),
                release: Arc::new(Notify::new()),
            }),
            Arc::new(NoTools),
        ));
        runtime
            .admit(
                "local",
                PipelineStart::text(run_id, "crash", "fake").start,
                0,
            )
            .await
            .expect("fixture operation");
        let worker = tokio::spawn({
            let runtime = runtime.clone();
            async move {
                for _ in 0..30 {
                    runtime
                        .tick("local", run_id, || 1)
                        .await
                        .expect("fixture operation");
                }
            }
        });
        started.notified().await;
        prefix = tokio::time::timeout(std::time::Duration::from_secs(3), async {
            loop {
                let run = runtime
                    .load("local", run_id)
                    .await
                    .expect("fixture operation");
                if run
                    .public_events
                    .iter()
                    .any(|event| matches!(event.kind, RunEventKind::OutputDelta { .. }))
                {
                    break run.public_events;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("fixture operation");
        worker.abort();
        let _ = worker.await;
    }
    struct MustNotRepeat;
    impl ModelPort for MustNotRepeat {
        fn stream(&self, _: ModelRequest) -> ModelEventStream {
            panic!("uncertain provider call repeated")
        }
    }
    let store = Arc::new(
        SqliteEventStore::open(&path, "interrupted")
            .await
            .expect("fixture operation"),
    );
    let runtime = ToolsPipelineRuntime::new(store, Arc::new(MustNotRepeat), Arc::new(NoTools));
    for _ in 0..30 {
        runtime
            .tick("local", run_id, || 200_000)
            .await
            .expect("fixture operation");
    }
    let recovered = runtime
        .load("local", run_id)
        .await
        .expect("fixture operation");
    assert!(recovered.public_events.starts_with(&prefix));
    assert!(matches!(
        recovered
            .public_events
            .last()
            .expect("fixture operation")
            .kind,
        RunEventKind::RunFailed { .. }
    ));
    assert_eq!(
        recovered
            .public_events
            .iter()
            .filter(|event| matches!(event.kind, RunEventKind::OutputDelta { .. }))
            .count(),
        1
    );
}
