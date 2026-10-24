use agent_core::{harness::*, pipeline::*};
use agent_extension::{
    pipeline::{SqlitePipelineStore, bounded_deltas},
    store::{FilesystemBlobStore, SqliteRunStore},
};
use agent_harness::pipeline::{
    ModelOperator, ModelOutputAggregator, PipelineRuntime, PipelineStart,
};
use futures_util::stream;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct FakeModel {
    calls: Arc<AtomicUsize>,
    tool: bool,
}

#[tokio::test]
async fn p6_model_artifact_must_match_durable_reducer_request() {
    let directory = tempfile::tempdir().expect("temp");
    let path = directory.path().join("runs.db");
    let calls = Arc::new(AtomicUsize::new(0));
    let blobs: Arc<dyn BlobStore> =
        Arc::new(FilesystemBlobStore::new(directory.path().join("blobs")));
    let store = Arc::new(SqlitePipelineStore::open(&path).expect("ledger"));
    let runs = Arc::new(SqliteRunStore::open(&path).await.expect("runs"));
    let runtime = PipelineRuntime::new(
        store.clone(),
        runs,
        blobs.clone(),
        Arc::new(FakeModel {
            calls: calls.clone(),
            tool: false,
        }),
    );
    let run_id = RunId::new();
    let mut start = PipelineStart::text(run_id, "hello", "fake");
    start.start.attachments.push(ModelAttachment {
        blob_id: BlobId::new(),
        media_type: "image/png".into(),
        name: None,
        size_bytes: None,
    });
    runtime.admit(start).await.expect("admit");
    let reference = store
        .load(run_id)
        .await
        .expect("load")
        .expect("record")
        .model_request_ref
        .expect("ref");
    let object = blobs.get(reference).await.expect("get").expect("object");
    let mut changed: ModelRequest = serde_json::from_slice(&object.data).expect("request");
    changed.messages.last_mut().expect("user").attachments[0].blob_id = BlobId::new();
    std::fs::write(
        directory
            .path()
            .join("blobs")
            .join(format!("{reference}.blob")),
        serde_json::to_vec(&changed).expect("serialize"),
    )
    .expect("inject tampered artifact");
    assert!(matches!(
        runtime.drive(run_id).await,
        Err(PipelineError::Conflict)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn p6_pending_model_reopen_reference_only_and_old_text_checkpoint() {
    struct Capture(std::sync::Mutex<Vec<ModelRequest>>);
    impl ModelPort for Capture {
        fn stream(&self, request: ModelRequest) -> ModelEventStream {
            self.0.lock().expect("lock").push(request);
            Box::pin(stream::iter([ModelEvent::Completed {
                finish_reason: FinishReason::Stop,
            }]))
        }
    }
    for multimodal in [false, true] {
        let directory = tempfile::tempdir().expect("temp");
        let path = directory.path().join("runs.db");
        let blobs: Arc<dyn BlobStore> =
            Arc::new(FilesystemBlobStore::new(directory.path().join("blobs")));
        let model = Arc::new(Capture(std::sync::Mutex::new(vec![])));
        let run_id = RunId::new();
        let mut start = PipelineStart::text(run_id, "", "fake");
        if multimodal {
            for name in ["first.png", "second.png"] {
                let blob = blobs
                    .put(PutBlob {
                        media_type: "image/png".into(),
                        name: Some(name.into()),
                        data: b"P6_PRIVATE_BINARY_SENTINEL".to_vec(),
                        created_at_ms: 1,
                    })
                    .await
                    .expect("blob");
                start.start.attachments.push(ModelAttachment {
                    blob_id: blob.blob_id,
                    media_type: blob.media_type,
                    name: blob.name,
                    size_bytes: Some(blob.size_bytes),
                });
            }
        } else {
            let mut old = serde_json::to_value(&start.start).expect("json");
            old.as_object_mut().expect("object").remove("attachments");
            start.start = serde_json::from_value(old).expect("old text start");
        }
        {
            let store = Arc::new(SqlitePipelineStore::open(&path).expect("ledger"));
            let runs = Arc::new(SqliteRunStore::open(&path).await.expect("runs"));
            let runtime = PipelineRuntime::new(store.clone(), runs, blobs.clone(), model.clone());
            runtime
                .admit(start.clone())
                .await
                .expect("admit pending model");
            let record = store.load(run_id).await.expect("load").expect("record");
            assert_eq!(record.ledger.phase, RunPhase::AwaitingModel);
            let reference = record.model_request_ref.expect("request ref");
            let artifact = blobs.get(reference).await.expect("blob").expect("artifact");
            let request: ModelRequest =
                serde_json::from_slice(&artifact.data).expect("request artifact");
            assert_eq!(
                request.messages.last().expect("user").attachments,
                start.start.attachments
            );
            let fact = record
                .facts
                .iter()
                .find(|event| event.event_type == "model.requested")
                .expect("fact");
            assert_eq!(fact.payload["request_ref"], serde_json::json!(reference));
            let serialized = serde_json::to_string(&record).expect("serialize");
            assert!(!serialized.contains("P6_PRIVATE_BINARY_SENTINEL"));
            assert!(!serialized.contains("base64"));
        }
        let store = Arc::new(SqlitePipelineStore::open(&path).expect("reopen ledger"));
        let runs = Arc::new(SqliteRunStore::open(&path).await.expect("reopen runs"));
        let runtime = PipelineRuntime::new(store, runs, blobs, model.clone());
        runtime.admit(start.clone()).await.expect("replay");
        runtime.drive(run_id).await.expect("resume model");
        let captured = model.0.lock().expect("capture");
        assert_eq!(captured.len(), 1);
        assert_eq!(
            captured[0].messages.last().expect("user").attachments,
            start.start.attachments
        );
    }
}
impl ModelPort for FakeModel {
    fn stream(&self, _: ModelRequest) -> ModelEventStream {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mut events = vec![
            ModelEvent::TextDelta {
                delta: "hello ".into(),
            },
            ModelEvent::TextDelta {
                delta: "world".into(),
            },
        ];
        if self.tool {
            events.push(ModelEvent::ToolCallStarted {
                call_id: "call".into(),
                name: "forbidden".into(),
            });
        }
        events.push(ModelEvent::Completed {
            finish_reason: FinishReason::Stop,
        });
        Box::pin(stream::iter(events))
    }
}

#[tokio::test]
async fn bounded_backpressure_and_close_are_explicit() {
    let (sender, mut receiver) = bounded_deltas(1).expect("channel");
    let item = DeltaItem {
        invocation_id: "invocation".into(),
        delta_seq: 1,
        channel: DeltaChannel::Text,
        fragment: "hello".into(),
    };
    sender.send(vec![item.clone()]).await.expect("first");
    let second = sender.send(vec![item]);
    tokio::pin!(second);
    assert!(matches!(
        futures_util::poll!(second.as_mut()),
        std::task::Poll::Pending
    ));
    receiver.recv().await.expect("drain");
    second.await.expect("unblocked");
    drop(receiver);
    assert!(
        sender
            .send(vec![DeltaItem {
                invocation_id: "invocation".into(),
                delta_seq: 2,
                channel: DeltaChannel::Text,
                fragment: "x".into()
            }])
            .await
            .is_err()
    );
}

#[tokio::test]
async fn sqlite_end_to_end_replay_reopen_and_tool_rejection() {
    for tool in [false, true] {
        let directory = tempfile::tempdir().expect("temp");
        let path = directory.path().join("runs.db");
        let calls = Arc::new(AtomicUsize::new(0));
        let model: Arc<dyn ModelPort> = Arc::new(FakeModel {
            calls: calls.clone(),
            tool,
        });
        let blobs: Arc<dyn BlobStore> =
            Arc::new(FilesystemBlobStore::new(directory.path().join("blobs")));
        let runs = Arc::new(SqliteRunStore::open(&path).await.expect("runs"));
        let ledger = Arc::new(SqlitePipelineStore::open(&path).expect("ledger"));
        let run_id = RunId::new();
        let runtime =
            PipelineRuntime::new(ledger.clone(), runs.clone(), blobs.clone(), model.clone());
        runtime
            .admit(PipelineStart::text(run_id, "hello", "fake"))
            .await
            .expect("admission");
        runtime
            .admit(PipelineStart::text(run_id, "hello", "fake"))
            .await
            .expect("duplicate admission");
        let record = ledger.load(run_id).await.expect("load").expect("record");
        assert_eq!(
            record
                .facts
                .iter()
                .filter(|event| event.event_type == "model.requested")
                .count(),
            1
        );
        runtime.drive(run_id).await.expect("drive");
        let record = ledger.load(run_id).await.expect("load").expect("record");
        assert_eq!(record.ledger.phase, RunPhase::Terminal);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(record.batches.len(), 1);
        assert_eq!(
            record.ledger.error.as_deref(),
            tool.then_some("unsupported_profile")
        );
        let replay = runs.events_after(run_id, 0, 512).await.expect("replay");
        assert_eq!(replay, record.public_events);
        assert!(matches!(
            replay.last().expect("terminal").kind,
            RunEventKind::RunCompleted { .. } | RunEventKind::RunFailed { .. }
        ));
        drop(runtime);
        drop(ledger);
        drop(runs);
        let ledger = Arc::new(SqlitePipelineStore::open(&path).expect("reopen"));
        let runs = Arc::new(SqliteRunStore::open(&path).await.expect("reopen runs"));
        let runtime = PipelineRuntime::new(ledger, runs.clone(), blobs, model);
        runtime.drive(run_id).await.expect("recover");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            runs.events_after(run_id, 0, 512).await.expect("replay"),
            replay
        );
    }
}

async fn lease(store: &dyn PipelineStore, run_id: RunId, expire: bool) -> InvocationToken {
    let mut record = store.load(run_id).await.expect("load").expect("record");
    let expected = record.generation;
    record.generation += 1;
    record.lease_owner = Some("test-worker".into());
    record.lease_expires_at_ms = if expire {
        0
    } else {
        agent_harness::unix_time_ms() + 120_000
    };
    let token = record.ledger.token.clone();
    store
        .compare_exchange(Some(expected), record)
        .await
        .expect("lease CAS");
    token
}

#[tokio::test]
async fn repair_dropped_tail_batch_idempotency_and_missing_artifact_failure() {
    for missing in [false, true] {
        let directory = tempfile::tempdir().expect("temp");
        let path = directory.path().join("runs.db");
        let model: Arc<dyn ModelPort> = Arc::new(FakeModel {
            calls: Arc::new(AtomicUsize::new(0)),
            tool: false,
        });
        let blobs: Arc<dyn BlobStore> =
            Arc::new(FilesystemBlobStore::new(directory.path().join("blobs")));
        let runs = Arc::new(SqliteRunStore::open(&path).await.expect("runs"));
        let ledger = Arc::new(SqlitePipelineStore::open(&path).expect("ledger"));
        let runtime =
            PipelineRuntime::new(ledger.clone(), runs.clone(), blobs.clone(), model.clone());
        let run_id = RunId::new();
        runtime
            .admit(PipelineStart::text(run_id, "input", "fake"))
            .await
            .expect("admit");
        let token = lease(ledger.as_ref(), run_id, false).await;
        let operator = ModelOperator::new(ledger.clone(), blobs.clone(), model);
        let output = ModelOutputAggregator::new(ledger.clone(), blobs.clone());
        let (sender, mut receiver) = bounded_deltas(2).expect("channel");
        operator
            .execute(run_id, token.clone(), sender)
            .await
            .expect("model");
        let prefix = receiver.recv().await.expect("prefix");
        output.flush(run_id, &token, &prefix).await.expect("batch");
        output
            .flush(run_id, &token, &prefix)
            .await
            .expect("redelivery");
        assert_eq!(
            ledger
                .load(run_id)
                .await
                .expect("load")
                .expect("record")
                .batches
                .len(),
            1
        );
        if missing {
            let mut record = ledger.load(run_id).await.expect("load").expect("record");
            let expected = record.generation;
            record.final_ref.as_mut().expect("final").artifact_ref = BlobId::new();
            record.generation += 1;
            ledger
                .compare_exchange(Some(expected), record)
                .await
                .expect("missing artifact injection");
        }
        output
            .seal(run_id, &token)
            .await
            .expect("repair or explicit failure");
        lease(ledger.as_ref(), run_id, true).await;
        runtime.drive(run_id).await.expect("recover terminal");
        let record = ledger.load(run_id).await.expect("load").expect("record");
        assert_eq!(record.ledger.phase, RunPhase::Terminal);
        assert_eq!(record.sealed, !missing);
        assert_eq!(
            record.ledger.error.as_deref(),
            missing.then_some("output_gap")
        );
        let snapshot = runs.get_run(run_id).await.expect("snapshot").expect("run");
        assert_eq!(
            snapshot.output,
            if missing { "hello " } else { "hello world" }
        );
    }
}

struct BlockedModel {
    entered: Arc<tokio::sync::Notify>,
    partial: bool,
}
impl ModelPort for BlockedModel {
    fn stream(&self, _: ModelRequest) -> ModelEventStream {
        self.entered.notify_one();
        if self.partial {
            Box::pin(futures_util::StreamExt::chain(
                stream::iter(vec![ModelEvent::TextDelta {
                    delta: "partial".into(),
                }]),
                stream::pending(),
            ))
        } else {
            Box::pin(stream::pending())
        }
    }
}

#[tokio::test]
async fn crash_reopen_at_request_started_partial_batch_and_preterminal() {
    for stage in 0..5 {
        let directory = tempfile::tempdir().expect("temp");
        let path = directory.path().join("runs.db");
        let calls = Arc::new(AtomicUsize::new(0));
        let model: Arc<dyn ModelPort> = Arc::new(FakeModel {
            calls: calls.clone(),
            tool: false,
        });
        let blobs: Arc<dyn BlobStore> =
            Arc::new(FilesystemBlobStore::new(directory.path().join("blobs")));
        let run_id = RunId::new();
        {
            let runs = Arc::new(SqliteRunStore::open(&path).await.expect("runs"));
            let ledger = Arc::new(SqlitePipelineStore::open(&path).expect("ledger"));
            let runtime = PipelineRuntime::new(ledger.clone(), runs, blobs.clone(), model.clone());
            runtime
                .admit(PipelineStart::text(run_id, "input", "fake"))
                .await
                .expect("admit");
            if stage > 0 {
                let token = lease(ledger.as_ref(), run_id, false).await;
                let output = ModelOutputAggregator::new(ledger.clone(), blobs.clone());
                let (sender, mut receiver) = bounded_deltas(2).expect("channel");
                if stage == 4 {
                    ModelOperator::new(ledger.clone(), blobs.clone(), model.clone())
                        .execute(run_id, token.clone(), sender)
                        .await
                        .expect("model");
                    output
                        .seal(run_id, &token)
                        .await
                        .expect("seal before coordinator");
                } else {
                    let entered = Arc::new(tokio::sync::Notify::new());
                    let operator = ModelOperator::new(
                        ledger.clone(),
                        blobs.clone(),
                        Arc::new(BlockedModel {
                            entered: entered.clone(),
                            partial: stage >= 2,
                        }),
                    );
                    let worker_token = token.clone();
                    let task = tokio::spawn(async move {
                        operator.execute(run_id, worker_token, sender).await
                    });
                    entered.notified().await;
                    if stage >= 2 {
                        let prefix = receiver.recv().await.expect("partial transient");
                        if stage == 3 {
                            output
                                .flush(run_id, &token, &prefix)
                                .await
                                .expect("batch commit");
                        }
                    }
                    task.abort();
                    assert!(task.await.expect_err("crash").is_cancelled());
                }
                lease(ledger.as_ref(), run_id, true).await;
            }
        }
        let runs = Arc::new(SqliteRunStore::open(&path).await.expect("reopen runs"));
        let ledger = Arc::new(SqlitePipelineStore::open(&path).expect("reopen ledger"));
        let runtime = PipelineRuntime::new(ledger.clone(), runs.clone(), blobs, model);
        runtime.drive(run_id).await.expect("recovery");
        runtime.drive(run_id).await.expect("idempotent recovery");
        let record = ledger.load(run_id).await.expect("load").expect("record");
        assert_eq!(record.ledger.phase, RunPhase::Terminal, "stage {stage}");
        assert_eq!(
            record
                .public_events
                .iter()
                .filter(|event| event.kind.is_terminal())
                .count(),
            1
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            usize::from(stage == 0 || stage == 4)
        );
        assert_eq!(record.ledger.error.is_none(), stage == 0 || stage == 4);
        assert_eq!(
            runs.events_after(run_id, 0, 512).await.expect("replay"),
            record.public_events
        );
    }
}

#[test]
fn coordinator_and_output_have_no_provider_or_tool_execution_calls() {
    let coordinator = include_str!("../src/pipeline/coordinator.rs");
    assert!(!coordinator.contains("ModelPort"));
    assert!(!coordinator.contains(".stream("));
    for source in [
        coordinator,
        include_str!("../src/pipeline/model.rs"),
        include_str!("../src/pipeline/output.rs"),
    ] {
        assert!(!source.contains("ToolPort"));
        assert!(!source.contains(".invoke("));
    }
}

#[tokio::test]
async fn stale_sqlite_cas_and_old_epoch_cannot_append_or_project() {
    let directory = tempfile::tempdir().expect("temp");
    let path = directory.path().join("runs.db");
    let model: Arc<dyn ModelPort> = Arc::new(FakeModel {
        calls: Arc::new(AtomicUsize::new(0)),
        tool: false,
    });
    let blobs: Arc<dyn BlobStore> =
        Arc::new(FilesystemBlobStore::new(directory.path().join("blobs")));
    let runs = Arc::new(SqliteRunStore::open(&path).await.expect("runs"));
    let ledger = Arc::new(SqlitePipelineStore::open(&path).expect("ledger"));
    let runtime = PipelineRuntime::new(ledger.clone(), runs, blobs.clone(), model);
    let run_id = RunId::new();
    let start = PipelineStart::text(run_id, "input", "fake");
    let (first, second) = tokio::join!(runtime.admit(start.clone()), runtime.admit(start.clone()));
    first.expect("first admission");
    second.expect("concurrent admission");
    let mut different = start;
    different
        .start
        .prior_messages
        .push(ModelMessage::system("different context"));
    assert_eq!(runtime.admit(different).await, Err(PipelineError::Conflict));
    let old = ledger.load(run_id).await.expect("load").expect("record");
    let token = lease(ledger.as_ref(), run_id, false).await;
    let mut stale = old.clone();
    stale.generation += 1;
    assert_eq!(
        ledger.compare_exchange(Some(old.generation), stale).await,
        Err(PipelineError::Fenced)
    );
    let mut current = ledger.load(run_id).await.expect("load").expect("record");
    let expected = current.generation;
    current.generation += 1;
    current.ledger.token.lease_epoch += 1;
    ledger
        .compare_exchange(Some(expected), current.clone())
        .await
        .expect("new epoch");
    let output = ModelOutputAggregator::new(ledger.clone(), blobs);
    let delta = DeltaItem {
        invocation_id: token.invocation_id.clone(),
        delta_seq: 1,
        channel: DeltaChannel::Text,
        fragment: "stale".into(),
    };
    assert_eq!(
        output.flush(run_id, &token, &[delta]).await,
        Err(PipelineError::Fenced)
    );
    assert_eq!(ledger.load(run_id).await.expect("load"), Some(current));
}

#[tokio::test]
async fn context_failure_is_projected_without_requesting_a_model() {
    let directory = tempfile::tempdir().expect("temp");
    let path = directory.path().join("runs.db");
    let calls = Arc::new(AtomicUsize::new(0));
    let model: Arc<dyn ModelPort> = Arc::new(FakeModel {
        calls: calls.clone(),
        tool: false,
    });
    let blobs: Arc<dyn BlobStore> =
        Arc::new(FilesystemBlobStore::new(directory.path().join("blobs")));
    let runs = Arc::new(SqliteRunStore::open(&path).await.expect("runs"));
    let ledger = Arc::new(SqlitePipelineStore::open(&path).expect("ledger"));
    let runtime = PipelineRuntime::new(ledger.clone(), runs, blobs, model);
    let run_id = RunId::new();
    runtime
        .fail_admission(
            PipelineStart::text(run_id, "input", "fake"),
            "context_build_failed",
        )
        .await
        .expect("fail context");
    runtime.drive(run_id).await.expect("recovery");
    let record = ledger.load(run_id).await.expect("load").expect("record");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(
        !record
            .facts
            .iter()
            .any(|fact| fact.event_type == "model.requested")
    );
    assert_eq!(record.ledger.phase, RunPhase::Terminal);
    assert_eq!(record.public_events.len(), 2);
}

#[tokio::test]
async fn transient_single_batch_and_out_of_order_inputs_have_identical_coverage() {
    let directory = tempfile::tempdir().expect("temp");
    let run_id = RunId::new();
    let mut baseline = None;
    for mode in 0..3 {
        let path = directory.path().join(format!("{mode}.db"));
        let model: Arc<dyn ModelPort> = Arc::new(FakeModel {
            calls: Arc::new(AtomicUsize::new(0)),
            tool: false,
        });
        let blobs: Arc<dyn BlobStore> = Arc::new(FilesystemBlobStore::new(
            directory.path().join(format!("blobs-{mode}")),
        ));
        let runs = Arc::new(SqliteRunStore::open(&path).await.expect("runs"));
        let ledger = Arc::new(SqlitePipelineStore::open(&path).expect("ledger"));
        let runtime = PipelineRuntime::new(ledger.clone(), runs, blobs.clone(), model);
        runtime
            .admit(PipelineStart::text(run_id, "input", "fake"))
            .await
            .expect("admit");
        let token = lease(ledger.as_ref(), run_id, false).await;
        let items: Vec<_> = [
            (DeltaChannel::Text, "hel"),
            (DeltaChannel::Text, "lo"),
            (DeltaChannel::Reasoning, "thought"),
            (DeltaChannel::Text, "!"),
        ]
        .into_iter()
        .enumerate()
        .map(|(index, (channel, fragment))| DeltaItem {
            invocation_id: token.invocation_id.clone(),
            delta_seq: index as u64 + 1,
            channel,
            fragment: fragment.into(),
        })
        .collect();
        let messages: Vec<_> = match mode {
            0 => items
                .iter()
                .cloned()
                .map(|item| DeltaMessage::Single { item })
                .collect(),
            1 => vec![DeltaMessage::Batch {
                items: items.clone(),
            }],
            _ => vec![DeltaMessage::Batch {
                items: vec![
                    items[3].clone(),
                    items[1].clone(),
                    items[1].clone(),
                    items[0].clone(),
                    items[2].clone(),
                ],
            }],
        };
        let (sender, receiver) = bounded_deltas(1).expect("channel");
        let output = ModelOutputAggregator::new(ledger.clone(), blobs);
        let (sent, consumed) = tokio::join!(
            async move {
                for message in messages {
                    sender.send_message(message).await?;
                }
                Ok::<_, PipelineError>(())
            },
            output.consume(run_id, token, receiver)
        );
        sent.expect("send");
        consumed.expect("consume");
        let record = ledger.load(run_id).await.expect("load").expect("record");
        assert!(
            !record
                .facts
                .iter()
                .any(|fact| fact.event_type.starts_with("model.delta"))
        );
        let rendered: Vec<_> = record
            .batches
            .iter()
            .flat_map(|batch| batch.segments.iter())
            .map(|segment| (segment.channel.clone(), segment.text.clone()))
            .collect();
        assert_eq!(record.coverage_digest, coverage(&items));
        let normalized = (record.durable_through, record.coverage_digest, rendered);
        if let Some(baseline) = &baseline {
            assert_eq!(&normalized, baseline);
        } else {
            baseline = Some(normalized);
        }
    }
}

struct TerminalAppendFailure(Arc<SqliteRunStore>);

impl RunStore for TerminalAppendFailure {
    fn create_run(&self, snapshot: RunSnapshot) -> RunStoreFuture<'_, RunSnapshot> {
        self.0.create_run(snapshot)
    }
    fn get_run(&self, run_id: RunId) -> RunStoreFuture<'_, Option<RunSnapshot>> {
        self.0.get_run(run_id)
    }
    fn set_execution_manifest(
        &self,
        run_id: RunId,
        manifest: serde_json::Value,
    ) -> RunStoreFuture<'_, RunSnapshot> {
        self.0.set_execution_manifest(run_id, manifest)
    }
    fn append_event(
        &self,
        event: RunEvent,
        observed_at_ms: i64,
    ) -> RunStoreFuture<'_, RunSnapshot> {
        self.append_events(vec![ObservedRunEvent::new(event, observed_at_ms)])
    }
    fn append_events(&self, events: Vec<ObservedRunEvent>) -> RunStoreFuture<'_, RunSnapshot> {
        if events.iter().any(|event| event.event.kind.is_terminal()) {
            Box::pin(async {
                Err(RunStoreError::backend(
                    "crash after projection outbox commit",
                ))
            })
        } else {
            self.0.append_events(events)
        }
    }
    fn events_after(
        &self,
        run_id: RunId,
        after_seq: u64,
        limit: usize,
    ) -> RunStoreFuture<'_, Vec<RunEvent>> {
        self.0.events_after(run_id, after_seq, limit)
    }
    fn unfinished_runs(&self) -> RunStoreFuture<'_, Vec<RunSnapshot>> {
        self.0.unfinished_runs()
    }
}

#[tokio::test]
async fn crash_between_public_outbox_and_runstore_append_replays_identical_sequences() {
    let directory = tempfile::tempdir().expect("temp");
    let path = directory.path().join("runs.db");
    let run_id = RunId::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let model: Arc<dyn ModelPort> = Arc::new(FakeModel {
        calls: calls.clone(),
        tool: false,
    });
    let blobs: Arc<dyn BlobStore> =
        Arc::new(FilesystemBlobStore::new(directory.path().join("blobs")));
    let expected;
    {
        let runs = Arc::new(SqliteRunStore::open(&path).await.expect("runs"));
        let ledger = Arc::new(SqlitePipelineStore::open(&path).expect("ledger"));
        let runtime = PipelineRuntime::new(
            ledger.clone(),
            Arc::new(TerminalAppendFailure(runs.clone())),
            blobs.clone(),
            model.clone(),
        );
        runtime
            .admit(PipelineStart::text(run_id, "input", "fake"))
            .await
            .expect("admit");
        assert!(matches!(
            runtime.drive(run_id).await,
            Err(PipelineError::Storage(_))
        ));
        expected = ledger
            .load(run_id)
            .await
            .expect("load")
            .expect("record")
            .public_events;
        assert!(expected.last().expect("terminal").kind.is_terminal());
        let committed = runs.events_after(run_id, 0, 512).await.expect("replay");
        assert!(expected.starts_with(&committed));
        assert!(!committed.last().expect("prefix").kind.is_terminal());
    }
    let runs = Arc::new(SqliteRunStore::open(&path).await.expect("reopen runs"));
    let ledger = Arc::new(SqlitePipelineStore::open(&path).expect("reopen ledger"));
    let runtime = PipelineRuntime::new(ledger, runs.clone(), blobs, model);
    runtime.drive(run_id).await.expect("recover");
    runtime.drive(run_id).await.expect("duplicate recover");
    assert_eq!(
        runs.events_after(run_id, 0, 512).await.expect("replay"),
        expected
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn provider_error_keeps_safe_code_message_and_retryability() {
    struct Failed;
    impl ModelPort for Failed {
        fn stream(&self, _: ModelRequest) -> ModelEventStream {
            Box::pin(stream::iter(vec![ModelEvent::Failed {
                error: ModelError::new(ModelErrorKind::RateLimited, "safe rate limit", true),
            }]))
        }
    }
    let directory = tempfile::tempdir().expect("temp");
    let path = directory.path().join("runs.db");
    let runs = Arc::new(SqliteRunStore::open(&path).await.expect("runs"));
    let ledger = Arc::new(SqlitePipelineStore::open(&path).expect("ledger"));
    let runtime = PipelineRuntime::new(
        ledger,
        runs.clone(),
        Arc::new(FilesystemBlobStore::new(directory.path().join("blobs"))),
        Arc::new(Failed),
    );
    let run_id = RunId::new();
    runtime
        .admit(PipelineStart::text(run_id, "input", "fake"))
        .await
        .expect("admit");
    runtime.drive(run_id).await.expect("drive");
    let events = runs.events_after(run_id, 0, 512).await.expect("events");
    assert_eq!(
        events.last().expect("terminal").kind,
        RunEventKind::RunFailed {
            code: "rate_limited".into(),
            message: "safe rate limit".into(),
            retryable: true
        }
    );
}
