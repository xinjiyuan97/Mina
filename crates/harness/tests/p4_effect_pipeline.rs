use agent_core::{harness::*, operator_fabric::*};
use agent_extension::store::SqliteEventStore;
use agent_harness::{
    approval_operator::ApprovalIngress,
    pipeline::{PipelineStart, ToolsPipelineRuntime},
};
use futures_util::stream;
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct FakeModel(AtomicUsize);

#[tokio::test]
async fn p6_pending_model_abort_reopen_and_tool_round_preserve_inputs() {
    struct Multimodal {
        calls: AtomicUsize,
        expected: Vec<ModelAttachment>,
        entered: tokio::sync::Notify,
    }
    impl ModelPort for Multimodal {
        fn stream(&self, request: ModelRequest) -> ModelEventStream {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            assert_eq!(
                request
                    .messages
                    .iter()
                    .find(|message| message.role == ModelRole::User)
                    .expect("user")
                    .attachments,
                self.expected
            );
            if call == 0 {
                self.entered.notify_one();
                return Box::pin(stream::pending());
            }
            let events = if call == 1 {
                vec![
                    ModelEvent::ToolCallStarted {
                        call_id: "read".into(),
                        name: "read".into(),
                    },
                    ModelEvent::ToolCallArgumentsDelta {
                        call_id: "read".into(),
                        delta: "{}".into(),
                    },
                    ModelEvent::Completed {
                        finish_reason: FinishReason::ToolCall,
                    },
                ]
            } else {
                assert!(
                    request
                        .messages
                        .iter()
                        .any(|message| message.role == ModelRole::Tool)
                );
                vec![ModelEvent::Completed {
                    finish_reason: FinishReason::Stop,
                }]
            };
            Box::pin(stream::iter(events))
        }
    }
    let directory = tempfile::tempdir().expect("temp");
    let path = directory.path().join("events.db");
    let blobs: Arc<dyn BlobStore> = Arc::new(agent_extension::store::FilesystemBlobStore::new(
        directory.path().join("blobs"),
    ));
    let mut start = PipelineStart::text(RunId::new(), "", "fake").start;
    start.config.allowed_tools = None;
    start.config.max_steps = 3;
    start.config.tool_timeout_ms = 1000;
    for name in ["first.png", "second.png"] {
        let blob = blobs
            .put(PutBlob {
                media_type: "image/png".into(),
                name: Some(name.into()),
                data: vec![1, 2, 3],
                created_at_ms: 0,
            })
            .await
            .expect("blob");
        start.attachments.push(ModelAttachment {
            blob_id: blob.blob_id,
            media_type: blob.media_type,
            name: blob.name,
            size_bytes: Some(blob.size_bytes),
        });
    }
    let model = Arc::new(Multimodal {
        calls: AtomicUsize::new(0),
        expected: start.attachments.clone(),
        entered: tokio::sync::Notify::new(),
    });
    struct ReadTool;
    impl ToolPort for ReadTool {
        fn definitions(&self) -> Vec<ToolDefinition> {
            vec![ToolDefinition::new(
                "read",
                "read",
                json!({"type":"object"}),
            )]
        }
        fn call(&self, _: ToolCallRequest) -> ToolCallFuture {
            Box::pin(async { Ok(ToolOutput::text("read")) })
        }
    }
    let tools = Arc::new(ReadTool);
    let run_id = start.run_id;
    {
        let fabric = Arc::new(SqliteEventStore::open(&path, "p6").await.expect("open"));
        let runtime = ToolsPipelineRuntime::new(fabric, model.clone(), tools.clone())
            .with_blobs(blobs.clone());
        runtime
            .admit("local", start.clone(), 0)
            .await
            .expect("admit");
        let worker = tokio::spawn(async move {
            for _ in 0..20 {
                runtime.tick("local", run_id, || 1).await.expect("tick");
            }
        });
        tokio::time::timeout(std::time::Duration::from_secs(10), model.entered.notified())
            .await
            .expect("pending model");
        worker.abort();
        assert!(worker.await.expect_err("abort").is_cancelled());
    }
    let fabric = Arc::new(SqliteEventStore::open(&path, "p6").await.expect("reopen"));
    let runtime = ToolsPipelineRuntime::new(fabric, model.clone(), tools.clone()).with_blobs(blobs);
    assert_eq!(
        runtime
            .load("local", run_id)
            .await
            .expect("persisted")
            .start
            .attachments,
        start.attachments
    );
    runtime
        .admit("local", start.clone(), 200_000)
        .await
        .expect("idempotent");
    for _ in 0..40 {
        if runtime
            .tick("local", run_id, || 200_000)
            .await
            .expect("reclaim")
            .eq(&0)
        {
            break;
        }
    }
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    assert!(matches!(
        runtime
            .load("local", run_id)
            .await
            .expect("interrupted")
            .reducer
            .outcome(),
        AgentLoopOutcome::Failed { .. }
    ));
    start.run_id = RunId::new();
    let run_id = start.run_id;
    runtime
        .admit("local", start, 200_000)
        .await
        .expect("new run");
    for _ in 0..40 {
        if runtime
            .tick("local", run_id, || 200_000)
            .await
            .expect("tool round")
            .eq(&0)
        {
            break;
        }
    }
    assert_eq!(model.calls.load(Ordering::SeqCst), 3);
    assert!(matches!(
        runtime
            .load("local", run_id)
            .await
            .expect("terminal")
            .reducer
            .outcome(),
        AgentLoopOutcome::Complete { .. }
    ));
}
impl ModelPort for FakeModel {
    fn stream(&self, request: ModelRequest) -> ModelEventStream {
        let turn = self.0.fetch_add(1, Ordering::SeqCst);
        let events = if turn == 0 {
            assert_eq!(request.tools.len(), 1);
            vec![
                ModelEvent::ToolCallStarted {
                    call_id: "call".into(),
                    name: "write".into(),
                },
                ModelEvent::ToolCallArgumentsDelta {
                    call_id: "call".into(),
                    delta: "{\"value\":42}".into(),
                },
                ModelEvent::Completed {
                    finish_reason: FinishReason::ToolCall,
                },
            ]
        } else {
            assert!(
                request
                    .messages
                    .iter()
                    .any(|message| message.role == ModelRole::Tool
                        && message.tool_call_id.as_deref() == Some("call"))
            );
            vec![
                ModelEvent::TextDelta {
                    delta: "done".into(),
                },
                ModelEvent::Completed {
                    finish_reason: FinishReason::Stop,
                },
            ]
        };
        Box::pin(stream::iter(events))
    }
}

struct FakeTool(AtomicUsize);

#[tokio::test]
async fn durable_run_deadline_survives_reopen_and_fences_late_approval() {
    let directory = tempfile::tempdir().expect("temp");
    let path = directory.path().join("deadline.db");
    let model = Arc::new(FakeModel(AtomicUsize::new(0)));
    let tools = Arc::new(FakeTool(AtomicUsize::new(0)));
    let mut start = PipelineStart::text(RunId::new(), "deadline", "fake").start;
    start.config.allowed_tools = None;
    start.config.max_steps = 3;
    let run_id = start.run_id;
    {
        let fabric = Arc::new(
            SqliteEventStore::open(&path, "deadline")
                .await
                .expect("fixture operation"),
        );
        let runtime = ToolsPipelineRuntime::new(fabric, model.clone(), tools.clone());
        runtime
            .admit("local", start, 0)
            .await
            .expect("fixture operation");
        assert_eq!(
            runtime
                .ensure_deadline("local", run_id, 100)
                .await
                .expect("fixture operation"),
            100
        );
        for _ in 0..20 {
            runtime
                .tick("local", run_id, || 1)
                .await
                .expect("fixture operation");
        }
        assert!(
            runtime
                .load("local", run_id)
                .await
                .expect("fixture operation")
                .reducer
                .is_waiting_for_approval()
        );
        runtime
            .reconcile_control("local", run_id, 99)
            .await
            .expect("fixture operation");
        assert!(
            runtime
                .load("local", run_id)
                .await
                .expect("fixture operation")
                .reducer
                .is_waiting_for_approval()
        );
    }
    let fabric = Arc::new(
        SqliteEventStore::open(&path, "deadline")
            .await
            .expect("fixture operation"),
    );
    let runtime = ToolsPipelineRuntime::new(fabric, model.clone(), tools.clone());
    assert_eq!(
        runtime
            .ensure_deadline("local", run_id, 999)
            .await
            .expect("fixture operation"),
        100
    );
    runtime
        .reconcile_control("local", run_id, 100)
        .await
        .expect("fixture operation");
    let failed = runtime
        .load("local", run_id)
        .await
        .expect("fixture operation");
    assert!(
        matches!(failed.reducer.outcome(), AgentLoopOutcome::Failed { ref error } if error.code == "run_timeout")
    );
    runtime
        .reconcile_control("local", run_id, 101)
        .await
        .expect("fixture operation");
    runtime
        .request_cancel("local", run_id, "gateway", 102)
        .await
        .expect("fixture operation");
    runtime
        .tick("local", run_id, || 103)
        .await
        .expect("fixture operation");
    assert_eq!(
        runtime
            .load("local", run_id)
            .await
            .expect("fixture operation")
            .public_events,
        failed.public_events
    );
    assert_eq!(tools.0.load(Ordering::SeqCst), 0);
    assert_eq!(model.0.load(Ordering::SeqCst), 1);
}
impl ToolPort for FakeTool {
    fn definitions(&self) -> Vec<ToolDefinition> {
        vec![
            ToolDefinition::new("write", "write", json!({"type":"object"}))
                .with_risk_level(ToolRiskLevel::High),
        ]
    }
    fn call(&self, _: ToolCallRequest) -> ToolCallFuture {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(ToolOutput::text("written")) })
    }
}

#[tokio::test]
async fn coordinator_model_validation_approval_tool_next_model_survives_reopen() {
    let directory = tempfile::tempdir().expect("temp");
    let path = directory.path().join("fabric.db");
    let model = Arc::new(FakeModel(AtomicUsize::new(0)));
    let tools = Arc::new(FakeTool(AtomicUsize::new(0)));
    let mut start = PipelineStart::text(RunId::new(), "write", "fake").start;
    start.config.allowed_tools = None;
    start.config.max_steps = 3;
    start.config.tool_timeout_ms = 1000;
    let run_id = start.run_id;
    let approval_id;
    {
        let fabric = Arc::new(SqliteEventStore::open(&path, "p4").await.expect("open"));
        let runtime = ToolsPipelineRuntime::new(fabric, model.clone(), tools.clone());
        runtime
            .admit("local", start.clone(), 0)
            .await
            .expect("admit");
        for _ in 0..20 {
            if runtime.tick("local", run_id, || 1).await.expect("tick") == 0 {
                break;
            }
        }
        let saved = runtime.load("local", run_id).await.expect("state");
        assert!(saved.reducer.is_waiting_for_approval());
        approval_id = saved.approvals["call"].approval_id;
        assert!(saved.public_events.iter().any(|event| matches!(event.kind, RunEventKind::ApprovalRequested {approval_id: id, ..} if id == approval_id)));
        assert_eq!(tools.0.load(Ordering::SeqCst), 0);
    }
    let fabric = Arc::new(SqliteEventStore::open(&path, "p4").await.expect("reopen"));
    let ingress = ApprovalIngress::new(fabric.clone());
    let resolution = ApprovalResolution {
        decision: ApprovalDecision::AllowOnce,
        reason: None,
    };
    assert!(
        ingress
            .resolve(
                "local",
                run_id,
                approval_id,
                "gateway:test",
                resolution.clone(),
                2
            )
            .await
            .expect("resolve")
    );
    assert!(
        !ingress
            .resolve("local", run_id, approval_id, "gateway:test", resolution, 2)
            .await
            .expect("duplicate")
    );
    let runtime = ToolsPipelineRuntime::new(fabric.clone(), model.clone(), tools.clone());
    runtime
        .admit("local", start, 3)
        .await
        .expect("idempotent admit");
    for _ in 0..30 {
        if runtime.tick("local", run_id, || 3).await.expect("tick") == 0 {
            break;
        }
    }
    let saved = runtime.load("local", run_id).await.expect("state");
    assert!(matches!(
        saved.reducer.outcome(),
        AgentLoopOutcome::Complete { .. }
    ));
    assert_eq!(model.0.load(Ordering::SeqCst), 2);
    assert_eq!(tools.0.load(Ordering::SeqCst), 1);
    assert!(matches!(
        saved.public_events.last().expect("terminal").kind,
        RunEventKind::RunCompleted { .. }
    ));
    for (index, event) in saved.public_events.iter().enumerate() {
        assert_eq!(event.seq, index as u64 + 1);
    }
    assert_eq!(
        runtime.tick("local", run_id, || 4).await.expect("replay"),
        0
    );
    let FabricReply::Dispatched(0) = fabric
        .execute(FabricCommand::DispatchOutbox { limit: 128 })
        .await
        .expect("drained")
    else {
        panic!("outbox")
    };
}

struct PairModel(AtomicUsize);
impl ModelPort for PairModel {
    fn stream(&self, request: ModelRequest) -> ModelEventStream {
        if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
            Box::pin(stream::iter(vec![
                ModelEvent::ToolCallStarted {
                    call_id: "first".into(),
                    name: "read".into(),
                },
                ModelEvent::ToolCallArgumentsDelta {
                    call_id: "first".into(),
                    delta: "{}".into(),
                },
                ModelEvent::ToolCallStarted {
                    call_id: "second".into(),
                    name: "read".into(),
                },
                ModelEvent::ToolCallArgumentsDelta {
                    call_id: "second".into(),
                    delta: "{}".into(),
                },
                ModelEvent::Completed {
                    finish_reason: FinishReason::ToolCall,
                },
            ]))
        } else {
            let calls: Vec<_> = request
                .messages
                .iter()
                .filter(|message| message.role == ModelRole::Tool)
                .map(|message| message.tool_call_id.as_deref().expect("call"))
                .collect();
            assert_eq!(calls, ["first", "second"]);
            Box::pin(stream::iter(vec![ModelEvent::Completed {
                finish_reason: FinishReason::Stop,
            }]))
        }
    }
}

struct PairTool {
    parallel: bool,
    barrier: Arc<tokio::sync::Barrier>,
    second_done: Arc<tokio::sync::Notify>,
    finished: Arc<std::sync::Mutex<Vec<String>>>,
}
impl ToolPort for PairTool {
    fn definitions(&self) -> Vec<ToolDefinition> {
        vec![
            ToolDefinition::new("read", "read", json!({"type":"object"})).with_execution_policy(
                ToolExecutionPolicy::read_only().with_concurrency(if self.parallel {
                    ToolConcurrency::ParallelSafe
                } else {
                    ToolConcurrency::Exclusive
                }),
            ),
        ]
    }
    fn call(&self, request: ToolCallRequest) -> ToolCallFuture {
        let barrier = self.barrier.clone();
        let second_done = self.second_done.clone();
        let finished = self.finished.clone();
        let parallel = self.parallel;
        Box::pin(async move {
            if parallel {
                barrier.wait().await;
                if request.call_id == "first" {
                    second_done.notified().await;
                }
            }
            finished
                .lock()
                .expect("order")
                .push(request.call_id.clone());
            if parallel && request.call_id == "second" {
                second_done.notify_one();
            }
            Ok(ToolOutput::text(request.call_id))
        })
    }
}

#[tokio::test]
async fn parallel_tools_overlap_finish_reversed_and_join_in_model_order_exclusive_stays_single() {
    for parallel in [true, false] {
        let directory = tempfile::tempdir().expect("temp");
        let fabric = Arc::new(
            SqliteEventStore::open(directory.path().join("fabric.db"), "p4")
                .await
                .expect("open"),
        );
        let tools = Arc::new(PairTool {
            parallel,
            barrier: Arc::new(tokio::sync::Barrier::new(2)),
            second_done: Arc::new(tokio::sync::Notify::new()),
            finished: Arc::new(std::sync::Mutex::new(vec![])),
        });
        let runtime = ToolsPipelineRuntime::new(
            fabric,
            Arc::new(PairModel(AtomicUsize::new(0))),
            tools.clone(),
        );
        let mut start = PipelineStart::text(RunId::new(), "read", "fake").start;
        start.config.max_steps = 3;
        start.config.allowed_tools = None;
        start.config.tool_timeout_ms = 5000;
        start.config.tool_call_strategy = ToolCallStrategy::ParallelSafe;
        let run_id = start.run_id;
        runtime.admit("local", start, 0).await.expect("admit");
        let mut saw_tools = false;
        for _ in 0..35 {
            let record = runtime.load("local", run_id).await.expect("record");
            let pending = record.reducer.pending_effects();
            if pending
                .iter()
                .any(|effect| matches!(effect.kind, AgentLoopEffectKind::InvokeTool { .. }))
            {
                saw_tools = true;
                assert_eq!(pending.len(), if parallel { 2 } else { 1 });
            }
            if runtime.tick("local", run_id, || 1).await.expect("tick") == 0 {
                break;
            }
        }
        assert!(saw_tools);
        assert!(matches!(
            runtime
                .load("local", run_id)
                .await
                .expect("state")
                .reducer
                .outcome(),
            AgentLoopOutcome::Complete { .. }
        ));
        assert_eq!(
            *tools.finished.lock().expect("order"),
            if parallel {
                vec!["second", "first"]
            } else {
                vec!["first", "second"]
            }
        );
    }
}

#[tokio::test]
async fn two_tenants_wait_for_approval_without_subscription_or_delivery_collisions() {
    let directory = tempfile::tempdir().expect("temp");
    let fabric = Arc::new(
        SqliteEventStore::open(directory.path().join("fabric.db"), "p4")
            .await
            .expect("open"),
    );
    for tenant in ["first-tenant", "second-tenant"] {
        let runtime = ToolsPipelineRuntime::new(
            fabric.clone(),
            Arc::new(FakeModel(AtomicUsize::new(0))),
            Arc::new(FakeTool(AtomicUsize::new(0))),
        );
        let mut start = PipelineStart::text(RunId::new(), "write", "fake").start;
        start.config.allowed_tools = None;
        start.config.max_steps = 3;
        let run_id = start.run_id;
        runtime.admit(tenant, start, 0).await.expect("admit");
        for _ in 0..20 {
            if runtime
                .tick(tenant, run_id, || 1)
                .await
                .expect("isolated tick")
                == 0
            {
                break;
            }
        }
        assert!(
            runtime
                .load(tenant, run_id)
                .await
                .expect("state")
                .reducer
                .is_waiting_for_approval()
        );
    }
}

#[tokio::test]
async fn denied_and_expired_approvals_return_to_model_without_tool_execution() {
    for expired in [false, true] {
        let directory = tempfile::tempdir().expect("temp");
        let fabric = Arc::new(
            SqliteEventStore::open(directory.path().join("fabric.db"), "p4")
                .await
                .expect("open"),
        );
        let tools = Arc::new(FakeTool(AtomicUsize::new(0)));
        let model = Arc::new(FakeModel(AtomicUsize::new(0)));
        let runtime = ToolsPipelineRuntime::new(fabric.clone(), model.clone(), tools.clone());
        let mut start = PipelineStart::text(RunId::new(), "write", "fake").start;
        start.config.allowed_tools = None;
        start.config.max_steps = 3;
        let run_id = start.run_id;
        runtime.admit("local", start, 0).await.expect("admit");
        for _ in 0..20 {
            if runtime.tick("local", run_id, || 1).await.expect("tick") == 0 {
                break;
            }
        }
        let saved = runtime.load("local", run_id).await.expect("load");
        let approval = &saved.approvals["call"];
        let ingress = ApprovalIngress::new(fabric.clone());
        let now = if expired { approval.expires_at_ms } else { 2 };
        if expired {
            ingress
                .expire("local", run_id, approval.approval_id, now)
                .await
                .expect("expire");
        } else {
            ingress
                .resolve(
                    "local",
                    run_id,
                    approval.approval_id,
                    "gateway:test",
                    ApprovalResolution {
                        decision: ApprovalDecision::Deny,
                        reason: None,
                    },
                    now,
                )
                .await
                .expect("deny");
        }
        for _ in 0..25 {
            if runtime.tick("local", run_id, || now).await.expect("tick") == 0 {
                break;
            }
        }
        assert!(matches!(
            runtime
                .load("local", run_id)
                .await
                .expect("state")
                .reducer
                .outcome(),
            AgentLoopOutcome::Complete { .. }
        ));
        assert_eq!(tools.0.load(Ordering::SeqCst), 0);
        assert_eq!(model.0.load(Ordering::SeqCst), 2);
    }
}

#[tokio::test]
async fn every_tick_reopens_sqlite_across_validation_tool_join_and_next_model() {
    let directory = tempfile::tempdir().expect("temp");
    let path = directory.path().join("fabric.db");
    let tools = Arc::new(FakeTool(AtomicUsize::new(0)));
    let model = Arc::new(FakeModel(AtomicUsize::new(0)));
    let mut start = PipelineStart::text(RunId::new(), "write", "fake").start;
    start.config.allowed_tools = None;
    start.config.max_steps = 3;
    start.config.tool_timeout_ms = 1000;
    start.config.approval_policy = ToolApprovalPolicy::new(100);
    let run_id = start.run_id;
    let mut completed = false;
    for tick in 0..30 {
        let fabric = Arc::new(SqliteEventStore::open(&path, "p4").await.expect("reopen"));
        let runtime = ToolsPipelineRuntime::new(fabric, model.clone(), tools.clone());
        runtime
            .admit("local", start.clone(), tick)
            .await
            .expect("admit/replay");
        let count = runtime.tick("local", run_id, || tick).await.expect("tick");
        completed = matches!(
            runtime
                .load("local", run_id)
                .await
                .expect("load")
                .reducer
                .outcome(),
            AgentLoopOutcome::Complete { .. }
        );
        if count == 0 {
            break;
        }
    }
    assert!(completed);
    assert_eq!(tools.0.load(Ordering::SeqCst), 1);
    assert_eq!(model.0.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn durable_run_cancel_fences_pending_approval_after_reopen() {
    let directory = tempfile::tempdir().expect("temp");
    let path = directory.path().join("fabric.db");
    let model = Arc::new(FakeModel(AtomicUsize::new(0)));
    let tools = Arc::new(FakeTool(AtomicUsize::new(0)));
    let mut start = PipelineStart::text(RunId::new(), "write", "fake").start;
    start.config.allowed_tools = None;
    start.config.max_steps = 3;
    let run_id = start.run_id;
    let approval_id;
    {
        let fabric = Arc::new(SqliteEventStore::open(&path, "p4").await.expect("open"));
        let runtime = ToolsPipelineRuntime::new(fabric, model.clone(), tools.clone());
        runtime.admit("local", start, 0).await.expect("admit");
        for _ in 0..20 {
            if runtime.tick("local", run_id, || 1).await.expect("tick") == 0 {
                break;
            }
        }
        approval_id = runtime
            .load("local", run_id)
            .await
            .expect("state")
            .approvals["call"]
            .approval_id;
        runtime
            .request_cancel("local", run_id, "gateway:test", 2)
            .await
            .expect("persist cancel");
        assert!(
            runtime
                .load("local", run_id)
                .await
                .expect("not yet reduced")
                .reducer
                .is_waiting_for_approval()
        );
    }
    let fabric = Arc::new(SqliteEventStore::open(&path, "p4").await.expect("reopen"));
    let runtime = ToolsPipelineRuntime::new(fabric.clone(), model.clone(), tools.clone());
    for _ in 0..10 {
        if runtime.tick("local", run_id, || 3).await.expect("tick") == 0 {
            break;
        }
    }
    let saved = runtime.load("local", run_id).await.expect("state");
    assert!(matches!(
        saved.reducer.outcome(),
        AgentLoopOutcome::Cancelled
    ));
    assert!(matches!(
        saved.public_events.last().expect("terminal").kind,
        RunEventKind::RunCancelled
    ));
    assert!(
        ApprovalIngress::new(fabric)
            .resolve(
                "local",
                run_id,
                approval_id,
                "gateway:test",
                ApprovalResolution {
                    decision: ApprovalDecision::AllowOnce,
                    reason: None
                },
                4
            )
            .await
            .is_err()
    );
    assert_eq!(tools.0.load(Ordering::SeqCst), 0);
    assert_eq!(model.0.load(Ordering::SeqCst), 1);
}

struct BlockingTool {
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    calls: AtomicUsize,
}
impl ToolPort for BlockingTool {
    fn definitions(&self) -> Vec<ToolDefinition> {
        FakeTool(AtomicUsize::new(0)).definitions()
    }
    fn call(&self, _: ToolCallRequest) -> ToolCallFuture {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let started = self.started.clone();
        let release = self.release.clone();
        Box::pin(async move {
            started.notify_one();
            release.notified().await;
            Ok(ToolOutput::text("late"))
        })
    }
}

#[tokio::test]
async fn run_cancel_fences_inflight_tool_success_without_next_model() {
    let directory = tempfile::tempdir().expect("temp");
    let fabric = Arc::new(
        SqliteEventStore::open(directory.path().join("fabric.db"), "p4")
            .await
            .expect("open"),
    );
    let tools = Arc::new(BlockingTool {
        started: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
        calls: AtomicUsize::new(0),
    });
    let model = Arc::new(FakeModel(AtomicUsize::new(0)));
    let runtime = Arc::new(ToolsPipelineRuntime::new(
        fabric,
        model.clone(),
        tools.clone(),
    ));
    let mut start = PipelineStart::text(RunId::new(), "write", "fake").start;
    start.config.allowed_tools = None;
    start.config.max_steps = 3;
    start.config.tool_timeout_ms = 10_000;
    start.config.approval_policy = ToolApprovalPolicy::new(100);
    let run_id = start.run_id;
    runtime.admit("local", start, 0).await.expect("admit");
    let mut pending = false;
    for _ in 0..20 {
        if runtime
            .load("local", run_id)
            .await
            .expect("state")
            .reducer
            .pending_effects()
            .iter()
            .any(|effect| matches!(effect.kind, AgentLoopEffectKind::InvokeTool { .. }))
        {
            pending = true;
            break;
        }
        runtime.tick("local", run_id, || 1).await.expect("tick");
    }
    assert!(pending);
    let executing = {
        let runtime = runtime.clone();
        tokio::spawn(async move { runtime.tick("local", run_id, || 2).await })
    };
    tools.started.notified().await;
    runtime
        .request_cancel("local", run_id, "gateway:test", 3)
        .await
        .expect("cancel request");
    runtime
        .tick("local", run_id, || 4)
        .await
        .expect("fence and cancel tool");
    tools.release.notify_one();
    assert_eq!(executing.await.expect("task"), Err(FabricError::Fenced));
    for _ in 0..10 {
        if runtime
            .tick("local", run_id, || 5)
            .await
            .expect("drain late facts")
            == 0
        {
            break;
        }
    }
    let state = runtime.load("local", run_id).await.expect("state");
    assert!(matches!(
        state.reducer.outcome(),
        AgentLoopOutcome::Cancelled
    ));
    assert_eq!(
        state
            .public_events
            .iter()
            .filter(|event| matches!(event.kind, RunEventKind::RunCancelled))
            .count(),
        1
    );
    assert_eq!(model.0.load(Ordering::SeqCst), 1);
    assert_eq!(tools.calls.load(Ordering::SeqCst), 1);
}

struct FailedModel;
impl ModelPort for FailedModel {
    fn stream(&self, _: ModelRequest) -> ModelEventStream {
        Box::pin(stream::iter(vec![ModelEvent::Failed {
            error: ModelError::new(ModelErrorKind::Timeout, "fake timeout", false),
        }]))
    }
}

#[tokio::test]
async fn model_failure_is_a_failed_fact_and_seals_before_failed_terminal() {
    let directory = tempfile::tempdir().expect("temp");
    let fabric = Arc::new(
        SqliteEventStore::open(directory.path().join("fabric.db"), "p4")
            .await
            .expect("open"),
    );
    let tools = Arc::new(FakeTool(AtomicUsize::new(0)));
    let runtime = ToolsPipelineRuntime::new(fabric.clone(), Arc::new(FailedModel), tools.clone());
    let start = PipelineStart::text(RunId::new(), "fail", "fake").start;
    let run_id = start.run_id;
    runtime.admit("local", start, 0).await.expect("admit");
    for _ in 0..20 {
        if runtime.tick("local", run_id, || 1).await.expect("tick") == 0 {
            break;
        }
    }
    let record = runtime.load("local", run_id).await.expect("state");
    assert!(matches!(
        record.reducer.outcome(),
        AgentLoopOutcome::Failed { .. }
    ));
    assert!(matches!(
        record.public_events.last().expect("terminal").kind,
        RunEventKind::RunFailed { .. }
    ));
    let route = record
        .routes
        .values()
        .find(|route| route.consumer == "model")
        .expect("model route");
    let FabricReply::Replay(facts) = fabric
        .execute(FabricCommand::ScanEvents {
            topic: route.topic.clone(),
            event_type: "model.failed".into(),
            after: 0,
            limit: 100,
        })
        .await
        .expect("facts")
    else {
        panic!("facts")
    };
    assert_eq!(facts.len(), 1);
    assert_eq!(tools.0.load(Ordering::SeqCst), 0);
}
