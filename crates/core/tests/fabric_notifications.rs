use agent_core::{
    agent::{memory_fabric::MemoryFabric, *},
    contracts::fabric::*,
};
use agent_extension::store::InMemoryRunStore;
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

struct CountedFabric {
    inner: MemoryFabric,
    reads: AtomicUsize,
}
impl OperatorFabric for CountedFabric {
    fn capabilities(&self) -> FabricCapabilities {
        self.inner.capabilities()
    }
    fn changes(&self) -> Option<tokio::sync::watch::Receiver<u64>> {
        self.inner.changes()
    }
    fn execute(&self, command: FabricCommand) -> FabricFuture<'_> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.execute(command)
    }
}
struct WaitingNode;
impl GraphOperator for WaitingNode {
    fn handle(&self, ctx: GraphContext) -> graph::GraphFuture<'_> {
        Box::pin(async move {
            Ok(GraphOutput::new(
                Value::Null,
                if ctx.event.kind == "idle.wake.v1" {
                    vec![GraphEvent::new("idle.done.v1", json!({"output":"awake"}))]
                } else {
                    vec![]
                },
            ))
        })
    }
}
#[tokio::test]
async fn many_idle_runs_do_not_poll_and_ingress_wakes_them_promptly() {
    let fabric = Arc::new(CountedFabric {
        inner: MemoryFabric::default(),
        reads: AtomicUsize::new(0),
    });
    let mut registry = OperatorRegistry::default();
    registry
        .register(
            OperatorDescriptor::new("test.waiting", 1)
                .input("idle.start.v1", json!({"type":"object"}))
                .input("idle.wake.v1", json!({"type":"object"}))
                .output("idle.done.v1", json!({"type":"object"}))
                .output("idle.failed.v1", json!({"type":"object"}))
                .output("idle.cancelled.v1", json!({"type":"object"})),
            |_, _| Ok(Arc::new(WaitingNode)),
        )
        .unwrap();
    let definition = AgentGraph::new("idle")
        .trigger(
            "start",
            agent_core::agent::TriggerDefinition::manual("idle.start.v1"),
        )
        .operator(
            "node",
            OperatorDefinition::new("test.waiting", 1, json!({})),
        )
        .subscribe("start", "start", "node", ["idle.start.v1"])
        .subscribe("wake", "$external", "node", ["idle.wake.v1"])
        .lifecycle(GraphLifecycle {
            start: GraphStart {
                trigger: "start".into(),
            },
            terminal: GraphTerminal {
                from: "node".into(),
                completed: "idle.done.v1".into(),
                failed: "idle.failed.v1".into(),
                cancelled: "idle.cancelled.v1".into(),
            },
        })
        .limits(GraphLimits::new(10000, 100))
        .finish()
        .unwrap();
    let engine = Arc::new(AgentEngine::with_registry(
        AgentResources::new(fabric.clone()),
        Arc::new(InMemoryRunStore::default()),
        registry,
    ));
    let mut started = vec![];
    for _ in 0..16 {
        started.push(
            engine
                .run_graph(definition.clone(), json!({}))
                .await
                .unwrap(),
        );
    }
    tokio::time::sleep(Duration::from_millis(150)).await;
    fabric.reads.store(0, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(250)).await;
    let reads = fabric.reads.load(Ordering::SeqCst);
    assert!(
        reads < 32,
        "16 idle runs executed {reads} storage operations in 250 ms"
    );
    let ingress = GraphIngress::new(fabric.clone());
    let first = started.remove(0);
    ingress
        .send(
            "local",
            first.run_id,
            agent_core::contracts::events::EventId::new(),
            GraphEvent::new("idle.wake.v1", json!({})),
        )
        .await
        .unwrap();
    let mut events = first.events;
    tokio::time::timeout(Duration::from_millis(750), async {
        while !events.recv().await.unwrap().kind.is_terminal() {}
    })
    .await
    .expect("notification must beat the one second fallback");
    assert_eq!(
        engine.get_run(first.run_id).await.unwrap().unwrap().output,
        "awake"
    );
    engine.stop_workers().await;
}

#[tokio::test]
async fn watch_keeps_a_change_between_read_and_wait_and_ignores_reads() {
    let fabric = MemoryFabric::default();
    let mut watcher = FabricWatch::new(&fabric);
    watcher.observe();
    fabric
        .execute(FabricCommand::Transact {
            checkpoint: CheckpointWrite {
                key: "notification-test".into(),
                expected: None,
                value: json!({}),
            },
            outbox: vec![],
            delivery: None,
        })
        .await
        .unwrap();
    tokio::time::timeout(
        Duration::from_millis(100),
        watcher.wait(Duration::from_secs(1)),
    )
    .await
    .unwrap();
    watcher.observe();
    fabric
        .execute(FabricCommand::ReadCheckpoint("notification-test".into()))
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(
            Duration::from_millis(30),
            watcher.wait(Duration::from_secs(1))
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn sqlite_notifies_after_commit_and_separate_connection_uses_fallback() {
    use agent_extension::store::SqliteEventStore;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("notifications.sqlite");
    let store = SqliteEventStore::open(&path, "first").await.unwrap();
    let mut watcher = FabricWatch::new(&store);
    watcher.observe();
    store
        .clone()
        .execute(FabricCommand::Transact {
            checkpoint: CheckpointWrite {
                key: "shared".into(),
                expected: None,
                value: json!(1),
            },
            outbox: vec![],
            delivery: None,
        })
        .await
        .unwrap();
    tokio::time::timeout(
        Duration::from_millis(200),
        watcher.wait(Duration::from_secs(1)),
    )
    .await
    .unwrap();
    watcher.observe();
    assert!(
        store
            .execute(FabricCommand::Transact {
                checkpoint: CheckpointWrite {
                    key: "shared".into(),
                    expected: None,
                    value: json!(2)
                },
                outbox: vec![],
                delivery: None
            })
            .await
            .is_err()
    );
    assert!(
        tokio::time::timeout(
            Duration::from_millis(30),
            watcher.wait(Duration::from_secs(1))
        )
        .await
        .is_err()
    );
    let remote = SqliteEventStore::open(&path, "second").await.unwrap();
    let version = match remote
        .execute(FabricCommand::ReadCheckpoint("shared".into()))
        .await
        .unwrap()
    {
        FabricReply::Checkpoint(Some(c)) => c.version,
        _ => panic!("checkpoint"),
    };
    remote
        .execute(FabricCommand::Transact {
            checkpoint: CheckpointWrite {
                key: "shared".into(),
                expected: Some(version),
                value: json!(3),
            },
            outbox: vec![],
            delivery: None,
        })
        .await
        .unwrap();
    watcher.wait(Duration::from_millis(30)).await;
    assert!(
        matches!(store.execute(FabricCommand::ReadCheckpoint("shared".into())).await.unwrap(),FabricReply::Checkpoint(Some(c)) if c.value==json!(3))
    );
}
