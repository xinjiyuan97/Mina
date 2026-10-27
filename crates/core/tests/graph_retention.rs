use agent_core::{
    agent::{
        AgentDefinition, AgentResources, GraphHost, OperatorRegistry, graph::run_checkpoint_key,
        memory_fabric::MemoryFabric,
    },
    contracts::fabric::*,
    contracts::*,
};
use futures_util::StreamExt;
use serde_json::json;
use std::{sync::Arc, time::Duration};
struct Text;
impl ModelPort for Text {
    fn stream(&self, _: ModelRequest) -> ModelEventStream {
        Box::pin(futures_util::stream::iter([
            ModelEvent::TextDelta {
                delta: "retained".into(),
            },
            ModelEvent::Completed {
                finish_reason: FinishReason::Stop,
            },
        ]))
    }
}
fn host(fabric: Arc<dyn OperatorFabric>) -> GraphHost {
    GraphHost::new(
        "local",
        AgentDefinition::from_yaml(include_str!("../../../config/agents/text.yaml")).unwrap(),
        OperatorRegistry::builtins(),
        AgentResources::new(fabric).model("primary", "v1", Arc::new(Text)),
    )
    .unwrap()
}
async fn completed(host: &GraphHost) -> RunId {
    let run = RunId::new();
    host.start(run, json!("retention")).await.unwrap();
    let events = tokio::time::timeout(
        Duration::from_secs(5),
        host.subscribe(run, 0).collect::<Vec<_>>(),
    )
    .await
    .unwrap();
    assert!(events.last().unwrap().as_ref().unwrap().kind.is_terminal());
    run
}
fn retire(run: RunId, dry_run: bool) -> FabricCommand {
    FabricCommand::RetireRun {
        tenant: "local".into(),
        run_id: run,
        now_ms: agent_core::agent::unix_time_ms() + 100_000,
        retention_ms: 1000,
        dry_run,
    }
}
async fn ready(fabric: &dyn OperatorFabric, run: RunId) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match fabric.execute(retire(run, true)).await {
                Ok(_) => return,
                Err(FabricError::Invalid(message))
                    if message == "pending_outbox"
                        || message == "unacknowledged_run_events"
                        || message == "run_cleanup_pending" =>
                {
                    tokio::time::sleep(Duration::from_millis(10)).await
                }
                other => panic!("retirement readiness: {other:?}"),
            }
        }
    })
    .await
    .unwrap();
}
async fn contract(fabric: Arc<dyn OperatorFabric>) {
    let host = host(fabric.clone());
    let run = completed(&host).await;
    let sibling = completed(&host).await;
    let sibling_events = host.events_after(sibling, 0, 128).await.unwrap();
    ready(fabric.as_ref(), run).await;
    assert!(
        matches!(fabric.execute(FabricCommand::RetireRun { tenant:"local".into(), run_id:run, now_ms:agent_core::agent::unix_time_ms(), retention_ms:1_000_000, dry_run:false }).await, Err(FabricError::Invalid(reason)) if reason == "retention_not_elapsed")
    );
    // A foreign record holding the graph checkpoint prevents deletion.
    fabric
        .execute(FabricCommand::Transact {
            checkpoint: CheckpointWrite {
                key: "shared-root".into(),
                expected: None,
                value: json!({"graph":run_checkpoint_key("local", run)}),
            },
            outbox: vec![],
            delivery: None,
        })
        .await
        .unwrap();
    assert!(
        matches!(fabric.execute(retire(run, false)).await, Err(FabricError::Invalid(reason)) if reason == "run_still_referenced")
    );
    let FabricReply::Checkpoint(Some(saved)) = fabric
        .execute(FabricCommand::ReadCheckpoint("shared-root".into()))
        .await
        .unwrap()
    else {
        panic!("root")
    };
    fabric
        .execute(FabricCommand::Transact {
            checkpoint: CheckpointWrite {
                key: "shared-root".into(),
                expected: Some(saved.version),
                value: json!({}),
            },
            outbox: vec![],
            delivery: None,
        })
        .await
        .unwrap();
    // Unacknowledged foreign consumers and active leases also protect the run.
    let first = host
        .engine
        .events_after("local", run, 0, 1)
        .await
        .unwrap()
        .remove(0);
    let FabricReply::Event(Some(event)) = fabric
        .execute(FabricCommand::ReadEvent(first.id))
        .await
        .unwrap()
    else {
        panic!("fact")
    };
    fabric
        .execute(FabricCommand::Subscribe(SubscriptionSpec {
            consumer: "audit".into(),
            subscription: "root".into(),
            topic: event.topic,
            event_type: Some(event.envelope.event_type),
            tenant: Some("local".into()),
        }))
        .await
        .unwrap();
    assert!(
        matches!(fabric.execute(retire(run, false)).await, Err(FabricError::Invalid(reason)) if reason == "unacknowledged_run_events")
    );
    let now = agent_core::agent::unix_time_ms();
    let FabricReply::Claimed(mut claims) = fabric
        .execute(FabricCommand::ClaimForConsumer {
            consumer: "audit".into(),
            worker: "audit".into(),
            now_ms: now,
            lease_ms: 1_000_000,
            limit: 1,
            max_attempts: 3,
        })
        .await
        .unwrap()
    else {
        panic!("claim")
    };
    assert_eq!(
        fabric.execute(retire(run, false)).await.unwrap_err(),
        FabricError::Fenced
    );
    fabric
        .execute(FabricCommand::Commit {
            lease: claims.remove(0).lease,
            now_ms: now + 1,
            result: json!({}),
            outbox: vec![],
        })
        .await
        .unwrap();
    let FabricReply::Collected(dry) = fabric.execute(retire(run, true)).await.unwrap() else {
        panic!("dry run")
    };
    assert_eq!(dry.logs, 2);
    assert!(!host.events_after(run, 0, 128).await.unwrap().is_empty());
    fabric.execute(retire(run, false)).await.unwrap();
    assert!(
        matches!(host.events_after(run, 0, 128).await, Err(FabricError::Invalid(reason)) if reason == "retention_expired")
    );
    assert!(host.start(run, json!("retention")).await.is_err());
    assert_eq!(
        host.events_after(sibling, 0, 128).await.unwrap(),
        sibling_events
    );
    fabric.execute(retire(run, false)).await.unwrap();
    host.engine.stop_workers().await;
}
#[tokio::test]
async fn memory_retirement_preserves_live_roots_leases_and_siblings() {
    contract(Arc::new(MemoryFabric::default())).await;
}
#[tokio::test]
async fn sqlite_retirement_preserves_live_roots_leases_and_siblings() {
    let dir = tempfile::tempdir().unwrap();
    let store =
        agent_extension::store::SqliteEventStore::open(dir.path().join("retention.db"), "test")
            .await
            .unwrap();
    contract(Arc::new(store)).await;
}
#[tokio::test]
async fn unstarted_graph_cannot_be_retired() {
    let fabric = Arc::new(MemoryFabric::default());
    let host = host(fabric.clone());
    let run = RunId::new();
    host.engine
        .admit("local", run, json!("wait"))
        .await
        .unwrap();
    assert!(
        matches!(fabric.execute(retire(run, false)).await, Err(FabricError::Invalid(reason)) if reason == "run_not_terminal")
    );
}
