use agent_core::harness::{
    Agent, AgentEvent, AgentEventStream, AgentMetadata, FinishReason, RunRequest,
};
use agent_harness::{Harness, RunOptions};
use agent_multi::{
    AgentDescriptor, HarnessAgentNode, Message, SqliteBus, SqliteBusHandle, Topic,
    serve_durable_node,
};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

struct Pending;
impl Agent for Pending {
    fn metadata(&self) -> AgentMetadata {
        AgentMetadata::new("pending", "1")
    }
    fn run(&self, _: RunRequest) -> AgentEventStream {
        Box::pin(futures_util::stream::pending())
    }
}
struct Echo;
impl Agent for Echo {
    fn metadata(&self) -> AgentMetadata {
        AgentMetadata::new("echo", "1")
    }
    fn run(&self, request: RunRequest) -> AgentEventStream {
        Box::pin(futures_util::stream::iter(vec![
            AgentEvent::text_delta(request.input),
            AgentEvent::completed(FinishReason::Stop),
        ]))
    }
}
fn node<A: Agent>(agent: A) -> Arc<HarnessAgentNode<A>> {
    Arc::new(HarnessAgentNode::new(
        AgentDescriptor {
            id: "worker".into(),
            role: "worker".into(),
        },
        Harness::new(agent),
        RunOptions::default(),
    ))
}

#[tokio::test]
async fn shutdown_interrupts_pending_harness_and_reopen_replays_then_acks() {
    let path = std::env::temp_dir().join(format!("mina-durable-{}.sqlite", Uuid::new_v4()));
    let request = Message::new(
        Uuid::new_v4(),
        "tasks",
        "supervisor",
        "task.request",
        json!({"input": "hello"}),
    );
    let topic = Topic::from("tasks");
    let result_topic = Topic::from(format!("run.{}.result", request.run_id));
    {
        let bus = SqliteBusHandle::new(SqliteBus::open(&path).unwrap());
        bus.publish(&request).unwrap();
        let cancel = CancellationToken::new();
        let task = tokio::spawn(serve_durable_node(
            node(Pending),
            bus.clone(),
            "worker",
            topic.clone(),
            cancel.clone(),
        ));
        let mut results = bus.subscribe("observer", result_topic.clone());
        let (_, started) = tokio::time::timeout(Duration::from_secs(2), results.recv_wait())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(started.kind, "task.started");
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let retained = bus.replay("worker", &topic, 10).unwrap();
        assert_eq!(retained.len(), 1);
        assert_eq!(retained[0].1.id, request.id);
        assert!(
            bus.replay("audit", &result_topic, 100)
                .unwrap()
                .iter()
                .all(|(_, m)| m.kind != "task.cancelled")
        );
    }
    {
        let bus = SqliteBusHandle::new(SqliteBus::open(&path).unwrap());
        assert_eq!(
            bus.replay("worker", &topic, 10).unwrap()[0].1.id,
            request.id
        );
        let cancel = CancellationToken::new();
        let task = tokio::spawn(serve_durable_node(
            node(Echo),
            bus.clone(),
            "worker",
            topic.clone(),
            cancel.clone(),
        ));
        tokio::time::timeout(Duration::from_secs(2), async {
            while !bus.replay("worker", &topic, 10).unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        cancel.cancel();
        task.await.unwrap().unwrap();
        let events = bus.replay("audit", &result_topic, 100).unwrap();
        let terminal = events
            .iter()
            .find(|(_, m)| m.kind == "task.completed")
            .unwrap();
        assert_eq!(terminal.1.correlation_id, Some(request.id));
    }
    let reopened = SqliteBus::open(&path).unwrap();
    assert!(
        reopened
            .read_after("worker", &topic, 10)
            .unwrap()
            .is_empty()
    );
    drop(reopened);
    std::fs::remove_file(path).unwrap();
}
