use agent_multi::*;
use serde_json::json;
use uuid::Uuid;

#[tokio::test]
async fn supervisor_checkpoint_survives_reopen() {
    let path = std::env::temp_dir().join(format!("mina-sup-checkpoint-{}.db", Uuid::new_v4()));
    let run = Uuid::new_v4();
    let s = Supervisor::new(run, InMemoryBus::new(8));
    let bus = SqliteBusHandle::new(SqliteBus::open(&path).unwrap());
    let worker = AgentDescriptor {
        id: "w".into(),
        role: "worker".into(),
    };
    let id = s
        .dispatch_durable(&bus, &worker, json!({"input":"x"}))
        .unwrap();
    let req = s.task(id).unwrap().request;
    assert!(s.mark_from_event(&lifecycle_event(
        &req,
        "w",
        event_kind::TASK_FAILED,
        json!("err")
    )));
    s.persist_checkpoint(&bus).await.unwrap();
    let restored = Supervisor::new(run, InMemoryBus::new(8));
    restored
        .restore_checkpoint_durable(&bus, "recovery")
        .await
        .unwrap();
    assert_eq!(restored.status(id), Some(TaskStatus::Failed));
    assert_eq!(restored.task(id).unwrap().attempts, 1);
    drop(bus);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn supervisor_rejects_checkpoint_from_another_run() {
    let bus = SqliteBusHandle::new(SqliteBus::open_memory().unwrap());
    let a = Supervisor::new(Uuid::new_v4(), InMemoryBus::new(2));
    let b = Supervisor::new(Uuid::new_v4(), InMemoryBus::new(2));
    a.persist_checkpoint(&bus).await.unwrap();
    assert!(
        b.restore_checkpoint_durable(&bus, "recovery")
            .await
            .is_err()
    );
}
