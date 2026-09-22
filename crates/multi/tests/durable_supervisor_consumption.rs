use agent_multi::*;
use serde_json::json;
use uuid::Uuid;

fn worker() -> AgentDescriptor {
    AgentDescriptor {
        id: "worker".into(),
        role: "worker".into(),
    }
}

#[tokio::test]
async fn storage_errors_are_not_reported_as_empty_or_timeout() {
    let path = std::env::temp_dir().join(format!("mina-corrupt-{}.db", Uuid::new_v4()));
    let supervisor = Supervisor::new(Uuid::new_v4(), InMemoryBus::new(8));
    let bus = SqliteBusHandle::new(SqliteBus::open(&path).unwrap());
    let id = supervisor
        .dispatch_durable(&bus, &worker(), json!({}))
        .unwrap();
    let event = lifecycle_event(
        &supervisor.task(id).unwrap().request,
        "worker",
        event_kind::TASK_COMPLETED,
        json!({}),
    );
    bus.publish(&event).unwrap();
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute(
        "UPDATE messages SET payload='invalid-json' WHERE id=?",
        [event.id.to_string()],
    )
    .unwrap();
    assert!(
        supervisor
            .consume_durable_once(bus.clone(), "owner")
            .await
            .is_err()
    );
    assert!(
        supervisor
            .run_durable_until_terminal(
                bus.clone(),
                "owner",
                &[id],
                std::time::Duration::from_secs(1)
            )
            .await
            .is_err()
    );
    assert_eq!(supervisor.status(id), Some(TaskStatus::Pending));
    conn.execute(
        "UPDATE messages SET payload='{}' WHERE id=?",
        [event.id.to_string()],
    )
    .unwrap();
    assert!(
        supervisor
            .consume_durable_once(bus.clone(), "owner")
            .await
            .unwrap()
    );
    assert_eq!(supervisor.status(id), Some(TaskStatus::Completed));
    drop(conn);
    drop(bus);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn rejected_and_duplicate_events_do_not_block_later_results() {
    let supervisor = Supervisor::new(Uuid::new_v4(), InMemoryBus::new(8));
    let bus = SqliteBusHandle::new(SqliteBus::open_memory().unwrap());
    let first = supervisor
        .dispatch_durable(&bus, &worker(), json!({}))
        .unwrap();
    let second = supervisor
        .dispatch_durable(&bus, &worker(), json!({}))
        .unwrap();
    let request = supervisor.task(first).unwrap().request;
    let mut foreign = lifecycle_event(&request, "foreign", event_kind::TASK_COMPLETED, json!({}));
    foreign.run_id = Uuid::new_v4();
    bus.publish(&foreign).unwrap();
    let completed = lifecycle_event(&request, "worker", event_kind::TASK_COMPLETED, json!({}));
    bus.publish(&completed).unwrap();
    let mut duplicate = completed.clone();
    duplicate.id = Uuid::new_v4();
    bus.publish(&duplicate).unwrap();
    bus.publish(&lifecycle_event(
        &supervisor.task(second).unwrap().request,
        "worker",
        event_kind::TASK_COMPLETED,
        json!({}),
    ))
    .unwrap();
    for _ in 0..4 {
        assert!(
            supervisor
                .consume_durable_once(bus.clone(), "owner")
                .await
                .unwrap()
        );
    }
    assert_eq!(supervisor.status(second), Some(TaskStatus::Completed));
    assert!(
        !supervisor
            .consume_durable_once(bus.clone(), "owner")
            .await
            .unwrap()
    );
    // Acknowledgement belongs to this consumer; other readers retain all events.
    assert_eq!(
        bus.replay("audit", &supervisor.result_topic(), 10)
            .unwrap()
            .len(),
        4
    );
}

#[tokio::test]
async fn failed_dependency_publication_is_reported_and_replay_can_retry_it() {
    let path = std::env::temp_dir().join(format!("mina-consume-{}.db", Uuid::new_v4()));
    let supervisor = Supervisor::new(Uuid::new_v4(), InMemoryBus::new(8));
    let bus = SqliteBusHandle::new(SqliteBus::open(&path).unwrap());
    let task = supervisor
        .dispatch_durable(&bus, &worker(), json!({}))
        .unwrap();
    let dependent = supervisor.schedule_after(&worker(), json!({}), &[task]);
    bus.publish(&lifecycle_event(
        &supervisor.task(task).unwrap().request,
        "worker",
        event_kind::TASK_COMPLETED,
        json!({}),
    ))
    .unwrap();
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute_batch("CREATE TRIGGER fail_dispatch BEFORE INSERT ON messages WHEN NEW.kind='task.request' BEGIN SELECT RAISE(ABORT, 'injected dispatch failure'); END;").unwrap();
    assert!(
        supervisor
            .consume_durable_once(bus.clone(), "owner")
            .await
            .is_err()
    );
    assert_eq!(supervisor.task(dependent).unwrap().attempts, 0);
    assert_eq!(
        bus.replay("owner", &supervisor.result_topic(), 10)
            .unwrap()
            .len(),
        1
    );
    conn.execute_batch("DROP TRIGGER fail_dispatch;").unwrap();
    assert!(
        supervisor
            .consume_durable_once(bus.clone(), "owner")
            .await
            .unwrap()
    );
    assert_eq!(supervisor.task(dependent).unwrap().attempts, 1);
    assert_eq!(
        bus.replay("worker", &supervisor.worker_topic(&worker()), 10)
            .unwrap()
            .len(),
        2
    );
    drop(conn);
    drop(bus);
    std::fs::remove_file(path).unwrap();
}
