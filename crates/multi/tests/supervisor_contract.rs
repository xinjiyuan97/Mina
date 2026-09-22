use agent_multi::*;
use serde_json::json;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
fn worker() -> AgentDescriptor {
    AgentDescriptor {
        id: "w".into(),
        role: "worker".into(),
    }
}

#[tokio::test]
async fn durable_runtime_timeout_persists_cancel_request() {
    let bus = InMemoryBus::new(8);
    let s = Supervisor::new(Uuid::new_v4(), bus);
    let path = std::env::temp_dir().join(format!("mina-supervisor-{}.db", Uuid::new_v4()));
    let durable = SqliteBusHandle::new(SqliteBus::open(&path).unwrap());
    let id = s
        .dispatch_durable(&durable, &worker(), json!({"input":"x"}))
        .unwrap();
    assert!(
        !s.run_durable_until_terminal(
            durable.clone(),
            "supervisor",
            &[id],
            Duration::from_millis(10)
        )
        .await
        .unwrap()
    );
    let rows = durable
        .replay("worker", &s.worker_topic(&worker()), 10)
        .unwrap();
    assert!(rows.iter().any(|(_, m)| m.kind == "task.cancel"));
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn durable_runtime_accepts_worker_cancelled_terminal_event() {
    let s = std::sync::Arc::new(Supervisor::new(Uuid::new_v4(), InMemoryBus::new(8)));
    let path = std::env::temp_dir().join(format!("mina-supervisor-{}.db", Uuid::new_v4()));
    let durable = SqliteBusHandle::new(SqliteBus::open(&path).unwrap());
    let id = s
        .dispatch_durable(&durable, &worker(), json!({"input":"x"}))
        .unwrap();
    let request = durable
        .replay("worker", &s.worker_topic(&worker()), 10)
        .unwrap()
        .remove(0)
        .1;
    let event = lifecycle_event(&request, "w", event_kind::TASK_CANCELLED, json!({}));
    durable.publish(&event).unwrap();
    assert!(
        s.run_durable_until_terminal(durable, "supervisor", &[id], Duration::from_millis(100))
            .await
            .unwrap()
    );
    assert_eq!(s.status(id), Some(TaskStatus::Cancelled));
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn durable_retry_runtime_stops_at_attempt_limit() {
    let s = Supervisor::new(Uuid::new_v4(), InMemoryBus::new(8));
    let bus = SqliteBusHandle::new(SqliteBus::open_memory().unwrap());
    let id = s.dispatch_durable(&bus, &worker(), json!({})).unwrap();
    let req = s.task(id).unwrap().request;
    bus.publish(&lifecycle_event(
        &req,
        "w",
        event_kind::TASK_FAILED,
        json!({}),
    ))
    .unwrap();
    let policy = RetryPolicy {
        max_attempts: 1,
        initial_delay: Duration::ZERO,
        max_delay: Duration::ZERO,
    };
    assert!(
        !s.run_durable_with_retry(bus, "owner", &[id], Duration::from_millis(100), policy)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn retry_republishes_and_rejects_previous_attempt_events() {
    let bus = InMemoryBus::new(16);
    let s = Supervisor::new(Uuid::new_v4(), bus.clone());
    let mut tasks = bus.subscribe(s.worker_topic(&worker()));
    let (id, _) = s
        .dispatch_with_id(&worker(), json!({"input":"work"}))
        .unwrap();
    let initial = tasks.recv().await.unwrap();
    assert!(matches!(s.retry(id, 2), Err(RetryError::NotFailed)));
    let failed = lifecycle_event(&initial, "w", event_kind::TASK_FAILED, json!({}));
    assert!(s.mark_from_event(&failed));
    let next = s.retry(id, 2).unwrap();
    let retried = tasks.recv().await.unwrap();
    assert_eq!(retried.id, next);
    assert_ne!(next, id);
    assert_eq!(retried.payload, initial.payload);
    assert!(!s.mark_from_event(&failed));
    assert_eq!(s.status(id), Some(TaskStatus::Pending));
    assert!(s.mark_from_event(&lifecycle_event(
        &retried,
        "w",
        event_kind::TASK_FAILED,
        json!({})
    )));
    assert!(matches!(s.retry(id, 2), Err(RetryError::Exhausted)));
    assert_eq!(s.task(id).unwrap().attempts, 2);
}

#[tokio::test]
async fn failed_retry_publish_preserves_record_and_foreign_events_are_ignored() {
    let bus = InMemoryBus::new(8);
    let s = Supervisor::new(Uuid::new_v4(), bus.clone());
    let mut tasks = bus.subscribe(s.worker_topic(&worker()));
    let (id, _) = s.dispatch_with_id(&worker(), json!({})).unwrap();
    let request = tasks.recv().await.unwrap();
    let good = lifecycle_event(&request, "w", event_kind::TASK_FAILED, json!("failed"));
    for variant in 0..4 {
        let mut bad = good.clone();
        match variant {
            0 => bad.run_id = Uuid::new_v4(),
            1 => bad.sender = "stranger".into(),
            2 => bad.topic = "wrong".into(),
            _ => bad.correlation_id = None,
        }
        assert!(!s.mark_from_event(&bad));
    }
    assert_eq!(s.status(id), Some(TaskStatus::Pending));
    assert!(s.mark_from_event(&good));
    drop(tasks);
    assert!(matches!(s.retry(id, 3), Err(RetryError::Publish(_))));
    assert_eq!(s.task(id).unwrap().attempts, 1);
    assert_eq!(s.task(id).unwrap().result.unwrap().id, good.id);
    assert!(!s.mark_from_event(&lifecycle_event(
        &request,
        "w",
        event_kind::TASK_COMPLETED,
        json!({})
    )));
}

#[tokio::test]
async fn aggregation_retains_out_of_order_results_and_wait_is_bounded() {
    let bus = InMemoryBus::new(16);
    let s = Supervisor::new(Uuid::new_v4(), bus.clone());
    let mut tasks = bus.subscribe(s.worker_topic(&worker()));
    let mut results = bus.subscribe(s.result_topic());
    let ids = s
        .dispatch_batch(&[(worker(), json!(1)), (worker(), json!(2))])
        .unwrap();
    let a = tasks.recv().await.unwrap();
    let b = tasks.recv().await.unwrap();
    bus.publish(lifecycle_event(
        &b,
        "w",
        event_kind::TASK_CANCELLED,
        json!(2),
    ))
    .unwrap();
    bus.publish(lifecycle_event(
        &a,
        "w",
        event_kind::TASK_COMPLETED,
        json!(1),
    ))
    .unwrap();
    let cancel = CancellationToken::new();
    let values = s
        .wait_for_tasks(&mut results, &ids, Duration::from_secs(1), &cancel)
        .await
        .unwrap();
    assert_eq!(values[0].payload, json!(1));
    assert_eq!(values[1].payload, json!(2));
    assert_eq!(
        s.wait_for_task(&mut results, ids[1]).await.unwrap().id,
        values[1].id
    );
    let (waiting, _) = s.dispatch_with_id(&worker(), json!(3)).unwrap();
    assert!(matches!(
        s.wait_for_tasks(&mut results, &[waiting], Duration::from_millis(5), &cancel)
            .await,
        Err(WaitError::Timeout)
    ));
    cancel.cancel();
    assert!(matches!(
        s.wait_for_tasks(&mut results, &[waiting], Duration::from_secs(1), &cancel)
            .await,
        Err(WaitError::Cancelled)
    ));
    assert_eq!(s.status(waiting), Some(TaskStatus::Pending));
}

#[tokio::test]
async fn single_task_wait_timeout_is_bounded_and_validates_id() {
    let bus = InMemoryBus::new(4);
    let s = Supervisor::new(Uuid::new_v4(), bus.clone());
    let mut results = bus.subscribe(s.result_topic());
    assert!(matches!(
        s.wait_for_task_timeout(&mut results, Uuid::new_v4(), Duration::from_millis(1))
            .await,
        Err(WaitError::UnknownTask)
    ));
    let _worker_sub = bus.subscribe(s.worker_topic(&worker()));
    let (id, _) = s.dispatch_with_id(&worker(), json!({})).unwrap();
    assert!(matches!(
        s.wait_for_task_timeout(&mut results, id, Duration::from_millis(5))
            .await,
        Err(WaitError::Timeout)
    ));
}

#[test]
fn partial_batch_exposes_dispatched_work() {
    let bus = InMemoryBus::new(8);
    let s = Supervisor::new(Uuid::new_v4(), bus.clone());
    let _tasks = bus.subscribe(s.worker_topic(&worker()));
    let missing = AgentDescriptor {
        id: "missing".into(),
        role: "worker".into(),
    };
    let error = s
        .dispatch_batch(&[(worker(), json!(1)), (missing, json!(2))])
        .unwrap_err();
    assert_eq!(error.dispatched.len(), 1);
    assert_eq!(s.status(error.dispatched[0]), Some(TaskStatus::Pending));
}

#[test]
fn registered_dispatch_rejects_unknown_workers_before_publish() {
    let bus = InMemoryBus::new(4);
    let mut s = Supervisor::new(Uuid::new_v4(), bus.clone());
    let known = worker();
    s.add_worker(known.clone());
    let _sub = bus.subscribe(s.worker_topic(&known));
    assert_eq!(s.dispatch_registered(&known, json!(1)).unwrap(), 1);
    let unknown = AgentDescriptor {
        id: "unknown".into(),
        role: "worker".into(),
    };
    assert!(matches!(
        s.dispatch_registered(&unknown, json!(2)),
        Err(DispatchError::UnknownWorker)
    ));
}

#[test]
fn supervisor_checkpoint_restores_task_records() {
    let bus = InMemoryBus::new(8);
    let s = Supervisor::new(Uuid::new_v4(), bus.clone());
    let _sub = bus.subscribe(s.worker_topic(&worker()));
    let (id, _) = s.dispatch_with_id(&worker(), json!({"x":1})).unwrap();
    let snapshot = s.checkpoint().unwrap();
    let restored = Supervisor::new(s.run_id, bus.clone());
    restored.restore_checkpoint(snapshot).unwrap();
    let record = restored.task(id).unwrap();
    assert_eq!(record.status, TaskStatus::Pending);
    assert_eq!(record.attempts, 1);
    assert_eq!(record.request.payload, json!({"x":1}));
}

#[test]
fn supervisor_checkpoint_rejects_duplicate_records() {
    let bus = InMemoryBus::new(4);
    let s = Supervisor::new(Uuid::new_v4(), bus.clone());
    let _sub = bus.subscribe(s.worker_topic(&worker()));
    let (_id, _) = s.dispatch_with_id(&worker(), json!({})).unwrap();
    let mut snapshot: serde_json::Value = s.checkpoint().unwrap();
    let duplicate = snapshot.as_array().unwrap()[0].clone();
    snapshot.as_array_mut().unwrap().push(duplicate);
    assert!(s.restore_checkpoint(snapshot).is_err());
}

#[test]
fn restored_failed_task_can_continue_bounded_retry() {
    let bus = InMemoryBus::new(8);
    let run = Uuid::new_v4();
    let original = Supervisor::new(run, bus.clone());
    let mut sub = bus.subscribe(original.worker_topic(&worker()));
    let (task_id, _) = original
        .dispatch_with_id(&worker(), json!({"x":1}))
        .unwrap();
    let request = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(sub.recv())
        .unwrap();
    assert!(original.mark_from_event(&lifecycle_event(
        &request,
        "w",
        event_kind::TASK_FAILED,
        json!({})
    )));
    let snapshot = original.checkpoint().unwrap();
    let restored = Supervisor::new(run, bus.clone());
    restored.restore_checkpoint(snapshot).unwrap();
    let next_id = restored.retry(task_id, 2).unwrap();
    let retried = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(sub.recv())
        .unwrap();
    assert_eq!(next_id, retried.id);
    assert_eq!(restored.task(task_id).unwrap().attempts, 2);
}

#[tokio::test]
async fn cancel_publishes_request_and_waits_for_worker_terminal() {
    let bus = InMemoryBus::new(8);
    let s = Supervisor::new(Uuid::new_v4(), bus.clone());
    let mut sub = bus.subscribe(s.worker_topic(&worker()));
    let (id, _) = s
        .dispatch_with_id(&worker(), json!({"input":"long"}))
        .unwrap();
    let task = sub.recv().await.unwrap();
    assert_eq!(s.cancel(id).unwrap(), 1);
    let cancel = sub.recv().await.unwrap();
    assert_eq!(cancel.kind, "task.cancel");
    assert_eq!(cancel.correlation_id, Some(task.id));
    assert_eq!(s.status(id), Some(TaskStatus::Pending));
    let terminal = lifecycle_event(&task, "w", event_kind::TASK_CANCELLED, json!({}));
    assert!(s.mark_from_event(&terminal));
    assert_eq!(s.status(id), Some(TaskStatus::Cancelled));
    assert!(matches!(s.cancel(id), Err(CancelError::AlreadyTerminal)));
}

#[test]
fn cancel_all_targets_only_non_terminal_tasks() {
    let bus = InMemoryBus::new(16);
    let s = Supervisor::new(Uuid::new_v4(), bus.clone());
    let mut subscriptions = bus.subscribe(s.worker_topic(&worker()));
    let (done, _) = s.dispatch_with_id(&worker(), json!(1)).unwrap();
    let done_request = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(subscriptions.recv())
        .unwrap();
    let (_pending, _) = s.dispatch_with_id(&worker(), json!(2)).unwrap();
    let _ = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(subscriptions.recv())
        .unwrap();
    assert!(s.mark_from_event(&lifecycle_event(
        &done_request,
        "w",
        event_kind::TASK_COMPLETED,
        json!({})
    )));
    let cancelled = s.cancel_all();
    assert_eq!(cancelled.len(), 1);
    assert!(cancelled[0].1.is_ok());
    assert_eq!(s.status(done), Some(TaskStatus::Completed));
}

#[tokio::test]
async fn scheduled_dependency_releases_after_prerequisite_completion() {
    let bus = InMemoryBus::new(8);
    let s = Supervisor::new(Uuid::new_v4(), bus.clone());
    let mut sub = bus.subscribe(s.worker_topic(&worker()));
    let (parent, _) = s.dispatch_with_id(&worker(), json!({"input":"a"})).unwrap();
    let child = s.schedule_after(&worker(), json!({"input":"b"}), &[parent]);
    assert!(s.release_ready().unwrap().is_empty());
    let event = lifecycle_event(
        &s.task(parent).unwrap().request,
        "w",
        "task.completed",
        json!({}),
    );
    assert!(s.mark_from_event(&event));
    let released = s.release_ready().unwrap();
    assert_eq!(released, vec![child]);
    assert_eq!(
        sub.recv().await.unwrap().id,
        s.task(parent).unwrap().request.id
    );
    assert_eq!(
        sub.recv().await.unwrap().id,
        s.task(child).unwrap().request.id
    );
}
