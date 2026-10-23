use agent_core::{event_runtime::EventId, operator_fabric::*};
use agent_extension::operator_fabric::MemoryFabric;
use agent_harness::operator_fabric::*;
use serde_json::json;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

fn event() -> FabricEvent {
    FabricEvent {
        topic: "test".into(),
        envelope: serde_json::from_value(json!({
            "schema_version":1,"engine_version":"operator_v1","projection_version":1,
            "event_id":EventId::new(),"event_type":"test.fact","tenant_id":"tenant",
            "run_id":"00000000-0000-0000-0000-000000000001","session_id":null,
            "effect_id":"effect","invocation_id":"invocation","attempt":1,
            "issued_revision":0,"lease_epoch":1,"correlation_id":"test","causation_id":null,
            "operator":{"name":"test","version":1},"operator_seq":1,
            "delivery_class":"durable","occurred_at_ms":0,"payload":{}
        }))
        .expect("envelope"),
        payload_schema: json!({"type":"object"}),
    }
}

fn runtime() -> FabricRuntime {
    FabricRuntime {
        config: FabricRuntimeConfig {
            worker: "worker".into(),
            claim_limit: 8,
            lease_ms: 10,
            max_attempts: 2,
            backoff_ms: 5,
            max_backoff_ms: 20,
        },
    }
}

async fn setup(store: &dyn OperatorFabric, subscriptions: usize) {
    store
        .execute(FabricCommand::Publish(event()))
        .await
        .expect("publish");
    for index in 0..subscriptions {
        store
            .execute(FabricCommand::Subscribe(SubscriptionSpec {
                consumer: "consumer".into(),
                subscription: format!("sub-{index}"),
                topic: "test".into(),
                event_type: None,
                tenant: None,
            }))
            .await
            .expect("subscribe");
    }
}

struct Handler {
    calls: AtomicUsize,
    fail: bool,
    clock: AtomicI64,
    advance_clock: bool,
}

impl FabricHandler for Handler {
    fn handle(&self, delivery: ClaimedDelivery) -> HandlerFuture<'_> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.advance_clock {
                self.clock.store(10, Ordering::SeqCst);
            }
            if self.fail {
                return Err("injected handler failure".into());
            }
            let mut next = delivery.event;
            next.envelope.event_id = EventId::stable("runtime-test", "outbox");
            next.envelope.operator_seq.0 += 1;
            next.topic = "output".into();
            Ok(HandlerOutput {
                result: json!({"saved":true}),
                outbox: vec![next],
            })
        })
    }
}

fn handler(fail: bool, advance_clock: bool) -> Handler {
    Handler {
        calls: AtomicUsize::new(0),
        fail,
        clock: AtomicI64::new(0),
        advance_clock,
    }
}

#[tokio::test]
async fn coordinator_commits_once_for_duplicate_deliveries() {
    let store = MemoryFabric::default();
    setup(&store, 2).await;
    let handler = handler(false, false);
    let report = runtime().tick(&store, &handler, || 0).await.expect("tick");
    assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
    assert_eq!(report.committed, 2);
    assert_eq!(report.reused, 1);
    assert_eq!(report.dispatched, 1);
    assert_eq!(
        runtime()
            .tick(&store, &handler, || 1)
            .await
            .expect("tick twice"),
        FabricTickReport::default()
    );
}

#[tokio::test]
async fn coordinator_backoff_then_dlq() {
    let store = MemoryFabric::default();
    setup(&store, 1).await;
    let handler = handler(true, false);
    assert_eq!(
        runtime()
            .tick(&store, &handler, || 0)
            .await
            .expect("retry")
            .retried,
        1
    );
    assert_eq!(
        runtime().tick(&store, &handler, || 4).await.expect("wait"),
        FabricTickReport::default()
    );
    assert_eq!(
        runtime()
            .tick(&store, &handler, || 5)
            .await
            .expect("dlq")
            .dead_lettered,
        1
    );
    assert_eq!(
        runtime()
            .tick(&store, &handler, || 100)
            .await
            .expect("no infinite retry"),
        FabricTickReport::default()
    );
    assert_eq!(handler.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn coordinator_checks_time_after_handler() {
    let store = MemoryFabric::default();
    setup(&store, 1).await;
    let handler = handler(false, true);
    let report = runtime()
        .tick(&store, &handler, || handler.clock.load(Ordering::SeqCst))
        .await
        .expect("expired handler");
    assert_eq!(report.fenced, 1);
    assert_eq!(report.committed, 0);
    assert_eq!(report.dispatched, 0);
}

#[tokio::test]
async fn coordinator_sqlite_receipt_survives_reopen() {
    use agent_extension::store::SqliteEventStore;
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("events.db");
    let store = SqliteEventStore::open(&path, "p1").await.expect("open");
    setup(&store, 1).await;
    let handler = handler(false, false);
    assert_eq!(
        runtime()
            .tick(&store, &handler, || 0)
            .await
            .expect("commit")
            .committed,
        1
    );
    drop(store);
    let store = SqliteEventStore::open(&path, "p1").await.expect("reopen");
    store
        .execute(FabricCommand::Subscribe(SubscriptionSpec {
            consumer: "consumer".into(),
            subscription: "late".into(),
            topic: "test".into(),
            event_type: None,
            tenant: None,
        }))
        .await
        .expect("late subscriber");
    let report = runtime()
        .tick(&store, &handler, || 1)
        .await
        .expect("reused durable receipt");
    assert_eq!(report.reused, 1);
    assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
}
