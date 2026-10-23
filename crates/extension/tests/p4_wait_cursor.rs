#![cfg(feature = "sqlite")]

use agent_core::{event_runtime::*, harness::RunId};
use agent_extension::store::SqliteEventStore;
use serde_json::json;

fn event(run_id: RunId, time: i64) -> PublishEvent {
    PublishEvent {
        event_id: EventId::new(),
        topic: "external.completed".into(),
        event_type: "external.completed".into(),
        schema_version: 1,
        source: EventSource::Gateway,
        subject: Some(format!("run/{run_id}")),
        correlation_id: Some(run_id.to_string()),
        causation_id: None,
        occurred_at_ms: time,
        recorded_at_ms: time,
        payload: json!({"time":time}),
    }
}

#[tokio::test]
async fn recorded_since_replays_registration_gap_but_not_old_events_and_preserves_now() {
    let directory = tempfile::tempdir().expect("temp");
    let path = directory.path().join("events.db");
    let run_id = RunId::new();
    let early = event(run_id, 9);
    let gap = event(run_id, 10);
    {
        let store = SqliteEventStore::open(&path, "p4").await.expect("open");
        store.publish(early.clone()).await.expect("old");
        store.publish(gap.clone()).await.expect("gap");
    }
    let request = CreateSubscription {
        subscription_id: SubscriptionId::new(),
        owner: SubscriptionOwner::Run { run_id },
        scope: SubscriptionScope::Run { run_id },
        filter: EventFilter {
            topics: vec!["external.completed".into()],
            sources: vec![EventSource::Gateway],
            correlation_id: Some(run_id.to_string()),
            ..Default::default()
        },
        delivery: DeliveryTarget::WakeRun {
            run_id,
            wait_key: "wait".into(),
        },
        mode: SubscriptionMode::Continuous,
        start_position: StartPosition::RecordedSince { recorded_at_ms: 10 },
        expires_at_ms: None,
        max_deliveries: None,
        created_at_ms: 10,
    };
    {
        let store = SqliteEventStore::open(&path, "p4")
            .await
            .expect("reopen register");
        store.subscribe(request.clone()).await.expect("register");
        let mut legacy = request.clone();
        legacy.subscription_id = SubscriptionId::new();
        legacy.start_position = StartPosition::Now;
        store.subscribe(legacy).await.expect("legacy Now");
    }
    let store = SqliteEventStore::open(&path, "p4")
        .await
        .expect("reopen claim");
    store
        .subscribe(request.clone())
        .await
        .expect("idempotent registration");
    let deliveries = store
        .claim_deliveries(ClaimDeliveries {
            worker_id: "worker".into(),
            now_ms: 20,
            lease_until_ms: 30,
            limit: 10,
        })
        .await
        .expect("claim");
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].delivery_id.event_id, gap.event_id);
    assert_eq!(
        deliveries[0].delivery_id.subscription_id,
        request.subscription_id
    );
    let old = store
        .publish(event(run_id, 8))
        .await
        .expect("out of order old timestamp");
    assert!(
        !old.delivery_ids
            .iter()
            .any(|delivery| delivery.subscription_id == request.subscription_id)
    );
}
