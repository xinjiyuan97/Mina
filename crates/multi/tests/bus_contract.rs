use agent_multi::{InMemoryBus, Message, PublishError, RecvError};
use serde_json::json;
use uuid::Uuid;

fn message(topic: &str, n: u32) -> Message {
    Message::new(Uuid::new_v4(), topic, "producer", "item", json!(n))
}

#[tokio::test]
async fn unrelated_topics_cannot_evict_or_count_as_receivers() {
    let bus = InMemoryBus::new(1);
    let mut quiet = bus.subscribe("quiet");
    let _noisy = bus.subscribe("noisy");
    assert!(matches!(
        bus.publish(message("absent", 0)),
        Err(PublishError::NoSubscribers)
    ));
    assert_eq!(bus.publish(message("quiet", 7)).unwrap(), 1);
    for i in 0..100 {
        bus.publish(message("noisy", i)).unwrap();
    }
    assert_eq!(quiet.recv().await.unwrap().payload, json!(7));
}

#[tokio::test]
async fn lag_recovers_and_closed_bus_drains_queued_messages() {
    let bus = InMemoryBus::new(2);
    let mut sub = bus.subscribe("t");
    for i in 0..4 {
        bus.publish(message("t", i)).unwrap();
    }
    assert!(matches!(sub.recv().await, Err(RecvError::Lagged(2))));
    drop(bus);
    assert_eq!(sub.recv().await.unwrap().payload, json!(2));
    assert_eq!(sub.recv().await.unwrap().payload, json!(3));
    assert!(matches!(sub.recv().await, Err(RecvError::Closed)));
}

#[tokio::test]
async fn resubscribe_does_not_replay_and_cancelled_receive_preserves_next_message() {
    let bus = InMemoryBus::new(4);
    let sub = bus.subscribe("t");
    bus.publish(message("t", 0)).unwrap();
    drop(sub);
    assert!(bus.publish(message("t", 1)).is_err());
    let mut sub = bus.subscribe("t");
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(10), sub.recv())
            .await
            .is_err()
    );
    bus.publish(message("t", 2)).unwrap();
    assert_eq!(sub.recv().await.unwrap().payload, json!(2));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_producers_deliver_identical_order_to_every_consumer() {
    let bus = InMemoryBus::new(1024);
    let mut a = bus.subscribe("t");
    let mut b = bus.subscribe("t");
    let mut producers = Vec::new();
    for producer in 0..4 {
        let bus = bus.clone();
        producers.push(tokio::spawn(async move {
            for seq in 0..100 {
                let mut m = message("t", seq);
                m.sender = producer.to_string();
                assert_eq!(bus.publish(m).unwrap(), 2);
                tokio::task::yield_now().await;
            }
        }));
    }
    for producer in producers {
        producer.await.unwrap();
    }
    let read = async {
        let mut next = [0; 4];
        let mut ids = std::collections::HashSet::new();
        for _ in 0..400 {
            let first = a.recv().await.unwrap();
            let second = b.recv().await.unwrap();
            assert_eq!(first.id, second.id);
            assert!(ids.insert(first.id));
            let p: usize = first.sender.parse().unwrap();
            assert_eq!(first.payload, json!(next[p]));
            next[p] += 1;
        }
        assert_eq!(next, [100; 4]);
    };
    tokio::time::timeout(std::time::Duration::from_secs(5), read)
        .await
        .unwrap();
}
