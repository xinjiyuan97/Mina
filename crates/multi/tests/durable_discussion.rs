use agent_multi::*;
use serde_json::{Value, json};
use std::time::Duration;
use uuid::Uuid;
struct Moderator(bool);
impl DiscussionModerator for Moderator {
    fn reduce(&self, _: Uuid, _: u32, turns: Vec<Message>) -> Result<Value, String> {
        if self.0 {
            return Err("unavailable".into());
        }
        assert_eq!(
            turns.iter().map(|m| m.sender.as_str()).collect::<Vec<_>>(),
            vec!["a", "b"]
        );
        Ok(json!({"accepted":2}))
    }
}
fn setup() -> (DiscussionSession, SqliteBusHandle, Topic) {
    let mut session = DiscussionSession::new(Uuid::new_v4(), 2);
    session.register("a");
    session.register("b");
    let topic = discussion_topic(session.run_id, 0);
    (
        session,
        SqliteBusHandle::new(SqliteBus::open_memory().unwrap()),
        topic,
    )
}
#[tokio::test]
async fn collects_distinct_unacked_turns_and_failure_preserves_whole_round() {
    let (session, bus, topic) = setup();
    for speaker in ["a", "b"] {
        bus.publish(&discussion_message(session.run_id, 0, speaker, json!({})))
            .unwrap();
    }
    assert!(
        session
            .collect_round_durable(
                &Moderator(true),
                bus.clone(),
                "collector",
                Duration::from_secs(1)
            )
            .await
            .is_err()
    );
    assert_eq!(bus.replay("collector", &topic, 100).unwrap().len(), 2);
    let decision = session
        .collect_round_durable(
            &Moderator(false),
            bus.clone(),
            "collector",
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(decision.payload["decision"]["accepted"], 2);
    let unread = bus.replay("collector", &topic, 100).unwrap();
    assert_eq!(unread.len(), 1); // Completion shares the round topic; inputs are acknowledged.
    assert_eq!(unread[0].1.id, decision.id);
}
#[tokio::test]
async fn duplicate_and_unknown_speakers_never_commit() {
    for speakers in [["a", "a"], ["outsider", "b"]] {
        let (session, bus, topic) = setup();
        for speaker in speakers {
            bus.publish(&discussion_message(session.run_id, 0, speaker, json!({})))
                .unwrap();
        }
        assert!(
            session
                .collect_round_durable(
                    &Moderator(false),
                    bus.clone(),
                    "collector",
                    Duration::from_secs(1)
                )
                .await
                .is_err()
        );
        assert_eq!(bus.replay("collector", &topic, 100).unwrap().len(), 2);
        assert_eq!(bus.replay("audit", &topic, 100).unwrap().len(), 2);
    }
}
#[tokio::test]
async fn missing_participant_times_out_without_reducing_or_acknowledging() {
    let (session, bus, topic) = setup();
    bus.publish(&discussion_message(session.run_id, 0, "a", json!({})))
        .unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        session.collect_round_durable(
            &Moderator(false),
            bus.clone(),
            "collector",
            Duration::from_millis(25),
        ),
    )
    .await
    .unwrap();
    assert!(result.is_err());
    assert_eq!(bus.replay("collector", &topic, 100).unwrap().len(), 1);
}

#[tokio::test]
async fn checkpoint_failure_rolls_back_decision_and_ack() {
    let (session, bus, topic) = setup();
    for speaker in ["a", "b"] {
        bus.publish(&discussion_message(session.run_id, 0, speaker, json!({})))
            .unwrap();
    }
    let path = std::env::temp_dir().join(format!("mina-discussion-{}.db", Uuid::new_v4()));
    let durable = SqliteBusHandle::new(SqliteBus::open(&path).unwrap());
    for speaker in ["a", "b"] {
        durable
            .publish(&discussion_message(session.run_id, 0, speaker, json!({})))
            .unwrap();
    }
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute_batch("CREATE TRIGGER fail_checkpoint BEFORE INSERT ON messages WHEN NEW.kind='discussion.checkpoint' BEGIN SELECT RAISE(ABORT, 'injected'); END;").unwrap();
    assert!(
        session
            .collect_round_durable(
                &Moderator(false),
                durable.clone(),
                "collector",
                Duration::from_secs(1)
            )
            .await
            .is_err()
    );
    assert_eq!(durable.replay("collector", &topic, 100).unwrap().len(), 2);
    assert_eq!(durable.replay("audit", &topic, 100).unwrap().len(), 2);
    drop(conn);
    drop(bus);
    drop(durable);
    std::fs::remove_file(path).unwrap();
}
