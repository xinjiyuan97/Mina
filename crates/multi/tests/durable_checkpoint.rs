use agent_multi::{DiscussionSession, SqliteBus, SqliteBusHandle};
use uuid::Uuid;

#[tokio::test]
async fn discussion_checkpoint_survives_reopen() {
    let path = std::env::temp_dir().join(format!("mina-checkpoint-{}.db", Uuid::new_v4()));
    let run = Uuid::new_v4();
    let mut session = DiscussionSession::new(run, 3);
    session.register("a");
    session.advance();
    let bus = SqliteBusHandle::new(SqliteBus::open(&path).unwrap());
    session.persist_checkpoint(&bus).await.unwrap();
    drop(bus);
    let reopened = SqliteBusHandle::new(SqliteBus::open(&path).unwrap());
    let restored = DiscussionSession::restore_checkpoint_durable(&reopened, run, "recovery")
        .await
        .unwrap();
    assert_eq!(restored.checkpoint(), session.checkpoint());
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn checkpoint_ids_are_idempotent_per_state_but_change_on_progress() {
    let bus = SqliteBusHandle::new(SqliteBus::open_memory().unwrap());
    let mut session = DiscussionSession::new(Uuid::new_v4(), 3);
    session.register("a");
    let first = session.persist_checkpoint(&bus).await.unwrap();
    let repeated = session.persist_checkpoint(&bus).await.unwrap();
    assert_eq!(first, repeated);
    session.advance();
    let next = session.persist_checkpoint(&bus).await.unwrap();
    assert_ne!(first, next);
}
