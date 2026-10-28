#![cfg(feature = "sqlite")]
use agent_core::contracts::{fabric::*, host::*};
use agent_extension::store::*;
use serde_json::json;
use std::time::Duration;
#[tokio::test]
async fn explicit_options_validate_schema_and_connection_ownership() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("options.db");
    let options = SqliteOpenOptions {
        journal_mode: SqliteJournalMode::Delete,
        synchronous: SqliteSynchronous::Normal,
        busy_timeout: Duration::from_millis(25),
        ..Default::default()
    };
    let store = SqliteEventStore::open_with_options(&path, "test", options.clone())
        .await
        .unwrap();
    let actual = store.configuration().await.unwrap();
    assert_eq!(actual.journal_mode, "delete");
    assert_eq!(actual.synchronous, 1);
    assert_eq!(actual.busy_timeout_ms, 25);
    assert!(actual.foreign_keys);
    let validate = SqliteOpenOptions {
        migration: SqliteMigration::ValidateOnly,
        ..options.clone()
    };
    assert!(
        SqliteEventStore::open_with_options(&path, "test", validate.clone())
            .await
            .is_err()
    );
    store
        .execute(FabricCommand::ReadCheckpoint("init".into()))
        .await
        .unwrap();
    SqliteEventStore::open_with_options(&path, "test", validate)
        .await
        .unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute_batch("BEGIN").unwrap();
    assert!(
        SqliteEventStore::from_connection(connection, "test", options)
            .await
            .is_err()
    );
    assert!(
        SqliteEventStore::from_connection(
            rusqlite::Connection::open_in_memory().unwrap(),
            "test",
            SqliteOpenOptions::default()
        )
        .await
        .is_err()
    );
    let wal = SqliteEventStore::open_with_options(
        dir.path().join("wal.db"),
        "test",
        SqliteOpenOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(wal.configuration().await.unwrap().synchronous, 2);
    assert_eq!(wal.configuration().await.unwrap().journal_mode, "wal");
}
#[tokio::test]
async fn host_record_and_checkpoint_rollback_on_real_commit_failure() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rollback.db");
    let store = SqliteEventStore::open(&path, "test").await.unwrap();
    store
        .execute(FabricCommand::ReadCheckpoint("init".into()))
        .await
        .unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute_batch("CREATE TABLE parent(id INTEGER PRIMARY KEY); CREATE TABLE child(id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED); CREATE TRIGGER fail_host AFTER INSERT ON workflow_fabric_records WHEN NEW.section='checkpoints' AND NEW.identity LIKE 'host-record:%' BEGIN INSERT INTO child VALUES(1); END;").unwrap();
    let record = HostRecord {
        tenant: "local".into(),
        namespace: "session".into(),
        run_id: None,
        operation_id: "1".into(),
        schema_version: 1,
        payload: json!("final"),
    };
    let command = FabricCommand::HostTransact {
        records: vec![record],
        completion: None,
        transaction: Box::new(FabricCommand::Transact {
            checkpoint: CheckpointWrite {
                key: "state".into(),
                expected: None,
                value: json!(1),
            },
            outbox: vec![],
            delivery: None,
        }),
    };
    assert!(store.execute(command).await.is_err());
    drop(store);
    let store = SqliteEventStore::open(&path, "test").await.unwrap();
    assert_eq!(
        store
            .execute(FabricCommand::ReadCheckpoint("state".into()))
            .await
            .unwrap(),
        FabricReply::Checkpoint(None)
    );
    assert_eq!(
        store
            .execute(FabricCommand::ReadCheckpoint(host_record_key(
                "local", "session", "1"
            )))
            .await
            .unwrap(),
        FabricReply::Checkpoint(None)
    );
}
#[tokio::test]
async fn maintenance_survives_reopen_and_fences_independent_connections() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fenced.db");
    let first = SqliteEventStore::open(&path, "a").await.unwrap();
    let other = SqliteEventStore::open(&path, "b").await.unwrap();
    first
        .execute(FabricCommand::BeginMaintenance {
            token: "owner".into(),
            now_ms: 0,
        })
        .await
        .unwrap();
    let write = || FabricCommand::Transact {
        checkpoint: CheckpointWrite {
            key: "blocked".into(),
            expected: None,
            value: json!(1),
        },
        outbox: vec![],
        delivery: None,
    };
    assert!(matches!(
        other.execute(write()).await,
        Err(FabricError::Fenced)
    ));
    drop(first);
    drop(other);
    let reopened = SqliteEventStore::open(&path, "c").await.unwrap();
    assert!(reopened.execute(write()).await.is_err());
    assert!(
        reopened
            .execute(FabricCommand::EndMaintenance {
                token: "wrong".into()
            })
            .await
            .is_err()
    );
    reopened
        .execute(FabricCommand::EndMaintenance {
            token: "owner".into(),
        })
        .await
        .unwrap();
    reopened.execute(write()).await.unwrap();
}

#[tokio::test]
async fn agent_directory_compatibility_fence_is_monotonic() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("directory.db");
    let store = SqliteEventStore::open(&path, "directory").await.unwrap();
    for (index, namespace) in ["$agent-run", "session"].into_iter().enumerate() {
        store
            .execute(FabricCommand::HostTransact {
                records: vec![HostRecord {
                    tenant: "local".into(),
                    namespace: namespace.into(),
                    run_id: None,
                    operation_id: "fixture".into(),
                    schema_version: 1,
                    payload: json!({"agent_id":"assistant"}),
                }],
                completion: None,
                transaction: Box::new(FabricCommand::Transact {
                    checkpoint: CheckpointWrite {
                        key: format!("fixture-{index}"),
                        expected: None,
                        value: json!(true),
                    },
                    outbox: vec![],
                    delivery: None,
                }),
            })
            .await
            .unwrap();
        let connection = rusqlite::Connection::open(&path).unwrap();
        let marker: String = connection
            .query_row(
                "SELECT state_json FROM workflow_operator_fabric_v1 WHERE singleton=1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&marker).unwrap()["schema_version"],
            6
        );
    }
    drop(store);
    SqliteEventStore::open_with_options(
        &path,
        "directory",
        SqliteOpenOptions {
            migration: SqliteMigration::ValidateOnly,
            ..Default::default()
        },
    )
    .await
    .unwrap();
}
