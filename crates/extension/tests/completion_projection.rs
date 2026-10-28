#![cfg(feature = "sqlite")]
#![allow(clippy::unwrap_used)] // Test setup and assertions intentionally fail fast.
#[allow(dead_code)]
#[path = "../examples/host_completion_projection.rs"]
mod example;

use agent_core::{
    agent::RunManager,
    contracts::{RunId, events::EventId, fabric::*, host::*},
};
use agent_extension::store::SqliteEventStore;
use rusqlite::params;
use serde_json::json;
use std::{path::Path, sync::Arc};

async fn manager(directory: &Path) -> RunManager {
    RunManager::new(
        "local",
        Arc::new(
            SqliteEventStore::open(directory.join("mina.db"), "projection-test")
                .await
                .unwrap(),
        ),
    )
}

// Invoked in its own process so exit skips Rust destructors, including SQLite rollback.
#[tokio::test]
async fn projection_crash_child() {
    let Ok(directory) = std::env::var("MINA_PROJECTION_CRASH_DIRECTORY") else {
        return;
    };
    let phase = std::env::var("MINA_PROJECTION_CRASH_PHASE").unwrap();
    let directory = Path::new(&directory);
    let manager = manager(directory).await;
    let mut host = example::open_host(&directory.join("host.db")).unwrap();
    let page = manager.list_completions(0, 1).await.unwrap();
    let (sequence, completion) = &page.items[0];
    let tx = example::stage_completion(&mut host, completion).unwrap();
    if phase == "before_commit" {
        std::process::exit(71);
    }
    tx.commit().unwrap();
    if phase == "before_ack" {
        std::process::exit(72);
    }
    manager
        .acknowledge_completion(example::CONSUMER, *sequence, &completion.completion_id)
        .await
        .unwrap();
    std::process::exit(73);
}

#[tokio::test]
async fn independent_database_projection_survives_process_exit() {
    for (phase, code, expected_messages, expected_offset) in [
        ("before_commit", 71, 0, 0),
        ("before_ack", 72, 1, 0),
        ("after_ack", 73, 1, 1),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let run = RunId::new();
        let completion = RunCompletion {
            schema_version: 1,
            tenant: "local".into(),
            run_id: run,
            completion_id: EventId::stable("run-completion", &json!(["local", run]).to_string())
                .to_string(),
            status: "completed".into(),
            output: Some(json!("final answer")),
            error: None,
            usage: None,
            completed_at_ms: 123,
            definition_digest: "fixture".into(),
        };
        // Seed a terminal fact using the public transaction contract. Engine-generated
        // completion and effect replay behavior are covered by static_agent_graph tests.
        let fabric = SqliteEventStore::open(directory.path().join("mina.db"), "seed")
            .await
            .unwrap();
        fabric
            .execute(FabricCommand::HostTransact {
                records: vec![],
                completion: Some(completion.clone()),
                transaction: Box::new(FabricCommand::Transact {
                    checkpoint: CheckpointWrite {
                        key: "fixture-terminal".into(),
                        expected: None,
                        value: json!("completed"),
                    },
                    outbox: vec![],
                    delivery: None,
                }),
            })
            .await
            .unwrap();
        drop(fabric);
        let mut host = example::open_host(&directory.path().join("host.db")).unwrap();
        let initial_manager = manager(directory.path()).await;
        assert!(
            example::project_pending(&initial_manager, &mut host)
                .await
                .is_err()
        );
        assert_eq!(
            initial_manager
                .completion_offset(example::CONSUMER)
                .await
                .unwrap(),
            0
        );
        drop(initial_manager);
        host.execute(
            "INSERT INTO admissions VALUES (?1,?2,?3)",
            params!["local", run.to_string(), "session-a"],
        )
        .unwrap();
        drop(host);
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "projection_crash_child", "--nocapture"])
            .env("MINA_PROJECTION_CRASH_DIRECTORY", directory.path())
            .env("MINA_PROJECTION_CRASH_PHASE", phase)
            .output()
            .unwrap();
        assert_eq!(
            child.status.code(),
            Some(code),
            "{phase}: {}",
            String::from_utf8_lossy(&child.stderr)
        );
        let manager = manager(directory.path()).await;
        host = example::open_host(&directory.path().join("host.db")).unwrap();
        let count = |host: &rusqlite::Connection| -> i64 {
            host.query_row("SELECT count(*) FROM final_messages", [], |row| row.get(0))
                .unwrap()
        };
        assert_eq!(count(&host), expected_messages, "{phase}");
        assert_eq!(
            manager.completion_offset(example::CONSUMER).await.unwrap(),
            expected_offset
        );
        example::project_pending(&manager, &mut host).await.unwrap();
        assert_eq!(count(&host), 1);
        assert_eq!(
            manager.completion_offset(example::CONSUMER).await.unwrap(),
            1
        );
        assert_eq!(
            example::project_pending(&manager, &mut host).await.unwrap(),
            0
        );
        assert_eq!(manager.completion_offset("profile-v1").await.unwrap(), 0);
        // Same identity with changed data cannot silently overwrite a final message.
        let mut altered = completion.clone();
        altered.output = Some(json!("different"));
        assert!(example::stage_completion(&mut host, &altered).is_err());
        let saved: String = host
            .query_row("SELECT completion_json FROM final_messages", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(
            serde_json::from_str::<RunCompletion>(&saved).unwrap(),
            completion
        );
    }
}
