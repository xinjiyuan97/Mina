#![cfg(feature = "sqlite")]

use std::{sync::Arc, time::Duration};

use agent_core::{
    harness::{
        FinishReason, ObservedRunEvent, OutputChannel, RunEvent, RunEventKind, RunId, RunSnapshot,
        RunStore,
    },
    memory::{MemoryId, MemoryLocator, MemoryStore},
};
use agent_extension::store::{SqliteMemoryStore, SqliteRunStore};
use tempfile::tempdir;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn run_and_memory_queries_progress_during_batched_event_writes() {
    let directory = tempdir().expect("temp directory should be created");
    let path = directory.path().join("shared.sqlite3");
    let runs = Arc::new(
        SqliteRunStore::open(&path)
            .await
            .expect("run store should open"),
    );
    let memories = Arc::new(
        SqliteMemoryStore::open(&path, "memory:test")
            .await
            .expect("memory store should open"),
    );
    let run_id = RunId::new();
    runs.create_run(RunSnapshot::new(run_id, "concurrent", 1))
        .await
        .expect("run should be created");

    let writer = {
        let runs = Arc::clone(&runs);
        tokio::spawn(async move {
            let mut events = vec![ObservedRunEvent::new(
                RunEvent::new(run_id, 1, RunEventKind::RunStarted),
                2,
            )];
            events.extend((2..=257).map(|seq| {
                ObservedRunEvent::new(
                    RunEvent::new(
                        run_id,
                        seq,
                        RunEventKind::OutputDelta {
                            channel: OutputChannel::AssistantText,
                            delta: "x".into(),
                        },
                    ),
                    i64::try_from(seq).unwrap_or(i64::MAX),
                )
            }));
            events.push(ObservedRunEvent::new(
                RunEvent::new(
                    run_id,
                    258,
                    RunEventKind::RunCompleted {
                        finish_reason: FinishReason::Stop,
                    },
                ),
                258,
            ));
            for batch in events.chunks(16) {
                runs.append_events(batch.to_vec())
                    .await
                    .expect("event batch should append");
            }
        })
    };

    let run_reader = {
        let runs = Arc::clone(&runs);
        tokio::spawn(async move {
            for _ in 0..100 {
                runs.get_run(run_id)
                    .await
                    .expect("snapshot query should progress");
                runs.events_after(run_id, 0, 32)
                    .await
                    .expect("event query should progress");
            }
        })
    };

    let memory_reader = tokio::spawn(async move {
        let missing = MemoryId::new();
        for _ in 0..100 {
            memories
                .get(MemoryLocator {
                    memory_id: missing,
                    version: None,
                })
                .await
                .expect("memory query should progress");
        }
    });

    tokio::time::timeout(Duration::from_secs(5), async {
        writer.await.expect("writer task should complete");
        run_reader.await.expect("run reader task should complete");
        memory_reader
            .await
            .expect("memory reader task should complete");
    })
    .await
    .expect("concurrent SQLite operations should not deadlock");

    let snapshot = runs
        .get_run(run_id)
        .await
        .expect("snapshot should load")
        .expect("snapshot should exist");
    assert_eq!(snapshot.output.len(), 256);
    assert!(snapshot.is_terminal());
}
