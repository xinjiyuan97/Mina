use super::SqliteEventStore;
use crate::operator_fabric::{FabricState, storage};
use agent_core::operator_fabric::*;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior};

impl OperatorFabric for SqliteEventStore {
    fn capabilities(&self) -> FabricCapabilities {
        FabricCapabilities {
            durable: true,
            ..crate::operator_fabric::MemoryFabric::default().capabilities()
        }
    }

    fn execute(&self, command: FabricCommand) -> FabricFuture<'_> {
        let connection = self.connection.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                let mut connection = connection.lock().map_err(storage)?;
                transact(&mut connection, command)
            })
            .await
            .map_err(storage)?
        })
    }
}

fn transact(
    connection: &mut Connection,
    command: FabricCommand,
) -> Result<FabricReply, FabricError> {
    let read_only = matches!(
        command,
        FabricCommand::InspectRetention { .. }
            | FabricCommand::Replay { .. }
            | FabricCommand::ReadCheckpoint(_)
            | FabricCommand::ReadEvent(_)
            | FabricCommand::ScanEvents { .. }
            | FabricCommand::Offset { .. }
            | FabricCommand::Receipt(_)
            | FabricCommand::DeadLetters { .. }
    );
    let behavior = if read_only {
        TransactionBehavior::Deferred
    } else {
        TransactionBehavior::Immediate
    };
    let transaction = connection
        .transaction_with_behavior(behavior)
        .map_err(storage)?;
    let encoded: Option<String> = transaction
        .query_row(
            "SELECT state_json FROM workflow_operator_fabric_v1 WHERE singleton = 1",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(storage)?;
    let mut state: FabricState = match encoded {
        Some(encoded) => {
            let value: serde_json::Value = serde_json::from_str(&encoded).map_err(storage)?;
            if value
                .get("schema_version")
                .is_some_and(|version| version.as_u64() != Some(1))
            {
                return Err(FabricError::Invalid("unsupported_fabric_version".into()));
            }
            if value["events"].as_array().is_some_and(|events| {
                events.iter().any(|event| {
                    let envelope = &event["event"]["envelope"];
                    envelope["schema_version"] != 1
                        || envelope["projection_version"] != 1
                        || envelope["operator"]["version"] != 1
                        || !matches!(
                            envelope["engine_version"].as_str(),
                            Some("legacy_machine" | "operator_v1")
                        )
                })
            }) {
                return Err(FabricError::Invalid(
                    "unsupported_fabric_event_version".into(),
                ));
            }
            serde_json::from_value(value).map_err(storage)?
        }
        None => FabricState::default(),
    };
    let reply = state.apply(command)?;
    if !read_only {
        let encoded = serde_json::to_string(&state).map_err(storage)?;
        transaction.execute("INSERT INTO workflow_operator_fabric_v1 (singleton, state_json) VALUES (1, ?1) ON CONFLICT(singleton) DO UPDATE SET state_json = excluded.state_json", [encoded]).map_err(storage)?;
    }
    transaction.commit().map_err(storage)?;
    Ok(reply)
}

#[cfg(test)]
mod p5_tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn p5_sqlite_full_never_acknowledges_the_unpersisted_transition() {
        let directory = tempfile::tempdir().expect("temp");
        let path = directory.path().join("full.db");
        let store = SqliteEventStore::open(&path, "p5").await.expect("open");
        let source: FabricEvent = serde_json::from_value(json!({
            "topic":"full", "payload_schema":true, "envelope": {
                "schema_version":1,"engine_version":"operator_v1","projection_version":1,
                "event_id":agent_core::event_runtime::EventId::stable("p5","full"),
                "event_type":"test.fact","tenant_id":"local","run_id":agent_core::harness::RunId::new(),
                "session_id":null,"effect_id":"effect","invocation_id":"invocation","attempt":1,
                "issued_revision":0,"lease_epoch":1,"correlation_id":"test","causation_id":null,
                "operator":{"name":"test","version":1},"operator_seq":1,
                "delivery_class":"durable","occurred_at_ms":0,"payload":{}
            }
        })).expect("event");
        store
            .execute(FabricCommand::Subscribe(SubscriptionSpec {
                consumer: "test".into(),
                subscription: "test".into(),
                topic: "full".into(),
                event_type: None,
                tenant: None,
            }))
            .await
            .expect("subscribe");
        store
            .execute(FabricCommand::Publish(source.clone()))
            .await
            .expect("publish");
        let FabricReply::Claimed(mut deliveries) = store
            .execute(FabricCommand::Claim {
                worker: "test".into(),
                now_ms: 0,
                lease_ms: 10,
                limit: 1,
                max_attempts: 3,
            })
            .await
            .expect("claim")
        else {
            panic!("claims")
        };
        {
            let connection = store.connection.lock().expect("lock");
            let pages: i64 = connection
                .query_row("PRAGMA page_count", [], |row| row.get(0))
                .expect("pages");
            connection
                .pragma_update(None, "max_page_count", pages)
                .expect("inject disk capacity");
        }
        let result = store
            .execute(FabricCommand::Transact {
                checkpoint: CheckpointWrite {
                    key: "full-checkpoint".into(),
                    expected: None,
                    value: json!("x".repeat(1024 * 1024)),
                },
                outbox: vec![],
                delivery: Some((deliveries.remove(0).lease, 1)),
            })
            .await;
        assert!(
            matches!(&result,Err(FabricError::Storage(message)) if message.contains("full")),
            "{result:?}"
        );
        drop(store);
        let store = SqliteEventStore::open(&path, "p5").await.expect("reopen");
        assert_eq!(
            store
                .execute(FabricCommand::Offset {
                    consumer: "test".into(),
                    subscription: "test".into()
                })
                .await,
            Ok(FabricReply::Offset(0))
        );
        assert_eq!(
            store
                .execute(FabricCommand::ReadCheckpoint("full-checkpoint".into()))
                .await,
            Ok(FabricReply::Checkpoint(None))
        );
        assert_eq!(
            store
                .execute(FabricCommand::Receipt(ReceiptKey::for_event(
                    "test", &source
                )))
                .await,
            Ok(FabricReply::Receipt(None))
        );
    }
}
