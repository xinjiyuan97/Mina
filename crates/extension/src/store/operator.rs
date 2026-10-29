mod indexed;
use super::SqliteEventStore;
use crate::operator_fabric::{FabricState, storage};
use agent_core::contracts::fabric::*;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior};

impl SqliteEventStore {
    /// Configure before sharing/cloning the adapter. Reapply after reopening a database.
    pub fn with_fabric_limits(mut self, limits: FabricLimits) -> Result<Self, FabricError> {
        limits.validate()?;
        self.fabric_limits = limits;
        Ok(self)
    }
}
impl OperatorFabric for SqliteEventStore {
    fn limits(&self) -> FabricLimits {
        self.fabric_limits
    }
    fn changes(&self) -> Option<tokio::sync::watch::Receiver<u64>> {
        Some(self.fabric_changed.subscribe())
    }
    fn capabilities(&self) -> FabricCapabilities {
        FabricCapabilities {
            durable: true,
            ..crate::operator_fabric::MemoryFabric::default().capabilities()
        }
    }

    fn execute(&self, command: FabricCommand) -> FabricFuture<'_> {
        let limits = self.fabric_limits;
        let writes_checkpoint = command.writes_checkpoint();
        let connection = self.connection.clone();
        let changed = self.fabric_changed.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                let mut connection = connection.lock().map_err(storage)?;
                let result = transact(&mut connection, command, limits);
                if let Ok(reply) = &result
                    && (writes_checkpoint || observable_change(reply))
                {
                    changed.send_modify(|generation| *generation = generation.wrapping_add(1));
                }
                result
            })
            .await
            .map_err(storage)?
        })
    }
}

fn transact(
    connection: &mut Connection,
    command: FabricCommand,
    limits: FabricLimits,
) -> Result<FabricReply, FabricError> {
    let needs_v6 = match &command {
        FabricCommand::HostTransact { records, .. } => {
            records.iter().any(|r| r.namespace == "$agent-run")
        }
        FabricCommand::ImportRun { bundle, .. } => bundle.graph_run()?.agent_id.is_some(),
        _ => false,
    };
    let needs_v5 = matches!(
        command,
        FabricCommand::HostTransact { .. }
            | FabricCommand::BeginMaintenance { .. }
            | FabricCommand::ImportRun { .. }
    );
    let read_only = matches!(
        command,
        FabricCommand::ReadLog { .. }
            | FabricCommand::ExportAgentHistory { .. }
            | FabricCommand::InspectRun { .. }
            | FabricCommand::ExportRun { .. }
            | FabricCommand::InspectRetention { .. }
            | FabricCommand::ArtifactRoots
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
    let value: serde_json::Value = match encoded {
        Some(encoded) => serde_json::from_str(&encoded).map_err(storage)?,
        None => serde_json::to_value(FabricState::default()).map_err(storage)?,
    };
    let previous_version = value["schema_version"].as_u64().unwrap_or(0);
    if value["schema_version"] == 3 {
        // Schema 3 already has relational rows; only upgrade the compatibility fence.
        transaction.execute("UPDATE workflow_operator_fabric_v1 SET state_json='{\"schema_version\":4}' WHERE singleton=1", []).map_err(storage)?;
    } else if value["schema_version"] != 4
        && value["schema_version"] != 5
        && value["schema_version"] != 6
    {
        validate_legacy(&value)?;
        let state: FabricState = serde_json::from_value(value).map_err(storage)?;
        persist_rows(
            &transaction,
            &Default::default(),
            &rows(serde_json::to_value(state).map_err(storage)?)?,
        )?;
        // Old binaries must reject the migrated database instead of silently overwriting it.
        transaction.execute("INSERT INTO workflow_operator_fabric_v1(singleton,state_json) VALUES(1,'{\"schema_version\":4}') ON CONFLICT(singleton) DO UPDATE SET state_json=excluded.state_json", []).map_err(storage)?;
    }
    if needs_v6 {
        transaction.execute("UPDATE workflow_operator_fabric_v1 SET state_json='{\"schema_version\":6}' WHERE singleton=1", []).map_err(storage)?;
    } else if needs_v5 && previous_version < 5 {
        transaction.execute("UPDATE workflow_operator_fabric_v1 SET state_json='{\"schema_version\":5}' WHERE singleton=1", []).map_err(storage)?;
    }
    let frozen: bool = transaction.query_row("SELECT EXISTS(SELECT 1 FROM workflow_fabric_records WHERE section='checkpoints' AND identity=?1)", [agent_core::agent::memory_fabric::archive::MAINTENANCE_KEY], |r| r.get(0)).map_err(storage)?;
    if frozen && !agent_core::agent::memory_fabric::archive::maintenance_allowed(&command) {
        return Err(FabricError::Fenced);
    }
    if let Some(reply) = indexed::execute(&transaction, &command)? {
        transaction.commit().map_err(storage)?;
        return Ok(reply);
    }
    if let FabricCommand::ReadLog { log, after, limit } = &command {
        validate_key(log)?;
        if *limit == 0 || *limit > MAX_BATCH_ITEMS {
            return Err(FabricError::Invalid("batch/page limit".into()));
        }
        let through = log_through(&transaction, log)?;
        let reply = if let Some(through) = through {
            let mut statement = transaction.prepare("SELECT value_json FROM workflow_fabric_records WHERE section='logs' AND identity=?1 AND position>?2 ORDER BY position LIMIT ?3").map_err(storage)?;
            let values = statement
                .query_map(
                    rusqlite::params![
                        log,
                        i64::try_from(*after).unwrap_or(i64::MAX),
                        *limit as i64
                    ],
                    |row| row.get::<_, String>(0),
                )
                .map_err(storage)?;
            let items = values
                .map(|value| serde_json::from_str(&value.map_err(storage)?).map_err(storage))
                .collect::<Result<Vec<_>, _>>()?;
            FabricReply::Log(Some(LogPage { through, items }))
        } else {
            FabricReply::Log(None)
        };
        transaction.commit().map_err(storage)?;
        return Ok(reply);
    }
    // Log appends use SQL watermarks; no historical log payload is loaded for writes.
    let (command, append) = match command {
        FabricCommand::TransactLog {
            log,
            expected,
            items,
            checkpoints,
            outbox,
            delivery,
        } => (
            FabricCommand::TransactMany {
                checkpoints,
                outbox,
                delivery,
            },
            Some((log, expected, items)),
        ),
        FabricCommand::GuardedTransactLog {
            lease,
            now_ms,
            log,
            expected,
            items,
            checkpoints,
        } => (
            FabricCommand::GuardedTransactMany {
                lease,
                now_ms,
                checkpoints,
                outbox: vec![],
            },
            Some((log, expected, items)),
        ),
        command => (command, None),
    };
    if let Some((log, expected, items)) = &append {
        validate_key(log)?;
        if log_through(&transaction, log)?.unwrap_or(0) != *expected {
            return Err(FabricError::Fenced);
        }
        if serde_json::to_vec(items).map_err(storage)?.len() > MAX_CHECKPOINT_BYTES {
            return Err(FabricError::Invalid("log transaction byte limit".into()));
        }
    }
    let all = matches!(
        command,
        FabricCommand::HostTransact { .. }
            | FabricCommand::BeginMaintenance { .. }
            | FabricCommand::EndMaintenance { .. }
            | FabricCommand::ExportAgentHistory { .. }
            | FabricCommand::InspectRun { .. }
            | FabricCommand::ExportRun { .. }
            | FabricCommand::ImportRun { .. }
            | FabricCommand::InspectRetention { .. }
            | FabricCommand::RetireRun { .. }
            | FabricCommand::CollectArtifacts { .. }
            | FabricCommand::ArtifactRoots
    );
    let collection = matches!(
        command,
        FabricCommand::RetireRun { .. } | FabricCommand::CollectArtifacts { .. }
    );
    let keys: Vec<&str> = match &command {
        FabricCommand::ReadCheckpoint(key) => vec![key],
        FabricCommand::Transact { checkpoint, .. }
        | FabricCommand::GuardedTransact { checkpoint, .. } => vec![&checkpoint.key],
        FabricCommand::TransactMany { checkpoints, .. }
        | FabricCommand::GuardedTransactMany { checkpoints, .. } => {
            checkpoints.iter().map(|c| c.key.as_str()).collect()
        }
        _ => vec![],
    };
    let checkpoint_only = matches!(command, FabricCommand::ReadCheckpoint(_));
    let scoped = indexed::scoped_rows(&transaction, &command)?;
    let is_scoped = scoped.is_some();
    let mut before = scoped.unwrap_or_default();
    if !checkpoint_only && !is_scoped {
        let sql = if all {
            "SELECT section,identity,position,value_json FROM workflow_fabric_records ORDER BY section,identity,position"
        } else {
            "SELECT section,identity,position,value_json FROM workflow_fabric_records WHERE section IN ('schema_version','lease_reclaims','events','subscriptions','receipts','dead_letters') ORDER BY section,identity,position"
        };
        read_rows(&transaction, sql, [], &mut before)?;
    }
    if !all {
        for key in keys {
            read_rows(
                &transaction,
                "SELECT section,identity,position,value_json FROM workflow_fabric_records WHERE section='checkpoints' AND identity=?1",
                [key],
                &mut before,
            )?;
        }
    }
    let value = assemble(&before)?;
    validate_legacy(&value)?;
    let mut state = serde_json::from_value::<FabricState>(value)
        .map_err(storage)?
        .with_limits(limits)?;
    let reply = match command {
        FabricCommand::BeginMaintenance { token, now_ms } => {
            super::wait_archive::check_leases(&transaction, now_ms)?;
            state.apply(FabricCommand::BeginMaintenance { token, now_ms })?
        }
        FabricCommand::ExportRun {
            tenant,
            run_id,
            maintenance_token,
        } => {
            let FabricReply::RunInspection(snapshot) = state.apply(FabricCommand::InspectRun {
                tenant: tenant.clone(),
                run_id,
            })?
            else {
                return Err(FabricError::Conflict);
            };
            let has_waits = snapshot.items.iter().any(|item| {
                item.payload["value"]["definition"]["operators"]
                    .as_object()
                    .is_some_and(|ops| ops.values().any(|op| op["type"] == "agent.wait"))
            });
            if has_waits {
                let dependencies = super::wait_archive::capture(
                    &transaction,
                    run_id,
                    &serde_json::to_value(snapshot).map_err(storage)?,
                )?;
                state.export_with_dependencies(&tenant, run_id, &maintenance_token, dependencies)?
            } else {
                state.apply(FabricCommand::ExportRun {
                    tenant,
                    run_id,
                    maintenance_token,
                })?
            }
        }
        FabricCommand::ImportRun {
            bundle,
            maintenance_token,
        } if bundle.dependencies.is_some() => {
            let marker =
                agent_core::contracts::run_archive::import_marker(&bundle.tenant, bundle.run_id);
            let already = before
                .get(&("checkpoints".into(), marker, 0))
                .is_some_and(|v| v["value"] == serde_json::json!([bundle.tenant, bundle.digest]));
            let reply = state.import_archive(bundle.clone(), &maintenance_token, true)?;
            if !already {
                let FabricReply::RunInspection(snapshot) =
                    state.apply(FabricCommand::InspectRun {
                        tenant: bundle.tenant.clone(),
                        run_id: bundle.run_id,
                    })?
                else {
                    return Err(FabricError::Conflict);
                };
                super::wait_archive::restore(
                    &transaction,
                    bundle.dependencies.as_ref().ok_or(FabricError::Conflict)?,
                    bundle.run_id,
                    &serde_json::to_value(snapshot).map_err(storage)?,
                )?;
            }
            reply
        }
        command => state.apply(command)?,
    };
    if !read_only {
        let after = rows(serde_json::to_value(state).map_err(storage)?)?;
        let after = if collection || all {
            collection_positions(&before, after)?
        } else if is_scoped {
            indexed::restore_positions(&transaction, &before, after)?
        } else {
            after
        };
        persist_rows(&transaction, &before, &after)?;
        if let Some((log, expected, items)) = append {
            let through = expected
                .checked_add(items.len() as u64)
                .ok_or_else(|| FabricError::Invalid("log overflow".into()))?;
            for (index, item) in items.into_iter().enumerate() {
                transaction
                    .execute(
                        "INSERT INTO workflow_fabric_records VALUES('logs',?1,?2,?3)",
                        rusqlite::params![
                            log,
                            i64::try_from(expected + index as u64 + 1).map_err(storage)?,
                            serde_json::to_string(&item).map_err(storage)?
                        ],
                    )
                    .map_err(storage)?;
            }
            transaction.execute("INSERT INTO workflow_fabric_records VALUES('log_heads',?1,0,?2) ON CONFLICT(section,identity,position) DO UPDATE SET value_json=excluded.value_json", rusqlite::params![log, through.to_string()]).map_err(storage)?;
        }
    }
    transaction.commit().map_err(storage)?;
    Ok(reply)
}

type Rows = std::collections::BTreeMap<(String, String, i64), serde_json::Value>;
fn validate_key(key: &str) -> Result<(), FabricError> {
    if key.trim().is_empty() || key.len() > MAX_KEY_BYTES {
        return Err(FabricError::Invalid("identity length".into()));
    }
    Ok(())
}
fn validate_legacy(value: &serde_json::Value) -> Result<(), FabricError> {
    if value
        .get("schema_version")
        .is_some_and(|v| !matches!(v.as_u64(), Some(1..=3)))
    {
        return Err(FabricError::Invalid("unsupported_fabric_version".into()));
    }
    if value["events"].as_array().is_some_and(|events| {
        events.iter().any(|event| {
            let e = &event["event"]["envelope"];
            e["schema_version"] != 1
                || e["projection_version"] != 1
                || !e["operator"]["version"]
                    .as_u64()
                    .is_some_and(|v| v > 0 && v <= u64::from(u32::MAX))
                || !matches!(
                    e["engine_version"].as_str(),
                    Some("legacy_machine" | "operator_v1")
                )
        })
    }) {
        return Err(FabricError::Invalid(
            "unsupported_fabric_event_version".into(),
        ));
    }
    Ok(())
}
fn rows(value: serde_json::Value) -> Result<Rows, FabricError> {
    let mut rows = Rows::new();
    for (section, value) in value.as_object().ok_or(FabricError::Conflict)? {
        match section.as_str() {
            "checkpoints" => {
                for (key, value) in value.as_object().ok_or(FabricError::Conflict)? {
                    rows.insert((section.clone(), key.clone(), 0), value.clone());
                }
            }
            "logs" => {
                for (key, values) in value.as_object().ok_or(FabricError::Conflict)? {
                    let values = values.as_array().ok_or(FabricError::Conflict)?;
                    rows.insert(
                        ("log_heads".into(), key.clone(), 0),
                        serde_json::json!(values.len()),
                    );
                    for (index, value) in values.iter().enumerate() {
                        rows.insert(
                            (section.clone(), key.clone(), index as i64 + 1),
                            value.clone(),
                        );
                    }
                }
            }
            "events" | "subscriptions" | "receipts" | "dead_letters" => {
                for (index, value) in value
                    .as_array()
                    .ok_or(FabricError::Conflict)?
                    .iter()
                    .enumerate()
                {
                    rows.insert(
                        (section.clone(), String::new(), index as i64),
                        value.clone(),
                    );
                }
            }
            _ => {
                rows.insert((section.clone(), String::new(), 0), value.clone());
            }
        }
    }
    Ok(rows)
}
fn assemble(rows: &Rows) -> Result<serde_json::Value, FabricError> {
    let mut value = serde_json::to_value(FabricState::default()).map_err(storage)?;
    for ((section, identity, _), item) in rows {
        match section.as_str() {
            "checkpoints" => {
                value[section][identity] = item.clone();
            }
            "log_heads" => {
                value["logs"]
                    .as_object_mut()
                    .ok_or(FabricError::Conflict)?
                    .entry(identity.clone())
                    .or_insert(serde_json::json!([]));
            }
            "logs" => {
                value["logs"]
                    .as_object_mut()
                    .ok_or(FabricError::Conflict)?
                    .entry(identity.clone())
                    .or_insert(serde_json::json!([]))
                    .as_array_mut()
                    .ok_or(FabricError::Conflict)?
                    .push(item.clone());
            }
            "events" | "subscriptions" | "receipts" | "dead_letters" => {
                value[section]
                    .as_array_mut()
                    .ok_or(FabricError::Conflict)?
                    .push(item.clone());
            }
            _ => {
                value[section] = item.clone();
            }
        }
    }
    Ok(value)
}
fn read_rows<P: rusqlite::Params>(
    tx: &rusqlite::Transaction<'_>,
    sql: &str,
    params: P,
    rows: &mut Rows,
) -> Result<(), FabricError> {
    let mut statement = tx.prepare(sql).map_err(storage)?;
    let records = statement
        .query_map(params, |r| {
            Ok((
                (
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, i64>(2)?,
                ),
                r.get::<_, String>(3)?,
            ))
        })
        .map_err(storage)?;
    for record in records {
        let (key, value) = record.map_err(storage)?;
        rows.insert(key, serde_json::from_str(&value).map_err(storage)?);
    }
    Ok(())
}
fn collection_positions(before: &Rows, after: Rows) -> Result<Rows, FabricError> {
    fn identity(section: &str, value: &serde_json::Value) -> Option<String> {
        match section {
            "subscriptions" => Some(value["spec"].to_string()),
            "receipts" => Some(value["key"].to_string()),
            "dead_letters" => Some(value.to_string()),
            "events" => Some(value["event"]["envelope"]["event_id"].to_string()),
            _ => None,
        }
    }
    let mut positions =
        std::collections::BTreeMap::<(String, String), std::collections::VecDeque<i64>>::new();
    for ((section, _, position), value) in before {
        if let Some(id) = identity(section, value) {
            positions
                .entry((section.clone(), id))
                .or_default()
                .push_back(*position);
        }
    }
    let mut tails = std::collections::BTreeMap::<String, i64>::new();
    for (section, _, position) in before.keys() {
        tails
            .entry(section.clone())
            .and_modify(|tail| *tail = (*tail).max(*position + 1))
            .or_insert(*position + 1);
    }
    let mut restored = Rows::new();
    for ((section, key, position), value) in after {
        let position = if let Some(id) = identity(&section, &value) {
            positions
                .get_mut(&(section.clone(), id))
                .and_then(|p| p.pop_front())
                .unwrap_or_else(|| {
                    let tail = tails.entry(section.clone()).or_default();
                    let next = *tail;
                    *tail += 1;
                    next
                })
        } else {
            position
        };
        restored.insert((section, key, position), value);
    }
    Ok(restored)
}

fn persist_rows(
    tx: &rusqlite::Transaction<'_>,
    before: &Rows,
    after: &Rows,
) -> Result<(), FabricError> {
    for ((section, key, position), value) in after {
        if before.get(&(section.clone(), key.clone(), *position)) == Some(value) {
            continue;
        }
        tx.execute("INSERT INTO workflow_fabric_records VALUES(?1,?2,?3,?4) ON CONFLICT(section,identity,position) DO UPDATE SET value_json=excluded.value_json", rusqlite::params![section, key, position, serde_json::to_string(value).map_err(storage)?]).map_err(storage)?;
    }
    for (section, key, position) in before.keys().filter(|key| !after.contains_key(*key)) {
        tx.execute(
            "DELETE FROM workflow_fabric_records WHERE section=?1 AND identity=?2 AND position=?3",
            rusqlite::params![section, key, position],
        )
        .map_err(storage)?;
    }
    Ok(())
}
fn log_through(tx: &rusqlite::Transaction<'_>, log: &str) -> Result<Option<u64>, FabricError> {
    let value: Option<String> = tx.query_row("SELECT value_json FROM workflow_fabric_records WHERE section='log_heads' AND identity=?1 AND position=0", [log], |row| row.get(0)).optional().map_err(storage)?;
    value
        .map(|v| serde_json::from_str(&v).map_err(storage))
        .transpose()
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
                "event_id":agent_core::contracts::events::EventId::stable("p5","full"),
                "event_type":"test.fact","tenant_id":"local","run_id":agent_core::contracts::RunId::new(),
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
