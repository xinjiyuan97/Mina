//! Logical archive adapter for a Run's isolated SQLite EventStore/JobStore.
//! Shared external stores fail closed: sequence identities are not remapped.
use crate::operator_fabric::storage;
use agent_core::contracts::{
    JobNotificationStatus, JobRecord, JobStatus, RunId, events::*, fabric::*,
};
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const TABLES: &[&str] = &[
    "workflow_events",
    "workflow_subscriptions",
    "workflow_deliveries",
    "workflow_timers",
    "workflow_jobs",
    "workflow_runs",
    "workflow_inbox",
    "workflow_effects",
];
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WaitArchive {
    version: u32,
    run_id: RunId,
    events: Vec<EventEnvelope>,
    subscriptions: Vec<Subscription>,
    deliveries: Vec<Delivery>,
    timers: Vec<Timer>,
    jobs: Vec<JobRecord>,
}
fn invalid(s: &str) -> FabricError {
    FabricError::Invalid(s.into())
}
fn encode(v: &impl Serialize) -> Result<String, FabricError> {
    serde_json::to_string(v).map_err(storage)
}
fn key(v: &impl Serialize) -> Result<String, FabricError> {
    serde_json::to_value(v)
        .map_err(storage)?
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| invalid("enum"))
}
fn read<T: serde::de::DeserializeOwned>(c: &Connection, sql: &str) -> Result<Vec<T>, FabricError> {
    let mut q = c.prepare(sql).map_err(storage)?;
    q.query_map([], |r| r.get::<_, String>(0))
        .map_err(storage)?
        .map(|r| serde_json::from_str(&r.map_err(storage)?).map_err(storage))
        .collect()
}
pub(super) fn install_fence(c: &Connection) -> Result<(), EventError> {
    let result = (|| -> rusqlite::Result<()> {
        for table in TABLES {
            for operation in ["INSERT", "UPDATE", "DELETE"] {
                c.execute_batch(&format!("CREATE TRIGGER IF NOT EXISTS {table}_maintenance_{operation} BEFORE {operation} ON {table} WHEN EXISTS(SELECT 1 FROM workflow_fabric_records WHERE section='checkpoints' AND identity='$fabric-maintenance-v1') BEGIN SELECT RAISE(ABORT, 'fabric maintenance fence'); END;"))?;
            }
        }
        Ok(())
    })();
    result.map_err(|e| EventError::backend(e.to_string()))
}
pub(super) fn check_leases(c: &Connection, now: i64) -> Result<(), FabricError> {
    for (table, column) in [
        ("workflow_deliveries", "lease_until_ms"),
        ("workflow_timers", "lease_until_ms"),
        ("workflow_jobs", "lease_until_ms"),
        ("workflow_jobs", "notification_lease_until_ms"),
    ] {
        let active: bool = c
            .query_row(
                &format!("SELECT EXISTS(SELECT 1 FROM {table} WHERE {column}>?1)"),
                [now],
                |r| r.get(0),
            )
            .map_err(storage)?;
        if active {
            return Err(FabricError::Fenced);
        }
    }
    Ok(())
}
pub(super) fn capture(
    c: &Connection,
    run: RunId,
    run_snapshot: &Value,
) -> Result<Value, FabricError> {
    for table in ["workflow_runs", "workflow_inbox", "workflow_effects"] {
        let count: i64 = c
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
            .map_err(storage)?;
        if count != 0 {
            return Err(invalid(
                "shared legacy flow store cannot be exported as a Run",
            ));
        }
    }
    let mut archive = WaitArchive {
        version: 1,
        run_id: run,
        events: read(
            c,
            "SELECT envelope_json FROM workflow_events ORDER BY sequence",
        )?,
        subscriptions: read(
            c,
            "SELECT subscription_json FROM workflow_subscriptions ORDER BY subscription_id",
        )?,
        deliveries: vec![],
        timers: read(
            c,
            "SELECT timer_json FROM workflow_timers ORDER BY timer_id",
        )?,
        jobs: read(c, "SELECT record_json FROM workflow_jobs ORDER BY job_id")?,
    };
    let mut q = c.prepare("SELECT subscription_id,event_id FROM workflow_deliveries ORDER BY subscription_id,event_id").map_err(storage)?;
    for row in q
        .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
        .map_err(storage)?
    {
        let (subscription, event) = row.map_err(storage)?;
        archive
            .deliveries
            .push(super::event::load_delivery_strings(c, &subscription, &event).map_err(storage)?);
    }
    validate(&archive, run_snapshot)?;
    Ok(json!({"adapter":"sqlite_waits","version":1,"state":archive}))
}
fn validate(a: &WaitArchive, snapshot: &Value) -> Result<(), FabricError> {
    if a.version != 1 {
        return Err(invalid("wait archive version"));
    }
    // Every stored external record must be part of this Run's declared wait dependency closure.
    let mut subscription_ids = std::collections::BTreeSet::new();
    let mut timer_ids = std::collections::BTreeSet::new();
    let mut job_ids = std::collections::BTreeSet::new();
    let mut event_ids = std::collections::BTreeSet::new();
    for item in snapshot["items"].as_array().into_iter().flatten() {
        let payload = &item["payload"]["value"];
        if let Some(map) = payload.as_object() {
            for registration in map.values().chain(std::iter::once(payload)) {
                if registration["run"] != json!(a.run_id) {
                    continue;
                }
                for wait in registration["suspension"]["waits"]
                    .as_array()
                    .into_iter()
                    .flatten()
                {
                    if let Some(id) = wait["subscription"]["subscription_id"].as_str() {
                        subscription_ids.insert(id.to_string());
                    }
                }
                for effect in registration["suspension"]["effects"]
                    .as_array()
                    .into_iter()
                    .flatten()
                {
                    let command = &effect["command"];
                    if let Some(id) = command["job_id"].as_str() {
                        job_ids.insert(id.to_string());
                    }
                    if let Some(id) = command["timer_id"].as_str() {
                        timer_ids.insert(id.to_string());
                    }
                    if let Some(id) = command["event_id"].as_str() {
                        event_ids.insert(id.to_string());
                    }
                }
            }
        }
    }
    if subscription_ids.iter().any(|id| {
        !a.subscriptions
            .iter()
            .any(|s| s.definition.subscription_id.to_string() == *id)
    }) || timer_ids.iter().any(|id| {
        !a.timers
            .iter()
            .any(|t| t.definition.timer_id.to_string() == *id)
    }) || job_ids
        .iter()
        .any(|id| !a.jobs.iter().any(|j| j.command.job_id.to_string() == *id))
        || event_ids
            .iter()
            .any(|id| !a.events.iter().any(|e| e.event.event_id.to_string() == *id))
    {
        return Err(invalid("incomplete external wait dependencies"));
    }
    for s in &a.subscriptions {
        s.definition.validate().map_err(storage)?;
        if s.definition.owner != (SubscriptionOwner::Run { run_id: a.run_id })
            || s.definition.scope != (SubscriptionScope::Run { run_id: a.run_id })
            || !subscription_ids.contains(&s.definition.subscription_id.to_string())
        {
            return Err(invalid("shared or unrelated external subscription"));
        }
    }
    for j in &a.jobs {
        if j.command.run_id != a.run_id || !job_ids.contains(&j.command.job_id.to_string()) {
            return Err(invalid("unrelated job"));
        }
        // Running jobs may have performed an external effect. Never silently requeue them.
        if j.status == JobStatus::Running {
            return Err(invalid("job outcome unknown: reconcile before export"));
        }
    }
    for t in &a.timers {
        t.definition.validate().map_err(storage)?;
        if !timer_ids.contains(&t.definition.timer_id.to_string()) {
            return Err(invalid("unrelated timer"));
        }
        event_ids.insert(t.definition.event.event_id.to_string());
    }
    let mut last = 0;
    for e in &a.events {
        validate_publish(&e.event).map_err(storage)?;
        if e.sequence <= last
            || (!event_ids.contains(&e.event.event_id.to_string())
                && !a.subscriptions.iter().any(|s| {
                    s.definition.filter.matches(e)
                        && e.event.subject.as_deref() == Some(format!("run/{}", a.run_id).as_str())
                }))
        {
            return Err(invalid("unrelated or unordered external event"));
        }
        last = e.sequence;
    }
    for d in &a.deliveries {
        if !a.subscriptions.iter().any(|s| {
            s.definition.subscription_id == d.delivery_id.subscription_id
                && s.definition.delivery == d.target
        }) || !a
            .events
            .iter()
            .any(|e| e.event.event_id == d.delivery_id.event_id && e.sequence == d.event_sequence)
        {
            return Err(invalid("external delivery dependency missing"));
        }
    }
    Ok(())
}
pub(super) fn restore(
    c: &Connection,
    dependencies: &Value,
    run: RunId,
    snapshot: &Value,
) -> Result<(), FabricError> {
    if dependencies["adapter"] != "sqlite_waits" || dependencies["version"] != 1 {
        return Err(invalid("unsupported external archive"));
    }
    let mut a: WaitArchive =
        serde_json::from_value(dependencies["state"].clone()).map_err(storage)?;
    if a.run_id != run {
        return Err(FabricError::Conflict);
    }
    validate(&a, snapshot)?;
    for table in TABLES {
        let count: i64 = c
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
            .map_err(storage)?;
        if count != 0 {
            return Err(invalid("external import destination must be empty"));
        }
    }
    // Temporarily remove the fence inside this write transaction; other connections cannot
    // observe this state. Restore it before commit. Any error rolls the whole transaction back.
    let fence: String = c.query_row("SELECT value_json FROM workflow_fabric_records WHERE section='checkpoints' AND identity='$fabric-maintenance-v1'", [], |r| r.get(0)).map_err(storage)?;
    c.execute("DELETE FROM workflow_fabric_records WHERE section='checkpoints' AND identity='$fabric-maintenance-v1'", []).map_err(storage)?;
    for e in &a.events {
        c.execute("INSERT INTO workflow_events(sequence,event_id,topic,event_type,source,subject,correlation_id,recorded_at_ms,command_json,envelope_json) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)", params![i64::try_from(e.sequence).map_err(storage)?,e.event.event_id.to_string(),e.event.topic,e.event.event_type,key(&e.event.source)?,e.event.subject,e.event.correlation_id,e.event.recorded_at_ms,encode(&e.event)?,encode(e)?]).map_err(storage)?;
    }
    for s in &a.subscriptions {
        super::event::insert_subscription(c, s, &encode(&s.definition)?).map_err(storage)?;
    }
    for d in &mut a.deliveries {
        d.lease_owner = None;
        d.lease_until_ms = None;
        if d.status == DeliveryStatus::Delivering {
            d.status = DeliveryStatus::RetryPending;
        }
        c.execute("INSERT INTO workflow_deliveries(subscription_id,event_id,event_sequence,status,attempts,next_attempt_at_ms,target_json,lease_owner,lease_until_ms,last_error,delivered_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,NULL,NULL,?8,?9)", params![d.delivery_id.subscription_id.to_string(),d.delivery_id.event_id.to_string(),i64::try_from(d.event_sequence).map_err(storage)?,key(&d.status)?,d.attempts,d.next_attempt_at_ms,encode(&d.target)?,d.last_error,d.delivered_at_ms]).map_err(storage)?;
    }
    for t in &mut a.timers {
        t.lease_owner = None;
        t.lease_until_ms = None;
        if t.status == TimerStatus::Firing {
            t.status = TimerStatus::Pending;
        }
        c.execute("INSERT INTO workflow_timers(timer_id,fire_at_ms,status,lease_owner,lease_until_ms,fired_event_id,definition_json,timer_json) VALUES(?1,?2,?3,NULL,NULL,?4,?5,?6)", params![t.definition.timer_id.to_string(),t.definition.fire_at_ms,key(&t.status)?,t.fired_event_id.map(|id| id.to_string()),encode(&t.definition)?,encode(t)?]).map_err(storage)?;
    }
    for j in &mut a.jobs {
        j.lease_owner = None;
        j.lease_until_ms = None;
        j.notification_lease_owner = None;
        j.notification_lease_until_ms = None;
        if j.notification_status == JobNotificationStatus::Publishing {
            j.notification_status = JobNotificationStatus::RetryPending;
        }
        let command = &j.command;
        c.execute("INSERT INTO workflow_jobs(job_id,run_id,kind,idempotency_key,status,next_attempt_at_ms,lease_owner,lease_until_ms,notification_status,notification_next_attempt_at_ms,notification_lease_owner,notification_lease_until_ms,dedupe_json,command_json,record_json) VALUES(?1,?2,?3,?4,?5,?6,NULL,NULL,?7,?8,NULL,NULL,?9,?10,?11)", params![command.job_id.to_string(),command.run_id.to_string(),command.kind,command.idempotency_key,key(&j.status)?,j.next_attempt_at_ms,key(&j.notification_status)?,j.notification_next_attempt_at_ms,encode(&(command.run_id,&command.kind,&command.input,&command.idempotency_key))?,encode(command)?,encode(j)?]).map_err(storage)?;
    }
    c.execute(
        "INSERT INTO workflow_fabric_records VALUES('checkpoints','$fabric-maintenance-v1',0,?1)",
        [fence],
    )
    .map_err(storage)?;
    Ok(())
}
