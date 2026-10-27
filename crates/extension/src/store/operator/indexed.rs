//! Indexed SQLite scheduling. Only eligible subscriptions and their next event are decoded.
use super::{storage, validate_key};
use agent_core::contracts::fabric::*;
use rusqlite::{OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Subscription {
    spec: SubscriptionSpec,
    offset: u64,
    epoch: u64,
    attempt: u32,
    due_at_ms: i64,
    lease: Option<DeliveryLease>,
}
fn invalid(message: &str) -> FabricError {
    FabricError::Invalid(message.into())
}
fn limit(value: usize) -> Result<(), FabricError> {
    if value == 0 || value > MAX_BATCH_ITEMS {
        return Err(invalid("batch/page limit"));
    }
    Ok(())
}
fn subscription(
    tx: &Transaction<'_>,
    consumer: &str,
    name: &str,
) -> Result<Subscription, FabricError> {
    let value: Option<String> = tx.query_row("SELECT value_json FROM workflow_fabric_records WHERE section='subscriptions' AND json_extract(value_json,'$.spec.consumer')=?1 AND json_extract(value_json,'$.spec.subscription')=?2", params![consumer, name], |r| r.get(0)).optional().map_err(storage)?;
    serde_json::from_str(&value.ok_or(FabricError::NotFound)?).map_err(storage)
}
fn receipt(tx: &Transaction<'_>, key: &ReceiptKey) -> Result<Option<Receipt>, FabricError> {
    let value: Option<String> = tx.query_row("SELECT value_json FROM workflow_fabric_records WHERE section='receipts' AND json_extract(value_json,'$.key.consumer')=?1 AND json_extract(value_json,'$.key.event_id')=?2 AND json_extract(value_json,'$.key.effect_id')=?3 AND json_extract(value_json,'$.key.invocation_id')=?4 AND json_extract(value_json,'$.key.attempt')=?5", params![key.consumer, key.event_id.to_string(), key.effect_id, key.invocation_id, key.attempt], |r| r.get(0)).optional().map_err(storage)?;
    value
        .map(|s| serde_json::from_str(&s).map_err(storage))
        .transpose()
}
fn decode_event(encoded: &str) -> Result<FabricEvent, FabricError> {
    let event: serde_json::Value = serde_json::from_str(encoded).map_err(storage)?;
    super::validate_legacy(&serde_json::json!({"schema_version":2,"events":[{"event":event}]}))?;
    serde_json::from_str(encoded).map_err(storage)
}
fn decode_stored(encoded: &str) -> Result<FabricEvent, FabricError> {
    let value: serde_json::Value = serde_json::from_str(encoded).map_err(storage)?;
    if value["retired"] == true {
        return Err(invalid("retention_expired"));
    }
    decode_event(&value["event"].to_string())
}
fn replay(
    tx: &Transaction<'_>,
    spec: &SubscriptionSpec,
    after: u64,
    count: usize,
) -> Result<Vec<ReplayItem>, FabricError> {
    // The global outbox barrier is intentional: an undispatched batch fences later facts.
    let mut statement = tx.prepare("SELECT value_json FROM workflow_fabric_records WHERE section='events' AND json_extract(value_json,'$.event.topic')=?1 AND (?2 IS NULL OR json_extract(value_json,'$.event.envelope.event_type')=?2) AND (?3 IS NULL OR json_extract(value_json,'$.event.envelope.tenant_id')=?3) AND position < COALESCE((SELECT MIN(position) FROM workflow_fabric_records WHERE section='events' AND json_extract(value_json,'$.visible')=0),9223372036854775807) ORDER BY position LIMIT ?4 OFFSET ?5").map_err(storage)?;
    let rows = statement
        .query_map(
            params![
                spec.topic,
                spec.event_type,
                spec.tenant,
                count as i64,
                i64::try_from(after).unwrap_or(i64::MAX)
            ],
            |r| r.get::<_, String>(0),
        )
        .map_err(storage)?;
    rows.enumerate()
        .map(|(index, row)| {
            let event = decode_stored(&row.map_err(storage)?)?;
            Ok(ReplayItem {
                offset: after + index as u64 + 1,
                event,
            })
        })
        .collect()
}
fn save_subscription(
    tx: &Transaction<'_>,
    position: i64,
    value: &Subscription,
) -> Result<(), FabricError> {
    tx.execute("UPDATE workflow_fabric_records SET value_json=?1 WHERE section='subscriptions' AND identity='' AND position=?2", params![serde_json::to_string(value).map_err(storage)?, position]).map_err(storage)?;
    Ok(())
}
fn append_dead_letter(tx: &Transaction<'_>, value: DeadLetter) -> Result<(), FabricError> {
    tx.execute("INSERT INTO workflow_fabric_records SELECT 'dead_letters','',COALESCE(MAX(position)+1,0),?1 FROM workflow_fabric_records WHERE section='dead_letters'", [serde_json::to_string(&value).map_err(storage)?]).map_err(storage)?;
    Ok(())
}
struct Claim<'a> {
    consumer: Option<&'a str>,
    worker: &'a str,
    now_ms: i64,
    lease_ms: i64,
    limit: usize,
    max_attempts: u32,
}
fn claim(tx: &Transaction<'_>, request: Claim<'_>) -> Result<FabricReply, FabricError> {
    let Claim {
        consumer,
        worker,
        now_ms,
        lease_ms,
        limit: batch_limit,
        max_attempts,
    } = request;
    validate_key(worker)?;
    if let Some(consumer) = consumer {
        validate_key(consumer)?;
    }
    limit(batch_limit)?;
    if lease_ms <= 0 || max_attempts == 0 {
        return Err(invalid("lease duration"));
    }
    let expires_at_ms = now_ms
        .checked_add(lease_ms)
        .ok_or_else(|| invalid("lease overflow"))?;
    // Separate query shapes let the consumer index prune unrelated subscriptions.
    let consumer_filter = if consumer.is_some() {
        " AND json_extract(value_json,'$.spec.consumer')=?2"
    } else {
        " AND ?2 IS NULL"
    };
    let index = if consumer.is_some() {
        "workflow_fabric_subscription_lookup"
    } else {
        "workflow_fabric_subscription_due"
    };
    let sql = format!(
        "SELECT position,value_json FROM workflow_fabric_records INDEXED BY {index} WHERE section='subscriptions' AND json_extract(value_json,'$.due_at_ms')<=?1 AND (json_extract(value_json,'$.lease') IS NULL OR json_extract(value_json,'$.lease.offset')<=json_extract(value_json,'$.offset') OR json_extract(value_json,'$.lease.expires_at_ms')<=?1){consumer_filter} ORDER BY position"
    );
    let mut statement = tx.prepare(&sql).map_err(storage)?;
    let mut rows = statement
        .query(params![now_ms, consumer])
        .map_err(storage)?;
    let mut claims = Vec::new();
    let mut reclaims = 0u64;
    // Accumulate only rows actually changed. Do not mutate a table under its read cursor.
    let mut updates = Vec::new();
    let mut dead_letters = Vec::new();
    while claims.len() < batch_limit {
        let Some(row) = rows.next().map_err(storage)? else {
            break;
        };
        let position: i64 = row.get(0).map_err(storage)?;
        let mut current: Subscription =
            serde_json::from_str(&row.get::<_, String>(1).map_err(storage)?).map_err(storage)?;
        let Some(item) = replay(tx, &current.spec, current.offset, 1)?.pop() else {
            continue;
        };
        if current.attempt >= max_attempts {
            let expired = current.lease.clone().ok_or(FabricError::Fenced)?;
            dead_letters.push(DeadLetter {
                event_id: item.event.envelope.event_id,
                lease: expired.clone(),
                error: "delivery lease expired after maximum attempts".into(),
                dead_lettered_at_ms: now_ms,
            });
            current.offset = expired.offset;
            current.attempt = 0;
            current.due_at_ms = i64::MIN;
            current.lease = None;
            updates.push((position, current));
            continue;
        }
        if current
            .lease
            .as_ref()
            .is_some_and(|lease| lease.offset > current.offset && lease.expires_at_ms <= now_ms)
        {
            reclaims += 1;
        }
        current.epoch = current
            .epoch
            .checked_add(1)
            .ok_or_else(|| invalid("epoch overflow"))?;
        current.attempt = current
            .attempt
            .checked_add(1)
            .ok_or_else(|| invalid("attempt overflow"))?;
        let lease = DeliveryLease {
            consumer: current.spec.consumer.clone(),
            subscription: current.spec.subscription.clone(),
            offset: item.offset,
            worker: worker.into(),
            epoch: current.epoch,
            expires_at_ms,
            delivery_attempt: current.attempt,
        };
        current.lease = Some(lease.clone());
        let committed = receipt(tx, &ReceiptKey::for_event(&lease.consumer, &item.event))?;
        claims.push(ClaimedDelivery {
            lease,
            event: item.event,
            committed,
        });
        updates.push((position, current));
    }
    drop(rows);
    drop(statement);
    for (position, current) in updates {
        save_subscription(tx, position, &current)?;
    }
    for letter in dead_letters {
        append_dead_letter(tx, letter)?;
    }
    if reclaims > 0 {
        let previous: Option<String> = tx
            .query_row(
                "SELECT value_json FROM workflow_fabric_records WHERE section='lease_reclaims'",
                [],
                |r| r.get(0),
            )
            .optional()
            .map_err(storage)?;
        let previous: u64 = previous
            .map(|s| serde_json::from_str(&s).map_err(storage))
            .transpose()?
            .unwrap_or(0);
        tx.execute("INSERT INTO workflow_fabric_records VALUES('lease_reclaims','',0,?1) ON CONFLICT(section,identity,position) DO UPDATE SET value_json=excluded.value_json", [previous.saturating_add(reclaims).to_string()]).map_err(storage)?;
    }
    Ok(FabricReply::Claimed(claims))
}
pub(super) fn execute(
    tx: &Transaction<'_>,
    command: &FabricCommand,
) -> Result<Option<FabricReply>, FabricError> {
    let result = match command {
        FabricCommand::ClaimForConsumer {
            consumer,
            worker,
            now_ms,
            lease_ms,
            limit,
            max_attempts,
        } => claim(
            tx,
            Claim {
                consumer: Some(consumer),
                worker,
                now_ms: *now_ms,
                lease_ms: *lease_ms,
                limit: *limit,
                max_attempts: *max_attempts,
            },
        )?,
        FabricCommand::Claim {
            worker,
            now_ms,
            lease_ms,
            limit,
            max_attempts,
        } => claim(
            tx,
            Claim {
                consumer: None,
                worker,
                now_ms: *now_ms,
                lease_ms: *lease_ms,
                limit: *limit,
                max_attempts: *max_attempts,
            },
        )?,
        FabricCommand::Offset {
            consumer,
            subscription: name,
        } => FabricReply::Offset(subscription(tx, consumer, name)?.offset),
        FabricCommand::Receipt(key) => FabricReply::Receipt(receipt(tx, key)?),
        FabricCommand::Replay {
            consumer,
            subscription: name,
            after,
            limit: count,
        } => {
            limit(*count)?;
            FabricReply::Replay(replay(
                tx,
                &subscription(tx, consumer, name)?.spec,
                *after,
                *count,
            )?)
        }
        FabricCommand::ReadEvent(id) => {
            let event: Option<String> = tx.query_row("SELECT value_json FROM workflow_fabric_records WHERE section='events' AND json_extract(value_json,'$.visible')=1 AND json_extract(value_json,'$.event.envelope.event_id')=?1", [id.to_string()], |r| r.get(0)).optional().map_err(storage)?;
            FabricReply::Event(event.map(|s| decode_stored(&s)).transpose()?)
        }
        FabricCommand::ScanEvents {
            topic,
            event_type,
            after,
            limit: count,
        } => {
            validate_key(topic)?;
            validate_key(event_type)?;
            limit(*count)?;
            let mut statement = tx.prepare("SELECT position,value_json FROM workflow_fabric_records WHERE section='events' AND json_extract(value_json,'$.event.topic')=?1 AND json_extract(value_json,'$.event.envelope.event_type')=?2 AND json_extract(value_json,'$.visible')=1 AND position>=?3 ORDER BY position LIMIT ?4").map_err(storage)?;
            let records = statement
                .query_map(
                    params![
                        topic,
                        event_type,
                        i64::try_from(*after).unwrap_or(i64::MAX),
                        *count as i64
                    ],
                    |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)),
                )
                .map_err(storage)?;
            let events = records
                .map(|r| {
                    let (position, encoded) = r.map_err(storage)?;
                    Ok(ReplayItem {
                        offset: position as u64 + 1,
                        event: decode_stored(&encoded)?,
                    })
                })
                .collect::<Result<Vec<_>, FabricError>>()?;
            FabricReply::Replay(events)
        }
        FabricCommand::DeadLetters {
            after,
            limit: count,
        } => {
            limit(*count)?;
            let mut statement = tx.prepare("SELECT value_json FROM workflow_fabric_records WHERE section='dead_letters' ORDER BY position LIMIT ?1 OFFSET ?2").map_err(storage)?;
            let records = statement
                .query_map(
                    params![*count as i64, i64::try_from(*after).unwrap_or(i64::MAX)],
                    |r| r.get::<_, String>(0),
                )
                .map_err(storage)?;
            FabricReply::DeadLetters(
                records
                    .map(|r| serde_json::from_str(&r.map_err(storage)?).map_err(storage))
                    .collect::<Result<_, _>>()?,
            )
        }
        _ => return Ok(None),
    };
    Ok(Some(result))
}

/// Load only the coordination records consulted by this transition. Array positions
/// remain database identities; compact positions used by Core are remapped on save.
pub(super) fn scoped_rows(
    tx: &Transaction<'_>,
    command: &FabricCommand,
) -> Result<Option<super::Rows>, FabricError> {
    use FabricCommand::*;
    let (lease, spec, outbox): (
        Option<&DeliveryLease>,
        Option<&SubscriptionSpec>,
        &[FabricEvent],
    ) = match command {
        Transact {
            outbox, delivery, ..
        }
        | TransactMany {
            outbox, delivery, ..
        } => (delivery.as_ref().map(|(l, _)| l), None, outbox),
        GuardedTransact { lease, outbox, .. }
        | GuardedTransactMany { lease, outbox, .. }
        | Commit { lease, outbox, .. } => (Some(lease), None, outbox),
        Retry { lease, .. } => (Some(lease), None, &[]),
        Subscribe(spec) => (None, Some(spec), &[]),
        Publish(event) => (None, None, std::slice::from_ref(event)),
        PublishBatch(events) => (None, None, events),
        PublishBounded {
            events,
            subscription,
            ..
        } => (None, Some(subscription), events),
        ReadCheckpoint(_) => (None, None, &[]),
        DispatchOutbox { .. } => (None, None, &[]),
        _ => return Ok(None),
    };
    let mut rows = super::Rows::new();
    super::read_rows(
        tx,
        "SELECT section,identity,position,value_json FROM workflow_fabric_records WHERE section IN ('schema_version','lease_reclaims')",
        [],
        &mut rows,
    )?;
    let mut stream_spec = None;
    if let Some(lease) = lease {
        super::read_rows(
            tx,
            "SELECT section,identity,position,value_json FROM workflow_fabric_records WHERE section='subscriptions' AND json_extract(value_json,'$.spec.consumer')=?1 AND json_extract(value_json,'$.spec.subscription')=?2",
            params![lease.consumer, lease.subscription],
            &mut rows,
        )?;
        stream_spec = Some(subscription(tx, &lease.consumer, &lease.subscription)?.spec);
    } else if let Some(spec) = spec {
        super::read_rows(
            tx,
            "SELECT section,identity,position,value_json FROM workflow_fabric_records WHERE section='subscriptions' AND json_extract(value_json,'$.spec.consumer')=?1 AND json_extract(value_json,'$.spec.subscription')=?2",
            params![spec.consumer, spec.subscription],
            &mut rows,
        )?;
        if matches!(command, PublishBounded { .. }) {
            stream_spec = Some(spec.clone());
        }
    }
    if let Some(spec) = &stream_spec {
        super::read_rows(
            tx,
            "SELECT section,identity,position,value_json FROM workflow_fabric_records WHERE section='events' AND json_extract(value_json,'$.event.topic')=?1 AND (?2 IS NULL OR json_extract(value_json,'$.event.envelope.event_type')=?2) AND (?3 IS NULL OR json_extract(value_json,'$.event.envelope.tenant_id')=?3) ORDER BY position",
            params![spec.topic, spec.event_type, spec.tenant],
            &mut rows,
        )?;
        // Preserve the global outbox visibility barrier even when it belongs to another topic.
        super::read_rows(
            tx,
            "SELECT section,identity,position,value_json FROM workflow_fabric_records WHERE section='events' AND json_extract(value_json,'$.visible')=0 ORDER BY position LIMIT 1",
            [],
            &mut rows,
        )?;
    }
    if let Some(lease) = lease {
        // Only the source receipt can affect this lease's commit/retry.
        if let Some(spec) = &stream_spec
            && let Some(source) = replay(tx, spec, lease.offset.saturating_sub(1), 1)?.pop()
        {
            super::read_rows(
                tx,
                "SELECT section,identity,position,value_json FROM workflow_fabric_records WHERE section='receipts' AND json_extract(value_json,'$.key.consumer')=?1 AND json_extract(value_json,'$.key.event_id')=?2",
                params![lease.consumer, source.event.envelope.event_id.to_string()],
                &mut rows,
            )?;
        }
    }
    for event in outbox {
        let run_key = agent_core::agent::graph::run_checkpoint_key(
            &event.envelope.tenant_id,
            event.envelope.run_id,
        );
        super::read_rows(
            tx,
            "SELECT section,identity,position,value_json FROM workflow_fabric_records WHERE section='checkpoints' AND identity=?1",
            [run_key],
            &mut rows,
        )?;
        // Sequence and engine checks need the last fact of the partition; dedup needs only the requested id.
        super::read_rows(
            tx,
            "SELECT section,identity,position,value_json FROM workflow_fabric_records WHERE section='events' AND json_extract(value_json,'$.event.envelope.tenant_id')=?1 AND json_extract(value_json,'$.event.envelope.run_id')=?2 ORDER BY position DESC LIMIT 1",
            params![event.envelope.tenant_id, event.envelope.run_id.to_string()],
            &mut rows,
        )?;
        super::read_rows(
            tx,
            "SELECT section,identity,position,value_json FROM workflow_fabric_records WHERE section='events' AND json_extract(value_json,'$.event.envelope.event_id')=?1",
            [event.envelope.event_id.to_string()],
            &mut rows,
        )?;
    }
    if matches!(command, DispatchOutbox { .. }) {
        super::read_rows(
            tx,
            "SELECT section,identity,position,value_json FROM workflow_fabric_records WHERE section='events' AND json_extract(value_json,'$.visible')=0 ORDER BY position",
            [],
            &mut rows,
        )?;
    }
    Ok(Some(rows))
}

pub(super) fn restore_positions(
    tx: &Transaction<'_>,
    before: &super::Rows,
    after: super::Rows,
) -> Result<super::Rows, FabricError> {
    let mut result = super::Rows::new();
    for section in ["events", "subscriptions", "receipts", "dead_letters"] {
        let positions: Vec<_> = before
            .keys()
            .filter(|(s, _, _)| s == section)
            .map(|(_, _, p)| *p)
            .collect();
        let mut next: Option<i64> = None;
        for ((s, identity, position), value) in after.iter().filter(|((s, _, _), _)| s == section) {
            let restored = if let Some(position) = positions.get(*position as usize) {
                *position
            } else {
                let position = match next {
                    Some(position) => position,
                    None => tx.query_row("SELECT COALESCE(MAX(position)+1,0) FROM workflow_fabric_records WHERE section=?1", [section], |r| r.get(0)).map_err(storage)?,
                };
                next = Some(
                    position
                        .checked_add(1)
                        .ok_or_else(|| invalid("record position overflow"))?,
                );
                position
            };
            result.insert((s.clone(), identity.clone(), restored), value.clone());
        }
    }
    result.extend(after.into_iter().filter(|((s, _, _), _)| {
        !matches!(
            s.as_str(),
            "events" | "subscriptions" | "receipts" | "dead_letters"
        )
    }));
    Ok(result)
}
