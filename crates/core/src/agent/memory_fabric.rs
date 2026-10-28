//! Reference transition engine shared by memory and the existing SQLite EventStore.

use std::sync::{Arc, Mutex};

use crate::{
    contracts::fabric::*,
    operator::{DeliveryClass, EngineVersion, PayloadContract},
};
use serde::{Deserialize, Serialize};
pub mod archive;
mod retention;

#[derive(Default)]
pub struct MemoryFabric {
    state: Mutex<FabricState>,
    changed: tokio::sync::watch::Sender<u64>,
}

impl OperatorFabric for MemoryFabric {
    fn changes(&self) -> Option<tokio::sync::watch::Receiver<u64>> {
        Some(self.changed.subscribe())
    }
    fn capabilities(&self) -> FabricCapabilities {
        FabricCapabilities {
            atomic_batch: true,
            continuous_offset: true,
            lease_fencing: true,
            transactional_outbox: true,
            durable: false,
        }
    }

    fn execute(&self, command: FabricCommand) -> FabricFuture<'_> {
        let writes_checkpoint = command.writes_checkpoint();
        Box::pin(async move {
            let mut state = self.state.lock().map_err(storage)?;
            // These commands either only read or validate completely before a
            // single infallible mutation. They need no rollback snapshot.
            let result = if matches!(
                &command,
                FabricCommand::ReadCheckpoint(_)
                    | FabricCommand::ReadLog { .. }
                    | FabricCommand::ReadEvent(_)
                    | FabricCommand::ScanEvents { .. }
                    | FabricCommand::Replay { .. }
                    | FabricCommand::Offset { .. }
                    | FabricCommand::Receipt(_)
                    | FabricCommand::DeadLetters { .. }
                    | FabricCommand::InspectRetention { .. }
                    | FabricCommand::Subscribe(_)
                    | FabricCommand::DispatchOutbox { .. }
                    | FabricCommand::RetireRun { .. }
                    | FabricCommand::CollectArtifacts { .. }
                    | FabricCommand::ArtifactRoots
            ) {
                state.apply(command)
            } else {
                state.apply_atomic(command)
            };
            if let Ok(reply) = &result
                && (writes_checkpoint || observable_change(reply))
            {
                self.changed
                    .send_modify(|generation| *generation = generation.wrapping_add(1));
            }
            result
        })
    }
}

pub fn storage(error: impl std::fmt::Display) -> FabricError {
    FabricError::Storage(error.to_string())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FabricState {
    #[serde(skip)]
    undo: Option<Undo>,
    #[serde(default = "schema_version")]
    schema_version: u64,
    #[serde(default)]
    lease_reclaims: u64,
    #[serde(default)]
    checkpoints: std::collections::BTreeMap<String, Arc<FabricCheckpoint>>,
    #[serde(default)]
    logs: std::collections::BTreeMap<String, Vec<Arc<serde_json::Value>>>,
    events: Vec<Arc<StoredEvent>>,
    subscriptions: Vec<StoredSubscription>,
    receipts: Vec<Arc<Receipt>>,
    dead_letters: Vec<DeadLetter>,
}

// Only overwritten records and append watermarks are needed for transaction rollback.
// Historical payloads and untouched containers are never copied.
#[derive(Debug, Clone)]
struct Undo {
    schema_version: u64,
    lease_reclaims: u64,
    events: usize,
    subscriptions: usize,
    receipts: usize,
    dead_letters: usize,
    changed_subscriptions: std::collections::BTreeMap<usize, StoredSubscription>,
    checkpoints: std::collections::BTreeMap<String, Option<Arc<FabricCheckpoint>>>,
    logs: std::collections::BTreeMap<String, Option<usize>>,
}

fn schema_version() -> u64 {
    1
}

impl Default for FabricState {
    fn default() -> Self {
        Self {
            undo: None,
            schema_version: schema_version(),
            lease_reclaims: 0,
            checkpoints: Default::default(),
            logs: Default::default(),
            events: Vec::new(),
            subscriptions: Vec::new(),
            receipts: Vec::new(),
            dead_letters: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredEvent {
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    retired: bool,
    event: FabricEvent,
    visible: bool,
    outbox_batch: Option<crate::contracts::events::EventId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredSubscription {
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

fn key(value: &str) -> Result<(), FabricError> {
    if value.trim().is_empty() || value.len() > MAX_KEY_BYTES {
        return Err(invalid("identity length"));
    }
    Ok(())
}

fn limit(value: usize) -> Result<(), FabricError> {
    if value == 0 || value > MAX_BATCH_ITEMS {
        return Err(invalid("batch/page limit"));
    }
    Ok(())
}

fn same_partition(first: &FabricEvent, second: &FabricEvent) -> bool {
    first.envelope.tenant_id == second.envelope.tenant_id
        && first.envelope.run_id == second.envelope.run_id
}

fn validate_events(events: &[FabricEvent], allow_empty: bool) -> Result<(), FabricError> {
    if events.is_empty() && allow_empty {
        return Ok(());
    }
    limit(events.len())?;
    let mut bytes = 0usize;
    for event in events {
        key(&event.topic)?;
        key(&event.envelope.event_type)?;
        if !same_partition(&events[0], event) {
            return Err(invalid(
                "atomic batch must belong to one tenant/run partition",
            ));
        }
        let size = serde_json::to_vec(event).map_err(storage)?.len();
        if size > MAX_EVENT_BYTES {
            return Err(invalid("event byte limit"));
        }
        bytes = bytes
            .checked_add(size)
            .ok_or_else(|| invalid("batch overflow"))?;
        if bytes > MAX_BATCH_BYTES {
            return Err(invalid("batch byte limit"));
        }
        let legacy_context = event.envelope.engine_version == EngineVersion::LegacyMachine
            && event.topic == crate::contracts::preparation::CONTEXT_TOPIC
            && matches!(
                event.envelope.event_type.as_str(),
                "run.requested" | "context.requested" | "context.completed" | "context.failed"
            );
        if event.envelope.delivery_class != DeliveryClass::Durable
            || (event.envelope.engine_version != EngineVersion::OperatorV1 && !legacy_context)
        {
            return Err(invalid("only operator_v1 durable events are supported"));
        }
        event
            .envelope
            .validate(&PayloadContract {
                event_type: event.envelope.event_type.clone(),
                delivery_class: DeliveryClass::Durable,
                schema: event.payload_schema.clone(),
                max_payload_bytes: MAX_EVENT_BYTES,
            })
            .map_err(|error| invalid(&error.to_string()))?;
    }
    Ok(())
}

impl FabricState {
    fn apply_atomic(&mut self, command: FabricCommand) -> Result<FabricReply, FabricError> {
        debug_assert!(self.undo.is_none());
        self.undo = Some(Undo {
            schema_version: self.schema_version,
            lease_reclaims: self.lease_reclaims,
            events: self.events.len(),
            subscriptions: self.subscriptions.len(),
            receipts: self.receipts.len(),
            dead_letters: self.dead_letters.len(),
            changed_subscriptions: Default::default(),
            checkpoints: Default::default(),
            logs: Default::default(),
        });
        let result = self.apply(command);
        let undo = self.undo.take().expect("transaction undo");
        if result.is_err() {
            self.schema_version = undo.schema_version;
            self.lease_reclaims = undo.lease_reclaims;
            self.events.truncate(undo.events);
            self.subscriptions.truncate(undo.subscriptions);
            self.receipts.truncate(undo.receipts);
            self.dead_letters.truncate(undo.dead_letters);
            for (index, value) in undo.changed_subscriptions {
                self.subscriptions[index] = value;
            }
            for (key, value) in undo.checkpoints {
                if let Some(value) = value {
                    self.checkpoints.insert(key, value);
                } else {
                    self.checkpoints.remove(&key);
                }
            }
            for (key, length) in undo.logs {
                if let Some(length) = length {
                    self.logs
                        .get_mut(&key)
                        .expect("existing log")
                        .truncate(length);
                } else {
                    self.logs.remove(&key);
                }
            }
        }
        result
    }

    fn remember_subscription(&mut self, index: usize) {
        if let Some(undo) = &mut self.undo
            && index < undo.subscriptions
        {
            undo.changed_subscriptions
                .entry(index)
                .or_insert_with(|| self.subscriptions[index].clone());
        }
    }

    fn subscription(&self, consumer: &str, subscription: &str) -> Result<usize, FabricError> {
        self.subscriptions
            .iter()
            .position(|item| {
                item.spec.consumer == consumer && item.spec.subscription == subscription
            })
            .ok_or(FabricError::NotFound)
    }

    // Keep filtering borrowed: callers seeking one offset must not clone the
    // entire subscription history (or even its skipped prefix).
    fn stream(&self, index: usize) -> impl Iterator<Item = ReplayItem> + '_ {
        let spec = &self.subscriptions[index].spec;
        self.events
            .iter()
            .take_while(|stored| stored.visible)
            .filter(move |stored| {
                let event = &stored.event;
                event.topic == spec.topic
                    && spec
                        .event_type
                        .as_ref()
                        .is_none_or(|kind| kind == &event.envelope.event_type)
                    && spec
                        .tenant
                        .as_ref()
                        .is_none_or(|tenant| tenant == &event.envelope.tenant_id)
            })
            .enumerate()
            .map(|(index, stored)| ReplayItem {
                offset: index as u64 + 1,
                event: stored.event.clone(),
            })
    }

    fn publish(
        &mut self,
        events: Vec<FabricEvent>,
        visible: bool,
    ) -> Result<FabricReply, FabricError> {
        validate_events(&events, false)?;
        let outbox_batch = (!visible).then_some(events[0].envelope.event_id);
        let mut ids = Vec::new();
        for event in events {
            self.ensure_available(&event)?;
            let event_id = event.envelope.event_id;
            if let Some(existing) = self
                .events
                .iter()
                .find(|stored| stored.event.envelope.event_id == event_id)
            {
                if existing.event != event {
                    return Err(FabricError::Conflict);
                }
            } else {
                let previous_event = self
                    .events
                    .iter()
                    .rev()
                    .find(|stored| same_partition(&stored.event, &event));
                if previous_event.is_some_and(|stored| {
                    stored.event.envelope.engine_version != event.envelope.engine_version
                }) {
                    return Err(invalid("engine version cannot change within a run"));
                }
                let previous =
                    previous_event.map_or(0, |stored| stored.event.envelope.operator_seq.0);
                if previous.checked_add(1) != Some(event.envelope.operator_seq.0) {
                    return Err(invalid("non-contiguous partition sequence"));
                }
                self.events.push(Arc::new(StoredEvent {
                    retired: false,
                    event,
                    visible,
                    outbox_batch,
                }));
            }
            ids.push(event_id);
        }
        Ok(FabricReply::Published(ids))
    }

    fn publish_bounded(
        &mut self,
        events: Vec<FabricEvent>,
        subscription: SubscriptionSpec,
        max_pending: usize,
    ) -> Result<FabricReply, FabricError> {
        if max_pending == 0 || max_pending > 4096 {
            return Err(invalid("pending limit"));
        }
        validate_events(&events, false)?;
        self.apply(FabricCommand::Subscribe(subscription.clone()))?;
        let index = self.subscription(&subscription.consumer, &subscription.subscription)?;
        let matches = |event: &FabricEvent| {
            event.topic == subscription.topic
                && subscription
                    .event_type
                    .as_ref()
                    .is_none_or(|kind| kind == &event.envelope.event_type)
                && subscription
                    .tenant
                    .as_ref()
                    .is_none_or(|tenant| tenant == &event.envelope.tenant_id)
        };
        let pending = self
            .events
            .iter()
            .filter(|stored| matches(&stored.event))
            .count()
            .saturating_sub(self.subscriptions[index].offset as usize);
        let added = events
            .iter()
            .filter(|event| {
                matches(event)
                    && !self
                        .events
                        .iter()
                        .any(|stored| stored.event.envelope.event_id == event.envelope.event_id)
            })
            .count();
        if added > 0 && pending.saturating_add(added) > max_pending {
            return Err(FabricError::Capacity);
        }
        self.publish(events, true)
    }

    fn validate_lease(&self, lease: &DeliveryLease, now_ms: i64) -> Result<usize, FabricError> {
        let index = self.subscription(&lease.consumer, &lease.subscription)?;
        let current = &self.subscriptions[index];
        if lease.offset > current.offset.saturating_add(1) {
            return Err(FabricError::AckHole);
        }
        if current.lease.as_ref() != Some(lease) || now_ms >= lease.expires_at_ms {
            return Err(FabricError::Fenced);
        }
        if let Some(source) = self
            .stream(index)
            .nth(usize::try_from(lease.offset.saturating_sub(1)).unwrap_or(usize::MAX))
        {
            self.ensure_available(&source.event)?;
        }
        Ok(index)
    }

    fn acknowledge(&mut self, index: usize, lease: &DeliveryLease) {
        self.remember_subscription(index);
        let subscription = &mut self.subscriptions[index];
        subscription.offset = lease.offset;
        subscription.attempt = 0;
        subscription.due_at_ms = i64::MIN;
    }

    pub fn apply(&mut self, command: FabricCommand) -> Result<FabricReply, FabricError> {
        self.apply_scoped(command, None)
    }

    fn apply_scoped(
        &mut self,
        command: FabricCommand,
        consumer: Option<&str>,
    ) -> Result<FabricReply, FabricError> {
        if self.checkpoints.contains_key(archive::MAINTENANCE_KEY)
            && !archive::maintenance_allowed(&command)
        {
            return Err(FabricError::Fenced);
        }
        match command {
            FabricCommand::BeginMaintenance { token, now_ms } => {
                self.begin_maintenance(token, now_ms)
            }
            FabricCommand::EndMaintenance { token } => self.end_maintenance(&token),
            FabricCommand::ExportAgentHistory { tenant, agent_id } => {
                self.export_agent_history(&tenant, &agent_id)
            }
            FabricCommand::InspectRun { tenant, run_id } => self.inspect_run(&tenant, run_id),
            FabricCommand::ExportRun {
                tenant,
                run_id,
                maintenance_token,
            } => self.export_run(&tenant, run_id, &maintenance_token),
            FabricCommand::ImportRun {
                bundle,
                maintenance_token,
            } => self.import_run(bundle, &maintenance_token),
            FabricCommand::HostTransact {
                records,
                completion,
                transaction,
            } => {
                if !matches!(
                    *transaction,
                    FabricCommand::Transact { .. }
                        | FabricCommand::TransactMany { .. }
                        | FabricCommand::TransactLog { .. }
                        | FabricCommand::Commit { .. }
                ) {
                    return Err(invalid("host transaction must contain a single commit"));
                }
                if records.len() > MAX_BATCH_ITEMS {
                    return Err(invalid("host record count"));
                }
                let mut writes = std::collections::BTreeMap::new();
                for record in records {
                    record.validate()?;
                    let key = crate::contracts::host::host_record_key(
                        &record.tenant,
                        &record.namespace,
                        &record.operation_id,
                    );
                    let value = serde_json::to_value(record).map_err(storage)?;
                    if writes
                        .insert(key, value.clone())
                        .is_some_and(|old| old != value)
                    {
                        return Err(FabricError::Conflict);
                    }
                }
                let mut completion_append = None;
                if let Some(record) = completion {
                    if record.schema_version != 1
                        || record.completion_id
                            != format!(
                                "{}",
                                crate::contracts::events::EventId::stable(
                                    "run-completion",
                                    &serde_json::json!([record.tenant, record.run_id]).to_string()
                                )
                            )
                    {
                        return Err(invalid("completion identity/version"));
                    }
                    let key = crate::contracts::host::host_record_key(
                        &record.tenant,
                        "$completion",
                        &record.run_id.to_string(),
                    );
                    let value = serde_json::to_value(&record).map_err(storage)?;
                    if let Some(old) = self.checkpoints.get(&key) {
                        if old.value != value {
                            return Err(FabricError::Conflict);
                        }
                    } else {
                        completion_append = Some((
                            crate::contracts::host::completion_log(&record.tenant),
                            value.clone(),
                        ));
                        writes.insert(key, value);
                    }
                }
                for (key, value) in &writes {
                    if self
                        .checkpoints
                        .get(key)
                        .is_some_and(|old| old.value != *value)
                    {
                        return Err(FabricError::Conflict);
                    }
                }
                let reply = self.apply_scoped(*transaction, consumer)?;
                for (key, value) in writes {
                    if !self.checkpoints.contains_key(&key) {
                        self.apply_scoped(
                            FabricCommand::Transact {
                                checkpoint: CheckpointWrite {
                                    key,
                                    expected: None,
                                    value,
                                },
                                outbox: vec![],
                                delivery: None,
                            },
                            consumer,
                        )?;
                    }
                }
                if let Some((log, value)) = completion_append {
                    if let Some(undo) = &mut self.undo {
                        undo.logs
                            .entry(log.clone())
                            .or_insert_with(|| self.logs.get(&log).map(Vec::len));
                    }
                    self.schema_version = self.schema_version.max(2);
                    self.logs.entry(log).or_default().push(Arc::new(value));
                }
                Ok(reply)
            }
            FabricCommand::RetireRun {
                tenant,
                run_id,
                now_ms,
                retention_ms,
                dry_run,
            } => self
                .retire_run(&tenant, run_id, now_ms, retention_ms, dry_run)
                .map(FabricReply::Collected),
            FabricCommand::CollectArtifacts {
                now_ms,
                grace_ms,
                external_roots,
                dry_run,
            } => self
                .collect_artifacts(now_ms, grace_ms, &external_roots, dry_run)
                .map(FabricReply::Collected),
            FabricCommand::ArtifactRoots => {
                Ok(FabricReply::ArtifactRoots(self.artifact_roots(&[])?))
            }
            FabricCommand::GuardedTransactLog {
                lease,
                now_ms,
                log,
                expected,
                items,
                checkpoints,
            } => {
                let index = self.validate_lease(&lease, now_ms)?;
                if self.subscriptions[index].offset >= lease.offset {
                    return Err(FabricError::Fenced);
                }
                self.apply(FabricCommand::TransactLog {
                    log,
                    expected,
                    items,
                    checkpoints,
                    outbox: vec![],
                    delivery: None,
                })
            }
            FabricCommand::ReadLog {
                log,
                after,
                limit: page_limit,
            } => {
                key(&log)?;
                limit(page_limit)?;
                let page = self.logs.get(&log).map(|items| {
                    let mut bytes = 0usize;
                    let values = items
                        .iter()
                        .skip(usize::try_from(after).unwrap_or(usize::MAX))
                        .take(page_limit)
                        .take_while(|item| {
                            let size = serde_json::to_vec(item.as_ref()).expect("JSON value").len();
                            bytes = bytes.saturating_add(size);
                            bytes <= MAX_CHECKPOINT_BYTES
                        })
                        .map(|item| item.as_ref().clone())
                        .collect();
                    LogPage {
                        through: items.len() as u64,
                        items: values,
                    }
                });
                Ok(FabricReply::Log(page))
            }
            FabricCommand::TransactLog {
                log,
                expected,
                items,
                checkpoints,
                outbox,
                delivery,
            } => {
                key(&log)?;
                let through = self.logs.get(&log).map_or(0, |items| items.len() as u64);
                if expected != through {
                    return Err(FabricError::Fenced);
                }
                if serde_json::to_vec(&items).map_err(storage)?.len() > MAX_CHECKPOINT_BYTES {
                    return Err(invalid("log transaction byte limit"));
                }
                let reply = self.apply(FabricCommand::TransactMany {
                    checkpoints,
                    outbox,
                    delivery,
                })?;
                if let Some(undo) = &mut self.undo {
                    undo.logs
                        .entry(log.clone())
                        .or_insert_with(|| self.logs.get(&log).map(Vec::len));
                }
                self.schema_version = self.schema_version.max(2);
                self.logs
                    .entry(log)
                    .or_default()
                    .extend(items.into_iter().map(Arc::new));
                Ok(reply)
            }

            FabricCommand::InspectRetention {
                now_ms,
                grace_ms,
                external_roots,
            } => {
                if grace_ms <= 0 || external_roots.len() > 4096 {
                    return Err(invalid("retention bounds"));
                }
                let roots = serde_json::to_string(&(
                    &self.events,
                    &self.receipts,
                    &self.dead_letters,
                    &self.logs,
                    self.checkpoints
                        .iter()
                        .filter(|(key, _)| !key.starts_with("pipeline-artifact:"))
                        .collect::<Vec<_>>(),
                    &external_roots,
                ))
                .map_err(storage)?;
                let mut report = RetentionInventory {
                    events: self.events.len(),
                    checkpoints: self.checkpoints.len(),
                    outbox_backlog: self.events.iter().filter(|event| !event.visible).count(),
                    oldest_outbox_age_ms: self
                        .events
                        .iter()
                        .filter(|event| !event.visible)
                        .map(|event| {
                            now_ms
                                .saturating_sub(event.event.envelope.occurred_at_ms)
                                .max(0)
                        })
                        .max()
                        .unwrap_or(0),
                    dead_letters: self.dead_letters.len(),
                    lease_reclaims: self.lease_reclaims,
                    active_leases: self
                        .subscriptions
                        .iter()
                        .filter(|subscription| {
                            subscription.lease.as_ref().is_some_and(|lease| {
                                lease.offset > subscription.offset && lease.expires_at_ms > now_ms
                            })
                        })
                        .count(),
                    protected_artifacts: 0,
                    orphan_candidates: vec![],
                };
                for (key, checkpoint) in &self.checkpoints {
                    let Some(id) = key
                        .strip_prefix("pipeline-artifact:")
                        .and_then(|id| id.parse::<crate::contracts::BlobId>().ok())
                    else {
                        continue;
                    };
                    if roots.contains(&id.to_string()) {
                        report.protected_artifacts += 1;
                    } else if checkpoint.value["metadata"]["created_at_ms"]
                        .as_i64()
                        .is_some_and(|created| {
                            now_ms >= created && now_ms.saturating_sub(created) >= grace_ms
                        })
                    {
                        report.orphan_candidates.push(id);
                    }
                }
                Ok(FabricReply::Retention(report))
            }
            FabricCommand::PublishBounded {
                events,
                subscription,
                max_pending,
            } => self.publish_bounded(events, subscription, max_pending),
            FabricCommand::TransactMany {
                mut checkpoints,
                outbox,
                delivery,
            } => {
                limit(checkpoints.len())?;
                let mut keys = std::collections::BTreeSet::new();
                let mut bytes = 0_usize;
                for checkpoint in &checkpoints {
                    if !keys.insert(&checkpoint.key) {
                        return Err(invalid("duplicate checkpoint write"));
                    }
                    bytes = bytes.saturating_add(
                        serde_json::to_vec(&checkpoint.value)
                            .map_err(storage)?
                            .len(),
                    );
                }
                if bytes > MAX_CHECKPOINT_BYTES {
                    return Err(invalid("checkpoint transaction byte limit"));
                }
                let last = checkpoints
                    .pop()
                    .ok_or_else(|| invalid("empty checkpoint transaction"))?;
                for checkpoint in checkpoints {
                    self.apply(FabricCommand::Transact {
                        checkpoint,
                        outbox: vec![],
                        delivery: None,
                    })?;
                }
                self.apply(FabricCommand::Transact {
                    checkpoint: last,
                    outbox,
                    delivery,
                })
            }
            FabricCommand::ClaimForConsumer {
                consumer,
                worker,
                now_ms,
                lease_ms,
                limit,
                max_attempts,
            } => {
                key(&consumer)?;
                self.apply_scoped(
                    FabricCommand::Claim {
                        worker,
                        now_ms,
                        lease_ms,
                        limit,
                        max_attempts,
                    },
                    Some(&consumer),
                )
            }
            FabricCommand::GuardedTransact {
                lease,
                now_ms,
                checkpoint,
                outbox,
            } => self.apply(FabricCommand::GuardedTransactMany {
                lease,
                now_ms,
                checkpoints: vec![checkpoint],
                outbox,
            }),
            FabricCommand::GuardedTransactMany {
                lease,
                now_ms,
                checkpoints,
                outbox,
            } => {
                let index = self.validate_lease(&lease, now_ms)?;
                if self.subscriptions[index].offset >= lease.offset {
                    return Err(FabricError::Fenced);
                }
                let source = self
                    .stream(index)
                    .nth(usize::try_from(lease.offset.saturating_sub(1)).unwrap_or(usize::MAX))
                    .ok_or(FabricError::AckHole)?
                    .event;
                if outbox.iter().any(|event| !same_partition(&source, event)) {
                    return Err(invalid("outbox belongs to another run"));
                }
                self.apply(FabricCommand::TransactMany {
                    checkpoints,
                    outbox,
                    delivery: None,
                })
            }
            FabricCommand::ReadCheckpoint(identity) => {
                key(&identity)?;
                if self
                    .checkpoints
                    .get(&identity)
                    .is_some_and(|c| c.value["retired"] == true)
                {
                    return Err(invalid("retention_expired"));
                }
                Ok(FabricReply::Checkpoint(
                    self.checkpoints
                        .get(&identity)
                        .map(|value| value.as_ref().clone()),
                ))
            }
            FabricCommand::Transact {
                checkpoint,
                mut outbox,
                delivery,
            } => {
                key(&checkpoint.key)?;
                if self
                    .checkpoints
                    .get(&checkpoint.key)
                    .map(|record| record.version)
                    != checkpoint.expected
                {
                    return Err(FabricError::Fenced);
                }
                if serde_json::to_vec(&checkpoint.value)
                    .map_err(storage)?
                    .len()
                    > MAX_CHECKPOINT_BYTES
                {
                    return Err(invalid("checkpoint byte limit"));
                }
                let version = checkpoint
                    .expected
                    .map_or(Some(0), |version| version.checked_add(1))
                    .ok_or_else(|| invalid("checkpoint version overflow"))?;
                let mut previous = outbox.first().map_or(0, |first| {
                    self.events
                        .iter()
                        .rev()
                        .find(|stored| same_partition(&stored.event, first))
                        .map_or(0, |stored| stored.event.envelope.operator_seq.0)
                });
                for event in &mut outbox {
                    if event.envelope.operator_seq.0 != 0 {
                        return Err(invalid("transaction sequences must be assigned by fabric"));
                    }
                    if self
                        .events
                        .iter()
                        .any(|stored| stored.event.envelope.event_id == event.envelope.event_id)
                    {
                        return Err(FabricError::Conflict);
                    }
                    previous = previous
                        .checked_add(1)
                        .ok_or_else(|| invalid("sequence overflow"))?;
                    event.envelope.operator_seq.0 = previous;
                }
                if let Some((lease, now_ms)) = delivery {
                    let index = self.validate_lease(&lease, now_ms)?;
                    let source = self
                        .stream(index)
                        .nth(usize::try_from(lease.offset.saturating_sub(1)).unwrap_or(usize::MAX))
                        .ok_or(FabricError::AckHole)?
                        .event;
                    if outbox.iter().any(|event| !same_partition(&source, event)) {
                        return Err(invalid("outbox belongs to another run"));
                    }
                    self.apply(FabricCommand::Commit {
                        lease, now_ms,
                        result: serde_json::json!({"checkpoint_key":checkpoint.key,"checkpoint_version":version}),
                        outbox,
                    })?;
                } else if !outbox.is_empty() {
                    self.publish(outbox, false)?;
                }
                let record = FabricCheckpoint {
                    version,
                    value: checkpoint.value,
                };
                if let Some(undo) = &mut self.undo {
                    undo.checkpoints
                        .entry(checkpoint.key.clone())
                        .or_insert_with(|| self.checkpoints.get(&checkpoint.key).cloned());
                }
                self.checkpoints
                    .insert(checkpoint.key, Arc::new(record.clone()));
                Ok(FabricReply::Checkpoint(Some(record)))
            }
            FabricCommand::ScanEvents {
                topic,
                event_type,
                after,
                limit: page_limit,
            } => {
                limit(page_limit)?;
                key(&topic)?;
                key(&event_type)?;
                let items: Vec<_> = self
                    .events
                    .iter()
                    .enumerate()
                    .skip(usize::try_from(after).unwrap_or(usize::MAX))
                    .filter(|(_, stored)| {
                        stored.visible
                            && stored.event.topic == topic
                            && stored.event.envelope.event_type == event_type
                    })
                    .take(page_limit)
                    .map(|(index, stored)| ReplayItem {
                        offset: index as u64 + 1,
                        event: stored.event.clone(),
                    })
                    .collect();
                for item in &items {
                    self.ensure_available(&item.event)?;
                }
                Ok(FabricReply::Replay(items))
            }
            FabricCommand::ReadEvent(event_id) => {
                let event = self
                    .events
                    .iter()
                    .find(|stored| stored.visible && stored.event.envelope.event_id == event_id)
                    .map(|stored| stored.event.clone());
                if let Some(event) = &event {
                    self.ensure_available(event)?;
                }
                Ok(FabricReply::Event(event))
            }
            FabricCommand::Publish(event) => self.publish(vec![event], true),
            FabricCommand::PublishBatch(events) => self.publish(events, true),
            FabricCommand::Subscribe(spec) => {
                for value in [&spec.consumer, &spec.subscription, &spec.topic]
                    .into_iter()
                    .chain(spec.event_type.iter())
                    .chain(spec.tenant.iter())
                {
                    key(value)?;
                }
                if let Ok(index) = self.subscription(&spec.consumer, &spec.subscription) {
                    if self.subscriptions[index].spec != spec {
                        return Err(FabricError::Conflict);
                    }
                } else {
                    self.subscriptions.push(StoredSubscription {
                        spec,
                        offset: 0,
                        epoch: 0,
                        attempt: 0,
                        due_at_ms: i64::MIN,
                        lease: None,
                    });
                }
                Ok(FabricReply::Subscribed)
            }
            FabricCommand::Replay {
                consumer,
                subscription,
                after,
                limit: page_limit,
            } => {
                limit(page_limit)?;
                let index = self.subscription(&consumer, &subscription)?;
                let items: Vec<_> = self
                    .stream(index)
                    .skip(usize::try_from(after).unwrap_or(usize::MAX))
                    .take(page_limit)
                    .collect();
                for item in &items {
                    self.ensure_available(&item.event)?;
                }
                Ok(FabricReply::Replay(items))
            }
            FabricCommand::Offset {
                consumer,
                subscription,
            } => {
                let index = self.subscription(&consumer, &subscription)?;
                Ok(FabricReply::Offset(self.subscriptions[index].offset))
            }
            FabricCommand::Claim {
                worker,
                now_ms,
                lease_ms,
                limit: batch_limit,
                max_attempts,
            } => {
                key(&worker)?;
                limit(batch_limit)?;
                if lease_ms <= 0 || max_attempts == 0 {
                    return Err(invalid("lease duration"));
                }
                let expires_at_ms = now_ms
                    .checked_add(lease_ms)
                    .ok_or_else(|| invalid("lease overflow"))?;
                let mut claims = Vec::new();
                for index in 0..self.subscriptions.len() {
                    if claims.len() == batch_limit {
                        break;
                    }
                    let current = &self.subscriptions[index];
                    if consumer.is_some_and(|consumer| consumer != current.spec.consumer) {
                        continue;
                    }
                    if current.due_at_ms > now_ms
                        || current.lease.as_ref().is_some_and(|lease| {
                            lease.offset > current.offset && lease.expires_at_ms > now_ms
                        })
                    {
                        continue;
                    }
                    let Some(item) = self
                        .stream(index)
                        .nth(usize::try_from(current.offset).unwrap_or(usize::MAX))
                    else {
                        continue;
                    };
                    self.ensure_available(&item.event)?;
                    if current.attempt >= max_attempts {
                        let expired = current.lease.clone().ok_or(FabricError::Fenced)?;
                        self.dead_letters.push(DeadLetter {
                            event_id: item.event.envelope.event_id,
                            lease: expired.clone(),
                            error: "delivery lease expired after maximum attempts".into(),
                            dead_lettered_at_ms: now_ms,
                        });
                        self.acknowledge(index, &expired);
                        self.subscriptions[index].lease = None;
                        continue;
                    }
                    self.remember_subscription(index);
                    let current = &mut self.subscriptions[index];
                    if current.lease.as_ref().is_some_and(|lease| {
                        lease.offset > current.offset && lease.expires_at_ms <= now_ms
                    }) {
                        self.lease_reclaims = self.lease_reclaims.saturating_add(1);
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
                        worker: worker.clone(),
                        epoch: current.epoch,
                        expires_at_ms,
                        delivery_attempt: current.attempt,
                    };
                    current.lease = Some(lease.clone());
                    let receipt_key = ReceiptKey::for_event(&lease.consumer, &item.event);
                    let committed = self
                        .receipts
                        .iter()
                        .find(|receipt| receipt.key == receipt_key)
                        .map(|receipt| receipt.as_ref().clone());
                    claims.push(ClaimedDelivery {
                        lease,
                        event: item.event,
                        committed,
                    });
                }
                Ok(FabricReply::Claimed(claims))
            }
            FabricCommand::Commit {
                lease,
                now_ms,
                result,
                outbox,
            } => {
                let index = self.validate_lease(&lease, now_ms)?;
                if serde_json::to_vec(&result).map_err(storage)?.len() > MAX_EVENT_BYTES {
                    return Err(invalid("result byte limit"));
                }
                validate_events(&outbox, true)?;
                let event = self
                    .stream(index)
                    .nth(usize::try_from(lease.offset.saturating_sub(1)).unwrap_or(usize::MAX))
                    .ok_or(FabricError::AckHole)?
                    .event;
                let receipt_key = ReceiptKey::for_event(&lease.consumer, &event);
                if let Some(receipt) = self
                    .receipts
                    .iter()
                    .find(|receipt| receipt.key == receipt_key)
                    .map(|receipt| receipt.as_ref().clone())
                {
                    if receipt.result != result || receipt.outbox != outbox {
                        return Err(FabricError::Conflict);
                    }
                    self.acknowledge(index, &lease);
                    return Ok(FabricReply::Committed(receipt));
                }
                if !outbox.is_empty() {
                    self.publish(outbox.clone(), false)?;
                }
                let receipt = Receipt {
                    key: receipt_key,
                    result,
                    outbox,
                    committed_at_ms: now_ms,
                };
                self.receipts.push(Arc::new(receipt.clone()));
                self.acknowledge(index, &lease);
                Ok(FabricReply::Committed(receipt))
            }
            FabricCommand::Retry {
                lease,
                now_ms,
                backoff_ms,
                max_attempts,
                error,
            } => {
                let index = self.validate_lease(&lease, now_ms)?;
                if self.subscriptions[index].offset >= lease.offset {
                    return Err(FabricError::Fenced);
                }
                if backoff_ms < 0 || max_attempts == 0 || error.len() > MAX_EVENT_BYTES {
                    return Err(invalid("retry policy/error"));
                }
                let due_at_ms = now_ms
                    .checked_add(backoff_ms)
                    .ok_or_else(|| invalid("retry overflow"))?;
                self.remember_subscription(index);
                let dead_lettered = lease.delivery_attempt >= max_attempts;
                if dead_lettered {
                    let item = self
                        .stream(index)
                        .nth(usize::try_from(lease.offset.saturating_sub(1)).unwrap_or(usize::MAX))
                        .ok_or(FabricError::AckHole)?;
                    self.dead_letters.push(DeadLetter {
                        event_id: item.event.envelope.event_id,
                        lease: lease.clone(),
                        error,
                        dead_lettered_at_ms: now_ms,
                    });
                    self.acknowledge(index, &lease);
                } else {
                    self.subscriptions[index].due_at_ms = due_at_ms;
                }
                self.subscriptions[index].lease = None;
                Ok(FabricReply::Retried { dead_lettered })
            }
            FabricCommand::Receipt(key) => Ok(FabricReply::Receipt(
                self.receipts
                    .iter()
                    .find(|receipt| receipt.key == key)
                    .map(|receipt| receipt.as_ref().clone()),
            )),
            FabricCommand::DispatchOutbox { limit: batch_limit } => {
                limit(batch_limit)?;
                let mut batches = Vec::new();
                for event in self.events.iter().filter(|event| !event.visible) {
                    if !batches.contains(&event.outbox_batch) {
                        if batches.len() == batch_limit {
                            break;
                        }
                        batches.push(event.outbox_batch);
                    }
                }
                let mut count = 0;
                for event in self
                    .events
                    .iter_mut()
                    .filter(|event| !event.visible && batches.contains(&event.outbox_batch))
                {
                    Arc::make_mut(event).visible = true;
                    count += 1;
                }
                Ok(FabricReply::Dispatched(count))
            }
            FabricCommand::DeadLetters {
                after,
                limit: page_limit,
            } => {
                limit(page_limit)?;
                Ok(FabricReply::DeadLetters(
                    self.dead_letters
                        .iter()
                        .skip(after)
                        .take(page_limit)
                        .cloned()
                        .collect(),
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn stored_event(visible: bool, kind: &str) -> Arc<StoredEvent> {
        let run = crate::contracts::RunId::new();
        let id = crate::contracts::events::EventId::new();
        let mut event: FabricEvent = serde_json::from_value(json!({"topic":"topic","payload_schema":true,"envelope":{
            "schema_version":1,"engine_version":"operator_v1","projection_version":1,"event_id":id,"event_type":"test.v1","tenant_id":"local","run_id":run,"session_id":null,"effect_id":"e","invocation_id":"i","attempt":1,"issued_revision":1,"lease_epoch":1,"correlation_id":"c","causation_id":null,"operator":{"name":"test","version":1},"operator_seq":0,"delivery_class":"durable","occurred_at_ms":1,"payload":{"text":"large payload"}
        }})).unwrap();
        event.topic = "topic".into();
        event.envelope.event_type = kind.into();
        Arc::new(StoredEvent {
            retired: false,
            event,
            visible,
            outbox_batch: None,
        })
    }

    #[test]
    fn atomic_checkpoint_does_not_replace_history_containers() {
        let mut state = FabricState::default();
        state.events = vec![stored_event(true, "output.batch"); 1024];
        state
            .logs
            .insert("history".into(), vec![Arc::new(json!("old")); 1024]);
        let events = state.events.as_ptr();
        let log = state.logs["history"].as_ptr();
        state
            .apply_atomic(FabricCommand::Transact {
                checkpoint: CheckpointWrite {
                    key: "new".into(),
                    expected: None,
                    value: json!("value"),
                },
                outbox: vec![],
                delivery: None,
            })
            .unwrap();
        assert_eq!(
            events,
            state.events.as_ptr(),
            "unrelated event container must not be cloned"
        );
        assert_eq!(
            log,
            state.logs["history"].as_ptr(),
            "unrelated log container must not be cloned"
        );
        assert!(state.undo.is_none());
    }

    #[test]
    fn atomic_claim_restores_earlier_leases_when_later_claim_overflows() {
        let mut state = FabricState::default();
        state.events.push(stored_event(true, "output.batch"));
        for subscription in ["first", "overflow"] {
            state
                .apply(FabricCommand::Subscribe(SubscriptionSpec {
                    consumer: "consumer".into(),
                    subscription: subscription.into(),
                    topic: "topic".into(),
                    event_type: None,
                    tenant: None,
                }))
                .unwrap();
        }
        state.subscriptions[1].epoch = u64::MAX;
        let before = serde_json::to_value(&state).unwrap();
        assert!(
            state
                .apply_atomic(FabricCommand::Claim {
                    worker: "worker".into(),
                    now_ms: 1,
                    lease_ms: 100,
                    limit: 2,
                    max_attempts: 3,
                })
                .is_err()
        );
        assert_eq!(serde_json::to_value(&state).unwrap(), before);
        assert!(state.undo.is_none());
    }

    #[test]
    fn snapshots_share_event_and_receipt_payloads_but_isolate_dispatch() {
        let mut state = FabricState::default();
        state.events.push(stored_event(false, "output.batch"));
        state.receipts.push(Arc::new(Receipt {
            key: ReceiptKey::for_event("consumer", &state.events[0].event),
            result: json!({"ok": true}),
            outbox: vec![state.events[0].event.clone()],
            committed_at_ms: 1,
        }));
        let snapshot = state.clone();
        assert!(Arc::ptr_eq(&state.events[0], &snapshot.events[0]));
        assert!(Arc::ptr_eq(&state.receipts[0], &snapshot.receipts[0]));
        state
            .apply(FabricCommand::DispatchOutbox { limit: 1 })
            .unwrap();
        assert!(state.events[0].visible);
        assert!(!snapshot.events[0].visible);
        let wire = serde_json::to_value(&state).unwrap();
        let restored: FabricState = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(serde_json::to_value(restored).unwrap(), wire);
    }

    #[test]
    fn paged_replay_keeps_filtered_offsets_and_outbox_visibility_barrier() {
        let mut state = FabricState::default();
        state.events = vec![
            stored_event(true, "other"),
            stored_event(true, "output.batch"),
            stored_event(false, "output.batch"),
            stored_event(true, "output.batch"),
        ];
        state
            .apply(FabricCommand::Subscribe(SubscriptionSpec {
                consumer: "consumer".into(),
                subscription: "subscription".into(),
                topic: "topic".into(),
                event_type: Some("output.batch".into()),
                tenant: Some("local".into()),
            }))
            .unwrap();
        let replay = |after, limit| FabricCommand::Replay {
            consumer: "consumer".into(),
            subscription: "subscription".into(),
            after,
            limit,
        };
        assert_eq!(state.stream(0).count(), 1);
        assert_eq!(
            state.apply(replay(1, 1)).unwrap(),
            FabricReply::Replay(vec![])
        );
        state
            .apply(FabricCommand::DispatchOutbox { limit: 1 })
            .unwrap();
        let FabricReply::Replay(page) = state.apply(replay(1, 1)).unwrap() else {
            panic!("reply")
        };
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].offset, 2);
        assert_eq!(page[0].event, state.events[2].event);
        assert_eq!(
            state.apply(replay(u64::MAX, 1)).unwrap(),
            FabricReply::Replay(vec![])
        );
    }

    #[test]
    fn checkpoint_snapshots_share_payloads_and_preserve_wire_format() {
        let mut state = FabricState::default();
        state.checkpoints.insert(
            "large".into(),
            Arc::new(FabricCheckpoint {
                version: 0,
                value: json!({"content": "x".repeat(1_000_000)}),
            }),
        );
        let snapshot = state.clone();
        assert!(Arc::ptr_eq(
            &state.checkpoints["large"],
            &snapshot.checkpoints["large"]
        ));
        let encoded = serde_json::to_value(&state).expect("serialize");
        assert_eq!(encoded["checkpoints"]["large"]["version"], 0);
        let restored: FabricState = serde_json::from_value(encoded.clone()).expect("deserialize");
        assert_eq!(serde_json::to_value(restored).expect("serialize"), encoded);
        state.checkpoints.insert(
            "large".into(),
            Arc::new(FabricCheckpoint {
                version: 1,
                value: json!("changed"),
            }),
        );
        assert_eq!(snapshot.checkpoints["large"].version, 0);
    }

    #[tokio::test]
    async fn failed_transaction_still_rolls_back_all_checkpoints() {
        let fabric = MemoryFabric::default();
        let write = |key: &str, expected| CheckpointWrite {
            key: key.into(),
            expected,
            value: json!("value"),
        };
        let result = fabric
            .execute(FabricCommand::TransactMany {
                checkpoints: vec![write("first", None), write("missing", Some(5))],
                outbox: vec![],
                delivery: None,
            })
            .await;
        assert!(result.is_err());
        assert!(matches!(
            fabric
                .execute(FabricCommand::ReadCheckpoint("first".into()))
                .await
                .expect("read"),
            FabricReply::Checkpoint(None)
        ));
    }
}
