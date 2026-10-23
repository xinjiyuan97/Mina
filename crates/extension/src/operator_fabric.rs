//! Reference transition engine shared by memory and the existing SQLite EventStore.

use std::sync::Mutex;

use agent_core::{
    operator::{DeliveryClass, EngineVersion, PayloadContract},
    operator_fabric::*,
};
use serde::{Deserialize, Serialize};

#[derive(Default)]
pub struct MemoryFabric {
    state: Mutex<FabricState>,
}

impl OperatorFabric for MemoryFabric {
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
        Box::pin(async move {
            let mut state = self.state.lock().map_err(storage)?;
            let mut candidate = state.clone();
            let reply = candidate.apply(command)?;
            *state = candidate;
            Ok(reply)
        })
    }
}

pub(crate) fn storage(error: impl std::fmt::Display) -> FabricError {
    FabricError::Storage(error.to_string())
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct FabricState {
    events: Vec<StoredEvent>,
    subscriptions: Vec<StoredSubscription>,
    receipts: Vec<Receipt>,
    dead_letters: Vec<DeadLetter>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredEvent {
    event: FabricEvent,
    visible: bool,
    outbox_batch: Option<agent_core::event_runtime::EventId>,
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
            && event.topic == agent_core::context_operator::CONTEXT_TOPIC
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
    fn subscription(&self, consumer: &str, subscription: &str) -> Result<usize, FabricError> {
        self.subscriptions
            .iter()
            .position(|item| {
                item.spec.consumer == consumer && item.spec.subscription == subscription
            })
            .ok_or(FabricError::NotFound)
    }

    fn stream(&self, index: usize) -> Vec<ReplayItem> {
        let spec = &self.subscriptions[index].spec;
        let mut offset = 0;
        let mut result = Vec::new();
        for stored in &self.events {
            if !stored.visible {
                break;
            }
            let event = &stored.event;
            if event.topic != spec.topic
                || spec
                    .event_type
                    .as_ref()
                    .is_some_and(|kind| kind != &event.envelope.event_type)
                || spec
                    .tenant
                    .as_ref()
                    .is_some_and(|tenant| tenant != &event.envelope.tenant_id)
            {
                continue;
            }
            offset += 1;
            result.push(ReplayItem {
                offset,
                event: event.clone(),
            });
        }
        result
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
                self.events.push(StoredEvent {
                    event,
                    visible,
                    outbox_batch,
                });
            }
            ids.push(event_id);
        }
        Ok(FabricReply::Published(ids))
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
        Ok(index)
    }

    fn acknowledge(&mut self, index: usize, lease: &DeliveryLease) {
        let subscription = &mut self.subscriptions[index];
        subscription.offset = lease.offset;
        subscription.attempt = 0;
        subscription.due_at_ms = i64::MIN;
    }

    pub(crate) fn apply(&mut self, command: FabricCommand) -> Result<FabricReply, FabricError> {
        match command {
            FabricCommand::ScanEvents {
                topic,
                event_type,
                after,
                limit: page_limit,
            } => {
                limit(page_limit)?;
                key(&topic)?;
                key(&event_type)?;
                Ok(FabricReply::Replay(
                    self.events
                        .iter()
                        .enumerate()
                        .filter(|(index, stored)| {
                            stored.visible
                                && *index as u64 + 1 > after
                                && stored.event.topic == topic
                                && stored.event.envelope.event_type == event_type
                        })
                        .take(page_limit)
                        .map(|(index, stored)| ReplayItem {
                            offset: index as u64 + 1,
                            event: stored.event.clone(),
                        })
                        .collect(),
                ))
            }
            FabricCommand::ReadEvent(event_id) => Ok(FabricReply::Event(
                self.events
                    .iter()
                    .find(|stored| stored.visible && stored.event.envelope.event_id == event_id)
                    .map(|stored| stored.event.clone()),
            )),
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
                Ok(FabricReply::Replay(
                    self.stream(index)
                        .into_iter()
                        .filter(|item| item.offset > after)
                        .take(page_limit)
                        .collect(),
                ))
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
                    if current.due_at_ms > now_ms
                        || current.lease.as_ref().is_some_and(|lease| {
                            lease.offset > current.offset && lease.expires_at_ms > now_ms
                        })
                    {
                        continue;
                    }
                    let Some(item) = self
                        .stream(index)
                        .into_iter()
                        .find(|item| item.offset == current.offset + 1)
                    else {
                        continue;
                    };
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
                    let current = &mut self.subscriptions[index];
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
                        .cloned();
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
                    .into_iter()
                    .find(|item| item.offset == lease.offset)
                    .ok_or(FabricError::AckHole)?
                    .event;
                let receipt_key = ReceiptKey::for_event(&lease.consumer, &event);
                if let Some(receipt) = self
                    .receipts
                    .iter()
                    .find(|receipt| receipt.key == receipt_key)
                    .cloned()
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
                self.receipts.push(receipt.clone());
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
                let dead_lettered = lease.delivery_attempt >= max_attempts;
                if dead_lettered {
                    let item = self
                        .stream(index)
                        .into_iter()
                        .find(|item| item.offset == lease.offset)
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
                    .cloned(),
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
                    event.visible = true;
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
