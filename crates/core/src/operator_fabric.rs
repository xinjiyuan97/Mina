//! Durable transport commands. Callers supply trusted schemas, identities and clock values.
//! This port does not authorize producers or transact external side effects.

use std::{future::Future, pin::Pin};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{event_runtime::EventId, operator::OperatorEnvelope};

pub const MAX_EVENT_BYTES: usize = 64 * 1024;
pub const MAX_BATCH_BYTES: usize = 1024 * 1024;
pub const MAX_BATCH_ITEMS: usize = 128;
pub const MAX_KEY_BYTES: usize = 256;
pub const MAX_CHECKPOINT_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FabricCheckpoint {
    pub version: u64,
    pub value: Value,
}

#[derive(Debug, Clone)]
pub struct CheckpointWrite {
    pub key: String,
    pub expected: Option<u64>,
    pub value: Value,
}

pub type FabricFuture<'scope> =
    Pin<Box<dyn Future<Output = Result<FabricReply, FabricError>> + Send + 'scope>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FabricCapabilities {
    pub atomic_batch: bool,
    pub continuous_offset: bool,
    pub lease_fencing: bool,
    pub transactional_outbox: bool,
    pub durable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FabricError {
    #[error("invalid or oversized command: {0}")]
    Invalid(String),
    #[error("identity/content conflict")]
    Conflict,
    #[error("unknown subscription")]
    NotFound,
    #[error("expired or stale delivery lease")]
    Fenced,
    #[error("non-contiguous acknowledgement")]
    AckHole,
    #[error("storage failure: {0}")]
    Storage(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FabricEvent {
    pub topic: String,
    pub envelope: OperatorEnvelope,
    pub payload_schema: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionSpec {
    pub consumer: String,
    pub subscription: String,
    pub topic: String,
    pub event_type: Option<String>,
    pub tenant: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplayItem {
    pub offset: u64,
    pub event: FabricEvent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryLease {
    pub consumer: String,
    pub subscription: String,
    pub offset: u64,
    pub worker: String,
    pub epoch: u64,
    pub expires_at_ms: i64,
    pub delivery_attempt: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClaimedDelivery {
    pub lease: DeliveryLease,
    pub event: FabricEvent,
    pub committed: Option<Receipt>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceiptKey {
    pub consumer: String,
    pub event_id: EventId,
    pub effect_id: String,
    pub invocation_id: String,
    pub attempt: u32,
}

impl ReceiptKey {
    pub fn for_event(consumer: &str, event: &FabricEvent) -> Self {
        Self {
            consumer: consumer.into(),
            event_id: event.envelope.event_id,
            effect_id: event.envelope.effect_id.clone(),
            invocation_id: event.envelope.invocation_id.clone(),
            attempt: event.envelope.attempt,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Receipt {
    pub key: ReceiptKey,
    pub result: Value,
    pub outbox: Vec<FabricEvent>,
    pub committed_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeadLetter {
    pub event_id: EventId,
    pub lease: DeliveryLease,
    pub error: String,
    pub dead_lettered_at_ms: i64,
}

#[derive(Debug, Clone)]
pub enum FabricCommand {
    TransactMany {
        checkpoints: Vec<CheckpointWrite>,
        outbox: Vec<FabricEvent>,
        delivery: Option<(DeliveryLease, i64)>,
    },
    GuardedTransact {
        lease: DeliveryLease,
        now_ms: i64,
        checkpoint: CheckpointWrite,
        outbox: Vec<FabricEvent>,
    },
    ReadCheckpoint(String),
    Transact {
        checkpoint: CheckpointWrite,
        outbox: Vec<FabricEvent>,
        delivery: Option<(DeliveryLease, i64)>,
    },
    ReadEvent(EventId),
    ScanEvents {
        topic: String,
        event_type: String,
        after: u64,
        limit: usize,
    },
    Publish(FabricEvent),
    PublishBatch(Vec<FabricEvent>),
    Subscribe(SubscriptionSpec),
    Replay {
        consumer: String,
        subscription: String,
        after: u64,
        limit: usize,
    },
    Offset {
        consumer: String,
        subscription: String,
    },
    Claim {
        worker: String,
        now_ms: i64,
        lease_ms: i64,
        limit: usize,
        max_attempts: u32,
    },
    ClaimForConsumer {
        consumer: String,
        worker: String,
        now_ms: i64,
        lease_ms: i64,
        limit: usize,
        max_attempts: u32,
    },
    Commit {
        lease: DeliveryLease,
        now_ms: i64,
        result: Value,
        outbox: Vec<FabricEvent>,
    },
    Retry {
        lease: DeliveryLease,
        now_ms: i64,
        backoff_ms: i64,
        max_attempts: u32,
        error: String,
    },
    Receipt(ReceiptKey),
    DispatchOutbox {
        limit: usize,
    },
    DeadLetters {
        after: usize,
        limit: usize,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum FabricReply {
    Checkpoint(Option<FabricCheckpoint>),
    Event(Option<FabricEvent>),
    Published(Vec<EventId>),
    Subscribed,
    Replay(Vec<ReplayItem>),
    Offset(u64),
    Claimed(Vec<ClaimedDelivery>),
    Committed(Receipt),
    Retried { dead_lettered: bool },
    Receipt(Option<Receipt>),
    Dispatched(usize),
    DeadLetters(Vec<DeadLetter>),
}

pub trait OperatorFabric: Send + Sync {
    fn capabilities(&self) -> FabricCapabilities;
    fn execute(&self, command: FabricCommand) -> FabricFuture<'_>;
}
