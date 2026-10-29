//! Durable transport commands. Callers supply trusted schemas, identities and clock values.
//! This port does not authorize producers or transact external side effects.

use std::{future::Future, pin::Pin};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{contracts::events::EventId, operator::OperatorEnvelope};

/// Default event/result/error limit; configured adapters expose `OperatorFabric::limits`.
pub const MAX_EVENT_BYTES: usize = 64 * 1024;
/// Default total serialized event bytes per batch.
pub const MAX_BATCH_BYTES: usize = 1024 * 1024;
pub const MAX_BATCH_ITEMS: usize = 128;
pub const MAX_KEY_BYTES: usize = 256;
pub const MAX_CHECKPOINT_BYTES: usize = 4 * 1024 * 1024;

/// Host-owned write limits. Existing persisted records remain readable when limits shrink.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FabricLimits {
    pub max_event_bytes: usize,
    pub max_batch_bytes: usize,
}
impl Default for FabricLimits {
    fn default() -> Self {
        Self {
            max_event_bytes: MAX_EVENT_BYTES,
            max_batch_bytes: MAX_BATCH_BYTES,
        }
    }
}
impl FabricLimits {
    pub fn validate(&self) -> Result<(), FabricError> {
        if self.max_event_bytes == 0
            || self.max_event_bytes > self.max_batch_bytes
            || self.max_batch_bytes > 64 * 1024 * 1024
        {
            return Err(FabricError::Invalid(
                "fabric limits require 0 < max_event_bytes <= max_batch_bytes <= 64 MiB".into(),
            ));
        }
        Ok(())
    }
}

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
    #[error("admission_backlog_full")]
    Capacity,
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
    BeginMaintenance {
        token: String,
        now_ms: i64,
    },
    EndMaintenance {
        token: String,
    },
    ExportAgentHistory {
        tenant: String,
        agent_id: String,
    },
    InspectRun {
        tenant: String,
        run_id: super::RunId,
    },
    ExportRun {
        tenant: String,
        run_id: super::RunId,
        maintenance_token: String,
    },
    /// Low-level trusted import; validate resource compatibility using RunManager first.
    ImportRun {
        bundle: super::run_archive::RunBundle,
        maintenance_token: String,
    },
    /// Atomic standard host records plus one Fabric transaction. Trusted host API.
    HostTransact {
        records: Vec<super::host::HostRecord>,
        completion: Option<super::host::RunCompletion>,
        transaction: Box<FabricCommand>,
    },
    /// Maintenance operation: retire terminal, quiescent run resources after retention.
    RetireRun {
        tenant: String,
        run_id: crate::contracts::RunId,
        now_ms: i64,
        retention_ms: i64,
        dry_run: bool,
    },
    /// Mark/sweep embedded FabricBlobStore artifacts. The host must supply external roots
    /// and quiesce all uploads/root changes until collection finishes.
    CollectArtifacts {
        now_ms: i64,
        grace_ms: i64,
        external_roots: Vec<crate::contracts::BlobId>,
        dry_run: bool,
    },
    /// Root inventory for external BlobStore maintenance under the host's maintenance barrier.
    ArtifactRoots,

    /// Append immutable values and commit checkpoints/outbox in one transaction.
    TransactLog {
        log: String,
        expected: u64,
        items: Vec<Value>,
        checkpoints: Vec<CheckpointWrite>,
        outbox: Vec<FabricEvent>,
        delivery: Option<(DeliveryLease, i64)>,
    },
    GuardedTransactLog {
        lease: DeliveryLease,
        now_ms: i64,
        log: String,
        expected: u64,
        items: Vec<Value>,
        checkpoints: Vec<CheckpointWrite>,
    },
    ReadLog {
        log: String,
        after: u64,
        limit: usize,
    },
    InspectRetention {
        now_ms: i64,
        grace_ms: i64,
        external_roots: Vec<crate::contracts::BlobId>,
    },
    PublishBounded {
        events: Vec<FabricEvent>,
        subscription: SubscriptionSpec,
        max_pending: usize,
    },
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
    /// Atomically write multiple records while retaining an active delivery.
    /// Unlike TransactMany with `delivery`, this does not acknowledge the lease.
    GuardedTransactMany {
        lease: DeliveryLease,
        now_ms: i64,
        checkpoints: Vec<CheckpointWrite>,
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
    Maintenance,
    RunInspection(super::run_archive::RunInspectionSnapshot),
    RunBundle(super::run_archive::RunBundle),
    AgentHistory(super::agent_history::AgentHistory),
    Collected(CollectionReport),
    ArtifactRoots(Vec<crate::contracts::BlobId>),
    /// None means no log exists (for compatibility with inline legacy history).
    Log(Option<LogPage>),
    Retention(RetentionInventory),
    Checkpoint(Option<FabricCheckpoint>),
    Event(Option<FabricEvent>),
    Published(Vec<EventId>),
    Subscribed,
    Replay(Vec<ReplayItem>),
    Offset(u64),
    Claimed(Vec<ClaimedDelivery>),
    Committed(Receipt),
    Retried {
        dead_lettered: bool,
    },
    Receipt(Option<Receipt>),
    Dispatched(usize),
    DeadLetters(Vec<DeadLetter>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionInventory {
    pub events: usize,
    pub checkpoints: usize,
    pub outbox_backlog: usize,
    pub oldest_outbox_age_ms: i64,
    pub dead_letters: usize,
    pub active_leases: usize,
    pub lease_reclaims: u64,
    pub protected_artifacts: usize,
    pub orphan_candidates: Vec<crate::contracts::BlobId>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectionReport {
    pub checkpoints: usize,
    pub logs: usize,
    pub subscriptions: usize,
    pub receipts: usize,
    pub dead_letters: usize,
    pub event_payloads: usize,
    pub artifacts: usize,
    pub artifact_bytes: u64,
    /// Legacy orphan uploads first observed by this pass (not deleted yet).
    #[serde(default)]
    pub quarantined_artifacts: usize,
    pub dry_run: bool,
}

pub trait OperatorFabric: Send + Sync {
    /// Limits enforced by this adapter; the graph preflight uses the same values.
    fn limits(&self) -> FabricLimits {
        FabricLimits::default()
    }
    /// In-process change hint. Durable reads remain authoritative; remote writers
    /// and adapters without notifications are discovered by the bounded fallback.
    fn changes(&self) -> Option<tokio::sync::watch::Receiver<u64>> {
        None
    }
    fn capabilities(&self) -> FabricCapabilities;
    fn execute(&self, command: FabricCommand) -> FabricFuture<'_>;
}

impl FabricCommand {
    /// Whether a successful command commits checkpoint or log state.
    pub fn writes_checkpoint(&self) -> bool {
        if matches!(
            self,
            Self::HostTransact { .. }
                | Self::BeginMaintenance { .. }
                | Self::EndMaintenance { .. }
                | Self::ImportRun { .. }
        ) {
            return true;
        }
        matches!(
            self,
            Self::Transact { .. }
                | Self::TransactMany { .. }
                | Self::TransactLog { .. }
                | Self::GuardedTransact { .. }
                | Self::GuardedTransactMany { .. }
                | Self::GuardedTransactLog { .. }
        )
    }
}

/// The high watermark and a bounded page after the requested offset.
#[derive(Debug, Clone, PartialEq)]
pub struct LogPage {
    pub through: u64,
    pub items: Vec<Value>,
}

/// Wake readers only after committed observable changes, never on reads/empty claims.
pub fn observable_change(reply: &FabricReply) -> bool {
    match reply {
        FabricReply::Published(events) => !events.is_empty(),
        FabricReply::Committed(_) | FabricReply::Retried { .. } => true,
        FabricReply::Dispatched(count) => *count > 0,
        FabricReply::Collected(report) => !report.dry_run,
        _ => false,
    }
}

/// Subscribe before reading, mark the current generation before each read pass,
/// then wait. A change racing the read is retained by watch, not lost like Notify.
pub struct FabricWatch(Option<tokio::sync::watch::Receiver<u64>>);
impl FabricWatch {
    pub fn new(fabric: &dyn OperatorFabric) -> Self {
        Self(fabric.changes())
    }
    pub fn observe(&mut self) {
        if let Some(rx) = &mut self.0 {
            rx.borrow_and_update();
        }
    }
    pub async fn wait(&mut self, fallback: std::time::Duration) {
        if let Some(rx) = &mut self.0 {
            if let Ok(Err(_)) = tokio::time::timeout(fallback, rx.changed()).await {
                self.0 = None;
                tokio::time::sleep(fallback).await;
            }
        } else {
            tokio::time::sleep(fallback).await;
        }
    }
}
