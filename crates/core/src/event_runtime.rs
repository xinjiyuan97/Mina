//! Provider-neutral durable event, subscription, delivery, and timer contracts.

use std::{future::Future, pin::Pin};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

use crate::harness::RunId;

macro_rules! uuid_id {
    ($name:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(Uuid);

        impl $name {
            #[must_use]
            pub fn new() -> Self {
                Self(Uuid::new_v4())
            }

            #[must_use]
            pub fn stable(namespace: &str, key: &str) -> Self {
                let name = format!("{}:{namespace}:{key}", stringify!($name));
                Self(Uuid::new_v5(&Uuid::NAMESPACE_URL, name.as_bytes()))
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(formatter)
            }
        }

        impl std::str::FromStr for $name {
            type Err = uuid::Error;

            fn from_str(source: &str) -> Result<Self, Self::Err> {
                Uuid::parse_str(source).map(Self)
            }
        }
    };
}

uuid_id!(EventId);
uuid_id!(SubscriptionId);
uuid_id!(TimerId);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventSource {
    System,
    Gateway,
    Agent,
    ToolWorker,
    JobWorker,
    Timer,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublishEvent {
    pub event_id: EventId,
    pub topic: String,
    pub event_type: String,
    pub schema_version: u32,
    pub source: EventSource,
    pub subject: Option<String>,
    pub correlation_id: Option<String>,
    pub causation_id: Option<EventId>,
    pub occurred_at_ms: i64,
    pub recorded_at_ms: i64,
    pub payload: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub sequence: u64,
    #[serde(flatten)]
    pub event: PublishEvent,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublishResult {
    pub event: EventEnvelope,
    pub delivery_ids: Vec<DeliveryId>,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeliveryId {
    pub subscription_id: SubscriptionId,
    pub event_id: EventId,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PayloadPredicate {
    Eq { path: String, value: Value },
    Exists { path: String },
    In { path: String, values: Vec<Value> },
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct EventFilter {
    #[serde(default)]
    pub topics: Vec<String>,
    #[serde(default)]
    pub event_types: Vec<String>,
    #[serde(default)]
    pub sources: Vec<EventSource>,
    pub subject: Option<String>,
    pub correlation_id: Option<String>,
    #[serde(default)]
    pub payload: Vec<PayloadPredicate>,
}

impl EventFilter {
    pub fn validate(&self) -> Result<(), EventError> {
        if self.topics.is_empty() && self.event_types.is_empty() {
            return Err(EventError::invalid(
                "event filter requires at least one topic or event type",
            ));
        }
        for pattern in self.topics.iter().chain(&self.event_types) {
            validate_pattern(pattern)?;
        }
        for predicate in &self.payload {
            let path = match predicate {
                PayloadPredicate::Eq { path, .. }
                | PayloadPredicate::Exists { path }
                | PayloadPredicate::In { path, .. } => path,
            };
            if !path.is_empty() && !path.starts_with('/') {
                return Err(EventError::invalid(
                    "payload predicate paths must be JSON pointers",
                ));
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn matches(&self, event: &EventEnvelope) -> bool {
        matches_patterns(&self.topics, &event.event.topic)
            && matches_patterns(&self.event_types, &event.event.event_type)
            && (self.sources.is_empty() || self.sources.contains(&event.event.source))
            && self
                .subject
                .as_ref()
                .is_none_or(|subject| event.event.subject.as_ref() == Some(subject))
            && self.correlation_id.as_ref().is_none_or(|correlation_id| {
                event.event.correlation_id.as_ref() == Some(correlation_id)
            })
            && self.payload.iter().all(|predicate| match predicate {
                PayloadPredicate::Eq { path, value } => {
                    event.event.payload.pointer(path) == Some(value)
                }
                PayloadPredicate::Exists { path } => event.event.payload.pointer(path).is_some(),
                PayloadPredicate::In { path, values } => event
                    .event
                    .payload
                    .pointer(path)
                    .is_some_and(|value| values.contains(value)),
            })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubscriptionMode {
    Once,
    Continuous,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubscriptionStatus {
    Active,
    Delivering,
    Completed,
    Paused,
    Expired,
    Cancelled,
    DeadLettered,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SubscriptionScope {
    Run { run_id: RunId },
    Global,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SubscriptionOwner {
    Run { run_id: RunId },
    System { component: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DeliveryTarget {
    WakeRun { run_id: RunId, wait_key: String },
    RustHandler { handler: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StartPosition {
    Now,
    Beginning,
    After { sequence: u64 },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateSubscription {
    pub subscription_id: SubscriptionId,
    pub owner: SubscriptionOwner,
    pub scope: SubscriptionScope,
    pub filter: EventFilter,
    pub delivery: DeliveryTarget,
    pub mode: SubscriptionMode,
    pub start_position: StartPosition,
    pub expires_at_ms: Option<i64>,
    pub max_deliveries: Option<u32>,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Subscription {
    #[serde(flatten)]
    pub definition: CreateSubscription,
    pub status: SubscriptionStatus,
    pub cursor: u64,
    pub delivery_count: u32,
}

impl CreateSubscription {
    pub fn validate(&self) -> Result<(), EventError> {
        self.filter.validate()?;
        if self.max_deliveries == Some(0) {
            return Err(EventError::invalid(
                "subscription max_deliveries must be greater than zero",
            ));
        }
        match &self.delivery {
            DeliveryTarget::WakeRun { run_id, wait_key } => {
                if wait_key.trim().is_empty() || wait_key.len() > 256 {
                    return Err(EventError::invalid(
                        "wake_run wait_key must contain 1 to 256 bytes",
                    ));
                }
                if let SubscriptionScope::Run { run_id: scoped_run } = self.scope
                    && scoped_run != *run_id
                {
                    return Err(EventError::invalid(
                        "wake_run target must match the subscription run scope",
                    ));
                }
            }
            DeliveryTarget::RustHandler { handler } => {
                if handler.trim().is_empty() || handler.len() > 128 {
                    return Err(EventError::invalid(
                        "Rust handler name must contain 1 to 128 bytes",
                    ));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryStatus {
    Pending,
    Delivering,
    Delivered,
    RetryPending,
    DeadLettered,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Delivery {
    pub delivery_id: DeliveryId,
    pub target: DeliveryTarget,
    pub event_sequence: u64,
    pub status: DeliveryStatus,
    pub attempts: u32,
    pub next_attempt_at_ms: i64,
    pub lease_owner: Option<String>,
    pub lease_until_ms: Option<i64>,
    pub last_error: Option<String>,
    pub delivered_at_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimDeliveries {
    pub worker_id: String,
    pub now_ms: i64,
    pub lease_until_ms: i64,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompleteDelivery {
    pub delivery_id: DeliveryId,
    pub worker_id: String,
    pub delivered_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryDelivery {
    pub delivery_id: DeliveryId,
    pub worker_id: String,
    pub next_attempt_at_ms: i64,
    pub error: String,
    pub dead_letter: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimerStatus {
    Pending,
    Firing,
    Fired,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScheduleOnce {
    pub timer_id: TimerId,
    pub fire_at_ms: i64,
    pub event: PublishEvent,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Timer {
    #[serde(flatten)]
    pub definition: ScheduleOnce,
    pub status: TimerStatus,
    pub lease_owner: Option<String>,
    pub lease_until_ms: Option<i64>,
    pub fired_event_id: Option<EventId>,
}

impl ScheduleOnce {
    pub fn validate(&self) -> Result<(), EventError> {
        validate_publish(&self.event)?;
        if self.event.source != EventSource::Timer {
            return Err(EventError::invalid(
                "timer events must use the trusted timer source",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimTimers {
    pub worker_id: String,
    pub now_ms: i64,
    pub lease_until_ms: i64,
    pub limit: usize,
}

pub fn validate_publish(command: &PublishEvent) -> Result<(), EventError> {
    validate_name(&command.topic, "event topic")?;
    validate_name(&command.event_type, "event type")?;
    if command.schema_version == 0 {
        return Err(EventError::invalid(
            "event schema_version must be greater than zero",
        ));
    }
    let payload_size = serde_json::to_vec(&command.payload)
        .map_err(|_| EventError::invalid("event payload could not be encoded"))?
        .len();
    if payload_size > 1_048_576 {
        return Err(EventError::invalid(
            "event payload exceeds the 1 MiB portable limit",
        ));
    }
    Ok(())
}

fn validate_name(value: &str, label: &str) -> Result<(), EventError> {
    if value.is_empty()
        || value.len() > 128
        || !value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"._-".contains(&byte)
        })
    {
        return Err(EventError::invalid(format!(
            "{label} must use 1 to 128 lowercase ASCII name characters"
        )));
    }
    Ok(())
}

fn validate_pattern(pattern: &str) -> Result<(), EventError> {
    let value = pattern.strip_suffix('*').unwrap_or(pattern);
    if value.contains('*') || value.is_empty() {
        return Err(EventError::invalid(
            "event patterns support only one trailing wildcard",
        ));
    }
    validate_name(value.trim_end_matches('.'), "event pattern")
}

fn matches_patterns(patterns: &[String], value: &str) -> bool {
    patterns.is_empty()
        || patterns.iter().any(|pattern| {
            pattern
                .strip_suffix('*')
                .map_or(pattern == value, |prefix| value.starts_with(prefix))
        })
}

pub type EventFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, EventError>> + Send + 'a>>;

pub trait EventStore: Send + Sync + 'static {
    fn descriptor(&self) -> EventComponentDescriptor;
    fn publish(&self, command: PublishEvent) -> EventFuture<'_, PublishResult>;
    fn get_event(&self, event_id: EventId) -> EventFuture<'_, Option<EventEnvelope>>;
    fn subscribe(&self, command: CreateSubscription) -> EventFuture<'_, Subscription>;
    fn get_subscription(
        &self,
        subscription_id: SubscriptionId,
    ) -> EventFuture<'_, Option<Subscription>>;
    fn cancel_subscription(
        &self,
        subscription_id: SubscriptionId,
        cancelled_at_ms: i64,
    ) -> EventFuture<'_, Subscription>;
    fn claim_deliveries(&self, command: ClaimDeliveries) -> EventFuture<'_, Vec<Delivery>>;
    fn complete_delivery(&self, command: CompleteDelivery) -> EventFuture<'_, Delivery>;
    fn retry_delivery(&self, command: RetryDelivery) -> EventFuture<'_, Delivery>;
    fn schedule_once(&self, command: ScheduleOnce) -> EventFuture<'_, Timer>;
    fn claim_due_timers(&self, command: ClaimTimers) -> EventFuture<'_, Vec<Timer>>;
    fn complete_timer(
        &self,
        timer_id: TimerId,
        worker_id: String,
        event_id: EventId,
    ) -> EventFuture<'_, Timer>;
    fn cancel_timer(&self, timer_id: TimerId, cancelled_at_ms: i64) -> EventFuture<'_, Timer>;
}

pub trait DeliveryRouter: Send + Sync + 'static {
    fn deliver(&self, delivery: Delivery, event: EventEnvelope) -> EventFuture<'_, ()>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventComponentDescriptor {
    pub identity: String,
    pub kind: String,
    pub version: String,
}

#[derive(Debug, Error)]
pub enum EventError {
    #[error("event contract validation failed: {0}")]
    Invalid(String),
    #[error("event resource was not found")]
    NotFound,
    #[error("event command conflicts with existing state: {0}")]
    Conflict(String),
    #[error("event runtime adapter failed: {0}")]
    Backend(String),
}

impl EventError {
    #[must_use]
    pub fn invalid(message: impl Into<String>) -> Self {
        Self::Invalid(message.into())
    }

    #[must_use]
    pub fn backend(message: impl Into<String>) -> Self {
        Self::Backend(message.into())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn event(topic: &str) -> EventEnvelope {
        EventEnvelope {
            sequence: 1,
            event: PublishEvent {
                event_id: EventId::new(),
                topic: topic.into(),
                event_type: topic.into(),
                schema_version: 1,
                source: EventSource::Gateway,
                subject: Some("run/one".into()),
                correlation_id: Some("approval-one".into()),
                causation_id: None,
                occurred_at_ms: 1,
                recorded_at_ms: 2,
                payload: json!({"decision": "allow", "nested": {"present": true}}),
            },
        }
    }

    #[test]
    fn declarative_filter_matches_topic_and_payload() {
        let filter = EventFilter {
            topics: vec!["approval.*".into()],
            correlation_id: Some("approval-one".into()),
            payload: vec![
                PayloadPredicate::Eq {
                    path: "/decision".into(),
                    value: json!("allow"),
                },
                PayloadPredicate::Exists {
                    path: "/nested/present".into(),
                },
            ],
            ..EventFilter::default()
        };
        filter.validate().expect("filter should be valid");
        assert!(filter.matches(&event("approval.resolved")));
        assert!(!filter.matches(&event("job.completed")));
    }

    #[test]
    fn publication_rejects_wildcard_topics() {
        let mut published = event("approval.resolved").event;
        published.topic = "approval.*".into();
        assert!(matches!(
            validate_publish(&published),
            Err(EventError::Invalid(_))
        ));
    }
}
