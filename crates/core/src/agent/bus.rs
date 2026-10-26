//! Low-level API for custom single-agent operators and durable event subscriptions.
use super::operator_fabric::{FabricHandler, FabricRuntime, FabricRuntimeConfig, FabricTickReport};
use crate::operator_fabric::*;
use std::{collections::BTreeMap, sync::Arc};

pub struct OperatorBus {
    fabric: Arc<dyn OperatorFabric>,
    runtime: FabricRuntime,
    operators: BTreeMap<String, Arc<dyn FabricHandler>>,
    order: Vec<String>,
    subscriptions: std::sync::Mutex<Vec<SubscriptionSpec>>,
    transactional: BTreeMap<String, Arc<dyn TransactionalOperator>>,
}
impl OperatorBus {
    pub fn new(fabric: Arc<dyn OperatorFabric>, config: FabricRuntimeConfig) -> Self {
        Self {
            fabric,
            runtime: FabricRuntime { config },
            operators: BTreeMap::new(),
            transactional: BTreeMap::new(),
            order: Vec::new(),
            subscriptions: Default::default(),
        }
    }
    pub fn fabric(&self) -> Arc<dyn OperatorFabric> {
        self.fabric.clone()
    }
    /// A consumer identifies one operator. It may subscribe to multiple event topics.
    pub async fn subscribe_operator(
        &mut self,
        subscription: SubscriptionSpec,
        handler: Arc<dyn FabricHandler>,
    ) -> Result<(), FabricError> {
        if self.transactional.contains_key(&subscription.consumer) {
            return Err(FabricError::Conflict);
        }
        if let Some(existing) = self.operators.get(&subscription.consumer)
            && !Arc::ptr_eq(existing, &handler)
        {
            return Err(FabricError::Conflict);
        }
        self.subscribe_events(subscription.clone()).await?;
        if !self.operators.contains_key(&subscription.consumer) {
            self.order.push(subscription.consumer.clone());
        }
        self.operators.insert(subscription.consumer, handler);
        Ok(())
    }
    /// Register an operator that atomically commits its own checkpoint and delivery.
    /// The bus must not issue a second Commit for these operators.
    pub fn register_transactional(
        &mut self,
        consumer: String,
        operator: Arc<dyn TransactionalOperator>,
    ) -> Result<(), FabricError> {
        if self.operators.contains_key(&consumer) || self.transactional.contains_key(&consumer) {
            return Err(FabricError::Conflict);
        }
        self.order.push(consumer.clone());
        self.transactional.insert(consumer, operator);
        Ok(())
    }

    pub async fn tick_consumer(
        &self,
        consumer: &str,
        clock: &(dyn Fn() -> i64 + Sync),
    ) -> Result<FabricTickReport, FabricError> {
        self.runtime.validate(self.fabric.as_ref())?;
        if let Some(handler) = self.operators.get(consumer) {
            return self
                .runtime
                .tick_for_consumer(self.fabric.as_ref(), handler.as_ref(), clock, consumer)
                .await;
        }
        let handler = self
            .transactional
            .get(consumer)
            .ok_or_else(|| FabricError::Invalid(format!("unregistered consumer: {consumer}")))?;
        let config = &self.runtime.config;
        self.fabric
            .execute(FabricCommand::DispatchOutbox {
                limit: config.claim_limit,
            })
            .await?;
        let FabricReply::Claimed(deliveries) = self
            .fabric
            .execute(FabricCommand::ClaimForConsumer {
                consumer: consumer.into(),
                worker: config.worker.clone(),
                now_ms: clock(),
                lease_ms: config.lease_ms,
                limit: config.claim_limit,
                max_attempts: config.max_attempts,
            })
            .await?
        else {
            return Err(FabricError::Conflict);
        };
        let claimed = deliveries.len();
        if claimed > 0 {
            handler.execute(deliveries, clock).await?;
        }
        let FabricReply::Dispatched(dispatched) = self
            .fabric
            .execute(FabricCommand::DispatchOutbox {
                limit: config.claim_limit,
            })
            .await?
        else {
            return Err(FabricError::Conflict);
        };
        Ok(FabricTickReport {
            claimed,
            dispatched,
            ..Default::default()
        })
    }

    pub async fn subscribe_events(
        &self,
        subscription: SubscriptionSpec,
    ) -> Result<(), FabricError> {
        self.fabric
            .execute(FabricCommand::Subscribe(subscription.clone()))
            .await?;
        let mut subscriptions = self.subscriptions.lock().expect("subscriptions");
        if !subscriptions.contains(&subscription) {
            subscriptions.push(subscription);
        }
        Ok(())
    }
    /// Unacknowledged deliveries include deferred retries and active leases.
    pub async fn is_idle(&self) -> Result<bool, FabricError> {
        self.runtime.validate(self.fabric.as_ref())?;
        loop {
            let FabricReply::Dispatched(count) = self
                .fabric
                .execute(FabricCommand::DispatchOutbox {
                    limit: self.runtime.config.claim_limit,
                })
                .await?
            else {
                return Err(FabricError::Conflict);
            };
            if count < self.runtime.config.claim_limit {
                break;
            }
        }
        let subscriptions = self.subscriptions.lock().expect("subscriptions").clone();
        for sub in subscriptions {
            if !self.operators.contains_key(&sub.consumer)
                && !self.transactional.contains_key(&sub.consumer)
            {
                continue;
            }
            let FabricReply::Offset(after) = self
                .fabric
                .execute(FabricCommand::Offset {
                    consumer: sub.consumer.clone(),
                    subscription: sub.subscription.clone(),
                })
                .await?
            else {
                return Err(FabricError::Conflict);
            };
            if !self
                .events_after(&sub.consumer, &sub.subscription, after, 1)
                .await?
                .is_empty()
            {
                return Ok(false);
            }
        }
        Ok(true)
    }
    pub async fn send(&self, event: FabricEvent) -> Result<(), FabricError> {
        self.fabric
            .execute(FabricCommand::Publish(event))
            .await
            .map(|_| ())
    }
    /// Read durable events after a cursor without acknowledging an operator delivery.
    pub async fn events_after(
        &self,
        consumer: &str,
        subscription: &str,
        after: u64,
        limit: usize,
    ) -> Result<Vec<ReplayItem>, FabricError> {
        match self
            .fabric
            .execute(FabricCommand::Replay {
                consumer: consumer.into(),
                subscription: subscription.into(),
                after,
                limit,
            })
            .await?
        {
            FabricReply::Replay(events) => Ok(events),
            _ => Err(FabricError::Storage("unexpected replay reply".into())),
        }
    }
    /// Run only registered consumers. Unrelated subscriptions are never claimed.
    pub async fn tick(
        &self,
        clock: impl Fn() -> i64 + Sync,
    ) -> Result<Vec<(String, FabricTickReport)>, FabricError> {
        self.tick_ready(clock, 1).await
    }
    /// Drain a bounded number of ready deliveries per consumer before moving on.
    pub async fn tick_ready(
        &self,
        clock: impl Fn() -> i64 + Sync,
        rounds: usize,
    ) -> Result<Vec<(String, FabricTickReport)>, FabricError> {
        let mut reports = Vec::new();
        for consumer in &self.order {
            for _ in 0..rounds.clamp(1, 128) {
                let report = self.tick_consumer(consumer, &clock).await?;
                let idle = report.claimed == 0;
                reports.push((consumer.clone(), report));
                if idle {
                    break;
                }
            }
        }
        Ok(reports)
    }
}

/// Stateful operators own checkpoint/outbox/ack transactions and error recovery.
/// On failure the lease remains recoverable; no generic retry can undo a partial
/// external side effect. Stateless handlers should use `FabricHandler` instead.
pub trait TransactionalOperator: Send + Sync {
    fn execute<'a>(
        &'a self,
        deliveries: Vec<ClaimedDelivery>,
        clock: &'a (dyn Fn() -> i64 + Sync),
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), FabricError>> + Send + 'a>>;
}
