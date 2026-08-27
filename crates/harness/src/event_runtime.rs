use std::sync::Arc;

use agent_core::event_runtime::{
    ClaimDeliveries, ClaimTimers, CompleteDelivery, DeliveryRouter, EventComponentDescriptor,
    EventError, EventStore, PublishEvent, PublishResult, RetryDelivery, ScheduleOnce, Subscription,
    Timer,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventRuntimeConfig {
    pub claim_limit: usize,
    pub lease_ms: i64,
    pub max_delivery_attempts: u32,
    pub retry_base_ms: i64,
}

impl Default for EventRuntimeConfig {
    fn default() -> Self {
        Self {
            claim_limit: 64,
            lease_ms: 30_000,
            max_delivery_attempts: 5,
            retry_base_ms: 1_000,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RuntimeTickReport {
    pub claimed: u64,
    pub completed: u64,
    pub retried: u64,
    pub dead_lettered: u64,
}

/// Stateless coordinator over durable event stores.
pub struct EventRuntime {
    store: Arc<dyn EventStore>,
    router: Arc<dyn DeliveryRouter>,
    config: EventRuntimeConfig,
}

impl EventRuntime {
    #[must_use]
    pub fn new(
        store: Arc<dyn EventStore>,
        router: Arc<dyn DeliveryRouter>,
        config: EventRuntimeConfig,
    ) -> Self {
        Self {
            store,
            router,
            config,
        }
    }

    #[must_use]
    pub fn descriptor(&self) -> EventComponentDescriptor {
        self.store.descriptor()
    }

    pub async fn publish(&self, command: PublishEvent) -> Result<PublishResult, EventError> {
        self.store.publish(command).await
    }

    pub async fn subscribe(
        &self,
        command: agent_core::event_runtime::CreateSubscription,
    ) -> Result<Subscription, EventError> {
        self.store.subscribe(command).await
    }

    pub async fn schedule_once(&self, command: ScheduleOnce) -> Result<Timer, EventError> {
        self.store.schedule_once(command).await
    }

    pub async fn dispatch_once(
        &self,
        worker_id: &str,
        now_ms: i64,
    ) -> Result<RuntimeTickReport, EventError> {
        let deliveries = self
            .store
            .claim_deliveries(ClaimDeliveries {
                worker_id: worker_id.to_owned(),
                now_ms,
                lease_until_ms: now_ms.saturating_add(self.config.lease_ms.max(1)),
                limit: self.config.claim_limit,
            })
            .await?;
        let mut report = RuntimeTickReport {
            claimed: u64::try_from(deliveries.len()).unwrap_or(u64::MAX),
            ..RuntimeTickReport::default()
        };
        for delivery in deliveries {
            let Some(event) = self.store.get_event(delivery.delivery_id.event_id).await? else {
                return Err(EventError::backend(
                    "claimed delivery references a missing event",
                ));
            };
            match self.router.deliver(delivery.clone(), event).await {
                Ok(()) => {
                    self.store
                        .complete_delivery(CompleteDelivery {
                            delivery_id: delivery.delivery_id,
                            worker_id: worker_id.to_owned(),
                            delivered_at_ms: now_ms,
                        })
                        .await?;
                    report.completed = report.completed.saturating_add(1);
                }
                Err(error) => {
                    let dead_letter = delivery.attempts >= self.config.max_delivery_attempts.max(1);
                    let retry_delay = self
                        .config
                        .retry_base_ms
                        .max(1)
                        .saturating_mul(i64::from(delivery.attempts.max(1)));
                    self.store
                        .retry_delivery(RetryDelivery {
                            delivery_id: delivery.delivery_id,
                            worker_id: worker_id.to_owned(),
                            next_attempt_at_ms: now_ms.saturating_add(retry_delay),
                            error: error.to_string(),
                            dead_letter,
                        })
                        .await?;
                    if dead_letter {
                        report.dead_lettered = report.dead_lettered.saturating_add(1);
                    } else {
                        report.retried = report.retried.saturating_add(1);
                    }
                }
            }
        }
        Ok(report)
    }

    pub async fn fire_timers_once(
        &self,
        worker_id: &str,
        now_ms: i64,
    ) -> Result<RuntimeTickReport, EventError> {
        let timers = self
            .store
            .claim_due_timers(ClaimTimers {
                worker_id: worker_id.to_owned(),
                now_ms,
                lease_until_ms: now_ms.saturating_add(self.config.lease_ms.max(1)),
                limit: self.config.claim_limit,
            })
            .await?;
        let mut report = RuntimeTickReport {
            claimed: u64::try_from(timers.len()).unwrap_or(u64::MAX),
            ..RuntimeTickReport::default()
        };
        for timer in timers {
            let event_id = timer.definition.event.event_id;
            match self.store.publish(timer.definition.event.clone()).await {
                Ok(_) => {
                    self.store
                        .complete_timer(timer.definition.timer_id, worker_id.to_owned(), event_id)
                        .await?;
                    report.completed = report.completed.saturating_add(1);
                }
                Err(_) => {
                    report.retried = report.retried.saturating_add(1);
                }
            }
        }
        Ok(report)
    }
}
