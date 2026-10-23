//! Transport-only coordination; handlers return data, never Agent decisions here.

use agent_core::operator_fabric::*;
use serde_json::Value;
use std::{future::Future, pin::Pin};

pub struct HandlerOutput {
    pub result: Value,
    pub outbox: Vec<FabricEvent>,
}

pub type HandlerFuture<'scope> =
    Pin<Box<dyn Future<Output = Result<HandlerOutput, String>> + Send + 'scope>>;

pub trait FabricHandler: Send + Sync {
    fn handle(&self, delivery: ClaimedDelivery) -> HandlerFuture<'_>;
}

pub struct FabricRuntimeConfig {
    pub worker: String,
    pub claim_limit: usize,
    pub lease_ms: i64,
    pub max_attempts: u32,
    pub backoff_ms: i64,
    pub max_backoff_ms: i64,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct FabricTickReport {
    pub committed: usize,
    pub reused: usize,
    pub retried: usize,
    pub dead_lettered: usize,
    pub fenced: usize,
    pub dispatched: usize,
}

pub struct FabricRuntime {
    pub config: FabricRuntimeConfig,
}

impl FabricRuntime {
    pub async fn tick(
        &self,
        store: &dyn OperatorFabric,
        handler: &dyn FabricHandler,
        clock: impl Fn() -> i64,
    ) -> Result<FabricTickReport, FabricError> {
        self.tick_scoped(store, handler, clock, None).await
    }

    pub async fn tick_for_consumer(
        &self,
        store: &dyn OperatorFabric,
        handler: &dyn FabricHandler,
        clock: impl Fn() -> i64,
        consumer: &str,
    ) -> Result<FabricTickReport, FabricError> {
        self.tick_scoped(store, handler, clock, Some(consumer))
            .await
    }

    async fn tick_scoped(
        &self,
        store: &dyn OperatorFabric,
        handler: &dyn FabricHandler,
        clock: impl Fn() -> i64,
        consumer: Option<&str>,
    ) -> Result<FabricTickReport, FabricError> {
        let config = &self.config;
        let capabilities = store.capabilities();
        if !capabilities.atomic_batch
            || !capabilities.continuous_offset
            || !capabilities.lease_fencing
            || !capabilities.transactional_outbox
            || config.worker.trim().is_empty()
            || config.worker.len() > MAX_KEY_BYTES
            || config.claim_limit == 0
            || config.claim_limit > MAX_BATCH_ITEMS
            || config.lease_ms <= 0
            || config.max_attempts == 0
            || config.backoff_ms <= 0
            || config.max_backoff_ms < config.backoff_ms
        {
            return Err(FabricError::Invalid(
                "unsupported capabilities or runtime configuration".into(),
            ));
        }
        let mut report = FabricTickReport {
            dispatched: self.dispatch(store).await?,
            ..FabricTickReport::default()
        };
        let command = match consumer {
            Some(consumer) => FabricCommand::ClaimForConsumer {
                consumer: consumer.into(),
                worker: config.worker.clone(),
                now_ms: clock(),
                lease_ms: config.lease_ms,
                limit: config.claim_limit,
                max_attempts: config.max_attempts,
            },
            None => FabricCommand::Claim {
                worker: config.worker.clone(),
                now_ms: clock(),
                lease_ms: config.lease_ms,
                limit: config.claim_limit,
                max_attempts: config.max_attempts,
            },
        };
        let FabricReply::Claimed(deliveries) = store.execute(command).await? else {
            return Err(protocol_error());
        };
        for delivery in deliveries {
            let lease = delivery.lease.clone();
            if clock() >= lease.expires_at_ms {
                report.fenced += 1;
                continue;
            }
            let FabricReply::Receipt(receipt) = store
                .execute(FabricCommand::Receipt(ReceiptKey::for_event(
                    &lease.consumer,
                    &delivery.event,
                )))
                .await?
            else {
                return Err(protocol_error());
            };
            let output = if let Some(receipt) = receipt {
                report.reused += 1;
                Ok(HandlerOutput {
                    result: receipt.result,
                    outbox: receipt.outbox,
                })
            } else {
                handler.handle(delivery).await
            };
            let failure = match output {
                Ok(output) => match store
                    .execute(FabricCommand::Commit {
                        lease: lease.clone(),
                        now_ms: clock(),
                        result: output.result,
                        outbox: output.outbox,
                    })
                    .await
                {
                    Ok(FabricReply::Committed(_)) => {
                        report.committed += 1;
                        None
                    }
                    Ok(_) => return Err(protocol_error()),
                    Err(FabricError::Fenced) => {
                        report.fenced += 1;
                        None
                    }
                    Err(error @ (FabricError::Invalid(_) | FabricError::Conflict)) => {
                        Some(error.to_string())
                    }
                    Err(error) => return Err(error),
                },
                Err(error) => Some(error),
            };
            if let Some(error) = failure {
                let multiplier = 1_i64
                    .checked_shl(lease.delivery_attempt.saturating_sub(1).min(62))
                    .unwrap_or(i64::MAX);
                let backoff_ms = config
                    .backoff_ms
                    .saturating_mul(multiplier)
                    .min(config.max_backoff_ms);
                match store
                    .execute(FabricCommand::Retry {
                        lease,
                        now_ms: clock(),
                        backoff_ms,
                        max_attempts: config.max_attempts,
                        error: error.chars().take(MAX_EVENT_BYTES / 4).collect(),
                    })
                    .await
                {
                    Ok(FabricReply::Retried {
                        dead_lettered: true,
                    }) => report.dead_lettered += 1,
                    Ok(FabricReply::Retried {
                        dead_lettered: false,
                    }) => report.retried += 1,
                    Err(FabricError::Fenced) => report.fenced += 1,
                    Err(error) => return Err(error),
                    Ok(_) => return Err(protocol_error()),
                }
            }
        }
        report.dispatched += self.dispatch(store).await?;
        Ok(report)
    }

    async fn dispatch(&self, store: &dyn OperatorFabric) -> Result<usize, FabricError> {
        match store
            .execute(FabricCommand::DispatchOutbox {
                limit: self.config.claim_limit,
            })
            .await?
        {
            FabricReply::Dispatched(count) => Ok(count),
            _ => Err(protocol_error()),
        }
    }
}

fn protocol_error() -> FabricError {
    FabricError::Storage("adapter returned an unexpected reply variant".into())
}
