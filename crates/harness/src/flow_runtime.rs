use std::sync::Arc;

use agent_core::harness::{
    CompleteFlowEffect, FlowEffectRouter, FlowError, FlowStore, RetryFlowEffect,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlowEffectRuntimeConfig {
    pub claim_limit: usize,
    pub lease_ms: i64,
    pub max_attempts: u32,
    pub retry_base_ms: i64,
}

impl Default for FlowEffectRuntimeConfig {
    fn default() -> Self {
        Self {
            claim_limit: 64,
            lease_ms: 30_000,
            max_attempts: 5,
            retry_base_ms: 1_000,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FlowEffectTickReport {
    pub claimed: u64,
    pub completed: u64,
    pub retried: u64,
    pub dead_lettered: u64,
}

pub struct FlowEffectRuntime {
    store: Arc<dyn FlowStore>,
    router: Arc<dyn FlowEffectRouter>,
    config: FlowEffectRuntimeConfig,
}

impl FlowEffectRuntime {
    #[must_use]
    pub fn new(
        store: Arc<dyn FlowStore>,
        router: Arc<dyn FlowEffectRouter>,
        config: FlowEffectRuntimeConfig,
    ) -> Self {
        Self {
            store,
            router,
            config,
        }
    }

    pub async fn dispatch_once(
        &self,
        worker_id: &str,
        now_ms: i64,
    ) -> Result<FlowEffectTickReport, FlowError> {
        let effects = self
            .store
            .claim_effects(
                worker_id.to_owned(),
                now_ms,
                now_ms.saturating_add(self.config.lease_ms.max(1)),
                self.config.claim_limit,
            )
            .await?;
        let mut report = FlowEffectTickReport {
            claimed: u64::try_from(effects.len()).unwrap_or(u64::MAX),
            ..FlowEffectTickReport::default()
        };
        for effect in effects {
            let effect_id = effect.effect_id;
            let attempts = effect.attempts;
            match self.router.execute(effect).await {
                Ok(()) => {
                    self.store
                        .complete_effect(CompleteFlowEffect {
                            effect_id,
                            worker_id: worker_id.to_owned(),
                            completed_at_ms: now_ms,
                        })
                        .await?;
                    report.completed = report.completed.saturating_add(1);
                }
                Err(error) => {
                    let dead_letter = attempts >= self.config.max_attempts.max(1);
                    let retry_delay = self
                        .config
                        .retry_base_ms
                        .max(1)
                        .saturating_mul(i64::from(attempts.max(1)));
                    self.store
                        .retry_effect(RetryFlowEffect {
                            effect_id,
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
}
