use std::sync::Arc;

use agent_core::{
    event_runtime::{EventId, EventSource, PublishEvent},
    harness::{
        ClaimJobNotifications, ClaimJobs, CompleteJob, CompleteJobNotification, FlowError,
        JobOutcome, JobRecord, JobRouter, JobStore, JobStoreError, RetryJob, RetryJobNotification,
        StartJob, SubmitJobResult,
    },
};

use crate::EventRuntime;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JobRuntimeConfig {
    pub claim_limit: usize,
    pub lease_ms: i64,
    pub max_attempts: u32,
    pub retry_base_ms: i64,
    pub max_notification_attempts: u32,
}

impl Default for JobRuntimeConfig {
    fn default() -> Self {
        Self {
            claim_limit: 32,
            lease_ms: 60_000,
            max_attempts: 3,
            retry_base_ms: 1_000,
            max_notification_attempts: 10,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct JobTickReport {
    pub claimed: u64,
    pub completed: u64,
    pub retried: u64,
    pub failed: u64,
    pub published: u64,
    pub dead_lettered: u64,
}

pub struct JobRuntime {
    store: Arc<dyn JobStore>,
    router: Arc<dyn JobRouter>,
    events: Arc<EventRuntime>,
    config: JobRuntimeConfig,
}

impl JobRuntime {
    #[must_use]
    pub fn new(
        store: Arc<dyn JobStore>,
        router: Arc<dyn JobRouter>,
        events: Arc<EventRuntime>,
        config: JobRuntimeConfig,
    ) -> Self {
        Self {
            store,
            router,
            events,
            config,
        }
    }

    pub async fn submit(&self, command: StartJob) -> Result<SubmitJobResult, JobStoreError> {
        command.validate().map_err(job_from_flow)?;
        self.store.submit(command).await
    }

    pub async fn execute_once(
        &self,
        worker_id: &str,
        now_ms: i64,
    ) -> Result<JobTickReport, JobStoreError> {
        let jobs = self
            .store
            .claim(ClaimJobs {
                worker_id: worker_id.to_owned(),
                now_ms,
                lease_until_ms: now_ms.saturating_add(self.config.lease_ms.max(1)),
                limit: self.config.claim_limit,
            })
            .await?;
        let mut report = JobTickReport {
            claimed: u64::try_from(jobs.len()).unwrap_or(u64::MAX),
            ..JobTickReport::default()
        };
        for job in jobs {
            let job_id = job.command.job_id;
            let attempts = job.attempts;
            match self.router.execute(job).await {
                Ok(value) => {
                    self.store
                        .complete(CompleteJob {
                            job_id,
                            worker_id: worker_id.to_owned(),
                            outcome: JobOutcome::Succeeded { value },
                            completed_at_ms: now_ms,
                        })
                        .await?;
                    report.completed = report.completed.saturating_add(1);
                }
                Err(error) if error.retryable && attempts < self.config.max_attempts.max(1) => {
                    let configured = u64::try_from(self.config.retry_base_ms.max(1))
                        .unwrap_or(u64::MAX)
                        .saturating_mul(u64::from(attempts.max(1)));
                    let delay = error.retry_after_ms.unwrap_or(configured);
                    self.store
                        .retry(RetryJob {
                            job_id,
                            worker_id: worker_id.to_owned(),
                            next_attempt_at_ms: now_ms
                                .saturating_add(i64::try_from(delay).unwrap_or(i64::MAX)),
                            error: error.message,
                        })
                        .await?;
                    report.retried = report.retried.saturating_add(1);
                }
                Err(error) => {
                    self.store
                        .complete(CompleteJob {
                            job_id,
                            worker_id: worker_id.to_owned(),
                            outcome: JobOutcome::Failed {
                                code: error.code,
                                message: error.message,
                                retryable: error.retryable,
                            },
                            completed_at_ms: now_ms,
                        })
                        .await?;
                    report.failed = report.failed.saturating_add(1);
                }
            }
        }
        Ok(report)
    }

    pub async fn publish_notifications_once(
        &self,
        worker_id: &str,
        now_ms: i64,
    ) -> Result<JobTickReport, JobStoreError> {
        let jobs = self
            .store
            .claim_notifications(ClaimJobNotifications {
                worker_id: worker_id.to_owned(),
                now_ms,
                lease_until_ms: now_ms.saturating_add(self.config.lease_ms.max(1)),
                limit: self.config.claim_limit,
            })
            .await?;
        let mut report = JobTickReport {
            claimed: u64::try_from(jobs.len()).unwrap_or(u64::MAX),
            ..JobTickReport::default()
        };
        for job in jobs {
            let job_id = job.command.job_id;
            let attempts = job.notification_attempts;
            let event = job_event(&job, now_ms)?;
            match self.events.publish(event).await {
                Ok(_) => {
                    self.store
                        .complete_notification(CompleteJobNotification {
                            job_id,
                            worker_id: worker_id.to_owned(),
                            notified_at_ms: now_ms,
                        })
                        .await?;
                    report.published = report.published.saturating_add(1);
                }
                Err(error) => {
                    let dead_letter = attempts >= self.config.max_notification_attempts.max(1);
                    self.store
                        .retry_notification(RetryJobNotification {
                            job_id,
                            worker_id: worker_id.to_owned(),
                            next_attempt_at_ms: now_ms.saturating_add(
                                self.config
                                    .retry_base_ms
                                    .max(1)
                                    .saturating_mul(i64::from(attempts.max(1))),
                            ),
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

fn job_event(job: &JobRecord, now_ms: i64) -> Result<PublishEvent, JobStoreError> {
    let outcome = job
        .outcome
        .as_ref()
        .ok_or_else(|| JobStoreError::Backend("terminal job has no outcome".into()))?;
    let topic = match outcome {
        JobOutcome::Succeeded { .. } => "job.completed",
        JobOutcome::Failed { .. } => "job.failed",
    };
    Ok(PublishEvent {
        event_id: EventId::stable("job-result", &job.command.job_id.to_string()),
        topic: topic.into(),
        event_type: topic.into(),
        schema_version: 1,
        source: EventSource::JobWorker,
        subject: Some(format!("run/{}", job.command.run_id)),
        correlation_id: Some(job.command.job_id.to_string()),
        causation_id: None,
        occurred_at_ms: job.completed_at_ms.unwrap_or(now_ms),
        recorded_at_ms: now_ms,
        payload: serde_json::json!({
            "job_id": job.command.job_id,
            "kind": job.command.kind,
            "outcome": outcome,
        }),
    })
}

fn job_from_flow(error: FlowError) -> JobStoreError {
    match error {
        FlowError::Invalid(message) => JobStoreError::Invalid(message),
        FlowError::NotFound => JobStoreError::NotFound,
        FlowError::Conflict(message) => JobStoreError::Conflict(message),
        FlowError::Backend(message) => JobStoreError::Backend(message),
    }
}
