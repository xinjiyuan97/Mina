use std::{future::Future, pin::Pin};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::harness::{JobId, StartJob};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobComponentDescriptor {
    pub identity: String,
    pub kind: String,
    pub version: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Pending,
    Running,
    RetryPending,
    Succeeded,
    Failed,
    Cancelled,
}

impl JobStatus {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobNotificationStatus {
    NotReady,
    Pending,
    Publishing,
    RetryPending,
    Published,
    DeadLettered,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum JobOutcome {
    Succeeded {
        value: Value,
    },
    Failed {
        code: String,
        message: String,
        retryable: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobRecord {
    #[serde(flatten)]
    pub command: StartJob,
    pub status: JobStatus,
    pub attempts: u32,
    pub next_attempt_at_ms: i64,
    pub lease_owner: Option<String>,
    pub lease_until_ms: Option<i64>,
    pub last_error: Option<String>,
    pub outcome: Option<JobOutcome>,
    pub completed_at_ms: Option<i64>,
    pub notification_status: JobNotificationStatus,
    pub notification_attempts: u32,
    pub notification_next_attempt_at_ms: i64,
    pub notification_lease_owner: Option<String>,
    pub notification_lease_until_ms: Option<i64>,
    pub notification_last_error: Option<String>,
    pub notified_at_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SubmitJobResult {
    pub job: JobRecord,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimJobs {
    pub worker_id: String,
    pub now_ms: i64,
    pub lease_until_ms: i64,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryJob {
    pub job_id: JobId,
    pub worker_id: String,
    pub next_attempt_at_ms: i64,
    pub error: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompleteJob {
    pub job_id: JobId,
    pub worker_id: String,
    pub outcome: JobOutcome,
    pub completed_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimJobNotifications {
    pub worker_id: String,
    pub now_ms: i64,
    pub lease_until_ms: i64,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompleteJobNotification {
    pub job_id: JobId,
    pub worker_id: String,
    pub notified_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryJobNotification {
    pub job_id: JobId,
    pub worker_id: String,
    pub next_attempt_at_ms: i64,
    pub error: String,
    pub dead_letter: bool,
}

pub type JobFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, JobStoreError>> + Send + 'a>>;

pub trait JobStore: Send + Sync + 'static {
    fn descriptor(&self) -> JobComponentDescriptor;
    fn submit(&self, command: StartJob) -> JobFuture<'_, SubmitJobResult>;
    fn get(&self, job_id: JobId) -> JobFuture<'_, Option<JobRecord>>;
    fn claim(&self, command: ClaimJobs) -> JobFuture<'_, Vec<JobRecord>>;
    fn retry(&self, command: RetryJob) -> JobFuture<'_, JobRecord>;
    fn complete(&self, command: CompleteJob) -> JobFuture<'_, JobRecord>;
    fn claim_notifications(&self, command: ClaimJobNotifications) -> JobFuture<'_, Vec<JobRecord>>;
    fn complete_notification(&self, command: CompleteJobNotification) -> JobFuture<'_, JobRecord>;
    fn retry_notification(&self, command: RetryJobNotification) -> JobFuture<'_, JobRecord>;
    fn cancel(&self, job_id: JobId, cancelled_at_ms: i64) -> JobFuture<'_, JobRecord>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobExecutionError {
    pub code: String,
    pub message: String,
    pub retryable: bool,
    pub retry_after_ms: Option<u64>,
}

pub type JobExecutionFuture =
    Pin<Box<dyn Future<Output = Result<Value, JobExecutionError>> + Send + 'static>>;

pub trait JobRouter: Send + Sync + 'static {
    fn execute(&self, job: JobRecord) -> JobExecutionFuture;
}

#[derive(Debug, Error)]
pub enum JobStoreError {
    #[error("job contract validation failed: {0}")]
    Invalid(String),
    #[error("job was not found")]
    NotFound,
    #[error("job state conflict: {0}")]
    Conflict(String),
    #[error("job store failed: {0}")]
    Backend(String),
}
