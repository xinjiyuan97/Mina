use std::sync::{Arc, Mutex};

use agent_core::harness::{
    ClaimJobNotifications, ClaimJobs, CompleteJob, CompleteJobNotification, JobComponentDescriptor,
    JobFuture, JobId, JobNotificationStatus, JobOutcome, JobRecord, JobStatus, JobStore,
    JobStoreError, RetryJob, RetryJobNotification, StartJob, SubmitJobResult,
};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

use super::SqliteEventStore;

const MAX_JOB_CLAIM_BATCH: usize = 1_000;

impl JobStore for SqliteEventStore {
    fn descriptor(&self) -> JobComponentDescriptor {
        JobComponentDescriptor {
            identity: self.identity.clone(),
            kind: "sqlite_job_store".into(),
            version: env!("CARGO_PKG_VERSION").into(),
        }
    }

    fn submit(&self, command: StartJob) -> JobFuture<'_, SubmitJobResult> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            command.validate().map_err(map_flow_error)?;
            run_blocking(move || submit_job(&connection, command)).await
        })
    }

    fn get(&self, job_id: JobId) -> JobFuture<'_, Option<JobRecord>> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            run_blocking(move || {
                let connection = lock(&connection)?;
                load_job_optional(&connection, job_id)
            })
            .await
        })
    }

    fn claim(&self, command: ClaimJobs) -> JobFuture<'_, Vec<JobRecord>> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            validate_claim(&command.worker_id, command.now_ms, command.lease_until_ms)?;
            run_blocking(move || claim_jobs(&connection, command)).await
        })
    }

    fn retry(&self, command: RetryJob) -> JobFuture<'_, JobRecord> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move { run_blocking(move || retry_job(&connection, command)).await })
    }

    fn complete(&self, command: CompleteJob) -> JobFuture<'_, JobRecord> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move { run_blocking(move || complete_job(&connection, command)).await })
    }

    fn claim_notifications(&self, command: ClaimJobNotifications) -> JobFuture<'_, Vec<JobRecord>> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            validate_claim(&command.worker_id, command.now_ms, command.lease_until_ms)?;
            run_blocking(move || claim_notifications(&connection, command)).await
        })
    }

    fn complete_notification(&self, command: CompleteJobNotification) -> JobFuture<'_, JobRecord> {
        let connection = Arc::clone(&self.connection);
        Box::pin(
            async move { run_blocking(move || complete_notification(&connection, command)).await },
        )
    }

    fn retry_notification(&self, command: RetryJobNotification) -> JobFuture<'_, JobRecord> {
        let connection = Arc::clone(&self.connection);
        Box::pin(
            async move { run_blocking(move || retry_notification(&connection, command)).await },
        )
    }

    fn cancel(&self, job_id: JobId, cancelled_at_ms: i64) -> JobFuture<'_, JobRecord> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            run_blocking(move || cancel_job(&connection, job_id, cancelled_at_ms)).await
        })
    }
}

fn submit_job(
    connection: &Mutex<Connection>,
    command: StartJob,
) -> Result<SubmitJobResult, JobStoreError> {
    let mut connection = lock(connection)?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| sql_error("begin job submission", error))?;
    let command_json = encode(&command, "job command")?;
    let dedupe_json = encode(
        &(
            command.run_id,
            &command.kind,
            &command.input,
            &command.idempotency_key,
        ),
        "job idempotency content",
    )?;
    if let Some((existing_command, record_json)) = transaction
        .query_row(
            "SELECT command_json, record_json FROM workflow_jobs WHERE job_id = ?1",
            [command.job_id.to_string()],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()
        .map_err(|error| sql_error("read replayed job", error))?
    {
        if existing_command != command_json {
            return Err(JobStoreError::Conflict(
                "job_id was reused with different content".into(),
            ));
        }
        return Ok(SubmitJobResult {
            job: decode(&record_json, "job record")?,
            replayed: true,
        });
    }
    if let Some((existing_dedupe, record_json)) = transaction
        .query_row(
            "SELECT dedupe_json, record_json FROM workflow_jobs
             WHERE run_id = ?1 AND kind = ?2 AND idempotency_key = ?3",
            params![
                command.run_id.to_string(),
                command.kind,
                command.idempotency_key,
            ],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()
        .map_err(|error| sql_error("read idempotent job", error))?
    {
        if existing_dedupe != dedupe_json {
            return Err(JobStoreError::Conflict(
                "job idempotency key was reused with different content".into(),
            ));
        }
        return Ok(SubmitJobResult {
            job: decode(&record_json, "job record")?,
            replayed: true,
        });
    }
    let job = JobRecord {
        next_attempt_at_ms: command.requested_at_ms,
        notification_next_attempt_at_ms: command.requested_at_ms,
        command,
        status: JobStatus::Pending,
        attempts: 0,
        lease_owner: None,
        lease_until_ms: None,
        last_error: None,
        outcome: None,
        completed_at_ms: None,
        notification_status: JobNotificationStatus::NotReady,
        notification_attempts: 0,
        notification_lease_owner: None,
        notification_lease_until_ms: None,
        notification_last_error: None,
        notified_at_ms: None,
    };
    transaction
        .execute(
            "INSERT INTO workflow_jobs (
                job_id, run_id, kind, idempotency_key, status, next_attempt_at_ms,
                lease_owner, lease_until_ms, notification_status,
                notification_next_attempt_at_ms, notification_lease_owner,
                notification_lease_until_ms, dedupe_json, command_json, record_json
             ) VALUES (?1, ?2, ?3, ?4, 'pending', ?5, NULL, NULL, 'not_ready',
                ?5, NULL, NULL, ?6, ?7, ?8)",
            params![
                job.command.job_id.to_string(),
                job.command.run_id.to_string(),
                job.command.kind,
                job.command.idempotency_key,
                job.next_attempt_at_ms,
                dedupe_json,
                command_json,
                encode(&job, "job record")?,
            ],
        )
        .map_err(|error| sql_error("insert job", error))?;
    transaction
        .commit()
        .map_err(|error| sql_error("commit job submission", error))?;
    Ok(SubmitJobResult {
        job,
        replayed: false,
    })
}

fn claim_jobs(
    connection: &Mutex<Connection>,
    command: ClaimJobs,
) -> Result<Vec<JobRecord>, JobStoreError> {
    let mut connection = lock(connection)?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| sql_error("begin job claim", error))?;
    let ids = query_job_ids(
        &transaction,
        "SELECT job_id FROM workflow_jobs
         WHERE ((status IN ('pending', 'retry_pending') AND next_attempt_at_ms <= ?1)
            OR (status = 'running' AND lease_until_ms IS NOT NULL AND lease_until_ms <= ?1))
         ORDER BY next_attempt_at_ms, job_id LIMIT ?2",
        command.now_ms,
        command.limit,
        "job claim",
    )?;
    let mut jobs = Vec::with_capacity(ids.len());
    for job_id in ids {
        let mut job = load_job(&transaction, job_id)?;
        job.status = JobStatus::Running;
        job.attempts = job.attempts.saturating_add(1);
        job.lease_owner = Some(command.worker_id.clone());
        job.lease_until_ms = Some(command.lease_until_ms);
        persist_job(&transaction, &job)?;
        jobs.push(job);
    }
    transaction
        .commit()
        .map_err(|error| sql_error("commit job claim", error))?;
    Ok(jobs)
}

fn retry_job(
    connection: &Mutex<Connection>,
    command: RetryJob,
) -> Result<JobRecord, JobStoreError> {
    let connection = lock(connection)?;
    let mut job = load_job(&connection, command.job_id)?;
    ensure_job_lease(&job, &command.worker_id)?;
    job.status = JobStatus::RetryPending;
    job.next_attempt_at_ms = command.next_attempt_at_ms;
    job.lease_owner = None;
    job.lease_until_ms = None;
    job.last_error = Some(command.error);
    persist_job(&connection, &job)?;
    Ok(job)
}

fn complete_job(
    connection: &Mutex<Connection>,
    command: CompleteJob,
) -> Result<JobRecord, JobStoreError> {
    let connection = lock(connection)?;
    let mut job = load_job(&connection, command.job_id)?;
    if job.status.is_terminal() && job.outcome.as_ref() == Some(&command.outcome) {
        return Ok(job);
    }
    ensure_job_lease(&job, &command.worker_id)?;
    job.status = match &command.outcome {
        JobOutcome::Succeeded { .. } => JobStatus::Succeeded,
        JobOutcome::Failed { .. } => JobStatus::Failed,
    };
    job.outcome = Some(command.outcome);
    job.completed_at_ms = Some(command.completed_at_ms);
    job.lease_owner = None;
    job.lease_until_ms = None;
    job.last_error = None;
    job.notification_status = JobNotificationStatus::Pending;
    job.notification_next_attempt_at_ms = command.completed_at_ms;
    persist_job(&connection, &job)?;
    Ok(job)
}

fn claim_notifications(
    connection: &Mutex<Connection>,
    command: ClaimJobNotifications,
) -> Result<Vec<JobRecord>, JobStoreError> {
    let mut connection = lock(connection)?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| sql_error("begin job notification claim", error))?;
    let ids = query_job_ids(
        &transaction,
        "SELECT job_id FROM workflow_jobs
         WHERE ((notification_status IN ('pending', 'retry_pending')
                    AND notification_next_attempt_at_ms <= ?1)
            OR (notification_status = 'publishing'
                    AND notification_lease_until_ms IS NOT NULL
                    AND notification_lease_until_ms <= ?1))
         ORDER BY notification_next_attempt_at_ms, job_id LIMIT ?2",
        command.now_ms,
        command.limit,
        "job notification claim",
    )?;
    let mut jobs = Vec::with_capacity(ids.len());
    for job_id in ids {
        let mut job = load_job(&transaction, job_id)?;
        job.notification_status = JobNotificationStatus::Publishing;
        job.notification_attempts = job.notification_attempts.saturating_add(1);
        job.notification_lease_owner = Some(command.worker_id.clone());
        job.notification_lease_until_ms = Some(command.lease_until_ms);
        persist_job(&transaction, &job)?;
        jobs.push(job);
    }
    transaction
        .commit()
        .map_err(|error| sql_error("commit job notification claim", error))?;
    Ok(jobs)
}

fn complete_notification(
    connection: &Mutex<Connection>,
    command: CompleteJobNotification,
) -> Result<JobRecord, JobStoreError> {
    let connection = lock(connection)?;
    let mut job = load_job(&connection, command.job_id)?;
    if job.notification_status == JobNotificationStatus::Published {
        return Ok(job);
    }
    ensure_notification_lease(&job, &command.worker_id)?;
    job.notification_status = JobNotificationStatus::Published;
    job.notification_lease_owner = None;
    job.notification_lease_until_ms = None;
    job.notification_last_error = None;
    job.notified_at_ms = Some(command.notified_at_ms);
    persist_job(&connection, &job)?;
    Ok(job)
}

fn retry_notification(
    connection: &Mutex<Connection>,
    command: RetryJobNotification,
) -> Result<JobRecord, JobStoreError> {
    let connection = lock(connection)?;
    let mut job = load_job(&connection, command.job_id)?;
    ensure_notification_lease(&job, &command.worker_id)?;
    job.notification_status = if command.dead_letter {
        JobNotificationStatus::DeadLettered
    } else {
        JobNotificationStatus::RetryPending
    };
    job.notification_next_attempt_at_ms = command.next_attempt_at_ms;
    job.notification_lease_owner = None;
    job.notification_lease_until_ms = None;
    job.notification_last_error = Some(command.error);
    persist_job(&connection, &job)?;
    Ok(job)
}

fn cancel_job(
    connection: &Mutex<Connection>,
    job_id: JobId,
    cancelled_at_ms: i64,
) -> Result<JobRecord, JobStoreError> {
    let connection = lock(connection)?;
    let mut job = load_job(&connection, job_id)?;
    if job.status.is_terminal() {
        return Ok(job);
    }
    job.status = JobStatus::Cancelled;
    job.completed_at_ms = Some(cancelled_at_ms);
    job.lease_owner = None;
    job.lease_until_ms = None;
    job.notification_status = JobNotificationStatus::Cancelled;
    job.notification_lease_owner = None;
    job.notification_lease_until_ms = None;
    persist_job(&connection, &job)?;
    Ok(job)
}

fn query_job_ids(
    connection: &Connection,
    sql: &str,
    now_ms: i64,
    limit: usize,
    operation: &str,
) -> Result<Vec<JobId>, JobStoreError> {
    let mut statement = connection
        .prepare(sql)
        .map_err(|error| sql_error(&format!("prepare {operation}"), error))?;
    statement
        .query_map(
            params![
                now_ms,
                i64::try_from(limit.clamp(1, MAX_JOB_CLAIM_BATCH)).unwrap_or(i64::MAX)
            ],
            |row| row.get::<_, String>(0),
        )
        .map_err(|error| sql_error(&format!("query {operation}"), error))?
        .map(|row| {
            row.map_err(|error| sql_error(&format!("read {operation}"), error))?
                .parse()
                .map_err(|_| JobStoreError::Backend("stored job id is invalid".into()))
        })
        .collect()
}

fn load_job(connection: &Connection, job_id: JobId) -> Result<JobRecord, JobStoreError> {
    load_job_optional(connection, job_id)?.ok_or(JobStoreError::NotFound)
}

fn load_job_optional(
    connection: &Connection,
    job_id: JobId,
) -> Result<Option<JobRecord>, JobStoreError> {
    connection
        .query_row(
            "SELECT record_json FROM workflow_jobs WHERE job_id = ?1",
            [job_id.to_string()],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|error| sql_error("load job", error))?
        .map(|json| decode(&json, "job record"))
        .transpose()
}

fn persist_job(connection: &Connection, job: &JobRecord) -> Result<(), JobStoreError> {
    connection
        .execute(
            "UPDATE workflow_jobs SET status = ?1, next_attempt_at_ms = ?2,
                lease_owner = ?3, lease_until_ms = ?4, notification_status = ?5,
                notification_next_attempt_at_ms = ?6, notification_lease_owner = ?7,
                notification_lease_until_ms = ?8, record_json = ?9 WHERE job_id = ?10",
            params![
                job_status_key(job.status),
                job.next_attempt_at_ms,
                job.lease_owner,
                job.lease_until_ms,
                notification_status_key(job.notification_status),
                job.notification_next_attempt_at_ms,
                job.notification_lease_owner,
                job.notification_lease_until_ms,
                encode(job, "job record")?,
                job.command.job_id.to_string(),
            ],
        )
        .map_err(|error| sql_error("persist job", error))?;
    Ok(())
}

fn ensure_job_lease(job: &JobRecord, worker_id: &str) -> Result<(), JobStoreError> {
    if job.status != JobStatus::Running || job.lease_owner.as_deref() != Some(worker_id) {
        return Err(JobStoreError::Conflict(
            "job execution lease is not owned by this worker".into(),
        ));
    }
    Ok(())
}

fn ensure_notification_lease(job: &JobRecord, worker_id: &str) -> Result<(), JobStoreError> {
    if job.notification_status != JobNotificationStatus::Publishing
        || job.notification_lease_owner.as_deref() != Some(worker_id)
    {
        return Err(JobStoreError::Conflict(
            "job notification lease is not owned by this worker".into(),
        ));
    }
    Ok(())
}

fn validate_claim(worker_id: &str, now_ms: i64, lease_until_ms: i64) -> Result<(), JobStoreError> {
    if worker_id.trim().is_empty() || worker_id.len() > 128 {
        return Err(JobStoreError::Invalid(
            "job worker_id must contain 1 to 128 bytes".into(),
        ));
    }
    if lease_until_ms <= now_ms {
        return Err(JobStoreError::Invalid(
            "job lease must expire after now".into(),
        ));
    }
    Ok(())
}

const fn job_status_key(status: JobStatus) -> &'static str {
    match status {
        JobStatus::Pending => "pending",
        JobStatus::Running => "running",
        JobStatus::RetryPending => "retry_pending",
        JobStatus::Succeeded => "succeeded",
        JobStatus::Failed => "failed",
        JobStatus::Cancelled => "cancelled",
    }
}

const fn notification_status_key(status: JobNotificationStatus) -> &'static str {
    match status {
        JobNotificationStatus::NotReady => "not_ready",
        JobNotificationStatus::Pending => "pending",
        JobNotificationStatus::Publishing => "publishing",
        JobNotificationStatus::RetryPending => "retry_pending",
        JobNotificationStatus::Published => "published",
        JobNotificationStatus::DeadLettered => "dead_lettered",
        JobNotificationStatus::Cancelled => "cancelled",
    }
}

fn lock(
    connection: &Mutex<Connection>,
) -> Result<std::sync::MutexGuard<'_, Connection>, JobStoreError> {
    connection
        .lock()
        .map_err(|_| JobStoreError::Backend("SQLite job connection lock was poisoned".into()))
}

fn encode(value: &impl serde::Serialize, label: &str) -> Result<String, JobStoreError> {
    serde_json::to_string(value)
        .map_err(|error| JobStoreError::Backend(format!("encode {label}: {error}")))
}

fn decode<T: serde::de::DeserializeOwned>(json: &str, label: &str) -> Result<T, JobStoreError> {
    serde_json::from_str(json)
        .map_err(|error| JobStoreError::Backend(format!("decode {label}: {error}")))
}

fn sql_error(operation: &str, error: rusqlite::Error) -> JobStoreError {
    JobStoreError::Backend(format!("{operation}: {error}"))
}

fn map_flow_error(error: agent_core::harness::FlowError) -> JobStoreError {
    match error {
        agent_core::harness::FlowError::Invalid(message) => JobStoreError::Invalid(message),
        agent_core::harness::FlowError::NotFound => JobStoreError::NotFound,
        agent_core::harness::FlowError::Conflict(message) => JobStoreError::Conflict(message),
        agent_core::harness::FlowError::Backend(message) => JobStoreError::Backend(message),
    }
}

async fn run_blocking<T: Send + 'static>(
    operation: impl FnOnce() -> Result<T, JobStoreError> + Send + 'static,
) -> Result<T, JobStoreError> {
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|error| JobStoreError::Backend(format!("job store worker failed: {error}")))?
}

#[cfg(test)]
mod tests {
    use agent_core::harness::RunId;
    use serde_json::json;
    use tempfile::tempdir;

    use super::*;

    #[tokio::test]
    async fn job_execution_and_notification_leases_survive_reopen() {
        let directory = tempdir().expect("temporary directory should exist");
        let path = directory.path().join("jobs.sqlite3");
        let run_id = RunId::new();
        let command = StartJob {
            job_id: JobId::new(),
            run_id,
            kind: "test.echo".into(),
            input: json!({"value": 1}),
            idempotency_key: "echo-1".into(),
            requested_at_ms: 1,
        };
        let store = SqliteEventStore::open(&path, "jobs:test")
            .await
            .expect("job store should open");
        let submitted = store
            .submit(command.clone())
            .await
            .expect("job should submit");
        assert!(!submitted.replayed);
        let replay = store
            .submit(StartJob {
                job_id: JobId::new(),
                requested_at_ms: 2,
                ..command.clone()
            })
            .await
            .expect("idempotency key should replay");
        assert!(replay.replayed);
        assert_eq!(replay.job.command.job_id, command.job_id);
        let claimed = store
            .claim(ClaimJobs {
                worker_id: "job-worker-1".into(),
                now_ms: 2,
                lease_until_ms: 3,
                limit: 1,
            })
            .await
            .expect("job should claim")
            .into_iter()
            .next()
            .expect("one job should exist");
        assert_eq!(claimed.attempts, 1);
        drop(store);

        let reopened = SqliteEventStore::open(&path, "jobs:test")
            .await
            .expect("job store should reopen");
        let reclaimed = reopened
            .claim(ClaimJobs {
                worker_id: "job-worker-2".into(),
                now_ms: 4,
                lease_until_ms: 10,
                limit: 1,
            })
            .await
            .expect("expired job should reclaim")
            .into_iter()
            .next()
            .expect("one reclaimed job should exist");
        assert_eq!(reclaimed.attempts, 2);
        reopened
            .complete(CompleteJob {
                job_id: command.job_id,
                worker_id: "job-worker-2".into(),
                outcome: JobOutcome::Succeeded {
                    value: json!({"value": 1}),
                },
                completed_at_ms: 5,
            })
            .await
            .expect("job should complete");
        let notification = reopened
            .claim_notifications(ClaimJobNotifications {
                worker_id: "notification-worker".into(),
                now_ms: 5,
                lease_until_ms: 10,
                limit: 1,
            })
            .await
            .expect("notification should claim")
            .into_iter()
            .next()
            .expect("one notification should exist");
        assert_eq!(
            notification.notification_status,
            JobNotificationStatus::Publishing
        );
        let published = reopened
            .complete_notification(CompleteJobNotification {
                job_id: command.job_id,
                worker_id: "notification-worker".into(),
                notified_at_ms: 6,
            })
            .await
            .expect("notification should complete");
        assert_eq!(
            published.notification_status,
            JobNotificationStatus::Published
        );
    }
}
