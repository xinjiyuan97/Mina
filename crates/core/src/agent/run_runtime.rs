//! Host-neutral run records. Execution belongs exclusively to AgentEngine.
use super::HarnessError;
use crate::harness::*;
use serde_json::Value;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::broadcast;

pub struct StartedRun {
    pub run_id: RunId,
    pub events: broadcast::Receiver<RunEvent>,
}

pub struct PlannedRun {
    pub run_id: RunId,
    pub input: String,
    pub attachments: Vec<ModelAttachment>,
    pub prior_messages: Vec<ModelMessage>,
    pub allowed_tools: Option<Vec<String>>,
    pub allow_run_adf: bool,
    pub max_steps: Option<u32>,
    pub context_fingerprint: Option<String>,
    pub execution_manifest: Option<Value>,
}

/// Run queries and pre-admission rejection shared by transport integrations.
#[derive(Clone)]
pub struct RunRepository {
    metadata: AgentMetadata,
    store: Arc<dyn RunStore>,
}
impl RunRepository {
    pub fn new(metadata: AgentMetadata, store: Arc<dyn RunStore>) -> Self {
        Self { metadata, store }
    }
    pub fn metadata(&self) -> AgentMetadata {
        self.metadata.clone()
    }
    pub fn run_store(&self) -> Arc<dyn RunStore> {
        self.store.clone()
    }
    pub async fn get_run(&self, run_id: RunId) -> Result<Option<RunSnapshot>, RunStoreError> {
        self.store.get_run(run_id).await
    }
    pub async fn events_after(
        &self,
        run_id: RunId,
        after: u64,
        limit: usize,
    ) -> Result<Vec<RunEvent>, RunStoreError> {
        self.store.events_after(run_id, after, limit).await
    }
}

#[derive(Debug, Error)]
pub enum RunRuntimeError {
    #[error(transparent)]
    Harness(#[from] HarnessError),
    #[error(transparent)]
    Store(#[from] RunStoreError),
    #[error(transparent)]
    Flow(#[from] FlowError),
    #[error("agent machine failed: {0}")]
    Machine(String),
    #[error("run persistence failed: {0}")]
    Persistence(String),
}

pub fn unix_time_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

impl RunRepository {
    pub async fn reject_unadmitted(
        &self,
        run_id: RunId,
        code: &str,
        message: &str,
    ) -> Result<RunSnapshot, RunRuntimeError> {
        if self
            .store
            .get_run(run_id)
            .await?
            .and_then(|run| run.execution_manifest)
            .and_then(|manifest| manifest.get("engine_version").cloned())
            .as_ref()
            .and_then(serde_json::Value::as_str)
            == Some("operator_v1")
        {
            return Err(RunRuntimeError::Machine(
                "operator_v1 projection is exclusively owned".into(),
            ));
        }
        let snapshot = self
            .store
            .append_event(
                RunEvent::new(run_id, 1, RunEventKind::RunStarted),
                unix_time_ms(),
            )
            .await?;
        let failed = self
            .store
            .append_event(
                RunEvent::new(
                    run_id,
                    snapshot.last_seq.saturating_add(1),
                    RunEventKind::RunFailed {
                        code: code.into(),
                        message: message.into(),
                        retryable: false,
                    },
                ),
                unix_time_ms(),
            )
            .await?;
        Ok(failed)
    }
}
