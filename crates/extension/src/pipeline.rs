use agent_core::pipeline::{DeltaItem, DeltaMessage, MAX_DELTA_BYTES, PipelineError};
use tokio::sync::mpsc;

pub const DATA_CAPACITY: usize = 32;
pub const MAX_MESSAGE_ITEMS: usize = 128;

mod artifacts;
pub use artifacts::FabricBlobStore;

#[derive(Clone)]
pub struct DeltaSender(mpsc::Sender<Vec<DeltaItem>>);
pub struct DeltaReceiver(mpsc::Receiver<Vec<DeltaItem>>);

pub fn bounded_deltas(capacity: usize) -> Result<(DeltaSender, DeltaReceiver), PipelineError> {
    if capacity == 0 || capacity > DATA_CAPACITY {
        return Err(PipelineError::Capacity);
    }
    let (sender, receiver) = mpsc::channel(capacity);
    Ok((DeltaSender(sender), DeltaReceiver(receiver)))
}

impl DeltaSender {
    pub async fn send_message(&self, message: DeltaMessage) -> Result<(), PipelineError> {
        self.send(message.into_items()).await
    }

    pub async fn send(&self, items: Vec<DeltaItem>) -> Result<(), PipelineError> {
        if items.is_empty()
            || items.len() > MAX_MESSAGE_ITEMS
            || serde_json::to_vec(&items)
                .map_err(|error| PipelineError::Storage(error.to_string()))?
                .len()
                > MAX_DELTA_BYTES
        {
            return Err(PipelineError::Capacity);
        }
        self.0.send(items).await.map_err(|_| PipelineError::Gap)
    }
}

impl DeltaReceiver {
    pub async fn recv(&mut self) -> Option<Vec<DeltaItem>> {
        self.0.recv().await
    }
}

#[cfg(feature = "sqlite")]
mod sqlite {
    use agent_core::{harness::RunId, pipeline::*};
    use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
    use std::{
        path::Path,
        sync::{Arc, Mutex},
        time::Duration,
    };

    #[derive(Clone)]
    pub struct SqlitePipelineStore(Arc<Mutex<Connection>>);

    fn storage(error: impl std::fmt::Display) -> PipelineError {
        PipelineError::Storage(error.to_string())
    }

    impl SqlitePipelineStore {
        pub fn open(path: impl AsRef<Path>) -> Result<Self, PipelineError> {
            let connection = Connection::open(path).map_err(storage)?;
            connection
                .busy_timeout(Duration::from_secs(5))
                .map_err(storage)?;
            connection.execute_batch("CREATE TABLE IF NOT EXISTS operator_runs_v1 (run_id TEXT PRIMARY KEY, generation INTEGER NOT NULL, record_json TEXT NOT NULL)").map_err(storage)?;
            Ok(Self(Arc::new(Mutex::new(connection))))
        }
    }

    impl PipelineStore for SqlitePipelineStore {
        fn load(&self, run_id: RunId) -> PipelineFuture<'_, Option<PipelineRecord>> {
            Box::pin(async move {
                let connection = self.0.clone();
                tokio::task::spawn_blocking(move || {
                    let connection = connection.lock().map_err(storage)?;
                    let encoded: Option<String> = connection
                        .query_row(
                            "SELECT record_json FROM operator_runs_v1 WHERE run_id = ?1",
                            [run_id.to_string()],
                            |row| row.get(0),
                        )
                        .optional()
                        .map_err(storage)?;
                    encoded
                        .map(|encoded| serde_json::from_str(&encoded).map_err(storage))
                        .transpose()
                })
                .await
                .map_err(storage)?
            })
        }

        fn compare_exchange(
            &self,
            expected: Option<u64>,
            record: PipelineRecord,
        ) -> PipelineFuture<'_, ()> {
            Box::pin(async move {
                let connection = self.0.clone();
                tokio::task::spawn_blocking(move || {
                    let mut connection = connection.lock().map_err(storage)?;
                    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate).map_err(storage)?;
                    let actual: Option<(i64, String)> = transaction.query_row("SELECT generation, record_json FROM operator_runs_v1 WHERE run_id = ?1", [record.ledger.run_id.to_string()], |row| Ok((row.get(0)?, row.get(1)?))).optional().map_err(storage)?;
                    if actual.as_ref().map(|(value, _)| *value as u64) != expected || record.generation != expected.map_or(0, |generation| generation + 1) { return Err(PipelineError::Fenced); }
                    let previous: Option<PipelineRecord> = actual.map(|(_, encoded)| serde_json::from_str(&encoded).map_err(storage)).transpose()?;
                    record.validate_append(previous.as_ref())?;
                    transaction.execute("INSERT INTO operator_runs_v1 (run_id, generation, record_json) VALUES (?1, ?2, ?3) ON CONFLICT(run_id) DO UPDATE SET generation=excluded.generation, record_json=excluded.record_json", params![record.ledger.run_id.to_string(), i64::try_from(record.generation).map_err(storage)?, serde_json::to_string(&record).map_err(storage)?]).map_err(storage)?;
                    transaction.commit().map_err(storage)
                }).await.map_err(storage)?
            })
        }
    }
}

#[cfg(feature = "sqlite")]
pub use sqlite::SqlitePipelineStore;
