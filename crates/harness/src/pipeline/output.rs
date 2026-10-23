use super::{fact, get, load, put, update};
use agent_core::{
    harness::{BlobStore, RunId, RunStore},
    pipeline::*,
};
use agent_extension::pipeline::DeltaReceiver;
use serde_json::json;
use std::{sync::Arc, time::Duration};

pub const BATCH_ITEMS: usize = 128;
pub const BATCH_BYTES: usize = 32 * 1024;
pub const BATCH_WINDOW: Duration = Duration::from_millis(40);

pub(super) fn batch_end(journal: &[DeltaItem], through: usize) -> Result<usize, PipelineError> {
    let mut end = through;
    let mut bytes = 0;
    while end < journal.len() && end - through < BATCH_ITEMS {
        let size = serde_json::to_vec(&journal[end])
            .map_err(super::storage)?
            .len();
        if end > through && bytes + size > BATCH_BYTES {
            break;
        }
        bytes += size;
        end += 1;
    }
    Ok(end)
}

pub struct ModelOutputAggregator {
    store: Arc<dyn PipelineStore>,
    blobs: Arc<dyn BlobStore>,
    runs: Option<Arc<dyn RunStore>>,
}

impl ModelOutputAggregator {
    pub fn new(store: Arc<dyn PipelineStore>, blobs: Arc<dyn BlobStore>) -> Self {
        Self {
            store,
            blobs,
            runs: None,
        }
    }

    pub fn with_projection(mut self, runs: Arc<dyn RunStore>) -> Self {
        self.runs = Some(runs);
        self
    }

    pub async fn consume(
        &self,
        run_id: RunId,
        token: InvocationToken,
        mut receiver: DeltaReceiver,
    ) -> Result<(), PipelineError> {
        let mut buffer =
            OutputBuffer::new(token.invocation_id.clone(), MAX_DELTAS, MAX_OUTPUT_BYTES);
        let mut interval = tokio::time::interval(BATCH_WINDOW);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        interval.tick().await;
        let mut pending_items = 0;
        let mut pending_bytes = 0;
        loop {
            tokio::select! {
                message = receiver.recv() => {
                    let Some(items) = message else { break; };
                    pending_items += items.len();
                    pending_bytes += items.iter().map(|item| item.fragment.len()).sum::<usize>();
                    buffer.push(items)?;
                    if pending_items < BATCH_ITEMS && pending_bytes < BATCH_BYTES { continue; }
                }
                _ = interval.tick(), if pending_items > 0 => {}
            }
            self.flush(run_id, &token, &buffer.contiguous()).await?;
            pending_items = 0;
            pending_bytes = 0;
        }
        self.flush(run_id, &token, &buffer.contiguous()).await
    }

    pub async fn flush(
        &self,
        run_id: RunId,
        token: &InvocationToken,
        journal: &[DeltaItem],
    ) -> Result<(), PipelineError> {
        loop {
            let record = load(self.store.as_ref(), run_id).await?;
            if &record.ledger.token != token || record.lease_expires_at_ms <= crate::unix_time_ms()
            {
                return Err(PipelineError::Fenced);
            }
            let through = record.durable_through as usize;
            if journal.len() < through || coverage(&journal[..through]) != record.coverage_digest {
                return Err(PipelineError::Gap);
            }
            if through == journal.len() {
                return Ok(());
            }
            if journal.iter().enumerate().any(|(index, item)| {
                item.delta_seq != index as u64 + 1
                    || item.invocation_id != token.invocation_id
                    || matches!(
                        item.channel,
                        DeltaChannel::ToolCall { .. } | DeltaChannel::Control
                    )
            }) {
                return Err(PipelineError::Conflict);
            }
            let end = batch_end(journal, through)?;
            let merged = segments(&journal[through..end]);
            let batch = OutputBatch {
                batch_id: format!("{}:{}:{}", token.invocation_id, through + 1, end),
                first_seq: through as u64 + 1,
                last_seq: end as u64,
                segments: merged.clone(),
                coverage_digest: coverage(&journal[..end]),
                artifact_ref: put(self.blobs.as_ref(), &merged).await?,
            };
            update(self.store.as_ref(), run_id, token, |record| {
                if record.durable_through != through as u64 {
                    return Err(PipelineError::Fenced);
                }
                if record.sealed || record.ledger.phase == RunPhase::Terminal {
                    return Err(PipelineError::Conflict);
                }
                record.durable_through = batch.last_seq;
                record.coverage_digest = batch.coverage_digest.clone();
                record.batches.push(batch.clone());
                fact(
                    record,
                    "output.batch",
                    &batch.batch_id,
                    json!(batch),
                    "output",
                );
                Ok(())
            })
            .await?;
            if let Some(runs) = &self.runs {
                super::RunEventProjector::project(self.store.as_ref(), runs.as_ref(), run_id)
                    .await?;
            }
        }
    }

    pub async fn seal(&self, run_id: RunId, token: &InvocationToken) -> Result<(), PipelineError> {
        let record = load(self.store.as_ref(), run_id).await?;
        if record.sealed
            || record
                .facts
                .iter()
                .any(|event| event.event_type == "output.failed")
        {
            return Ok(());
        }
        let final_ref = record.final_ref.ok_or(PipelineError::Gap)?;
        let repaired = async {
            let artifact: ModelFinal = get(self.blobs.as_ref(), final_ref.artifact_ref).await?;
            if artifact.journal.len() as u64 != final_ref.final_delta_seq
                || coverage(&artifact.journal) != final_ref.coverage_digest
                || artifact.coverage_digest != final_ref.coverage_digest
            {
                return Err(PipelineError::Gap);
            }
            self.flush(run_id, token, &artifact.journal).await
        }
        .await;
        if let Err(error) = repaired {
            if matches!(error, PipelineError::Fenced) {
                return Err(error);
            }
            return update(self.store.as_ref(), run_id, token, |record| {
                fact(
                    record,
                    "output.failed",
                    "1",
                    json!({"code":"output_gap", "durable_through":record.durable_through}),
                    "output",
                );
                Ok(())
            })
            .await;
        }
        update(self.store.as_ref(), run_id, token, |record| {
            if record.durable_through != final_ref.final_delta_seq || record.coverage_digest != final_ref.coverage_digest { return Err(PipelineError::Gap); }
            record.sealed = true;
            fact(record, "output.sealed", "1", json!({"final_delta_seq":record.durable_through,"coverage_digest":record.coverage_digest}), "output");
            Ok(())
        }).await
    }
}
