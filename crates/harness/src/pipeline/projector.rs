use super::{load, storage};
use agent_core::{harness::*, pipeline::*};

pub struct RunEventProjector;

impl RunEventProjector {
    pub async fn append(runs: &dyn RunStore, events: Vec<RunEvent>) -> Result<(), PipelineError> {
        runs.append_events(
            events
                .into_iter()
                .map(|event| ObservedRunEvent::new(event, crate::unix_time_ms()))
                .collect(),
        )
        .await
        .map(|_| ())
        .map_err(storage)
    }
    pub async fn project(
        store: &dyn PipelineStore,
        runs: &dyn RunStore,
        run_id: RunId,
    ) -> Result<(), PipelineError> {
        loop {
            let mut record = load(store, run_id).await?;
            let expected = record.generation;
            let mut kinds = vec![RunEventKind::RunStarted];
            for batch in &record.batches {
                for segment in &batch.segments {
                    let channel = match segment.channel {
                        DeltaChannel::Text => OutputChannel::AssistantText,
                        DeltaChannel::Reasoning => OutputChannel::AssistantReasoning,
                        DeltaChannel::ToolCall { .. } | DeltaChannel::Control => {
                            return Err(PipelineError::UnsupportedProfile);
                        }
                    };
                    kinds.push(RunEventKind::OutputDelta {
                        channel,
                        delta: segment.text.clone(),
                    });
                }
            }
            if record.ledger.phase == RunPhase::Terminal {
                if !record.sealed
                    && !record
                        .facts
                        .iter()
                        .any(|event| event.event_type == "output.failed")
                {
                    return Err(PipelineError::Gap);
                }
                if let Some(usage) = record
                    .final_ref
                    .as_ref()
                    .and_then(|reference| reference.usage)
                {
                    kinds.push(RunEventKind::UsageUpdated { usage });
                }
                kinds.push(if let Some(code) = &record.ledger.error {
                    let model_error = record
                        .final_ref
                        .as_ref()
                        .and_then(|reference| reference.model_error.as_ref())
                        .filter(|error| error.kind().code() == code);
                    RunEventKind::RunFailed {
                        code: code.clone(),
                        message: model_error
                            .map_or_else(|| code.clone(), |error| error.safe_message().to_owned()),
                        retryable: model_error.is_some_and(ModelError::retryable),
                    }
                } else {
                    RunEventKind::RunCompleted {
                        finish_reason: record
                            .final_ref
                            .as_ref()
                            .ok_or(PipelineError::Gap)?
                            .finish_reason,
                    }
                });
            }
            let events: Vec<_> = kinds
                .into_iter()
                .enumerate()
                .map(|(index, kind)| RunEvent::new(run_id, index as u64 + 1, kind))
                .collect();
            if !events.starts_with(&record.public_events) {
                return Err(PipelineError::Conflict);
            }
            if record.public_events != events {
                record.public_events = events;
                record.generation += 1;
                match store.compare_exchange(Some(expected), record.clone()).await {
                    Err(PipelineError::Fenced) => continue,
                    result => result?,
                }
            }
            let projected = runs
                .get_run(run_id)
                .await
                .map_err(storage)?
                .map_or(0, |run| run.last_seq);
            let projection_lag_events = record
                .public_events
                .last()
                .map_or(0, |event| event.seq)
                .saturating_sub(projected);
            Self::append(runs, record.public_events).await?;
            if projection_lag_events > 0 {
                tracing::info!(
                    engine = "operator_v1",
                    profile = "operator_v1_text",
                    projection_lag_events,
                    "projection caught up"
                );
            }
            return Ok(());
        }
    }
}
