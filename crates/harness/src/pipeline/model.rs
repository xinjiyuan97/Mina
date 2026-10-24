use super::{fact, get, load, put, update};
use agent_core::{harness::*, pipeline::*};
use agent_extension::pipeline::DeltaSender;
use futures_util::StreamExt;
use serde_json::json;
use std::{sync::Arc, time::Duration};

pub struct ModelOperator {
    store: Arc<dyn PipelineStore>,
    blobs: Arc<dyn BlobStore>,
    model: Arc<dyn ModelPort>,
}

impl ModelOperator {
    pub fn new(
        store: Arc<dyn PipelineStore>,
        blobs: Arc<dyn BlobStore>,
        model: Arc<dyn ModelPort>,
    ) -> Self {
        Self {
            store,
            blobs,
            model,
        }
    }

    pub async fn execute(
        &self,
        run_id: RunId,
        token: InvocationToken,
        sender: DeltaSender,
    ) -> Result<(), PipelineError> {
        let started_at = std::time::Instant::now();
        let record = load(self.store.as_ref(), run_id).await?;
        if record.ledger.token != token || record.lease_expires_at_ms <= crate::unix_time_ms() {
            return Err(PipelineError::Fenced);
        }
        let requested = record
            .facts
            .iter()
            .find(|event| event.event_type == "model.requested")
            .ok_or(PipelineError::Conflict)?;
        let requested: ModelRequested =
            serde_json::from_value(requested.payload.clone()).map_err(super::storage)?;
        if Some(requested.request_ref) != record.model_request_ref {
            return Err(PipelineError::Conflict);
        }
        if record.final_ref.is_some()
            || record
                .facts
                .iter()
                .any(|event| event.event_type == "model.started")
        {
            return Ok(());
        }
        let request: ModelRequest = get(
            self.blobs.as_ref(),
            record.model_request_ref.ok_or(PipelineError::Conflict)?,
        )
        .await?;
        if !record.reducer.pending_effects().iter().any(|effect| {
            effect.effect_id == token.effect_id
                && matches!(&effect.kind, AgentLoopEffectKind::InvokeModel { request: expected, .. } if expected == &request)
        }) {
            return Err(PipelineError::Conflict);
        }
        if !request.tools.is_empty() {
            return self
                .save(
                    run_id,
                    &token,
                    ModelFinal {
                        model_error: None,
                        journal: vec![],
                        coverage_digest: coverage(&[]),
                        usage: None,
                        finish_reason: FinishReason::Unknown,
                        error: Some("unsupported_profile".into()),
                    },
                )
                .await;
        }
        let started = update(self.store.as_ref(), run_id, &token, |record| {
            if record
                .facts
                .iter()
                .any(|event| event.event_type == "model.started")
            {
                return Err(PipelineError::Conflict);
            }
            fact(
                record,
                "model.started",
                "1",
                json!({"request_ref":record.model_request_ref}),
                "model",
            );
            Ok(())
        })
        .await;
        if matches!(started, Err(PipelineError::Conflict)) {
            return Ok(());
        }
        started?;
        let mut result = ModelFinal {
            model_error: None,
            journal: vec![],
            coverage_digest: coverage(&[]),
            usage: None,
            finish_reason: FinishReason::Unknown,
            error: None,
        };
        let timeout = Duration::from_millis(record.reducer.config.model_timeout_ms.min(90_000));
        let outcome = tokio::time::timeout(timeout, async {
            let mut stream = self.model.stream(request);
            let mut bytes = 0;
            while let Some(event) = stream.next().await {
                let delta = match event {
                    ModelEvent::TextDelta { delta } => Some((DeltaChannel::Text, delta)),
                    ModelEvent::ReasoningDelta { delta } => Some((DeltaChannel::Reasoning, delta)),
                    ModelEvent::ToolCallStarted { .. }
                    | ModelEvent::ToolCallArgumentsDelta { .. }
                    | ModelEvent::ProviderToolCallStarted { .. }
                    | ModelEvent::ProviderToolCallCompleted { .. } => {
                        return Some("unsupported_profile".into());
                    }
                    ModelEvent::Usage { usage } => {
                        result.usage = Some(usage);
                        None
                    }
                    ModelEvent::Completed { finish_reason } => {
                        result.finish_reason = finish_reason;
                        return (finish_reason == FinishReason::ToolCall)
                            .then(|| "unsupported_profile".into());
                    }
                    ModelEvent::Failed { error } => {
                        let code = error.kind().code().into();
                        result.model_error = Some(error);
                        return Some(code);
                    }
                    ModelEvent::Accepted { .. }
                    | ModelEvent::ReasoningStarted { .. }
                    | ModelEvent::ReasoningCompleted { .. } => None,
                    _ => return Some("upstream_protocol_violation".into()),
                };
                if let Some((channel, fragment)) = delta {
                    if fragment.len() + token.invocation_id.len() + 64 > MAX_DELTA_BYTES
                        || bytes + fragment.len() > MAX_OUTPUT_BYTES
                        || result.journal.len() >= MAX_DELTAS
                    {
                        return Some("output_capacity".into());
                    }
                    bytes += fragment.len();
                    let item = DeltaItem {
                        invocation_id: token.invocation_id.clone(),
                        delta_seq: result.journal.len() as u64 + 1,
                        channel,
                        fragment,
                    };
                    result.journal.push(item.clone());
                    if sender.send(vec![item]).await.is_err() {
                        return Some("output_lag".into());
                    }
                }
            }
            Some("upstream_protocol_violation".into())
        })
        .await;
        result.error = outcome.unwrap_or_else(|_| Some("upstream_timeout".into()));
        result.coverage_digest = coverage(&result.journal);
        let saved = self.save(run_id, &token, result).await;
        tracing::info!(
            operator = "model",
            engine = "operator_v1",
            profile = "operator_v1_text",
            operator_latency_ms = started_at.elapsed().as_millis() as u64,
            success = saved.is_ok(),
            "model operator finished"
        );
        saved
    }

    pub async fn interrupted(
        &self,
        run_id: RunId,
        token: &InvocationToken,
    ) -> Result<(), PipelineError> {
        self.save(
            run_id,
            token,
            ModelFinal {
                model_error: None,
                journal: vec![],
                coverage_digest: coverage(&[]),
                usage: None,
                finish_reason: FinishReason::Unknown,
                error: Some("model_interrupted".into()),
            },
        )
        .await
    }

    async fn save(
        &self,
        run_id: RunId,
        token: &InvocationToken,
        result: ModelFinal,
    ) -> Result<(), PipelineError> {
        let reference = FinalRef {
            artifact_ref: put(self.blobs.as_ref(), &result).await?,
            model_error: result.model_error,
            final_delta_seq: result.journal.len() as u64,
            coverage_digest: result.coverage_digest,
            usage: result.usage,
            finish_reason: result.finish_reason,
            error: result.error,
        };
        update(self.store.as_ref(), run_id, token, |record| {
            if record.final_ref.is_some() {
                return Err(PipelineError::Conflict);
            }
            record.final_ref = Some(reference.clone());
            fact(
                record,
                if reference.error.is_some() {
                    "model.failed"
                } else {
                    "model.completed"
                },
                "1",
                json!(reference),
                "model",
            );
            Ok(())
        })
        .await
    }
}
