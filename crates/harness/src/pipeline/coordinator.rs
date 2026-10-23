use super::{PipelineStart, get, intents, load, put, storage};
use agent_core::{harness::*, pipeline::*};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::sync::Arc;

pub struct RunCoordinator {
    store: Arc<dyn PipelineStore>,
    blobs: Arc<dyn BlobStore>,
}

impl RunCoordinator {
    pub fn new(store: Arc<dyn PipelineStore>, blobs: Arc<dyn BlobStore>) -> Self {
        Self { store, blobs }
    }

    pub async fn claim(
        &self,
        run_id: RunId,
        now: i64,
    ) -> Result<Option<PipelineRecord>, PipelineError> {
        let mut record = load(self.store.as_ref(), run_id).await?;
        if record.ledger.phase == RunPhase::Terminal
            || (record.lease_owner.is_some() && record.lease_expires_at_ms > now)
        {
            return Ok(None);
        }
        let expected = record.generation;
        if record.lease_owner.is_some() {
            record.ledger.token.lease_epoch += 1;
        }
        record.lease_owner = Some(format!("process-{}", std::process::id()));
        record.lease_expires_at_ms = now + 120_000;
        record.generation += 1;
        match self
            .store
            .compare_exchange(Some(expected), record.clone())
            .await
        {
            Err(PipelineError::Fenced) => Ok(None),
            Err(error) => Err(error),
            Ok(()) => Ok(Some(record)),
        }
    }

    pub async fn admit(
        &self,
        start: PipelineStart,
        runs: &dyn RunStore,
    ) -> Result<(), PipelineError> {
        self.admit_outcome(start, runs, None).await
    }

    pub async fn admit_outcome(
        &self,
        start: PipelineStart,
        runs: &dyn RunStore,
        failure: Option<&str>,
    ) -> Result<(), PipelineError> {
        let run_id = start.start.run_id;
        let start_digest = format!(
            "sha256:{:x}",
            Sha256::digest(serde_json::to_vec(&(&start.start, failure)).map_err(storage)?)
        );
        if let Some(existing) = self.store.load(run_id).await? {
            if existing.start_digest != start_digest {
                return Err(PipelineError::Conflict);
            }
            return Ok(());
        }
        if start
            .start
            .config
            .allowed_tools
            .as_ref()
            .is_none_or(|tools| !tools.is_empty())
            || start.start.config.allow_run_adf
        {
            return Err(PipelineError::UnsupportedProfile);
        }
        let mut snapshot =
            RunSnapshot::new(run_id, start.start.input.clone(), crate::unix_time_ms());
        snapshot = snapshot.with_execution_manifest(
            json!({"engine_version":"operator_v1", "projection_version":1}),
        );
        if let Some(existing) = runs.get_run(run_id).await.map_err(storage)? {
            if existing.last_seq != 0 {
                if self
                    .store
                    .load(run_id)
                    .await?
                    .is_some_and(|record| record.start_digest == start_digest)
                {
                    return Ok(());
                }
                return Err(PipelineError::Conflict);
            }
            runs.set_execution_manifest(
                run_id,
                snapshot.execution_manifest.clone().expect("manifest"),
            )
            .await
            .map_err(storage)?;
        } else if let Err(error) = runs.create_run(snapshot).await {
            let existing = runs
                .get_run(run_id)
                .await
                .map_err(storage)?
                .ok_or_else(|| storage(&error))?;
            if existing.input != start.start.input
                || existing
                    .execution_manifest
                    .as_ref()
                    .and_then(|manifest| manifest["engine_version"].as_str())
                    != Some("operator_v1")
            {
                return Err(storage(error));
            }
        }
        let started = AgentLoopReducer::start(start.start);
        let initial_effect_id = started.effects[0].effect_id.clone();
        let input = if let Some(code) = failure {
            AgentLoopInput::ToolSetFailed {
                effect_id: initial_effect_id.clone(),
                error: MachineError {
                    code: code.into(),
                    message: code.into(),
                    retryable: false,
                },
            }
        } else {
            AgentLoopInput::ToolSetLoaded {
                effect_id: initial_effect_id.clone(),
                tool_set: ToolSetSnapshot::new(1, vec![], vec![]).map_err(storage)?,
            }
        };
        let transition = AgentLoopReducer::dispatch(started.state, input);
        let (effect_id, request_ref) = if failure.is_some() {
            (initial_effect_id, None)
        } else {
            let effect = transition.effects.first().ok_or(PipelineError::Conflict)?;
            let AgentLoopEffectKind::InvokeModel { request, .. } = &effect.kind else {
                return Err(PipelineError::UnsupportedProfile);
            };
            (
                effect.effect_id.clone(),
                Some(put(self.blobs.as_ref(), request).await?),
            )
        };
        let mut ledger =
            RunLedger::new(run_id, effect_id.clone(), format!("{effect_id}:attempt:1"));
        let token = ledger.token.clone();
        let control = failure.map_or(Control::ContextCompleted, |code| Control::ContextFailed {
            code: code.into(),
        });
        let emitted = ledger.reduce(0, &token, control)?;
        let mut record = PipelineRecord {
            start_digest: start_digest.clone(),
            operator_sequence_base: start.context_sequence,
            session_id: start.session_id,
            generation: 0,
            ledger,
            reducer: transition.state,
            model_request_ref: request_ref,
            final_ref: None,
            batches: vec![],
            durable_through: 0,
            coverage_digest: coverage(&[]),
            sealed: failure.is_some(),
            lease_owner: None,
            lease_expires_at_ms: 0,
            facts: vec![],
            public_events: vec![],
        };
        intents(&mut record, emitted);
        record.facts[0].causation_id = Some(start.context_event_id);
        match self.store.compare_exchange(None, record).await {
            Err(PipelineError::Fenced) => {
                if load(self.store.as_ref(), run_id).await?.start_digest == start_digest {
                    Ok(())
                } else {
                    Err(PipelineError::Conflict)
                }
            }
            result => result,
        }
    }

    pub async fn reconcile(&self, run_id: RunId) -> Result<(), PipelineError> {
        loop {
            let mut record = load(self.store.as_ref(), run_id).await?;
            let expected = record.generation;
            let token = record.ledger.token.clone();
            let mut changed = false;
            for event in record.facts.clone() {
                let control = match event.event_type.as_str() {
                    "model.started" => Control::ModelStarted,
                    "model.completed" => Control::ModelCompleted,
                    "model.failed" => Control::ModelFailed {
                        code: record
                            .final_ref
                            .as_ref()
                            .and_then(|value| value.error.clone())
                            .unwrap_or_else(|| "model_failed".into()),
                    },
                    "output.sealed" => Control::OutputSealed,
                    "output.failed" => Control::OutputFailed {
                        code: event.payload["code"]
                            .as_str()
                            .unwrap_or("output_failed")
                            .into(),
                    },
                    _ => continue,
                };
                if record.ledger.inbox.contains(&control) {
                    continue;
                }
                if matches!(
                    control,
                    Control::ModelCompleted | Control::ModelFailed { .. }
                ) {
                    let final_ref = record.final_ref.as_ref().ok_or(PipelineError::Conflict)?;
                    let final_artifact: ModelFinal =
                        get(self.blobs.as_ref(), final_ref.artifact_ref)
                            .await
                            .unwrap_or_else(|_| ModelFinal {
                                model_error: None,
                                journal: vec![],
                                coverage_digest: coverage(&[]),
                                usage: final_ref.usage,
                                finish_reason: FinishReason::Unknown,
                                error: Some("output_gap".into()),
                            });
                    let mut events = final_artifact
                        .journal
                        .iter()
                        .filter_map(|item| match item.channel {
                            DeltaChannel::Text => Some(ModelEvent::TextDelta {
                                delta: item.fragment.clone(),
                            }),
                            DeltaChannel::Reasoning => Some(ModelEvent::ReasoningDelta {
                                delta: item.fragment.clone(),
                            }),
                            DeltaChannel::ToolCall { .. } => None,
                        })
                        .collect::<Vec<_>>();
                    if let Some(usage) = final_artifact.usage {
                        events.push(ModelEvent::Usage { usage });
                    }
                    events.push(if let Some(code) = final_artifact.error {
                        ModelEvent::Failed {
                            error: final_artifact.model_error.unwrap_or_else(|| {
                                ModelError::new(ModelErrorKind::ProtocolViolation, code, false)
                            }),
                        }
                    } else {
                        ModelEvent::Completed {
                            finish_reason: final_artifact.finish_reason,
                        }
                    });
                    for event in events {
                        record.reducer = AgentLoopReducer::dispatch(
                            record.reducer,
                            AgentLoopInput::ModelEvent {
                                effect_id: token.effect_id.clone(),
                                event,
                            },
                        )
                        .state;
                    }
                    if !record.reducer.pending_effects().is_empty() {
                        return Err(PipelineError::UnsupportedProfile);
                    }
                }
                if matches!(control, Control::OutputSealed) && !record.sealed {
                    return Err(PipelineError::Conflict);
                }
                let emitted = record
                    .ledger
                    .reduce(record.ledger.revision, &token, control)?;
                intents(&mut record, emitted);
                changed = true;
            }
            if !changed {
                return Ok(());
            }
            record.generation += 1;
            match self.store.compare_exchange(Some(expected), record).await {
                Err(PipelineError::Fenced) => continue,
                result => return result,
            }
        }
    }
}
