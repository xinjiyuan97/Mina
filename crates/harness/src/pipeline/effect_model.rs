use super::effect_coordinator::{invalid, read, reuse};
use agent_core::{
    effect_pipeline::*,
    event_runtime::EventId,
    harness::*,
    operator::{OperatorIdentity, OperatorSequence},
    operator_fabric::*,
};
use futures_util::StreamExt;
use serde_json::json;
use std::{sync::Arc, time::Duration};

pub struct EffectModelOperator {
    pub(crate) fabric: Arc<dyn OperatorFabric>,
    pub(crate) model: Arc<dyn ModelPort>,
    pub(crate) blobs: Arc<dyn BlobStore>,
}

pub(crate) fn typed_fact(
    delivery: &ClaimedDelivery,
    kind: &str,
    payload: serde_json::Value,
    identity: &str,
    producer: &str,
    now_ms: i64,
) -> FabricEvent {
    let mut event = result_fact(delivery, kind, identity, producer, now_ms);
    event.envelope.event_id = EventId::stable(
        "pipeline-effect-data",
        &format!("{}:{kind}:{identity}", delivery.event.envelope.event_id),
    );
    event.envelope.payload = payload;
    event.payload_schema = agent_core::pipeline::payload_schema(kind);
    event
}

pub(crate) fn result_fact(
    delivery: &ClaimedDelivery,
    kind: &str,
    key: &str,
    producer: &str,
    now_ms: i64,
) -> FabricEvent {
    let mut result = delivery.event.clone();
    result.envelope.event_id = EventId::stable(
        "effect-result",
        &format!("{}:{kind}", delivery.event.envelope.event_id),
    );
    result.envelope.event_type = kind.into();
    result.envelope.causation_id = Some(delivery.event.envelope.event_id);
    result.envelope.operator = OperatorIdentity {
        name: producer.into(),
        version: 1,
    };
    result.envelope.operator_seq = OperatorSequence(0);
    result.envelope.occurred_at_ms = now_ms;
    result.envelope.payload = json!({"checkpoint_key":key});
    result.payload_schema = reference_schema();
    result
}

fn failed(message: &str) -> Vec<ModelEvent> {
    vec![ModelEvent::Failed {
        error: ModelError::new(ModelErrorKind::Internal, message, false),
    }]
}

impl EffectModelOperator {
    pub async fn cancel_requested(
        &self,
        delivery: ClaimedDelivery,
        now_ms: i64,
    ) -> Result<(), FabricError> {
        if reuse(self.fabric.as_ref(), &delivery, now_ms).await? {
            return Ok(());
        }
        let envelope = &delivery.event.envelope;
        let key = format!(
            "{}:result",
            model_key(&envelope.tenant_id, envelope.run_id, &envelope.effect_id)
        );
        if envelope.event_type != "model.cancel.requested"
            || envelope.operator.name != "coordinator"
            || envelope.payload["checkpoint_key"].as_str() != Some(&key)
        {
            return Err(FabricError::Conflict);
        }
        let existing = read::<EffectModelExecution>(self.fabric.as_ref(), key.clone()).await;
        let expected = match existing {
            Ok((_, execution)) if execution.events.is_some() => {
                self.fabric
                    .execute(FabricCommand::Commit {
                        lease: delivery.lease,
                        now_ms,
                        result: json!({"already_finished":true}),
                        outbox: vec![],
                    })
                    .await?;
                return Ok(());
            }
            Ok((version, _)) => Some(version),
            Err(FabricError::NotFound) => None,
            Err(error) => return Err(error),
        };
        let execution = EffectModelExecution {
            lease: delivery.lease.clone(),
            events: Some(failed("model_cancelled")),
            final_ref: None,
        };
        self.fabric
            .execute(FabricCommand::Transact {
                checkpoint: CheckpointWrite {
                    key: key.clone(),
                    expected,
                    value: json!(execution),
                },
                outbox: vec![result_fact(
                    &delivery,
                    "model.cancelled",
                    &key,
                    "model",
                    now_ms,
                )],
                delivery: Some((delivery.lease, now_ms)),
            })
            .await?;
        Ok(())
    }
    pub async fn execute(
        &self,
        delivery: ClaimedDelivery,
        sender: agent_extension::pipeline::DeltaSender,
        drained: tokio::sync::oneshot::Receiver<()>,
        clock: impl Fn() -> i64,
    ) -> Result<(), FabricError> {
        if reuse(self.fabric.as_ref(), &delivery, clock()).await? {
            return Ok(());
        }
        let envelope = &delivery.event.envelope;
        let (_, run): (_, EffectRun) = read(
            self.fabric.as_ref(),
            run_key(&envelope.tenant_id, envelope.run_id),
        )
        .await?;
        if !run
            .reducer
            .pending_effects()
            .iter()
            .any(|effect| effect.effect_id == envelope.effect_id)
        {
            self.fabric
                .execute(FabricCommand::Commit {
                    lease: delivery.lease,
                    now_ms: clock(),
                    result: json!({"fenced":true}),
                    outbox: vec![],
                })
                .await?;
            return Ok(());
        }
        let key = model_key(&envelope.tenant_id, envelope.run_id, &envelope.effect_id);
        if envelope.event_type != "model.requested" {
            return Err(FabricError::Conflict);
        }
        let (version, request): (_, EffectModelRequest) =
            read(self.fabric.as_ref(), key.clone()).await?;
        if version != 0
            || request.tenant_id != envelope.tenant_id
            || request.run_id != envelope.run_id
            || request.effect_id != envelope.effect_id
            || envelope.payload != json!({"request_ref":request.request_ref})
        {
            return Err(FabricError::Conflict);
        }
        let artifact: ModelRequest = super::get(self.blobs.as_ref(), request.request_ref)
            .await
            .map_err(invalid)?;
        if artifact != request.request {
            return Err(FabricError::Conflict);
        }
        let key = format!("{key}:result");
        let existing = read::<EffectModelExecution>(self.fabric.as_ref(), key.clone()).await;
        let (version, mut execution) = match existing {
            Ok((version, execution)) => {
                if execution.events.is_none() && execution.lease.expires_at_ms > clock() {
                    return Ok(());
                }
                (version, execution)
            }
            Err(FabricError::NotFound) => {
                let execution = EffectModelExecution {
                    lease: delivery.lease.clone(),
                    events: None,
                    final_ref: None,
                };
                self.fabric
                    .execute(FabricCommand::GuardedTransact {
                        lease: delivery.lease.clone(),
                        now_ms: clock(),
                        checkpoint: CheckpointWrite {
                            key: key.clone(),
                            expected: None,
                            value: json!(execution),
                        },
                        outbox: vec![typed_fact(
                            &delivery,
                            "model.started",
                            json!({"request_ref":request.request_ref}),
                            "started",
                            "model",
                            clock(),
                        )],
                    })
                    .await?;
                (0, execution)
            }
            Err(error) => return Err(error),
        };
        let was_reclaimed = execution.lease != delivery.lease;
        if execution.events.is_none() {
            use agent_core::pipeline::*;
            let mut journal = Vec::new();
            let mut usage = None;
            let terminal = if was_reclaimed {
                if let Ok((_, output)) = read::<EffectOutputState>(
                    self.fabric.as_ref(),
                    output_key(&envelope.tenant_id, envelope.run_id, &envelope.effect_id),
                )
                .await
                    && let Some(reference) = output.journal_ref
                {
                    journal = super::get(self.blobs.as_ref(), reference)
                        .await
                        .map_err(invalid)?;
                }
                failed("model_interrupted").remove(0)
            } else {
                let remaining = delivery.lease.expires_at_ms.saturating_sub(clock()).max(0) as u64;
                let timeout = Duration::from_millis(request.timeout_ms.min(remaining));
                let events = tokio::time::timeout(timeout, async {
                    let mut stream = self.model.stream(request.request);
                    let mut bytes = 0_usize;
                    while let Some(event) = stream.next().await {
                        let terminal = matches!(
                            event,
                            ModelEvent::Completed { .. } | ModelEvent::Failed { .. }
                        );
                        if terminal {
                            return Ok::<_, FabricError>(event);
                        }
                        if let ModelEvent::Usage { usage: reported } = &event {
                            usage = Some(*reported);
                        }
                        let item =
                            model_delta(&envelope.effect_id, journal.len() as u64 + 1, &event)
                                .map_err(invalid)?;
                        bytes = bytes.saturating_add(item.fragment.len());
                        if bytes > MAX_OUTPUT_BYTES || journal.len() >= MAX_DELTAS {
                            return Ok(failed("model_output_capacity").remove(0));
                        }
                        if sender.send(vec![item.clone()]).await.is_err() {
                            return Ok(failed("output_lag").remove(0));
                        }
                        journal.push(item);
                    }
                    Ok(failed("model_stream_ended").remove(0))
                })
                .await;
                match events {
                    Ok(events) => events?,
                    Err(_) => failed("model_timeout").remove(0),
                }
            };
            drop(sender);
            drained.await.map_err(invalid)?;
            let (finish_reason, model_error, error) = match &terminal {
                ModelEvent::Completed { finish_reason } => (*finish_reason, None, None),
                ModelEvent::Failed { error } => (
                    FinishReason::Unknown,
                    Some(error.clone()),
                    Some(error.kind().code().to_owned()),
                ),
                _ => return Err(FabricError::Conflict),
            };
            let final_output = ModelFinal {
                coverage_digest: coverage(&journal),
                journal,
                usage,
                finish_reason,
                model_error: model_error.clone(),
                error: error.clone(),
            };
            execution.final_ref = Some(FinalRef {
                artifact_ref: super::put(self.blobs.as_ref(), &final_output)
                    .await
                    .map_err(invalid)?,
                final_delta_seq: final_output.journal.len() as u64,
                coverage_digest: final_output.coverage_digest,
                usage,
                finish_reason,
                model_error,
                error,
            });
            execution.events = Some(vec![terminal]);
        }
        execution.lease = delivery.lease.clone();
        let kind = if matches!(
            execution.events.as_ref().and_then(|events| events.last()),
            Some(ModelEvent::Failed { .. })
        ) {
            "model.failed"
        } else {
            "model.completed"
        };
        self.fabric
            .execute(FabricCommand::Transact {
                checkpoint: CheckpointWrite {
                    key: key.clone(),
                    expected: Some(version),
                    value: json!(execution),
                },
                outbox: vec![typed_fact(
                    &delivery,
                    kind,
                    json!(execution.final_ref),
                    "final",
                    "model",
                    clock(),
                )],
                delivery: Some((delivery.lease, clock())),
            })
            .await?;
        Ok(())
    }
}
