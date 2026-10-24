use super::{
    effect_coordinator::{invalid, read, reuse},
    effect_model::typed_fact,
};
use agent_core::{effect_pipeline::*, harness::BlobStore, operator_fabric::*, pipeline::*};
use agent_extension::pipeline::DeltaReceiver;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::Notify;

pub struct EffectOutputOperator {
    pub(crate) fabric: Arc<dyn OperatorFabric>,
    pub(crate) blobs: Arc<dyn BlobStore>,
}

impl EffectOutputOperator {
    async fn state(
        &self,
        delivery: &ClaimedDelivery,
    ) -> Result<(Option<u64>, EffectOutputState), FabricError> {
        let envelope = &delivery.event.envelope;
        match read(
            self.fabric.as_ref(),
            output_key(&envelope.tenant_id, envelope.run_id, &envelope.effect_id),
        )
        .await
        {
            Ok((version, state)) => Ok((Some(version), state)),
            Err(FabricError::NotFound) => Ok((
                None,
                EffectOutputState {
                    coverage_digest: coverage(&[]),
                    ..Default::default()
                },
            )),
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn consume(
        &self,
        delivery: &ClaimedDelivery,
        mut receiver: DeltaReceiver,
        notify: &Notify,
        clock: &impl Fn() -> i64,
    ) -> Result<(), FabricError> {
        let mut buffer = OutputBuffer::new(
            delivery.event.envelope.effect_id.clone(),
            MAX_DELTAS,
            MAX_OUTPUT_BYTES,
        );
        let mut interval = tokio::time::interval(super::output::BATCH_WINDOW);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                items = receiver.recv() => match items {
                    Some(items) => { buffer.push(items).map_err(invalid)?; }
                    None => { self.flush(delivery, &buffer.contiguous(), clock()).await?; notify.notify_one(); return Ok(()); }
                },
                _ = interval.tick() => {
                    self.flush(delivery, &buffer.contiguous(), clock()).await?;
                    notify.notify_one();
                }
            }
        }
    }

    async fn flush(
        &self,
        delivery: &ClaimedDelivery,
        journal: &[DeltaItem],
        now_ms: i64,
    ) -> Result<(), FabricError> {
        let envelope = &delivery.event.envelope;
        loop {
            let (expected, mut output) = self.state(delivery).await?;
            let through = output.durable_through as usize;
            if journal.is_empty() {
                return Ok(());
            }
            if through > journal.len() || coverage(&journal[..through]) != output.coverage_digest {
                return Err(invalid("output_gap"));
            }
            if through == journal.len() {
                return Ok(());
            }
            if output.sealed {
                return Err(FabricError::Fenced);
            }
            let end = super::output::batch_end(journal, through).map_err(invalid)?;
            let merged = segments(&journal[through..end]);
            let batch = OutputBatch {
                batch_id: format!("{}:{}:{}", envelope.effect_id, through + 1, end),
                first_seq: through as u64 + 1,
                last_seq: end as u64,
                coverage_digest: coverage(&journal[..end]),
                artifact_ref: super::put(self.blobs.as_ref(), &merged)
                    .await
                    .map_err(invalid)?,
                segments: merged,
            };
            output.durable_through = batch.last_seq;
            output.coverage_digest = batch.coverage_digest.clone();
            output.journal_ref = Some(
                super::put(self.blobs.as_ref(), &journal[..end])
                    .await
                    .map_err(invalid)?,
            );
            output.batches.push(batch.clone());
            let event = typed_fact(
                delivery,
                "output.batch",
                json!(batch),
                &batch.batch_id,
                "output",
                now_ms,
            );
            match self
                .fabric
                .execute(FabricCommand::GuardedTransact {
                    lease: delivery.lease.clone(),
                    now_ms,
                    checkpoint: CheckpointWrite {
                        key: output_key(&envelope.tenant_id, envelope.run_id, &envelope.effect_id),
                        expected,
                        value: json!(output),
                    },
                    outbox: vec![event],
                })
                .await
            {
                Err(FabricError::Fenced) => return Err(FabricError::Fenced),
                result => {
                    result?;
                    tracing::info!(
                        engine = "operator_v1",
                        profile = "operator_v1_tools",
                        delta_batch_items = batch.last_seq.saturating_sub(batch.first_seq) + 1,
                        delta_batch_bytes = serde_json::to_vec(&batch).map_err(invalid)?.len(),
                        "durable delta batch saved"
                    );
                }
            }
        }
    }

    pub async fn execute(&self, delivery: ClaimedDelivery, now_ms: i64) -> Result<(), FabricError> {
        if reuse(self.fabric.as_ref(), &delivery, now_ms).await? {
            return Ok(());
        }
        match self.seal(&delivery, now_ms).await {
            Ok(()) => Ok(()),
            Err(error @ (FabricError::Fenced | FabricError::Storage(_))) => Err(error),
            Err(_) => {
                let envelope = &delivery.event.envelope;
                let (_, state) = self.state(&delivery).await?;
                let payload = json!({"code":"output_gap","durable_through":state.durable_through});
                self.fabric
                    .execute(FabricCommand::Transact {
                        checkpoint: CheckpointWrite {
                            key: format!(
                                "{}:failed",
                                output_key(
                                    &envelope.tenant_id,
                                    envelope.run_id,
                                    &envelope.effect_id
                                )
                            ),
                            expected: None,
                            value: payload.clone(),
                        },
                        outbox: vec![typed_fact(
                            &delivery,
                            "output.failed",
                            payload,
                            "failure",
                            "output",
                            now_ms,
                        )],
                        delivery: Some((delivery.lease, now_ms)),
                    })
                    .await?;
                Ok(())
            }
        }
    }

    async fn seal(&self, delivery: &ClaimedDelivery, now_ms: i64) -> Result<(), FabricError> {
        let envelope = &delivery.event.envelope;
        let key = format!(
            "{}:result",
            model_key(&envelope.tenant_id, envelope.run_id, &envelope.effect_id)
        );
        let (_, execution): (_, EffectModelExecution) = read(self.fabric.as_ref(), key).await?;
        let reference = execution.final_ref.ok_or(FabricError::Conflict)?;
        if !matches!(
            envelope.event_type.as_str(),
            "model.completed" | "model.failed"
        ) || envelope.payload != json!(reference)
        {
            return Err(FabricError::Conflict);
        }
        let final_output: ModelFinal = super::get(self.blobs.as_ref(), reference.artifact_ref)
            .await
            .map_err(invalid)?;
        let (_, previous) = self.state(delivery).await?;
        let mut buffer =
            OutputBuffer::new(envelope.effect_id.clone(), MAX_DELTAS, MAX_OUTPUT_BYTES);
        if let Some(journal_ref) = previous.journal_ref {
            buffer
                .push(
                    super::get(self.blobs.as_ref(), journal_ref)
                        .await
                        .map_err(invalid)?,
                )
                .map_err(invalid)?;
        }
        buffer.repair(&final_output.journal).map_err(invalid)?;
        if final_output.journal.len() as u64 != reference.final_delta_seq
            || coverage(&final_output.journal) != reference.coverage_digest
            || final_output.coverage_digest != reference.coverage_digest
        {
            return Err(invalid("output_gap"));
        }
        self.flush(delivery, &final_output.journal, now_ms).await?;
        let (expected, mut output) = self.state(delivery).await?;
        output.sealed = true;
        self.fabric.execute(FabricCommand::Transact {
            checkpoint: CheckpointWrite { key: output_key(&envelope.tenant_id, envelope.run_id, &envelope.effect_id), expected, value: json!(output) },
            outbox: vec![typed_fact(delivery, "output.sealed", json!({"final_delta_seq":reference.final_delta_seq,"coverage_digest":reference.coverage_digest}), "seal", "output", now_ms)],
            delivery: Some((delivery.lease.clone(), now_ms)),
        }).await?;
        Ok(())
    }
}
