mod coordinator;
mod model;
mod output;
mod projector;

pub use coordinator::RunCoordinator;
pub use model::ModelOperator;
pub use output::ModelOutputAggregator;
pub use projector::RunEventProjector;

use agent_core::operator_fabric::{FabricCommand, FabricEvent, OperatorFabric};
use agent_core::{event_runtime::EventId, harness::*, operator::*, pipeline::*};
use agent_extension::pipeline::{DATA_CAPACITY, bounded_deltas};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::json;
use std::sync::Arc;

#[derive(Clone)]
pub struct PipelineStart {
    pub start: AgentLoopStart,
    pub context_event_id: EventId,
    pub context_sequence: u64,
    pub session_id: Option<SessionId>,
}

impl PipelineStart {
    pub fn text(run_id: RunId, input: &str, model: &str) -> Self {
        Self {
            context_sequence: 0,
            session_id: None,
            context_event_id: EventId::stable(
                "agent-context-v1",
                &format!("{run_id}:context.completed"),
            ),
            start: AgentLoopStart {
                run_id,
                input: input.into(),
                attachments: vec![],
                prior_messages: vec![],
                context_fingerprint: None,
                skill_lock: None,
                config: AgentLoopReducerConfig {
                    model: model.into(),
                    system_prompt: String::new(),
                    max_output_tokens: None,
                    max_steps: 1,
                    allowed_tools: Some(vec![]),
                    allow_run_adf: false,
                    approval_policy: Default::default(),
                    tool_call_strategy: Default::default(),
                    model_timeout_ms: 90_000,
                    tool_timeout_ms: 1,
                },
            },
        }
    }
}

#[derive(Clone)]
pub struct PipelineRuntime {
    pub store: Arc<dyn PipelineStore>,
    pub runs: Arc<dyn RunStore>,
    pub blobs: Arc<dyn BlobStore>,
    model: Arc<dyn ModelPort>,
    fabric: Arc<dyn OperatorFabric>,
}

impl PipelineRuntime {
    pub fn new(
        store: Arc<dyn PipelineStore>,
        runs: Arc<dyn RunStore>,
        blobs: Arc<dyn BlobStore>,
        model: Arc<dyn ModelPort>,
    ) -> Self {
        Self {
            store,
            runs,
            blobs,
            model,
            fabric: Arc::new(agent_extension::operator_fabric::MemoryFabric::default()),
        }
    }

    pub fn with_fabric(mut self, fabric: Arc<dyn OperatorFabric>) -> Self {
        self.fabric = fabric;
        self
    }

    pub async fn dispatch(&self, run_id: RunId) -> Result<(), PipelineError> {
        for envelope in load(self.store.as_ref(), run_id).await?.facts {
            let payload_schema = payload_schema(&envelope.event_type);
            self.fabric
                .execute(FabricCommand::Publish(FabricEvent {
                    topic: PIPELINE_TOPIC.into(),
                    envelope,
                    payload_schema,
                }))
                .await
                .map_err(storage)?;
        }
        Ok(())
    }

    pub async fn admit(&self, start: PipelineStart) -> Result<(), PipelineError> {
        let run_id = start.start.run_id;
        RunCoordinator::new(self.store.clone(), self.blobs.clone())
            .admit(start, self.runs.as_ref())
            .await?;
        self.dispatch(run_id).await?;
        RunEventProjector::project(self.store.as_ref(), self.runs.as_ref(), run_id).await
    }

    pub async fn fail_admission(
        &self,
        start: PipelineStart,
        code: &str,
    ) -> Result<(), PipelineError> {
        let run_id = start.start.run_id;
        RunCoordinator::new(self.store.clone(), self.blobs.clone())
            .admit_outcome(start, self.runs.as_ref(), Some(code))
            .await?;
        self.dispatch(run_id).await?;
        RunEventProjector::project(self.store.as_ref(), self.runs.as_ref(), run_id).await
    }

    pub async fn drive(&self, run_id: RunId) -> Result<(), PipelineError> {
        let coordinator = RunCoordinator::new(self.store.clone(), self.blobs.clone());
        let record = load(self.store.as_ref(), run_id).await?;
        if record.ledger.phase == RunPhase::Terminal {
            self.dispatch(run_id).await?;
            return RunEventProjector::project(self.store.as_ref(), self.runs.as_ref(), run_id)
                .await;
        }
        let Some(record) = coordinator.claim(run_id, crate::unix_time_ms()).await? else {
            return Ok(());
        };
        let token = record.ledger.token.clone();
        self.dispatch(run_id).await?;
        let aggregator = ModelOutputAggregator::new(self.store.clone(), self.blobs.clone())
            .with_projection(self.runs.clone());
        if record.final_ref.is_none() {
            let model =
                ModelOperator::new(self.store.clone(), self.blobs.clone(), self.model.clone());
            if record
                .facts
                .iter()
                .any(|fact| fact.event_type == "model.started")
            {
                model.interrupted(run_id, &token).await?;
            } else {
                let (sender, receiver) = bounded_deltas(DATA_CAPACITY)?;
                let (model_result, output_result) = tokio::join!(
                    model.execute(run_id, token.clone(), sender),
                    aggregator.consume(run_id, token.clone(), receiver)
                );
                model_result?;
                if let Err(error) = output_result {
                    if matches!(error, PipelineError::Fenced | PipelineError::Storage(_)) {
                        return Err(error);
                    }
                    update(self.store.as_ref(), run_id, &token, |record| {
                        fact(
                            record,
                            "output.failed",
                            "1",
                            json!({"code":"output_gap", "durable_through":record.durable_through}),
                            "output",
                        );
                        Ok(())
                    })
                    .await?;
                }
            }
        }
        coordinator.reconcile(run_id).await?;
        aggregator.seal(run_id, &token).await?;
        coordinator.reconcile(run_id).await?;
        self.dispatch(run_id).await?;
        RunEventProjector::project(self.store.as_ref(), self.runs.as_ref(), run_id).await
    }
}

pub(super) fn storage(error: impl std::fmt::Display) -> PipelineError {
    PipelineError::Storage(error.to_string())
}

pub(super) async fn load(
    store: &dyn PipelineStore,
    run_id: RunId,
) -> Result<PipelineRecord, PipelineError> {
    store
        .load(run_id)
        .await?
        .ok_or_else(|| storage("missing operator run"))
}

pub(super) async fn put<T: Serialize>(
    blobs: &dyn BlobStore,
    value: &T,
) -> Result<BlobId, PipelineError> {
    Ok(blobs
        .put(PutBlob {
            media_type: "application/json".into(),
            name: None,
            data: serde_json::to_vec(value).map_err(storage)?,
            created_at_ms: crate::unix_time_ms(),
        })
        .await
        .map_err(storage)?
        .blob_id)
}

pub(super) async fn get<T: DeserializeOwned>(
    blobs: &dyn BlobStore,
    reference: BlobId,
) -> Result<T, PipelineError> {
    let blob = blobs
        .get(reference)
        .await
        .map_err(storage)?
        .ok_or_else(|| storage("missing artifact"))?;
    serde_json::from_slice(&blob.data).map_err(storage)
}

pub(super) async fn update(
    store: &dyn PipelineStore,
    run_id: RunId,
    token: &InvocationToken,
    change: impl Fn(&mut PipelineRecord) -> Result<(), PipelineError>,
) -> Result<(), PipelineError> {
    loop {
        let mut record = load(store, run_id).await?;
        if &record.ledger.token != token || record.lease_expires_at_ms <= crate::unix_time_ms() {
            return Err(PipelineError::Fenced);
        }
        let expected = record.generation;
        change(&mut record)?;
        record.generation += 1;
        match store.compare_exchange(Some(expected), record).await {
            Err(PipelineError::Fenced) => continue,
            result => return result,
        }
    }
}

pub(super) fn fact(
    record: &mut PipelineRecord,
    kind: &str,
    key: &str,
    payload: serde_json::Value,
    operator: &str,
) {
    let event_id = EventId::stable(
        "operator-p3",
        &format!("{}:{kind}:{key}", record.ledger.run_id),
    );
    if record.facts.iter().any(|event| event.event_id == event_id) {
        return;
    }
    let token = &record.ledger.token;
    let cause = record.facts.last().map(|event| event.event_id);
    record.facts.push(OperatorEnvelope {
        schema_version: 1,
        engine_version: EngineVersion::OperatorV1,
        projection_version: 1,
        event_id,
        event_type: kind.into(),
        tenant_id: "local".into(),
        run_id: record.ledger.run_id,
        session_id: record.session_id,
        effect_id: token.effect_id.clone(),
        invocation_id: token.invocation_id.clone(),
        attempt: token.attempt,
        issued_revision: RunRevision(token.issued_revision),
        lease_epoch: LeaseEpoch(token.lease_epoch),
        correlation_id: record.ledger.run_id.to_string(),
        causation_id: cause,
        operator: OperatorIdentity {
            name: operator.into(),
            version: 1,
        },
        operator_seq: OperatorSequence(
            record.operator_sequence_base + record.facts.len() as u64 + 1,
        ),
        delivery_class: DeliveryClass::Durable,
        occurred_at_ms: crate::unix_time_ms(),
        payload,
    });
}

pub(super) fn intents(record: &mut PipelineRecord, intents: Vec<Intent>) {
    for intent in intents {
        let (kind, payload) = match intent {
            Intent::RunStarted => ("run.started", json!({})),
            Intent::ModelRequested => (
                "model.requested",
                json!({"request_ref": record.model_request_ref}),
            ),
            Intent::RunCompleted => (
                "run.completed",
                json!({"finish_reason": record.final_ref.as_ref().map(|value| value.finish_reason)}),
            ),
            Intent::RunFailed => ("run.failed", json!({"code": record.ledger.error})),
        };
        fact(record, kind, "1", payload, "coordinator");
    }
}
