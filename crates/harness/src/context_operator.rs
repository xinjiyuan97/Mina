//! P2 Context execution and bounded admission barrier.

use crate::{PlannedRun, operator_fabric::*, unix_time_ms};
use agent_core::{
    context::{ContextBuildRequest, ContextEngine},
    context_operator::*,
    event_runtime::EventId,
    harness::{BlobId, BlobStore, PutBlob, RunId},
    memory::MemoryScope,
    operator::*,
    operator_fabric::*,
    skill::{ResolveSkillsRequest, SkillOrchestrator, compile_skills},
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{sync::Arc, time::Duration};
use tokio::sync::Notify;

pub struct AdmissionFacade {
    fabric: Arc<dyn OperatorFabric>,
    operator: ContextOperator,
    changed: Notify,
    work: Notify,
}

pub struct ContextOperator {
    blobs: Arc<dyn BlobStore>,
    preparer: Arc<dyn ContextPreparer>,
}

pub struct PreparedRun {
    pub plan: PlannedRun,
    pub context_fingerprint: String,
    pub max_steps: u32,
}

impl AdmissionFacade {
    pub fn new(
        fabric: Arc<dyn OperatorFabric>,
        blobs: Arc<dyn BlobStore>,
        preparer: Arc<dyn ContextPreparer>,
    ) -> Self {
        Self {
            fabric,
            operator: ContextOperator { blobs, preparer },
            changed: Notify::new(),
            work: Notify::new(),
        }
    }

    pub fn context_operator(&self) -> &ContextOperator {
        &self.operator
    }

    pub async fn admit(
        &self,
        request: ContextRequest,
        timeout: Duration,
    ) -> Result<PreparedRun, AdmissionError> {
        self.publish(request.clone()).await?;
        let prepared = self.wait(request.run_id, timeout).await?;
        Ok(PreparedRun {
            context_fingerprint: prepared.pack.fingerprint.clone(),
            max_steps: request.max_steps,
            plan: legacy_plan(request, prepared),
        })
    }

    pub async fn publish(&self, request: ContextRequest) -> Result<(), FabricError> {
        if request.request_hash.is_empty() || request.max_steps == 0 {
            return Err(FabricError::Invalid(
                "missing request hash or step budget".into(),
            ));
        }
        if let Some(existing) = self.request(request.run_id).await? {
            return if existing == request {
                Ok(())
            } else {
                Err(FabricError::Conflict)
            };
        }
        let payload = RequestPayload {
            request_hash: request.request_hash.clone(),
            request_ref: self.put(&request).await?,
            max_steps: request.max_steps,
        };
        let now = unix_time_ms();
        let events = [RUN_REQUESTED, CONTEXT_REQUESTED]
            .into_iter()
            .enumerate()
            .map(|(index, kind)| event(&request, kind, index as u64 + 1, now, json!(payload)))
            .collect();
        match self
            .fabric
            .execute(FabricCommand::PublishBatch(events))
            .await
        {
            Ok(FabricReply::Published(_)) => {}
            Err(FabricError::Conflict) => {
                if self.request(request.run_id).await?.as_ref() != Some(&request) {
                    return Err(FabricError::Conflict);
                }
            }
            Err(error) => return Err(error),
            Ok(_) => return Err(storage("unexpected publish reply")),
        }
        self.work.notify_one();
        Ok(())
    }

    pub async fn request(&self, run_id: RunId) -> Result<Option<ContextRequest>, FabricError> {
        let Some(event) = self.read_event(run_id, RUN_REQUESTED).await? else {
            return Ok(None);
        };
        let payload: RequestPayload =
            serde_json::from_value(event.envelope.payload).map_err(storage)?;
        self.get(payload.request_ref).await.map(Some)
    }

    pub async fn requests(&self) -> Result<Vec<ContextRequest>, FabricError> {
        let mut requests = Vec::new();
        let mut after = 0;
        loop {
            let FabricReply::Replay(page) = self
                .fabric
                .execute(FabricCommand::ScanEvents {
                    topic: CONTEXT_TOPIC.into(),
                    event_type: RUN_REQUESTED.into(),
                    after,
                    limit: 128,
                })
                .await?
            else {
                return Err(storage("unexpected scan reply"));
            };
            if page.is_empty() {
                break;
            }
            for item in page {
                after = item.offset;
                let payload: RequestPayload =
                    serde_json::from_value(item.event.envelope.payload).map_err(storage)?;
                requests.push(self.get(payload.request_ref).await?);
            }
        }
        Ok(requests)
    }

    pub async fn outcome(&self, run_id: RunId) -> Result<Option<ContextOutcome>, FabricError> {
        if let Some(event) = self.read_event(run_id, CONTEXT_COMPLETED).await? {
            return serde_json::from_value(event.envelope.payload)
                .map(ContextOutcome::Completed)
                .map(Some)
                .map_err(storage);
        }
        if let Some(event) = self.read_event(run_id, CONTEXT_FAILED).await? {
            return serde_json::from_value::<ContextFailurePayload>(event.envelope.payload)
                .map(|payload| Some(ContextOutcome::Failed(payload)))
                .map_err(storage);
        }
        Ok(None)
    }

    async fn read_event(
        &self,
        run_id: RunId,
        kind: &str,
    ) -> Result<Option<FabricEvent>, FabricError> {
        match self
            .fabric
            .execute(FabricCommand::ReadEvent(event_id(run_id, kind)))
            .await?
        {
            FabricReply::Event(event) => Ok(event),
            _ => Err(storage("unexpected event reply")),
        }
    }

    pub async fn tick(&self) -> Result<FabricTickReport, FabricError> {
        self.fabric
            .execute(FabricCommand::Subscribe(SubscriptionSpec {
                consumer: CONTEXT_CONSUMER.into(),
                subscription: CONTEXT_CONSUMER.into(),
                topic: CONTEXT_TOPIC.into(),
                event_type: Some(CONTEXT_REQUESTED.into()),
                tenant: Some("local".into()),
            }))
            .await?;
        let runtime = FabricRuntime {
            config: FabricRuntimeConfig {
                worker: format!("context-{}", std::process::id()),
                claim_limit: 128,
                lease_ms: 300_000,
                max_attempts: 10,
                backoff_ms: 100,
                max_backoff_ms: 30_000,
            },
        };
        let report = runtime
            .tick(self.fabric.as_ref(), &self.operator, unix_time_ms)
            .await?;
        self.changed.notify_waiters();
        Ok(report)
    }

    pub fn start_worker(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let facade = Arc::clone(self);
        tokio::spawn(async move {
            let mut recovery = tokio::time::interval(Duration::from_secs(1));
            loop {
                tokio::select! { _ = facade.work.notified() => {}, _ = recovery.tick() => {} }
                loop {
                    match facade.tick().await {
                        Ok(report) if report.committed > 0 => continue,
                        Ok(_) => break,
                        Err(error) => {
                            tracing::error!(%error, "Context fabric tick failed");
                            break;
                        }
                    }
                }
            }
        })
    }

    pub async fn wait(
        &self,
        run_id: RunId,
        timeout: Duration,
    ) -> Result<PreparedContext, AdmissionError> {
        if timeout.is_zero() {
            return Err(AdmissionError::Timeout);
        }
        tokio::time::timeout(timeout, async {
            let mut recovery = tokio::time::interval(Duration::from_secs(1));
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if let Some(event) = self.read_event(run_id, CONTEXT_COMPLETED).await? {
                    let completed: ContextCompleted =
                        serde_json::from_value(event.envelope.payload).map_err(storage)?;
                    return Ok(self.get(completed.prepared_ref).await?);
                }
                if let Some(event) = self.read_event(run_id, CONTEXT_FAILED).await? {
                    let failed: ContextFailurePayload =
                        serde_json::from_value(event.envelope.payload).map_err(storage)?;
                    return Err(AdmissionError::Preparation(
                        self.get(failed.error_ref).await?,
                    ));
                }
                tokio::select! { _ = &mut changed => {}, _ = recovery.tick() => {} }
            }
        })
        .await
        .map_err(|_| AdmissionError::Timeout)?
    }

    async fn put(&self, value: &impl Serialize) -> Result<BlobId, FabricError> {
        self.operator.put(value).await
    }

    async fn get<Value: DeserializeOwned>(&self, blob_id: BlobId) -> Result<Value, FabricError> {
        self.operator.get(blob_id).await
    }
}

impl ContextOperator {
    async fn put(&self, value: &impl Serialize) -> Result<BlobId, FabricError> {
        self.blobs
            .put(PutBlob {
                media_type: "application/json".into(),
                name: None,
                data: serde_json::to_vec(value).map_err(storage)?,
                created_at_ms: unix_time_ms(),
            })
            .await
            .map(|metadata| metadata.blob_id)
            .map_err(storage)
    }

    async fn get<Value: DeserializeOwned>(&self, blob_id: BlobId) -> Result<Value, FabricError> {
        let object = self
            .blobs
            .get(blob_id)
            .await
            .map_err(storage)?
            .ok_or_else(|| storage("missing admission artifact"))?;
        serde_json::from_slice(&object.data).map_err(storage)
    }
}

impl FabricHandler for ContextOperator {
    fn handle(&self, delivery: ClaimedDelivery) -> HandlerFuture<'_> {
        Box::pin(async move {
            if delivery.event.envelope.event_type != CONTEXT_REQUESTED
                || delivery.event.envelope.engine_version != EngineVersion::LegacyMachine
            {
                return Err("not a P2 Context request".into());
            }
            let payload: RequestPayload =
                serde_json::from_value(delivery.event.envelope.payload.clone())
                    .map_err(|error| error.to_string())?;
            let request: ContextRequest = self
                .get(payload.request_ref)
                .await
                .map_err(|error| error.to_string())?;
            if request.run_id != delivery.event.envelope.run_id
                || request.request_hash != payload.request_hash
                || request.session_id != delivery.event.envelope.session_id
                || request.max_steps != payload.max_steps
            {
                return Err("Context request identity mismatch".into());
            }
            let (kind, payload, outcome) = match self.preparer.prepare(request.clone()).await {
                Ok(prepared) => {
                    let completed = ContextCompleted {
                        request_hash: request.request_hash.clone(),
                        prepared_ref: self
                            .put(&prepared)
                            .await
                            .map_err(|error| error.to_string())?,
                        fingerprint: prepared.pack.fingerprint,
                        skill_lock: prepared.skill_lock,
                        memory_refs: prepared.pack.memory_refs,
                        artifact_refs: prepared.pack.artifact_refs,
                        max_steps: request.max_steps,
                    };
                    (
                        CONTEXT_COMPLETED,
                        json!(completed),
                        ContextOutcome::Completed(completed),
                    )
                }
                Err(failed) => {
                    let payload = ContextFailurePayload {
                        request_hash: request.request_hash.clone(),
                        max_steps: request.max_steps,
                        error_ref: self.put(&failed).await.map_err(|error| error.to_string())?,
                        code: failed.code,
                    };
                    (
                        CONTEXT_FAILED,
                        json!(payload),
                        ContextOutcome::Failed(payload),
                    )
                }
            };
            Ok(HandlerOutput {
                result: json!(outcome),
                outbox: vec![event(&request, kind, 3, unix_time_ms(), payload)],
            })
        })
    }
}

pub fn event_id(run_id: RunId, kind: &str) -> EventId {
    EventId::stable("agent-context-v1", &format!("{run_id}:{kind}"))
}

fn event(
    request: &ContextRequest,
    kind: &str,
    sequence: u64,
    now: i64,
    payload: serde_json::Value,
) -> FabricEvent {
    FabricEvent {
        topic: CONTEXT_TOPIC.into(),
        envelope: OperatorEnvelope {
            schema_version: 1,
            engine_version: EngineVersion::LegacyMachine,
            projection_version: 1,
            event_id: event_id(request.run_id, kind),
            event_type: kind.into(),
            tenant_id: "local".into(),
            run_id: request.run_id,
            session_id: request.session_id,
            effect_id: format!("{}:context", request.run_id),
            invocation_id: format!("{}:context:1", request.run_id),
            attempt: 1,
            issued_revision: RunRevision(0),
            lease_epoch: LeaseEpoch(1),
            correlation_id: request.run_id.to_string(),
            causation_id: match sequence {
                1 => None,
                2 => Some(event_id(request.run_id, RUN_REQUESTED)),
                _ => Some(event_id(request.run_id, CONTEXT_REQUESTED)),
            },
            operator: OperatorIdentity {
                name: "context".into(),
                version: 1,
            },
            operator_seq: OperatorSequence(sequence),
            delivery_class: DeliveryClass::Durable,
            occurred_at_ms: now,
            payload,
        },
        payload_schema: payload_schema(kind),
    }
}

fn storage(error: impl std::fmt::Display) -> FabricError {
    FabricError::Storage(error.to_string())
}

pub struct AgentContextPreparer {
    pub context: ContextEngine,
    pub skills: Arc<SkillOrchestrator>,
}

impl ContextPreparer for AgentContextPreparer {
    fn prepare(&self, request: ContextRequest) -> PrepareFuture<'_> {
        Box::pin(async move {
            let policy = &request.policy;
            let resolved_skills = self
                .skills
                .resolve(ResolveSkillsRequest {
                    explicit: request.explicit_skills,
                    profile_defaults: policy.default_skills.clone(),
                    current_input: request.input.clone(),
                    max_skills: policy.max_skills,
                })
                .await
                .map_err(|error| ContextFailed {
                    code: "skill_resolution_failed".into(),
                    message: error.to_string(),
                })?;
            let mut hasher = Sha256::new();
            hasher.update(request.input.as_bytes());
            for attachment in &request.attachments {
                hasher.update(attachment.blob_id.to_string().as_bytes());
                hasher.update(attachment.media_type.as_bytes());
            }
            for skill in &resolved_skills {
                hasher.update(skill.locked.skill_id.0.as_bytes());
                hasher.update(skill.locked.version.as_bytes());
                hasher.update(skill.locked.digest.as_bytes());
            }
            let request_digest = format!("sha256:{:x}", hasher.finalize());
            let skill_lock = compile_skills(&resolved_skills).skill_lock;
            let memory_scopes = if policy.memory_enabled {
                let mut scopes = vec![MemoryScope(policy.memory_scope.clone())];
                if let Some(session_id) = request.session_id {
                    scopes.push(MemoryScope(format!("session:{session_id}")));
                }
                scopes
            } else {
                vec![MemoryScope("__memory_disabled__".into())]
            };
            let pack = self
                .context
                .build(ContextBuildRequest {
                    run_id: request.run_id,
                    session_id: request.session_id,
                    through_message_ordinal: request.through_message_ordinal,
                    current_input: request.input,
                    request_digest: request_digest.clone(),
                    agent_profile: policy.agent_profile.clone(),
                    agent_instruction: policy.agent_instruction.clone(),
                    model_profile: policy.model_profile.clone(),
                    resolved_skills,
                    memory_scopes,
                    available_tools: policy.available_tools.clone(),
                    max_memories: policy.max_memories,
                    budget: policy.budget.clone(),
                    policy_version: policy.policy_version,
                    now_ms: unix_time_ms(),
                })
                .await
                .map_err(|error| ContextFailed {
                    code: "context_build_failed".into(),
                    message: error.to_string(),
                })?;
            Ok(PreparedContext {
                pack,
                skill_lock,
                request_digest,
            })
        })
    }
}

/// Temporary P2 bridge. Remove when the P3/P4 operator executor replaces legacy starts.
pub fn legacy_plan(request: ContextRequest, prepared: PreparedContext) -> PlannedRun {
    let pack = prepared.pack;
    let manifest = json!({
        "schema_version": 1, "context_fingerprint": pack.fingerprint, "context_budget": pack.budget,
        "skill_refs": pack.skill_refs, "memory_refs": pack.memory_refs, "artifact_refs": pack.artifact_refs,
        "components": pack.component_descriptors,
        "token_accounting": { "provider_usage_preferred": true, "fallback": "token_estimator" },
        "step_budget": { "max_steps": request.max_steps, "final_summary_step": true },
        "tool_grants": { "static_tools": pack.effective_tools, "allow_run_adf": request.policy.allow_run_adf },
        "tool_runtime": { "script": request.policy.script_runtime, "adf_enabled": request.policy.allow_run_adf },
    });
    PlannedRun {
        run_id: request.run_id,
        input: request.input,
        attachments: request.attachments,
        prior_messages: pack.messages,
        allowed_tools: Some(pack.effective_tools),
        allow_run_adf: request.policy.allow_run_adf,
        max_steps: Some(request.max_steps),
        context_fingerprint: Some(pack.fingerprint),
        execution_manifest: Some(manifest),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AdmissionError {
    #[error("context admission timed out")]
    Timeout,
    #[error(transparent)]
    Fabric(#[from] FabricError),
    #[error(transparent)]
    Preparation(#[from] ContextFailed),
}

pub type PrepareFuture<'scope> = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<PreparedContext, ContextFailed>> + Send + 'scope>,
>;
pub trait ContextPreparer: Send + Sync {
    fn prepare(&self, request: ContextRequest) -> PrepareFuture<'_>;
}
