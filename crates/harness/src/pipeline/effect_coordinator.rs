use crate::approval_operator::ApprovalIngress;
use agent_core::{
    effect_pipeline::*,
    event_runtime::EventId,
    harness::*,
    operator_fabric::*,
    tool_authorization::{ApprovalStatus, BoundApproval},
    tool_operator::{ToolExecutionRecord, ToolExecutionResult, ToolRequest},
    tool_preparation::{PreparationKind, PreparationRequest, PreparationResult},
};
use serde::de::DeserializeOwned;
use serde_json::json;
use std::{collections::BTreeMap, sync::Arc};

pub struct ToolsRunCoordinator {
    pub(crate) fabric: Arc<dyn OperatorFabric>,
    blobs: Arc<dyn BlobStore>,
}

pub(crate) fn invalid(error: impl std::fmt::Display) -> FabricError {
    FabricError::Invalid(error.to_string())
}

pub(crate) async fn read<T: DeserializeOwned>(
    fabric: &dyn OperatorFabric,
    key: String,
) -> Result<(u64, T), FabricError> {
    let FabricReply::Checkpoint(Some(saved)) =
        fabric.execute(FabricCommand::ReadCheckpoint(key)).await?
    else {
        return Err(FabricError::NotFound);
    };
    Ok((
        saved.version,
        serde_json::from_value(saved.value).map_err(invalid)?,
    ))
}

pub(crate) async fn reuse(
    fabric: &dyn OperatorFabric,
    delivery: &ClaimedDelivery,
    now_ms: i64,
) -> Result<bool, FabricError> {
    if !matches!(fabric.execute(FabricCommand::ReadEvent(delivery.event.envelope.event_id)).await?, FabricReply::Event(Some(event)) if event == delivery.event)
    {
        return Err(FabricError::Conflict);
    }
    let FabricReply::Receipt(receipt) = fabric
        .execute(FabricCommand::Receipt(ReceiptKey::for_event(
            &delivery.lease.consumer,
            &delivery.event,
        )))
        .await?
    else {
        return Err(FabricError::Conflict);
    };
    if let Some(receipt) = receipt {
        fabric
            .execute(FabricCommand::Commit {
                lease: delivery.lease.clone(),
                now_ms,
                result: receipt.result,
                outbox: receipt.outbox,
            })
            .await?;
        return Ok(true);
    }
    Ok(false)
}

impl ToolsRunCoordinator {
    pub fn new(fabric: Arc<dyn OperatorFabric>) -> Self {
        Self {
            blobs: Arc::new(agent_extension::pipeline::FabricBlobStore(fabric.clone())),
            fabric,
        }
    }

    pub fn with_blobs(mut self, blobs: Arc<dyn BlobStore>) -> Self {
        self.blobs = blobs;
        self
    }

    pub async fn load(&self, tenant: &str, run_id: RunId) -> Result<EffectRun, FabricError> {
        Ok(read(self.fabric.as_ref(), run_key(tenant, run_id)).await?.1)
    }

    pub async fn admit(
        &self,
        tenant: &str,
        start: AgentLoopStart,
        now_ms: i64,
    ) -> Result<(), FabricError> {
        self.admit_outcome(tenant, start, now_ms, None).await
    }

    pub async fn admit_outcome(
        &self,
        tenant: &str,
        start: AgentLoopStart,
        now_ms: i64,
        failure: Option<&str>,
    ) -> Result<(), FabricError> {
        if tenant.trim().is_empty() {
            return Err(invalid("tenant required"));
        }
        let key = run_key(tenant, start.run_id);
        loop {
            match read::<EffectRun>(self.fabric.as_ref(), key.clone()).await {
                Ok((_, previous)) => {
                    return if previous.start == start
                        && previous.profile == TOOLS_PROFILE
                        && previous.admission_failure.as_deref() == failure
                    {
                        Ok(())
                    } else {
                        Err(FabricError::Conflict)
                    };
                }
                Err(FabricError::NotFound) => {}
                Err(error) => return Err(error),
            }
            let mut transition = AgentLoopReducer::start(start.clone());
            if let Some(code) = failure {
                transition = AgentLoopReducer::dispatch(
                    transition.state,
                    AgentLoopInput::RuntimeFailed {
                        error: MachineError {
                            code: code.into(),
                            message: "context preparation failed".into(),
                            retryable: false,
                        },
                    },
                );
            }
            let mut record = EffectRun {
                tenant_id: tenant.into(),
                profile: TOOLS_PROFILE.into(),
                start: start.clone(),
                reducer: transition.state,
                public_events: vec![RunEvent::new(start.run_id, 1, RunEventKind::RunStarted)],
                routes: BTreeMap::new(),
                toolset_effect: None,
                validated: BTreeMap::new(),
                approvals: BTreeMap::new(),
                wait: None,
                output_through: BTreeMap::new(),
                admission_failure: failure.map(str::to_owned),
            };
            let (mut checkpoints, mut outbox) =
                self.route(&mut record, transition.effects, now_ms).await?;
            if let Some(code) = failure {
                record.public_events.push(RunEvent::new(
                    start.run_id,
                    2,
                    RunEventKind::RunFailed {
                        code: code.into(),
                        message: "context preparation failed".into(),
                        retryable: false,
                    },
                ));
                let mut route = control_route(tenant, start.run_id);
                route.event_type = "run.failed".into();
                outbox.push(fact(
                    tenant,
                    start.run_id,
                    "context-failure",
                    &route,
                    json!({"code":code}),
                    agent_core::pipeline::payload_schema("run.failed"),
                    now_ms,
                ));
            }
            checkpoints.push(CheckpointWrite {
                key: key.clone(),
                expected: None,
                value: json!(record),
            });
            match self
                .fabric
                .execute(FabricCommand::TransactMany {
                    checkpoints,
                    outbox,
                    delivery: None,
                })
                .await
            {
                Err(FabricError::Fenced) => continue,
                result => return result.map(|_| ()),
            }
        }
    }

    async fn route(
        &self,
        record: &mut EffectRun,
        effects: Vec<AgentLoopEffect>,
        now_ms: i64,
    ) -> Result<(Vec<CheckpointWrite>, Vec<FabricEvent>), FabricError> {
        let mut checkpoints = Vec::new();
        let mut outbox = Vec::new();
        for effect in effects {
            if record.routes.contains_key(&effect.effect_id) {
                continue;
            }
            let identity = EventId::stable(
                "effect-topic",
                &serde_json::to_string(&(
                    &record.tenant_id,
                    record.start.run_id,
                    &effect.effect_id,
                ))
                .map_err(invalid)?,
            );
            let mut route = EffectRoute {
                topic: format!("agent.effect.{identity}"),
                event_type: String::new(),
                consumer: String::new(),
            };
            let request = match effect.kind {
                AgentLoopEffectKind::LoadToolSet { .. } => {
                    record.toolset_effect = Some(effect.effect_id.clone());
                    record.validated.clear();
                    record.approvals.clear();
                    Some(PreparationRequest {
                        tenant_id: record.tenant_id.clone(),
                        run_id: record.start.run_id,
                        effect_id: effect.effect_id.clone(),
                        kind: PreparationKind::ToolSet,
                    })
                }
                AgentLoopEffectKind::ValidateTools {
                    tool_set_revision,
                    calls,
                    ..
                } => Some(PreparationRequest {
                    tenant_id: record.tenant_id.clone(),
                    run_id: record.start.run_id,
                    effect_id: effect.effect_id.clone(),
                    kind: PreparationKind::Validate {
                        toolset_effect_id: record
                            .toolset_effect
                            .clone()
                            .ok_or(FabricError::Conflict)?,
                        revision: tool_set_revision,
                        calls,
                        allowed_tools: record.reducer.config.allowed_tools.clone(),
                        allow_run_adf: record.reducer.config.allow_run_adf,
                    },
                }),
                AgentLoopEffectKind::RequestApproval {
                    approval_id, call, ..
                } => {
                    let validated = record
                        .validated
                        .get(&call.call_id)
                        .ok_or(FabricError::Conflict)?
                        .clone();
                    if validated.invocation != call {
                        return Err(FabricError::Conflict);
                    }
                    let approval = BoundApproval::for_reducer(
                        record.start.run_id,
                        &effect.effect_id,
                        validated,
                        now_ms,
                        now_ms.saturating_add(300_000),
                        record.reducer.next_step,
                    )
                    .map_err(invalid)?;
                    if approval.approval_id != approval_id {
                        return Err(FabricError::Conflict);
                    }
                    let (write, event) =
                        ApprovalIngress::pending_request(&record.tenant_id, &approval)?;
                    route.topic = event.topic.clone();
                    route.event_type = "approval.requested".into();
                    route.consumer = "approval".into();
                    record.approvals.insert(call.call_id, approval);
                    checkpoints.push(write);
                    outbox.push(event);
                    None
                }
                AgentLoopEffectKind::InvokeTool {
                    call,
                    tool_set_revision,
                    timeout_ms,
                    ..
                } => {
                    let validated = record
                        .validated
                        .get(&call.call_id)
                        .ok_or(FabricError::Conflict)?
                        .clone();
                    if validated.invocation != call
                        || validated.tool_set_revision != tool_set_revision
                    {
                        return Err(FabricError::Conflict);
                    }
                    let request = ToolRequest {
                        tenant_id: record.tenant_id.clone(),
                        run_id: record.start.run_id,
                        effect_id: effect.effect_id.clone(),
                        validated,
                        timeout_ms,
                        approval: record.approvals.get(&call.call_id).cloned(),
                        approval_policy: record.reducer.config.approval_policy,
                    };
                    route.event_type = "tool.requested".into();
                    route.consumer = "tool".into();
                    outbox.push(fact(
                        &record.tenant_id,
                        record.start.run_id,
                        &effect.effect_id,
                        &route,
                        json!({"request_key":request.checkpoint_key()}),
                        agent_core::tool_operator::payload_schema("tool.requested"),
                        now_ms,
                    ));
                    checkpoints.push(CheckpointWrite {
                        key: request.checkpoint_key(),
                        expected: None,
                        value: json!(request),
                    });
                    None
                }
                AgentLoopEffectKind::InvokeModel {
                    request,
                    timeout_ms,
                    ..
                } => {
                    let key = model_key(&record.tenant_id, record.start.run_id, &effect.effect_id);
                    let request = EffectModelRequest {
                        tenant_id: record.tenant_id.clone(),
                        run_id: record.start.run_id,
                        effect_id: effect.effect_id.clone(),
                        request_ref: super::put(self.blobs.as_ref(), &request)
                            .await
                            .map_err(invalid)?,
                        request,
                        timeout_ms,
                    };
                    route.event_type = "model.requested".into();
                    route.consumer = "model".into();
                    outbox.push(fact(
                        &record.tenant_id,
                        record.start.run_id,
                        &effect.effect_id,
                        &route,
                        json!({"request_ref":request.request_ref}),
                        agent_core::pipeline::payload_schema("model.requested"),
                        now_ms,
                    ));
                    checkpoints.push(CheckpointWrite {
                        key,
                        expected: None,
                        value: json!(request),
                    });
                    None
                }
            };
            if let Some(request) = request {
                route.event_type = request.event_type().into();
                route.consumer = "preparation".into();
                outbox.push(fact(
                    &record.tenant_id,
                    record.start.run_id,
                    &effect.effect_id,
                    &route,
                    json!({"request_key":request.checkpoint_key()}),
                    agent_core::tool_preparation::payload_schema(request.event_type()),
                    now_ms,
                ));
                checkpoints.push(CheckpointWrite {
                    key: request.checkpoint_key(),
                    expected: None,
                    value: json!(request),
                });
            }
            record.routes.insert(effect.effect_id, route);
        }
        Ok((checkpoints, outbox))
    }

    pub async fn consume(&self, delivery: ClaimedDelivery, now_ms: i64) -> Result<(), FabricError> {
        if reuse(self.fabric.as_ref(), &delivery, now_ms).await? {
            return Ok(());
        }
        let envelope = &delivery.event.envelope;
        loop {
            let (version, mut record): (_, EffectRun) = read(
                self.fabric.as_ref(),
                run_key(&envelope.tenant_id, envelope.run_id),
            )
            .await?;
            let pending_before = record.reducer.pending_effects();
            let wait_before = record.wait.clone();
            let pending = pending_before
                .iter()
                .find(|effect| effect.effect_id == envelope.effect_id)
                .cloned();
            let inputs = if envelope.event_type == "run.timeout.requested"
                && matches!(
                    record.reducer.outcome(),
                    AgentLoopOutcome::Running | AgentLoopOutcome::Suspended { .. }
                ) {
                let key = format!("{}:deadline", run_key(&record.tenant_id, envelope.run_id));
                if envelope.payload["checkpoint_key"].as_str() != Some(&key)
                    || envelope.operator.name != "timer"
                {
                    return Err(FabricError::Conflict);
                }
                let (_, deadline): (_, i64) = read(self.fabric.as_ref(), key).await?;
                if now_ms < deadline || envelope.occurred_at_ms != deadline {
                    return Err(FabricError::Conflict);
                }
                vec![AgentLoopInput::RuntimeFailed {
                    error: MachineError {
                        code: "run_timeout".into(),
                        message: "run exceeded its durable deadline".into(),
                        retryable: false,
                    },
                }]
            } else if envelope.event_type == "run.cancel.requested"
                && matches!(
                    record.reducer.outcome(),
                    AgentLoopOutcome::Running | AgentLoopOutcome::Suspended { .. }
                )
            {
                let key = format!("{}:cancel", run_key(&record.tenant_id, envelope.run_id));
                if envelope.payload["checkpoint_key"].as_str() != Some(&key) {
                    return Err(FabricError::Conflict);
                }
                let (_, requested): (_, RunCancelRequest) = read(self.fabric.as_ref(), key).await?;
                if requested.tenant_id != record.tenant_id
                    || requested.run_id != envelope.run_id
                    || requested.actor.trim().is_empty()
                {
                    return Err(FabricError::Conflict);
                }
                vec![AgentLoopInput::Cancelled]
            } else if envelope.event_type == "wait.resolved"
                && record
                    .wait
                    .as_ref()
                    .is_some_and(|wait| wait.effect_id == envelope.effect_id)
                && matches!(record.reducer.outcome(), AgentLoopOutcome::Suspended { .. })
            {
                let wait = record.wait.take().ok_or(FabricError::Conflict)?;
                let key = format!("{}:wake", wait.key());
                if envelope.payload["checkpoint_key"].as_str() != Some(&key) {
                    return Err(FabricError::Conflict);
                }
                let (_, event): (_, agent_core::event_runtime::EventEnvelope) =
                    read(self.fabric.as_ref(), key).await?;
                let content = resumed_tool_content(&[FlowInboxItem {
                    event,
                    consumed_revision: None,
                }])
                .map_err(|error| invalid(error.message))?;
                record.public_events.push(RunEvent::new(
                    envelope.run_id,
                    record.public_events.len() as u64 + 1,
                    RunEventKind::RunResumed {
                        activation_id: ActivationId::stable("effect-wake", &wait.key()),
                        checkpoint_revision: version,
                    },
                ));
                vec![AgentLoopInput::ToolResumed {
                    call_id: wait.call_id,
                    content,
                }]
            } else if let Some(effect) = pending {
                self.inputs(&mut record, &delivery, &effect).await?
            } else {
                vec![]
            };
            if inputs.is_empty() {
                self.fabric
                    .execute(FabricCommand::Commit {
                        lease: delivery.lease,
                        now_ms,
                        result: json!({"ignored":true}),
                        outbox: vec![],
                    })
                    .await?;
                return Ok(());
            }
            let mut effects = Vec::new();
            for input in inputs {
                let transition = AgentLoopReducer::dispatch(record.reducer, input);
                record.reducer = transition.state;
                for event in transition.events {
                    record.public_events.push(RunEvent::new(
                        envelope.run_id,
                        record.public_events.len() as u64 + 1,
                        event.into_run_event_kind(),
                    ));
                }
                effects.extend(transition.effects);
            }
            let (mut checkpoints, mut outbox) = self.route(&mut record, effects, now_ms).await?;
            if let Some(wait) = wait_before
                && !matches!(record.reducer.outcome(), AgentLoopOutcome::Suspended { .. })
            {
                let mut route = wait.route.clone();
                route.event_type = "wait.cancel.requested".into();
                outbox.push(fact(
                    &record.tenant_id,
                    envelope.run_id,
                    &wait.effect_id,
                    &route,
                    json!({"checkpoint_key":wait.key()}),
                    reference_schema(),
                    now_ms,
                ));
                record.wait = None;
            }
            if let AgentLoopOutcome::Suspended { mut waits, effects } = record.reducer.outcome()
                && record.wait.is_none()
            {
                for wait in &mut waits {
                    if wait.subscription.start_position
                        == agent_core::event_runtime::StartPosition::Now
                    {
                        wait.subscription.start_position =
                            agent_core::event_runtime::StartPosition::RecordedSince {
                                recorded_at_ms: wait.subscription.created_at_ms,
                            };
                    }
                }
                let effect_id = format!("{}:wait", envelope.effect_id);
                let route = EffectRoute {
                    topic: format!(
                        "agent.wait.{}",
                        EventId::stable(
                            "wait-topic",
                            &format!(
                                "{}:{effect_id}",
                                run_key(&record.tenant_id, envelope.run_id)
                            )
                        )
                    ),
                    event_type: "wait.register.requested".into(),
                    consumer: "wait".into(),
                };
                let request = EffectWaitRequest {
                    tenant_id: record.tenant_id.clone(),
                    run_id: envelope.run_id,
                    effect_id: effect_id.clone(),
                    call_id: record
                        .reducer
                        .pending_tool_resume_call_id()
                        .ok_or(FabricError::Conflict)?
                        .into(),
                    waits,
                    effects,
                    route: route.clone(),
                };
                super::effect_wait::validate_new(&request)?;
                checkpoints.push(CheckpointWrite {
                    key: request.key(),
                    expected: None,
                    value: json!(request),
                });
                for wait in &request.waits {
                    checkpoints.push(CheckpointWrite {
                        key: wait_binding_key(wait.subscription.subscription_id),
                        expected: None,
                        value: json!(request),
                    });
                }
                outbox.push(fact(
                    &record.tenant_id,
                    envelope.run_id,
                    &effect_id,
                    &route,
                    json!({"checkpoint_key":request.key()}),
                    reference_schema(),
                    now_ms,
                ));
                record.routes.insert(effect_id, route);
                record.wait = Some(request);
                record.public_events.push(RunEvent::new(
                    envelope.run_id,
                    record.public_events.len() as u64 + 1,
                    RunEventKind::RunWaiting {
                        checkpoint_revision: version + 1,
                        wait_count: record.wait.as_ref().expect("wait").waits.len() as u32,
                    },
                ));
            }
            if matches!(
                record.reducer.outcome(),
                AgentLoopOutcome::Cancelled | AgentLoopOutcome::Failed { .. }
            ) {
                for effect in pending_before {
                    let mut route = record
                        .routes
                        .get(&effect.effect_id)
                        .ok_or(FabricError::Conflict)?
                        .clone();
                    match effect.kind {
                        AgentLoopEffectKind::RequestApproval { approval_id, .. } => {
                            if let Some((write, event)) = ApprovalIngress::new(self.fabric.clone())
                                .cancellation_write(
                                    &record.tenant_id,
                                    envelope.run_id,
                                    approval_id,
                                    now_ms,
                                )
                                .await?
                            {
                                checkpoints.push(write);
                                outbox.push(event);
                            }
                        }
                        AgentLoopEffectKind::InvokeTool { .. } => {
                            let (_, request): (_, ToolRequest) = read(
                                self.fabric.as_ref(),
                                agent_core::tool_operator::request_key(
                                    &record.tenant_id,
                                    envelope.run_id,
                                    &effect.effect_id,
                                ),
                            )
                            .await?;
                            route.event_type = "tool.cancel.requested".into();
                            outbox.push(fact(&record.tenant_id, envelope.run_id, &effect.effect_id, &route, json!({"result_key":request.execution_key(),"call_id":request.validated.invocation.call_id,"code":"run_cancelled"}), agent_core::tool_operator::payload_schema("tool.cancel.requested"), now_ms));
                        }
                        AgentLoopEffectKind::InvokeModel { .. } => {
                            route.event_type = "model.cancel.requested".into();
                            outbox.push(fact(&record.tenant_id, envelope.run_id, &effect.effect_id, &route, json!({"checkpoint_key":format!("{}:result", model_key(&record.tenant_id, envelope.run_id, &effect.effect_id))}), reference_schema(), now_ms));
                        }
                        _ => {}
                    }
                }
            }
            let terminal = match record.reducer.outcome() {
                AgentLoopOutcome::Complete { finish_reason } => Some((
                    "run.completed",
                    RunEventKind::RunCompleted { finish_reason },
                )),
                AgentLoopOutcome::Failed { error } => Some((
                    "run.failed",
                    RunEventKind::RunFailed {
                        code: error.code,
                        message: error.message,
                        retryable: error.retryable,
                    },
                )),
                AgentLoopOutcome::Cancelled => Some(("run.cancelled", RunEventKind::RunCancelled)),
                _ => None,
            };
            if let Some((kind, terminal)) = terminal {
                record.public_events.push(RunEvent::new(
                    envelope.run_id,
                    record.public_events.len() as u64 + 1,
                    terminal,
                ));
                outbox.push(fact(
                    &record.tenant_id,
                    envelope.run_id,
                    &envelope.effect_id,
                    &EffectRoute {
                        topic: delivery.event.topic.clone(),
                        event_type: kind.into(),
                        consumer: "coordinator".into(),
                    },
                    json!({"checkpoint_key":run_key(&record.tenant_id, envelope.run_id)}),
                    reference_schema(),
                    now_ms,
                ));
            }
            checkpoints.push(CheckpointWrite {
                key: run_key(&record.tenant_id, envelope.run_id),
                expected: Some(version),
                value: json!(record),
            });
            match self
                .fabric
                .execute(FabricCommand::TransactMany {
                    checkpoints,
                    outbox,
                    delivery: Some((delivery.lease.clone(), now_ms)),
                })
                .await
            {
                Err(FabricError::Fenced) => {
                    if reuse(self.fabric.as_ref(), &delivery, now_ms).await? {
                        return Ok(());
                    }
                    let (current, _): (_, EffectRun) = read(
                        self.fabric.as_ref(),
                        run_key(&envelope.tenant_id, envelope.run_id),
                    )
                    .await?;
                    if current == version {
                        return Err(FabricError::Fenced);
                    }
                }
                result => return result.map(|_| ()),
            }
        }
    }

    async fn inputs(
        &self,
        record: &mut EffectRun,
        delivery: &ClaimedDelivery,
        effect: &AgentLoopEffect,
    ) -> Result<Vec<AgentLoopInput>, FabricError> {
        let envelope = &delivery.event.envelope;
        let effect_id = effect.effect_id.clone();
        let kind = envelope.event_type.as_str();
        match (&effect.kind, kind) {
            (
                AgentLoopEffectKind::LoadToolSet { .. } | AgentLoopEffectKind::ValidateTools { .. },
                "toolset.completed"
                | "toolset.failed"
                | "tool.validation.completed"
                | "tool.validation.failed",
            ) => {
                let key = format!(
                    "{}:result",
                    agent_core::tool_preparation::request_key(
                        &record.tenant_id,
                        envelope.run_id,
                        &effect_id
                    )
                );
                if envelope.payload["result_key"].as_str() != Some(&key) {
                    return Err(FabricError::Conflict);
                }
                let (_, result): (_, PreparationResult) = read(self.fabric.as_ref(), key).await?;
                match (&effect.kind, result) {
                    (
                        AgentLoopEffectKind::LoadToolSet { .. },
                        PreparationResult::ToolSet { snapshot },
                    ) => Ok(vec![AgentLoopInput::ToolSetLoaded {
                        effect_id,
                        tool_set: snapshot,
                    }]),
                    (
                        AgentLoopEffectKind::LoadToolSet { .. },
                        PreparationResult::Failed { code },
                    ) => Ok(vec![AgentLoopInput::ToolSetFailed {
                        effect_id,
                        error: MachineError {
                            message: code.clone(),
                            code,
                            retryable: false,
                        },
                    }]),
                    (AgentLoopEffectKind::ValidateTools { calls, .. }, result) => {
                        let error = match result {
                            PreparationResult::Validated { calls: validated } => {
                                for call in validated {
                                    record
                                        .validated
                                        .insert(call.invocation.call_id.clone(), call);
                                }
                                None
                            }
                            PreparationResult::Failed { code } => Some(PortableToolError {
                                message: code.clone(),
                                code,
                                category: ToolErrorCategory::InvalidRequest,
                                retryable: false,
                                retry_after_ms: None,
                            }),
                            _ => return Err(FabricError::Conflict),
                        };
                        Ok(vec![AgentLoopInput::ToolsValidated {
                            effect_id,
                            results: calls
                                .iter()
                                .map(|call| ToolValidation {
                                    call_id: call.call_id.clone(),
                                    error: error.clone(),
                                })
                                .collect(),
                        }])
                    }
                    _ => Err(FabricError::Conflict),
                }
            }
            (
                AgentLoopEffectKind::RequestApproval {
                    approval_id, call, ..
                },
                "approval.resolved" | "approval.expired" | "approval.cancelled",
            ) => {
                let approval = ApprovalIngress::new(self.fabric.clone())
                    .load(&record.tenant_id, envelope.run_id, *approval_id)
                    .await?;
                if approval.effect_id != effect_id {
                    return Err(FabricError::Conflict);
                }
                let resolution = match &approval.status {
                    ApprovalStatus::Resolved { resolution, .. } => resolution.clone(),
                    ApprovalStatus::Expired | ApprovalStatus::Cancelled => ApprovalResolution {
                        decision: ApprovalDecision::Deny,
                        reason: Some(kind.into()),
                    },
                    _ => return Err(FabricError::Conflict),
                };
                record.approvals.insert(call.call_id.clone(), approval);
                Ok(vec![AgentLoopInput::ApprovalResolved {
                    effect_id,
                    resolution,
                }])
            }
            (
                AgentLoopEffectKind::InvokeTool { call, .. },
                "tool.completed"
                | "tool.failed"
                | "tool.suspended"
                | "tool.cancelled"
                | "operator.execution.uncertain",
            ) => {
                let key = agent_core::tool_operator::request_key(
                    &record.tenant_id,
                    envelope.run_id,
                    &effect_id,
                );
                let (_, request): (_, ToolRequest) = read(self.fabric.as_ref(), key).await?;
                if request.validated.invocation != *call
                    || envelope.payload["result_key"].as_str() != Some(&request.execution_key())
                {
                    return Err(FabricError::Conflict);
                }
                let (_, execution): (_, ToolExecutionRecord) =
                    read(self.fabric.as_ref(), request.execution_key()).await?;
                let result = match execution.result.ok_or(FabricError::Conflict)? {
                    ToolExecutionResult::Finished { result } => result,
                    ToolExecutionResult::Uncertain { code } => ToolInvocationResult::Failed {
                        error: PortableToolError {
                            message: code.clone(),
                            code,
                            category: ToolErrorCategory::Unavailable,
                            retryable: false,
                            retry_after_ms: None,
                        },
                    },
                    ToolExecutionResult::Cancelled => ToolInvocationResult::Failed {
                        error: PortableToolError {
                            message: "tool_cancelled".into(),
                            code: "tool_cancelled".into(),
                            category: ToolErrorCategory::Unavailable,
                            retryable: false,
                            retry_after_ms: None,
                        },
                    },
                };
                Ok(vec![AgentLoopInput::ToolFinished { effect_id, result }])
            }
            (AgentLoopEffectKind::InvokeModel { .. }, "output.failed") => {
                let (_, failure): (_, serde_json::Value) = read(
                    self.fabric.as_ref(),
                    format!(
                        "{}:failed",
                        output_key(&record.tenant_id, envelope.run_id, &effect_id)
                    ),
                )
                .await?;
                if failure != envelope.payload {
                    return Err(FabricError::Conflict);
                }
                Ok(vec![AgentLoopInput::RuntimeFailed {
                    error: MachineError {
                        code: "output_gap".into(),
                        message: "model output coverage could not be verified".into(),
                        retryable: false,
                    },
                }])
            }
            (AgentLoopEffectKind::InvokeModel { .. }, "output.batch") => {
                use agent_core::pipeline::*;
                let batch: OutputBatch =
                    serde_json::from_value(envelope.payload.clone()).map_err(invalid)?;
                let (_, output): (_, EffectOutputState) = read(
                    self.fabric.as_ref(),
                    output_key(&record.tenant_id, envelope.run_id, &effect_id),
                )
                .await?;
                if !output.batches.contains(&batch) {
                    return Err(FabricError::Conflict);
                }
                let through = record.output_through.get(&effect_id).copied().unwrap_or(0);
                if batch.last_seq <= through {
                    return Ok(vec![]);
                }
                if batch.first_seq != through + 1 {
                    return Err(FabricError::Conflict);
                }
                let stored: Vec<OutputSegment> =
                    super::get(self.blobs.as_ref(), batch.artifact_ref)
                        .await
                        .map_err(invalid)?;
                if stored != batch.segments {
                    return Err(FabricError::Conflict);
                }
                let inputs = stored
                    .iter()
                    .map(|segment| {
                        delta_event(&DeltaItem {
                            invocation_id: effect_id.clone(),
                            delta_seq: segment.first_seq,
                            channel: segment.channel.clone(),
                            fragment: segment.text.clone(),
                        })
                        .map(|event| AgentLoopInput::ModelEvent {
                            effect_id: effect_id.clone(),
                            event,
                        })
                        .map_err(invalid)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                record.output_through.insert(effect_id, batch.last_seq);
                Ok(inputs)
            }
            (AgentLoopEffectKind::InvokeModel { .. }, "output.sealed") => {
                let key = format!(
                    "{}:result",
                    model_key(&record.tenant_id, envelope.run_id, &effect_id)
                );
                let (_, execution): (_, EffectModelExecution) =
                    read(self.fabric.as_ref(), key).await?;
                let reference = execution.final_ref.ok_or(FabricError::Conflict)?;
                let (_, output): (_, EffectOutputState) = read(
                    self.fabric.as_ref(),
                    output_key(&record.tenant_id, envelope.run_id, &effect_id),
                )
                .await?;
                if !output.sealed
                    || output.durable_through != reference.final_delta_seq
                    || output.coverage_digest != reference.coverage_digest
                    || record.output_through.get(&effect_id).copied().unwrap_or(0)
                        != reference.final_delta_seq
                    || envelope.payload
                        != json!({"final_delta_seq":reference.final_delta_seq,"coverage_digest":reference.coverage_digest})
                {
                    return Err(FabricError::Conflict);
                }
                let event = execution
                    .events
                    .and_then(|events| events.last().cloned())
                    .ok_or(FabricError::Conflict)?;
                Ok(vec![AgentLoopInput::ModelEvent { effect_id, event }])
            }
            _ => Ok(vec![]),
        }
    }
}
