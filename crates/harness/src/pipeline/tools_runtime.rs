use super::{
    effect_coordinator::ToolsRunCoordinator, effect_model::EffectModelOperator,
    effect_output::EffectOutputOperator,
};
use crate::{tool_operator::ToolOperator, tool_preparation::ToolPreparationOperator};
use agent_core::{effect_pipeline::*, event_runtime::EventId, harness::*, operator_fabric::*};
use futures_util::future::try_join_all;
use std::sync::Arc;

pub struct ToolsPipelineRuntime {
    fabric: Arc<dyn OperatorFabric>,
    model: Arc<dyn ModelPort>,
    tools: Arc<dyn ToolPort>,
    wait: Option<super::EffectWaitBridge>,
    blobs: Arc<dyn BlobStore>,
    runs: Option<Arc<dyn RunStore>>,
}

impl ToolsPipelineRuntime {
    pub async fn ensure_deadline(
        &self,
        tenant: &str,
        run_id: RunId,
        deadline_ms: i64,
    ) -> Result<i64, FabricError> {
        self.load(tenant, run_id).await?;
        let key = format!("{}:deadline", run_key(tenant, run_id));
        match self
            .fabric
            .execute(FabricCommand::Transact {
                checkpoint: CheckpointWrite {
                    key: key.clone(),
                    expected: None,
                    value: serde_json::json!(deadline_ms),
                },
                outbox: vec![],
                delivery: None,
            })
            .await
        {
            Ok(_) => Ok(deadline_ms),
            Err(FabricError::Fenced) => {
                let (_, stored) =
                    super::effect_coordinator::read(self.fabric.as_ref(), key).await?;
                Ok(stored)
            }
            Err(error) => Err(error),
        }
    }

    pub async fn reconcile_control(
        &self,
        tenant: &str,
        run_id: RunId,
        now_ms: i64,
    ) -> Result<(), FabricError> {
        let record = self.load(tenant, run_id).await?;
        self.wire(&record).await?;
        let key = format!("{}:deadline", run_key(tenant, run_id));
        if let FabricReply::Checkpoint(Some(saved)) = self
            .fabric
            .execute(FabricCommand::ReadCheckpoint(key.clone()))
            .await?
            && saved
                .value
                .as_i64()
                .is_some_and(|deadline| deadline <= now_ms)
            && matches!(
                record.reducer.outcome(),
                AgentLoopOutcome::Running | AgentLoopOutcome::Suspended { .. }
            )
        {
            let mut route = control_route(tenant, run_id);
            route.event_type = "run.timeout.requested".into();
            let mut event = fact(
                tenant,
                run_id,
                "run-deadline",
                &route,
                serde_json::json!({"checkpoint_key":key}),
                reference_schema(),
                saved.value.as_i64().ok_or(FabricError::Conflict)?,
            );
            event.envelope.operator.name = "timer".into();
            match self
                .fabric
                .execute(FabricCommand::Transact {
                    checkpoint: CheckpointWrite {
                        key: format!("{key}:fired"),
                        expected: None,
                        value: serde_json::json!(true),
                    },
                    outbox: vec![event],
                    delivery: None,
                })
                .await
            {
                Ok(_) | Err(FabricError::Fenced) => {}
                Err(error) => return Err(error),
            }
        }
        self.drain_coordinator(tenant, run_id, &|| now_ms).await?;
        self.fabric
            .execute(FabricCommand::DispatchOutbox { limit: 128 })
            .await?;
        let FabricReply::Claimed(deliveries) = self
            .fabric
            .execute(FabricCommand::ClaimForConsumer {
                consumer: Self::consumer(tenant, run_id, "cancel"),
                worker: format!("control-worker-{}", std::process::id()),
                now_ms,
                lease_ms: 120_000,
                limit: 128,
                max_attempts: 10,
            })
            .await?
        else {
            return Err(FabricError::Conflict);
        };
        self.cancel_deliveries(deliveries, &|| now_ms).await?;
        self.project(tenant, run_id).await
    }

    async fn cancel_deliveries(
        &self,
        deliveries: Vec<ClaimedDelivery>,
        clock: &impl Fn() -> i64,
    ) -> Result<(), FabricError> {
        for delivery in deliveries {
            if delivery.event.envelope.event_type == "wait.cancel.requested" {
                self.wait
                    .as_ref()
                    .ok_or_else(|| FabricError::Invalid("wait ports required".into()))?
                    .execute(delivery, clock())
                    .await?;
            } else if delivery.event.envelope.event_type == "tool.cancel.requested" {
                ToolOperator::new(self.fabric.clone(), self.tools.clone())
                    .cancel_requested(delivery, clock())
                    .await?;
            } else {
                EffectModelOperator {
                    fabric: self.fabric.clone(),
                    model: self.model.clone(),
                    blobs: self.blobs.clone(),
                }
                .cancel_requested(delivery, clock())
                .await?;
            }
        }
        Ok(())
    }
    pub fn with_blobs(mut self, blobs: Arc<dyn BlobStore>) -> Self {
        self.blobs = blobs;
        self
    }
    pub fn with_projection(mut self, runs: Arc<dyn RunStore>) -> Self {
        self.runs = Some(runs);
        self
    }
    async fn project(&self, tenant: &str, run_id: RunId) -> Result<(), FabricError> {
        if let Some(runs) = &self.runs {
            super::RunEventProjector::append(
                runs.as_ref(),
                self.load(tenant, run_id).await?.public_events,
            )
            .await
            .map_err(super::effect_coordinator::invalid)?;
        }
        Ok(())
    }
    pub fn with_wait_ports(
        mut self,
        events: Arc<dyn agent_core::event_runtime::EventStore>,
        jobs: Arc<dyn JobStore>,
    ) -> Self {
        self.wait = Some(super::EffectWaitBridge::new(
            self.fabric.clone(),
            events,
            jobs,
        ));
        self
    }
    pub async fn request_cancel(
        &self,
        tenant: &str,
        run_id: RunId,
        actor: &str,
        now_ms: i64,
    ) -> Result<(), FabricError> {
        if actor.trim().is_empty() {
            return Err(FabricError::Invalid("actor required".into()));
        }
        self.load(tenant, run_id).await?;
        let key = format!("{}:cancel", run_key(tenant, run_id));
        let request = RunCancelRequest {
            tenant_id: tenant.into(),
            run_id,
            actor: actor.into(),
            requested_at_ms: now_ms,
        };
        let mut event = fact(
            tenant,
            run_id,
            "run-control",
            &control_route(tenant, run_id),
            serde_json::json!({"checkpoint_key":key}),
            reference_schema(),
            now_ms,
        );
        event.envelope.operator.name = "gateway".into();
        match self
            .fabric
            .execute(FabricCommand::Transact {
                checkpoint: CheckpointWrite {
                    key: key.clone(),
                    expected: None,
                    value: serde_json::json!(request),
                },
                outbox: vec![event],
                delivery: None,
            })
            .await
        {
            Err(FabricError::Fenced) => {
                let (_, stored): (_, RunCancelRequest) =
                    super::effect_coordinator::read(self.fabric.as_ref(), key).await?;
                if stored.tenant_id != tenant || stored.run_id != run_id {
                    return Err(FabricError::Conflict);
                }
                Ok(())
            }
            result => result.map(|_| ()),
        }
    }
    fn consumer(tenant: &str, run_id: RunId, role: &str) -> String {
        format!(
            "tools-pipeline:{role}:{}",
            EventId::stable(
                "pipeline-consumer",
                &serde_json::to_string(&(tenant, run_id)).expect("identity")
            )
        )
    }
    pub fn new(
        fabric: Arc<dyn OperatorFabric>,
        model: Arc<dyn ModelPort>,
        tools: Arc<dyn ToolPort>,
    ) -> Self {
        Self {
            blobs: Arc::new(agent_extension::pipeline::FabricBlobStore(fabric.clone())),
            runs: None,
            fabric,
            model,
            tools,
            wait: None,
        }
    }

    pub async fn admit(
        &self,
        tenant: &str,
        start: AgentLoopStart,
        now_ms: i64,
    ) -> Result<(), FabricError> {
        ToolsRunCoordinator::new(self.fabric.clone())
            .with_blobs(self.blobs.clone())
            .admit(tenant, start, now_ms)
            .await
    }

    pub async fn fail_admission(
        &self,
        tenant: &str,
        start: AgentLoopStart,
        now_ms: i64,
        code: &str,
    ) -> Result<(), FabricError> {
        ToolsRunCoordinator::new(self.fabric.clone())
            .with_blobs(self.blobs.clone())
            .admit_outcome(tenant, start, now_ms, Some(code))
            .await
    }

    pub async fn load(&self, tenant: &str, run_id: RunId) -> Result<EffectRun, FabricError> {
        ToolsRunCoordinator::new(self.fabric.clone())
            .with_blobs(self.blobs.clone())
            .load(tenant, run_id)
            .await
    }

    async fn wire(&self, record: &EffectRun) -> Result<(), FabricError> {
        let control = control_route(&record.tenant_id, record.start.run_id);
        self.fabric
            .execute(FabricCommand::Subscribe(SubscriptionSpec {
                consumer: Self::consumer(&record.tenant_id, record.start.run_id, "coordinator"),
                subscription: control.topic.clone(),
                topic: control.topic,
                event_type: None,
                tenant: Some(record.tenant_id.clone()),
            }))
            .await?;
        for route in record.routes.values() {
            for (consumer, event_type) in [
                ("coordinator", None),
                (route.consumer.as_str(), Some(route.event_type.clone())),
            ] {
                if consumer == "approval" {
                    continue;
                }
                self.fabric
                    .execute(FabricCommand::Subscribe(SubscriptionSpec {
                        consumer: Self::consumer(&record.tenant_id, record.start.run_id, consumer),
                        subscription: route.topic.clone(),
                        topic: route.topic.clone(),
                        event_type,
                        tenant: Some(record.tenant_id.clone()),
                    }))
                    .await?;
            }
            if route.consumer == "model" {
                for kind in ["model.completed", "model.failed"] {
                    self.fabric
                        .execute(FabricCommand::Subscribe(SubscriptionSpec {
                            consumer: Self::consumer(
                                &record.tenant_id,
                                record.start.run_id,
                                "output",
                            ),
                            subscription: format!("{}:{kind}", route.topic),
                            topic: route.topic.clone(),
                            event_type: Some(kind.into()),
                            tenant: Some(record.tenant_id.clone()),
                        }))
                        .await?;
                }
            }
            if matches!(route.consumer.as_str(), "model" | "tool" | "wait") {
                self.fabric
                    .execute(FabricCommand::Subscribe(SubscriptionSpec {
                        consumer: Self::consumer(&record.tenant_id, record.start.run_id, "cancel"),
                        subscription: route.topic.clone(),
                        topic: route.topic.clone(),
                        event_type: Some(format!("{}.cancel.requested", route.consumer)),
                        tenant: Some(record.tenant_id.clone()),
                    }))
                    .await?;
            }
        }
        Ok(())
    }

    pub async fn tick(
        &self,
        tenant: &str,
        run_id: RunId,
        clock: impl Fn() -> i64,
    ) -> Result<usize, FabricError> {
        let coordinator =
            ToolsRunCoordinator::new(self.fabric.clone()).with_blobs(self.blobs.clone());
        self.wire(&coordinator.load(tenant, run_id).await?).await?;
        self.fabric
            .execute(FabricCommand::DispatchOutbox { limit: 128 })
            .await?;
        let mut processed = 0;
        for consumer in [
            "coordinator",
            "cancel",
            "preparation",
            "model",
            "output",
            "tool",
            "wait",
            "coordinator",
        ] {
            let FabricReply::Claimed(deliveries) = self
                .fabric
                .execute(FabricCommand::ClaimForConsumer {
                    consumer: Self::consumer(tenant, run_id, consumer),
                    worker: format!("tools-worker-{}", std::process::id()),
                    now_ms: clock(),
                    lease_ms: 120_000,
                    limit: 128,
                    max_attempts: 10,
                })
                .await?
            else {
                return Err(FabricError::Conflict);
            };
            processed += deliveries.len();
            match consumer {
                "wait" => {
                    for delivery in deliveries {
                        self.wait
                            .as_ref()
                            .ok_or_else(|| FabricError::Invalid("wait ports required".into()))?
                            .execute(delivery, clock())
                            .await?;
                    }
                }
                "cancel" => {
                    self.cancel_deliveries(deliveries, &clock).await?;
                }
                "preparation" => {
                    let operator =
                        ToolPreparationOperator::new(self.fabric.clone(), self.tools.clone());
                    for delivery in deliveries {
                        operator.execute(delivery, clock()).await?;
                    }
                }
                "model" => {
                    let operator = EffectModelOperator {
                        fabric: self.fabric.clone(),
                        model: self.model.clone(),
                        blobs: self.blobs.clone(),
                    };
                    for delivery in deliveries {
                        let output = EffectOutputOperator {
                            fabric: self.fabric.clone(),
                            blobs: self.blobs.clone(),
                        };
                        let (sender, receiver) = agent_extension::pipeline::bounded_deltas(
                            agent_extension::pipeline::DATA_CAPACITY,
                        )
                        .map_err(super::effect_coordinator::invalid)?;
                        let (done, drained) = tokio::sync::oneshot::channel();
                        let notify = tokio::sync::Notify::new();
                        let workers = async {
                            tokio::try_join!(
                                operator.execute(delivery.clone(), sender, drained, &clock),
                                async {
                                    output.consume(&delivery, receiver, &notify, &clock).await?;
                                    let _ = done.send(());
                                    Ok::<_, FabricError>(())
                                }
                            )?;
                            Ok::<_, FabricError>(())
                        };
                        tokio::pin!(workers);
                        loop {
                            tokio::select! {
                                result = &mut workers => { result?; break; },
                                _ = notify.notified() => { self.drain_coordinator(tenant, run_id, &clock).await?; }
                            }
                        }
                    }
                }
                "output" => {
                    let operator = EffectOutputOperator {
                        fabric: self.fabric.clone(),
                        blobs: self.blobs.clone(),
                    };
                    for delivery in deliveries {
                        operator.execute(delivery, clock()).await?;
                    }
                }
                "tool" => {
                    let operator = ToolOperator::new(self.fabric.clone(), self.tools.clone());
                    try_join_all(
                        deliveries
                            .into_iter()
                            .map(|delivery| operator.execute(delivery, &clock)),
                    )
                    .await?;
                }
                _ => {
                    for delivery in deliveries {
                        if delivery.event.envelope.run_id == run_id
                            && delivery.event.envelope.tenant_id == tenant
                        {
                            coordinator.consume(delivery, clock()).await?;
                        } else {
                            self.fabric
                                .execute(FabricCommand::Commit {
                                    lease: delivery.lease,
                                    now_ms: clock(),
                                    result: serde_json::json!({"other_run":true}),
                                    outbox: vec![],
                                })
                                .await?;
                        }
                    }
                }
            }
            self.fabric
                .execute(FabricCommand::DispatchOutbox { limit: 128 })
                .await?;
        }
        self.project(tenant, run_id).await?;
        Ok(processed)
    }

    async fn drain_coordinator(
        &self,
        tenant: &str,
        run_id: RunId,
        clock: &impl Fn() -> i64,
    ) -> Result<(), FabricError> {
        let coordinator =
            ToolsRunCoordinator::new(self.fabric.clone()).with_blobs(self.blobs.clone());
        loop {
            self.fabric
                .execute(FabricCommand::DispatchOutbox { limit: 128 })
                .await?;
            let FabricReply::Claimed(deliveries) = self
                .fabric
                .execute(FabricCommand::ClaimForConsumer {
                    consumer: Self::consumer(tenant, run_id, "coordinator"),
                    worker: format!("stream-coordinator-{}", std::process::id()),
                    now_ms: clock(),
                    lease_ms: 120_000,
                    limit: 128,
                    max_attempts: 10,
                })
                .await?
            else {
                return Err(FabricError::Conflict);
            };
            if deliveries.is_empty() {
                break;
            }
            for delivery in deliveries {
                if delivery.event.envelope.tenant_id == tenant
                    && delivery.event.envelope.run_id == run_id
                {
                    coordinator.consume(delivery, clock()).await?;
                } else {
                    self.fabric
                        .execute(FabricCommand::Commit {
                            lease: delivery.lease,
                            now_ms: clock(),
                            result: serde_json::json!({"other_run":true}),
                            outbox: vec![],
                        })
                        .await?;
                }
            }
        }
        self.project(tenant, run_id).await
    }
}
