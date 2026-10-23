use super::effect_coordinator::{invalid, read, reuse};
use agent_core::{effect_pipeline::*, event_runtime::*, harness::*, operator_fabric::*};
use serde_json::json;
use std::{collections::BTreeSet, sync::Arc};

#[derive(Clone)]
pub struct EffectWaitBridge {
    fabric: Arc<dyn OperatorFabric>,
    events: Arc<dyn EventStore>,
    jobs: Arc<dyn JobStore>,
}

pub(crate) fn validate(request: &EffectWaitRequest) -> Result<(), FabricError> {
    if request.waits.is_empty() {
        return Err(invalid("empty wait"));
    }
    let mut subscriptions = BTreeSet::new();
    for wait in &request.waits {
        let subscription = &wait.subscription;
        subscription.validate().map_err(invalid)?;
        if subscription.owner
            != (SubscriptionOwner::Run {
                run_id: request.run_id,
            })
            || subscription.scope
                != (SubscriptionScope::Run {
                    run_id: request.run_id,
                })
            || subscription.delivery
                != (DeliveryTarget::WakeRun {
                    run_id: request.run_id,
                    wait_key: wait.wait_key.clone(),
                })
            || subscription.filter.sources.is_empty()
            || subscription.filter.correlation_id.is_none()
            || !subscriptions.insert(subscription.subscription_id.to_string())
        {
            return Err(invalid(
                "wait must have unique subscription and bound run/source/correlation",
            ));
        }
    }
    for effect in &request.effects {
        effect.validate().map_err(invalid)?;
        if let EffectRequest::StartJob { command } = effect
            && command.run_id != request.run_id
        {
            return Err(invalid("job belongs to another run"));
        }
    }
    Ok(())
}

impl EffectWaitBridge {
    pub fn new(
        fabric: Arc<dyn OperatorFabric>,
        events: Arc<dyn EventStore>,
        jobs: Arc<dyn JobStore>,
    ) -> Self {
        Self {
            fabric,
            events,
            jobs,
        }
    }

    pub fn job_router(&self, inner: Arc<dyn JobRouter>) -> Arc<dyn JobRouter> {
        Arc::new(WaitJobs {
            bridge: self.clone(),
            inner,
        })
    }

    async fn publish_phase(
        &self,
        request: &EffectWaitRequest,
        identity: &str,
        kind: &str,
        now_ms: i64,
    ) -> Result<(), FabricError> {
        let key = format!("{}:{identity}:{kind}", request.key());
        let mut route = request.route.clone();
        route.event_type = kind.into();
        let mut event = fact(
            &request.tenant_id,
            request.run_id,
            &format!("{}:{identity}", request.effect_id),
            &route,
            json!({"checkpoint_key":key}),
            reference_schema(),
            now_ms,
        );
        event.envelope.operator.name = "wait_bridge".into();
        let result = self
            .fabric
            .execute(FabricCommand::Transact {
                checkpoint: CheckpointWrite {
                    key: key.clone(),
                    expected: None,
                    value: json!({"phase":kind}),
                },
                outbox: vec![event],
                delivery: None,
            })
            .await;
        match result {
            Ok(_) | Err(FabricError::Fenced) => Ok(()),
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn execute(
        &self,
        delivery: ClaimedDelivery,
        now_ms: i64,
    ) -> Result<(), FabricError> {
        if reuse(self.fabric.as_ref(), &delivery, now_ms).await? {
            return Ok(());
        }
        let envelope = &delivery.event.envelope;
        let key = envelope.payload["checkpoint_key"]
            .as_str()
            .ok_or(FabricError::Conflict)?;
        let (_, request): (_, EffectWaitRequest) = read(self.fabric.as_ref(), key.into()).await?;
        validate(&request)?;
        if request.tenant_id != envelope.tenant_id
            || request.run_id != envelope.run_id
            || request.effect_id != envelope.effect_id
            || request.key() != key
        {
            return Err(FabricError::Conflict);
        }
        let (_, run): (_, EffectRun) = read(
            self.fabric.as_ref(),
            run_key(&request.tenant_id, request.run_id),
        )
        .await?;
        if envelope.event_type == "wait.cancel.requested" {
            if run.wait.as_ref() == Some(&request)
                && matches!(run.reducer.outcome(), AgentLoopOutcome::Suspended { .. })
            {
                return Err(FabricError::Conflict);
            }
            self.cleanup(&request, now_ms).await?;
        } else if envelope.event_type != "wait.register.requested" {
            return Err(FabricError::Conflict);
        } else if run.wait.as_ref() == Some(&request)
            && matches!(run.reducer.outcome(), AgentLoopOutcome::Suspended { .. })
        {
            for wait in &request.waits {
                self.events
                    .subscribe(wait.subscription.clone())
                    .await
                    .map_err(invalid)?;
            }
            for effect in &request.effects {
                match effect {
                    EffectRequest::StartJob { command } => {
                        self.publish_phase(
                            &request,
                            &command.job_id.to_string(),
                            "job.requested",
                            now_ms,
                        )
                        .await?;
                        let binding = format!("effect-job-binding:{}", command.job_id);
                        match self
                            .fabric
                            .execute(FabricCommand::Transact {
                                checkpoint: CheckpointWrite {
                                    key: binding.clone(),
                                    expected: None,
                                    value: json!(request),
                                },
                                outbox: vec![],
                                delivery: None,
                            })
                            .await
                        {
                            Ok(_) => {}
                            Err(FabricError::Fenced) => {
                                let (_, stored): (_, EffectWaitRequest) =
                                    read(self.fabric.as_ref(), binding).await?;
                                if stored != request {
                                    return Err(FabricError::Conflict);
                                }
                            }
                            Err(error) => return Err(error),
                        }
                        self.jobs.submit(command.clone()).await.map_err(invalid)?;
                    }
                    EffectRequest::ScheduleTimer { command } => {
                        self.publish_phase(
                            &request,
                            &command.timer_id.to_string(),
                            "timer.schedule.requested",
                            now_ms,
                        )
                        .await?;
                        self.events
                            .schedule_once(command.clone())
                            .await
                            .map_err(invalid)?;
                        self.publish_phase(
                            &request,
                            &command.timer_id.to_string(),
                            "timer.scheduled",
                            now_ms,
                        )
                        .await?;
                    }
                    EffectRequest::PublishEvent { command } => {
                        self.events
                            .publish(command.clone())
                            .await
                            .map_err(invalid)?;
                    }
                }
            }
            let (_, current): (_, EffectRun) = read(
                self.fabric.as_ref(),
                run_key(&request.tenant_id, request.run_id),
            )
            .await?;
            if current.wait.as_ref() != Some(&request) {
                self.cleanup(&request, now_ms).await?;
            }
        }
        self.fabric
            .execute(FabricCommand::Commit {
                lease: delivery.lease,
                now_ms,
                result: json!({"registered":true}),
                outbox: vec![],
            })
            .await?;
        Ok(())
    }

    async fn cleanup(&self, request: &EffectWaitRequest, now_ms: i64) -> Result<(), FabricError> {
        for wait in &request.waits {
            match self
                .events
                .cancel_subscription(wait.subscription.subscription_id, now_ms)
                .await
            {
                Ok(_) | Err(EventError::NotFound) => {}
                Err(error) => return Err(invalid(error)),
            }
        }
        for effect in &request.effects {
            match effect {
                EffectRequest::StartJob { command } => {
                    self.publish_phase(
                        request,
                        &command.job_id.to_string(),
                        "job.cancel.requested",
                        now_ms,
                    )
                    .await?;
                    match self.jobs.cancel(command.job_id, now_ms).await {
                        Ok(job) if job.status == JobStatus::Cancelled => {
                            self.publish_phase(
                                request,
                                &command.job_id.to_string(),
                                "job.cancelled",
                                now_ms,
                            )
                            .await?
                        }
                        Ok(_) | Err(JobStoreError::NotFound) => {}
                        Err(error) => return Err(invalid(error)),
                    }
                }
                EffectRequest::ScheduleTimer { command } => {
                    self.publish_phase(
                        request,
                        &command.timer_id.to_string(),
                        "timer.cancel.requested",
                        now_ms,
                    )
                    .await?;
                    match self.events.cancel_timer(command.timer_id, now_ms).await {
                        Ok(timer) if timer.status == TimerStatus::Cancelled => {
                            self.publish_phase(
                                request,
                                &command.timer_id.to_string(),
                                "timer.cancelled",
                                now_ms,
                            )
                            .await?
                        }
                        Err(EventError::Conflict(_)) => {
                            let timer = self
                                .events
                                .schedule_once(command.clone())
                                .await
                                .map_err(invalid)?;
                            if timer.status != TimerStatus::Fired {
                                return Err(FabricError::Conflict);
                            }
                        }
                        Ok(_) | Err(EventError::NotFound) => {}
                        Err(error) => return Err(invalid(error)),
                    }
                }
                EffectRequest::PublishEvent { .. } => {}
            }
        }
        Ok(())
    }
}

impl DeliveryRouter for EffectWaitBridge {
    fn deliver(&self, delivery: Delivery, event: EventEnvelope) -> EventFuture<'_, ()> {
        Box::pin(async move {
            let (_, request): (_, EffectWaitRequest) = read(
                self.fabric.as_ref(),
                wait_binding_key(delivery.delivery_id.subscription_id),
            )
            .await
            .map_err(|error| EventError::backend(error.to_string()))?;
            let wait = request
                .waits
                .iter()
                .find(|wait| {
                    wait.subscription.subscription_id == delivery.delivery_id.subscription_id
                })
                .ok_or(EventError::NotFound)?;
            if self.events.get_event(event.event.event_id).await? != Some(event.clone())
                || event.event.subject.as_deref() != Some(&format!("run/{}", request.run_id))
                || event.event.event_id != delivery.delivery_id.event_id
                || event.sequence != delivery.event_sequence
                || delivery.target != wait.subscription.delivery
                || !wait.subscription.filter.matches(&event)
                || !wait
                    .subscription
                    .start_position
                    .includes_recorded_time(event.event.recorded_at_ms)
                || wait
                    .subscription
                    .expires_at_ms
                    .is_some_and(|expiry| event.event.recorded_at_ms >= expiry)
            {
                return Err(EventError::invalid("untrusted wait delivery"));
            }
            let key = format!("{}:wake", request.key());
            let mut route = request.route.clone();
            route.event_type = "wait.resolved".into();
            let mut outbox = vec![fact(
                &request.tenant_id,
                request.run_id,
                &request.effect_id,
                &route,
                json!({"checkpoint_key":key}),
                reference_schema(),
                event.event.recorded_at_ms,
            )];
            let phase = match event.event.source {
                EventSource::JobWorker => Some(event.event.event_type.as_str()),
                EventSource::Timer => Some("timer.fired"),
                _ => None,
            };
            if let Some(phase) = phase {
                route.event_type = phase.into();
                outbox.push(fact(
                    &request.tenant_id,
                    request.run_id,
                    &request.effect_id,
                    &route,
                    json!({"checkpoint_key":key}),
                    reference_schema(),
                    event.event.recorded_at_ms,
                ));
            }
            for fact in &mut outbox {
                fact.envelope.operator.name = "wait_bridge".into();
            }
            match self
                .fabric
                .execute(FabricCommand::Transact {
                    checkpoint: CheckpointWrite {
                        key,
                        expected: None,
                        value: json!(event),
                    },
                    outbox,
                    delivery: None,
                })
                .await
            {
                Ok(_) | Err(FabricError::Fenced) => Ok(()),
                Err(error) => Err(EventError::backend(error.to_string())),
            }
        })
    }
}

struct WaitJobs {
    bridge: EffectWaitBridge,
    inner: Arc<dyn JobRouter>,
}
impl JobRouter for WaitJobs {
    fn execute(&self, job: JobRecord) -> JobExecutionFuture {
        let bridge = self.bridge.clone();
        let inner = self.inner.clone();
        Box::pin(async move {
            let binding = read::<EffectWaitRequest>(
                bridge.fabric.as_ref(),
                format!("effect-job-binding:{}", job.command.job_id),
            )
            .await;
            match binding {
                Ok((_, request)) => {
                    let (_, run): (_, EffectRun) = read(
                        bridge.fabric.as_ref(),
                        run_key(&request.tenant_id, request.run_id),
                    )
                    .await
                    .map_err(|error| JobExecutionError {
                        code: "wait_bridge".into(),
                        message: error.to_string(),
                        retryable: true,
                        retry_after_ms: None,
                    })?;
                    if run.wait.as_ref() != Some(&request)
                        || !matches!(run.reducer.outcome(), AgentLoopOutcome::Suspended { .. })
                    {
                        return Err(JobExecutionError {
                            code: "run_fenced".into(),
                            message: "run no longer waits for this job".into(),
                            retryable: false,
                            retry_after_ms: None,
                        });
                    }
                    bridge
                        .publish_phase(
                            &request,
                            &job.command.job_id.to_string(),
                            "job.started",
                            crate::unix_time_ms(),
                        )
                        .await
                }
                Err(FabricError::NotFound) => Ok(()),
                Err(error) => Err(error),
            }
            .map_err(|error| JobExecutionError {
                code: "wait_bridge".into(),
                message: error.to_string(),
                retryable: true,
                retry_after_ms: None,
            })?;
            inner.execute(job).await
        })
    }
}
