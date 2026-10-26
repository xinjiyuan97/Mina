//! Durable external wait registration and delivery for static graphs.
//! The registration journal precedes external effects, allowing recovery and cleanup.
use super::*;
use crate::{event_runtime::*, harness::*, operator_fabric::*};
use serde_json::json;

#[derive(Clone)]
pub struct GraphWaitBridge {
    fabric: Arc<dyn OperatorFabric>,
    events: Arc<dyn EventStore>,
    jobs: Arc<dyn JobStore>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Registration {
    tenant: String,
    run: RunId,
    dispatch: Value,
    suspension: ToolSuspension,
}
impl Registration {
    fn key(&self) -> String {
        format!(
            "graph-wait:{}",
            EventId::stable(
                "graph-wait",
                &json!([
                    self.tenant,
                    self.run,
                    self.dispatch["batch_id"],
                    self.dispatch["id"]
                ])
                .to_string()
            )
        )
    }
}
fn index(tenant: &str, run: RunId) -> String {
    format!(
        "graph-waits:{}",
        EventId::stable("graph-waits", &json!([tenant, run]).to_string())
    )
}
fn binding(id: SubscriptionId) -> String {
    format!("graph-wait-subscription:{id}")
}
async fn checkpoint<T: serde::de::DeserializeOwned>(
    fabric: &dyn OperatorFabric,
    key: String,
) -> Result<Option<(u64, T)>, FabricError> {
    match fabric.execute(FabricCommand::ReadCheckpoint(key)).await? {
        FabricReply::Checkpoint(Some(saved)) => Ok(Some((
            saved.version,
            serde_json::from_value(saved.value).map_err(|e| invalid("wait", e))?,
        ))),
        FabricReply::Checkpoint(None) => Ok(None),
        _ => Err(FabricError::Conflict),
    }
}
async fn put(fabric: &dyn OperatorFabric, key: String, value: Value) -> Result<(), FabricError> {
    match fabric
        .execute(FabricCommand::Transact {
            checkpoint: CheckpointWrite {
                key: key.clone(),
                expected: None,
                value: value.clone(),
            },
            outbox: vec![],
            delivery: None,
        })
        .await
    {
        Ok(_) => Ok(()),
        Err(FabricError::Fenced)
            if checkpoint::<Value>(fabric, key)
                .await?
                .is_some_and(|(_, old)| old == value) =>
        {
            Ok(())
        }
        Err(e) => Err(e),
    }
}
impl GraphWaitBridge {
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
    async fn register(&self, registration: Registration) -> Result<(), FabricError> {
        validate(&registration)?;
        let key = index(&registration.tenant, registration.run);
        // Persist the cancellation index before registering anything externally.
        loop {
            let old =
                checkpoint::<BTreeMap<String, Registration>>(self.fabric.as_ref(), key.clone())
                    .await?;
            let (version, mut all) = old.map_or((None, BTreeMap::new()), |(v, s)| (Some(v), s));
            if let Some(saved) = all.get(&registration.key()) {
                if saved != &registration {
                    return Err(FabricError::Conflict);
                }
                break;
            }
            all.insert(registration.key(), registration.clone());
            match self
                .fabric
                .execute(FabricCommand::Transact {
                    checkpoint: CheckpointWrite {
                        key: key.clone(),
                        expected: version,
                        value: json!(all),
                    },
                    outbox: vec![],
                    delivery: None,
                })
                .await
            {
                Err(FabricError::Fenced) => continue,
                Err(e) => return Err(e),
                Ok(_) => break,
            }
        }
        if self.terminal(&registration).await? {
            return self.cleanup(&registration).await;
        }
        for wait in &registration.suspension.waits {
            put(
                self.fabric.as_ref(),
                binding(wait.subscription.subscription_id),
                json!(registration),
            )
            .await?;
            self.events
                .subscribe(wait.subscription.clone())
                .await
                .map_err(|e| invalid("wait.subscribe", e))?;
        }
        for effect in &registration.suspension.effects {
            if self.terminal(&registration).await? {
                break;
            }
            match effect {
                EffectRequest::StartJob { command } => {
                    put(
                        self.fabric.as_ref(),
                        format!("graph-job-binding:{}", command.job_id),
                        json!(registration),
                    )
                    .await?;
                    self.jobs
                        .submit(command.clone())
                        .await
                        .map_err(|e| invalid("wait.job", e))?;
                }
                EffectRequest::ScheduleTimer { command } => {
                    self.events
                        .schedule_once(command.clone())
                        .await
                        .map_err(|e| invalid("wait.timer", e))?;
                }
                EffectRequest::PublishEvent { command } => {
                    self.events
                        .publish(command.clone())
                        .await
                        .map_err(|e| invalid("wait.publish", e))?;
                }
            }
        }
        if self.terminal(&registration).await? {
            self.cleanup(&registration).await?;
        }
        Ok(())
    }
    async fn terminal(&self, r: &Registration) -> Result<bool, FabricError> {
        Ok(super::runtime::read(self.fabric.as_ref(), &r.tenant, r.run)
            .await?
            .1
            .is_terminal())
    }
    async fn cleanup(&self, r: &Registration) -> Result<(), FabricError> {
        let now = crate::agent::unix_time_ms();
        for wait in &r.suspension.waits {
            match self
                .events
                .cancel_subscription(wait.subscription.subscription_id, now)
                .await
            {
                Ok(_) | Err(EventError::NotFound) => {}
                Err(e) => return Err(invalid("wait.cancel", e)),
            }
        }
        for effect in &r.suspension.effects {
            match effect {
                EffectRequest::StartJob { command } => {
                    match self.jobs.cancel(command.job_id, now).await {
                        Ok(_) | Err(JobStoreError::NotFound) => {}
                        Err(e) => return Err(invalid("wait.cancel_job", e)),
                    }
                }
                EffectRequest::ScheduleTimer { command } => {
                    match self.events.cancel_timer(command.timer_id, now).await {
                        Ok(_) | Err(EventError::NotFound) => {}
                        Err(EventError::Conflict(_)) => {
                            if self
                                .events
                                .schedule_once(command.clone())
                                .await
                                .map_err(|e| invalid("wait.timer", e))?
                                .status
                                != TimerStatus::Fired
                            {
                                return Err(FabricError::Conflict);
                            }
                        }
                        Err(e) => return Err(invalid("wait.cancel_timer", e)),
                    }
                }
                EffectRequest::PublishEvent { .. } => {}
            }
        }
        put(
            self.fabric.as_ref(),
            format!("{}:cleaned", r.key()),
            json!(true),
        )
        .await
    }
    pub async fn cleanup_run(&self, tenant: &str, run: RunId) -> Result<(), FabricError> {
        if let Some((_, all)) =
            checkpoint::<BTreeMap<String, Registration>>(self.fabric.as_ref(), index(tenant, run))
                .await?
        {
            for registration in all.values() {
                self.cleanup(registration).await?;
            }
        }
        Ok(())
    }
}
fn validate(r: &Registration) -> Result<(), FabricError> {
    if r.suspension.waits.is_empty()
        || r.suspension.waits.len() > 32
        || r.suspension.effects.len() > 32
    {
        return Err(invalid("wait", "invalid resource count"));
    }
    let mut ids = BTreeSet::new();
    for wait in &r.suspension.waits {
        let sub = &wait.subscription;
        sub.validate().map_err(|e| invalid("wait", e))?;
        if sub.owner != (SubscriptionOwner::Run { run_id: r.run })
            || sub.scope != (SubscriptionScope::Run { run_id: r.run })
            || sub.delivery
                != (DeliveryTarget::WakeRun {
                    run_id: r.run,
                    wait_key: wait.wait_key.clone(),
                })
            || sub.filter.sources.is_empty()
            || sub.filter.correlation_id.is_none()
            || !ids.insert(sub.subscription_id.to_string())
        {
            return Err(invalid("wait", "unbound or duplicate subscription"));
        }
    }
    for effect in &r.suspension.effects {
        effect.validate().map_err(|e| invalid("wait.effect", e))?;
        if let EffectRequest::StartJob { command } = effect
            && command.run_id != r.run
        {
            return Err(invalid("wait.job", "run mismatch"));
        }
    }
    Ok(())
}
impl DeliveryRouter for GraphWaitBridge {
    fn deliver(&self, delivery: Delivery, event: EventEnvelope) -> EventFuture<'_, ()> {
        Box::pin(async move {
            let (_, r) = checkpoint::<Registration>(
                self.fabric.as_ref(),
                binding(delivery.delivery_id.subscription_id),
            )
            .await
            .map_err(|e| EventError::backend(e.to_string()))?
            .ok_or(EventError::NotFound)?;
            let wait = r
                .suspension
                .waits
                .iter()
                .find(|w| w.subscription.subscription_id == delivery.delivery_id.subscription_id)
                .ok_or(EventError::NotFound)?;
            if self.events.get_event(event.event.event_id).await? != Some(event.clone())
                || event.event.subject.as_deref() != Some(&format!("run/{}", r.run))
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
                    .is_some_and(|t| event.event.recorded_at_ms >= t)
            {
                return Err(EventError::invalid("untrusted graph wait delivery"));
            }
            // First matching event wins, including across redelivery or a process restart.
            let key = format!("{}:wake", r.key());
            match put(self.fabric.as_ref(), key.clone(), json!(event)).await {
                Ok(()) | Err(FabricError::Fenced) => {}
                Err(e) => return Err(EventError::backend(e.to_string())),
            }
            GraphIngress::new(self.fabric.clone())
                .send(
                    &r.tenant,
                    r.run,
                    EventId::stable("graph-wake", &key),
                    GraphEvent::new("wait.woken.v1", json!({"key":r.key()})),
                )
                .await
                .map_err(|e| EventError::backend(e.to_string()))?;
            Ok(())
        })
    }
}

pub(super) fn register(registry: &mut OperatorRegistry) {
    let mut desc = builtins::descriptor(
        "agent.wait",
        &["wait.requested.v1", "wait.woken.v1", "engine.terminal.v1"],
        &["gate.result.v1", "wait.registered.v1", "wait.completed.v1"],
        json!({"type":"object","additionalProperties":false,"required":["wait_service"],"properties":{"wait_service":{"type":"string","minLength":1}}}),
        true,
    );
    desc.resources
        .insert("wait_service".into(), ResourceKind::WaitService);
    registry
        .register(desc, |config, resources| {
            let name = config["wait_service"]
                .as_str()
                .ok_or_else(|| invalid("wait_service", "required"))?;
            let service = resources
                .waits
                .get(name)
                .ok_or_else(|| invalid("wait_service", "missing"))?
                .1
                .clone();
            Ok(Arc::new(Wait { service }))
        })
        .expect("wait registration");
}
struct Wait {
    service: Arc<GraphWaitBridge>,
}
impl GraphOperator for Wait {
    fn handle(&self, ctx: GraphContext) -> GraphFuture<'_> {
        Box::pin(async move {
            let mut saved: BTreeMap<String, bool> = if ctx.state.is_null() {
                BTreeMap::new()
            } else {
                serde_json::from_value(ctx.state).map_err(|e| invalid("wait.state", e))?
            };
            let mut events = vec![];
            match ctx.event.kind.as_str() {
                "engine.terminal.v1" => self.service.cleanup_run(&ctx.tenant, ctx.run_id).await?,
                "wait.requested.v1" => {
                    let r = Registration {
                        tenant: ctx.tenant,
                        run: ctx.run_id,
                        dispatch: ctx.event.payload["dispatch"].clone(),
                        suspension: serde_json::from_value(ctx.event.payload["suspension"].clone())
                            .map_err(|e| invalid("suspension", e))?,
                    };
                    self.service.register(r.clone()).await?;
                    if !saved.contains_key(&r.key()) {
                        events.push(GraphEvent::new(
                            "wait.registered.v1",
                            json!({"key":r.key(),"wait_count":r.suspension.waits.len()}),
                        ));
                    }
                    saved.entry(r.key()).or_insert(false);
                }
                "wait.woken.v1" => {
                    if ctx.event.from != "$external" {
                        return Err(invalid("wait.woken", "host ingress required"));
                    }
                    let key = ctx.event.payload["key"]
                        .as_str()
                        .ok_or_else(|| invalid("wait.woken", "key required"))?;
                    if saved.get(key) != Some(&true) {
                        let (_, all) = checkpoint::<BTreeMap<String, Registration>>(
                            self.service.fabric.as_ref(),
                            index(&ctx.tenant, ctx.run_id),
                        )
                        .await?
                        .ok_or(FabricError::NotFound)?;
                        if let Some(r) = all.get(key) {
                            let (_, event) = checkpoint::<EventEnvelope>(
                                self.service.fabric.as_ref(),
                                format!("{key}:wake"),
                            )
                            .await?
                            .ok_or(FabricError::NotFound)?;
                            self.service.cleanup(r).await?;
                            let content = resumed_tool_content(&[FlowInboxItem {
                                event,
                                consumed_revision: None,
                            }])
                            .map_err(|e| invalid("wait.result", format!("{e:?}")))?;
                            events.push(GraphEvent::new("wait.completed.v1", json!({"key":key})));
                            events.push(GraphEvent::new("gate.result.v1",json!({"batch_id":r.dispatch["batch_id"],"id":r.dispatch["id"],"payload":{"ok":true,"output":content}})));
                            saved.insert(key.into(), true);
                        }
                    }
                }
                _ => return Err(invalid("wait.event", "unknown event")),
            }
            Ok(GraphOutput::new(json!(saved), events))
        })
    }
}

pub(super) fn validate_suspension(
    run: RunId,
    suspension: &ToolSuspension,
) -> Result<(), FabricError> {
    validate(&Registration {
        tenant: String::new(),
        run,
        dispatch: Value::Null,
        suspension: suspension.clone(),
    })
}
fn machine_error(code: &str, message: &str, retryable: bool) -> MachineError {
    MachineError {
        code: code.into(),
        message: message.into(),
        retryable,
    }
}
pub fn resumed_tool_content(inbox: &[FlowInboxItem]) -> Result<String, MachineError> {
    if inbox.is_empty() {
        return Err(machine_error(
            "tool_resume_event_missing",
            "the suspended tool resumed without a matching event",
            true,
        ));
    }
    let events = inbox
        .iter()
        .map(|item| {
            let event = &item.event;
            serde_json::json!({
                "sequence": event.sequence,
                "event_id": event.event.event_id,
                "topic": event.event.topic,
                "event_type": event.event.event_type,
                "schema_version": event.event.schema_version,
                "source": event.event.source,
                "subject": event.event.subject,
                "correlation_id": event.event.correlation_id,
                "causation_id": event.event.causation_id,
                "occurred_at_ms": event.event.occurred_at_ms,
                "payload": event.event.payload,
            })
        })
        .collect::<Vec<_>>();
    serde_json::to_string(&serde_json::json!({
        "ok": true,
        "events": events,
    }))
    .map_err(|_| {
        machine_error(
            "tool_resume_event_invalid",
            "the suspended tool event could not be encoded",
            false,
        )
    })
}

impl GraphWaitBridge {
    pub fn job_router(&self, inner: Arc<dyn JobRouter>) -> Arc<dyn JobRouter> {
        Arc::new(WaitJobs {
            bridge: self.clone(),
            inner,
        })
    }
}
struct WaitJobs {
    bridge: GraphWaitBridge,
    inner: Arc<dyn JobRouter>,
}
impl JobRouter for WaitJobs {
    fn execute(&self, job: JobRecord) -> JobExecutionFuture {
        let bridge = self.bridge.clone();
        let inner = self.inner.clone();
        Box::pin(async move {
            let validate = async {
                if let Some((_, r)) = checkpoint::<Registration>(
                    bridge.fabric.as_ref(),
                    format!("graph-job-binding:{}", job.command.job_id),
                )
                .await?
                    && (bridge.terminal(&r).await?
                        || checkpoint::<Value>(bridge.fabric.as_ref(), format!("{}:wake", r.key()))
                            .await?
                            .is_some())
                {
                    return Err(FabricError::Conflict);
                }
                Ok::<_, FabricError>(())
            }
            .await;
            validate.map_err(|e| JobExecutionError {
                code: "run_fenced".into(),
                message: e.to_string(),
                retryable: !matches!(e, FabricError::Conflict),
                retry_after_ms: None,
            })?;
            inner.execute(job).await
        })
    }
}
