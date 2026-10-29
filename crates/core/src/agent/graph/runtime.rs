use super::*;
use crate::{
    agent::{OperatorBus, bus::TransactionalOperator, executor::FabricRuntimeConfig},
    contracts::events::EventId,
    contracts::fabric::*,
    operator::*,
};
use serde_json::json;
use std::collections::HashMap;
use tokio::{sync::Mutex, task::JoinHandle};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GraphStatus {
    Running,
    Completed,
    Failed,
    Cancelled,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublishedGraphEvent {
    pub id: EventId,
    pub from: String,
    pub kind: String,
    pub payload: Value,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Invocation {
    event_id: EventId,
    expires_at_ms: i64,
}
/// Token accounting at each successfully committed operator state transition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphStepUsage {
    pub instance: String,
    pub event_id: EventId,
    pub usage: Option<TokenUsage>,
    pub cumulative_usage: Option<TokenUsage>,
}
fn accumulate_usage(previous: Option<TokenUsage>, next: Option<TokenUsage>) -> Option<TokenUsage> {
    match (previous, next) {
        (Some(a), Some(b)) => Some(TokenUsage {
            input_tokens: a.input_tokens.saturating_add(b.input_tokens),
            output_tokens: a.output_tokens.saturating_add(b.output_tokens),
            total_tokens: a.total_tokens.saturating_add(b.total_tokens),
            source: if a.source == b.source {
                a.source
            } else {
                crate::contracts::TokenUsageSource::Mixed
            },
        }),
        (a, b) => a.or(b),
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphRun {
    pub tenant: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    pub run_id: RunId,
    pub definition: AgentDefinition,
    pub input: Value,
    /// Source node selected when this Run was admitted.
    pub trigger: String,
    pub status: GraphStatus,
    pub output: Option<Value>,
    pub error: Option<String>,
    pub deadline_ms: i64,
    pub deliveries: u64,
    pub event_count: u64,
    /// Terminal observers and external wait cleanup have drained.
    #[serde(default)]
    pub cleanup_complete: bool,
    #[serde(default)]
    pub usage: Option<TokenUsage>,
    #[serde(default)]
    pub step_usage: Vec<GraphStepUsage>,
    #[serde(default)]
    external: BTreeMap<String, GraphEvent>,
    bindings: BTreeMap<String, String>,
    descriptors: BTreeMap<String, OperatorDescriptor>,
    states: BTreeMap<String, Value>,
    inflight: BTreeMap<String, Invocation>,
}
impl GraphRun {
    pub fn validate_resources(
        &self,
        registry: &OperatorRegistry,
        resources: &AgentResources,
    ) -> Result<(), FabricError> {
        if resources.agent_id.is_some() && self.agent_id != resources.agent_id {
            return Err(invalid("recovery", "agent identity changed"));
        }
        let compiled = compile(self.definition.clone(), registry, resources)?;
        compiled
            .definition
            .validate_trigger(registry, &self.trigger, &self.input)?;
        if compiled.bindings != self.bindings || compiled.descriptors != self.descriptors {
            return Err(invalid("recovery", "resource or operator contract changed"));
        }
        Ok(())
    }
    pub fn is_terminal(&self) -> bool {
        self.status != GraphStatus::Running
    }
}
pub(super) fn root(tenant: &str, run: RunId) -> String {
    format!(
        "static-agent:{}",
        EventId::stable("static-agent-run", &format!("{tenant}:{run}"))
    )
}
fn topic(tenant: &str, run: RunId, from: &str, event: &str) -> String {
    format!(
        "{}:{}",
        root(tenant, run),
        EventId::stable("static-agent-topic", &format!("{from}:{event}"))
    )
}
const PARALLEL_LANES: usize = 16;
fn lane(event: &FabricEvent) -> usize {
    let identity = event.envelope.payload["id"]
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| {
            event
                .envelope
                .causation_id
                .unwrap_or(event.envelope.event_id)
                .to_string()
        });
    identity
        .bytes()
        .fold(0usize, |sum, byte| sum.wrapping_add(byte as usize))
        % PARALLEL_LANES
}
fn delivery_lane(operator: &OperatorDefinition, event: &FabricEvent) -> usize {
    // A batch containing exclusive, suspending or reviewed tools stays on one FIFO.
    // Only a wholly parallel-safe batch may be distributed across independent leases.
    if operator.kind == "agent.tools"
        && !builtins::parallel_tool_batch(&operator.config, &event.envelope.payload)
    {
        0
    } else {
        lane(event)
    }
}
fn lane_topic(base: &str, instance: &str, lane: usize) -> String {
    format!("{base}:{instance}:{lane}")
}
fn routed_events(run: &GraphRun, events: &[FabricEvent]) -> Vec<FabricEvent> {
    let mut outbox = events.to_vec();
    for event in events {
        let mut targets = BTreeSet::new();
        for sub in &run.definition.subscriptions {
            if sub.from == event.envelope.operator.name
                && sub.events.contains(&event.envelope.event_type)
                && run.descriptors[&sub.subscriber].stateless_parallel
                && targets.insert(sub.subscriber.clone())
            {
                let mut routed = event.clone();
                routed.topic = lane_topic(
                    &event.topic,
                    &sub.subscriber,
                    delivery_lane(&run.definition.operators[&sub.subscriber], event),
                );
                routed.envelope.causation_id = Some(event.envelope.event_id);
                routed.envelope.event_id = EventId::stable(
                    "graph-delivery-lane",
                    &format!("{}:{}", event.envelope.event_id, sub.subscriber),
                );
                outbox.push(routed);
            }
        }
    }
    outbox
}
fn consumer(tenant: &str, run: RunId, instance: &str) -> String {
    format!("{}:{instance}", root(tenant, run))
}
pub(super) fn log_key(tenant: &str, run: RunId) -> String {
    format!("{}:events", root(tenant, run))
}
fn now() -> i64 {
    crate::agent::unix_time_ms()
}
pub(super) async fn read(
    fabric: &dyn OperatorFabric,
    tenant: &str,
    run: RunId,
) -> Result<(u64, GraphRun), FabricError> {
    match fabric
        .execute(FabricCommand::ReadCheckpoint(root(tenant, run)))
        .await?
    {
        FabricReply::Checkpoint(Some(saved)) if saved.value["retired"] == true => {
            Err(FabricError::Invalid("retention_expired".into()))
        }
        FabricReply::Checkpoint(Some(saved)) => Ok((
            saved.version,
            serde_json::from_value(saved.value).map_err(|e| invalid("run", e))?,
        )),
        FabricReply::Checkpoint(None) => Err(FabricError::NotFound),
        _ => Err(FabricError::Conflict),
    }
}
fn make_event(
    tenant: &str,
    run: RunId,
    from: &str,
    event: GraphEvent,
    id: EventId,
    schema: Value,
) -> FabricEvent {
    FabricEvent {
        topic: topic(tenant, run, from, &event.kind),
        payload_schema: schema,
        envelope: OperatorEnvelope {
            schema_version: 1,
            engine_version: EngineVersion::OperatorV1,
            projection_version: 1,
            event_id: id,
            event_type: event.kind,
            tenant_id: tenant.into(),
            run_id: run,
            session_id: None,
            effect_id: id.to_string(),
            invocation_id: id.to_string(),
            attempt: 1,
            issued_revision: RunRevision(1),
            lease_epoch: LeaseEpoch(1),
            correlation_id: run.to_string(),
            causation_id: None,
            operator: OperatorIdentity {
                name: from.into(),
                version: 1,
            },
            operator_seq: OperatorSequence(0),
            delivery_class: DeliveryClass::Durable,
            occurred_at_ms: now(),
            payload: event.payload,
        },
    }
}
fn published(event: &FabricEvent) -> PublishedGraphEvent {
    PublishedGraphEvent {
        id: event.envelope.event_id,
        from: event.envelope.operator.name.clone(),
        kind: event.envelope.event_type.clone(),
        payload: event.envelope.payload.clone(),
    }
}
async fn commit(
    fabric: &dyn OperatorFabric,
    version: Option<u64>,
    mut run: GraphRun,
    events: Vec<FabricEvent>,
    delivery: Option<DeliveryLease>,
) -> Result<(), FabricError> {
    // Reserve checkpoint headroom for terminal status and future invocation markers.
    let outbox = routed_events(&run, &events);
    let mut batch_bytes = 0;
    if outbox.len() > MAX_BATCH_ITEMS {
        return Err(FabricError::Capacity);
    }
    for event in &outbox {
        let bytes = serde_json::to_vec(event)
            .map_err(|e| invalid("event", e))?
            .len();
        batch_bytes += bytes;
        if bytes > fabric.limits().max_event_bytes || batch_bytes > fabric.limits().max_batch_bytes
        {
            return Err(FabricError::Capacity);
        }
    }
    if serde_json::to_vec(&run)
        .map_err(|e| invalid("run", e))?
        .len()
        > MAX_CHECKPOINT_BYTES - 64 * 1024
    {
        return Err(FabricError::Capacity);
    }
    let completion = events
        .iter()
        .find(|e| e.envelope.event_type == "engine.terminal.v1")
        .map(|event| {
            Ok::<_, FabricError>(crate::contracts::host::RunCompletion {
                schema_version: 1,
                tenant: run.tenant.clone(),
                run_id: run.run_id,
                completion_id: EventId::stable(
                    "run-completion",
                    &json!([run.tenant, run.run_id]).to_string(),
                )
                .to_string(),
                status: serde_json::to_value(run.status)
                    .map_err(|e| invalid("completion", e))?
                    .as_str()
                    .unwrap_or("unknown")
                    .into(),
                output: run.output.clone(),
                error: run.error.clone(),
                usage: run.usage.as_ref().map(|u| json!(u)),
                completed_at_ms: event.envelope.occurred_at_ms,
                definition_digest: crate::agent::bindings::digest(&run.definition)?,
            })
        })
        .transpose()?;
    let expected = run.event_count;
    run.event_count += events.len() as u64;
    let items = events
        .iter()
        .map(|event| serde_json::to_value(published(event)).expect("event serialization"))
        .collect();
    let transaction = FabricCommand::TransactLog {
        log: log_key(&run.tenant, run.run_id),
        expected,
        items,
        checkpoints: vec![CheckpointWrite {
            key: root(&run.tenant, run.run_id),
            expected: version,
            value: serde_json::to_value(&run).map_err(|e| invalid("run", e))?,
        }],
        outbox,
        delivery: delivery.map(|lease| (lease, now())),
    };
    let records = run
        .agent_id
        .as_ref()
        .filter(|_| version.is_none())
        .map(|id| {
            vec![crate::contracts::agent_history::membership(
                &run.tenant,
                id,
                run.run_id,
            )]
        })
        .unwrap_or_default();
    fabric
        .execute(if completion.is_some() || !records.is_empty() {
            FabricCommand::HostTransact {
                records,
                completion,
                transaction: Box::new(transaction),
            }
        } else {
            transaction
        })
        .await
        .map(|_| ())
}
fn terminal_event(run: &GraphRun) -> FabricEvent {
    make_event(
        &run.tenant,
        run.run_id,
        "$engine",
        GraphEvent::new(
            "engine.terminal.v1",
            json!({"status":run.status,"output":run.output,"error":run.error,"usage":run.usage}),
        ),
        EventId::stable("static-agent-terminal", &root(&run.tenant, run.run_id)),
        builtins::object_schema(),
    )
}
async fn finish(
    fabric: &dyn OperatorFabric,
    tenant: &str,
    id: RunId,
    status: GraphStatus,
    error: Option<&str>,
) -> Result<(), FabricError> {
    loop {
        let (version, mut run) = read(fabric, tenant, id).await?;
        if run.is_terminal() {
            return Ok(());
        }
        run.status = status;
        run.error = error.map(Into::into);
        let event = terminal_event(&run);
        match commit(fabric, Some(version), run, vec![event], None).await {
            Err(FabricError::Fenced) => continue,
            result => return result,
        }
    }
}
/// Durable, idempotent host ingress. It cannot impersonate an operator instance.
#[derive(Clone)]
pub struct GraphIngress {
    fabric: Arc<dyn OperatorFabric>,
}
impl GraphIngress {
    pub fn new(fabric: Arc<dyn OperatorFabric>) -> Self {
        Self { fabric }
    }
    pub async fn send(
        &self,
        tenant: &str,
        run_id: RunId,
        id: EventId,
        event: GraphEvent,
    ) -> Result<bool, FabricError> {
        loop {
            let (version, mut run) = read(self.fabric.as_ref(), tenant, run_id).await?;
            if let Some(prior) = run.external.get(&id.to_string()) {
                return if prior == &event {
                    Ok(false)
                } else {
                    Err(FabricError::Conflict)
                };
            }
            if run.is_terminal() {
                return Ok(false);
            }
            let mut schema = None;
            for sub in &run.definition.subscriptions {
                if sub.from == "$external" && sub.events.contains(&event.kind) {
                    let input = &run.descriptors[&sub.subscriber].inputs[&event.kind];
                    validate_value(input, &event.payload, "external.payload")?;
                    schema = Some(input.clone());
                }
            }
            let schema =
                schema.ok_or_else(|| invalid("external.event", "no declared subscriber"))?;
            run.external.insert(id.to_string(), event.clone());
            let scoped_id = EventId::stable(
                "static-agent-ingress",
                &format!("{}:{id}", root(tenant, run_id)),
            );
            let fact = make_event(
                tenant,
                run_id,
                "$external",
                event.clone(),
                scoped_id,
                schema,
            );
            match commit(self.fabric.as_ref(), Some(version), run, vec![fact], None).await {
                Err(FabricError::Fenced) => continue,
                result => return result.map(|_| true),
            }
        }
    }
}
struct Handler {
    compiled: Arc<Compiled>,
    resources: AgentResources,
    tenant: String,
    run_id: RunId,
    instance: String,
    cancellation: RunCancellation,
}
impl Handler {
    async fn ack(&self, delivery: &ClaimedDelivery) -> Result<(), FabricError> {
        self.resources
            .fabric
            .execute(FabricCommand::Commit {
                lease: delivery.lease.clone(),
                now_ms: now(),
                result: json!({"ignored":true}),
                outbox: vec![],
            })
            .await
            .map(|_| ())
    }
    async fn handle(&self, delivery: ClaimedDelivery) -> Result<(), FabricError> {
        let fabric = self.resources.fabric.as_ref();
        let mut incoming = published(&delivery.event);
        let desc = &self.compiled.descriptors[&self.instance];
        let base_topic = topic(&self.tenant, self.run_id, &incoming.from, &incoming.kind);
        let expected_topic = if desc.stateless_parallel {
            incoming.id = delivery
                .event
                .envelope
                .causation_id
                .ok_or(FabricError::Conflict)?;
            lane_topic(
                &base_topic,
                &self.instance,
                delivery_lane(
                    &self.compiled.definition.operators[&self.instance],
                    &delivery.event,
                ),
            )
        } else {
            base_topic
        };
        let invocation_key = if desc.stateless_parallel {
            format!("{}:{}", self.instance, incoming.id)
        } else {
            self.instance.clone()
        };
        if delivery.event.envelope.run_id != self.run_id
            || delivery.event.envelope.tenant_id != self.tenant
            || delivery.event.topic != expected_topic
            || !self.compiled.definition.subscriptions.iter().any(|s| {
                s.subscriber == self.instance
                    && s.from == incoming.from
                    && s.events.contains(&incoming.kind)
            })
        {
            return Err(invalid("delivery", "source or scope mismatch"));
        }
        validate_value(
            desc.inputs
                .get(&incoming.kind)
                .ok_or_else(|| invalid("event", "undeclared input"))?,
            &incoming.payload,
            "event.payload",
        )?;
        let state = loop {
            let (version, mut run) = read(fabric, &self.tenant, self.run_id).await?;
            let terminal = &run.definition.lifecycle.terminal;
            let is_terminal_event = incoming.kind == "engine.terminal.v1"
                || (incoming.from == terminal.from
                    && [&terminal.completed, &terminal.failed, &terminal.cancelled]
                        .contains(&&incoming.kind));
            if run.is_terminal() && !is_terminal_event {
                return self.ack(&delivery).await;
            }
            if run.deliveries >= run.definition.limits.max_deliveries {
                finish(
                    fabric,
                    &self.tenant,
                    self.run_id,
                    GraphStatus::Failed,
                    Some("delivery_budget_exhausted"),
                )
                .await?;
                return self.ack(&delivery).await;
            }
            if !run.is_terminal() && now() >= run.deadline_ms {
                finish(
                    fabric,
                    &self.tenant,
                    self.run_id,
                    GraphStatus::Failed,
                    Some("run_timeout"),
                )
                .await?;
                return self.ack(&delivery).await;
            }
            if let Some(previous) = run.inflight.get(&invocation_key) {
                if previous.expires_at_ms > now() {
                    return self
                        .resources
                        .fabric
                        .execute(FabricCommand::Retry {
                            lease: delivery.lease.clone(),
                            now_ms: now(),
                            backoff_ms: 10,
                            max_attempts: u32::MAX,
                            error: "node_busy".into(),
                        })
                        .await
                        .map(|_| ());
                }
                if !desc.replay_safe || previous.event_id != incoming.id {
                    finish(
                        fabric,
                        &self.tenant,
                        self.run_id,
                        GraphStatus::Failed,
                        Some("interrupted_operator"),
                    )
                    .await?;
                    return self.ack(&delivery).await;
                }
            }
            let state = run
                .states
                .get(&self.instance)
                .cloned()
                .unwrap_or(Value::Null);
            run.deliveries += 1;
            run.inflight.insert(
                invocation_key.clone(),
                Invocation {
                    event_id: incoming.id,
                    expires_at_ms: delivery.lease.expires_at_ms,
                },
            );
            match fabric
                .execute(FabricCommand::GuardedTransact {
                    lease: delivery.lease.clone(),
                    now_ms: now(),
                    checkpoint: CheckpointWrite {
                        key: root(&self.tenant, self.run_id),
                        expected: Some(version),
                        value: serde_json::to_value(run).map_err(|e| invalid("run", e))?,
                    },
                    outbox: vec![],
                })
                .await
            {
                Err(FabricError::Fenced) if now() < delivery.lease.expires_at_ms => continue,
                Err(error) => return Err(error),
                Ok(_) => break state,
            }
        };
        let (_, snapshot) = read(fabric, &self.tenant, self.run_id).await?;
        let terminal_observer = snapshot.is_terminal() && incoming.kind == "engine.terminal.v1";
        let context = GraphContext {
            tenant: self.tenant.clone(),
            run_id: self.run_id,
            instance: self.instance.clone(),
            event: incoming.clone(),
            state,
            cancellation: if terminal_observer {
                RunCancellation::new()
            } else {
                self.cancellation.child()
            },
        };
        let remaining = snapshot.deadline_ms.saturating_sub(now()).max(1) as u64;
        let execution = self.compiled.operators[&self.instance].handle(context);
        let execution =
            tokio::time::timeout(std::time::Duration::from_millis(remaining), execution);
        tokio::pin!(execution);
        let mut activity = FabricWatch::new(fabric);
        let output = loop {
            tokio::select! {
                result = &mut execution => break match result {
                    Ok(Ok(output)) => Some(output),
                    Ok(Err(_)) => { finish(fabric, &self.tenant, self.run_id, GraphStatus::Failed, Some("operator_failed")).await?; None },
                    Err(_) => { self.cancellation.cancel(); finish(fabric, &self.tenant, self.run_id, GraphStatus::Failed, Some("run_timeout")).await?; None },
                },
                _ = self.cancellation.cancelled(), if !terminal_observer => break None,
                _ = activity.wait(std::time::Duration::from_secs(1)), if !snapshot.is_terminal() => {
                    if read(fabric, &self.tenant, self.run_id).await?.1.is_terminal() {
                        self.cancellation.cancel();
                        break None;
                    }
                },
            }
        };
        let Some(output) = output else {
            return self.ack(&delivery).await;
        };
        if desc.stateless_parallel && !output.state.is_null() {
            finish(
                fabric,
                &self.tenant,
                self.run_id,
                GraphStatus::Failed,
                Some("parallel_operator_mutated_state"),
            )
            .await?;
            return self.ack(&delivery).await;
        }
        if output.events.len() > MAX_BATCH_ITEMS {
            finish(
                fabric,
                &self.tenant,
                self.run_id,
                GraphStatus::Failed,
                Some("output_capacity_exceeded"),
            )
            .await?;
            return self.ack(&delivery).await;
        }
        for event in &output.events {
            let Some(schema) = desc.outputs.get(&event.kind) else {
                finish(
                    fabric,
                    &self.tenant,
                    self.run_id,
                    GraphStatus::Failed,
                    Some("undeclared_output"),
                )
                .await?;
                return self.ack(&delivery).await;
            };
            if validate_value(schema, &event.payload, "output.payload").is_err() {
                finish(
                    fabric,
                    &self.tenant,
                    self.run_id,
                    GraphStatus::Failed,
                    Some("invalid_output"),
                )
                .await?;
                return self.ack(&delivery).await;
            }
        }
        loop {
            let (version, mut run) = read(fabric, &self.tenant, self.run_id).await?;
            // Cancellation or another terminal producer won the CAS. Never publish late effects.
            if run.is_terminal() {
                if run
                    .inflight
                    .get(&invocation_key)
                    .is_some_and(|i| i.event_id == incoming.id)
                {
                    run.inflight.remove(&invocation_key);
                    if snapshot.is_terminal() {
                        run.states
                            .insert(self.instance.clone(), output.state.clone());
                    }
                }
                match commit(
                    fabric,
                    Some(version),
                    run,
                    vec![],
                    Some(delivery.lease.clone()),
                )
                .await
                {
                    Err(FabricError::Fenced) if now() < delivery.lease.expires_at_ms => continue,
                    Err(FabricError::Capacity) => {
                        finish(
                            fabric,
                            &self.tenant,
                            self.run_id,
                            GraphStatus::Failed,
                            Some("output_capacity_exceeded"),
                        )
                        .await?;
                        return self.ack(&delivery).await;
                    }
                    result => return result,
                }
            }
            if now() >= run.deadline_ms {
                finish(
                    fabric,
                    &self.tenant,
                    self.run_id,
                    GraphStatus::Failed,
                    Some("run_timeout"),
                )
                .await?;
                return self.ack(&delivery).await;
            }
            if run
                .inflight
                .get(&invocation_key)
                .is_none_or(|i| i.event_id != incoming.id)
            {
                return Err(FabricError::Fenced);
            }
            run.inflight.remove(&invocation_key);
            run.states
                .insert(self.instance.clone(), output.state.clone());
            run.usage = accumulate_usage(run.usage, output.usage);
            run.step_usage.push(GraphStepUsage {
                instance: self.instance.clone(),
                event_id: incoming.id,
                usage: output.usage,
                cumulative_usage: run.usage,
            });
            let mut events = vec![];
            for (index, event) in output.events.iter().enumerate() {
                let mut fact = make_event(
                    &self.tenant,
                    self.run_id,
                    &self.instance,
                    event.clone(),
                    EventId::stable(
                        "static-agent-output",
                        &format!("{}:{}:{index}", incoming.id, self.instance),
                    ),
                    desc.outputs[&event.kind].clone(),
                );
                fact.envelope.operator.version = desc.version;
                fact.envelope.causation_id = Some(incoming.id);
                events.push(fact);
                let terminal = &run.definition.lifecycle.terminal;
                if terminal.from == self.instance {
                    if event.kind == terminal.completed {
                        run.status = GraphStatus::Completed;
                        run.output = Some(event.payload.clone());
                    } else if event.kind == terminal.failed {
                        run.status = GraphStatus::Failed;
                        run.output = Some(event.payload.clone());
                        run.error = Some(
                            event.payload["code"]
                                .as_str()
                                .unwrap_or("agent_failed")
                                .into(),
                        );
                    } else if event.kind == terminal.cancelled {
                        run.status = GraphStatus::Cancelled;
                    }
                }
                if run.is_terminal() {
                    break;
                }
            }
            if run.is_terminal() {
                events.push(terminal_event(&run));
            }
            match commit(
                fabric,
                Some(version),
                run,
                events,
                Some(delivery.lease.clone()),
            )
            .await
            {
                Err(FabricError::Fenced) if now() < delivery.lease.expires_at_ms => continue,
                Err(FabricError::Capacity) => {
                    finish(
                        fabric,
                        &self.tenant,
                        self.run_id,
                        GraphStatus::Failed,
                        Some("output_capacity_exceeded"),
                    )
                    .await?;
                    return self.ack(&delivery).await;
                }
                result => return result,
            }
        }
    }
}
impl TransactionalOperator for Handler {
    fn execute<'a>(
        &'a self,
        deliveries: Vec<ClaimedDelivery>,
        _: &'a (dyn Fn() -> i64 + Sync),
    ) -> Pin<Box<dyn Future<Output = Result<(), FabricError>> + Send + 'a>> {
        Box::pin(async move {
            if self.compiled.descriptors[&self.instance].stateless_parallel {
                use futures_util::StreamExt;
                let results: Vec<_> = futures_util::stream::iter(deliveries)
                    .map(|delivery| self.handle(delivery))
                    .buffer_unordered(16)
                    .collect()
                    .await;
                for result in results {
                    result?;
                }
            } else {
                for delivery in deliveries {
                    self.handle(delivery).await?;
                }
            }
            Ok(())
        })
    }
}
struct Worker {
    cancellation: RunCancellation,
    task: JoinHandle<()>,
}
pub struct GraphEngine {
    definition: AgentDefinition,
    registry: OperatorRegistry,
    resources: AgentResources,
    active: Mutex<HashMap<(String, RunId), Worker>>,
}
pub struct ConfiguredAgent {
    pub engine: Arc<GraphEngine>,
}
impl ConfiguredAgent {
    pub async fn run(&self, input: impl Into<Value>) -> Result<GraphRun, FabricError> {
        self.engine.run("local", input.into()).await
    }
    /// Fire a named manual trigger, creating an independent Run.
    pub async fn trigger(
        &self,
        trigger: &str,
        payload: impl Into<Value>,
    ) -> Result<GraphRun, FabricError> {
        self.engine.trigger("local", trigger, payload.into()).await
    }
}
impl GraphEngine {
    pub(super) fn new(
        compiled: Compiled,
        registry: OperatorRegistry,
        resources: AgentResources,
    ) -> Self {
        Self {
            definition: compiled.definition,
            registry,
            resources,
            active: Mutex::new(HashMap::new()),
        }
    }
    pub fn definition(&self) -> &AgentDefinition {
        &self.definition
    }
    pub async fn run(
        self: &Arc<Self>,
        tenant: &str,
        input: Value,
    ) -> Result<GraphRun, FabricError> {
        self.start(tenant, RunId::new(), input).await
    }
    pub async fn trigger(
        self: &Arc<Self>,
        tenant: &str,
        trigger: &str,
        payload: Value,
    ) -> Result<GraphRun, FabricError> {
        self.start_trigger(tenant, RunId::new(), trigger, payload)
            .await
    }
    /// A stable RunId deduplicates retries of the same trigger occurrence.
    pub async fn start_trigger(
        self: &Arc<Self>,
        tenant: &str,
        run_id: RunId,
        trigger: &str,
        payload: Value,
    ) -> Result<GraphRun, FabricError> {
        self.admit_trigger(tenant, run_id, trigger, payload).await?;
        self.recover(tenant, run_id).await
    }
    pub async fn start(
        self: &Arc<Self>,
        tenant: &str,
        run_id: RunId,
        input: Value,
    ) -> Result<GraphRun, FabricError> {
        self.admit(tenant, run_id, input).await?;
        self.recover(tenant, run_id).await
    }
    /// Persist immutable input and the start event without running external work.
    pub async fn admit(
        &self,
        tenant: &str,
        run_id: RunId,
        input: Value,
    ) -> Result<GraphRun, FabricError> {
        self.admit_trigger(
            tenant,
            run_id,
            &self.definition.lifecycle.start.trigger,
            input,
        )
        .await
    }
    /// Persist a trigger occurrence and its initial event atomically, without execution.
    pub async fn admit_trigger(
        &self,
        tenant: &str,
        run_id: RunId,
        trigger: &str,
        input: Value,
    ) -> Result<GraphRun, FabricError> {
        if tenant.trim().is_empty() || tenant.len() > 128 {
            return Err(invalid("tenant", "invalid tenant"));
        }
        if self
            .resources
            .agent_id
            .as_ref()
            .is_some_and(|id| id.trim().is_empty() || id.len() > 128)
        {
            return Err(invalid("agent_id", "invalid host identity"));
        }
        let compiled = compile(self.definition.clone(), &self.registry, &self.resources)?;
        compiled
            .definition
            .validate_trigger(&self.registry, trigger, &input)?;
        let source = &compiled.definition.triggers[trigger];
        let run = GraphRun {
            tenant: tenant.into(),
            agent_id: self.resources.agent_id.clone(),
            run_id,
            definition: compiled.definition.clone(),
            input: input.clone(),
            trigger: trigger.into(),
            status: GraphStatus::Running,
            output: None,
            error: None,
            deadline_ms: now().saturating_add(compiled.definition.limits.timeout_ms as i64),
            deliveries: 0,
            event_count: 0,
            cleanup_complete: false,
            usage: None,
            step_usage: vec![],
            external: BTreeMap::new(),
            bindings: compiled.bindings.clone(),
            descriptors: compiled.descriptors.clone(),
            states: BTreeMap::new(),
            inflight: BTreeMap::new(),
        };
        let input_schema = compiled
            .definition
            .subscriptions
            .iter()
            .find(|s| s.from == trigger)
            .map(|s| compiled.descriptors[&s.subscriber].inputs[&source.event].clone())
            .expect("validated input");
        let event = make_event(
            tenant,
            run_id,
            trigger,
            GraphEvent::new(&source.event, input),
            EventId::stable("static-agent-start", &root(tenant, run_id)),
            input_schema,
        );
        match commit(
            self.resources.fabric.as_ref(),
            None,
            run.clone(),
            vec![event],
            None,
        )
        .await
        {
            Err(FabricError::Fenced) => {
                let saved = self.get_run(tenant, run_id).await?;
                if saved.agent_id != run.agent_id
                    || saved.trigger != run.trigger
                    || saved.input != run.input
                    || saved.definition != run.definition
                    || saved.bindings != run.bindings
                    || saved.descriptors != run.descriptors
                {
                    return Err(invalid("run", "immutable input/graph conflict"));
                }
            }
            Err(e) => return Err(e),
            Ok(()) => {}
        }
        self.get_run(tenant, run_id).await
    }
    pub async fn get_run(&self, tenant: &str, run: RunId) -> Result<GraphRun, FabricError> {
        Ok(read(self.resources.fabric.as_ref(), tenant, run).await?.1)
    }
    /// Resume the last atomically committed state, including pending triggers and waits.
    /// Does not replay historical output or rewind uncertain external effects.
    pub async fn recover_run(
        self: &Arc<Self>,
        tenant: &str,
        run: RunId,
    ) -> Result<GraphRun, FabricError> {
        self.recover(tenant, run).await
    }
    pub async fn recover(
        self: &Arc<Self>,
        tenant: &str,
        run: RunId,
    ) -> Result<GraphRun, FabricError> {
        let saved = self.get_run(tenant, run).await?;
        if self.resources.agent_id.is_some() && saved.agent_id != self.resources.agent_id {
            return Err(invalid("recovery", "agent identity changed"));
        }
        let compiled = compile(saved.definition.clone(), &self.registry, &self.resources)?;
        compiled
            .definition
            .validate_trigger(&self.registry, &saved.trigger, &saved.input)?;
        if compiled.bindings != saved.bindings || compiled.descriptors != saved.descriptors {
            return Err(invalid("recovery", "resource or operator contract changed"));
        }
        self.spawn(tenant, run, Arc::new(compiled)).await?;
        Ok(saved)
    }
    async fn bus(
        &self,
        tenant: &str,
        run: RunId,
        compiled: Arc<Compiled>,
        cancellation: RunCancellation,
    ) -> Result<OperatorBus, FabricError> {
        let mut bus = OperatorBus::new(
            self.resources.fabric.clone(),
            FabricRuntimeConfig {
                worker: format!("graph-{}", std::process::id()),
                claim_limit: 16,
                lease_ms: compiled
                    .definition
                    .limits
                    .timeout_ms
                    .saturating_add(60_000)
                    .min(i64::MAX as u64) as i64,
                max_attempts: u32::MAX,
                backoff_ms: 10,
                max_backoff_ms: 1000,
            },
        );
        for name in compiled.operators.keys() {
            bus.register_transactional(
                consumer(tenant, run, name),
                Arc::new(Handler {
                    compiled: compiled.clone(),
                    resources: self.resources.clone(),
                    tenant: tenant.into(),
                    run_id: run,
                    instance: name.clone(),
                    cancellation: cancellation.clone(),
                }),
            )?;
        }
        for sub in &compiled.definition.subscriptions {
            for event in &sub.events {
                let base = topic(tenant, run, &sub.from, event);
                let parallel = compiled.descriptors[&sub.subscriber].stateless_parallel;
                for lane in 0..if parallel { PARALLEL_LANES } else { 1 } {
                    bus.subscribe_events(SubscriptionSpec {
                        consumer: consumer(tenant, run, &sub.subscriber),
                        subscription: format!("{}:{event}:{lane}", sub.id),
                        topic: if parallel {
                            lane_topic(&base, &sub.subscriber, lane)
                        } else {
                            base.clone()
                        },
                        event_type: Some(event.clone()),
                        tenant: Some(tenant.into()),
                    })
                    .await?;
                }
            }
        }
        Ok(bus)
    }
    async fn spawn(
        self: &Arc<Self>,
        tenant: &str,
        run: RunId,
        compiled: Arc<Compiled>,
    ) -> Result<(), FabricError> {
        if self.get_run(tenant, run).await?.cleanup_complete {
            return Ok(());
        }
        let mut active = self.active.lock().await;
        let key = (tenant.to_string(), run);
        if active.contains_key(&key) {
            return Ok(());
        }
        let cancellation = RunCancellation::new();
        let bus = self
            .bus(tenant, run, compiled, cancellation.clone())
            .await?;
        let engine = self.clone();
        let tenant = tenant.to_string();
        let task = tokio::spawn(async move {
            let mut activity = FabricWatch::new(engine.resources.fabric.as_ref());
            loop {
                activity.observe();
                let mut fallback = std::time::Duration::from_secs(1);
                if matches!(engine.get_run(&tenant, run).await, Err(FabricError::Invalid(reason)) if reason == "retention_expired")
                {
                    break;
                }
                if let Ok(state) = engine.get_run(&tenant, run).await
                    && !state.is_terminal()
                {
                    fallback = std::time::Duration::from_millis(
                        state.deadline_ms.saturating_sub(now()).clamp(1, 1000) as u64,
                    );
                }
                if let Ok(state) = engine.get_run(&tenant, run).await
                    && !state.is_terminal()
                    && now() >= state.deadline_ms
                {
                    let _ = finish(
                        engine.resources.fabric.as_ref(),
                        &tenant,
                        run,
                        GraphStatus::Failed,
                        Some("run_timeout"),
                    )
                    .await;
                }
                if let Err(error) = bus.tick_ready(now, 16).await {
                    tracing::warn!(%run,%error,"static graph delivery deferred");
                }
                if engine
                    .get_run(&tenant, run)
                    .await
                    .is_ok_and(|r| r.is_terminal())
                    && bus.is_idle().await.unwrap_or(false)
                {
                    let mut cleaned = true;
                    for (_, bridge) in engine.resources.waits.values() {
                        if let Err(error) = bridge.cleanup_run(&tenant, run).await {
                            cleaned = false;
                            tracing::warn!(%run,%error,"graph wait cleanup deferred");
                        }
                    }
                    if cleaned {
                        match read(engine.resources.fabric.as_ref(), &tenant, run).await {
                            Ok((version, mut saved)) => {
                                saved.cleanup_complete = true;
                                if commit(
                                    engine.resources.fabric.as_ref(),
                                    Some(version),
                                    saved,
                                    vec![],
                                    None,
                                )
                                .await
                                .is_ok()
                                {
                                    break;
                                }
                            }
                            Err(FabricError::Invalid(reason)) if reason == "retention_expired" => {
                                break;
                            }
                            _ => {}
                        }
                    }
                }
                activity.wait(fallback).await;
            }
            for resource in engine.resources.toolsets.values() {
                resource.port.close_run(run);
            }
            engine.active.lock().await.remove(&(tenant, run));
        });
        active.insert(key, Worker { cancellation, task });
        Ok(())
    }
    pub async fn fail(&self, tenant: &str, run: RunId, code: &str) -> Result<(), FabricError> {
        finish(
            self.resources.fabric.as_ref(),
            tenant,
            run,
            GraphStatus::Failed,
            Some(code),
        )
        .await
    }
    pub async fn cancel(&self, tenant: &str, run: RunId) -> Result<(), FabricError> {
        finish(
            self.resources.fabric.as_ref(),
            tenant,
            run,
            GraphStatus::Cancelled,
            None,
        )
        .await?;
        if let Some(worker) = self.active.lock().await.get(&(tenant.into(), run)) {
            worker.cancellation.cancel();
        }
        Ok(())
    }
    pub async fn stop_workers(&self) {
        let workers: Vec<_> = self.active.lock().await.drain().collect();
        for ((tenant, run), worker) in workers {
            worker.cancellation.cancel();
            worker.task.abort();
            let _ = worker.task.await;
            if self
                .get_run(&tenant, run)
                .await
                .is_ok_and(|run| run.is_terminal())
            {
                for (_, bridge) in self.resources.waits.values() {
                    if let Err(error) = bridge.cleanup_run(&tenant, run).await {
                        tracing::warn!(%run,%error,"graph wait cleanup requires recovery");
                    }
                }
            }
            for r in self.resources.toolsets.values() {
                r.port.close_run(run);
            }
        }
    }
    pub async fn events_after(
        &self,
        tenant: &str,
        run: RunId,
        after: u64,
        limit: usize,
    ) -> Result<Vec<PublishedGraphEvent>, FabricError> {
        if limit == 0 || limit > 128 {
            return Err(invalid("limit", "expected 1..128"));
        }
        match self
            .resources
            .fabric
            .execute(FabricCommand::ReadLog {
                log: log_key(tenant, run),
                after,
                limit,
            })
            .await?
        {
            FabricReply::Log(Some(page)) => page
                .items
                .into_iter()
                .map(|v| serde_json::from_value(v).map_err(|e| invalid("event", e)))
                .collect(),
            FabricReply::Log(None) => Ok(vec![]),
            _ => Err(FabricError::Conflict),
        }
    }
    /// Bounded pull-based subscription. Cursor is the count of consumed log events.
    pub fn subscribe(
        self: &Arc<Self>,
        tenant: impl Into<String>,
        run: RunId,
        after: u64,
    ) -> Pin<Box<dyn futures_core::Stream<Item = Result<PublishedGraphEvent, FabricError>> + Send>>
    {
        let engine = self.clone();
        let tenant = tenant.into();
        Box::pin(async_stream::stream! {let mut cursor=after;
        let mut activity = FabricWatch::new(engine.resources.fabric.as_ref());
        loop { activity.observe();
            match engine.events_after(&tenant,run,cursor,128).await {
                Ok(events) if !events.is_empty()=>{for event in events{cursor+=1;yield Ok(event);}continue;},
                Err(error)=>{yield Err(error);return;},_=>{}
            }
            match engine.get_run(&tenant,run).await {Ok(state) if state.is_terminal() && cursor>=state.event_count=>return,Err(error)=>{yield Err(error);return;},_=>{}}
            activity.wait(std::time::Duration::from_secs(1)).await;
        }})
    }
}
