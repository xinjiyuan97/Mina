//! Convenience Agent adapter over the durable static graph host.
use super::*;
use crate::contracts::*;
use futures_util::StreamExt;
use serde_json::json;
use sha2::{Digest, Sha256};

#[derive(Clone)]
pub struct GraphAgent {
    definition: AgentDefinition,
    registry: OperatorRegistry,
    resources: AgentResources,
    approvals: Arc<dyn ApprovalPort>,
    preparation: Option<(
        Arc<dyn crate::agent::host::preparation::ContextPreparer>,
        crate::contracts::preparation::ContextPolicy,
    )>,
}
impl GraphAgent {
    pub fn new(
        definition: AgentDefinition,
        registry: OperatorRegistry,
        resources: AgentResources,
    ) -> Result<Self, FabricError> {
        definition.validate(&registry, &resources)?;
        Ok(Self {
            definition,
            registry,
            resources,
            approvals: Arc::new(RejectAllApprovals),
            preparation: None,
        })
    }
    pub fn with_approval_port(mut self, approvals: Arc<dyn ApprovalPort>) -> Self {
        self.approvals = approvals;
        self
    }
    pub fn with_context_preparation(
        mut self,
        preparer: Arc<dyn crate::agent::host::preparation::ContextPreparer>,
        policy: crate::contracts::preparation::ContextPolicy,
    ) -> Self {
        self.preparation = Some((preparer, policy));
        self
    }
    pub fn definition(&self) -> &AgentDefinition {
        &self.definition
    }
}
pub(super) struct JournalModel {
    pub(super) toolsets: Vec<Arc<dyn ToolPort>>,
    pub(super) port: Arc<dyn ModelPort>,
    pub(super) journal: super::host::PublicJournal,
}
impl ModelPort for JournalModel {
    fn stream(&self, request: ModelRequest) -> ModelEventStream {
        let mut protected = BTreeSet::new();
        let mut unknown = false;
        for port in &self.toolsets {
            match port.tool_set_snapshot(request.run_id) {
                Ok(snapshot) => protected.extend(
                    snapshot
                        .bindings
                        .into_iter()
                        .filter(|b| b.argument_visibility == ToolArgumentVisibility::DigestOnly)
                        .map(|b| b.name),
                ),
                Err(_) => unknown = true,
            }
        }
        let run = request.run_id;
        let mut stream = self.port.stream(request);
        let journal = self.journal.clone();
        Box::pin(async_stream::stream! {
            let mut digests: BTreeMap<String, (Sha256, usize)> = BTreeMap::new();
            let mut visible = BTreeSet::new();
            while let Some(event) = stream.next().await {
                if let ModelEvent::ToolCallStarted { call_id, name } = &event {
                    if unknown || protected.contains(name) {
                        digests.entry(call_id.clone()).or_insert_with(|| (Sha256::new(), 0));
                    } else { visible.insert(call_id.clone()); }
                }
                if matches!(event, ModelEvent::Completed { .. } | ModelEvent::Failed { .. }) {
                    for (call_id, (digest, bytes)) in std::mem::take(&mut digests) {
                        let delta = json!({"redacted":true,"bytes":bytes,"digest":format!("sha256:{:x}",digest.finalize())}).to_string();
                        if let Err(_error) = journal.append(run, AgentEvent::ToolCallArgumentsDelta { call_id, delta }).await {
                            yield ModelEvent::Failed { error: ModelError::new(ModelErrorKind::Internal, "Could not persist model output", true) }; return;
                        }
                    }
                }
                let preview = match &event {
                    ModelEvent::ReasoningStarted { redacted } => Some(AgentEvent::ReasoningStarted { redacted: *redacted }),
                    ModelEvent::ReasoningDelta { delta } => Some(AgentEvent::OutputDelta { channel: OutputChannel::AssistantReasoning, delta: delta.clone() }),
                    ModelEvent::ReasoningCompleted { redacted } => Some(AgentEvent::ReasoningCompleted { redacted: *redacted }),
                    ModelEvent::TextDelta { delta } => Some(AgentEvent::OutputDelta { channel: OutputChannel::AssistantText, delta: delta.clone() }),
                    ModelEvent::ToolCallStarted { call_id, name } => Some(AgentEvent::ToolCallStarted { call_id: call_id.clone(), name: name.clone() }),
                    ModelEvent::ToolCallArgumentsDelta { call_id, delta } => {
                        // Unannounced calls fail the model protocol, but their arguments
                        // must not leak through the public journal before that failure.
                        if !visible.contains(call_id) { digests.entry(call_id.clone()).or_insert_with(|| (Sha256::new(), 0)); }
                        if let Some((digest, bytes)) = digests.get_mut(call_id) {
                            digest.update(delta.as_bytes());
                            *bytes = bytes.saturating_add(delta.len());
                            None
                        } else { Some(AgentEvent::ToolCallArgumentsDelta { call_id: call_id.clone(), delta: delta.clone() }) }
                    },
                    _ => None,
                };
                if let Some(preview) = preview && let Err(_error) = journal.append(run, preview).await {
                    yield ModelEvent::Failed { error: ModelError::new(ModelErrorKind::Internal, "Could not persist model output", true) }; return;
                }
                yield event;
            }
        })
    }
}
struct StopOnDrop(Arc<GraphEngine>, RunId);
impl Drop for StopOnDrop {
    fn drop(&mut self) {
        let engine = self.0.clone();
        let run_id = self.1;
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let _ = engine.cancel("local", run_id).await;
                engine.stop_workers().await;
            });
        }
    }
}
impl Agent for GraphAgent {
    fn metadata(&self) -> AgentMetadata {
        AgentMetadata::new(&self.definition.name, env!("CARGO_PKG_VERSION"))
            .with_capability("event_stream")
            .with_capability("static_graph")
    }
    fn run(&self, mut request: RunRequest) -> AgentEventStream {
        let preparation = self.preparation.clone();
        let mut definition = self.definition.clone();
        let registry = self.registry.clone();
        let resources = self.resources.clone();
        let approvals = self.approvals.clone();
        let ingress = GraphIngress::new(resources.fabric.clone());
        Box::pin(async_stream::stream! {
            if request.cancellation.is_cancelled() { yield AgentEvent::Cancelled; return; }
            if let Some((preparer, mut policy)) = preparation {
                if let Some(allowed) = &request.allowed_tools { policy.available_tools.retain(|name| allowed.contains(name)); }
                policy.allow_run_adf &= request.allow_run_adf;
                let max = request.max_steps.unwrap_or_else(|| definition.operators.values().filter(|op| op.kind == "agent.context").filter_map(|op|op.config["max_steps"].as_u64()).min().unwrap_or(1) as u32);
                let prepared = tokio::select! {
                    _ = request.cancellation.cancelled() => { yield AgentEvent::Cancelled; return; }
                    result = crate::agent::host::preparation::prepare_agent_request(preparer.as_ref(), &request, policy, max) => result,
                };
                match prepared {
                    Ok(plan) => { request.prior_messages = plan.prior_messages; request.allowed_tools = plan.allowed_tools; request.allow_run_adf = plan.allow_run_adf; request.max_steps = plan.max_steps; }
                    Err(error) => { yield AgentEvent::failed(error.code, "Context preparation failed", false); return; }
                }
            }
            if let Err(error) = definition.constrain_run(&crate::agent::RunOptions {
                allowed_tools: request.allowed_tools.clone(), allow_run_adf: request.allow_run_adf,
                max_steps: request.max_steps,
            }) {
                yield AgentEvent::failed("graph_invalid", error.to_string(), false); return;
            }
            let host = match GraphHost::new("local", definition, registry, resources) {
                Ok(host) => host,
                Err(error) => { yield AgentEvent::failed("graph_invalid", error.to_string(), false); return; }
            };
            let _guard = StopOnDrop(host.engine.clone(), request.run_id);
            let run_id = request.run_id;
            let input = json!({"input":request.input,"attachments":request.attachments,"prior_messages":request.prior_messages});
            if let Err(error) = host.engine.start("local", run_id, input).await {
                yield AgentEvent::failed("graph_start_failed", error.to_string(), false); return;
            }
            let mut events = host.engine.subscribe("local", run_id, 0);
            let mut public = host.subscribe(run_id, 0);
            let mut graph_ended = false;
            type Decision = Pin<Box<dyn Future<Output = (crate::contracts::events::EventId, GraphEvent)> + Send>>;
            let mut decisions = futures_util::stream::FuturesUnordered::<Decision>::new();
            loop {
                tokio::select! {
                    biased;
                    _ = request.cancellation.cancelled() => {
                        let _ = host.engine.cancel("local", run_id).await;
                        host.engine.stop_workers().await;
                        yield AgentEvent::Cancelled; return;
                    }
                    Some((id, decision)) = decisions.next(), if !decisions.is_empty() => {
                        if let Err(error) = ingress.send("local", run_id, id, decision).await {
                            yield AgentEvent::failed("approval_delivery_failed", error.to_string(), true); return;
                        }
                    }
                    next = public.next() => {
                        match next {
                            Some(Ok(event)) => {
                                let terminal = event.kind.is_terminal();
                                if let Some(event) = semantic_event(event.kind) { yield event; }
                                if terminal { host.engine.stop_workers().await; return; }
                            }
                            Some(Err(error)) => { yield AgentEvent::failed("graph_read_failed", error.to_string(), true); return; }
                            None => { yield AgentEvent::failed("graph_missing_terminal", "Graph ended without a terminal event", false); return; }
                        }
                    }
                    next = events.next(), if !graph_ended => {
                        let event = match next {
                            Some(Ok(event)) => event,
                            Some(Err(error)) => { yield AgentEvent::failed("graph_read_failed", error.to_string(), true); return; }
                            None => { graph_ended = true; continue; }
                        };
                        if event.kind == "approval.requested.v1" {
                            let approval: ApprovalRequest = match serde_json::from_value(event.payload.clone()) {
                                Ok(approval) => approval,
                                Err(error) => { yield AgentEvent::failed("approval_invalid", error.to_string(), false); return; }
                            };
                            let port = approvals.clone();
                            let expires = event.payload["expires_at_ms"].as_i64().unwrap_or(0);
                            let binding = event.payload["binding_digest"].clone();
                            decisions.push(Box::pin(async move {
                                let remaining = expires.saturating_sub(crate::agent::unix_time_ms()).max(0) as u64;
                                let result = tokio::time::timeout(std::time::Duration::from_millis(remaining), port.request(approval.clone())).await;
                                let event = match result {
                                    Ok(result) => GraphEvent::new("approval.decided.v1", json!({"approval_id":approval.approval_id,"actor":"host","binding_digest":binding,"resolution":result.unwrap_or_else(|_| ApprovalResolution::deny(Some("Approval provider failed".into())))})),
                                    Err(_) => GraphEvent::new("approval.expire.v1", json!({"approval_id":approval.approval_id})),
                                };
                                (crate::contracts::events::EventId::stable("graph-host-approval", &approval.approval_id.to_string()), event)
                            }));
                        }

                    }
                }
            }
        })
    }
}

fn semantic_event(kind: RunEventKind) -> Option<AgentEvent> {
    if matches!(
        kind,
        RunEventKind::RunStarted
            | RunEventKind::RunWaiting { .. }
            | RunEventKind::RunResumed { .. }
    ) {
        return None;
    }
    // Run events intentionally share the semantic payload schema with AgentEvent.
    let mut value = serde_json::to_value(kind).expect("event serialization");
    match value["type"].as_str() {
        Some("run_completed") => value["type"] = "completed".into(),
        Some("run_cancelled") => value["type"] = "cancelled".into(),
        Some("run_failed") => value["type"] = "failed".into(),
        _ => {}
    }
    Some(serde_json::from_value(value).expect("semantic event schema"))
}
