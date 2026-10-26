//! Durable public event projection for static graphs. This host observes facts;
//! model/tool scheduling remains entirely in the subscription graph.
use super::*;
use crate::{event_runtime::EventId, harness::*, operator_fabric::*};
use serde_json::json;

#[derive(Default, Serialize, Deserialize)]
struct Cursor {
    graph: u64,
    public: u64,
    terminal: bool,
    #[serde(default)]
    toolsets: BTreeMap<String, (u64, String)>,
}
#[derive(Clone)]
pub(super) struct PublicJournal {
    fabric: Arc<dyn OperatorFabric>,
    tenant: String,
}
impl PublicJournal {
    fn key(&self, run: RunId) -> String {
        public_journal_key(&self.tenant, run)
    }
    async fn load(&self, run: RunId) -> Result<(Option<u64>, Cursor), FabricError> {
        match self
            .fabric
            .execute(FabricCommand::ReadCheckpoint(self.key(run)))
            .await?
        {
            FabricReply::Checkpoint(Some(saved)) => Ok((
                Some(saved.version),
                serde_json::from_value(saved.value).map_err(|e| invalid("public cursor", e))?,
            )),
            FabricReply::Checkpoint(None) => Ok((None, Cursor::default())),
            _ => Err(FabricError::Conflict),
        }
    }
    async fn write(
        &self,
        run: RunId,
        version: Option<u64>,
        mut cursor: Cursor,
        kinds: Vec<RunEventKind>,
    ) -> Result<(), FabricError> {
        let expected = cursor.public;
        let mut items = vec![];
        for kind in kinds {
            if cursor.terminal {
                break;
            }
            cursor.terminal = kind.is_terminal();
            cursor.public += 1;
            items.push(
                serde_json::to_value(RunEvent::new(run, cursor.public, kind))
                    .map_err(|e| invalid("public event", e))?,
            );
        }
        self.fabric
            .execute(FabricCommand::TransactLog {
                log: format!("{}:events", self.key(run)),
                expected,
                items,
                checkpoints: vec![CheckpointWrite {
                    key: self.key(run),
                    expected: version,
                    value: serde_json::to_value(cursor).map_err(|e| invalid("public cursor", e))?,
                }],
                outbox: vec![],
                delivery: None,
            })
            .await
            .map(|_| ())
    }
    /// Cursor and mapped events commit together. A crash cannot skip or duplicate a fact.
    pub(super) async fn sync(&self, run: RunId) -> Result<(), FabricError> {
        loop {
            let (version, mut cursor) = self.load(run).await?;
            // Only initialization needs the full graph checkpoint. Streaming reads
            // the small cursor and append log, not the growing conversation state.
            if version.is_none() {
                runtime::read(self.fabric.as_ref(), &self.tenant, run).await?;
            }
            if cursor.terminal {
                return Ok(());
            }
            let page = match self
                .fabric
                .execute(FabricCommand::ReadLog {
                    log: runtime::log_key(&self.tenant, run),
                    after: cursor.graph,
                    limit: 1,
                })
                .await?
            {
                FabricReply::Log(Some(page)) => page,
                FabricReply::Log(None) => return Ok(()),
                _ => return Err(FabricError::Conflict),
            };
            let Some(value) = page.items.into_iter().next() else {
                return Ok(());
            };
            let event: PublishedGraphEvent =
                serde_json::from_value(value).map_err(|e| invalid("graph event", e))?;
            let mut kinds = vec![];
            if cursor.public == 0 {
                kinds.push(RunEventKind::RunStarted);
            }
            for semantic in semantic_events(&event)? {
                if let AgentEvent::ToolSetUpdated {
                    revision, digest, ..
                } = &semantic
                {
                    let identity = (*revision, digest.clone());
                    if cursor.toolsets.get(&event.from) == Some(&identity) {
                        continue;
                    }
                    cursor.toolsets.insert(event.from.clone(), identity);
                }
                kinds.push(semantic.into_run_event_kind());
            }
            if event.kind == "wait.registered.v1" {
                kinds.push(RunEventKind::RunWaiting {
                    checkpoint_revision: cursor.graph + 1,
                    wait_count: event.payload["wait_count"].as_u64().unwrap_or(1) as u32,
                });
            }
            if event.kind == "wait.completed.v1" {
                kinds.push(RunEventKind::RunResumed {
                    activation_id: ActivationId::stable("graph-wake", &event.id.to_string()),
                    checkpoint_revision: cursor.graph + 1,
                });
            }
            cursor.graph += 1;
            match self.write(run, version, cursor, kinds).await {
                Err(FabricError::Fenced) => continue,
                result => result?,
            }
        }
    }
    pub(super) async fn append(&self, run: RunId, event: AgentEvent) -> Result<(), FabricError> {
        self.sync(run).await?;
        loop {
            let (version, cursor) = self.load(run).await?;
            if cursor.terminal {
                return Ok(());
            }
            match self
                .write(
                    run,
                    version,
                    cursor,
                    vec![event.clone().into_run_event_kind()],
                )
                .await
            {
                Err(FabricError::Fenced) => continue,
                result => return result,
            }
        }
    }
    async fn read(
        &self,
        run: RunId,
        after: u64,
        limit: usize,
    ) -> Result<Vec<RunEvent>, FabricError> {
        if limit == 0 || limit > 128 {
            return Err(invalid("limit", "expected 1..128"));
        }
        self.sync(run).await?;
        match self
            .fabric
            .execute(FabricCommand::ReadLog {
                log: format!("{}:events", self.key(run)),
                after,
                limit,
            })
            .await?
        {
            FabricReply::Log(Some(page)) => page
                .items
                .into_iter()
                .map(|item| serde_json::from_value(item).map_err(|e| invalid("public event", e)))
                .collect(),
            FabricReply::Log(None) => Ok(vec![]),
            _ => Err(FabricError::Conflict),
        }
    }
}

/// A tenant-scoped durable host. Recreate with the same resource revisions and
/// call recover after restart. Dropping an HTTP subscriber never cancels the run.
#[derive(Clone)]
pub struct GraphHost {
    pub engine: Arc<GraphEngine>,
    journal: PublicJournal,
}
impl GraphHost {
    pub fn new(
        tenant: impl Into<String>,
        definition: AgentDefinition,
        registry: OperatorRegistry,
        mut resources: AgentResources,
    ) -> Result<Self, FabricError> {
        let tenant = tenant.into();
        if tenant.trim().is_empty() || tenant.len() > 128 {
            return Err(invalid("tenant", "invalid tenant"));
        }
        let journal = PublicJournal {
            tenant,
            fabric: resources.fabric.clone(),
        };
        let toolsets: Vec<_> = resources
            .toolsets
            .values()
            .map(|r| r.port.clone())
            .collect();
        for resource in resources.models.values_mut() {
            resource.port = Arc::new(super::stream::JournalModel {
                port: resource.port.clone(),
                toolsets: toolsets.clone(),
                journal: journal.clone(),
            });
        }
        let agent = AgentBuilder::new(registry, resources).build(definition)?;
        Ok(Self {
            engine: agent.engine,
            journal,
        })
    }
    pub async fn start(&self, run: RunId, input: Value) -> Result<GraphRun, FabricError> {
        self.engine.start(&self.journal.tenant, run, input).await
    }
    pub async fn recover(&self, run: RunId) -> Result<GraphRun, FabricError> {
        let saved = self.engine.recover(&self.journal.tenant, run).await?;
        self.journal.sync(run).await?;
        Ok(saved)
    }
    async fn approval_facts(&self, run: RunId) -> Result<Vec<PublishedGraphEvent>, FabricError> {
        let mut after = 0;
        let mut facts = vec![];
        loop {
            let page = self
                .engine
                .events_after(&self.journal.tenant, run, after, 128)
                .await?;
            if page.is_empty() {
                return Ok(facts);
            }
            after += page.len() as u64;
            facts.extend(page.into_iter().filter(|e| e.kind.starts_with("approval.")));
        }
    }
    /// Trusted host ingress; the HTTP layer supplies the authenticated actor.
    pub async fn approve(
        &self,
        run: RunId,
        approval: ApprovalId,
        actor: &str,
        resolution: ApprovalResolution,
    ) -> Result<bool, FabricError> {
        if actor.trim().is_empty() {
            return Err(FabricError::Invalid("actor required".into()));
        }
        let facts = self.approval_facts(run).await?;
        let id = json!(approval);
        let request = facts
            .iter()
            .find(|e| e.kind == "approval.requested.v1" && e.payload["approval_id"] == id)
            .ok_or(FabricError::NotFound)?;
        for fact in &facts {
            if fact.payload["approval_id"] != id {
                continue;
            }
            if fact.kind == "approval.decided.v1" {
                return if fact.payload["resolution"] == json!(resolution)
                    && fact.payload["actor"] == actor
                {
                    Ok(false)
                } else {
                    Err(FabricError::Conflict)
                };
            }
        }
        let saved = self.engine.get_run(&self.journal.tenant, run).await?;
        if saved.is_terminal() {
            return Err(FabricError::Invalid(
                crate::tool_authorization::AuthorizationError::Cancelled.to_string(),
            ));
        }
        if request.payload["expires_at_ms"].as_i64().unwrap_or(0) <= crate::agent::unix_time_ms() {
            return Err(FabricError::Invalid(
                crate::tool_authorization::AuthorizationError::Expired.to_string(),
            ));
        }
        GraphIngress::new(self.journal.fabric.clone()).send(&self.journal.tenant, run,
            EventId::stable("graph-host-decision", &approval.to_string()),
            GraphEvent::new("approval.decided.v1", json!({"approval_id":approval,"binding_digest":request.payload["binding_digest"],"actor":actor,"resolution":resolution}))).await
    }
    pub async fn expire_approvals(&self, run: RunId) -> Result<(), FabricError> {
        if self
            .engine
            .get_run(&self.journal.tenant, run)
            .await?
            .is_terminal()
        {
            return Ok(());
        }
        let mut pending = BTreeMap::new();
        for event in self.approval_facts(run).await? {
            let id = event.payload["approval_id"].to_string();
            match event.kind.as_str() {
                "approval.requested.v1" => {
                    pending.insert(id, event.payload);
                }
                "approval.finished.v1" | "approval.expire.v1" | "approval.decided.v1" => {
                    pending.remove(&id);
                }
                _ => {}
            }
        }
        for (id, request) in pending {
            if request["expires_at_ms"].as_i64().unwrap_or(0) <= crate::agent::unix_time_ms() {
                GraphIngress::new(self.journal.fabric.clone())
                    .send(
                        &self.journal.tenant,
                        run,
                        EventId::stable("graph-host-expiry", &id),
                        GraphEvent::new(
                            "approval.expire.v1",
                            json!({"approval_id":request["approval_id"]}),
                        ),
                    )
                    .await?;
            }
        }
        Ok(())
    }
    pub async fn cancel(&self, run: RunId) -> Result<(), FabricError> {
        self.engine.cancel(&self.journal.tenant, run).await?;
        self.journal.sync(run).await
    }
    pub async fn events_after(
        &self,
        run: RunId,
        after: u64,
        limit: usize,
    ) -> Result<Vec<RunEvent>, FabricError> {
        self.journal.read(run, after, limit).await
    }
    /// Caller creates the RunStore run record first. Repeated projection is safe,
    /// including after a crash between RunStore append and the next page read.
    pub async fn project_into(&self, run: RunId, runs: &dyn RunStore) -> Result<(), FabricError> {
        let mut after = runs
            .get_run(run)
            .await
            .map_err(|e| FabricError::Storage(e.to_string()))?
            .ok_or(FabricError::NotFound)?
            .last_seq;
        loop {
            let page = self.events_after(run, after, 128).await?;
            if page.is_empty() {
                return Ok(());
            }
            after = page.last().expect("nonempty page").seq;
            runs.append_events(
                page.into_iter()
                    .map(|event| ObservedRunEvent::new(event, crate::agent::unix_time_ms()))
                    .collect(),
            )
            .await
            .map_err(|e| FabricError::Storage(e.to_string()))?;
        }
    }
    pub fn subscribe(
        &self,
        run: RunId,
        after: u64,
    ) -> Pin<Box<dyn futures_core::Stream<Item = Result<RunEvent, FabricError>> + Send>> {
        let host = self.clone();
        Box::pin(async_stream::stream! {
            let mut after = after;
            loop {
                match host.events_after(run, after, 128).await {
                    Ok(page) => {
                        for event in page {
                            after = event.seq;
                            let terminal = event.kind.is_terminal();
                            yield Ok(event);
                            if terminal { return; }
                        }
                        match host.journal.load(run).await {
                            Ok((_, cursor)) if cursor.terminal && after >= cursor.public => return,
                            Err(error) => { yield Err(error); return; }
                            _ => {}
                        }
                    }
                    Err(error) => { yield Err(error); return; }
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        })
    }
}

fn semantic_events(event: &PublishedGraphEvent) -> Result<Vec<AgentEvent>, FabricError> {
    let payload = &event.payload;
    let parse = |value| serde_json::from_value(value).map_err(|e| invalid("public projection", e));
    Ok(match event.kind.as_str() {
        "model.requested.v1" if !payload["tool_snapshot"].is_null() => {
            let snapshot: ToolSetSnapshot =
                serde_json::from_value(payload["tool_snapshot"].clone())
                    .map_err(|e| invalid("tool snapshot", e))?;
            vec![AgentEvent::ToolSetUpdated {
                revision: snapshot.revision,
                digest: snapshot.digest,
                tools: snapshot
                    .definitions
                    .iter()
                    .map(|tool| tool.name.clone())
                    .collect(),
                dynamic_tools: snapshot
                    .bindings
                    .iter()
                    .filter(|binding| binding.kind == ToolBindingKind::AgentDefined)
                    .map(|binding| binding.name.clone())
                    .collect(),
            }]
        }
        "approval.requested.v1" => {
            let request: ApprovalRequest =
                serde_json::from_value(payload.clone()).map_err(|e| invalid("approval", e))?;
            vec![AgentEvent::ApprovalRequested {
                approval_id: request.approval_id,
                call_id: request.call_id,
                tool_name: request.tool_name,
                risk_level: request.risk_level,
                arguments: request.arguments,
            }]
        }
        "approval.finished.v1" if payload["status"]["status"] == "resolved" => vec![parse(
            json!({"type":"approval_resolved","approval_id":payload["approval_id"],"call_id":payload["call_id"],"resolution":payload["status"]["resolution"]}),
        )?],
        "tool.authorized.v1" => vec![parse(
            json!({"type":"tool_execution_started","call_id":payload["id"],"arguments":payload["authorization"]["call"]["invocation"]["public_arguments"]}),
        )?],
        "gate.result.v1" if payload["payload"]["output"].is_string() => {
            let call_id = payload["id"].as_str().unwrap_or_default().into();
            let output = payload["payload"]["output"].as_str().unwrap_or_default();
            if payload["payload"]["ok"] == false {
                let error = &payload["payload"]["error"];
                vec![AgentEvent::ToolExecutionFailed {
                    call_id,
                    code: error["code"].as_str().unwrap_or("tool_failed").into(),
                    message: error["message"].as_str().unwrap_or(output).into(),
                    category: serde_json::from_value(error["category"].clone())
                        .unwrap_or(ToolErrorCategory::Internal),
                    retryable: false,
                    retry_after_ms: None,
                }]
            } else {
                vec![AgentEvent::ToolExecutionCompleted {
                    call_id,
                    output: output.into(),
                }]
            }
        }
        "engine.terminal.v1" => {
            let mut events = vec![];
            if !payload["usage"].is_null() {
                events.push(AgentEvent::UsageUpdated {
                    usage: serde_json::from_value(payload["usage"].clone())
                        .map_err(|e| invalid("usage", e))?,
                });
            }
            events.push(match payload["status"].as_str() {
                Some("completed") => AgentEvent::Completed {
                    finish_reason: FinishReason::Stop,
                },
                Some("cancelled") => AgentEvent::Cancelled,
                _ => {
                    let error = payload["error"].as_str().unwrap_or("graph_failed");
                    AgentEvent::failed(
                        error,
                        payload["output"]["message"].as_str().unwrap_or(error),
                        payload["output"]["retryable"].as_bool().unwrap_or(false),
                    )
                }
            });
            events
        }
        _ => vec![],
    })
}
