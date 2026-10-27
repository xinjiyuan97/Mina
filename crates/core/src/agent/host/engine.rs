//! Durable application lifecycle over the static subscription graph.
use crate::agent::{
    AgentDefinition, AgentResources, GraphAgentConfig, GraphHost, OperatorRegistry, PlannedRun,
    RunRuntimeError, StartedRun,
};
use crate::{contracts::fabric::*, contracts::*};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::{
    sync::{Mutex, broadcast},
    task::JoinHandle,
};

pub type AgentRunConfig = GraphAgentConfig;
#[derive(Clone, Serialize, Deserialize)]
struct Admission {
    definition: AgentDefinition,
    input: Value,
    #[serde(default)]
    raw_input: bool,
    #[serde(default)]
    run_input: Option<String>,
    context_fingerprint: Option<String>,
    allowed_tools: Option<Vec<String>>,
    allow_run_adf: bool,
    max_steps: Option<u32>,
    failure: Option<String>,
    manifest: Value,
}
struct Active {
    host: GraphHost,
    sender: broadcast::Sender<RunEvent>,
    task: JoinHandle<()>,
}
pub struct AgentEngine {
    resources: AgentResources,
    registry: OperatorRegistry,
    runs: Arc<dyn RunStore>,
    starts: Mutex<()>,
    active: Mutex<HashMap<RunId, Active>>,
}
fn persistence(error: impl std::fmt::Display) -> RunRuntimeError {
    RunRuntimeError::Persistence(error.to_string())
}
fn key(run: RunId) -> String {
    format!("graph-admission:{run}")
}
impl AgentEngine {
    pub fn new(resources: AgentResources, runs: Arc<dyn RunStore>) -> Self {
        Self::with_registry(resources, runs, OperatorRegistry::builtins())
    }
    /// Host registry is retained for new runs and reconstruction after restart.
    pub fn with_registry(
        resources: AgentResources,
        runs: Arc<dyn RunStore>,
        registry: OperatorRegistry,
    ) -> Self {
        Self {
            resources,
            runs,
            registry,
            starts: Mutex::new(()),
            active: Mutex::new(HashMap::new()),
        }
    }
    async fn load(&self, run: RunId) -> Result<Admission, FabricError> {
        match self
            .resources
            .fabric
            .execute(FabricCommand::ReadCheckpoint(key(run)))
            .await?
        {
            FabricReply::Checkpoint(Some(record)) => serde_json::from_value(record.value)
                .map_err(|e| FabricError::Invalid(e.to_string())),
            FabricReply::Checkpoint(None) => Err(FabricError::NotFound),
            _ => Err(FabricError::Conflict),
        }
    }
    pub async fn host(&self, run: RunId) -> Result<GraphHost, FabricError> {
        if let Some(active) = self.active.lock().await.get(&run) {
            return Ok(active.host.clone());
        }
        let saved = self.load(run).await?;
        GraphHost::new(
            "local",
            saved.definition,
            self.registry.clone(),
            self.resources.clone(),
        )
    }
    pub async fn run(
        self: &Arc<Self>,
        config: &AgentRunConfig,
        input: impl Into<String>,
    ) -> Result<StartedRun, RunRuntimeError> {
        self.start(
            config,
            PlannedRun {
                run_id: RunId::new(),
                input: input.into(),
                attachments: vec![],
                prior_messages: vec![],
                allowed_tools: None,
                allow_run_adf: config.allow_run_adf,
                max_steps: Some(config.max_steps),
                context_fingerprint: None,
                execution_manifest: None,
            },
        )
        .await
    }
    pub async fn start(
        self: &Arc<Self>,
        config: &AgentRunConfig,
        plan: PlannedRun,
    ) -> Result<StartedRun, RunRuntimeError> {
        self.start_outcome(config, plan, None).await
    }
    /// Persist admission before execution. Reopening pins the saved graph and deadline.
    pub async fn admit(
        &self,
        config: &AgentRunConfig,
        plan: PlannedRun,
        failure: Option<&str>,
    ) -> Result<GraphHost, RunRuntimeError> {
        self.admit_inner(config, None, None, plan, failure).await
    }
    /// Admit an arbitrary graph with a protocol-specific JSON start payload.
    /// `plan.input` is the human-readable RunStore label, not the graph payload.
    pub async fn admit_graph(
        &self,
        definition: AgentDefinition,
        payload: Value,
        plan: PlannedRun,
        failure: Option<&str>,
    ) -> Result<GraphHost, RunRuntimeError> {
        self.admit_inner(
            &AgentRunConfig::new("custom", ""),
            Some(definition),
            Some(payload),
            plan,
            failure,
        )
        .await
    }
    pub async fn start_graph(
        self: &Arc<Self>,
        definition: AgentDefinition,
        payload: Value,
        plan: PlannedRun,
    ) -> Result<StartedRun, RunRuntimeError> {
        self.start_graph_outcome(definition, payload, plan, None)
            .await
    }
    pub async fn start_graph_outcome(
        self: &Arc<Self>,
        definition: AgentDefinition,
        payload: Value,
        plan: PlannedRun,
        failure: Option<&str>,
    ) -> Result<StartedRun, RunRuntimeError> {
        let _guard = self.starts.lock().await;
        let run = plan.run_id;
        let host = self.admit_graph(definition, payload, plan, failure).await?;
        self.drive(run, host).await
    }
    pub async fn run_graph(
        self: &Arc<Self>,
        definition: AgentDefinition,
        payload: Value,
    ) -> Result<StartedRun, RunRuntimeError> {
        let input = payload
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| payload.to_string());
        self.start_graph(
            definition,
            payload,
            PlannedRun {
                run_id: RunId::new(),
                input,
                attachments: vec![],
                prior_messages: vec![],
                allowed_tools: None,
                allow_run_adf: false,
                max_steps: None,
                context_fingerprint: None,
                execution_manifest: None,
            },
        )
        .await
    }
    async fn admit_inner(
        &self,
        config: &AgentRunConfig,
        definition: Option<AgentDefinition>,
        payload: Option<Value>,
        plan: PlannedRun,
        failure: Option<&str>,
    ) -> Result<GraphHost, RunRuntimeError> {
        if payload.is_none() && plan.input.trim().is_empty() && plan.attachments.is_empty() {
            return Err(super::HarnessError::InvalidInput.into());
        }
        let options = super::RunOptions {
            allowed_tools: plan.allowed_tools.clone(),
            allow_run_adf: plan.allow_run_adf,
            max_steps: plan.max_steps,
        };
        let definition = definition
            .map(|mut d| {
                d.constrain_run(&options)?;
                Ok::<_, FabricError>(d)
            })
            .transpose()
            .map_err(persistence)?;
        let run = plan.run_id;
        let raw_input = payload.is_some();
        let input = payload.unwrap_or_else(|| json!({"input":plan.input,"attachments":plan.attachments,"prior_messages":plan.prior_messages}));
        let mut manifest = plan.execution_manifest.unwrap_or_else(|| json!({}));
        manifest["engine_version"] = json!("operator_v1");
        if manifest["profile"].is_null() {
            manifest["profile"] = json!("operator_v1_tools");
        }
        manifest["projection_version"] = json!(1);
        manifest["runtime"] = json!("static_graph_v1");
        crate::operator::validate_execution_manifest(&manifest).map_err(persistence)?;
        let saved = match self.load(run).await {
            Ok(saved) => {
                if saved.input != input
                    || saved.raw_input != raw_input
                    || saved
                        .run_input
                        .as_ref()
                        .is_some_and(|label| label != &plan.input)
                    || definition
                        .as_ref()
                        .is_some_and(|graph| graph != &saved.definition)
                    || saved.context_fingerprint != plan.context_fingerprint
                    || saved.allowed_tools != plan.allowed_tools
                    || saved.allow_run_adf != plan.allow_run_adf
                    || saved.max_steps != plan.max_steps
                    || saved.failure.as_deref() != failure
                    || saved.manifest != manifest
                {
                    return Err(RunRuntimeError::Machine(
                        "immutable run context conflict".into(),
                    ));
                }
                saved
            }
            Err(FabricError::NotFound) => {
                let mut config = config.clone();
                if definition.is_none()
                    && let Some(max) = plan.max_steps
                {
                    if max == 0 || max > config.max_steps || max > super::MAX_RUN_STEPS {
                        return Err(super::HarnessError::InvalidMaxSteps { requested: max }.into());
                    }
                    config.max_steps = max;
                }
                config.allow_run_adf &= plan.allow_run_adf;
                let mut definition = match definition {
                    Some(definition) => definition,
                    None => config.definition().map_err(persistence)?,
                };
                definition.constrain_run(&options).map_err(persistence)?;
                definition
                    .validate(&self.registry, &self.resources)
                    .map_err(persistence)?;
                definition
                    .validate_start(&self.registry, &input)
                    .map_err(persistence)?;
                let saved = Admission {
                    definition,
                    input,
                    raw_input,
                    run_input: Some(plan.input.clone()),
                    context_fingerprint: plan.context_fingerprint,
                    allowed_tools: plan.allowed_tools,
                    allow_run_adf: plan.allow_run_adf,
                    max_steps: plan.max_steps,
                    failure: failure.map(Into::into),
                    manifest,
                };
                match self
                    .resources
                    .fabric
                    .execute(FabricCommand::Transact {
                        checkpoint: CheckpointWrite {
                            key: key(run),
                            expected: None,
                            value: serde_json::to_value(&saved).map_err(persistence)?,
                        },
                        outbox: vec![],
                        delivery: None,
                    })
                    .await
                {
                    Ok(_) => saved,
                    Err(FabricError::Fenced) => {
                        let existing = self.load(run).await.map_err(persistence)?;
                        if existing.input != saved.input
                            || existing.definition != saved.definition
                            || existing.raw_input != saved.raw_input
                            || existing.run_input != saved.run_input
                            || existing.context_fingerprint != saved.context_fingerprint
                            || existing.allowed_tools != saved.allowed_tools
                            || existing.allow_run_adf != saved.allow_run_adf
                            || existing.max_steps != saved.max_steps
                            || existing.failure != saved.failure
                            || existing.manifest != saved.manifest
                        {
                            return Err(persistence("admission conflict"));
                        }
                        existing
                    }
                    Err(error) => return Err(persistence(error)),
                }
            }
            Err(error) => return Err(persistence(error)),
        };
        match self.runs.get_run(run).await? {
            Some(previous) => {
                if previous.input != plan.input {
                    return Err(persistence("run input conflict"));
                }
                if previous.last_seq == 0 {
                    if let Err(error) = self
                        .runs
                        .set_execution_manifest(run, saved.manifest.clone())
                        .await
                        && self
                            .runs
                            .get_run(run)
                            .await?
                            .and_then(|r| r.execution_manifest)
                            .as_ref()
                            != Some(&saved.manifest)
                    {
                        return Err(error.into());
                    }
                } else if previous.execution_manifest.as_ref() != Some(&saved.manifest) {
                    return Err(persistence("run manifest conflict"));
                }
            }
            None => {
                if let Err(error) = self
                    .runs
                    .create_run(
                        RunSnapshot::new(run, &plan.input, super::unix_time_ms())
                            .with_execution_manifest(saved.manifest.clone()),
                    )
                    .await
                    && !self.runs.get_run(run).await?.is_some_and(|r| {
                        r.input == plan.input
                            && r.execution_manifest.as_ref() == Some(&saved.manifest)
                    })
                {
                    return Err(error.into());
                }
            }
        }
        let host = GraphHost::new(
            "local",
            saved.definition,
            self.registry.clone(),
            self.resources.clone(),
        )
        .map_err(persistence)?;
        host.engine
            .admit("local", run, saved.input)
            .await
            .map_err(persistence)?;
        if let Some(code) = &saved.failure {
            host.engine
                .fail("local", run, code)
                .await
                .map_err(persistence)?;
        }
        Ok(host)
    }
    pub async fn start_outcome(
        self: &Arc<Self>,
        config: &AgentRunConfig,
        plan: PlannedRun,
        failure: Option<&str>,
    ) -> Result<StartedRun, RunRuntimeError> {
        let _guard = self.starts.lock().await;
        let run = plan.run_id;
        let host = self.admit(config, plan, failure).await?;
        self.drive(run, host).await
    }
    async fn drive(
        self: &Arc<Self>,
        run: RunId,
        host: GraphHost,
    ) -> Result<StartedRun, RunRuntimeError> {
        let mut active = self.active.lock().await;
        if let Some(active) = active.get(&run) {
            return Ok(StartedRun {
                run_id: run,
                events: active.sender.subscribe(),
            });
        }
        host.recover(run).await.map_err(persistence)?;
        // Admission failures and already-terminal runs must be queryable before returning.
        host.project_into(run, self.runs.as_ref())
            .await
            .map_err(persistence)?;
        let (sender, events) = broadcast::channel(256);
        let engine = self.clone();
        let publish = sender.clone();
        let worker_host = host.clone();
        let task = tokio::spawn(async move {
            let mut cursor = 0;
            let mut activity = FabricWatch::new(engine.resources.fabric.as_ref());
            let mut controls_at = tokio::time::Instant::now();
            loop {
                activity.observe();
                let result = async {
                    if tokio::time::Instant::now() >= controls_at {
                        worker_host.expire_approvals(run).await?;
                        controls_at = tokio::time::Instant::now() + Duration::from_millis(250);
                    }
                    worker_host.project_into(run, engine.runs.as_ref()).await?;
                    engine
                        .runs
                        .events_after(run, cursor, 128)
                        .await
                        .map_err(|e| FabricError::Storage(e.to_string()))
                }
                .await;
                match result {
                    Ok(events) => {
                        for event in events {
                            cursor = event.seq;
                            let terminal = event.kind.is_terminal();
                            let _ = publish.send(event);
                            if terminal {
                                engine.active.lock().await.remove(&run);
                                return;
                            }
                        }
                    }
                    Err(error) => tracing::warn!(%run, %error, "graph projection/control deferred"),
                }
                activity.wait(Duration::from_secs(1)).await;
            }
        });
        active.insert(run, Active { host, sender, task });
        Ok(StartedRun {
            run_id: run,
            events,
        })
    }
    pub async fn recover(self: &Arc<Self>, run: RunId) -> Result<StartedRun, RunRuntimeError> {
        let _guard = self.starts.lock().await;
        let saved = self.load(run).await.map_err(persistence)?;
        let plan = PlannedRun {
            run_id: run,
            input: saved
                .run_input
                .clone()
                .or_else(|| saved.input["input"].as_str().map(str::to_owned))
                .ok_or_else(|| persistence("invalid admission input"))?,
            attachments: if saved.raw_input {
                vec![]
            } else {
                serde_json::from_value(saved.input["attachments"].clone()).map_err(persistence)?
            },
            prior_messages: if saved.raw_input {
                vec![]
            } else {
                serde_json::from_value(saved.input["prior_messages"].clone())
                    .map_err(persistence)?
            },
            allowed_tools: saved.allowed_tools,
            allow_run_adf: saved.allow_run_adf,
            max_steps: saved.max_steps,
            context_fingerprint: saved.context_fingerprint,
            execution_manifest: Some(saved.manifest),
        };
        // admit reads the pinned definition; this placeholder is never used to rebuild it.
        let host = self
            .admit_inner(
                &AgentRunConfig::new("recovery", ""),
                None,
                saved.raw_input.then_some(saved.input),
                plan,
                saved.failure.as_deref(),
            )
            .await?;
        self.drive(run, host).await
    }
    pub async fn stop_workers(&self) {
        let active: Vec<_> = self
            .active
            .lock()
            .await
            .drain()
            .map(|(_, active)| active)
            .collect();
        for active in active {
            active.task.abort();
            let _ = active.task.await;
            active.host.engine.stop_workers().await;
        }
    }
    pub async fn get_run(&self, run: RunId) -> Result<Option<RunSnapshot>, RunStoreError> {
        self.runs.get_run(run).await
    }
    pub async fn events_after(
        &self,
        run: RunId,
        after: u64,
        limit: usize,
    ) -> Result<Vec<RunEvent>, RunStoreError> {
        self.host(run)
            .await
            .map_err(|e| RunStoreError::backend(e.to_string()))?
            .events_after(run, after, limit)
            .await
            .map_err(|e| RunStoreError::backend(e.to_string()))
    }
    /// Durable numeric pages. Disconnecting a reader leaves execution running.
    pub fn event_pages(
        self: &Arc<Self>,
        run: RunId,
        after: u64,
    ) -> std::pin::Pin<
        Box<dyn futures_core::Stream<Item = Result<Vec<RunEvent>, RunStoreError>> + Send>,
    > {
        let engine = self.clone();
        Box::pin(async_stream::stream! {
            let mut after = after;
            let mut activity = FabricWatch::new(engine.resources.fabric.as_ref());
            loop {
                activity.observe();
                match engine.events_after(run, after, 128).await {
                    Ok(page) if !page.is_empty() => {
                        after = page.last().expect("page").seq;
                        let terminal = page.last().expect("page").kind.is_terminal();
                        yield Ok(page);
                        if terminal { return; }
                        continue;
                    }
                    Ok(_) => {
                        if engine.get_run(run).await.ok().flatten().is_some_and(|r| r.is_terminal() && after >= r.last_seq) { return; }
                    }
                    Err(error) => { yield Err(error); return; }
                }
                activity.wait(Duration::from_secs(1)).await;
            }
        })
    }
    pub async fn subscribe(&self, run: RunId) -> Option<broadcast::Receiver<RunEvent>> {
        self.active
            .lock()
            .await
            .get(&run)
            .map(|a| a.sender.subscribe())
    }
    pub async fn cancel(&self, run: RunId, actor: &str) -> Result<(), FabricError> {
        if actor.trim().is_empty() {
            return Err(FabricError::Invalid("actor required".into()));
        }
        let host = self.host(run).await?;
        host.cancel(run).await?;
        host.project_into(run, self.runs.as_ref()).await
    }
    pub async fn approve(
        &self,
        run: RunId,
        approval: ApprovalId,
        actor: &str,
        resolution: ApprovalResolution,
    ) -> Result<bool, FabricError> {
        self.host(run)
            .await?
            .approve(run, approval, actor, resolution)
            .await
    }
}
