use super::*;
use agent_core::{effect_pipeline::TOOLS_PROFILE, operator::EngineVersion};
use agent_harness::{
    PlannedRun,
    pipeline::{EffectWaitBridge, PipelineStart, ToolsPipelineRuntime},
};
use std::collections::HashMap;
use tokio::{sync::Mutex, task::JoinHandle};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum EngineProfile {
    Legacy,
    Text,
    Tools,
}
impl EngineProfile {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Legacy => "legacy",
            Self::Text => "operator_v1_text",
            Self::Tools => TOOLS_PROFILE,
        }
    }
    pub fn engine(self) -> EngineVersion {
        match self {
            Self::Legacy => EngineVersion::LegacyMachine,
            _ => EngineVersion::OperatorV1,
        }
    }
}
pub(super) fn parse_profile(
    value: Option<&str>,
) -> Result<EngineProfile, Box<dyn std::error::Error>> {
    match value {
        None | Some("legacy") => Ok(EngineProfile::Legacy),
        Some("operator_v1_text") => Ok(EngineProfile::Text),
        Some(TOOLS_PROFILE) => Ok(EngineProfile::Tools),
        _ => {
            Err("MINA_ENGINE_PROFILE must be legacy, operator_v1_text or operator_v1_tools".into())
        }
    }
}
pub(super) fn configured_profile() -> Result<EngineProfile, Box<dyn std::error::Error>> {
    match env::var("MINA_ENGINE_PROFILE") {
        Ok(value) => parse_profile(Some(&value)),
        Err(env::VarError::NotPresent) => parse_profile(None),
        Err(error) => Err(error.into()),
    }
}
fn persistence(error: impl std::fmt::Display) -> RunRuntimeError {
    RunRuntimeError::Persistence(error.to_string())
}

type ActiveRun = (broadcast::Sender<RunEvent>, JoinHandle<()>);

pub(super) struct ToolsServerPipeline {
    pub runtime: Arc<ToolsPipelineRuntime>,
    runs: Arc<dyn RunStore>,
    starts: Mutex<()>,
    active: Mutex<HashMap<RunId, ActiveRun>>,
}

impl ToolsServerPipeline {
    pub fn new(runtime: Arc<ToolsPipelineRuntime>, runs: Arc<dyn RunStore>) -> Self {
        Self {
            runtime,
            runs,
            starts: Mutex::new(()),
            active: Mutex::new(HashMap::new()),
        }
    }

    pub async fn start(
        self: &Arc<Self>,
        state: &AppState,
        plan: PlannedRun,
    ) -> Result<StartedRun, RunRuntimeError> {
        self.start_outcome(state, plan, None).await
    }

    async fn start_outcome(
        self: &Arc<Self>,
        state: &AppState,
        plan: PlannedRun,
        failure: Option<&str>,
    ) -> Result<StartedRun, RunRuntimeError> {
        let _guard = self.starts.lock().await;
        let run_id = plan.run_id;
        let previous = self.runs.get_run(run_id).await.map_err(persistence)?;
        if let Some(manifest) = previous
            .as_ref()
            .and_then(|run| run.execution_manifest.as_ref())
        {
            agent_core::operator::validate_execution_manifest(manifest).map_err(persistence)?;
        }
        if let Some(previous) = &previous
            && (previous.input != plan.input
                || previous
                    .execution_manifest
                    .as_ref()
                    .is_some_and(|manifest| {
                        manifest.get("engine_version").is_some()
                            && (manifest["engine_version"] != "operator_v1"
                                || manifest["profile"] != TOOLS_PROFILE)
                    })
                || (previous.last_seq > 0
                    && previous
                        .execution_manifest
                        .as_ref()
                        .is_none_or(|manifest| manifest["profile"] != TOOLS_PROFILE)))
        {
            return Err(RunRuntimeError::Machine(
                "run engine/profile is immutable".into(),
            ));
        }
        match self.runtime.load("local", run_id).await {
            Ok(record) => {
                if record.profile != TOOLS_PROFILE
                    || record.start.input != plan.input
                    || record.start.attachments != plan.attachments
                    || record.start.prior_messages != plan.prior_messages
                    || record.start.context_fingerprint != plan.context_fingerprint
                    || record.admission_failure.as_deref() != failure
                {
                    return Err(RunRuntimeError::Machine(
                        "run context/profile conflict".into(),
                    ));
                }
            }
            Err(agent_core::operator_fabric::FabricError::NotFound) => {
                let mut start =
                    PipelineStart::text(run_id, &plan.input, &state.config.default_model().model)
                        .start;
                start.attachments = plan.attachments;
                start.prior_messages = plan.prior_messages;
                start.context_fingerprint = plan.context_fingerprint;
                start.config.allowed_tools = plan.allowed_tools;
                start.config.allow_run_adf = plan.allow_run_adf;
                start.config.max_steps = plan.max_steps.unwrap_or(state.config.agent().max_steps);
                start.config.max_output_tokens = state.config.default_model().max_output_tokens;
                start.config.approval_policy = state.config.approval().policy();
                start.config.tool_call_strategy = state.config.agent().tool_call_strategy;
                start.config.model_timeout_ms = state
                    .config
                    .agent()
                    .model_timeout_seconds
                    .saturating_mul(1000);
                start.config.tool_timeout_ms = state
                    .config
                    .agent()
                    .tool_timeout_seconds
                    .saturating_mul(1000);
                if let Some(code) = failure {
                    self.runtime
                        .fail_admission("local", start, agent_harness::unix_time_ms(), code)
                        .await
                        .map_err(persistence)?;
                } else {
                    self.runtime
                        .admit("local", start, agent_harness::unix_time_ms())
                        .await
                        .map_err(persistence)?;
                }
            }
            Err(error) => return Err(persistence(error)),
        }
        let mut manifest = plan
            .execution_manifest
            .unwrap_or_else(|| serde_json::json!({}));
        manifest["engine_version"] = serde_json::json!("operator_v1");
        manifest["profile"] = serde_json::json!(TOOLS_PROFILE);
        manifest["projection_version"] = serde_json::json!(1);
        if let Some(previous) = previous {
            if previous.last_seq == 0 {
                self.runs
                    .set_execution_manifest(run_id, manifest)
                    .await
                    .map_err(persistence)?;
            }
        } else if let Err(error) = self
            .runs
            .create_run(
                RunSnapshot::new(run_id, plan.input, agent_harness::unix_time_ms())
                    .with_execution_manifest(manifest),
            )
            .await
            && !self
                .runs
                .get_run(run_id)
                .await
                .map_err(persistence)?
                .is_some_and(|run| {
                    run.execution_manifest
                        .as_ref()
                        .is_some_and(|manifest| manifest["profile"] == TOOLS_PROFILE)
                })
        {
            return Err(persistence(error));
        }
        if failure.is_some() {
            self.runtime
                .tick("local", run_id, agent_harness::unix_time_ms)
                .await
                .map_err(persistence)?;
            let (_, events) = broadcast::channel(1);
            return Ok(StartedRun { run_id, events });
        }
        let snapshot = self
            .runs
            .get_run(run_id)
            .await
            .map_err(persistence)?
            .ok_or_else(|| persistence("missing admitted run"))?;
        let timeout_ms = state
            .config
            .agent()
            .run_timeout_seconds
            .saturating_mul(1000)
            .min(i64::MAX as u64) as i64;
        self.runtime
            .ensure_deadline(
                "local",
                run_id,
                snapshot.created_at_ms.saturating_add(timeout_ms),
            )
            .await
            .map_err(persistence)?;
        let mut active = self.active.lock().await;
        if let Some((sender, _)) = active.get(&run_id) {
            return Ok(StartedRun {
                run_id,
                events: sender.subscribe(),
            });
        }
        let (sender, events) = broadcast::channel(256);
        let driver = self.clone();
        let publish = sender.clone();
        let task = tokio::spawn(async move {
            let pump = async {
                loop {
                    match driver
                        .runtime
                        .tick("local", run_id, agent_harness::unix_time_ms)
                        .await
                    {
                        Ok(processed) => {
                            if driver
                                .runs
                                .get_run(run_id)
                                .await
                                .ok()
                                .flatten()
                                .is_some_and(|run| run.is_terminal())
                            {
                                break;
                            }
                            if processed != 0 {
                                continue;
                            }
                        }
                        Err(error) => {
                            tracing::error!(%run_id, %error, "operator tools drive failed; no legacy fallback")
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            };
            let relay = async {
                let mut cursor = 0;
                let mut interval = tokio::time::interval(Duration::from_millis(25));
                loop {
                    interval.tick().await;
                    let Ok(events) = driver.runs.events_after(run_id, cursor, 512).await else {
                        continue;
                    };
                    for event in events {
                        cursor = event.seq;
                        let terminal = event.kind.is_terminal();
                        let _ = publish.send(event);
                        if terminal {
                            return;
                        }
                    }
                }
            };
            let controls = async {
                let mut interval = tokio::time::interval(Duration::from_millis(25));
                loop {
                    interval.tick().await;
                    if let Err(error) = driver
                        .runtime
                        .reconcile_control("local", run_id, agent_harness::unix_time_ms())
                        .await
                    {
                        tracing::error!(%run_id, %error, "operator control reconciliation failed");
                    }
                    if driver
                        .runs
                        .get_run(run_id)
                        .await
                        .ok()
                        .flatten()
                        .is_some_and(|run| run.is_terminal())
                    {
                        return;
                    }
                }
            };
            let execution = async {
                tokio::select! {
                    _ = pump => {},
                    _ = controls => {},
                }
            };
            tokio::join!(execution, relay);
            driver.active.lock().await.remove(&run_id);
        });
        active.insert(run_id, (sender, task));
        Ok(StartedRun { run_id, events })
    }

    #[cfg(test)]
    pub async fn stop_workers(&self) {
        let tasks: Vec<_> = self
            .active
            .lock()
            .await
            .drain()
            .map(|(_, (_, task))| task)
            .collect();
        for task in tasks {
            task.abort();
            let _ = task.await;
        }
    }
}

pub(super) async fn fail_context(
    state: &AppState,
    request: &agent_core::context_operator::ContextRequest,
    code: &str,
) -> Result<(), RunRuntimeError> {
    let pipeline = state.tools_pipeline.as_ref().ok_or_else(|| {
        RunRuntimeError::Machine("operator_v1_tools unavailable; no legacy fallback".into())
    })?;
    pipeline
        .start_outcome(
            state,
            PlannedRun {
                run_id: request.run_id,
                input: request.input.clone(),
                attachments: request.attachments.clone(),
                prior_messages: vec![],
                allowed_tools: Some(request.policy.available_tools.clone()),
                allow_run_adf: request.policy.allow_run_adf,
                max_steps: Some(request.max_steps),
                context_fingerprint: None,
                execution_manifest: None,
            },
            Some(code),
        )
        .await?;
    Ok(())
}

pub(super) async fn cancel(state: &AppState, run_id: RunId) -> Option<Response> {
    let snapshot = match state.runtime.get_run(run_id).await {
        Ok(Some(snapshot)) => snapshot,
        Ok(None) => return None,
        Err(error) => return Some(map_store_error(error)),
    };
    let manifest = snapshot.execution_manifest.as_ref()?;
    if manifest["engine_version"] != "operator_v1" {
        return None;
    }
    if snapshot.is_terminal() {
        return Some(api_error(
            StatusCode::CONFLICT,
            "run_not_active",
            "run is terminal",
            false,
        ));
    }
    if manifest["profile"] != TOOLS_PROFILE {
        return Some(api_error(
            StatusCode::CONFLICT,
            "unsupported_profile",
            "this operator profile does not expose cancel",
            false,
        ));
    }
    let Some(pipeline) = &state.tools_pipeline else {
        return Some(api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "operator_unavailable",
            "operator_v1_tools unavailable; no legacy fallback",
            true,
        ));
    };
    Some(
        match pipeline
            .runtime
            .request_cancel(
                "local",
                run_id,
                "gateway:http",
                agent_harness::unix_time_ms(),
            )
            .await
        {
            Ok(()) => (
                StatusCode::ACCEPTED,
                Json(CancelRunResponse {
                    run_id,
                    status: "cancellation_requested",
                }),
            )
                .into_response(),
            Err(error) => api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "cancel_persistence_error",
                error.to_string(),
                true,
            ),
        },
    )
}

pub(super) struct EngineDeliveryRouter {
    runs: Arc<dyn RunStore>,
    legacy: Arc<dyn DeliveryRouter>,
    tools: Arc<EffectWaitBridge>,
}
impl EngineDeliveryRouter {
    pub fn new(
        runs: Arc<dyn RunStore>,
        legacy: Arc<dyn DeliveryRouter>,
        tools: Arc<EffectWaitBridge>,
    ) -> Self {
        Self {
            runs,
            legacy,
            tools,
        }
    }
}
impl DeliveryRouter for EngineDeliveryRouter {
    fn deliver(&self, delivery: Delivery, event: EventEnvelope) -> EventFuture<'_, ()> {
        Box::pin(async move {
            if let DeliveryTarget::WakeRun { run_id, .. } = &delivery.target
                && let Some(run) = self
                    .runs
                    .get_run(*run_id)
                    .await
                    .map_err(|error| EventError::backend(error.to_string()))?
                && let Some(manifest) = run.execution_manifest
                && manifest["engine_version"] == "operator_v1"
            {
                if manifest["profile"] != TOOLS_PROFILE {
                    return Err(EventError::invalid(
                        "operator profile cannot accept a native wait",
                    ));
                }
                return self.tools.deliver(delivery, event).await;
            }
            self.legacy.deliver(delivery, event).await
        })
    }
}
