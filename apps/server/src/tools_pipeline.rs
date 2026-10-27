use super::*;
use agent_core::operator::EngineVersion;
pub(super) const TOOLS_PROFILE: &str = "operator_v1_tools";
use agent_core::agent::{AgentResources, GraphWaitBridge};
use agent_harness::PlannedRun;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum EngineProfile {
    Text,
    Tools,
}
impl EngineProfile {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Text => "operator_v1_text",
            Self::Tools => TOOLS_PROFILE,
        }
    }
    pub fn engine(self) -> EngineVersion {
        EngineVersion::OperatorV1
    }
}
pub(super) fn parse_profile(
    value: Option<&str>,
) -> Result<EngineProfile, Box<dyn std::error::Error>> {
    match value {
        None => Ok(EngineProfile::Tools),
        Some("operator_v1_text") => Ok(EngineProfile::Text),
        Some(TOOLS_PROFILE) => Ok(EngineProfile::Tools),
        _ => {
            Err("MINA_ENGINE_PROFILE must be operator_v1_text or operator_v1_tools; legacy has been removed".into())
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
pub(super) struct ServerAgentRuntime {
    suspending: bool,
    definition: Option<agent_core::agent::AgentDefinition>,
    pub engine: Arc<agent_core::agent::AgentEngine>,
}
impl ServerAgentRuntime {
    pub fn new(resources: AgentResources, runs: Arc<dyn RunStore>) -> Self {
        Self {
            definition: None,
            suspending: resources.has_wait_service("durable"),
            engine: Arc::new(agent_core::agent::AgentEngine::new(resources, runs)),
        }
    }
    pub fn with_graph(
        resources: AgentResources,
        runs: Arc<dyn RunStore>,
        definition: agent_core::agent::AgentDefinition,
        registry: agent_core::agent::OperatorRegistry,
    ) -> Result<Self, agent_core::contracts::fabric::FabricError> {
        definition.validate(&registry, &resources)?;
        Ok(Self {
            suspending: resources.has_wait_service("durable"),
            definition: Some(definition),
            engine: Arc::new(agent_core::agent::AgentEngine::with_registry(
                resources, runs, registry,
            )),
        })
    }
    pub async fn start(
        self: &Arc<Self>,
        state: &AppState,
        plan: PlannedRun,
    ) -> Result<StartedRun, RunRuntimeError> {
        self.start_outcome(state, plan, None).await
    }
    pub(super) async fn start_outcome(
        self: &Arc<Self>,
        state: &AppState,
        plan: PlannedRun,
        failure: Option<&str>,
    ) -> Result<StartedRun, RunRuntimeError> {
        if let Some(definition) = &self.definition {
            let payload = serde_json::json!({"input":plan.input,"attachments":plan.attachments,"prior_messages":plan.prior_messages});
            return self
                .engine
                .start_graph_outcome(definition.clone(), payload, plan, failure)
                .await;
        }
        let mut config = agent_core::agent::GraphAgentConfig::new(
            &state.config.default_model().model,
            &state.config.agent().system_prompt,
        );
        config.max_steps = state.config.agent().max_steps;
        config.max_output_tokens = state.config.default_model().max_output_tokens;
        config.review_level = state.config.approval().policy().review_level();
        config.parallel_tools = state.config.agent().tool_call_strategy
            == agent_core::contracts::ToolCallStrategy::ParallelSafe;
        config.allow_run_adf = plan.allow_run_adf;
        config.allow_suspend = self.suspending
            && plan
                .execution_manifest
                .as_ref()
                .is_none_or(|m| m["profile"] != "operator_v1_text");
        config.model_timeout_ms = state
            .config
            .agent()
            .model_timeout_seconds
            .saturating_mul(1000);
        config.tool_timeout_ms = state
            .config
            .agent()
            .tool_timeout_seconds
            .saturating_mul(1000);
        config.run_timeout_ms = state
            .config
            .agent()
            .run_timeout_seconds
            .saturating_mul(1000);
        self.engine.start_outcome(&config, plan, failure).await
    }

    #[cfg(test)]
    pub async fn stop_workers(&self) {
        self.engine.stop_workers().await;
    }
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
            "the run is persisted but no longer active",
            false,
        ));
    }
    let selected = match manifest["profile"].as_str() {
        Some("operator_v1_text") => &state.pipeline,
        Some(TOOLS_PROFILE) => &state.tools_pipeline,
        _ => {
            return Some(api_error(
                StatusCode::CONFLICT,
                "unsupported_profile",
                "unknown graph profile",
                false,
            ));
        }
    };
    let Some(pipeline) = selected else {
        return Some(api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "operator_unavailable",
            "operator_v1_tools unavailable; no legacy fallback",
            true,
        ));
    };
    Some(match pipeline.engine.cancel(run_id, "gateway:http").await {
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
    })
}

pub(super) struct EngineDeliveryRouter {
    runs: Arc<dyn RunStore>,
    auxiliary: Arc<dyn DeliveryRouter>,
    tools: Arc<GraphWaitBridge>,
}
impl EngineDeliveryRouter {
    pub fn new(
        runs: Arc<dyn RunStore>,
        auxiliary: Arc<dyn DeliveryRouter>,
        tools: Arc<GraphWaitBridge>,
    ) -> Self {
        Self {
            runs,
            auxiliary,
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
            if matches!(delivery.target, DeliveryTarget::WakeRun { .. }) {
                return Err(EventError::invalid("legacy run execution has been removed"));
            }
            self.auxiliary.deliver(delivery, event).await
        })
    }
}
