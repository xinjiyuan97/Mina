//! Convenience configuration translated to the same static graph used by files.
use super::*;
use serde_json::json;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphAgentConfig {
    pub model: String,
    pub system_prompt: String,
    pub max_steps: u32,
    pub max_output_tokens: Option<u32>,
    pub model_timeout_ms: u64,
    pub tool_timeout_ms: u64,
    pub run_timeout_ms: u64,
    pub review_level: u8,
    pub allow_run_adf: bool,
    pub parallel_tools: bool,
    pub allow_suspend: bool,
    #[serde(default)]
    pub compaction: Option<CompactorConfig>,
    #[serde(default)]
    pub on_max_steps: OnMaxSteps,
    #[serde(default)]
    pub finalizer: FinalizerConfig,
}
impl GraphAgentConfig {
    pub fn new(model: impl Into<String>, system_prompt: impl Into<String>) -> Self {
        Self {
            model: model.into(),
            system_prompt: system_prompt.into(),
            max_steps: 8,
            max_output_tokens: None,
            model_timeout_ms: 120_000,
            tool_timeout_ms: 30_000,
            run_timeout_ms: 900_000,
            review_level: 50,
            allow_run_adf: false,
            parallel_tools: false,
            allow_suspend: false,
            compaction: None,
            on_max_steps: OnMaxSteps::Fail,
            finalizer: FinalizerConfig::default(),
        }
    }
    /// Resource names: primary (model), workspace (tools), durable (optional waits).
    pub fn definition(&self) -> Result<AgentDefinition, FabricError> {
        let op = |kind: &str, config: Value| OperatorDefinition::new(kind, 1, config);
        let mut model =
            json!({"model":"primary","model_name":self.model,"timeout_ms":self.model_timeout_ms});
        if let Some(limit) = self.max_output_tokens {
            model["max_output_tokens"] = json!(limit);
        }
        let mut graph = AgentGraph::new("tool-assistant")
            .trigger("start", TriggerDefinition::manual("agent.start.v1"))
            .operator("context",op("agent.context",json!({"system_prompt":self.system_prompt,"max_steps":self.max_steps,"toolset":"workspace","allow_run_adf":self.allow_run_adf})))
            .operator("llm",op("agent.model",model))
            .operator("gate",op("agent.gate",json!({})))
            .operator("policy",op("agent.tool_policy",json!({"toolset":"workspace","approval_timeout_ms":self.run_timeout_ms.min(86_400_000),"review_level":self.review_level,"allow_run_adf":self.allow_run_adf})))
            .operator("tools",op("agent.tools",json!({"toolset":"workspace","timeout_ms":self.tool_timeout_ms,"review_level":self.review_level,"allow_run_adf":self.allow_run_adf,"allow_suspend":self.allow_suspend,"parallel":self.parallel_tools})))
            .subscribe("start","start","context",["agent.start.v1"])
            .subscribe("model","context","llm",["model.requested.v1","context.failed.v1"])
            .subscribe("batch","llm","gate",["stop.tool_calls.v1"])
            .subscribe("authorize","gate","policy",["gate.dispatch.v1"])
            .subscribe("decide","$external","policy",["approval.decided.v1","approval.expire.v1"])
            .subscribe("execute","policy","tools",["tool.authorized.v1"])
            .subscribe("reject","policy","gate",["gate.result.v1"])
            .subscribe("result","tools","gate",["gate.result.v1"])
            .subscribe("continue","gate","context",["gate.completed.v1"])
            .lifecycle(GraphLifecycle::model("llm"))
            .limits(GraphLimits::new(self.run_timeout_ms,10_000));
        if self.allow_suspend {
            graph = graph
                .operator("wait", op("agent.wait", json!({"wait_service":"durable"})))
                .subscribe("wait", "tools", "wait", ["wait.requested.v1"])
                .subscribe("wake", "$external", "wait", ["wait.woken.v1"])
                .subscribe("resumed", "wait", "gate", ["gate.result.v1"])
                .subscribe("cleanup", "$engine", "wait", ["engine.terminal.v1"]);
        }
        let mut definition = graph.finish()?;
        if self.on_max_steps == OnMaxSteps::Finalize {
            definition.enable_finalization(self.finalizer.clone())?;
        }
        if let Some(config) = &self.compaction {
            definition.enable_compaction(config.clone())?;
        }
        Ok(definition)
    }
}

impl AgentDefinition {
    /// Apply host run restrictions without widening a graph's configured capabilities.
    /// Custom operators retain their own protocol-specific policy configuration.
    pub fn constrain_run(&mut self, options: &crate::agent::RunOptions) -> Result<(), FabricError> {
        if options
            .max_steps
            .is_some_and(|n| n == 0 || n > crate::agent::MAX_RUN_STEPS)
        {
            return Err(invalid("max_steps", "must be between 1 and 100"));
        }
        for op in self.operators.values_mut() {
            if op.kind == "agent.context"
                && let Some(max) = options.max_steps
            {
                let ceiling = op.config["max_steps"]
                    .as_u64()
                    .ok_or_else(|| invalid("max_steps", "context budget required"))?;
                op.config["max_steps"] = json!(ceiling.min(u64::from(max)));
            }
            if matches!(
                op.kind.as_str(),
                "agent.context" | "agent.tool_policy" | "agent.tools"
            ) {
                op.config["allow_run_adf"] = json!(
                    options.allow_run_adf && op.config["allow_run_adf"].as_bool().unwrap_or(false)
                );
                if let Some(allowed) = &options.allowed_tools {
                    let configured: Option<Vec<String>> = op
                        .config
                        .get("allowed_tools")
                        .map(|v| serde_json::from_value(v.clone()))
                        .transpose()
                        .map_err(|e| invalid("allowed_tools", e))?;
                    op.config["allowed_tools"] = json!(
                        allowed
                            .iter()
                            .filter(|name| configured.as_ref().is_none_or(|v| v.contains(name)))
                            .collect::<Vec<_>>()
                    );
                }
            }
        }
        if let Some(mut snapshot) = self.binding_snapshot.take() {
            snapshot.resolved_digest = crate::agent::bindings::digest(self)?;
            self.binding_snapshot = Some(snapshot);
        }
        Ok(())
    }
}

impl AgentDefinition {
    /// Validate a protocol-specific start payload before durable admission.
    pub fn validate_start(
        &self,
        registry: &OperatorRegistry,
        payload: &Value,
    ) -> Result<(), FabricError> {
        self.validate_trigger(registry, &self.lifecycle.start.trigger, payload)
    }
    /// Validate a named trigger and its payload before any durable admission.
    pub fn validate_trigger(
        &self,
        registry: &OperatorRegistry,
        trigger: &str,
        payload: &Value,
    ) -> Result<(), FabricError> {
        let source = self
            .triggers
            .get(trigger)
            .ok_or_else(|| invalid("trigger", "unknown trigger"))?;
        if source.kind != "manual" || source.version != 1 || source.config != serde_json::json!({})
        {
            return Err(invalid(
                "trigger",
                "unsupported trigger type, version or config",
            ));
        }
        let mut found = false;
        for subscription in &self.subscriptions {
            if subscription.from == trigger && subscription.events.contains(&source.event) {
                found = true;
                let op = self
                    .operators
                    .get(&subscription.subscriber)
                    .ok_or_else(|| invalid("trigger", "subscriber does not exist"))?;
                let descriptor = registry
                    .describe(&op.kind, op.version)
                    .ok_or_else(|| invalid("operator", "unregistered type"))?;
                let schema = descriptor
                    .inputs
                    .get(&source.event)
                    .ok_or_else(|| invalid("trigger", "undeclared input"))?;
                validate_value(schema, payload, "trigger.payload")?;
            }
        }
        if !found {
            return Err(invalid("trigger", "no subscriber"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CompactorConfig {
    pub model: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_name: Option<String>,
    pub context_window_tokens: u64,
    pub reserved_output_tokens: u64,
    pub summary_context_window_tokens: u64,
    pub safety_margin_tokens: u64,
    pub max_output_tokens: u32,
    pub trigger_percent: u64,
    pub target_percent: u64,
    pub keep_recent_blocks: usize,
    pub timeout_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attachment_tokens: Option<u64>,
    pub fallback: String,
}
impl Default for CompactorConfig {
    fn default() -> Self {
        Self {
            model: String::new(),
            model_name: None,
            context_window_tokens: 0,
            reserved_output_tokens: 0,
            summary_context_window_tokens: 0,
            safety_margin_tokens: 1024,
            max_output_tokens: 2048,
            trigger_percent: 80,
            target_percent: 60,
            keep_recent_blocks: 2,
            timeout_ms: 30_000,
            attachment_tokens: None,
            fallback: "fail".into(),
        }
    }
}

impl CompactorConfig {
    /// Model context capacities must come from the host/provider configuration.
    pub fn new(
        model: impl Into<String>,
        context_window_tokens: u64,
        reserved_output_tokens: u64,
        summary_context_window_tokens: u64,
    ) -> Self {
        Self {
            model: model.into(),
            context_window_tokens,
            reserved_output_tokens,
            summary_context_window_tokens,
            ..Self::default()
        }
    }
}
impl AgentDefinition {
    /// Enable pre-request compaction for the convenience graph's context/llm nodes.
    pub fn enable_compaction(&mut self, config: CompactorConfig) -> Result<(), FabricError> {
        if self.binding_snapshot.is_some() || self.operators.contains_key("compactor") {
            return Err(invalid("compaction", "configure before binding, once"));
        }
        let context = self
            .operators
            .get("context")
            .ok_or_else(|| invalid("compaction", "context node required"))?;
        let model = self
            .operators
            .get("llm")
            .ok_or_else(|| invalid("compaction", "llm node required"))?;
        if context.kind != "agent.context" || model.kind != "agent.model" {
            return Err(invalid("compaction", "expected built-in context and model"));
        }
        let reserve = u32::try_from(config.reserved_output_tokens)
            .map_err(|e| invalid("reserved_output_tokens", e))?;
        if reserve == 0
            || model.config["max_output_tokens"]
                .as_u64()
                .is_some_and(|n| n > u64::from(reserve))
        {
            return Err(invalid(
                "reserved_output_tokens",
                "must cover model output limit",
            ));
        }
        if self.operators.values().any(|op| {
            op.kind == "agent.finalizer"
                && op.config["max_output_tokens"]
                    .as_u64()
                    .is_none_or(|n| n > u64::from(reserve))
        }) {
            return Err(invalid(
                "reserved_output_tokens",
                "must cover finalizer output limit",
            ));
        }
        let value = serde_json::to_value(config).map_err(|e| invalid("compaction", e))?;
        self.operators.get_mut("context").expect("checked").version = 3;
        let model = self.operators.get_mut("llm").expect("checked");
        if model.config.get("max_output_tokens").is_none() {
            model.config["max_output_tokens"] = json!(reserve);
        }
        self.operators.insert(
            "compactor".into(),
            OperatorDefinition::new("agent.compactor", 1, value),
        );
        self.subscriptions.extend([
            OperatorSubscription {
                id: "prepare-context".into(),
                from: "context".into(),
                subscriber: "compactor".into(),
                events: vec!["context.prepare.requested.v1".into()],
            },
            OperatorSubscription {
                id: "prepared-context".into(),
                from: "compactor".into(),
                subscriber: "context".into(),
                events: vec!["context.prepared.v1".into()],
            },
        ]);
        Ok(())
    }
}

/// Behavior when the normal model-round budget has been exhausted.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnMaxSteps {
    #[default]
    Fail,
    Finalize,
}

/// A separate, bounded final-response call using the main model resource.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FinalizerConfig {
    pub max_output_tokens: u32,
    pub timeout_ms: u64,
}
impl Default for FinalizerConfig {
    fn default() -> Self {
        Self {
            max_output_tokens: 2048,
            timeout_ms: 30_000,
        }
    }
}

impl AgentDefinition {
    /// Enable one tool-free final response after the last permitted tool batch.
    /// Configure before binding; existing compaction wiring is preserved.
    pub fn enable_finalization(&mut self, config: FinalizerConfig) -> Result<(), FabricError> {
        if self.binding_snapshot.is_some() || self.operators.contains_key("finalizer") {
            return Err(invalid("finalizer", "configure before binding, once"));
        }
        let context = self
            .operators
            .get("context")
            .ok_or_else(|| invalid("finalizer", "context node required"))?;
        let model = self
            .operators
            .get("llm")
            .ok_or_else(|| invalid("finalizer", "llm node required"))?;
        if context.kind != "agent.context"
            || model.kind != "agent.model"
            || self.lifecycle.terminal != GraphLifecycle::model("llm").terminal
        {
            return Err(invalid(
                "finalizer",
                "expected built-in context/model and model lifecycle",
            ));
        }
        if config.max_output_tokens == 0 || config.timeout_ms == 0 || config.timeout_ms > 86_400_000
        {
            return Err(invalid(
                "finalizer",
                "positive output limit and timeout <= 86400000 required",
            ));
        }
        if self.operators.values().any(|op| {
            op.kind == "agent.compactor"
                && op.config["reserved_output_tokens"]
                    .as_u64()
                    .is_none_or(|n| n < u64::from(config.max_output_tokens))
        }) {
            return Err(invalid(
                "finalizer",
                "compactor reserve must cover finalizer output limit",
            ));
        }
        let mut value = model.config.clone();
        value["max_output_tokens"] = json!(config.max_output_tokens);
        value["timeout_ms"] = json!(config.timeout_ms);
        self.operators.get_mut("context").expect("checked").config["on_max_steps"] =
            json!("finalize");
        self.operators.insert(
            "finalizer".into(),
            OperatorDefinition::new("agent.finalizer", 1, value),
        );
        self.subscriptions.extend([
            OperatorSubscription {
                id: "request-finalize".into(),
                from: "context".into(),
                subscriber: "finalizer".into(),
                events: vec!["finalize.requested.v1".into()],
            },
            OperatorSubscription {
                id: "forward-terminal".into(),
                from: "llm".into(),
                subscriber: "finalizer".into(),
                events: vec![
                    "stop.completed.v1".into(),
                    "stop.failed.v1".into(),
                    "stop.cancelled.v1".into(),
                ],
            },
        ]);
        self.lifecycle.terminal = GraphLifecycle::model("finalizer").terminal;
        Ok(())
    }
}
