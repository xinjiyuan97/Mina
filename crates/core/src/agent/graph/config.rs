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
            .operator("context",op("agent.context",json!({"system_prompt":self.system_prompt,"max_steps":self.max_steps,"toolset":"workspace","allow_run_adf":self.allow_run_adf})))
            .operator("llm",op("agent.model",model))
            .operator("gate",op("agent.gate",json!({})))
            .operator("policy",op("agent.tool_policy",json!({"toolset":"workspace","approval_timeout_ms":self.run_timeout_ms.min(86_400_000),"review_level":self.review_level,"allow_run_adf":self.allow_run_adf})))
            .operator("tools",op("agent.tools",json!({"toolset":"workspace","timeout_ms":self.tool_timeout_ms,"review_level":self.review_level,"allow_run_adf":self.allow_run_adf,"allow_suspend":self.allow_suspend,"parallel":self.parallel_tools})))
            .subscribe("start","$input","context",["agent.start.v1"])
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
        graph.finish()
    }
}
