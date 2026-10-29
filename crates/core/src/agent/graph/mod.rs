//! Static, protocol-checked single-agent graphs. Both construction paths use this engine.
mod builtins;
mod config;
pub use config::{CompactorConfig, FinalizerConfig, GraphAgentConfig, OnMaxSteps};
mod host;
mod runtime;
mod stream;
pub use host::GraphHost;
mod waits;
use super::definition::invalid;
use super::definition::*;
use crate::{
    contracts::fabric::{FabricError, OperatorFabric},
    contracts::{ModelPort, RunCancellation, RunId, TokenUsage, ToolPort},
};
pub use runtime::{
    ConfiguredAgent, GraphEngine, GraphIngress, GraphRun, GraphStatus, GraphStepUsage,
    PublishedGraphEvent,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    pin::Pin,
    sync::Arc,
};
pub use stream::GraphAgent;
pub use waits::{GraphWaitBridge, resumed_tool_content};

pub type GraphFuture<'a> =
    Pin<Box<dyn Future<Output = Result<GraphOutput, FabricError>> + Send + 'a>>;
pub trait GraphOperator: Send + Sync {
    fn handle(&self, context: GraphContext) -> GraphFuture<'_>;
}
#[derive(Clone)]
pub struct GraphContext {
    pub tenant: String,
    pub run_id: RunId,
    pub instance: String,
    pub event: PublishedGraphEvent,
    pub state: Value,
    pub cancellation: RunCancellation,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct GraphEvent {
    pub kind: String,
    pub payload: Value,
}
impl GraphEvent {
    pub fn new(kind: impl Into<String>, payload: Value) -> Self {
        Self {
            kind: kind.into(),
            payload,
        }
    }
}
pub struct GraphOutput {
    /// Usage incurred by this invocation, not the run cumulative total.
    pub usage: Option<TokenUsage>,
    pub state: Value,
    pub events: Vec<GraphEvent>,
}
impl GraphOutput {
    pub fn with_usage(mut self, usage: Option<TokenUsage>) -> Self {
        self.usage = usage;
        self
    }

    pub fn new(state: Value, events: Vec<GraphEvent>) -> Self {
        Self {
            state,
            events,
            usage: None,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ResourceKind {
    Model,
    Toolset,
    WaitService,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OperatorDescriptor {
    pub kind: String,
    pub version: u32,
    pub config_schema: Value,
    pub inputs: BTreeMap<String, Value>,
    pub outputs: BTreeMap<String, Value>,
    /// Config field -> host resource type. Required fields are specified by config_schema.
    pub resources: BTreeMap<String, ResourceKind>,
    /// Only pure/idempotent handlers may be replayed after an interrupted invocation.
    pub replay_safe: bool,
    /// Independent invocations may overlap; handlers must return null state.
    #[serde(default)]
    pub stateless_parallel: bool,
}
impl OperatorDescriptor {
    pub fn new(kind: impl Into<String>, version: u32) -> Self {
        Self {
            kind: kind.into(),
            version,
            config_schema: serde_json::json!({"type":"object","additionalProperties":false}),
            inputs: BTreeMap::new(),
            outputs: BTreeMap::new(),
            resources: BTreeMap::new(),
            replay_safe: false,
            stateless_parallel: false,
        }
    }
    pub fn input(mut self, event: impl Into<String>, schema: Value) -> Self {
        self.inputs.insert(event.into(), schema);
        self
    }
    pub fn output(mut self, event: impl Into<String>, schema: Value) -> Self {
        self.outputs.insert(event.into(), schema);
        self
    }
}
#[derive(Clone)]
struct ModelResource {
    revision: String,
    port: Arc<dyn ModelPort>,
}
#[derive(Clone)]
struct ToolResource {
    revision: String,
    port: Arc<dyn ToolPort>,
    scheduling: Arc<tokio::sync::RwLock<()>>,
}
#[derive(Clone)]
pub struct AgentResources {
    pub fabric: Arc<dyn OperatorFabric>,
    models: BTreeMap<String, ModelResource>,
    toolsets: BTreeMap<String, ToolResource>,
    waits: BTreeMap<String, (String, Arc<GraphWaitBridge>)>,
    host_policy: Option<super::bindings::HostPolicy>,
    agent_id: Option<String>,
}
impl AgentResources {
    /// Stable host identity, independent of graph names and revisions.
    pub fn with_agent_id(mut self, agent_id: impl Into<String>) -> Self {
        self.agent_id = Some(agent_id.into());
        self
    }
    /// Enforced on both construction and recovery before resource factories run.
    pub fn with_host_policy(mut self, policy: super::bindings::HostPolicy) -> Self {
        self.host_policy = Some(policy);
        self
    }
    pub fn has_wait_service(&self, name: &str) -> bool {
        self.waits.contains_key(name)
    }
    pub fn new(fabric: Arc<dyn OperatorFabric>) -> Self {
        Self {
            fabric,
            models: BTreeMap::new(),
            toolsets: BTreeMap::new(),
            waits: BTreeMap::new(),
            host_policy: None,
            agent_id: None,
        }
    }
    pub fn model(
        mut self,
        name: impl Into<String>,
        revision: impl Into<String>,
        port: Arc<dyn ModelPort>,
    ) -> Self {
        self.models.insert(
            name.into(),
            ModelResource {
                revision: revision.into(),
                port,
            },
        );
        self
    }
    pub fn toolset(
        mut self,
        name: impl Into<String>,
        revision: impl Into<String>,
        port: Arc<dyn ToolPort>,
    ) -> Self {
        self.toolsets.insert(
            name.into(),
            ToolResource {
                revision: revision.into(),
                port,
                scheduling: Arc::new(tokio::sync::RwLock::new(())),
            },
        );
        self
    }
    pub fn wait_service(
        mut self,
        name: impl Into<String>,
        revision: impl Into<String>,
        bridge: Arc<GraphWaitBridge>,
    ) -> Self {
        self.waits.insert(name.into(), (revision.into(), bridge));
        self
    }
    pub fn get_model(&self, name: &str) -> Result<Arc<dyn ModelPort>, FabricError> {
        self.models
            .get(name)
            .map(|r| r.port.clone())
            .ok_or_else(|| invalid("resources", format!("missing model {name}")))
    }
    pub fn get_toolset(&self, name: &str) -> Result<Arc<dyn ToolPort>, FabricError> {
        self.toolsets
            .get(name)
            .map(|r| r.port.clone())
            .ok_or_else(|| invalid("resources", format!("missing toolset {name}")))
    }
}
type Factory =
    dyn Fn(&Value, &AgentResources) -> Result<Arc<dyn GraphOperator>, FabricError> + Send + Sync;
#[derive(Clone)]
struct Registration {
    descriptor: OperatorDescriptor,
    factory: Arc<Factory>,
}
#[derive(Clone, Default)]
pub struct OperatorRegistry {
    entries: BTreeMap<(String, u32), Registration>,
}
impl OperatorRegistry {
    pub fn describe(&self, kind: &str, version: u32) -> Option<&OperatorDescriptor> {
        self.entries
            .get(&(kind.into(), version))
            .map(|entry| &entry.descriptor)
    }
    pub fn builtins() -> Self {
        builtins::registry()
    }
    pub fn register(
        &mut self,
        descriptor: OperatorDescriptor,
        factory: impl Fn(&Value, &AgentResources) -> Result<Arc<dyn GraphOperator>, FabricError>
        + Send
        + Sync
        + 'static,
    ) -> Result<(), FabricError> {
        let key = (descriptor.kind.clone(), descriptor.version);
        if descriptor.kind.trim().is_empty()
            || descriptor.version == 0
            || self.entries.contains_key(&key)
        {
            return Err(invalid("registry", "invalid or duplicate type/version"));
        }
        compile_schema(&descriptor.config_schema, "registry.config_schema")?;
        for (name, schema) in descriptor.inputs.iter().chain(&descriptor.outputs) {
            if !event_name(name) {
                return Err(invalid(
                    "registry.events",
                    format!("invalid versioned event {name}"),
                ));
            }
            compile_schema(schema, "registry.events")?;
            for existing in self
                .entries
                .values()
                .map(|r| &r.descriptor)
                .chain(std::iter::once(&descriptor))
            {
                for other in [existing.inputs.get(name), existing.outputs.get(name)]
                    .into_iter()
                    .flatten()
                {
                    if other != schema {
                        return Err(invalid(
                            "registry.events",
                            format!("conflicting schema for {name}"),
                        ));
                    }
                }
            }
        }
        self.entries.insert(
            key,
            Registration {
                descriptor,
                factory: Arc::new(factory),
            },
        );
        Ok(())
    }
    fn get(&self, op: &OperatorDefinition) -> Result<&Registration, FabricError> {
        self.entries
            .get(&(op.kind.clone(), op.version))
            .ok_or_else(|| {
                invalid(
                    "operators.type",
                    format!("unregistered {}@{}", op.kind, op.version),
                )
            })
    }
}
fn compile_schema(schema: &Value, path: &str) -> Result<jsonschema::JSONSchema, FabricError> {
    // External references would make the static contract depend on network/filesystem state.
    fn local(value: &Value) -> bool {
        match value {
            Value::Object(map) => map.iter().all(|(k, v)| {
                (k != "$ref" || v.as_str().is_some_and(|s| s.starts_with('#'))) && local(v)
            }),
            Value::Array(values) => values.iter().all(local),
            _ => true,
        }
    }
    if !local(schema) {
        return Err(invalid(path, "only local schema references are supported"));
    }
    jsonschema::JSONSchema::compile(schema).map_err(|e| invalid(path, e))
}
pub(crate) fn validate_value(schema: &Value, value: &Value, path: &str) -> Result<(), FabricError> {
    let compiled = compile_schema(schema, path)?;
    if !compiled.is_valid(value) {
        return Err(invalid(path, "value does not satisfy schema"));
    }
    Ok(())
}
fn identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}
fn event_name(value: &str) -> bool {
    value.len() <= 128
        && value.rsplit_once(".v").is_some_and(|(name, version)| {
            !name.is_empty() && version.parse::<u32>().is_ok_and(|v| v > 0)
        })
}
#[derive(Clone)]
struct Compiled {
    definition: AgentDefinition,
    operators: BTreeMap<String, Arc<dyn GraphOperator>>,
    descriptors: BTreeMap<String, OperatorDescriptor>,
    bindings: BTreeMap<String, String>,
}
impl AgentDefinition {
    pub fn validate(
        &self,
        registry: &OperatorRegistry,
        resources: &AgentResources,
    ) -> Result<(), FabricError> {
        compile(self.clone(), registry, resources).map(|_| ())
    }
}
fn compile(
    definition: AgentDefinition,
    registry: &OperatorRegistry,
    resources: &AgentResources,
) -> Result<Compiled, FabricError> {
    if let Some(policy) = &resources.host_policy {
        let mut constrained = definition.clone();
        constrained.apply_host_policy(policy)?;
        if constrained != definition {
            return Err(invalid(
                "host_policy",
                "graph requires reauthorization under current host policy",
            ));
        }
    }
    let caps = resources.fabric.capabilities();
    if !(caps.atomic_batch
        && caps.continuous_offset
        && caps.lease_fencing
        && caps.transactional_outbox)
    {
        return Err(invalid(
            "resources.fabric",
            "transactional graph capabilities required",
        ));
    }
    let d = &definition;
    d.validate_binding_snapshot()?;
    if d.name.trim().is_empty() || d.name.len() > 128 {
        return Err(invalid("name", "required, at most 128 bytes"));
    }
    if d.operators.is_empty() || d.operators.len() > 64 || d.subscriptions.len() > 256 {
        return Err(invalid("operators", "graph size outside bounds"));
    }
    if d.limits.timeout_ms == 0
        || d.limits.timeout_ms > i64::MAX as u64
        || d.limits.max_deliveries == 0
    {
        return Err(invalid("limits", "positive bounded limits required"));
    }
    let mut descriptors = BTreeMap::new();
    let mut bindings = BTreeMap::new();
    for (name, op) in &d.operators {
        let path = format!("operators.{name}");
        if !identifier(name) {
            return Err(invalid(&path, "invalid instance name"));
        }
        let desc = &registry.get(op)?.descriptor;
        validate_value(&desc.config_schema, &op.config, &format!("{path}.config"))?;
        for (field, kind) in &desc.resources {
            if op.config.get(field).is_none() {
                continue;
            }
            let resource = op.config[field]
                .as_str()
                .ok_or_else(|| invalid(&path, format!("missing resource reference {field}")))?;
            let revision = match kind {
                ResourceKind::Model => resources.models.get(resource).map(|r| &r.revision),
                ResourceKind::Toolset => resources.toolsets.get(resource).map(|r| &r.revision),
                ResourceKind::WaitService => resources.waits.get(resource).map(|r| &r.0),
            }
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| invalid(&path, format!("resource {resource} missing or unversioned")))?;
            bindings.insert(format!("{name}:{field}:{resource}"), revision.clone());
        }
        descriptors.insert(name.clone(), desc.clone());
    }
    for (name, op) in &d.operators {
        if op.kind == "agent.context" && op.config["on_max_steps"] == "finalize" {
            let wired = d.subscriptions.iter().any(|s| {
                s.from == *name
                    && s.events.iter().any(|e| e == "finalize.requested.v1")
                    && s.subscriber == d.lifecycle.terminal.from
                    && d.operators
                        .get(&s.subscriber)
                        .is_some_and(|o| o.kind == "agent.finalizer")
            });
            if !wired {
                return Err(invalid(
                    "on_max_steps",
                    "finalize requires a finalizer wired as terminal outlet",
                ));
            }
        }
    }
    // The preflight budget must cover the actual downstream generation limit.
    for (name, op) in &d.operators {
        if op.kind != "agent.compactor" {
            continue;
        }
        let reserve = op.config["reserved_output_tokens"].as_u64().unwrap_or(0);
        for response in &d.subscriptions {
            if response.from != *name || !response.events.iter().any(|e| e == "context.prepared.v1")
            {
                continue;
            }
            for request in &d.subscriptions {
                if request.from != response.subscriber
                    || !request.events.iter().any(|e| {
                        matches!(e.as_str(), "model.requested.v1" | "finalize.requested.v1")
                    })
                {
                    continue;
                }
                if let Some(model) = d.operators.get(&request.subscriber)
                    && matches!(model.kind.as_str(), "agent.model" | "agent.finalizer")
                    && model.config["max_output_tokens"]
                        .as_u64()
                        .is_none_or(|limit| limit > reserve)
                {
                    return Err(invalid(
                        "compactor.reserved_output_tokens",
                        "explicit downstream model output limit must fit reserve",
                    ));
                }
            }
        }
    }
    if d.triggers.is_empty() || d.triggers.len() > 32 {
        return Err(invalid("triggers", "expected 1..32 trigger nodes"));
    }
    for (name, trigger) in &d.triggers {
        let path = format!("triggers.{name}");
        if !identifier(name) || d.operators.contains_key(name) {
            return Err(invalid(&path, "invalid or duplicate node name"));
        }
        if trigger.kind != "manual" || trigger.version != 1 {
            return Err(invalid(&path, "unsupported trigger type or version"));
        }
        if trigger.config != serde_json::json!({}) || !event_name(&trigger.event) {
            return Err(invalid(
                &path,
                "manual trigger requires empty config and a versioned event",
            ));
        }
    }
    if !d.triggers.contains_key(&d.lifecycle.start.trigger) {
        return Err(invalid("lifecycle.start.trigger", "unknown trigger"));
    }
    let terminal = &d.lifecycle.terminal;
    let terminal_desc = descriptors
        .get(&terminal.from)
        .ok_or_else(|| invalid("lifecycle.terminal.from", "unknown instance"))?;
    let terminal_events = [&terminal.completed, &terminal.failed, &terminal.cancelled];
    if terminal_events.into_iter().collect::<BTreeSet<_>>().len() != 3
        || terminal_events
            .iter()
            .any(|e| !terminal_desc.outputs.contains_key(*e))
    {
        return Err(invalid(
            "lifecycle.terminal",
            "three distinct declared outputs required",
        ));
    }
    let mut ids = BTreeSet::new();
    let mut edges = BTreeSet::new();
    let mut trigger_schemas = BTreeMap::new();
    for (i, sub) in d.subscriptions.iter().enumerate() {
        let path = format!("subscriptions[{i}]");
        if !identifier(&sub.id) || !ids.insert(&sub.id) {
            return Err(invalid(
                &format!("{path}.id"),
                "invalid or duplicate subscription ID",
            ));
        }
        let to = descriptors
            .get(&sub.subscriber)
            .ok_or_else(|| invalid(&format!("{path}.subscriber"), "unknown instance"))?;
        if sub.events.is_empty() || sub.events.len() > 32 {
            return Err(invalid(&format!("{path}.events"), "expected 1..32 events"));
        }
        for event in &sub.events {
            let schema = to
                .inputs
                .get(event)
                .ok_or_else(|| invalid(&path, format!("subscriber does not accept {event}")))?;
            if !edges.insert((&sub.from, &sub.subscriber, event)) {
                return Err(invalid(&path, "duplicate edge"));
            }
            if let Some(trigger) = d.triggers.get(&sub.from) {
                if event != &trigger.event {
                    return Err(invalid(&path, "trigger only publishes its declared event"));
                }
                if trigger_schemas
                    .insert(sub.from.clone(), schema)
                    .is_some_and(|old| old != schema)
                {
                    return Err(invalid(&path, "inconsistent trigger payload schema"));
                }
            } else if sub.from == "$external" {
                // Host ingress is explicit in the graph and uses the subscriber's registered schema.
            } else if sub.from == "$engine" {
                if event != "engine.terminal.v1" || schema != &builtins::object_schema() {
                    return Err(invalid(&path, "unknown engine event protocol"));
                }
            } else {
                let from = descriptors
                    .get(&sub.from)
                    .ok_or_else(|| invalid(&format!("{path}.from"), "unknown publisher"))?;
                if from.outputs.get(event) != Some(schema) {
                    return Err(invalid(
                        &path,
                        format!("publisher protocol mismatch for {event}"),
                    ));
                }
            }
        }
    }
    for name in d.triggers.keys() {
        if !trigger_schemas.contains_key(name) {
            return Err(invalid(&format!("triggers.{name}"), "no subscriber"));
        }
    }
    let mut reachable: BTreeSet<String> = d.triggers.keys().cloned().collect();
    reachable.extend(["$engine".to_string(), "$external".to_string()]);
    loop {
        let count = reachable.len();
        for sub in &d.subscriptions {
            if reachable.contains(&sub.from) {
                reachable.insert(sub.subscriber.clone());
            }
        }
        if reachable.len() == count {
            break;
        }
    }
    for name in d.operators.keys() {
        if !reachable.contains(name) {
            return Err(invalid(&format!("operators.{name}"), "unreachable"));
        }
    }
    for trigger in d.triggers.keys() {
        let mut start_reachable = BTreeSet::from([trigger.clone()]);
        loop {
            let count = start_reachable.len();
            for sub in &d.subscriptions {
                if start_reachable.contains(&sub.from) {
                    start_reachable.insert(sub.subscriber.clone());
                }
            }
            if start_reachable.len() == count {
                break;
            }
        }
        if !start_reachable.contains(&terminal.from) {
            return Err(invalid(
                "lifecycle.terminal.from",
                format!("must be reachable from trigger {trigger}"),
            ));
        }
    }
    // No mandatory built-in roles: custom protocols use the identical executor.
    let mut operators = BTreeMap::new();
    for (name, op) in &d.operators {
        operators.insert(
            name.clone(),
            (registry.get(op)?.factory)(&op.config, resources)?,
        );
    }
    Ok(Compiled {
        definition,
        operators,
        descriptors,
        bindings,
    })
}
pub struct AgentBuilder {
    registry: OperatorRegistry,
    resources: AgentResources,
}
impl AgentBuilder {
    pub fn new(registry: OperatorRegistry, resources: AgentResources) -> Self {
        Self {
            registry,
            resources,
        }
    }
    pub fn build_with_bindings(
        mut self,
        definition: AgentDefinition,
        bindings: super::bindings::HostBindings,
        policy: super::bindings::HostPolicy,
    ) -> Result<ConfiguredAgent, FabricError> {
        self.resources.host_policy = Some(policy.clone());
        self.build(definition.bind(bindings, policy)?)
    }
    pub fn build(self, definition: AgentDefinition) -> Result<ConfiguredAgent, FabricError> {
        let compiled = compile(definition, &self.registry, &self.resources)?;
        Ok(ConfiguredAgent {
            engine: Arc::new(GraphEngine::new(compiled, self.registry, self.resources)),
        })
    }
    pub fn from_file_with_bindings(
        self,
        path: impl AsRef<std::path::Path>,
        bindings: super::bindings::HostBindings,
        policy: super::bindings::HostPolicy,
    ) -> Result<ConfiguredAgent, FabricError> {
        self.build_with_bindings(AgentDefinition::from_file(path)?, bindings, policy)
    }
    pub fn from_file(
        self,
        path: impl AsRef<std::path::Path>,
    ) -> Result<ConfiguredAgent, FabricError> {
        self.build(AgentDefinition::from_file(path)?)
    }
}

/// Stable storage identities used by Fabric retention and inspection.
pub fn run_checkpoint_key(tenant: &str, run: RunId) -> String {
    runtime::root(tenant, run)
}
pub fn event_log_key(tenant: &str, run: RunId) -> String {
    runtime::log_key(tenant, run)
}
pub fn public_journal_key(tenant: &str, run: RunId) -> String {
    format!(
        "graph-public:{}",
        crate::contracts::events::EventId::stable("graph-public", &format!("{tenant}:{run}"))
    )
}
