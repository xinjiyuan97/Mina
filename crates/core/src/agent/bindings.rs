//! Versioned host inputs, resolved before operator factories are called.
use super::definition::{AgentDefinition, invalid};
use crate::contracts::fabric::FabricError;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BindingContract {
    pub id: String,
    pub version: u32,
    /// JSON Schema for the entire host values object. Unknown inputs are forbidden.
    pub schema: Value,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostBindings {
    pub contract_id: String,
    pub version: u32,
    pub values: BTreeMap<String, Value>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostPolicy {
    pub allowed_tools: BTreeSet<String>,
    pub allowed_models: BTreeSet<String>,
    pub max_steps: u32,
    pub max_output_tokens: u32,
    pub timeout_ms: u64,
    pub review_level: u8,
    pub allow_run_adf: bool,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BindingSnapshot {
    pub source_digest: String,
    pub bindings: HostBindings,
    pub host_policy: HostPolicy,
    pub resolved_digest: String,
}
pub(crate) fn digest(value: &impl Serialize) -> Result<String, FabricError> {
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(value).map_err(|e| invalid("digest", e))?)
    ))
}
impl AgentDefinition {
    /// Resolve a distributed graph without relying on any internal instance names.
    pub fn bind(mut self, bindings: HostBindings, policy: HostPolicy) -> Result<Self, FabricError> {
        if self.schema_version != 3 || self.binding_snapshot.is_some() {
            return Err(invalid("bindings", "expected unresolved v3 graph"));
        }
        let contract = self
            .bindings
            .as_ref()
            .ok_or_else(|| invalid("bindings", "contract required"))?;
        if contract.id.is_empty()
            || contract.version == 0
            || contract.id != bindings.contract_id
            || contract.version != bindings.version
        {
            return Err(invalid("bindings", "contract identity/version mismatch"));
        }
        if contract.schema["type"] != "object" || contract.schema["additionalProperties"] != false {
            return Err(invalid("bindings.schema", "closed object schema required"));
        }
        super::graph::validate_value(&contract.schema, &json!(bindings.values), "bindings.values")?;
        if serde_json::to_vec(&bindings)
            .map_err(|e| invalid("bindings", e))?
            .len()
            > 1024 * 1024
        {
            return Err(invalid("bindings", "byte limit"));
        }
        let source_digest = digest(&self)?;
        let mut used = BTreeSet::new();
        fn resolve(
            value: &mut Value,
            values: &BTreeMap<String, Value>,
            used: &mut BTreeSet<String>,
        ) -> Result<(), FabricError> {
            match value {
                Value::Object(map) if map.contains_key("$binding") => {
                    if map.len() != 1 {
                        return Err(invalid("$binding", "reference must occupy entire value"));
                    }
                    let name = map["$binding"]
                        .as_str()
                        .ok_or_else(|| invalid("$binding", "string required"))?;
                    let replacement = values
                        .get(name)
                        .ok_or_else(|| invalid(name, "missing binding"))?
                        .clone();
                    used.insert(name.to_string());
                    *value = replacement;
                }
                Value::Object(map) => {
                    for value in map.values_mut() {
                        resolve(value, values, used)?;
                    }
                }
                Value::Array(items) => {
                    for value in items {
                        resolve(value, values, used)?;
                    }
                }
                _ => {}
            }
            Ok(())
        }
        for op in self.operators.values_mut() {
            resolve(&mut op.config, &bindings.values, &mut used)?;
        }
        if let Some(required) = contract.schema["required"].as_array() {
            for name in required.iter().filter_map(Value::as_str) {
                if !used.contains(name) {
                    return Err(invalid(name, "required binding unused"));
                }
            }
        }
        self.apply_host_policy(&policy)?;
        let resolved_digest = digest(&self)?;
        self.binding_snapshot = Some(BindingSnapshot {
            source_digest,
            bindings,
            host_policy: policy,
            resolved_digest,
        });
        Ok(self)
    }
    /// Restrict built-in resource calls; unknown custom implementations fail closed.
    pub fn apply_host_policy(&mut self, policy: &HostPolicy) -> Result<(), FabricError> {
        if policy.max_steps == 0
            || policy.max_steps > 100
            || policy.max_output_tokens == 0
            || policy.timeout_ms == 0
            || policy.review_level > 100
        {
            return Err(invalid("host_policy", "invalid bounds"));
        }
        self.limits.timeout_ms = self.limits.timeout_ms.min(policy.timeout_ms);
        let review = self
            .operators
            .values()
            .filter(|o| matches!(o.kind.as_str(), "agent.tools" | "agent.tool_policy"))
            .map(|o| o.config["review_level"].as_u64().unwrap_or(50))
            .chain([u64::from(policy.review_level)])
            .min()
            .unwrap_or(50);
        for op in self.operators.values_mut() {
            if !matches!(
                op.kind.as_str(),
                "agent.context"
                    | "agent.model"
                    | "agent.gate"
                    | "agent.tools"
                    | "agent.tool_policy"
                    | "agent.wait"
            ) {
                return Err(invalid(
                    "host_policy",
                    "custom operator requires a reviewed policy adapter",
                ));
            }
            if op.kind == "agent.model" {
                let model = op.config["model_name"].as_str().ok_or_else(|| {
                    invalid("model_name", "explicit host-approved model required")
                })?;
                if !policy.allowed_models.contains(model) {
                    return Err(invalid("model_name", "model forbidden by host"));
                }
                let max = op.config["max_output_tokens"]
                    .as_u64()
                    .unwrap_or(u64::from(policy.max_output_tokens));
                op.config["max_output_tokens"] =
                    json!(max.min(u64::from(policy.max_output_tokens)));
            }
            if matches!(op.kind.as_str(), "agent.tools" | "agent.tool_policy") {
                op.config["review_level"] = json!(review);
            }
            if let Some(timeout) = op.config.get_mut("timeout_ms") {
                *timeout = json!(
                    timeout
                        .as_u64()
                        .ok_or_else(|| invalid("timeout_ms", "integer required"))?
                        .min(policy.timeout_ms)
                );
            }
        }
        self.constrain_run(&super::RunOptions {
            max_steps: Some(policy.max_steps),
            allowed_tools: Some(policy.allowed_tools.iter().cloned().collect()),
            allow_run_adf: policy.allow_run_adf,
        })
    }
    pub(crate) fn validate_binding_snapshot(&self) -> Result<(), FabricError> {
        if self.schema_version == 2 {
            if self.bindings.is_some() || self.binding_snapshot.is_some() {
                return Err(invalid("bindings", "v2 does not support bindings"));
            }
            return Ok(());
        }
        if self.schema_version != 3 {
            return Err(invalid("schema_version", "expected 2 or 3"));
        }
        let snapshot = self
            .binding_snapshot
            .as_ref()
            .ok_or_else(|| invalid("bindings", "host bindings must be resolved before build"))?;
        let mut definition = self.clone();
        definition.binding_snapshot = None;
        if digest(&definition)? != snapshot.resolved_digest {
            return Err(invalid("bindings", "resolved graph changed after binding"));
        }
        Ok(())
    }
}
