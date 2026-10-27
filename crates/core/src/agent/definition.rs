//! Static graph format shared by file and code construction.
use crate::contracts::fabric::FabricError;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, path::Path};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentDefinition {
    pub schema_version: u32,
    pub name: String,
    pub triggers: BTreeMap<String, TriggerDefinition>,
    pub operators: BTreeMap<String, OperatorDefinition>,
    pub subscriptions: Vec<OperatorSubscription>,
    pub lifecycle: GraphLifecycle,
    pub limits: GraphLimits,
}
/// A source node that starts a new Run; it is not an operator within a Run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TriggerDefinition {
    #[serde(rename = "type")]
    pub kind: String,
    pub version: u32,
    pub event: String,
    pub config: Value,
}
impl TriggerDefinition {
    pub fn manual(event: impl Into<String>) -> Self {
        Self {
            kind: "manual".into(),
            version: 1,
            event: event.into(),
            config: serde_json::json!({}),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorDefinition {
    #[serde(rename = "type")]
    pub kind: String,
    pub version: u32,
    pub config: Value,
}
impl OperatorDefinition {
    pub fn new(kind: impl Into<String>, version: u32, config: Value) -> Self {
        Self {
            kind: kind.into(),
            version,
            config,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorSubscription {
    pub id: String,
    pub subscriber: String,
    pub from: String,
    pub events: Vec<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphLifecycle {
    pub start: GraphStart,
    pub terminal: GraphTerminal,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphStart {
    pub trigger: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphTerminal {
    pub from: String,
    pub completed: String,
    pub failed: String,
    pub cancelled: String,
}
impl GraphLifecycle {
    pub fn model(instance: impl Into<String>) -> Self {
        Self {
            start: GraphStart {
                trigger: "start".into(),
            },
            terminal: GraphTerminal {
                from: instance.into(),
                completed: "stop.completed.v1".into(),
                failed: "stop.failed.v1".into(),
                cancelled: "stop.cancelled.v1".into(),
            },
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphLimits {
    pub timeout_ms: u64,
    pub max_deliveries: u64,
}
impl GraphLimits {
    pub fn new(timeout_ms: u64, max_deliveries: u64) -> Self {
        Self {
            timeout_ms,
            max_deliveries,
        }
    }
}
pub(crate) fn invalid(path: &str, message: impl std::fmt::Display) -> FabricError {
    FabricError::Invalid(format!("{path}: {message}"))
}
impl AgentDefinition {
    /// Select the default trigger before admitting a Run. The selected definition is pinned.
    pub fn with_start_trigger(mut self, trigger: impl Into<String>) -> Result<Self, FabricError> {
        let trigger = trigger.into();
        if !self.triggers.contains_key(&trigger) {
            return Err(invalid("lifecycle.start.trigger", "unknown trigger"));
        }
        self.lifecycle.start.trigger = trigger;
        Ok(self)
    }
    pub fn from_json(source: &str) -> Result<Self, FabricError> {
        let value: UniqueValue =
            serde_json::from_str(source).map_err(|e| invalid("document", e))?;
        serde_json::from_value(value.0).map_err(|e| invalid("document", e))
    }
    pub fn from_yaml(source: &str) -> Result<Self, FabricError> {
        // Value parsing rejects duplicate mapping keys; reject tags and non-string keys
        // explicitly before conversion to the same JSON data model used by code.
        let value: serde_yaml::Value =
            serde_yaml::from_str(source).map_err(|e| invalid("document", e))?;
        fn check(value: &serde_yaml::Value) -> Result<(), FabricError> {
            match value {
                serde_yaml::Value::Tagged(_) => {
                    return Err(invalid("document", "YAML tags are unsupported"));
                }
                serde_yaml::Value::Mapping(map) => {
                    for (key, value) in map {
                        if !matches!(key, serde_yaml::Value::String(_)) {
                            return Err(invalid("document", "mapping keys must be strings"));
                        }
                        check(value)?;
                    }
                }
                serde_yaml::Value::Sequence(items) => {
                    for item in items {
                        check(item)?;
                    }
                }
                _ => {}
            }
            Ok(())
        }
        check(&value)?;
        let json = serde_json::to_value(value).map_err(|e| invalid("document", e))?;
        serde_json::from_value(json).map_err(|e| invalid("document", e))
    }
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, FabricError> {
        let path = path.as_ref();
        let source = std::fs::read_to_string(path).map_err(|e| invalid("file", e))?;
        match path.extension().and_then(|s| s.to_str()) {
            Some("json") => Self::from_json(&source),
            Some("yaml" | "yml") => Self::from_yaml(&source),
            _ => Err(invalid("file", "expected .json, .yaml or .yml")),
        }
    }
    pub fn to_json(&self) -> Result<String, FabricError> {
        serde_json::to_string_pretty(self).map_err(|e| invalid("document", e))
    }
}

/// A serializable graph builder, not a second executor.
pub struct AgentGraph {
    name: String,
    triggers: BTreeMap<String, TriggerDefinition>,
    operators: BTreeMap<String, OperatorDefinition>,
    subscriptions: Vec<OperatorSubscription>,
    lifecycle: Option<GraphLifecycle>,
    limits: Option<GraphLimits>,
    duplicate: Option<String>,
}
impl AgentGraph {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            triggers: BTreeMap::new(),
            operators: BTreeMap::new(),
            subscriptions: vec![],
            lifecycle: None,
            limits: None,
            duplicate: None,
        }
    }
    pub fn trigger(mut self, name: impl Into<String>, trigger: TriggerDefinition) -> Self {
        let name = name.into();
        if self.triggers.insert(name.clone(), trigger).is_some() {
            self.duplicate = Some(name);
        }
        self
    }
    pub fn operator(mut self, name: impl Into<String>, operator: OperatorDefinition) -> Self {
        let name = name.into();
        if self.operators.insert(name.clone(), operator).is_some() {
            self.duplicate = Some(name);
        }
        self
    }
    pub fn subscribe(
        mut self,
        id: impl Into<String>,
        from: impl Into<String>,
        subscriber: impl Into<String>,
        events: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.subscriptions.push(OperatorSubscription {
            id: id.into(),
            from: from.into(),
            subscriber: subscriber.into(),
            events: events.into_iter().map(Into::into).collect(),
        });
        self
    }
    pub fn lifecycle(mut self, lifecycle: GraphLifecycle) -> Self {
        self.lifecycle = Some(lifecycle);
        self
    }
    pub fn limits(mut self, limits: GraphLimits) -> Self {
        self.limits = Some(limits);
        self
    }
    pub fn finish(self) -> Result<AgentDefinition, FabricError> {
        if let Some(name) = self.duplicate {
            return Err(invalid("nodes", format!("duplicate instance {name}")));
        }
        Ok(AgentDefinition {
            schema_version: 2,
            name: self.name,
            triggers: self.triggers,
            operators: self.operators,
            subscriptions: self.subscriptions,
            lifecycle: self
                .lifecycle
                .ok_or_else(|| invalid("lifecycle", "required"))?,
            limits: self.limits.ok_or_else(|| invalid("limits", "required"))?,
        })
    }
}

// serde_json::Value normally accepts duplicate keys. Configuration must not.
struct UniqueValue(Value);
impl<'de> Deserialize<'de> for UniqueValue {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = UniqueValue;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a JSON value without duplicate keys")
            }
            fn visit_bool<E: serde::de::Error>(self, v: bool) -> Result<Self::Value, E> {
                Ok(UniqueValue(v.into()))
            }
            fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<Self::Value, E> {
                Ok(UniqueValue(v.into()))
            }
            fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<Self::Value, E> {
                Ok(UniqueValue(v.into()))
            }
            fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<Self::Value, E> {
                serde_json::Number::from_f64(v)
                    .map(|n| UniqueValue(Value::Number(n)))
                    .ok_or_else(|| E::custom("non-finite number"))
            }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
                Ok(UniqueValue(v.into()))
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::Null))
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<Self::Value, A::Error> {
                let mut values = vec![];
                while let Some(value) = seq.next_element::<UniqueValue>()? {
                    values.push(value.0);
                }
                Ok(UniqueValue(Value::Array(values)))
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<Self::Value, A::Error> {
                let mut values = serde_json::Map::new();
                while let Some((key, value)) = map.next_entry::<String, UniqueValue>()? {
                    if values.insert(key.clone(), value.0).is_some() {
                        return Err(serde::de::Error::custom(format!("duplicate key {key}")));
                    }
                }
                Ok(UniqueValue(Value::Object(values)))
            }
        }
        deserializer.deserialize_any(Visitor)
    }
}
