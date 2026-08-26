//! Versioned data contract shared by the harness, built-in tools, and external tool hosts.
//!
//! Rust traits and async runtimes intentionally do not appear in this crate. The same
//! JSON envelopes can cross a C ABI, a subprocess boundary, or a remote transport.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

/// ABI and JSON envelope version implemented by this crate.
pub const TOOL_PROTOCOL_VERSION: u32 = 1;

/// Provider-neutral function exposed by an Agent to a model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
    #[serde(default)]
    pub risk_level: ToolRiskLevel,
}

impl ToolDefinition {
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        input_schema: Value,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            input_schema,
            risk_level: ToolRiskLevel::default(),
        }
    }

    #[must_use]
    pub const fn with_risk_level(mut self, risk_level: ToolRiskLevel) -> Self {
        self.risk_level = risk_level;
        self
    }
}

/// Advisory blast radius declared by the tool author, never by model output.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolRiskLevel {
    #[default]
    Low,
    Medium,
    High,
}

impl ToolRiskLevel {
    #[must_use]
    pub const fn requires_approval(self) -> bool {
        matches!(self, Self::Medium | Self::High)
    }
}

/// Manifest returned by one external plugin. One statically linked plugin may expose
/// multiple tools through a single ABI entry point.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolPluginManifest {
    pub protocol_version: u32,
    pub plugin_name: String,
    pub plugin_version: String,
    pub tools: Vec<ToolDefinition>,
}

impl ToolPluginManifest {
    #[must_use]
    pub fn new(
        plugin_name: impl Into<String>,
        plugin_version: impl Into<String>,
        tools: Vec<ToolDefinition>,
    ) -> Self {
        Self {
            protocol_version: TOOL_PROTOCOL_VERSION,
            plugin_name: plugin_name.into(),
            plugin_version: plugin_version.into(),
            tools,
        }
    }

    pub fn validate(&self) -> Result<(), ContractError> {
        if self.protocol_version != TOOL_PROTOCOL_VERSION {
            return Err(ContractError::UnsupportedProtocolVersion(
                self.protocol_version,
            ));
        }
        if self.plugin_name.trim().is_empty() {
            return Err(ContractError::EmptyPluginName);
        }
        if self.plugin_version.trim().is_empty() {
            return Err(ContractError::EmptyPluginVersion);
        }

        let mut names = HashSet::with_capacity(self.tools.len());
        for tool in &self.tools {
            let name = tool.name.trim();
            if name.is_empty() {
                return Err(ContractError::EmptyToolName);
            }
            if !names.insert(name) {
                return Err(ContractError::DuplicateToolName(name.to_owned()));
            }
        }
        Ok(())
    }
}

/// One invocation sent to an external tool plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolInvokeRequest {
    pub protocol_version: u32,
    pub run_id: String,
    pub call_id: String,
    pub tool_name: String,
    pub arguments: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_unix_ms: Option<u64>,
}

impl ToolInvokeRequest {
    #[must_use]
    pub fn new(
        run_id: impl Into<String>,
        call_id: impl Into<String>,
        tool_name: impl Into<String>,
        arguments: Value,
    ) -> Self {
        Self {
            protocol_version: TOOL_PROTOCOL_VERSION,
            run_id: run_id.into(),
            call_id: call_id.into(),
            tool_name: tool_name.into(),
            arguments,
            deadline_unix_ms: None,
        }
    }

    #[must_use]
    pub const fn with_deadline_unix_ms(mut self, deadline_unix_ms: u64) -> Self {
        self.deadline_unix_ms = Some(deadline_unix_ms);
        self
    }

    pub fn validate(&self) -> Result<(), ContractError> {
        if self.protocol_version != TOOL_PROTOCOL_VERSION {
            return Err(ContractError::UnsupportedProtocolVersion(
                self.protocol_version,
            ));
        }
        if self.run_id.trim().is_empty() {
            return Err(ContractError::EmptyRunId);
        }
        if self.call_id.trim().is_empty() {
            return Err(ContractError::EmptyCallId);
        }
        if self.tool_name.trim().is_empty() {
            return Err(ContractError::EmptyToolName);
        }
        Ok(())
    }
}

/// Semantic result returned by an external tool. ABI/transport failures are separate
/// from this envelope so a host can distinguish a broken plugin from a normal tool error.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ToolInvokeResult {
    Success {
        content: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        structured_content: Option<Value>,
    },
    Error {
        error: ToolInvokeError,
    },
}

impl ToolInvokeResult {
    #[must_use]
    pub fn success(content: impl Into<String>) -> Self {
        Self::Success {
            content: content.into(),
            structured_content: None,
        }
    }

    #[must_use]
    pub fn error(code: impl Into<String>, message: impl Into<String>, retryable: bool) -> Self {
        Self::Error {
            error: ToolInvokeError {
                code: code.into(),
                message: message.into(),
                category: ToolErrorCategory::Unknown,
                retryable,
                retry_after_ms: None,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolInvokeError {
    pub code: String,
    pub message: String,
    #[serde(default)]
    pub category: ToolErrorCategory,
    pub retryable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolErrorCategory {
    InvalidRequest,
    NotFound,
    PermissionDenied,
    Conflict,
    ResourceExhausted,
    Timeout,
    Cancelled,
    Unavailable,
    Internal,
    #[default]
    Unknown,
}

impl ToolErrorCategory {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidRequest => "invalid_request",
            Self::NotFound => "not_found",
            Self::PermissionDenied => "permission_denied",
            Self::Conflict => "conflict",
            Self::ResourceExhausted => "resource_exhausted",
            Self::Timeout => "timeout",
            Self::Cancelled => "cancelled",
            Self::Unavailable => "unavailable",
            Self::Internal => "internal",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ContractError {
    #[error("unsupported tool protocol version {0}")]
    UnsupportedProtocolVersion(u32),
    #[error("plugin name must not be empty")]
    EmptyPluginName,
    #[error("plugin version must not be empty")]
    EmptyPluginVersion,
    #[error("run id must not be empty")]
    EmptyRunId,
    #[error("call id must not be empty")]
    EmptyCallId,
    #[error("tool name must not be empty")]
    EmptyToolName,
    #[error("duplicate tool name `{0}`")]
    DuplicateToolName(String),
}

#[cfg(test)]
mod tests {
    use jsonschema::{Draft, JSONSchema};
    use serde_json::json;

    use super::*;

    #[test]
    fn validates_and_round_trips_a_plugin_manifest() {
        let manifest = ToolPluginManifest::new(
            "example.tools",
            "1.2.3",
            vec![
                ToolDefinition::new(
                    "lookup",
                    "Look up one record.",
                    json!({
                        "type": "object",
                        "properties": {"id": {"type": "string"}},
                        "required": ["id"],
                        "additionalProperties": false
                    }),
                )
                .with_risk_level(ToolRiskLevel::Medium),
            ],
        );

        manifest.validate().expect("manifest should be valid");
        let encoded = serde_json::to_value(&manifest).expect("manifest should serialize");
        assert_eq!(encoded["protocol_version"], 1);
        assert_eq!(encoded["tools"][0]["risk_level"], "medium");
        assert_eq!(
            serde_json::from_value::<ToolPluginManifest>(encoded)
                .expect("manifest should deserialize"),
            manifest
        );
    }

    #[test]
    fn rejects_duplicate_tools_and_unknown_versions() {
        let definition = ToolDefinition::new("same", "Same tool.", json!({"type": "object"}));
        let duplicate = ToolPluginManifest::new(
            "example.tools",
            "1.0.0",
            vec![definition.clone(), definition],
        );
        assert_eq!(
            duplicate.validate(),
            Err(ContractError::DuplicateToolName("same".into()))
        );

        let mut request = ToolInvokeRequest::new("run", "call", "same", json!({}));
        request.protocol_version = 99;
        assert_eq!(
            request.validate(),
            Err(ContractError::UnsupportedProtocolVersion(99))
        );
    }

    #[test]
    fn uses_stable_tagged_result_envelopes() {
        let success = serde_json::to_value(ToolInvokeResult::success("done"))
            .expect("success should serialize");
        assert_eq!(success, json!({"status": "success", "content": "done"}));

        let error = serde_json::to_value(ToolInvokeResult::error(
            "upstream_timeout",
            "the upstream timed out",
            true,
        ))
        .expect("error should serialize");
        assert_eq!(error["status"], "error");
        assert_eq!(error["error"]["retryable"], true);
    }

    #[test]
    fn rust_envelopes_match_the_published_json_schemas() {
        let manifest = serde_json::to_value(ToolPluginManifest::new(
            "example.tools",
            "1.0.0",
            vec![ToolDefinition::new(
                "lookup",
                "Look up a record.",
                json!({"type": "object"}),
            )],
        ))
        .expect("manifest should serialize");
        assert_matches_schema(
            include_str!("../../../../contracts/tool/v1/manifest.schema.json"),
            &manifest,
        );

        let request = serde_json::to_value(ToolInvokeRequest::new(
            "run-1",
            "call-1",
            "lookup",
            json!({}),
        ))
        .expect("request should serialize");
        assert_matches_schema(
            include_str!("../../../../contracts/tool/v1/invoke-request.schema.json"),
            &request,
        );

        let result = serde_json::to_value(ToolInvokeResult::success("done"))
            .expect("result should serialize");
        assert_matches_schema(
            include_str!("../../../../contracts/tool/v1/invoke-result.schema.json"),
            &result,
        );
    }

    fn assert_matches_schema(source: &str, instance: &Value) {
        let schema: Value = serde_json::from_str(source).expect("schema should be valid JSON");
        let compiled = JSONSchema::options()
            .with_draft(Draft::Draft202012)
            .compile(&schema)
            .expect("schema should compile");
        if let Err(errors) = compiled.validate(instance) {
            let messages = errors.map(|error| error.to_string()).collect::<Vec<_>>();
            panic!("instance did not match schema: {messages:?}");
        }
    }
}
