use std::{collections::BTreeMap, fmt, future::Future, pin::Pin, sync::Arc};

pub use crate::tool::{ToolDefinition, ToolErrorCategory, ToolRiskLevel};
use jsonschema::JSONSchema;
use serde_json::Value;
use thiserror::Error;

use crate::harness::{RunCancellation, RunId};

/// One validated invocation crossing the Agent -> tool boundary.
#[derive(Debug, Clone)]
pub struct ToolCallRequest {
    pub run_id: RunId,
    pub call_id: String,
    pub name: String,
    pub arguments: Value,
    pub cancellation: RunCancellation,
}

/// Text returned to the model after a tool finishes successfully.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolOutput {
    pub content: String,
}

impl ToolOutput {
    #[must_use]
    pub fn text(content: impl Into<String>) -> Self {
        Self {
            content: content.into(),
        }
    }
}

/// Safe tool failure. The Agent normally returns this to the model so it can
/// recover or explain the failure instead of terminating the entire run.
#[derive(Debug, Error)]
#[error("{message}")]
pub struct ToolError {
    code: String,
    message: String,
    category: ToolErrorCategory,
    retryable: bool,
    retry_after_ms: Option<u64>,
}

impl ToolError {
    #[must_use]
    pub fn new(code: impl Into<String>, message: impl Into<String>, retryable: bool) -> Self {
        let code = code.into();
        Self {
            category: infer_category(&code),
            code,
            message: message.into(),
            retryable,
            retry_after_ms: None,
        }
    }

    #[must_use]
    pub const fn with_category(mut self, category: ToolErrorCategory) -> Self {
        self.category = category;
        self
    }

    #[must_use]
    pub const fn with_retry_after_ms(mut self, retry_after_ms: u64) -> Self {
        self.retry_after_ms = Some(retry_after_ms);
        self
    }

    #[must_use]
    pub fn code(&self) -> &str {
        &self.code
    }

    #[must_use]
    pub fn safe_message(&self) -> &str {
        &self.message
    }

    #[must_use]
    pub const fn retryable(&self) -> bool {
        self.retryable
    }

    #[must_use]
    pub const fn category(&self) -> ToolErrorCategory {
        self.category
    }

    #[must_use]
    pub const fn retry_after_ms(&self) -> Option<u64> {
        self.retry_after_ms
    }
}

fn infer_category(code: &str) -> ToolErrorCategory {
    if code.contains("invalid") {
        ToolErrorCategory::InvalidRequest
    } else if code.contains("not_found") {
        ToolErrorCategory::NotFound
    } else if code.contains("denied")
        || code.contains("rejected")
        || code.contains("outside_workspace")
        || code.contains("protected")
        || code.contains("not_allowed")
    {
        ToolErrorCategory::PermissionDenied
    } else if code.contains("conflict") || code.contains("ambiguous") {
        ToolErrorCategory::Conflict
    } else if code.contains("too_large") || code.contains("resource") {
        ToolErrorCategory::ResourceExhausted
    } else if code.contains("timeout") {
        ToolErrorCategory::Timeout
    } else if code.contains("cancelled") {
        ToolErrorCategory::Cancelled
    } else if code.contains("unavailable") || code.contains("spawn_failed") {
        ToolErrorCategory::Unavailable
    } else if code.contains("failed") || code.contains("io") {
        ToolErrorCategory::Internal
    } else {
        ToolErrorCategory::Unknown
    }
}

pub type ToolCallFuture =
    Pin<Box<dyn Future<Output = Result<ToolOutput, ToolError>> + Send + 'static>>;

/// Hexagonal tool boundary implemented by a registry, an MCP bridge, a remote
/// tool host, or a deterministic test fake.
pub trait ToolPort: Send + Sync + 'static {
    fn definitions(&self) -> Vec<ToolDefinition>;

    /// Validate before approval so a user is never asked to approve a call that
    /// the registry would reject immediately afterwards.
    fn validate(&self, _name: &str, _arguments: &Value) -> Result<(), ToolError> {
        Ok(())
    }

    fn call(&self, request: ToolCallRequest) -> ToolCallFuture;
}

/// One executable tool. Registration metadata and execution stay together so
/// hosts cannot accidentally advertise a tool that they cannot invoke.
pub trait Tool: Send + Sync + 'static {
    fn definition(&self) -> ToolDefinition;

    fn call(&self, request: ToolCallRequest) -> ToolCallFuture;
}

/// A validated collection of tools exposed through the provider-neutral tool
/// port. Schemas are compiled once at registration and checked before dispatch.
#[derive(Default)]
pub struct ToolRegistry {
    tools: BTreeMap<String, RegisteredTool>,
}

struct RegisteredTool {
    definition: ToolDefinition,
    input_validator: JSONSchema,
    tool: Arc<dyn Tool>,
}

impl fmt::Debug for ToolRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolRegistry")
            .field("tools", &self.tools.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl ToolRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<T>(&mut self, tool: T) -> Result<(), ToolRegistrationError>
    where
        T: Tool,
    {
        let mut definition = tool.definition();
        let name = definition.name.trim().to_owned();
        if name.is_empty() {
            return Err(ToolRegistrationError::EmptyName);
        }
        if self.tools.contains_key(&name) {
            return Err(ToolRegistrationError::DuplicateName(name));
        }
        definition.name.clone_from(&name);

        let input_validator = JSONSchema::compile(&definition.input_schema).map_err(|error| {
            ToolRegistrationError::InvalidSchema {
                name: name.clone(),
                message: error.to_string(),
            }
        })?;
        self.tools.insert(
            name,
            RegisteredTool {
                definition,
                input_validator,
                tool: Arc::new(tool),
            },
        );
        Ok(())
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.tools.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tools.is_empty()
    }
}

impl ToolPort for ToolRegistry {
    fn definitions(&self) -> Vec<ToolDefinition> {
        self.tools
            .values()
            .map(|registered| registered.definition.clone())
            .collect()
    }

    fn validate(&self, name: &str, arguments: &Value) -> Result<(), ToolError> {
        let Some(registered) = self.tools.get(name) else {
            return Err(ToolError::new(
                "tool_not_found",
                "the requested tool is not registered",
                false,
            ));
        };

        if let Err(mut errors) = registered.input_validator.validate(arguments) {
            let detail = errors.next().map_or_else(
                || "arguments do not match the tool schema".into(),
                |error| error.to_string(),
            );
            return Err(ToolError::new(
                "invalid_tool_arguments",
                format!("tool arguments are invalid: {detail}"),
                false,
            ));
        }

        Ok(())
    }

    fn call(&self, request: ToolCallRequest) -> ToolCallFuture {
        if let Err(error) = self.validate(&request.name, &request.arguments) {
            return Box::pin(async move { Err(error) });
        }

        let registered = self
            .tools
            .get(&request.name)
            .expect("validated tool must remain registered");
        registered.tool.call(request)
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ToolRegistrationError {
    #[error("tool name must not be empty")]
    EmptyName,

    #[error("tool `{0}` is already registered")]
    DuplicateName(String),

    #[error("tool `{name}` has an invalid input schema: {message}")]
    InvalidSchema { name: String, message: String },
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[derive(Clone)]
    struct TestTool {
        name: &'static str,
    }

    impl Tool for TestTool {
        fn definition(&self) -> ToolDefinition {
            ToolDefinition::new(
                self.name,
                "A deterministic test tool.",
                json!({
                    "type": "object",
                    "properties": { "value": { "type": "string" } },
                    "required": ["value"],
                    "additionalProperties": false
                }),
            )
        }

        fn call(&self, request: ToolCallRequest) -> ToolCallFuture {
            Box::pin(async move { Ok(ToolOutput::text(request.arguments.to_string())) })
        }
    }

    fn request(name: &str, arguments: Value) -> ToolCallRequest {
        ToolCallRequest {
            run_id: RunId::new(),
            call_id: "call_1".into(),
            name: name.into(),
            arguments,
            cancellation: RunCancellation::new(),
        }
    }

    #[tokio::test]
    async fn registers_and_dispatches_a_validated_tool() {
        let mut registry = ToolRegistry::new();
        registry
            .register(TestTool { name: "test" })
            .expect("tool should register");

        assert_eq!(registry.definitions()[0].name, "test");
        let output = registry
            .call(request("test", json!({"value": "ok"})))
            .await
            .expect("valid call should succeed");
        assert_eq!(output.content, r#"{"value":"ok"}"#);
    }

    #[test]
    fn preserves_declared_tool_risk() {
        let mut registry = ToolRegistry::new();
        registry
            .register(RiskyTestTool)
            .expect("tool should register");

        assert_eq!(registry.definitions()[0].risk_level, ToolRiskLevel::High);
        assert!(registry.definitions()[0].risk_level.requires_approval());
    }

    #[test]
    fn rejects_duplicate_names() {
        let mut registry = ToolRegistry::new();
        registry
            .register(TestTool { name: "test" })
            .expect("first tool should register");

        let error = registry
            .register(TestTool { name: "test" })
            .expect_err("duplicate should fail");
        assert_eq!(error, ToolRegistrationError::DuplicateName("test".into()));
    }

    #[tokio::test]
    async fn rejects_invalid_arguments_before_dispatch() {
        let mut registry = ToolRegistry::new();
        registry
            .register(TestTool { name: "test" })
            .expect("tool should register");

        let error = registry
            .call(request("test", json!({"value": 42})))
            .await
            .expect_err("schema mismatch should fail");
        assert_eq!(error.code(), "invalid_tool_arguments");
        assert_eq!(error.category(), ToolErrorCategory::InvalidRequest);
    }

    #[tokio::test]
    async fn reports_an_unknown_tool() {
        let registry = ToolRegistry::new();
        let error = registry
            .call(request("missing", json!({})))
            .await
            .expect_err("unknown tool should fail");

        assert_eq!(error.code(), "tool_not_found");
        assert_eq!(error.category(), ToolErrorCategory::NotFound);
    }

    #[test]
    fn preserves_retry_guidance_for_external_tools() {
        let error = ToolError::new("backend_unavailable", "try later", true)
            .with_category(ToolErrorCategory::Unavailable)
            .with_retry_after_ms(500);

        assert!(error.retryable());
        assert_eq!(error.category(), ToolErrorCategory::Unavailable);
        assert_eq!(error.retry_after_ms(), Some(500));
    }

    struct RiskyTestTool;

    impl Tool for RiskyTestTool {
        fn definition(&self) -> ToolDefinition {
            ToolDefinition::new("risky", "A risky test tool.", json!({"type": "object"}))
                .with_risk_level(ToolRiskLevel::High)
        }

        fn call(&self, _request: ToolCallRequest) -> ToolCallFuture {
            Box::pin(async { Ok(ToolOutput::text("ok")) })
        }
    }
}
