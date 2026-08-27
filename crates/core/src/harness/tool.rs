use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
};

pub use crate::tool::{
    ToolCompletion, ToolConcurrency, ToolDefinition, ToolErrorCategory, ToolExecutionPolicy,
    ToolIdempotency, ToolRetryPolicy, ToolRiskLevel,
};
use jsonschema::JSONSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::harness::{EffectRequest, RunCancellation, RunId, WaitSpec};

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
#[derive(Debug, Clone, PartialEq)]
pub struct ToolOutput {
    pub content: String,
    pub suspension: Option<ToolSuspension>,
}

impl ToolOutput {
    #[must_use]
    pub fn text(content: impl Into<String>) -> Self {
        Self {
            content: content.into(),
            suspension: None,
        }
    }

    #[must_use]
    pub fn suspend(waits: Vec<WaitSpec>, effects: Vec<EffectRequest>) -> Self {
        Self {
            content: String::new(),
            suspension: Some(ToolSuspension { waits, effects }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolSuspension {
    pub waits: Vec<WaitSpec>,
    pub effects: Vec<EffectRequest>,
}

/// Host-owned visibility policy for arguments written to public events,
/// persistence, and ordinary observability sinks. It does not alter the full
/// arguments delivered to the tool or returned to the model conversation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolArgumentVisibility {
    #[default]
    Full,
    DigestOnly,
}

/// Origin of one binding in a run-scoped tool-set snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolBindingKind {
    Static,
    AdfManagement,
    AgentDefined,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolBinding {
    pub name: String,
    pub kind: ToolBindingKind,
    pub argument_visibility: ToolArgumentVisibility,
}

/// Immutable tool view used for exactly one model step. Calls from that model
/// response must be validated and dispatched against the same revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolSetSnapshot {
    pub revision: u64,
    pub digest: String,
    pub definitions: Vec<ToolDefinition>,
    pub bindings: Vec<ToolBinding>,
}

impl ToolSetSnapshot {
    pub fn new(
        revision: u64,
        mut definitions: Vec<ToolDefinition>,
        mut bindings: Vec<ToolBinding>,
    ) -> Result<Self, ToolError> {
        definitions.sort_by(|left, right| left.name.cmp(&right.name));
        bindings.sort_by(|left, right| left.name.cmp(&right.name));
        let definition_names = definitions
            .iter()
            .map(|definition| definition.name.as_str())
            .collect::<BTreeSet<_>>();
        let binding_names = bindings
            .iter()
            .map(|binding| binding.name.as_str())
            .collect::<BTreeSet<_>>();
        if definitions.len() != definition_names.len()
            || bindings.len() != binding_names.len()
            || definition_names != binding_names
        {
            return Err(ToolError::new(
                "tool_set_invalid",
                "tool-set definitions and bindings must be unique and have matching names",
                false,
            ));
        }
        let bytes = serde_json::to_vec(&(&definitions, &bindings)).map_err(|_| {
            ToolError::new(
                "tool_set_digest_failed",
                "tool-set metadata could not be encoded",
                false,
            )
        })?;
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        Ok(Self {
            revision,
            digest: format!("sha256:{:x}", hasher.finalize()),
            definitions,
            bindings,
        })
    }

    #[must_use]
    pub fn binding(&self, name: &str) -> Option<&ToolBinding> {
        self.bindings.iter().find(|binding| binding.name == name)
    }

    pub fn retain(
        mut self,
        mut predicate: impl FnMut(&ToolBinding) -> bool,
    ) -> Result<Self, ToolError> {
        self.bindings.retain(|binding| predicate(binding));
        let names = self
            .bindings
            .iter()
            .map(|binding| binding.name.as_str())
            .collect::<BTreeSet<_>>();
        self.definitions
            .retain(|definition| names.contains(definition.name.as_str()));
        Self::new(self.revision, self.definitions, self.bindings)
    }
}

#[must_use]
pub fn digest_only_arguments(arguments: &Value) -> Value {
    let encoded = serde_json::to_vec(arguments).unwrap_or_default();
    digest_only_bytes(&encoded)
}

#[must_use]
pub fn digest_only_bytes(bytes: &[u8]) -> Value {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    json!({
        "redacted": true,
        "bytes": bytes.len(),
        "digest": format!("sha256:{:x}", hasher.finalize()),
    })
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

    /// Returns the immutable binding set for one model step. Static tool ports
    /// inherit this implementation; dynamic ports override it per run.
    fn tool_set_snapshot(&self, _run_id: RunId) -> Result<ToolSetSnapshot, ToolError> {
        let definitions = self.definitions();
        let bindings = definitions
            .iter()
            .map(|definition| ToolBinding {
                name: definition.name.clone(),
                kind: ToolBindingKind::Static,
                argument_visibility: self.argument_visibility(&definition.name),
            })
            .collect();
        ToolSetSnapshot::new(0, definitions, bindings)
    }

    fn argument_visibility(&self, _name: &str) -> ToolArgumentVisibility {
        ToolArgumentVisibility::Full
    }

    /// Validate before approval so a user is never asked to approve a call that
    /// the registry would reject immediately afterwards.
    fn validate(&self, _name: &str, _arguments: &Value) -> Result<(), ToolError> {
        Ok(())
    }

    fn validate_at(
        &self,
        _run_id: RunId,
        _tool_set_revision: u64,
        name: &str,
        arguments: &Value,
    ) -> Result<(), ToolError> {
        self.validate(name, arguments)
    }

    fn call(&self, request: ToolCallRequest) -> ToolCallFuture;

    fn call_at(&self, _tool_set_revision: u64, request: ToolCallRequest) -> ToolCallFuture {
        self.call(request)
    }

    fn close_run(&self, _run_id: RunId) {}
}

/// Marker for a ToolPort that participates in run-scoped snapshot semantics.
/// Static ports receive the default revision-zero implementation above.
pub trait RunToolSession: ToolPort {}

impl<T> RunToolSession for T where T: ToolPort + ?Sized {}

/// One executable tool. Registration metadata and execution stay together so
/// hosts cannot accidentally advertise a tool that they cannot invoke.
pub trait Tool: Send + Sync + 'static {
    fn definition(&self) -> ToolDefinition;

    fn argument_visibility(&self) -> ToolArgumentVisibility {
        ToolArgumentVisibility::Full
    }

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
    argument_visibility: ToolArgumentVisibility,
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
        definition.execution.validate().map_err(|error| {
            ToolRegistrationError::InvalidExecutionPolicy {
                name: name.clone(),
                message: error.to_string(),
            }
        })?;

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
                argument_visibility: tool.argument_visibility(),
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

    fn argument_visibility(&self, name: &str) -> ToolArgumentVisibility {
        self.tools
            .get(name)
            .map_or(ToolArgumentVisibility::Full, |registered| {
                registered.argument_visibility
            })
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

    #[error("tool `{name}` has an invalid execution policy: {message}")]
    InvalidExecutionPolicy { name: String, message: String },
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

    #[test]
    fn rejects_automatic_retries_without_idempotency() {
        let mut registry = ToolRegistry::new();
        let error = registry
            .register(PolicyTestTool {
                policy: ToolExecutionPolicy::default()
                    .with_retry(ToolRetryPolicy::bounded(2, 1, 10)),
            })
            .expect_err("unknown side effects must not be replayed automatically");

        assert!(matches!(
            error,
            ToolRegistrationError::InvalidExecutionPolicy { .. }
        ));
    }

    #[test]
    fn rejects_parallel_tools_that_can_suspend() {
        let mut registry = ToolRegistry::new();
        let error = registry
            .register(PolicyTestTool {
                policy: ToolExecutionPolicy::read_only()
                    .with_concurrency(ToolConcurrency::ParallelSafe)
                    .with_completion(ToolCompletion::MaySuspend),
            })
            .expect_err("a suspending tool cannot join a parallel batch");

        assert!(matches!(
            error,
            ToolRegistrationError::InvalidExecutionPolicy { .. }
        ));
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

    struct PolicyTestTool {
        policy: ToolExecutionPolicy,
    }

    impl Tool for PolicyTestTool {
        fn definition(&self) -> ToolDefinition {
            ToolDefinition::new(
                "policy_test",
                "A tool with a configurable execution policy.",
                json!({"type": "object"}),
            )
            .with_execution_policy(self.policy)
        }

        fn call(&self, _request: ToolCallRequest) -> ToolCallFuture {
            Box::pin(async { Ok(ToolOutput::text("ok")) })
        }
    }

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
