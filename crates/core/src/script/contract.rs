use std::{future::Future, pin::Pin};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::harness::{RunCancellation, RunId};

pub const SCRIPT_ABI_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScriptIsolation {
    InProcess,
    Process,
    Container,
    VirtualMachine,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScriptRuntimeDescriptor {
    pub identity: String,
    pub kind: String,
    pub version: String,
    pub engine: String,
    pub engine_version: String,
    pub abi_version: u32,
    pub isolation: ScriptIsolation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScriptLanguage {
    #[serde(rename = "javascript")]
    JavaScript,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScriptPurpose {
    Eval,
    Tool,
    FlowStart,
    FlowResume,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScriptModuleRef {
    pub module_id: String,
    pub revision: u64,
    pub digest: String,
}

/// Source handed to an execution runtime. Artifact resolution happens before
/// this boundary so the engine never reaches into a database or filesystem.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ScriptSource {
    Inline {
        source: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expected_digest: Option<String>,
    },
    ResolvedArtifact {
        reference: ScriptModuleRef,
        source: String,
    },
}

impl ScriptSource {
    #[must_use]
    pub fn source(&self) -> &str {
        match self {
            Self::Inline { source, .. } | Self::ResolvedArtifact { source, .. } => source,
        }
    }

    #[must_use]
    pub fn expected_digest(&self) -> Option<&str> {
        match self {
            Self::Inline {
                expected_digest, ..
            } => expected_digest.as_deref(),
            Self::ResolvedArtifact { reference, .. } => Some(&reference.digest),
        }
    }

    #[must_use]
    pub fn module_ref(&self) -> Option<&ScriptModuleRef> {
        match self {
            Self::Inline { .. } => None,
            Self::ResolvedArtifact { reference, .. } => Some(reference),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ScriptCapability {
    Console,
    DeterministicClock,
    SeededRandom,
    ToolInvoke { allow: Vec<String> },
    EventCommand { allow_topics: Vec<String> },
    JobCommand { allow_kinds: Vec<String> },
    RunControl,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScriptLimits {
    pub timeout_ms: u64,
    pub memory_bytes: u64,
    pub max_stack_bytes: u64,
    pub max_output_bytes: u64,
    pub max_log_lines: u32,
    pub max_log_bytes: u64,
    pub max_host_calls: u32,
}

impl Default for ScriptLimits {
    fn default() -> Self {
        Self {
            timeout_ms: 2_000,
            memory_bytes: 32 * 1024 * 1024,
            max_stack_bytes: 1024 * 1024,
            max_output_bytes: 1024 * 1024,
            max_log_lines: 100,
            max_log_bytes: 64 * 1024,
            max_host_calls: 32,
        }
    }
}

impl ScriptLimits {
    pub fn validate_against(&self, ceiling: &Self) -> Result<(), ScriptError> {
        let valid = self.timeout_ms > 0
            && self.memory_bytes > 0
            && self.max_stack_bytes > 0
            && self.max_output_bytes > 0
            && self.max_log_lines > 0
            && self.max_log_bytes > 0
            && self.timeout_ms <= ceiling.timeout_ms
            && self.memory_bytes <= ceiling.memory_bytes
            && self.max_stack_bytes <= ceiling.max_stack_bytes
            && self.max_output_bytes <= ceiling.max_output_bytes
            && self.max_log_lines <= ceiling.max_log_lines
            && self.max_log_bytes <= ceiling.max_log_bytes
            && self.max_host_calls <= ceiling.max_host_calls;
        if valid {
            Ok(())
        } else {
            Err(ScriptError::new(
                ScriptErrorKind::InvalidRequest,
                "script_invalid_limits",
                "script limits must be positive and cannot exceed the host ceiling",
                false,
            ))
        }
    }
}

#[derive(Debug, Clone)]
pub struct ScriptValidationRequest {
    pub language: ScriptLanguage,
    pub purpose: ScriptPurpose,
    pub source: ScriptSource,
    pub export: String,
    pub requested_capabilities: Vec<ScriptCapability>,
    pub limits: ScriptLimits,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScriptValidationOutput {
    pub source_digest: String,
    pub source_bytes: u64,
    pub export: String,
}

#[derive(Debug, Clone)]
pub struct ScriptExecutionRequest {
    pub execution_id: String,
    pub run_id: Option<RunId>,
    pub language: ScriptLanguage,
    pub purpose: ScriptPurpose,
    pub source: ScriptSource,
    pub export: String,
    pub input: Value,
    pub granted_capabilities: Vec<ScriptCapability>,
    pub limits: ScriptLimits,
    pub cancellation: RunCancellation,
}

impl ScriptExecutionRequest {
    #[must_use]
    pub fn inline(
        execution_id: impl Into<String>,
        source: impl Into<String>,
        input: Value,
    ) -> Self {
        Self {
            execution_id: execution_id.into(),
            run_id: None,
            language: ScriptLanguage::JavaScript,
            purpose: ScriptPurpose::Eval,
            source: ScriptSource::Inline {
                source: source.into(),
                expected_digest: None,
            },
            export: "main".into(),
            input,
            granted_capabilities: Vec::new(),
            limits: ScriptLimits::default(),
            cancellation: RunCancellation::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScriptLogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScriptLog {
    pub level: ScriptLogLevel,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScriptUsage {
    pub duration_ms: u64,
    pub peak_memory_bytes: Option<u64>,
    pub host_calls: u32,
    pub output_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScriptExecutionOutput {
    pub value: Value,
    pub logs: Vec<ScriptLog>,
    pub usage: ScriptUsage,
    pub module_digest: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScriptErrorKind {
    InvalidRequest,
    InvalidSource,
    InvalidResult,
    NotFound,
    CapabilityDenied,
    ResourceExhausted,
    Timeout,
    Cancelled,
    Unavailable,
    Runtime,
    Internal,
}

#[derive(Debug, Error)]
#[error("{message}")]
pub struct ScriptError {
    kind: ScriptErrorKind,
    code: String,
    message: String,
    retryable: bool,
}

impl ScriptError {
    #[must_use]
    pub fn new(
        kind: ScriptErrorKind,
        code: impl Into<String>,
        message: impl Into<String>,
        retryable: bool,
    ) -> Self {
        Self {
            kind,
            code: code.into(),
            message: message.into(),
            retryable,
        }
    }

    #[must_use]
    pub const fn kind(&self) -> ScriptErrorKind {
        self.kind
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
}

pub type ScriptValidationFuture =
    Pin<Box<dyn Future<Output = Result<ScriptValidationOutput, ScriptError>> + Send + 'static>>;
pub type ScriptExecutionFuture =
    Pin<Box<dyn Future<Output = Result<ScriptExecutionOutput, ScriptError>> + Send + 'static>>;

pub trait ScriptRuntime: Send + Sync + 'static {
    fn descriptor(&self) -> ScriptRuntimeDescriptor;
    fn validate(&self, request: ScriptValidationRequest) -> ScriptValidationFuture;
    fn execute(&self, request: ScriptExecutionRequest) -> ScriptExecutionFuture;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_limits_cannot_raise_the_host_ceiling() {
        let ceiling = ScriptLimits::default();
        assert!(ceiling.validate_against(&ceiling).is_ok());

        let mut requested = ceiling;
        requested.timeout_ms += 1;
        let error = requested
            .validate_against(&ceiling)
            .expect_err("a caller cannot raise the host timeout");
        assert_eq!(error.code(), "script_invalid_limits");
    }

    #[test]
    fn resolved_artifacts_expose_the_locked_digest() {
        let source = ScriptSource::ResolvedArtifact {
            reference: ScriptModuleRef {
                module_id: "module-1".into(),
                revision: 3,
                digest: "sha256:locked".into(),
            },
            source: "export function main() { return 1; }".into(),
        };

        assert_eq!(source.expected_digest(), Some("sha256:locked"));
        assert_eq!(source.module_ref().map(|item| item.revision), Some(3));
    }
}
