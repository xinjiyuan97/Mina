use std::{future::Future, pin::Pin};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScriptRuntimeDescriptor {
    pub identity: String,
    pub kind: String,
    pub version: String,
    pub engine: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScriptLimits {
    pub timeout_ms: u64,
    pub memory_bytes: u64,
    pub max_stack_bytes: u64,
    pub max_output_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct ScriptExecutionRequest {
    pub execution_id: String,
    pub source: String,
    pub input: Value,
    pub limits: ScriptLimits,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScriptExecutionOutput {
    pub value: Value,
    pub logs: Vec<String>,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScriptErrorKind {
    InvalidSource,
    CapabilityDenied,
    ResourceExhausted,
    Timeout,
    Cancelled,
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

pub type ScriptExecutionFuture =
    Pin<Box<dyn Future<Output = Result<ScriptExecutionOutput, ScriptError>> + Send + 'static>>;

/// Harness-owned script runtime. The planned QuickJS implementation is an
/// optional runtime feature, while host capabilities remain explicit ports.
pub trait ScriptRuntime: Send + Sync + 'static {
    fn descriptor(&self) -> ScriptRuntimeDescriptor;
    fn execute(&self, request: ScriptExecutionRequest) -> ScriptExecutionFuture;
}
