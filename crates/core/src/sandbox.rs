//! Process sandbox contract implemented by host-specific extensions.

use std::{future::Future, path::PathBuf, pin::Pin, time::Duration};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IsolationStrength {
    None,
    Process,
    Container,
    VirtualMachine,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessSandboxDescriptor {
    pub identity: String,
    pub kind: String,
    pub version: String,
    pub isolation: IsolationStrength,
    pub network_isolated: bool,
    pub filesystem_isolated: bool,
    pub resource_limited: bool,
}

#[derive(Debug, Clone)]
pub struct ProcessSandboxRequest {
    pub execution_id: String,
    pub program: String,
    pub args: Vec<String>,
    pub workspace_root: PathBuf,
    pub cancellation: CancellationToken,
    pub timeout: Option<Duration>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessSandboxOutput {
    pub exit_code: Option<i32>,
    pub success: bool,
    pub stdout: String,
    pub stderr: String,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
    pub duration_ms: u64,
}

#[derive(Debug, Clone)]
pub struct ProcessSandboxSessionRequest {
    pub execution_id: String,
    pub program: String,
    pub args: Vec<String>,
    pub workspace_root: PathBuf,
    pub cancellation: CancellationToken,
    pub timeout: Option<Duration>,
    pub yield_time: Duration,
    pub max_output_bytes: usize,
}

#[derive(Debug, Clone)]
pub struct ProcessSandboxWriteRequest {
    pub session_id: String,
    pub input: String,
    pub close_stdin: bool,
    pub terminate: bool,
    pub yield_time: Duration,
    pub max_output_bytes: usize,
    pub cancellation: CancellationToken,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessSandboxSessionOutput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub exit_code: Option<i32>,
    pub output: String,
    pub output_truncated: bool,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxErrorKind {
    InvalidRequest,
    PolicyDenied,
    SpawnFailed,
    Io,
    ResourceExhausted,
    Timeout,
    Cancelled,
    Unavailable,
    Internal,
}

#[derive(Debug, Error)]
#[error("{message}")]
pub struct SandboxError {
    kind: SandboxErrorKind,
    code: String,
    message: String,
    retryable: bool,
}

impl SandboxError {
    #[must_use]
    pub fn new(
        kind: SandboxErrorKind,
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
    pub const fn kind(&self) -> SandboxErrorKind {
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

pub type ProcessSandboxFuture =
    Pin<Box<dyn Future<Output = Result<ProcessSandboxOutput, SandboxError>> + Send + 'static>>;
pub type ProcessSandboxSessionFuture = Pin<
    Box<dyn Future<Output = Result<ProcessSandboxSessionOutput, SandboxError>> + Send + 'static>,
>;

pub trait ProcessSandbox: Send + Sync + 'static {
    fn descriptor(&self) -> ProcessSandboxDescriptor;
    fn execute(&self, request: ProcessSandboxRequest) -> ProcessSandboxFuture;
    fn start(&self, request: ProcessSandboxSessionRequest) -> ProcessSandboxSessionFuture;
    fn write(&self, request: ProcessSandboxWriteRequest) -> ProcessSandboxSessionFuture;
}
