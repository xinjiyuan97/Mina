use std::{path::Path, sync::Arc, time::Duration};

use agent_core::harness::{
    Tool, ToolCallFuture, ToolCallRequest, ToolDefinition, ToolError, ToolOutput, ToolRiskLevel,
};
use serde::Deserialize;

use crate::{
    sandbox::{
        HostProcessSandbox, ProcessSandbox, ProcessSandboxRequest, ProcessSandboxSessionRequest,
        ProcessSandboxWriteRequest, SandboxError, SandboxErrorKind,
    },
    tool::workspace::Workspace,
};

const DEFAULT_TIMEOUT_MS: u64 = 10_000;
const MAX_TIMEOUT_MS: u64 = 300_000;
const DEFAULT_YIELD_MS: u64 = 10_000;
const MAX_YIELD_MS: u64 = 30_000;
const DEFAULT_OUTPUT_BYTES: usize = 40_000;
const MAX_OUTPUT_BYTES: usize = 256 * 1024;

pub struct ShellCommandTool {
    workspace: Workspace,
    sandbox: Arc<dyn ProcessSandbox>,
    max_output_bytes: usize,
}

impl std::fmt::Debug for ShellCommandTool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ShellCommandTool")
            .field("workspace", &self.workspace)
            .field("sandbox", &self.sandbox.descriptor())
            .field("max_output_bytes", &self.max_output_bytes)
            .finish()
    }
}

impl ShellCommandTool {
    pub fn new(workspace_root: impl AsRef<Path>) -> Result<Self, std::io::Error> {
        Ok(Self::from_workspace_with_sandbox(
            Workspace::new(workspace_root)?,
            Arc::new(HostProcessSandbox::default()),
        ))
    }

    pub fn with_max_output_bytes(mut self, max_output_bytes: usize) -> Self {
        self.max_output_bytes = max_output_bytes.clamp(1, MAX_OUTPUT_BYTES);
        self
    }

    pub(crate) fn from_workspace_with_sandbox(
        workspace: Workspace,
        sandbox: Arc<dyn ProcessSandbox>,
    ) -> Self {
        Self {
            workspace,
            sandbox,
            max_output_bytes: DEFAULT_OUTPUT_BYTES,
        }
    }
}

impl Tool for ShellCommandTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition::new(
            "shell_command",
            "Run one shell command inside the configured workspace and return bounded stdout, stderr, exit status and duration. The host adapter is not a security sandbox.",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "command": {"type": "string", "minLength": 1, "description": "Shell script to execute"},
                    "workdir": {"type": "string", "description": "Workspace-relative working directory; defaults to the workspace root"},
                    "timeout_ms": {"type": "integer", "minimum": 1, "maximum": MAX_TIMEOUT_MS, "default": DEFAULT_TIMEOUT_MS}
                },
                "required": ["command"],
                "additionalProperties": false
            }),
        )
        .with_risk_level(ToolRiskLevel::High)
    }

    fn call(&self, request: ToolCallRequest) -> ToolCallFuture {
        let workspace = self.workspace.clone();
        let sandbox = Arc::clone(&self.sandbox);
        let max_output_bytes = self.max_output_bytes;
        Box::pin(async move {
            let arguments: ShellCommandArguments = parse_arguments(request.arguments)?;
            if arguments.command.trim().is_empty()
                || arguments.timeout_ms == 0
                || arguments.timeout_ms > MAX_TIMEOUT_MS
            {
                return Err(invalid_arguments());
            }
            let workdir = resolve_workdir(&workspace, arguments.workdir.as_deref()).await?;
            let output = sandbox
                .execute(ProcessSandboxRequest {
                    execution_id: format!("{}:{}", request.run_id, request.call_id),
                    program: "/bin/sh".into(),
                    args: vec!["-c".into(), arguments.command],
                    workspace_root: workdir,
                    cancellation: request.cancellation.token(),
                    timeout: Some(Duration::from_millis(arguments.timeout_ms)),
                })
                .await
                .map_err(map_sandbox_error)?;
            let (stdout, stdout_truncated) =
                cap_text(output.stdout, max_output_bytes, output.stdout_truncated);
            let (stderr, stderr_truncated) =
                cap_text(output.stderr, max_output_bytes, output.stderr_truncated);
            Ok(ToolOutput::text(
                serde_json::json!({
                    "sandbox": sandbox.descriptor(),
                    "exit_code": output.exit_code,
                    "success": output.success,
                    "stdout": stdout,
                    "stderr": stderr,
                    "stdout_truncated": stdout_truncated,
                    "stderr_truncated": stderr_truncated,
                    "duration_ms": output.duration_ms
                })
                .to_string(),
            ))
        })
    }
}

pub struct ExecCommandTool {
    workspace: Workspace,
    sandbox: Arc<dyn ProcessSandbox>,
}

impl std::fmt::Debug for ExecCommandTool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExecCommandTool")
            .field("workspace", &self.workspace)
            .field("sandbox", &self.sandbox.descriptor())
            .finish()
    }
}

impl ExecCommandTool {
    pub fn new(workspace_root: impl AsRef<Path>) -> Result<Self, std::io::Error> {
        Self::from_workspace_root_with_sandbox(
            workspace_root,
            Arc::new(HostProcessSandbox::default()),
        )
    }

    pub fn from_workspace_root_with_sandbox(
        workspace_root: impl AsRef<Path>,
        sandbox: Arc<dyn ProcessSandbox>,
    ) -> Result<Self, std::io::Error> {
        Ok(Self {
            workspace: Workspace::new(workspace_root)?,
            sandbox,
        })
    }

    pub(crate) fn from_workspace_with_sandbox(
        workspace: Workspace,
        sandbox: Arc<dyn ProcessSandbox>,
    ) -> Self {
        Self { workspace, sandbox }
    }
}

impl Tool for ExecCommandTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition::new(
            "exec_command",
            "Run a shell command, returning output or a session ID for ongoing interaction through write_stdin. Plain pipes are used; PTY allocation is not supported.",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "cmd": {"type": "string", "minLength": 1, "description": "Shell command to execute"},
                    "workdir": {"type": "string", "description": "Workspace-relative working directory; defaults to the workspace root"},
                    "shell": {"type": "string", "minLength": 1, "description": "Shell binary; defaults to /bin/sh"},
                    "yield_time_ms": {"type": "integer", "minimum": 0, "maximum": MAX_YIELD_MS, "default": DEFAULT_YIELD_MS},
                    "max_output_tokens": {"type": "integer", "minimum": 1, "maximum": 65536, "default": 10000},
                    "timeout_ms": {"type": "integer", "minimum": 1, "maximum": MAX_TIMEOUT_MS, "description": "Optional total session runtime limit"}
                },
                "required": ["cmd"],
                "additionalProperties": false
            }),
        )
        .with_risk_level(ToolRiskLevel::High)
    }

    fn call(&self, request: ToolCallRequest) -> ToolCallFuture {
        let workspace = self.workspace.clone();
        let sandbox = Arc::clone(&self.sandbox);
        Box::pin(async move {
            let arguments: ExecCommandArguments = parse_arguments(request.arguments)?;
            if arguments.cmd.trim().is_empty()
                || arguments.yield_time_ms > MAX_YIELD_MS
                || arguments.max_output_tokens == 0
                || arguments.max_output_tokens > 65_536
                || arguments
                    .timeout_ms
                    .is_some_and(|value| value == 0 || value > MAX_TIMEOUT_MS)
            {
                return Err(invalid_arguments());
            }
            let workdir = resolve_workdir(&workspace, arguments.workdir.as_deref()).await?;
            let shell = arguments.shell.unwrap_or_else(|| "/bin/sh".into());
            if shell.trim().is_empty() {
                return Err(invalid_arguments());
            }
            let output = sandbox
                .start(ProcessSandboxSessionRequest {
                    execution_id: format!("{}:{}", request.run_id, request.call_id),
                    program: shell,
                    args: vec!["-c".into(), arguments.cmd],
                    workspace_root: workdir,
                    cancellation: request.cancellation.token(),
                    timeout: arguments.timeout_ms.map(Duration::from_millis),
                    yield_time: Duration::from_millis(arguments.yield_time_ms),
                    max_output_bytes: token_budget_bytes(arguments.max_output_tokens),
                })
                .await
                .map_err(map_sandbox_error)?;
            Ok(ToolOutput::text(session_output_json(output)))
        })
    }
}

pub struct WriteStdinTool {
    sandbox: Arc<dyn ProcessSandbox>,
}

impl std::fmt::Debug for WriteStdinTool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WriteStdinTool")
            .field("sandbox", &self.sandbox.descriptor())
            .finish()
    }
}

impl WriteStdinTool {
    #[must_use]
    pub fn new(sandbox: Arc<dyn ProcessSandbox>) -> Self {
        Self { sandbox }
    }
}

impl Tool for WriteStdinTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition::new(
            "write_stdin",
            "Write characters to an existing exec_command session, poll recent output, close stdin, or terminate the managed process.",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "session_id": {"type": "string", "minLength": 1, "description": "Identifier returned by exec_command"},
                    "chars": {"type": "string", "default": "", "description": "UTF-8 characters to write; empty input polls"},
                    "close_stdin": {"type": "boolean", "default": false, "description": "Close stdin after any write"},
                    "terminate": {"type": "boolean", "default": false, "description": "Terminate the whole managed process group; cannot be combined with chars or close_stdin"},
                    "yield_time_ms": {"type": "integer", "minimum": 0, "maximum": MAX_YIELD_MS, "default": 250},
                    "max_output_tokens": {"type": "integer", "minimum": 1, "maximum": 65536, "default": 10000}
                },
                "required": ["session_id"],
                "additionalProperties": false
            }),
        )
        .with_risk_level(ToolRiskLevel::High)
    }

    fn call(&self, request: ToolCallRequest) -> ToolCallFuture {
        let sandbox = Arc::clone(&self.sandbox);
        Box::pin(async move {
            let arguments: WriteStdinArguments = parse_arguments(request.arguments)?;
            if arguments.session_id.trim().is_empty()
                || arguments.yield_time_ms > MAX_YIELD_MS
                || arguments.max_output_tokens == 0
                || arguments.max_output_tokens > 65_536
                || (arguments.terminate && (arguments.close_stdin || !arguments.chars.is_empty()))
            {
                return Err(invalid_arguments());
            }
            let output = sandbox
                .write(ProcessSandboxWriteRequest {
                    session_id: arguments.session_id,
                    input: arguments.chars,
                    close_stdin: arguments.close_stdin,
                    terminate: arguments.terminate,
                    yield_time: Duration::from_millis(arguments.yield_time_ms),
                    max_output_bytes: token_budget_bytes(arguments.max_output_tokens),
                    cancellation: request.cancellation.token(),
                })
                .await
                .map_err(map_sandbox_error)?;
            Ok(ToolOutput::text(session_output_json(output)))
        })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ShellCommandArguments {
    command: String,
    workdir: Option<String>,
    #[serde(default = "default_timeout_ms")]
    timeout_ms: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecCommandArguments {
    cmd: String,
    workdir: Option<String>,
    shell: Option<String>,
    #[serde(default = "default_yield_ms")]
    yield_time_ms: u64,
    #[serde(default = "default_output_tokens")]
    max_output_tokens: usize,
    timeout_ms: Option<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WriteStdinArguments {
    session_id: String,
    #[serde(default)]
    chars: String,
    #[serde(default)]
    close_stdin: bool,
    #[serde(default)]
    terminate: bool,
    #[serde(default = "default_write_yield_ms")]
    yield_time_ms: u64,
    #[serde(default = "default_output_tokens")]
    max_output_tokens: usize,
}

async fn resolve_workdir(
    workspace: &Workspace,
    requested: Option<&str>,
) -> Result<std::path::PathBuf, ToolError> {
    let path = workspace.resolve_existing(requested.unwrap_or("")).await?;
    let metadata = tokio::fs::metadata(&path).await.map_err(|_| {
        ToolError::new(
            "workdir_unavailable",
            "the requested working directory is unavailable",
            false,
        )
    })?;
    if !metadata.is_dir() {
        return Err(ToolError::new(
            "workdir_not_directory",
            "the requested working directory is not a directory",
            false,
        ));
    }
    Ok(path)
}

fn session_output_json(output: agent_core::sandbox::ProcessSandboxSessionOutput) -> String {
    let mut value = serde_json::json!({
        "exit_code": output.exit_code,
        "output": output.output,
        "output_truncated": output.output_truncated,
        "duration_ms": output.duration_ms
    });
    if let Some(session_id) = output.session_id {
        value["session_id"] = serde_json::Value::String(session_id);
    }
    value.to_string()
}

fn cap_text(value: String, cap: usize, already_truncated: bool) -> (String, bool) {
    if value.len() <= cap {
        return (value, already_truncated);
    }
    let mut boundary = cap;
    while !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    (value[..boundary].to_owned(), true)
}

fn token_budget_bytes(tokens: usize) -> usize {
    tokens.saturating_mul(4).clamp(1, MAX_OUTPUT_BYTES)
}

fn parse_arguments<T: for<'de> Deserialize<'de>>(
    arguments: serde_json::Value,
) -> Result<T, ToolError> {
    serde_json::from_value(arguments).map_err(|_| invalid_arguments())
}

fn invalid_arguments() -> ToolError {
    ToolError::new(
        "invalid_tool_arguments",
        "terminal arguments do not match the declared schema",
        false,
    )
}

fn map_sandbox_error(error: SandboxError) -> ToolError {
    let category = match error.kind() {
        SandboxErrorKind::InvalidRequest => agent_core::harness::ToolErrorCategory::InvalidRequest,
        SandboxErrorKind::PolicyDenied => agent_core::harness::ToolErrorCategory::PermissionDenied,
        SandboxErrorKind::ResourceExhausted => {
            agent_core::harness::ToolErrorCategory::ResourceExhausted
        }
        SandboxErrorKind::Timeout => agent_core::harness::ToolErrorCategory::Timeout,
        SandboxErrorKind::Cancelled => agent_core::harness::ToolErrorCategory::Cancelled,
        SandboxErrorKind::SpawnFailed | SandboxErrorKind::Unavailable => {
            agent_core::harness::ToolErrorCategory::Unavailable
        }
        SandboxErrorKind::Io | SandboxErrorKind::Internal => {
            agent_core::harness::ToolErrorCategory::Internal
        }
    };
    ToolError::new(error.code(), error.safe_message(), error.retryable()).with_category(category)
}

const fn default_timeout_ms() -> u64 {
    DEFAULT_TIMEOUT_MS
}
const fn default_yield_ms() -> u64 {
    DEFAULT_YIELD_MS
}
const fn default_write_yield_ms() -> u64 {
    250
}
const fn default_output_tokens() -> usize {
    10_000
}
