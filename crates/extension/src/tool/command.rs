use std::{path::Path, sync::Arc};

use crate::sandbox::{
    HostProcessSandbox, ProcessSandbox, ProcessSandboxRequest, SandboxError, SandboxErrorKind,
};
use agent_core::harness::{
    Tool, ToolCallFuture, ToolCallRequest, ToolDefinition, ToolError, ToolErrorCategory,
    ToolOutput, ToolRiskLevel,
};
use serde::Deserialize;

use crate::tool::workspace::Workspace;

pub struct RunCommandTool {
    workspace: Workspace,
    sandbox: Arc<dyn ProcessSandbox>,
}

impl std::fmt::Debug for RunCommandTool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RunCommandTool")
            .field("workspace", &self.workspace)
            .field("sandbox", &self.sandbox.descriptor())
            .finish()
    }
}

impl RunCommandTool {
    pub fn new(workspace_root: impl AsRef<Path>) -> Result<Self, std::io::Error> {
        Ok(Self::from_workspace(Workspace::new(workspace_root)?))
    }

    pub(crate) fn from_workspace(workspace: Workspace) -> Self {
        Self {
            workspace,
            sandbox: Arc::new(HostProcessSandbox::default()),
        }
    }

    pub(crate) fn from_workspace_with_sandbox(
        workspace: Workspace,
        sandbox: Arc<dyn ProcessSandbox>,
    ) -> Self {
        Self { workspace, sandbox }
    }
}

impl Tool for RunCommandTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition::new(
            "run_command",
            "Run one executable through the host-selected ProcessSandbox. This does not invoke a shell. Use an explicit program and argument array.",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "program": {
                        "type": "string",
                        "minLength": 1,
                        "description": "Executable name or path"
                    },
                    "args": {
                        "type": "array",
                        "items": {"type": "string"},
                        "maxItems": 128,
                        "default": []
                    }
                },
                "required": ["program"],
                "additionalProperties": false
            }),
        )
        .with_risk_level(ToolRiskLevel::High)
    }

    fn call(&self, request: ToolCallRequest) -> ToolCallFuture {
        let workspace_root = self.workspace.root().to_owned();
        let sandbox = Arc::clone(&self.sandbox);
        Box::pin(async move {
            let arguments: RunCommandArguments =
                serde_json::from_value(request.arguments).map_err(|_| invalid_arguments())?;
            if arguments.program.trim().is_empty() || arguments.args.len() > 128 {
                return Err(invalid_arguments());
            }
            if request.cancellation.is_cancelled() {
                return Err(cancelled());
            }

            let output = sandbox
                .execute(ProcessSandboxRequest {
                    execution_id: format!("{}:{}", request.run_id, request.call_id),
                    program: arguments.program,
                    args: arguments.args,
                    workspace_root,
                    cancellation: request.cancellation.token(),
                })
                .await
                .map_err(map_sandbox_error)?;

            Ok(ToolOutput::text(
                serde_json::json!({
                    "sandbox": sandbox.descriptor(),
                    "exit_code": output.exit_code,
                    "success": output.success,
                    "stdout": output.stdout,
                    "stderr": output.stderr,
                    "stdout_truncated": output.stdout_truncated,
                    "stderr_truncated": output.stderr_truncated,
                    "duration_ms": output.duration_ms
                })
                .to_string(),
            ))
        })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RunCommandArguments {
    program: String,
    #[serde(default)]
    args: Vec<String>,
}

fn invalid_arguments() -> ToolError {
    ToolError::new(
        "invalid_tool_arguments",
        "command arguments do not match the declared schema",
        false,
    )
}

fn cancelled() -> ToolError {
    ToolError::new("tool_cancelled", "tool execution was cancelled", false)
}

fn map_sandbox_error(error: SandboxError) -> ToolError {
    let category = match error.kind() {
        SandboxErrorKind::InvalidRequest => ToolErrorCategory::InvalidRequest,
        SandboxErrorKind::PolicyDenied => ToolErrorCategory::PermissionDenied,
        SandboxErrorKind::ResourceExhausted => ToolErrorCategory::ResourceExhausted,
        SandboxErrorKind::Timeout => ToolErrorCategory::Timeout,
        SandboxErrorKind::Cancelled => ToolErrorCategory::Cancelled,
        SandboxErrorKind::SpawnFailed | SandboxErrorKind::Unavailable => {
            ToolErrorCategory::Unavailable
        }
        SandboxErrorKind::Io | SandboxErrorKind::Internal => ToolErrorCategory::Internal,
    };
    ToolError::new(error.code(), error.safe_message(), error.retryable()).with_category(category)
}

#[cfg(test)]
mod tests {
    use agent_core::harness::{RunCancellation, RunId, Tool, ToolCallRequest, ToolRiskLevel};
    use serde_json::json;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn command_tool_is_always_high_risk() {
        let directory = tempdir().expect("temporary workspace should be created");
        let tool = RunCommandTool::new(directory.path()).expect("workspace should be valid");

        assert_eq!(tool.definition().risk_level, ToolRiskLevel::High);
    }

    #[tokio::test]
    async fn runs_a_direct_program_and_returns_structured_output() {
        let directory = tempdir().expect("temporary workspace should be created");
        let tool = RunCommandTool::new(directory.path()).expect("workspace should be valid");
        let executable = std::env::current_exe().expect("test executable should be available");
        let output = tool
            .call(ToolCallRequest {
                run_id: RunId::new(),
                call_id: "call-1".into(),
                name: "run_command".into(),
                arguments: json!({
                    "program": executable,
                    "args": ["--list"]
                }),
                cancellation: RunCancellation::new(),
            })
            .await
            .expect("test executable should run");
        let value: serde_json::Value =
            serde_json::from_str(&output.content).expect("output should be JSON");

        assert_eq!(value["success"], true);
        assert_eq!(value["stdout_truncated"], false);
        assert_eq!(value["sandbox"]["kind"], "host_process");
        assert_eq!(value["sandbox"]["isolation"], "none");
    }
}
