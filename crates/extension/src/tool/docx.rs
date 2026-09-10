use std::path::Path;

use agent_core::harness::{
    Tool, ToolCallFuture, ToolCallRequest, ToolConcurrency, ToolDefinition, ToolError,
    ToolExecutionPolicy, ToolOutput, ToolRetryPolicy,
};
use serde::Deserialize;

use crate::{artifact::docx::DocxValidator, tool::native_path::NativePathWorkspace};

#[derive(Debug)]
pub struct DocxCheckTool {
    workspace: NativePathWorkspace,
}

impl DocxCheckTool {
    pub fn new(workspace_root: impl AsRef<Path>) -> Result<Self, std::io::Error> {
        Ok(Self {
            workspace: NativePathWorkspace::new(workspace_root)?,
        })
    }

    pub(crate) const fn from_workspace(workspace: NativePathWorkspace) -> Self {
        Self { workspace }
    }
}

impl Tool for DocxCheckTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition::new(
            "docx_check",
            "Check the package structure, XML, content types, relationships, document body, bookmarks and relationship references of an unpacked OOXML directory or .docx file inside the workspace. This does not verify visual layout.",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "minLength": 1,
                        "description": "Workspace-relative path to an unpacked OOXML directory or .docx file"
                    }
                },
                "required": ["path"],
                "additionalProperties": false
            }),
        )
        .with_execution_policy(
            ToolExecutionPolicy::read_only()
                .with_concurrency(ToolConcurrency::ParallelSafe)
                .with_retry(ToolRetryPolicy::bounded(2, 25, 250)),
        )
    }

    fn call(&self, request: ToolCallRequest) -> ToolCallFuture {
        let workspace = self.workspace.clone();
        Box::pin(async move {
            if request.cancellation.is_cancelled() {
                return Err(cancelled());
            }
            let arguments: DocxCheckArguments =
                serde_json::from_value(request.arguments).map_err(|_| invalid_arguments())?;
            let path = workspace.resolve_existing(&arguments.path).await?;
            let display_path = workspace.display_relative(&path);
            let report =
                tokio::task::spawn_blocking(move || DocxValidator::new().validate_path(path))
                    .await
                    .map_err(|_| check_failed())?
                    .map_err(|_| check_failed())?;
            if request.cancellation.is_cancelled() {
                return Err(cancelled());
            }
            Ok(ToolOutput::text(
                serde_json::json!({
                    "path": display_path,
                    "report": report
                })
                .to_string(),
            ))
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DocxCheckArguments {
    path: String,
}

fn invalid_arguments() -> ToolError {
    ToolError::new(
        "invalid_tool_arguments",
        "tool arguments do not match the declared schema",
        false,
    )
}

fn cancelled() -> ToolError {
    ToolError::new("tool_cancelled", "tool execution was cancelled", false)
}

fn check_failed() -> ToolError {
    ToolError::new(
        "docx_check_failed",
        "the DOCX checker could not inspect the requested workspace path",
        false,
    )
}

#[cfg(test)]
mod tests {
    use agent_core::harness::{RunCancellation, RunId, ToolRiskLevel};
    use serde_json::{Value, json};
    use tempfile::tempdir;

    use super::*;

    fn request(arguments: Value) -> ToolCallRequest {
        ToolCallRequest {
            run_id: RunId::new(),
            call_id: "docx-check-call".into(),
            name: "docx_check".into(),
            arguments,
            cancellation: RunCancellation::new(),
        }
    }

    #[test]
    fn definition_is_read_only_and_parallel_safe() {
        let directory = tempdir().expect("temporary workspace");
        let definition = DocxCheckTool::new(directory.path())
            .expect("checker")
            .definition();
        assert_eq!(definition.name, "docx_check");
        assert_eq!(definition.risk_level, ToolRiskLevel::Low);
        assert_eq!(
            definition.execution.concurrency,
            ToolConcurrency::ParallelSafe
        );
    }

    #[tokio::test]
    async fn invalid_package_is_a_successful_report_and_traversal_is_rejected() {
        let directory = tempdir().expect("temporary workspace");
        std::fs::create_dir(directory.path().join("document")).expect("package directory");
        let tool = DocxCheckTool::new(directory.path()).expect("checker");
        let output = tool
            .call(request(json!({"path": "document"})))
            .await
            .expect("findings are not tool failures");
        let output: Value = serde_json::from_str(&output.content).expect("JSON output");
        assert_eq!(output["report"]["valid"], false);

        let error = tool
            .call(request(json!({"path": "../document.docx"})))
            .await
            .expect_err("path traversal should fail");
        assert_eq!(error.code(), "path_outside_workspace");
    }
}
