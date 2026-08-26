use agent_core::harness::{
    Tool, ToolCallFuture, ToolCallRequest, ToolDefinition, ToolError, ToolOutput, ToolRiskLevel,
};
use std::path::Path;

use serde::Deserialize;

use crate::tool::workspace::Workspace;

const MAX_TEXT_FILE_BYTES: u64 = 1024 * 1024;
const MAX_DIRECTORY_ENTRIES: usize = 500;

#[derive(Debug)]
pub struct ReadTool {
    workspace: Workspace,
}

impl ReadTool {
    pub fn new(workspace_root: impl AsRef<Path>) -> Result<Self, std::io::Error> {
        Ok(Self {
            workspace: Workspace::new(workspace_root)?,
        })
    }

    pub(crate) const fn from_workspace(workspace: Workspace) -> Self {
        Self { workspace }
    }
}

impl Tool for ReadTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition::new(
            "read",
            "Read one UTF-8 text file inside the configured workspace. Paths are relative to the workspace root.",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "minLength": 1,
                        "description": "Workspace-relative file path"
                    }
                },
                "required": ["path"],
                "additionalProperties": false
            }),
        )
    }

    fn call(&self, request: ToolCallRequest) -> ToolCallFuture {
        let workspace = self.workspace.clone();
        Box::pin(async move {
            if request.cancellation.is_cancelled() {
                return Err(cancelled());
            }
            let arguments: ReadTextFileArguments = parse_arguments(request.arguments)?;
            let path = workspace.resolve_existing(&arguments.path).await?;
            let metadata = tokio::fs::metadata(&path)
                .await
                .map_err(|_| unavailable_path())?;
            if !metadata.is_file() {
                return Err(ToolError::new(
                    "path_not_file",
                    "the requested workspace path is not a file",
                    false,
                ));
            }
            if metadata.len() > MAX_TEXT_FILE_BYTES {
                return Err(ToolError::new(
                    "file_too_large",
                    "the requested text file exceeds the 1 MiB tool limit",
                    false,
                ));
            }

            let bytes = tokio::fs::read(&path)
                .await
                .map_err(|_| unavailable_path())?;
            let content = String::from_utf8(bytes).map_err(|_| {
                ToolError::new(
                    "file_not_utf8",
                    "the requested file is not valid UTF-8 text",
                    false,
                )
            })?;
            Ok(ToolOutput::text(
                serde_json::json!({
                    "path": workspace.display_relative(&path),
                    "content": content
                })
                .to_string(),
            ))
        })
    }
}

#[derive(Debug)]
pub struct WriteTool {
    workspace: Workspace,
}

impl WriteTool {
    pub fn new(workspace_root: impl AsRef<Path>) -> Result<Self, std::io::Error> {
        Ok(Self {
            workspace: Workspace::new(workspace_root)?,
        })
    }

    pub(crate) const fn from_workspace(workspace: Workspace) -> Self {
        Self { workspace }
    }
}

impl Tool for WriteTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition::new(
            "write",
            "Create or replace one UTF-8 text file inside the workspace. The parent directory must already exist and symbolic-link destinations are rejected.",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "minLength": 1,
                        "description": "Workspace-relative destination path"
                    },
                    "content": {
                        "type": "string",
                        "description": "Complete UTF-8 file content"
                    }
                },
                "required": ["path", "content"],
                "additionalProperties": false
            }),
        )
        .with_risk_level(ToolRiskLevel::Medium)
    }

    fn call(&self, request: ToolCallRequest) -> ToolCallFuture {
        let workspace = self.workspace.clone();
        Box::pin(async move {
            if request.cancellation.is_cancelled() {
                return Err(cancelled());
            }
            let arguments: WriteArguments = parse_arguments(request.arguments)?;
            if arguments.content.len() as u64 > MAX_TEXT_FILE_BYTES {
                return Err(file_too_large());
            }
            let path = workspace.resolve_for_write(&arguments.path).await?;
            let created = tokio::fs::symlink_metadata(&path).await.is_err();
            if !created {
                let metadata = tokio::fs::metadata(&path)
                    .await
                    .map_err(|_| unavailable_path())?;
                if !metadata.is_file() {
                    return Err(ToolError::new(
                        "path_not_file",
                        "the requested workspace path is not a file",
                        false,
                    ));
                }
            }
            tokio::fs::write(&path, arguments.content.as_bytes())
                .await
                .map_err(|_| write_failed())?;
            Ok(ToolOutput::text(
                serde_json::json!({
                    "path": workspace.display_relative(&path),
                    "bytes_written": arguments.content.len(),
                    "created": created
                })
                .to_string(),
            ))
        })
    }
}

#[derive(Debug)]
pub struct EditTool {
    workspace: Workspace,
}

impl EditTool {
    pub fn new(workspace_root: impl AsRef<Path>) -> Result<Self, std::io::Error> {
        Ok(Self {
            workspace: Workspace::new(workspace_root)?,
        })
    }

    pub(crate) const fn from_workspace(workspace: Workspace) -> Self {
        Self { workspace }
    }
}

impl Tool for EditTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition::new(
            "edit",
            "Replace an exact UTF-8 text fragment in one existing workspace file. By default the old text must occur exactly once.",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "minLength": 1,
                        "description": "Workspace-relative file path"
                    },
                    "old_text": {
                        "type": "string",
                        "minLength": 1,
                        "description": "Exact text to replace"
                    },
                    "new_text": {
                        "type": "string",
                        "description": "Replacement text"
                    },
                    "replace_all": {
                        "type": "boolean",
                        "default": false,
                        "description": "Replace every exact occurrence instead of requiring one"
                    }
                },
                "required": ["path", "old_text", "new_text"],
                "additionalProperties": false
            }),
        )
        .with_risk_level(ToolRiskLevel::Medium)
    }

    fn call(&self, request: ToolCallRequest) -> ToolCallFuture {
        let workspace = self.workspace.clone();
        Box::pin(async move {
            if request.cancellation.is_cancelled() {
                return Err(cancelled());
            }
            let arguments: EditArguments = parse_arguments(request.arguments)?;
            if arguments.old_text.is_empty() {
                return Err(ToolError::new(
                    "empty_edit_match",
                    "old_text must not be empty",
                    false,
                ));
            }
            let path = workspace.resolve_for_write(&arguments.path).await?;
            let metadata = tokio::fs::metadata(&path)
                .await
                .map_err(|_| unavailable_path())?;
            if !metadata.is_file() {
                return Err(ToolError::new(
                    "path_not_file",
                    "the requested workspace path is not a file",
                    false,
                ));
            }
            if metadata.len() > MAX_TEXT_FILE_BYTES {
                return Err(file_too_large());
            }
            let bytes = tokio::fs::read(&path)
                .await
                .map_err(|_| unavailable_path())?;
            let content = String::from_utf8(bytes).map_err(|_| {
                ToolError::new(
                    "file_not_utf8",
                    "the requested file is not valid UTF-8 text",
                    false,
                )
            })?;
            let occurrences = content.matches(&arguments.old_text).count();
            if occurrences == 0 {
                return Err(ToolError::new(
                    "edit_match_not_found",
                    "old_text was not found in the requested file",
                    false,
                ));
            }
            if occurrences > 1 && !arguments.replace_all {
                return Err(ToolError::new(
                    "edit_match_ambiguous",
                    "old_text occurs more than once; provide more context or set replace_all",
                    false,
                ));
            }
            let edited = if arguments.replace_all {
                content.replace(&arguments.old_text, &arguments.new_text)
            } else {
                content.replacen(&arguments.old_text, &arguments.new_text, 1)
            };
            if edited.len() as u64 > MAX_TEXT_FILE_BYTES {
                return Err(file_too_large());
            }
            if request.cancellation.is_cancelled() {
                return Err(cancelled());
            }
            tokio::fs::write(&path, edited.as_bytes())
                .await
                .map_err(|_| write_failed())?;
            Ok(ToolOutput::text(
                serde_json::json!({
                    "path": workspace.display_relative(&path),
                    "replacements": if arguments.replace_all { occurrences } else { 1 },
                    "bytes_written": edited.len()
                })
                .to_string(),
            ))
        })
    }
}

#[derive(Debug)]
pub struct ListDirectoryTool {
    workspace: Workspace,
}

impl ListDirectoryTool {
    pub fn new(workspace_root: impl AsRef<Path>) -> Result<Self, std::io::Error> {
        Ok(Self {
            workspace: Workspace::new(workspace_root)?,
        })
    }

    pub(crate) const fn from_workspace(workspace: Workspace) -> Self {
        Self { workspace }
    }
}

impl Tool for ListDirectoryTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition::new(
            "list_directory",
            "List direct children of one directory inside the configured workspace. Paths are relative to the workspace root.",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Workspace-relative directory path; defaults to the root"
                    }
                },
                "additionalProperties": false
            }),
        )
    }

    fn call(&self, request: ToolCallRequest) -> ToolCallFuture {
        let workspace = self.workspace.clone();
        Box::pin(async move {
            let arguments: ListDirectoryArguments = parse_arguments(request.arguments)?;
            let path = workspace.resolve_existing(&arguments.path).await?;
            let mut directory = tokio::fs::read_dir(&path)
                .await
                .map_err(|_| unavailable_path())?;
            let mut entries = Vec::new();
            let mut truncated = false;

            while let Some(entry) = directory
                .next_entry()
                .await
                .map_err(|_| unavailable_path())?
            {
                if request.cancellation.is_cancelled() {
                    return Err(cancelled());
                }
                if entries.len() == MAX_DIRECTORY_ENTRIES {
                    truncated = true;
                    break;
                }
                let file_type = entry.file_type().await.map_err(|_| unavailable_path())?;
                let kind = if file_type.is_dir() {
                    "directory"
                } else if file_type.is_file() {
                    "file"
                } else if file_type.is_symlink() {
                    "symlink"
                } else {
                    "other"
                };
                entries.push(serde_json::json!({
                    "path": workspace.display_relative(&entry.path()),
                    "kind": kind
                }));
            }
            entries.sort_by(|left, right| left["path"].as_str().cmp(&right["path"].as_str()));

            Ok(ToolOutput::text(
                serde_json::json!({
                    "path": workspace.display_relative(&path),
                    "entries": entries,
                    "truncated": truncated
                })
                .to_string(),
            ))
        })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadTextFileArguments {
    path: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WriteArguments {
    path: String,
    content: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EditArguments {
    path: String,
    old_text: String,
    new_text: String,
    #[serde(default)]
    replace_all: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ListDirectoryArguments {
    #[serde(default = "workspace_root")]
    path: String,
}

fn workspace_root() -> String {
    ".".into()
}

fn parse_arguments<T>(arguments: serde_json::Value) -> Result<T, ToolError>
where
    T: for<'de> Deserialize<'de>,
{
    serde_json::from_value(arguments).map_err(|_| {
        ToolError::new(
            "invalid_tool_arguments",
            "tool arguments do not match the declared schema",
            false,
        )
    })
}

fn unavailable_path() -> ToolError {
    ToolError::new(
        "path_unavailable",
        "the requested workspace path is unavailable",
        false,
    )
}

fn file_too_large() -> ToolError {
    ToolError::new(
        "file_too_large",
        "the requested text file exceeds the 1 MiB tool limit",
        false,
    )
}

fn write_failed() -> ToolError {
    ToolError::new(
        "file_write_failed",
        "the requested workspace file could not be written",
        false,
    )
}

fn cancelled() -> ToolError {
    ToolError::new("tool_cancelled", "tool execution was cancelled", false)
}

#[cfg(test)]
mod tests {
    use agent_core::harness::{RunCancellation, RunId, ToolCallRequest};
    use serde_json::json;
    use tempfile::tempdir;

    use super::*;

    #[tokio::test]
    async fn reads_utf8_files_inside_the_workspace() {
        let directory = tempdir().expect("temporary workspace should be created");
        std::fs::write(directory.path().join("hello.txt"), "hello")
            .expect("fixture should be written");
        let tool = ReadTool::new(directory.path()).expect("workspace should be valid");
        let output = tool
            .call(ToolCallRequest {
                run_id: RunId::new(),
                call_id: "call-1".into(),
                name: "read".into(),
                arguments: json!({"path": "hello.txt"}),
                cancellation: RunCancellation::new(),
            })
            .await
            .expect("file should be read");

        let value: serde_json::Value =
            serde_json::from_str(&output.content).expect("output should be JSON");
        assert_eq!(value["path"], "hello.txt");
        assert_eq!(value["content"], "hello");
    }

    #[tokio::test]
    async fn rejects_parent_directory_traversal() {
        let directory = tempdir().expect("temporary workspace should be created");
        let tool = ReadTool::new(directory.path()).expect("workspace should be valid");
        let error = tool
            .call(ToolCallRequest {
                run_id: RunId::new(),
                call_id: "call-1".into(),
                name: "read".into(),
                arguments: json!({"path": "../outside.txt"}),
                cancellation: RunCancellation::new(),
            })
            .await
            .expect_err("path traversal should fail");

        assert_eq!(error.code(), "path_outside_workspace");
    }

    #[tokio::test]
    async fn rejects_host_protected_secret_paths() {
        let directory = tempdir().expect("temporary workspace should be created");
        std::fs::create_dir(directory.path().join("config"))
            .expect("config directory should be created");
        std::fs::write(
            directory.path().join("config/mina.toml"),
            "api_key='secret'",
        )
        .expect("fixture should be written");
        let tool = ReadTool::new(directory.path()).expect("workspace should be valid");
        let error = tool
            .call(ToolCallRequest {
                run_id: RunId::new(),
                call_id: "call-1".into(),
                name: "read".into(),
                arguments: json!({"path": "config/mina.toml"}),
                cancellation: RunCancellation::new(),
            })
            .await
            .expect_err("protected path should fail");

        assert_eq!(error.code(), "path_protected");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rejects_symlinks_that_escape_the_workspace() {
        use std::os::unix::fs::symlink;

        let directory = tempdir().expect("temporary workspace should be created");
        let outside = tempdir().expect("outside directory should be created");
        std::fs::write(outside.path().join("secret.txt"), "outside")
            .expect("outside fixture should be written");
        symlink(
            outside.path().join("secret.txt"),
            directory.path().join("link.txt"),
        )
        .expect("fixture symlink should be created");
        let tool = ReadTool::new(directory.path()).expect("workspace should be valid");
        let error = tool
            .call(ToolCallRequest {
                run_id: RunId::new(),
                call_id: "call-1".into(),
                name: "read".into(),
                arguments: json!({"path": "link.txt"}),
                cancellation: RunCancellation::new(),
            })
            .await
            .expect_err("escaping symlink should fail");

        assert_eq!(error.code(), "path_outside_workspace");
    }

    #[tokio::test]
    async fn writes_and_edits_workspace_files_with_medium_risk() {
        let directory = tempdir().expect("temporary workspace should be created");
        let write = WriteTool::new(directory.path()).expect("workspace should be valid");
        let edit = EditTool::new(directory.path()).expect("workspace should be valid");

        assert_eq!(write.definition().risk_level, ToolRiskLevel::Medium);
        assert_eq!(edit.definition().risk_level, ToolRiskLevel::Medium);

        write
            .call(ToolCallRequest {
                run_id: RunId::new(),
                call_id: "write-1".into(),
                name: "write".into(),
                arguments: json!({"path": "notes.txt", "content": "alpha beta"}),
                cancellation: RunCancellation::new(),
            })
            .await
            .expect("file should be written");
        let output = edit
            .call(ToolCallRequest {
                run_id: RunId::new(),
                call_id: "edit-1".into(),
                name: "edit".into(),
                arguments: json!({
                    "path": "notes.txt",
                    "old_text": "beta",
                    "new_text": "gamma"
                }),
                cancellation: RunCancellation::new(),
            })
            .await
            .expect("file should be edited");

        let value: serde_json::Value =
            serde_json::from_str(&output.content).expect("output should be JSON");
        assert_eq!(value["replacements"], 1);
        assert_eq!(
            std::fs::read_to_string(directory.path().join("notes.txt"))
                .expect("written file should be readable"),
            "alpha gamma"
        );
    }

    #[tokio::test]
    async fn edit_rejects_ambiguous_matches() {
        let directory = tempdir().expect("temporary workspace should be created");
        std::fs::write(directory.path().join("notes.txt"), "same same")
            .expect("fixture should be written");
        let tool = EditTool::new(directory.path()).expect("workspace should be valid");
        let error = tool
            .call(ToolCallRequest {
                run_id: RunId::new(),
                call_id: "edit-1".into(),
                name: "edit".into(),
                arguments: json!({
                    "path": "notes.txt",
                    "old_text": "same",
                    "new_text": "changed"
                }),
                cancellation: RunCancellation::new(),
            })
            .await
            .expect_err("ambiguous edit should fail");

        assert_eq!(error.code(), "edit_match_ambiguous");
    }
}
