use std::sync::Arc;

use agent_core::harness::{
    Tool, ToolCallFuture, ToolCallRequest, ToolConcurrency, ToolDefinition, ToolError,
    ToolErrorCategory, ToolExecutionPolicy, ToolOutput, ToolRetryPolicy, ToolRiskLevel,
};
use serde::Deserialize;

use crate::workspace::{
    FileKind, ListRequest, ReadRequest, WorkspaceError, WorkspaceErrorKind, WorkspaceFs,
    WorkspacePath, WriteMode, WriteRequest,
};

const MAX_TEXT_FILE_BYTES: u64 = 1024 * 1024;
const MAX_DIRECTORY_ENTRIES: usize = 500;

pub struct ReadTool {
    workspace: Arc<dyn WorkspaceFs>,
}

impl std::fmt::Debug for ReadTool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReadTool")
            .field("workspace", &self.workspace.descriptor())
            .finish()
    }
}

impl ReadTool {
    #[must_use]
    pub fn new(workspace: Arc<dyn WorkspaceFs>) -> Self {
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
        .with_execution_policy(read_only_policy())
    }

    fn call(&self, request: ToolCallRequest) -> ToolCallFuture {
        let workspace = Arc::clone(&self.workspace);
        Box::pin(async move {
            if request.cancellation.is_cancelled() {
                return Err(cancelled());
            }
            let arguments: ReadTextFileArguments = parse_arguments(request.arguments)?;
            let path = parse_tool_path(&arguments.path)?;
            let content = workspace
                .read(ReadRequest {
                    path: path.clone(),
                    offset: 0,
                    length: None,
                    max_bytes: MAX_TEXT_FILE_BYTES,
                })
                .await
                .map_err(map_workspace_error)?;
            let content = String::from_utf8(content.bytes).map_err(|_| {
                ToolError::new(
                    "file_not_utf8",
                    "the requested file is not valid UTF-8 text",
                    false,
                )
            })?;
            Ok(ToolOutput::text(
                serde_json::json!({
                    "path": path.as_str(),
                    "content": content
                })
                .to_string(),
            ))
        })
    }
}

pub struct WriteTool {
    workspace: Arc<dyn WorkspaceFs>,
}

impl std::fmt::Debug for WriteTool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WriteTool")
            .field("workspace", &self.workspace.descriptor())
            .finish()
    }
}

impl WriteTool {
    #[must_use]
    pub fn new(workspace: Arc<dyn WorkspaceFs>) -> Self {
        Self { workspace }
    }
}

impl Tool for WriteTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition::new(
            "write",
            "Create or replace one UTF-8 text file inside the workspace. The parent directory must already exist.",
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
        let workspace = Arc::clone(&self.workspace);
        Box::pin(async move {
            if request.cancellation.is_cancelled() {
                return Err(cancelled());
            }
            let arguments: WriteArguments = parse_arguments(request.arguments)?;
            if arguments.content.len() as u64 > MAX_TEXT_FILE_BYTES {
                return Err(file_too_large());
            }
            let path = parse_tool_path(&arguments.path)?;
            let created = match workspace.stat(path.clone()).await {
                Ok(metadata) if metadata.kind != FileKind::File => return Err(path_not_file()),
                Ok(_) => false,
                Err(error) if error.kind() == WorkspaceErrorKind::NotFound => true,
                Err(error) => return Err(map_workspace_error(error)),
            };
            let metadata = workspace
                .write(WriteRequest {
                    path: path.clone(),
                    bytes: arguments.content.as_bytes().to_vec(),
                    mode: WriteMode::Truncate,
                    create_parents: false,
                })
                .await
                .map_err(map_workspace_error)?;
            Ok(ToolOutput::text(
                serde_json::json!({
                    "path": path.as_str(),
                    "bytes_written": arguments.content.len(),
                    "created": created,
                    "revision": metadata.revision
                })
                .to_string(),
            ))
        })
    }
}

pub struct EditTool {
    workspace: Arc<dyn WorkspaceFs>,
}

impl std::fmt::Debug for EditTool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EditTool")
            .field("workspace", &self.workspace.descriptor())
            .finish()
    }
}

impl EditTool {
    #[must_use]
    pub fn new(workspace: Arc<dyn WorkspaceFs>) -> Self {
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
        let workspace = Arc::clone(&self.workspace);
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
            let path = parse_tool_path(&arguments.path)?;
            let content = workspace
                .read(ReadRequest {
                    path: path.clone(),
                    offset: 0,
                    length: None,
                    max_bytes: MAX_TEXT_FILE_BYTES,
                })
                .await
                .map_err(map_workspace_error)?;
            let content = String::from_utf8(content.bytes).map_err(|_| {
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
            let metadata = workspace
                .write(WriteRequest {
                    path: path.clone(),
                    bytes: edited.as_bytes().to_vec(),
                    mode: WriteMode::Truncate,
                    create_parents: false,
                })
                .await
                .map_err(map_workspace_error)?;
            Ok(ToolOutput::text(
                serde_json::json!({
                    "path": path.as_str(),
                    "replacements": if arguments.replace_all { occurrences } else { 1 },
                    "bytes_written": edited.len(),
                    "revision": metadata.revision
                })
                .to_string(),
            ))
        })
    }
}

pub struct ListDirectoryTool {
    workspace: Arc<dyn WorkspaceFs>,
}

impl std::fmt::Debug for ListDirectoryTool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ListDirectoryTool")
            .field("workspace", &self.workspace.descriptor())
            .finish()
    }
}

impl ListDirectoryTool {
    #[must_use]
    pub fn new(workspace: Arc<dyn WorkspaceFs>) -> Self {
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
        .with_execution_policy(read_only_policy())
    }

    fn call(&self, request: ToolCallRequest) -> ToolCallFuture {
        let workspace = Arc::clone(&self.workspace);
        Box::pin(async move {
            let arguments: ListDirectoryArguments = parse_arguments(request.arguments)?;
            let path = parse_tool_path(&arguments.path)?;
            let mut page = workspace
                .list(ListRequest {
                    path: path.clone(),
                    cursor: None,
                    limit: MAX_DIRECTORY_ENTRIES + 1,
                })
                .await
                .map_err(map_workspace_error)?;
            if request.cancellation.is_cancelled() {
                return Err(cancelled());
            }
            let truncated =
                page.entries.len() > MAX_DIRECTORY_ENTRIES || page.next_cursor.is_some();
            page.entries.truncate(MAX_DIRECTORY_ENTRIES);
            let entries = page
                .entries
                .into_iter()
                .filter(|entry| !is_protected_path(&entry.path))
                .map(|entry| {
                    serde_json::json!({
                        "path": entry.path.as_str(),
                        "kind": match entry.kind {
                            FileKind::File => "file",
                            FileKind::Directory => "directory",
                            FileKind::Symlink => "symlink",
                            FileKind::Other => "other",
                        }
                    })
                })
                .collect::<Vec<_>>();
            Ok(ToolOutput::text(
                serde_json::json!({
                    "path": path.as_str(),
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

pub(crate) fn parse_tool_path(path: &str) -> Result<WorkspacePath, ToolError> {
    let path = WorkspacePath::parse(path).map_err(|_| {
        ToolError::new(
            "path_outside_workspace",
            "tool paths must remain inside the configured workspace",
            false,
        )
    })?;
    if is_protected_path(&path) {
        return Err(ToolError::new(
            "path_protected",
            "the requested path is protected by the host file policy",
            false,
        ));
    }
    Ok(path)
}

pub(crate) fn is_protected_path(path: &WorkspacePath) -> bool {
    let file_name = path.file_name().unwrap_or_default();
    path.storage_key().split('/').any(|part| part == ".git")
        || path.storage_key() == "config/mina.toml"
        || file_name == ".env"
        || file_name.starts_with(".env.")
        || file_name.ends_with(".pem")
        || file_name.ends_with(".key")
}

pub(crate) fn map_workspace_error(error: WorkspaceError) -> ToolError {
    let category = match error.kind() {
        WorkspaceErrorKind::InvalidPath => ToolErrorCategory::InvalidRequest,
        WorkspaceErrorKind::NotFound => ToolErrorCategory::NotFound,
        WorkspaceErrorKind::AlreadyExists | WorkspaceErrorKind::Conflict => {
            ToolErrorCategory::Conflict
        }
        WorkspaceErrorKind::PermissionDenied => ToolErrorCategory::PermissionDenied,
        WorkspaceErrorKind::TooLarge => ToolErrorCategory::ResourceExhausted,
        WorkspaceErrorKind::Unavailable => ToolErrorCategory::Unavailable,
        WorkspaceErrorKind::NotFile
        | WorkspaceErrorKind::NotDirectory
        | WorkspaceErrorKind::Unsupported
        | WorkspaceErrorKind::Backend => ToolErrorCategory::Internal,
    };
    ToolError::new(error.code(), error.safe_message(), error.retryable()).with_category(category)
}

const fn read_only_policy() -> ToolExecutionPolicy {
    ToolExecutionPolicy::read_only()
        .with_concurrency(ToolConcurrency::ParallelSafe)
        .with_retry(ToolRetryPolicy::bounded(2, 25, 250))
}

fn path_not_file() -> ToolError {
    ToolError::new(
        "path_not_file",
        "the requested workspace path is not a file",
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

fn cancelled() -> ToolError {
    ToolError::new("tool_cancelled", "tool execution was cancelled", false)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use agent_core::harness::{RunCancellation, RunId, ToolCallRequest};
    use serde_json::json;
    use tempfile::tempdir;

    use super::*;
    use crate::workspace::NativeWorkspaceFs;

    fn native_workspace(path: &Path) -> Arc<dyn WorkspaceFs> {
        Arc::new(NativeWorkspaceFs::new(path).expect("workspace should be valid"))
    }

    #[tokio::test]
    async fn reads_utf8_files_inside_the_workspace() {
        let directory = tempdir().expect("temporary workspace should be created");
        std::fs::write(directory.path().join("hello.txt"), "hello")
            .expect("fixture should be written");
        let tool = ReadTool::new(native_workspace(directory.path()));
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
        let tool = ReadTool::new(native_workspace(directory.path()));
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
        let tool = ReadTool::new(native_workspace(directory.path()));
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
        let tool = ReadTool::new(native_workspace(directory.path()));
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

        assert_eq!(error.code(), "workspace_path_outside_root");
    }

    #[tokio::test]
    async fn writes_and_edits_workspace_files_with_medium_risk() {
        let directory = tempdir().expect("temporary workspace should be created");
        let write = WriteTool::new(native_workspace(directory.path()));
        let edit = EditTool::new(native_workspace(directory.path()));

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
        let tool = EditTool::new(native_workspace(directory.path()));
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
