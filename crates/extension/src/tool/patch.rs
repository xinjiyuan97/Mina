use std::sync::Arc;

use agent_core::harness::{
    Tool, ToolCallFuture, ToolCallRequest, ToolDefinition, ToolError, ToolOutput, ToolRiskLevel,
};
use serde::Deserialize;

use crate::{
    tool::filesystem::{map_workspace_error, parse_tool_path},
    workspace::{
        FileKind, ReadRequest, RemoveRequest, WorkspaceErrorKind, WorkspaceFs, WorkspacePath,
        WriteMode, WriteRequest,
    },
};

const MAX_PATCH_BYTES: usize = 1024 * 1024;
const MAX_PATCH_FILES: usize = 128;

pub struct ApplyPatchTool {
    workspace: Arc<dyn WorkspaceFs>,
}

impl std::fmt::Debug for ApplyPatchTool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ApplyPatchTool")
            .field("workspace", &self.workspace.descriptor())
            .finish()
    }
}

impl ApplyPatchTool {
    #[must_use]
    pub fn new(workspace: Arc<dyn WorkspaceFs>) -> Self {
        Self { workspace }
    }
}

impl Tool for ApplyPatchTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition::new(
            "apply_patch",
            "Apply a Codex-style patch to UTF-8 files inside the configured workspace. Paths are workspace-relative; protected paths, traversal, and symbolic-link destinations are rejected.",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "patch": {
                        "type": "string",
                        "minLength": 1,
                        "maxLength": MAX_PATCH_BYTES,
                        "description": "Patch body delimited by *** Begin Patch and *** End Patch"
                    }
                },
                "required": ["patch"],
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
            let arguments: ApplyPatchArguments =
                serde_json::from_value(request.arguments).map_err(|_| invalid_arguments())?;
            if arguments.patch.is_empty() || arguments.patch.len() > MAX_PATCH_BYTES {
                return Err(invalid_arguments());
            }
            let operations = parse_patch(&arguments.patch)?;
            if operations.is_empty() || operations.len() > MAX_PATCH_FILES {
                return Err(invalid_patch(
                    "patch must contain between 1 and 128 file operations",
                ));
            }

            let prepared = prepare_operations(workspace.as_ref(), operations).await?;
            if request.cancellation.is_cancelled() {
                return Err(cancelled());
            }
            let mut changed = Vec::with_capacity(prepared.len());
            for operation in prepared {
                if request.cancellation.is_cancelled() {
                    return Err(cancelled());
                }
                match operation.change {
                    PreparedChange::Write(content) => {
                        workspace
                            .write(WriteRequest {
                                path: operation.path.clone(),
                                bytes: content,
                                mode: WriteMode::Truncate,
                                create_parents: false,
                            })
                            .await
                            .map_err(map_workspace_error)?;
                    }
                    PreparedChange::Delete => {
                        workspace
                            .remove(RemoveRequest {
                                path: operation.path.clone(),
                                recursive: false,
                            })
                            .await
                            .map_err(map_workspace_error)?;
                    }
                }
                changed.push(operation.path.as_str().to_owned());
            }
            Ok(ToolOutput::text(
                serde_json::json!({
                    "files_changed": changed.len(),
                    "paths": changed
                })
                .to_string(),
            ))
        })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ApplyPatchArguments {
    patch: String,
}

enum PatchOperation {
    Add { path: String, content: String },
    Delete { path: String },
    Update { path: String, hunks: Vec<PatchHunk> },
}

struct PatchHunk {
    old: String,
    new: String,
}

struct PreparedOperation {
    path: WorkspacePath,
    change: PreparedChange,
}

enum PreparedChange {
    Write(Vec<u8>),
    Delete,
}

fn parse_patch(patch: &str) -> Result<Vec<PatchOperation>, ToolError> {
    let lines = patch.lines().collect::<Vec<_>>();
    if lines.first() != Some(&"*** Begin Patch") || lines.last() != Some(&"*** End Patch") {
        return Err(invalid_patch(
            "patch must start with *** Begin Patch and end with *** End Patch",
        ));
    }
    let mut operations = Vec::new();
    let mut index = 1;
    while index + 1 < lines.len() {
        let header = lines[index];
        if let Some(path) = header.strip_prefix("*** Add File: ") {
            validate_patch_path(path)?;
            index += 1;
            let mut content = String::new();
            while index + 1 < lines.len() && !lines[index].starts_with("*** ") {
                let line = lines[index].strip_prefix('+').ok_or_else(|| {
                    invalid_patch("every added-file content line must start with +")
                })?;
                content.push_str(line);
                content.push('\n');
                index += 1;
            }
            operations.push(PatchOperation::Add {
                path: path.to_owned(),
                content,
            });
        } else if let Some(path) = header.strip_prefix("*** Delete File: ") {
            validate_patch_path(path)?;
            operations.push(PatchOperation::Delete {
                path: path.to_owned(),
            });
            index += 1;
        } else if let Some(path) = header.strip_prefix("*** Update File: ") {
            validate_patch_path(path)?;
            index += 1;
            let mut hunks = Vec::new();
            while index + 1 < lines.len() && !lines[index].starts_with("*** ") {
                if !lines[index].starts_with("@@") {
                    return Err(invalid_patch("update content must begin with an @@ hunk"));
                }
                index += 1;
                let mut old = String::new();
                let mut new = String::new();
                while index + 1 < lines.len()
                    && !lines[index].starts_with("@@")
                    && !lines[index].starts_with("*** ")
                {
                    let line = lines[index];
                    let Some(prefix) = line.chars().next() else {
                        return Err(invalid_patch("hunk lines must start with space, +, or -"));
                    };
                    let content = &line[prefix.len_utf8()..];
                    match prefix {
                        ' ' => {
                            old.push_str(content);
                            old.push('\n');
                            new.push_str(content);
                            new.push('\n');
                        }
                        '-' => {
                            old.push_str(content);
                            old.push('\n');
                        }
                        '+' => {
                            new.push_str(content);
                            new.push('\n');
                        }
                        _ => {
                            return Err(invalid_patch("hunk lines must start with space, +, or -"));
                        }
                    }
                    index += 1;
                }
                if old.is_empty() {
                    return Err(invalid_patch("update hunks must match existing text"));
                }
                hunks.push(PatchHunk { old, new });
            }
            if hunks.is_empty() {
                return Err(invalid_patch("update operation contains no hunks"));
            }
            operations.push(PatchOperation::Update {
                path: path.to_owned(),
                hunks,
            });
        } else {
            return Err(invalid_patch("unrecognized patch operation"));
        }
    }
    Ok(operations)
}

async fn prepare_operations(
    workspace: &dyn WorkspaceFs,
    operations: Vec<PatchOperation>,
) -> Result<Vec<PreparedOperation>, ToolError> {
    let mut prepared = Vec::with_capacity(operations.len());
    for operation in operations {
        match operation {
            PatchOperation::Add { path, content } => {
                if content.len() > MAX_PATCH_BYTES {
                    return Err(invalid_patch("patched file exceeds the 1 MiB limit"));
                }
                let path = parse_tool_path(&path)?;
                match workspace.stat(path.clone()).await {
                    Ok(metadata) if metadata.kind != FileKind::File => {
                        return Err(path_not_file());
                    }
                    Ok(_) => {}
                    Err(error) if error.kind() == WorkspaceErrorKind::NotFound => {}
                    Err(error) => return Err(map_workspace_error(error)),
                }
                prepared.push(PreparedOperation {
                    path,
                    change: PreparedChange::Write(content.into_bytes()),
                });
            }
            PatchOperation::Delete { path } => {
                let path = parse_tool_path(&path)?;
                ensure_regular_file(workspace, path.clone()).await?;
                prepared.push(PreparedOperation {
                    path,
                    change: PreparedChange::Delete,
                });
            }
            PatchOperation::Update { path, hunks } => {
                let path = parse_tool_path(&path)?;
                ensure_regular_file(workspace, path.clone()).await?;
                let bytes = workspace
                    .read(ReadRequest {
                        path: path.clone(),
                        offset: 0,
                        length: None,
                        max_bytes: MAX_PATCH_BYTES as u64,
                    })
                    .await
                    .map_err(map_workspace_error)?
                    .bytes;
                if bytes.len() > MAX_PATCH_BYTES {
                    return Err(invalid_patch("patched file exceeds the 1 MiB limit"));
                }
                let mut content = String::from_utf8(bytes).map_err(|_| {
                    ToolError::new(
                        "file_not_utf8",
                        "patched files must be valid UTF-8 text",
                        false,
                    )
                })?;
                for hunk in hunks {
                    let matches = content.matches(&hunk.old).count();
                    if matches == 0 {
                        return Err(ToolError::new(
                            "patch_context_not_found",
                            "an update hunk did not match the target file",
                            false,
                        ));
                    }
                    if matches > 1 {
                        return Err(ToolError::new(
                            "patch_context_ambiguous",
                            "an update hunk matches the target file more than once",
                            false,
                        ));
                    }
                    content = content.replacen(&hunk.old, &hunk.new, 1);
                }
                if content.len() > MAX_PATCH_BYTES {
                    return Err(invalid_patch("patched file exceeds the 1 MiB limit"));
                }
                prepared.push(PreparedOperation {
                    path,
                    change: PreparedChange::Write(content.into_bytes()),
                });
            }
        }
    }
    Ok(prepared)
}

fn validate_patch_path(path: &str) -> Result<(), ToolError> {
    if path.trim().is_empty() {
        return Err(invalid_patch("patch file paths must not be empty"));
    }
    Ok(())
}

async fn ensure_regular_file(
    workspace: &dyn WorkspaceFs,
    path: WorkspacePath,
) -> Result<(), ToolError> {
    let metadata = workspace.stat(path).await.map_err(map_workspace_error)?;
    if metadata.kind == FileKind::Symlink {
        return Err(path_is_symlink());
    }
    if metadata.kind != FileKind::File {
        return Err(path_not_file());
    }
    Ok(())
}

fn invalid_arguments() -> ToolError {
    ToolError::new(
        "invalid_tool_arguments",
        "patch arguments do not match the declared schema",
        false,
    )
}

fn invalid_patch(message: &'static str) -> ToolError {
    ToolError::new("invalid_patch", message, false)
}

fn path_not_file() -> ToolError {
    ToolError::new(
        "path_not_file",
        "a patch target is not a regular file",
        false,
    )
}

fn path_is_symlink() -> ToolError {
    ToolError::new(
        "path_is_symlink",
        "patch tools do not write through symbolic links",
        false,
    )
}

fn cancelled() -> ToolError {
    ToolError::new("tool_cancelled", "tool execution was cancelled", false)
}
