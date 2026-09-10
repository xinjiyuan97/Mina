use std::{future::Future, pin::Pin, sync::Arc};

use agent_core::harness::{
    RunCancellation, Tool, ToolCallFuture, ToolCallRequest, ToolConcurrency, ToolDefinition,
    ToolError, ToolExecutionPolicy, ToolOutput, ToolRetryPolicy, ToolRiskLevel,
};
use serde::{Deserialize, Serialize};

use crate::{
    tool::filesystem::{is_protected_path, map_workspace_error},
    workspace::{FileKind, ListRequest, ReadRequest, WorkspaceFs, WorkspacePath},
};

const MAX_RESULTS: usize = 100;
const MAX_SEARCH_FILES: usize = 10_000;
const MAX_SEARCH_FILE_BYTES: u64 = 1024 * 1024;
const MAX_SNIPPET_CHARS: usize = 500;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchBackendDescriptor {
    pub identity: String,
    pub kind: String,
    pub version: String,
    pub external_network: bool,
}

#[derive(Debug, Clone)]
pub struct SearchRequest {
    pub query: String,
    pub limit: usize,
    pub cancellation: RunCancellation,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchHit {
    pub title: String,
    pub uri: String,
    pub snippet: String,
    pub score: f32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

pub type SearchFuture =
    Pin<Box<dyn Future<Output = Result<Vec<SearchHit>, ToolError>> + Send + 'static>>;

/// Backend-neutral search boundary. A workspace scanner, web provider, remote
/// service or MCP adapter can implement the same port.
pub trait SearchBackend: Send + Sync + 'static {
    fn descriptor(&self) -> SearchBackendDescriptor;
    fn risk_level(&self) -> ToolRiskLevel;
    fn search(&self, request: SearchRequest) -> SearchFuture;
}

pub struct SearchTool {
    backend: Arc<dyn SearchBackend>,
}

impl std::fmt::Debug for SearchTool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SearchTool")
            .field("backend", &self.backend.descriptor())
            .finish()
    }
}

impl SearchTool {
    #[must_use]
    pub fn new(backend: Arc<dyn SearchBackend>) -> Self {
        Self { backend }
    }
}

impl Tool for SearchTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition::new(
            "search",
            "Search through the configured backend and return structured URI, title, snippet, score and metadata results.",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "minLength": 1,
                        "description": "Search query"
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": MAX_RESULTS,
                        "default": 20
                    }
                },
                "required": ["query"],
                "additionalProperties": false
            }),
        )
        .with_risk_level(self.backend.risk_level())
        .with_execution_policy(
            ToolExecutionPolicy::read_only()
                .with_concurrency(ToolConcurrency::ParallelSafe)
                .with_retry(ToolRetryPolicy::bounded(2, 100, 1_000)),
        )
    }

    fn call(&self, request: ToolCallRequest) -> ToolCallFuture {
        let backend = Arc::clone(&self.backend);
        Box::pin(async move {
            let arguments: SearchArguments =
                serde_json::from_value(request.arguments).map_err(|_| invalid_arguments())?;
            if arguments.query.trim().is_empty() {
                return Err(invalid_arguments());
            }
            let results = backend
                .search(SearchRequest {
                    query: arguments.query,
                    limit: arguments.limit.clamp(1, MAX_RESULTS),
                    cancellation: request.cancellation,
                })
                .await?;
            Ok(ToolOutput::text(
                serde_json::json!({
                    "backend": backend.descriptor(),
                    "results": results
                })
                .to_string(),
            ))
        })
    }
}

#[derive(Clone)]
pub struct WorkspaceSearchBackend {
    workspace: Arc<dyn WorkspaceFs>,
}

impl std::fmt::Debug for WorkspaceSearchBackend {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WorkspaceSearchBackend")
            .field("workspace", &self.workspace.descriptor())
            .finish()
    }
}

impl WorkspaceSearchBackend {
    #[must_use]
    pub fn new(workspace: Arc<dyn WorkspaceFs>) -> Self {
        Self { workspace }
    }
}

impl SearchBackend for WorkspaceSearchBackend {
    fn descriptor(&self) -> SearchBackendDescriptor {
        SearchBackendDescriptor {
            identity: "search:workspace-text".into(),
            kind: "workspace_text".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            external_network: false,
        }
    }

    fn risk_level(&self) -> ToolRiskLevel {
        ToolRiskLevel::Low
    }

    fn search(&self, request: SearchRequest) -> SearchFuture {
        let workspace = Arc::clone(&self.workspace);
        Box::pin(async move { search_workspace(workspace, request).await })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchArguments {
    query: String,
    #[serde(default = "default_limit")]
    limit: usize,
}

fn default_limit() -> usize {
    20
}

async fn search_workspace(
    workspace: Arc<dyn WorkspaceFs>,
    request: SearchRequest,
) -> Result<Vec<SearchHit>, ToolError> {
    let lowered_query = request.query.to_lowercase();
    let mut stack = vec![WorkspacePath::root()];
    let mut scanned_files = 0_usize;
    let mut results = Vec::new();

    while let Some(directory) = stack.pop() {
        if request.cancellation.is_cancelled() {
            return Err(ToolError::new(
                "tool_cancelled",
                "tool execution was cancelled",
                false,
            ));
        }
        let mut cursor = None;
        loop {
            let page = workspace
                .list(ListRequest {
                    path: directory.clone(),
                    cursor,
                    limit: 500,
                })
                .await
                .map_err(map_workspace_error)?;
            for entry in page.entries {
                if entry.kind == FileKind::Symlink || is_protected_path(&entry.path) {
                    continue;
                }
                if entry.kind == FileKind::Directory {
                    if !ignored_directory(&entry.path) {
                        stack.push(entry.path);
                    }
                    continue;
                }
                if entry.kind != FileKind::File || entry.size > MAX_SEARCH_FILE_BYTES {
                    continue;
                }
                scanned_files += 1;
                if scanned_files > MAX_SEARCH_FILES {
                    break;
                }
                let Ok(content) = workspace
                    .read(ReadRequest {
                        path: entry.path.clone(),
                        offset: 0,
                        length: None,
                        max_bytes: MAX_SEARCH_FILE_BYTES,
                    })
                    .await
                else {
                    continue;
                };
                let Ok(content) = std::str::from_utf8(&content.bytes) else {
                    continue;
                };
                for (index, line) in content.lines().enumerate() {
                    if !line.to_lowercase().contains(&lowered_query) {
                        continue;
                    }
                    let relative = entry.path.as_str().to_owned();
                    results.push(SearchHit {
                        title: format!("{relative}:{}", index + 1),
                        uri: relative,
                        snippet: truncate(line.trim(), MAX_SNIPPET_CHARS),
                        score: 1.0,
                        metadata: Some(serde_json::json!({"line": index + 1})),
                    });
                    if results.len() >= request.limit {
                        return Ok(results);
                    }
                }
            }
            if scanned_files > MAX_SEARCH_FILES {
                break;
            }
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        if scanned_files > MAX_SEARCH_FILES {
            break;
        }
    }
    Ok(results)
}

fn ignored_directory(path: &WorkspacePath) -> bool {
    matches!(path.file_name(), Some("target" | "node_modules" | ".next"))
}

fn truncate(value: &str, maximum_chars: usize) -> String {
    let mut chars = value.chars();
    let truncated: String = chars.by_ref().take(maximum_chars).collect();
    if chars.next().is_some() {
        format!("{truncated}…")
    } else {
        truncated
    }
}

fn invalid_arguments() -> ToolError {
    ToolError::new(
        "invalid_tool_arguments",
        "search arguments do not match the declared schema",
        false,
    )
}

#[cfg(test)]
mod tests {
    use agent_core::harness::{RunCancellation, RunId};
    use serde_json::json;
    use tempfile::tempdir;

    use super::*;
    use crate::workspace::NativeWorkspaceFs;

    struct ExternalFakeBackend;

    impl SearchBackend for ExternalFakeBackend {
        fn descriptor(&self) -> SearchBackendDescriptor {
            SearchBackendDescriptor {
                identity: "search:test-external".into(),
                kind: "test_external".into(),
                version: "1".into(),
                external_network: true,
            }
        }

        fn risk_level(&self) -> ToolRiskLevel {
            ToolRiskLevel::Medium
        }

        fn search(&self, request: SearchRequest) -> SearchFuture {
            Box::pin(async move {
                Ok(vec![SearchHit {
                    title: request.query,
                    uri: "https://example.invalid/result".into(),
                    snippet: "adapter result".into(),
                    score: 0.9,
                    metadata: None,
                }])
            })
        }
    }

    #[tokio::test]
    async fn workspace_backend_returns_structured_matches_and_skips_secrets() {
        let directory = tempdir().expect("temporary workspace should be created");
        std::fs::create_dir(directory.path().join("config"))
            .expect("config directory should be created");
        std::fs::write(directory.path().join("lib.rs"), "fn searchable_symbol() {}")
            .expect("fixture should be written");
        std::fs::write(
            directory.path().join("config/mina.toml"),
            "searchable_symbol = 'secret'",
        )
        .expect("secret fixture should be written");
        let workspace: Arc<dyn WorkspaceFs> =
            Arc::new(NativeWorkspaceFs::new(directory.path()).expect("workspace should be valid"));
        let tool = SearchTool::new(Arc::new(WorkspaceSearchBackend::new(workspace)));
        let output = tool
            .call(ToolCallRequest {
                run_id: RunId::new(),
                call_id: "search-1".into(),
                name: "search".into(),
                arguments: json!({"query": "searchable_symbol"}),
                cancellation: RunCancellation::new(),
            })
            .await
            .expect("search should succeed");
        let value: serde_json::Value =
            serde_json::from_str(&output.content).expect("output should be JSON");

        assert_eq!(value["results"].as_array().map(Vec::len), Some(1));
        assert_eq!(value["results"][0]["uri"], "lib.rs");
        assert_eq!(value["backend"]["kind"], "workspace_text");
    }

    #[tokio::test]
    async fn tool_accepts_a_replaceable_external_backend_and_uses_its_risk() {
        let tool = SearchTool::new(Arc::new(ExternalFakeBackend));
        assert_eq!(tool.definition().risk_level, ToolRiskLevel::Medium);

        let output = tool
            .call(ToolCallRequest {
                run_id: RunId::new(),
                call_id: "search-2".into(),
                name: "search".into(),
                arguments: json!({"query": "replaceable"}),
                cancellation: RunCancellation::new(),
            })
            .await
            .expect("external backend should be called");
        let value: serde_json::Value =
            serde_json::from_str(&output.content).expect("output should be JSON");

        assert_eq!(value["backend"]["external_network"], true);
        assert_eq!(value["results"][0]["title"], "replaceable");
    }
}
