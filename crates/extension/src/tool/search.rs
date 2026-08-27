use std::{future::Future, path::Path, pin::Pin, sync::Arc};

use agent_core::harness::{
    RunCancellation, Tool, ToolCallFuture, ToolCallRequest, ToolConcurrency, ToolDefinition,
    ToolError, ToolExecutionPolicy, ToolOutput, ToolRetryPolicy, ToolRiskLevel,
};
use serde::{Deserialize, Serialize};

use crate::tool::workspace::Workspace;

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

#[derive(Debug, Clone)]
pub struct WorkspaceSearchBackend {
    workspace: Workspace,
}

impl WorkspaceSearchBackend {
    pub fn new(workspace_root: impl AsRef<Path>) -> Result<Self, std::io::Error> {
        Ok(Self {
            workspace: Workspace::new(workspace_root)?,
        })
    }

    pub(crate) const fn from_workspace(workspace: Workspace) -> Self {
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
        let workspace = self.workspace.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || search_workspace(workspace, request))
                .await
                .map_err(|_| {
                    ToolError::new(
                        "search_backend_failed",
                        "the search backend could not complete the request",
                        true,
                    )
                })?
        })
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

fn search_workspace(
    workspace: Workspace,
    request: SearchRequest,
) -> Result<Vec<SearchHit>, ToolError> {
    let lowered_query = request.query.to_lowercase();
    let mut stack = vec![workspace.root().to_path_buf()];
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
        let entries = std::fs::read_dir(&directory).map_err(|_| search_failed())?;
        for entry in entries {
            let entry = entry.map_err(|_| search_failed())?;
            let path = entry.path();
            let metadata = std::fs::symlink_metadata(&path).map_err(|_| search_failed())?;
            if metadata.file_type().is_symlink() || workspace.is_protected(&path) {
                continue;
            }
            if metadata.is_dir() {
                if !ignored_directory(&path) {
                    stack.push(path);
                }
                continue;
            }
            if !metadata.is_file() || metadata.len() > MAX_SEARCH_FILE_BYTES {
                continue;
            }
            scanned_files += 1;
            if scanned_files > MAX_SEARCH_FILES {
                break;
            }
            let Ok(bytes) = std::fs::read(&path) else {
                continue;
            };
            let Ok(content) = std::str::from_utf8(&bytes) else {
                continue;
            };
            for (index, line) in content.lines().enumerate() {
                if !line.to_lowercase().contains(&lowered_query) {
                    continue;
                }
                let relative = workspace.display_relative(&path);
                results.push(SearchHit {
                    title: format!("{relative}:{}", index + 1),
                    uri: relative.clone(),
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
    }
    Ok(results)
}

fn ignored_directory(path: &Path) -> bool {
    matches!(
        path.file_name().and_then(|name| name.to_str()),
        Some("target" | "node_modules" | ".next")
    )
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

fn search_failed() -> ToolError {
    ToolError::new(
        "search_backend_failed",
        "the search backend could not read the configured source",
        true,
    )
}

#[cfg(test)]
mod tests {
    use agent_core::harness::{RunCancellation, RunId};
    use serde_json::json;
    use tempfile::tempdir;

    use super::*;

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
        let tool = SearchTool::new(Arc::new(
            WorkspaceSearchBackend::new(directory.path()).expect("workspace should be valid"),
        ));
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
