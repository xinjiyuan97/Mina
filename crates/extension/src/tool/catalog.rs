use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::sandbox::ProcessSandbox;
use agent_core::harness::{ToolRegistrationError, ToolRegistry};
use thiserror::Error;

use crate::tool::{
    command::RunCommandTool,
    filesystem::{EditTool, ListDirectoryTool, ReadTool, WriteTool},
    search::{SearchBackend, SearchTool, WorkspaceSearchBackend},
    time_tool::GetCurrentTimeTool,
    workspace::Workspace,
};

/// Host-side selection of built-in tools. High-risk tools require an explicit
/// builder opt-in in addition to the harness approval policy.
#[derive(Clone)]
pub struct BuiltinToolCatalog {
    workspace_root: PathBuf,
    command_enabled: bool,
    process_sandbox: Option<Arc<dyn ProcessSandbox>>,
    search_enabled: bool,
    search_backend: Option<Arc<dyn SearchBackend>>,
}

impl std::fmt::Debug for BuiltinToolCatalog {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BuiltinToolCatalog")
            .field("workspace_root", &self.workspace_root)
            .field("command_enabled", &self.command_enabled)
            .field(
                "process_sandbox",
                &self
                    .process_sandbox
                    .as_ref()
                    .map(|value| value.descriptor()),
            )
            .field("search_enabled", &self.search_enabled)
            .field(
                "search_backend",
                &self.search_backend.as_ref().map(|value| value.descriptor()),
            )
            .finish()
    }
}

impl BuiltinToolCatalog {
    #[must_use]
    pub fn new(workspace_root: impl Into<PathBuf>) -> Self {
        Self {
            workspace_root: workspace_root.into(),
            command_enabled: false,
            process_sandbox: None,
            search_enabled: true,
            search_backend: None,
        }
    }

    #[must_use]
    pub const fn enable_command_tool(mut self) -> Self {
        self.command_enabled = true;
        self
    }

    #[must_use]
    pub fn with_process_sandbox(mut self, sandbox: Arc<dyn ProcessSandbox>) -> Self {
        self.process_sandbox = Some(sandbox);
        self
    }

    #[must_use]
    pub fn with_search_backend(mut self, backend: Arc<dyn SearchBackend>) -> Self {
        self.search_enabled = true;
        self.search_backend = Some(backend);
        self
    }

    #[must_use]
    pub const fn disable_search(mut self) -> Self {
        self.search_enabled = false;
        self
    }

    pub fn build(self) -> Result<ToolRegistry, BuiltinToolCatalogError> {
        let workspace = Workspace::new(&self.workspace_root)?;
        let mut registry = ToolRegistry::new();
        registry.register(GetCurrentTimeTool)?;
        registry.register(ReadTool::from_workspace(workspace.clone()))?;
        registry.register(ListDirectoryTool::from_workspace(workspace.clone()))?;
        registry.register(WriteTool::from_workspace(workspace.clone()))?;
        registry.register(EditTool::from_workspace(workspace.clone()))?;
        if self.search_enabled {
            let backend = self.search_backend.unwrap_or_else(|| {
                Arc::new(WorkspaceSearchBackend::from_workspace(workspace.clone()))
            });
            registry.register(SearchTool::new(backend))?;
        }
        if self.command_enabled {
            let command = if let Some(sandbox) = self.process_sandbox {
                RunCommandTool::from_workspace_with_sandbox(workspace, sandbox)
            } else {
                RunCommandTool::from_workspace(workspace)
            };
            registry.register(command)?;
        }
        Ok(registry)
    }

    #[must_use]
    pub fn workspace_root(&self) -> &Path {
        &self.workspace_root
    }
}

#[derive(Debug, Error)]
pub enum BuiltinToolCatalogError {
    #[error("workspace root is unavailable")]
    Workspace(#[from] std::io::Error),
    #[error(transparent)]
    Registration(#[from] ToolRegistrationError),
}

#[cfg(test)]
mod tests {
    use agent_core::harness::{ToolPort, ToolRiskLevel};
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn safe_catalog_excludes_command_execution() {
        let directory = tempdir().expect("temporary workspace should be created");
        let registry = BuiltinToolCatalog::new(directory.path())
            .build()
            .expect("catalog should build");
        let definitions = registry.definitions();

        assert_eq!(definitions.len(), 6);
        assert_eq!(
            definitions
                .iter()
                .find(|tool| tool.name == "read")
                .map(|tool| tool.risk_level),
            Some(ToolRiskLevel::Low)
        );
        assert_eq!(
            definitions
                .iter()
                .find(|tool| tool.name == "write")
                .map(|tool| tool.risk_level),
            Some(ToolRiskLevel::Medium)
        );
        assert_eq!(
            definitions
                .iter()
                .find(|tool| tool.name == "edit")
                .map(|tool| tool.risk_level),
            Some(ToolRiskLevel::Medium)
        );
        assert!(definitions.iter().any(|tool| tool.name == "search"));
        assert!(!definitions.iter().any(|tool| tool.name == "run_command"));
    }

    #[test]
    fn command_execution_requires_explicit_host_opt_in() {
        let directory = tempdir().expect("temporary workspace should be created");
        let registry = BuiltinToolCatalog::new(directory.path())
            .enable_command_tool()
            .build()
            .expect("catalog should build");
        let command = registry
            .definitions()
            .into_iter()
            .find(|tool| tool.name == "run_command")
            .expect("command tool should be registered");

        assert_eq!(command.risk_level, ToolRiskLevel::High);
        assert!(command.risk_level.requires_approval());
    }
}
