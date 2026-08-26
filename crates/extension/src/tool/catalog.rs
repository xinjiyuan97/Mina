use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::sandbox::ProcessSandbox;
use agent_core::harness::{ToolRegistrationError, ToolRegistry};
use thiserror::Error;

use crate::tool::{
    filesystem::{EditTool, ListDirectoryTool, ReadTool, WriteTool},
    patch::ApplyPatchTool,
    search::{SearchBackend, SearchTool, WorkspaceSearchBackend},
    terminal::{ExecCommandTool, ShellCommandTool, WriteStdinTool},
    time_tool::GetCurrentTimeTool,
    workspace::Workspace,
};

/// Host-side selection of built-in tools. High-risk tools require an explicit
/// builder opt-in in addition to the harness approval policy.
#[derive(Clone)]
pub struct BuiltinToolCatalog {
    workspace_root: PathBuf,
    terminal_enabled: bool,
    process_sandbox: Option<Arc<dyn ProcessSandbox>>,
    search_enabled: bool,
    search_backend: Option<Arc<dyn SearchBackend>>,
}

impl std::fmt::Debug for BuiltinToolCatalog {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BuiltinToolCatalog")
            .field("workspace_root", &self.workspace_root)
            .field("terminal_enabled", &self.terminal_enabled)
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
            terminal_enabled: false,
            process_sandbox: None,
            search_enabled: true,
            search_backend: None,
        }
    }

    /// Enables the shell, managed-session, stdin and workspace patch tools.
    /// Terminal process tools remain high risk
    /// and therefore require the harness ApprovalPort.
    #[must_use]
    pub const fn enable_terminal_tools(mut self) -> Self {
        self.terminal_enabled = true;
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
        if self.terminal_enabled {
            let sandbox = self
                .process_sandbox
                .unwrap_or_else(|| Arc::new(crate::sandbox::HostProcessSandbox::default()));
            registry.register(ShellCommandTool::from_workspace_with_sandbox(
                workspace.clone(),
                Arc::clone(&sandbox),
            ))?;
            registry.register(ExecCommandTool::from_workspace_with_sandbox(
                workspace.clone(),
                Arc::clone(&sandbox),
            ))?;
            registry.register(WriteStdinTool::new(sandbox))?;
            registry.register(ApplyPatchTool::from_workspace(workspace))?;
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
    fn terminal_execution_requires_explicit_host_opt_in() {
        let directory = tempdir().expect("temporary workspace should be created");
        let registry = BuiltinToolCatalog::new(directory.path())
            .enable_terminal_tools()
            .build()
            .expect("catalog should build");
        let definitions = registry.definitions();
        assert!(!definitions.iter().any(|tool| tool.name == "run_command"));
        for name in ["shell_command", "exec_command", "write_stdin"] {
            let terminal = definitions
                .iter()
                .find(|tool| tool.name == name)
                .expect("terminal tool should be registered");
            assert_eq!(terminal.risk_level, ToolRiskLevel::High);
            assert!(terminal.risk_level.requires_approval());
        }
    }
}
