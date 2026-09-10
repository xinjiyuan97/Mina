use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::workspace::{NativeWorkspaceFs, WorkspaceFs};
use agent_core::harness::{ToolRegistrationError, ToolRegistry};
use agent_core::sandbox::ProcessSandbox;
use thiserror::Error;

#[cfg(feature = "docx-tools")]
use crate::tool::docx::DocxCheckTool;
#[cfg(feature = "pptx-tools")]
use crate::tool::pptx::PptxCheckTool;
#[cfg(feature = "xlsx-tools")]
use crate::tool::xlsx::XlsxCheckTool;
use crate::tool::{
    filesystem::{EditTool, ListDirectoryTool, ReadTool, WriteTool},
    native_path::NativePathWorkspace,
    patch::ApplyPatchTool,
    search::{SearchBackend, SearchTool, WorkspaceSearchBackend},
    terminal::{ExecCommandTool, ShellCommandTool, WriteStdinTool},
    time_tool::GetCurrentTimeTool,
};

/// Host-side selection of built-in tools. High-risk tools require an explicit
/// builder opt-in in addition to the harness approval policy.
#[derive(Clone)]
pub struct BuiltinToolCatalog {
    workspace_root: PathBuf,
    file_workspace: Option<Arc<dyn WorkspaceFs>>,
    terminal_enabled: bool,
    process_sandbox: Option<Arc<dyn ProcessSandbox>>,
    search_enabled: bool,
    search_backend: Option<Arc<dyn SearchBackend>>,
    pptx_enabled: bool,
    xlsx_enabled: bool,
    docx_enabled: bool,
}

impl std::fmt::Debug for BuiltinToolCatalog {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BuiltinToolCatalog")
            .field("workspace_root", &self.workspace_root)
            .field(
                "file_workspace",
                &self.file_workspace.as_ref().map(|value| value.descriptor()),
            )
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
            .field("pptx_enabled", &self.pptx_enabled)
            .field("xlsx_enabled", &self.xlsx_enabled)
            .field("docx_enabled", &self.docx_enabled)
            .finish()
    }
}

impl BuiltinToolCatalog {
    #[must_use]
    pub fn new(workspace_root: impl Into<PathBuf>) -> Self {
        Self {
            workspace_root: workspace_root.into(),
            file_workspace: None,
            terminal_enabled: false,
            process_sandbox: None,
            search_enabled: true,
            search_backend: None,
            pptx_enabled: false,
            xlsx_enabled: false,
            docx_enabled: false,
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

    /// Replaces the native filesystem used by portable file and workspace
    /// search tools. Server deployments can inject S3 here; desktop and local
    /// deployments keep the default native adapter.
    #[must_use]
    pub fn with_file_workspace(mut self, workspace: Arc<dyn WorkspaceFs>) -> Self {
        self.file_workspace = Some(workspace);
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

    /// Enables read-only structural validation for unpacked OOXML directories
    /// and packaged `.pptx` files inside the workspace.
    #[cfg(feature = "pptx-tools")]
    #[must_use]
    pub const fn enable_pptx_tools(mut self) -> Self {
        self.pptx_enabled = true;
        self
    }

    /// Enables read-only structural validation for unpacked OOXML directories
    /// and packaged `.xlsx` files inside the workspace.
    #[cfg(feature = "xlsx-tools")]
    #[must_use]
    pub const fn enable_xlsx_tools(mut self) -> Self {
        self.xlsx_enabled = true;
        self
    }

    /// Enables read-only structural validation for unpacked OOXML directories
    /// and packaged `.docx` files inside the workspace.
    #[cfg(feature = "docx-tools")]
    #[must_use]
    pub const fn enable_docx_tools(mut self) -> Self {
        self.docx_enabled = true;
        self
    }

    /// Enables the PowerPoint, Excel and Word OOXML structural checkers.
    #[cfg(all(feature = "pptx-tools", feature = "xlsx-tools", feature = "docx-tools"))]
    #[must_use]
    pub const fn enable_office_tools(mut self) -> Self {
        self.pptx_enabled = true;
        self.xlsx_enabled = true;
        self.docx_enabled = true;
        self
    }

    pub fn build(self) -> Result<ToolRegistry, BuiltinToolCatalogError> {
        let native_paths = NativePathWorkspace::new(&self.workspace_root)?;
        let file_workspace: Arc<dyn WorkspaceFs> = match self.file_workspace {
            Some(workspace) => workspace,
            None => Arc::new(NativeWorkspaceFs::new(&self.workspace_root)?),
        };
        let mut registry = ToolRegistry::new();
        registry.register(GetCurrentTimeTool)?;
        registry.register(ReadTool::new(Arc::clone(&file_workspace)))?;
        registry.register(ListDirectoryTool::new(Arc::clone(&file_workspace)))?;
        registry.register(WriteTool::new(Arc::clone(&file_workspace)))?;
        registry.register(EditTool::new(Arc::clone(&file_workspace)))?;
        if self.search_enabled {
            let backend = self.search_backend.unwrap_or_else(|| {
                Arc::new(WorkspaceSearchBackend::new(Arc::clone(&file_workspace)))
            });
            registry.register(SearchTool::new(backend))?;
        }
        #[cfg(feature = "pptx-tools")]
        if self.pptx_enabled {
            registry.register(PptxCheckTool::from_workspace(native_paths.clone()))?;
        }
        #[cfg(feature = "xlsx-tools")]
        if self.xlsx_enabled {
            registry.register(XlsxCheckTool::from_workspace(native_paths.clone()))?;
        }
        #[cfg(feature = "docx-tools")]
        if self.docx_enabled {
            registry.register(DocxCheckTool::from_workspace(native_paths.clone()))?;
        }
        if self.terminal_enabled {
            let sandbox = self
                .process_sandbox
                .unwrap_or_else(|| Arc::new(crate::sandbox::HostProcessSandbox::default()));
            registry.register(ShellCommandTool::from_workspace_with_sandbox(
                native_paths.clone(),
                Arc::clone(&sandbox),
            ))?;
            registry.register(ExecCommandTool::from_workspace_with_sandbox(
                native_paths,
                Arc::clone(&sandbox),
            ))?;
            registry.register(WriteStdinTool::new(sandbox))?;
            registry.register(ApplyPatchTool::new(file_workspace))?;
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

    #[cfg(feature = "pptx-tools")]
    #[test]
    fn pptx_checker_requires_explicit_host_opt_in() {
        let directory = tempdir().expect("temporary workspace should be created");
        let safe = BuiltinToolCatalog::new(directory.path())
            .build()
            .expect("catalog should build");
        assert!(
            !safe
                .definitions()
                .iter()
                .any(|tool| tool.name == "pptx_check")
        );

        let enabled = BuiltinToolCatalog::new(directory.path())
            .enable_pptx_tools()
            .build()
            .expect("catalog should build");
        let checker = enabled
            .definitions()
            .into_iter()
            .find(|tool| tool.name == "pptx_check")
            .expect("PPTX checker should be registered");
        assert_eq!(checker.risk_level, ToolRiskLevel::Low);
    }

    #[cfg(all(feature = "pptx-tools", feature = "xlsx-tools", feature = "docx-tools"))]
    #[test]
    fn office_checkers_can_be_enabled_together() {
        let directory = tempdir().expect("temporary workspace should be created");
        let registry = BuiltinToolCatalog::new(directory.path())
            .enable_office_tools()
            .build()
            .expect("catalog should build");
        for name in ["pptx_check", "xlsx_check", "docx_check"] {
            let checker = registry
                .definitions()
                .into_iter()
                .find(|tool| tool.name == name)
                .unwrap_or_else(|| panic!("{name} should be registered"));
            assert_eq!(checker.risk_level, ToolRiskLevel::Low);
        }
    }
}
