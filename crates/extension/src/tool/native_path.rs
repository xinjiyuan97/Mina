use std::{
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use agent_core::harness::ToolError;

/// Native path resolver used only by process and native-only package tools.
/// Portable file tools use `crate::workspace::WorkspaceFs` instead.
#[derive(Debug, Clone)]
pub(crate) struct NativePathWorkspace {
    root: Arc<PathBuf>,
}

impl NativePathWorkspace {
    pub(crate) fn new(root: impl AsRef<Path>) -> Result<Self, std::io::Error> {
        Ok(Self {
            root: Arc::new(std::fs::canonicalize(root)?),
        })
    }

    pub(crate) async fn resolve_existing(&self, requested: &str) -> Result<PathBuf, ToolError> {
        let relative = validated_relative(requested, true)?;
        reject_protected(relative)?;
        let resolved = tokio::fs::canonicalize(self.root.join(relative))
            .await
            .map_err(|error| match error.kind() {
                std::io::ErrorKind::NotFound => ToolError::new(
                    "path_not_found",
                    "the requested workspace path does not exist",
                    false,
                ),
                _ => ToolError::new(
                    "path_unavailable",
                    "the requested workspace path is unavailable",
                    false,
                ),
            })?;
        if !resolved.starts_with(self.root.as_path()) {
            return Err(outside_workspace());
        }
        Ok(resolved)
    }

    #[allow(dead_code)]
    pub(crate) fn display_relative(&self, path: &Path) -> String {
        path.strip_prefix(self.root.as_path())
            .unwrap_or(path)
            .to_string_lossy()
            .replace(std::path::MAIN_SEPARATOR, "/")
    }
}

fn reject_protected(relative: &Path) -> Result<(), ToolError> {
    let normalized = relative.to_string_lossy().replace('\\', "/");
    let file_name = relative
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    let contains_git = relative
        .components()
        .any(|component| component.as_os_str() == ".git");
    let protected = contains_git
        || normalized == "config/mina.toml"
        || file_name == ".env"
        || file_name.starts_with(".env.")
        || file_name.ends_with(".pem")
        || file_name.ends_with(".key");
    if protected {
        return Err(ToolError::new(
            "path_protected",
            "the requested path is protected by the host file policy",
            false,
        ));
    }
    Ok(())
}

fn validated_relative(requested: &str, allow_root: bool) -> Result<&Path, ToolError> {
    let relative = if requested.trim().is_empty() && allow_root {
        Path::new(".")
    } else {
        Path::new(requested)
    };
    if relative.as_os_str().is_empty()
        || relative.is_absolute()
        || relative.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err(outside_workspace());
    }
    Ok(relative)
}

fn outside_workspace() -> ToolError {
    ToolError::new(
        "path_outside_workspace",
        "tool paths must remain inside the configured workspace",
        false,
    )
}
