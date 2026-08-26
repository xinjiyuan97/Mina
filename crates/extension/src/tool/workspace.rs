use std::{
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use agent_core::harness::ToolError;

#[derive(Debug, Clone)]
pub(crate) struct Workspace {
    root: Arc<PathBuf>,
}

impl Workspace {
    pub(crate) fn new(root: impl AsRef<Path>) -> Result<Self, std::io::Error> {
        Ok(Self {
            root: Arc::new(std::fs::canonicalize(root)?),
        })
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
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

    pub(crate) async fn resolve_for_write(&self, requested: &str) -> Result<PathBuf, ToolError> {
        let relative = validated_relative(requested, false)?;
        reject_protected(relative)?;
        let target = self.root.join(relative);

        match tokio::fs::symlink_metadata(&target).await {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() {
                    return Err(ToolError::new(
                        "path_is_symlink",
                        "mutating tools do not write through symbolic links",
                        false,
                    ));
                }
                let resolved = tokio::fs::canonicalize(&target)
                    .await
                    .map_err(|_| unavailable_path())?;
                if !resolved.starts_with(self.root.as_path()) {
                    return Err(outside_workspace());
                }
                Ok(resolved)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let parent = target.parent().ok_or_else(outside_workspace)?;
                let resolved_parent = tokio::fs::canonicalize(parent).await.map_err(|error| {
                    if error.kind() == std::io::ErrorKind::NotFound {
                        ToolError::new(
                            "parent_not_found",
                            "the destination parent directory does not exist",
                            false,
                        )
                    } else {
                        unavailable_path()
                    }
                })?;
                if !resolved_parent.starts_with(self.root.as_path()) {
                    return Err(outside_workspace());
                }
                let file_name = target.file_name().ok_or_else(outside_workspace)?;
                Ok(resolved_parent.join(file_name))
            }
            Err(_) => Err(unavailable_path()),
        }
    }

    pub(crate) fn display_relative(&self, path: &Path) -> String {
        path.strip_prefix(self.root.as_path())
            .unwrap_or(path)
            .to_string_lossy()
            .replace(std::path::MAIN_SEPARATOR, "/")
    }

    pub(crate) fn is_protected(&self, path: &Path) -> bool {
        path.strip_prefix(self.root.as_path())
            .map_or(true, |relative| reject_protected(relative).is_err())
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

fn unavailable_path() -> ToolError {
    ToolError::new(
        "path_unavailable",
        "the requested workspace path is unavailable",
        false,
    )
}

fn outside_workspace() -> ToolError {
    ToolError::new(
        "path_outside_workspace",
        "tool paths must remain inside the configured workspace",
        false,
    )
}
