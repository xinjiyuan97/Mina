use std::{fmt, future::Future, pin::Pin};

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[cfg(not(target_arch = "wasm32"))]
pub type WorkspaceFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, WorkspaceError>> + Send + 'a>>;
#[cfg(target_arch = "wasm32")]
pub type WorkspaceFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, WorkspaceError>> + 'a>>;

#[cfg(not(target_arch = "wasm32"))]
pub trait WorkspaceRuntime: Send + Sync {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send + Sync> WorkspaceRuntime for T {}

#[cfg(target_arch = "wasm32")]
pub trait WorkspaceRuntime {}
#[cfg(target_arch = "wasm32")]
impl<T> WorkspaceRuntime for T {}

/// A normalized path relative to a workspace root.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WorkspacePath(String);

impl WorkspacePath {
    pub fn parse(path: impl AsRef<str>) -> Result<Self, WorkspaceError> {
        let raw = path.as_ref().trim();
        if raw.is_empty() || raw == "." {
            return Ok(Self::root());
        }
        if raw.starts_with('/') || raw.starts_with('\\') || raw.contains('\0') {
            return Err(WorkspaceError::invalid_path());
        }

        let mut normalized = Vec::new();
        for component in raw.split(['/', '\\']) {
            match component {
                "" | "." => {}
                ".." => return Err(WorkspaceError::invalid_path()),
                value if value.ends_with(':') && normalized.is_empty() => {
                    return Err(WorkspaceError::invalid_path());
                }
                value => normalized.push(value),
            }
        }
        Ok(Self(normalized.join("/")))
    }

    #[must_use]
    pub const fn root() -> Self {
        Self(String::new())
    }

    #[must_use]
    pub fn is_root(&self) -> bool {
        self.0.is_empty()
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        if self.is_root() { "." } else { &self.0 }
    }

    #[must_use]
    pub fn storage_key(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn file_name(&self) -> Option<&str> {
        self.0.rsplit('/').next().filter(|value| !value.is_empty())
    }

    #[must_use]
    pub fn parent(&self) -> Option<Self> {
        if self.is_root() {
            None
        } else {
            Some(Self(
                self.0
                    .rsplit_once('/')
                    .map_or_else(String::new, |(parent, _)| parent.to_owned()),
            ))
        }
    }

    pub fn join(&self, child: &str) -> Result<Self, WorkspaceError> {
        let child = Self::parse(child)?;
        if child.is_root() {
            return Ok(self.clone());
        }
        if self.is_root() {
            Ok(child)
        } else {
            Ok(Self(format!("{}/{}", self.0, child.0)))
        }
    }
}

impl fmt::Display for WorkspacePath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceDescriptor {
    pub identity: String,
    pub kind: String,
    pub version: String,
    pub persistent: bool,
    pub shared: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceCapabilities {
    pub readable: bool,
    pub writable: bool,
    pub directories: bool,
    pub range_read: bool,
    pub append: bool,
    pub atomic_rename: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileKind {
    File,
    Directory,
    Symlink,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileMetadata {
    pub path: WorkspacePath,
    pub kind: FileKind,
    pub size: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modified_at_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectoryEntry {
    pub path: WorkspacePath,
    pub kind: FileKind,
    pub size: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ReadRequest {
    pub path: WorkspacePath,
    pub offset: u64,
    pub length: Option<u64>,
    pub max_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileContent {
    pub bytes: Vec<u8>,
    pub metadata: FileMetadata,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteMode {
    CreateNew,
    Truncate,
    Append,
}

#[derive(Debug, Clone)]
pub struct WriteRequest {
    pub path: WorkspacePath,
    pub bytes: Vec<u8>,
    pub mode: WriteMode,
    pub create_parents: bool,
}

#[derive(Debug, Clone)]
pub struct ListRequest {
    pub path: WorkspacePath,
    pub cursor: Option<String>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectoryPage {
    pub entries: Vec<DirectoryEntry>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CreateDirRequest {
    pub path: WorkspacePath,
    pub recursive: bool,
}

#[derive(Debug, Clone)]
pub struct RemoveRequest {
    pub path: WorkspacePath,
    pub recursive: bool,
}

#[derive(Debug, Clone)]
pub struct RenameRequest {
    pub from: WorkspacePath,
    pub to: WorkspacePath,
    pub overwrite: bool,
}

/// Environment-neutral workspace filesystem. Concrete implementations are
/// selected by the application and injected into tools.
pub trait WorkspaceFs: WorkspaceRuntime + 'static {
    fn descriptor(&self) -> WorkspaceDescriptor;
    fn capabilities(&self) -> WorkspaceCapabilities;
    fn stat(&self, path: WorkspacePath) -> WorkspaceFuture<'_, FileMetadata>;
    fn read(&self, request: ReadRequest) -> WorkspaceFuture<'_, FileContent>;
    fn write(&self, request: WriteRequest) -> WorkspaceFuture<'_, FileMetadata>;
    fn list(&self, request: ListRequest) -> WorkspaceFuture<'_, DirectoryPage>;
    fn create_dir(&self, request: CreateDirRequest) -> WorkspaceFuture<'_, FileMetadata>;
    fn remove(&self, request: RemoveRequest) -> WorkspaceFuture<'_, ()>;
    fn rename(&self, request: RenameRequest) -> WorkspaceFuture<'_, FileMetadata>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceErrorKind {
    InvalidPath,
    NotFound,
    AlreadyExists,
    NotFile,
    NotDirectory,
    PermissionDenied,
    TooLarge,
    Conflict,
    Unsupported,
    Unavailable,
    Backend,
}

#[derive(Debug, Clone, Error)]
#[error("{message}")]
pub struct WorkspaceError {
    kind: WorkspaceErrorKind,
    code: String,
    message: String,
    retryable: bool,
}

impl WorkspaceError {
    #[must_use]
    pub fn new(
        kind: WorkspaceErrorKind,
        code: impl Into<String>,
        message: impl Into<String>,
        retryable: bool,
    ) -> Self {
        Self {
            kind,
            code: code.into(),
            message: message.into(),
            retryable,
        }
    }

    #[must_use]
    pub fn invalid_path() -> Self {
        Self::new(
            WorkspaceErrorKind::InvalidPath,
            "workspace_invalid_path",
            "path must be relative to the workspace and must not contain parent traversal",
            false,
        )
    }

    #[must_use]
    pub const fn kind(&self) -> WorkspaceErrorKind {
        self.kind
    }

    #[must_use]
    pub fn code(&self) -> &str {
        &self.code
    }

    #[must_use]
    pub fn safe_message(&self) -> &str {
        &self.message
    }

    #[must_use]
    pub const fn retryable(&self) -> bool {
        self.retryable
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_portable_workspace_paths() {
        assert_eq!(
            WorkspacePath::parse("src\\nested/./lib.rs")
                .expect("path should parse")
                .storage_key(),
            "src/nested/lib.rs"
        );
        assert!(WorkspacePath::parse("../secret").is_err());
        assert!(WorkspacePath::parse("/absolute").is_err());
        assert!(WorkspacePath::parse("C:\\absolute").is_err());
    }
}
