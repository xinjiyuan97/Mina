use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::UNIX_EPOCH,
};

use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use super::{
    CreateDirRequest, DirectoryEntry, DirectoryPage, FileContent, FileKind, FileMetadata,
    ListRequest, ReadRequest, RemoveRequest, RenameRequest, WorkspaceCapabilities,
    WorkspaceDescriptor, WorkspaceError, WorkspaceErrorKind, WorkspaceFs, WorkspaceFuture,
    WorkspacePath, WriteMode, WriteRequest,
};

/// Workspace backed by a root directory on the local/native filesystem.
///
/// Desktop and server deployments use this same adapter. Every resolved path
/// is checked against the canonical root, and mutations never follow a final
/// symbolic link.
#[derive(Debug, Clone)]
pub struct NativeWorkspaceFs {
    root: Arc<PathBuf>,
}

impl NativeWorkspaceFs {
    pub fn new(root: impl AsRef<Path>) -> Result<Self, std::io::Error> {
        Ok(Self {
            root: Arc::new(std::fs::canonicalize(root)?),
        })
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    async fn resolve_existing(&self, path: &WorkspacePath) -> Result<PathBuf, WorkspaceError> {
        let candidate = self.join(path);
        let resolved = tokio::fs::canonicalize(candidate)
            .await
            .map_err(map_io_error)?;
        self.ensure_inside(resolved)
    }

    async fn resolve_for_write(
        &self,
        path: &WorkspacePath,
        create_parents: bool,
    ) -> Result<PathBuf, WorkspaceError> {
        if path.is_root() {
            return Err(WorkspaceError::new(
                WorkspaceErrorKind::NotFile,
                "workspace_path_not_file",
                "the workspace root is not a file",
                false,
            ));
        }
        let candidate = self.join(path);
        match tokio::fs::symlink_metadata(&candidate).await {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() {
                    return Err(WorkspaceError::new(
                        WorkspaceErrorKind::PermissionDenied,
                        "workspace_symlink_write_denied",
                        "mutating operations do not follow symbolic links",
                        false,
                    ));
                }
                let resolved = tokio::fs::canonicalize(candidate)
                    .await
                    .map_err(map_io_error)?;
                self.ensure_inside(resolved)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let parent = candidate
                    .parent()
                    .ok_or_else(WorkspaceError::invalid_path)?;
                if create_parents {
                    self.create_parents_safely(parent).await?;
                }
                let resolved_parent = tokio::fs::canonicalize(parent)
                    .await
                    .map_err(map_io_error)?;
                let resolved_parent = self.ensure_inside(resolved_parent)?;
                let name = candidate
                    .file_name()
                    .ok_or_else(WorkspaceError::invalid_path)?;
                Ok(resolved_parent.join(name))
            }
            Err(error) => Err(map_io_error(error)),
        }
    }

    async fn create_parents_safely(&self, parent: &Path) -> Result<(), WorkspaceError> {
        let relative = parent
            .strip_prefix(self.root.as_path())
            .map_err(|_| WorkspaceError::invalid_path())?;
        let mut current = self.root.as_path().to_path_buf();
        for component in relative.components() {
            current.push(component);
            match tokio::fs::symlink_metadata(&current).await {
                Ok(metadata) => {
                    if metadata.file_type().is_symlink() || !metadata.is_dir() {
                        return Err(WorkspaceError::new(
                            WorkspaceErrorKind::PermissionDenied,
                            "workspace_parent_unavailable",
                            "a destination parent is not a safe directory",
                            false,
                        ));
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    tokio::fs::create_dir(&current)
                        .await
                        .map_err(map_io_error)?;
                }
                Err(error) => return Err(map_io_error(error)),
            }
        }
        Ok(())
    }

    fn join(&self, path: &WorkspacePath) -> PathBuf {
        if path.is_root() {
            self.root.as_path().to_path_buf()
        } else {
            self.root.join(path.storage_key())
        }
    }

    fn ensure_inside(&self, path: PathBuf) -> Result<PathBuf, WorkspaceError> {
        if path.starts_with(self.root.as_path()) {
            Ok(path)
        } else {
            Err(WorkspaceError::new(
                WorkspaceErrorKind::PermissionDenied,
                "workspace_path_outside_root",
                "resolved path is outside the workspace root",
                false,
            ))
        }
    }

    async fn metadata(
        &self,
        logical_path: WorkspacePath,
        native_path: &Path,
    ) -> Result<FileMetadata, WorkspaceError> {
        let metadata = tokio::fs::metadata(native_path)
            .await
            .map_err(map_io_error)?;
        Ok(metadata_from_std(logical_path, &metadata))
    }
}

impl WorkspaceFs for NativeWorkspaceFs {
    fn descriptor(&self) -> WorkspaceDescriptor {
        WorkspaceDescriptor {
            identity: format!("workspace:native:{}", self.root.display()),
            kind: "native".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            persistent: true,
            shared: false,
        }
    }

    fn capabilities(&self) -> WorkspaceCapabilities {
        WorkspaceCapabilities {
            readable: true,
            writable: true,
            directories: true,
            range_read: true,
            append: true,
            atomic_rename: true,
        }
    }

    fn stat(&self, path: WorkspacePath) -> WorkspaceFuture<'_, FileMetadata> {
        Box::pin(async move {
            let candidate = self.join(&path);
            let link_metadata = tokio::fs::symlink_metadata(&candidate)
                .await
                .map_err(map_io_error)?;
            if link_metadata.file_type().is_symlink() {
                return Ok(metadata_from_std(path, &link_metadata));
            }
            let native_path = self.ensure_inside(
                tokio::fs::canonicalize(candidate)
                    .await
                    .map_err(map_io_error)?,
            )?;
            self.metadata(path, &native_path).await
        })
    }

    fn read(&self, request: ReadRequest) -> WorkspaceFuture<'_, FileContent> {
        Box::pin(async move {
            let native_path = self.resolve_existing(&request.path).await?;
            let metadata = self.metadata(request.path.clone(), &native_path).await?;
            if metadata.kind != FileKind::File {
                return Err(not_file());
            }
            let available = metadata.size.saturating_sub(request.offset);
            let requested = request.length.unwrap_or(available).min(available);
            if requested > request.max_bytes {
                return Err(WorkspaceError::new(
                    WorkspaceErrorKind::TooLarge,
                    "workspace_read_too_large",
                    "requested file content exceeds the configured read limit",
                    false,
                ));
            }
            let length = usize::try_from(requested).map_err(|_| {
                WorkspaceError::new(
                    WorkspaceErrorKind::TooLarge,
                    "workspace_read_too_large",
                    "requested file content cannot fit in memory",
                    false,
                )
            })?;
            let mut file = tokio::fs::File::open(native_path)
                .await
                .map_err(map_io_error)?;
            file.seek(std::io::SeekFrom::Start(request.offset))
                .await
                .map_err(map_io_error)?;
            let mut bytes = vec![0; length];
            file.read_exact(&mut bytes).await.map_err(map_io_error)?;
            Ok(FileContent { bytes, metadata })
        })
    }

    fn write(&self, request: WriteRequest) -> WorkspaceFuture<'_, FileMetadata> {
        Box::pin(async move {
            let native_path = self
                .resolve_for_write(&request.path, request.create_parents)
                .await?;
            if let Ok(existing) = tokio::fs::metadata(&native_path).await
                && !existing.is_file()
            {
                return Err(not_file());
            }
            let mut options = tokio::fs::OpenOptions::new();
            options.write(true);
            match request.mode {
                WriteMode::CreateNew => {
                    options.create_new(true);
                }
                WriteMode::Truncate => {
                    options.create(true).truncate(true);
                }
                WriteMode::Append => {
                    options.create(true).append(true);
                }
            }
            let mut file = options.open(&native_path).await.map_err(map_io_error)?;
            file.write_all(&request.bytes).await.map_err(map_io_error)?;
            file.flush().await.map_err(map_io_error)?;
            self.metadata(request.path, &native_path).await
        })
    }

    fn list(&self, request: ListRequest) -> WorkspaceFuture<'_, DirectoryPage> {
        Box::pin(async move {
            let native_path = self.resolve_existing(&request.path).await?;
            if !tokio::fs::metadata(&native_path)
                .await
                .map_err(map_io_error)?
                .is_dir()
            {
                return Err(not_directory());
            }
            let limit = request.limit.clamp(1, 10_000);
            let mut reader = tokio::fs::read_dir(native_path)
                .await
                .map_err(map_io_error)?;
            let mut entries = Vec::new();
            while let Some(entry) = reader.next_entry().await.map_err(map_io_error)? {
                let name = entry.file_name().to_string_lossy().into_owned();
                if request
                    .cursor
                    .as_ref()
                    .is_some_and(|cursor| name <= *cursor)
                {
                    continue;
                }
                let path = request.path.join(&name)?;
                let metadata = entry.metadata().await.map_err(map_io_error)?;
                let file_type = entry.file_type().await.map_err(map_io_error)?;
                entries.push(DirectoryEntry {
                    path,
                    kind: kind_from_file_type(file_type),
                    size: metadata.len(),
                    revision: revision(&metadata),
                });
            }
            entries.sort_by(|left, right| left.path.cmp(&right.path));
            let next_cursor = if entries.len() > limit {
                entries
                    .get(limit - 1)
                    .and_then(|entry| entry.path.file_name())
                    .map(str::to_owned)
            } else {
                None
            };
            entries.truncate(limit);
            Ok(DirectoryPage {
                entries,
                next_cursor,
            })
        })
    }

    fn create_dir(&self, request: CreateDirRequest) -> WorkspaceFuture<'_, FileMetadata> {
        Box::pin(async move {
            let native_path = self.join(&request.path);
            if request.recursive {
                self.create_parents_safely(&native_path).await?;
            } else {
                let parent = native_path
                    .parent()
                    .ok_or_else(WorkspaceError::invalid_path)?;
                let resolved_parent = tokio::fs::canonicalize(parent)
                    .await
                    .map_err(map_io_error)?;
                self.ensure_inside(resolved_parent)?;
                tokio::fs::create_dir(&native_path)
                    .await
                    .map_err(map_io_error)?;
            }
            let resolved = self.resolve_existing(&request.path).await?;
            self.metadata(request.path, &resolved).await
        })
    }

    fn remove(&self, request: RemoveRequest) -> WorkspaceFuture<'_, ()> {
        Box::pin(async move {
            if request.path.is_root() {
                return Err(WorkspaceError::new(
                    WorkspaceErrorKind::PermissionDenied,
                    "workspace_remove_root_denied",
                    "the workspace root cannot be removed",
                    false,
                ));
            }
            let native_path = self.resolve_for_write(&request.path, false).await?;
            let metadata = tokio::fs::metadata(&native_path)
                .await
                .map_err(map_io_error)?;
            if metadata.is_dir() {
                if request.recursive {
                    tokio::fs::remove_dir_all(native_path)
                        .await
                        .map_err(map_io_error)
                } else {
                    tokio::fs::remove_dir(native_path)
                        .await
                        .map_err(map_io_error)
                }
            } else if metadata.is_file() {
                tokio::fs::remove_file(native_path)
                    .await
                    .map_err(map_io_error)
            } else {
                Err(not_file())
            }
        })
    }

    fn rename(&self, request: RenameRequest) -> WorkspaceFuture<'_, FileMetadata> {
        Box::pin(async move {
            if request.from.is_root() || request.to.is_root() {
                return Err(WorkspaceError::invalid_path());
            }
            let source = self.resolve_existing(&request.from).await?;
            let destination = self.resolve_for_write(&request.to, false).await?;
            if !request.overwrite && tokio::fs::symlink_metadata(&destination).await.is_ok() {
                return Err(WorkspaceError::new(
                    WorkspaceErrorKind::AlreadyExists,
                    "workspace_destination_exists",
                    "the destination already exists",
                    false,
                ));
            }
            tokio::fs::rename(source, &destination)
                .await
                .map_err(map_io_error)?;
            self.metadata(request.to, &destination).await
        })
    }
}

fn metadata_from_std(path: WorkspacePath, metadata: &std::fs::Metadata) -> FileMetadata {
    FileMetadata {
        path,
        kind: kind_from_file_type(metadata.file_type()),
        size: metadata.len(),
        modified_at_ms: modified_at_ms(metadata),
        revision: revision(metadata),
    }
}

fn kind_from_file_type(file_type: std::fs::FileType) -> FileKind {
    if file_type.is_file() {
        FileKind::File
    } else if file_type.is_dir() {
        FileKind::Directory
    } else if file_type.is_symlink() {
        FileKind::Symlink
    } else {
        FileKind::Other
    }
}

fn modified_at_ms(metadata: &std::fs::Metadata) -> Option<i64> {
    let millis = metadata
        .modified()
        .ok()?
        .duration_since(UNIX_EPOCH)
        .ok()?
        .as_millis();
    i64::try_from(millis).ok()
}

fn revision(metadata: &std::fs::Metadata) -> Option<String> {
    Some(format!(
        "native:{}:{}",
        metadata.len(),
        modified_at_ms(metadata).unwrap_or_default()
    ))
}

fn map_io_error(error: std::io::Error) -> WorkspaceError {
    let (kind, code, retryable) = match error.kind() {
        std::io::ErrorKind::NotFound => {
            (WorkspaceErrorKind::NotFound, "workspace_not_found", false)
        }
        std::io::ErrorKind::AlreadyExists => (
            WorkspaceErrorKind::AlreadyExists,
            "workspace_already_exists",
            false,
        ),
        std::io::ErrorKind::PermissionDenied => (
            WorkspaceErrorKind::PermissionDenied,
            "workspace_permission_denied",
            false,
        ),
        std::io::ErrorKind::InvalidInput | std::io::ErrorKind::InvalidData => (
            WorkspaceErrorKind::InvalidPath,
            "workspace_invalid_request",
            false,
        ),
        _ => (WorkspaceErrorKind::Unavailable, "workspace_io_failed", true),
    };
    WorkspaceError::new(
        kind,
        code,
        "workspace filesystem operation failed",
        retryable,
    )
}

fn not_file() -> WorkspaceError {
    WorkspaceError::new(
        WorkspaceErrorKind::NotFile,
        "workspace_path_not_file",
        "the requested workspace path is not a file",
        false,
    )
}

fn not_directory() -> WorkspaceError {
    WorkspaceError::new(
        WorkspaceErrorKind::NotDirectory,
        "workspace_path_not_directory",
        "the requested workspace path is not a directory",
        false,
    )
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[tokio::test]
    async fn reads_writes_lists_and_renames_inside_root() {
        let directory = tempdir().expect("temporary workspace");
        let workspace = NativeWorkspaceFs::new(directory.path()).expect("native workspace");
        let path = WorkspacePath::parse("nested/hello.txt").expect("valid path");
        workspace
            .write(WriteRequest {
                path: path.clone(),
                bytes: b"hello".to_vec(),
                mode: WriteMode::Truncate,
                create_parents: true,
            })
            .await
            .expect("write");
        let content = workspace
            .read(ReadRequest {
                path: path.clone(),
                offset: 0,
                length: None,
                max_bytes: 32,
            })
            .await
            .expect("read");
        assert_eq!(content.bytes, b"hello");

        let page = workspace
            .list(ListRequest {
                path: WorkspacePath::parse("nested").expect("valid path"),
                cursor: None,
                limit: 10,
            })
            .await
            .expect("list");
        assert_eq!(page.entries.len(), 1);

        let renamed = WorkspacePath::parse("nested/renamed.txt").expect("valid path");
        workspace
            .rename(RenameRequest {
                from: path,
                to: renamed.clone(),
                overwrite: false,
            })
            .await
            .expect("rename");
        assert_eq!(workspace.stat(renamed).await.expect("stat").size, 5);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rejects_symlink_escape_and_symlink_mutation() {
        use std::os::unix::fs::symlink;

        let directory = tempdir().expect("temporary workspace");
        let outside = tempdir().expect("outside directory");
        std::fs::write(outside.path().join("secret"), "secret").expect("fixture");
        symlink(outside.path().join("secret"), directory.path().join("link")).expect("symlink");
        let workspace = NativeWorkspaceFs::new(directory.path()).expect("native workspace");
        let path = WorkspacePath::parse("link").expect("valid path");
        assert_eq!(
            workspace
                .read(ReadRequest {
                    path: path.clone(),
                    offset: 0,
                    length: None,
                    max_bytes: 32,
                })
                .await
                .expect_err("must reject")
                .kind(),
            WorkspaceErrorKind::PermissionDenied
        );
        assert_eq!(
            workspace
                .write(WriteRequest {
                    path,
                    bytes: Vec::new(),
                    mode: WriteMode::Truncate,
                    create_parents: false,
                })
                .await
                .expect_err("must reject")
                .kind(),
            WorkspaceErrorKind::PermissionDenied
        );
    }
}
