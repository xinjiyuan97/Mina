use std::sync::Arc;

use aws_sdk_s3::{Client, primitives::ByteStream};

use super::{
    CreateDirRequest, DirectoryEntry, DirectoryPage, FileContent, FileKind, FileMetadata,
    ListRequest, ReadRequest, RemoveRequest, RenameRequest, WorkspaceCapabilities,
    WorkspaceDescriptor, WorkspaceError, WorkspaceErrorKind, WorkspaceFs, WorkspaceFuture,
    WorkspacePath, WriteMode, WriteRequest,
};

/// Workspace backed by objects under one S3 bucket prefix.
///
/// Credential discovery, endpoint selection, retries, and HTTP policy remain
/// application concerns: the server constructs and injects the configured S3
/// client. Directories use prefix semantics. Rename is copy-then-delete and is
/// therefore intentionally reported as non-atomic.
#[derive(Clone)]
pub struct S3WorkspaceFs {
    client: Client,
    bucket: Arc<str>,
    prefix: Arc<str>,
}

impl std::fmt::Debug for S3WorkspaceFs {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("S3WorkspaceFs")
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

impl S3WorkspaceFs {
    pub fn new(
        client: Client,
        bucket: impl Into<String>,
        prefix: impl AsRef<str>,
    ) -> Result<Self, WorkspaceError> {
        let bucket = bucket.into();
        if bucket.trim().is_empty() {
            return Err(WorkspaceError::new(
                WorkspaceErrorKind::InvalidPath,
                "workspace_invalid_bucket",
                "S3 workspace bucket must not be empty",
                false,
            ));
        }
        let prefix = WorkspacePath::parse(prefix)?.storage_key().to_owned();
        Ok(Self {
            client,
            bucket: bucket.into(),
            prefix: prefix.into(),
        })
    }

    fn key(&self, path: &WorkspacePath) -> String {
        match (self.prefix.is_empty(), path.is_root()) {
            (true, true) => String::new(),
            (true, false) => path.storage_key().to_owned(),
            (false, true) => self.prefix.to_string(),
            (false, false) => format!("{}/{}", self.prefix, path.storage_key()),
        }
    }

    fn directory_prefix(&self, path: &WorkspacePath) -> String {
        let key = self.key(path);
        if key.is_empty() || key.ends_with('/') {
            key
        } else {
            format!("{key}/")
        }
    }

    async fn copy_object(&self, from: &str, to: &str) -> Result<(), WorkspaceError> {
        self.client
            .copy_object()
            .bucket(self.bucket.as_ref())
            .key(to)
            .copy_source(format!("{}/{}", self.bucket, encode_copy_source_key(from)))
            .send()
            .await
            .map_err(|_| backend_error())?;
        Ok(())
    }

    async fn delete_key(&self, key: &str) -> Result<(), WorkspaceError> {
        self.client
            .delete_object()
            .bucket(self.bucket.as_ref())
            .key(key)
            .send()
            .await
            .map_err(|_| backend_error())?;
        Ok(())
    }

    async fn keys_under(&self, prefix: &str) -> Result<Vec<String>, WorkspaceError> {
        let mut continuation = None;
        let mut keys = Vec::new();
        loop {
            let output = self
                .client
                .list_objects_v2()
                .bucket(self.bucket.as_ref())
                .prefix(prefix)
                .set_continuation_token(continuation)
                .send()
                .await
                .map_err(|_| backend_error())?;
            keys.extend(
                output
                    .contents()
                    .iter()
                    .filter_map(|object| object.key().map(str::to_owned)),
            );
            if !output.is_truncated().unwrap_or(false) {
                break;
            }
            continuation = output.next_continuation_token().map(str::to_owned);
            if continuation.is_none() {
                break;
            }
        }
        Ok(keys)
    }
}

impl WorkspaceFs for S3WorkspaceFs {
    fn descriptor(&self) -> WorkspaceDescriptor {
        WorkspaceDescriptor {
            identity: format!("workspace:s3:{}/{}", self.bucket, self.prefix),
            kind: "s3".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            persistent: true,
            shared: true,
        }
    }

    fn capabilities(&self) -> WorkspaceCapabilities {
        WorkspaceCapabilities {
            readable: true,
            writable: true,
            directories: true,
            range_read: true,
            append: false,
            atomic_rename: false,
        }
    }

    fn stat(&self, path: WorkspacePath) -> WorkspaceFuture<'_, FileMetadata> {
        Box::pin(async move {
            if path.is_root() {
                return Ok(directory_metadata(path));
            }
            let key = self.key(&path);
            if let Ok(output) = self
                .client
                .head_object()
                .bucket(self.bucket.as_ref())
                .key(&key)
                .send()
                .await
            {
                return Ok(FileMetadata {
                    path,
                    kind: FileKind::File,
                    size: non_negative_size(output.content_length()),
                    modified_at_ms: output
                        .last_modified()
                        .map(|value| value.secs().saturating_mul(1_000)),
                    revision: output.e_tag().map(str::to_owned),
                });
            }

            let output = self
                .client
                .list_objects_v2()
                .bucket(self.bucket.as_ref())
                .prefix(self.directory_prefix(&path))
                .max_keys(1)
                .send()
                .await
                .map_err(|_| backend_error())?;
            if output.key_count().unwrap_or_default() > 0 {
                Ok(directory_metadata(path))
            } else {
                Err(not_found())
            }
        })
    }

    fn read(&self, request: ReadRequest) -> WorkspaceFuture<'_, FileContent> {
        Box::pin(async move {
            let metadata = self.stat(request.path.clone()).await?;
            if metadata.kind != FileKind::File {
                return Err(not_file());
            }
            let available = metadata.size.saturating_sub(request.offset);
            let length = request.length.unwrap_or(available).min(available);
            if length > request.max_bytes {
                return Err(WorkspaceError::new(
                    WorkspaceErrorKind::TooLarge,
                    "workspace_read_too_large",
                    "requested file content exceeds the configured read limit",
                    false,
                ));
            }
            if length == 0 {
                return Ok(FileContent {
                    bytes: Vec::new(),
                    metadata,
                });
            }
            let end = request.offset.saturating_add(length).saturating_sub(1);
            let output = self
                .client
                .get_object()
                .bucket(self.bucket.as_ref())
                .key(self.key(&request.path))
                .range(format!("bytes={}-{}", request.offset, end))
                .send()
                .await
                .map_err(|_| backend_error())?;
            let bytes = output
                .body
                .collect()
                .await
                .map_err(|_| backend_error())?
                .into_bytes()
                .to_vec();
            Ok(FileContent { bytes, metadata })
        })
    }

    fn write(&self, request: WriteRequest) -> WorkspaceFuture<'_, FileMetadata> {
        Box::pin(async move {
            if request.path.is_root() {
                return Err(not_file());
            }
            if request.mode == WriteMode::Append {
                return Err(WorkspaceError::new(
                    WorkspaceErrorKind::Unsupported,
                    "workspace_append_unsupported",
                    "S3 workspaces do not support atomic append",
                    false,
                ));
            }
            if request.mode == WriteMode::CreateNew {
                match self.stat(request.path.clone()).await {
                    Ok(_) => {
                        return Err(WorkspaceError::new(
                            WorkspaceErrorKind::AlreadyExists,
                            "workspace_already_exists",
                            "workspace path already exists",
                            false,
                        ));
                    }
                    Err(error) if error.kind() == WorkspaceErrorKind::NotFound => {}
                    Err(error) => return Err(error),
                }
            }
            self.client
                .put_object()
                .bucket(self.bucket.as_ref())
                .key(self.key(&request.path))
                .body(ByteStream::from(request.bytes))
                .send()
                .await
                .map_err(|_| backend_error())?;
            self.stat(request.path).await
        })
    }

    fn list(&self, request: ListRequest) -> WorkspaceFuture<'_, DirectoryPage> {
        Box::pin(async move {
            let prefix = self.directory_prefix(&request.path);
            let output = self
                .client
                .list_objects_v2()
                .bucket(self.bucket.as_ref())
                .prefix(&prefix)
                .delimiter("/")
                .set_continuation_token(request.cursor)
                .max_keys(i32::try_from(request.limit.clamp(1, 1_000)).unwrap_or(1_000))
                .send()
                .await
                .map_err(|_| backend_error())?;
            let mut entries = Vec::new();
            for object in output.contents() {
                let Some(key) = object.key() else { continue };
                if key == prefix {
                    continue;
                }
                let relative = key.strip_prefix(&prefix).unwrap_or(key);
                if relative.contains('/') {
                    continue;
                }
                entries.push(DirectoryEntry {
                    path: request.path.join(relative)?,
                    kind: FileKind::File,
                    size: non_negative_size(object.size()),
                    revision: object.e_tag().map(str::to_owned),
                });
            }
            for common in output.common_prefixes() {
                let Some(key) = common.prefix() else { continue };
                let relative = key
                    .strip_prefix(&prefix)
                    .unwrap_or(key)
                    .trim_end_matches('/');
                if relative.is_empty() {
                    continue;
                }
                entries.push(DirectoryEntry {
                    path: request.path.join(relative)?,
                    kind: FileKind::Directory,
                    size: 0,
                    revision: None,
                });
            }
            entries.sort_by(|left, right| left.path.cmp(&right.path));
            Ok(DirectoryPage {
                entries,
                next_cursor: output.next_continuation_token().map(str::to_owned),
            })
        })
    }

    fn create_dir(&self, request: CreateDirRequest) -> WorkspaceFuture<'_, FileMetadata> {
        Box::pin(async move {
            if request.path.is_root() {
                return Ok(directory_metadata(request.path));
            }
            self.client
                .put_object()
                .bucket(self.bucket.as_ref())
                .key(self.directory_prefix(&request.path))
                .body(ByteStream::from(Vec::new()))
                .send()
                .await
                .map_err(|_| backend_error())?;
            Ok(directory_metadata(request.path))
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
            let metadata = self.stat(request.path.clone()).await?;
            if metadata.kind == FileKind::File {
                return self.delete_key(&self.key(&request.path)).await;
            }
            let prefix = self.directory_prefix(&request.path);
            let keys = self.keys_under(&prefix).await?;
            if !request.recursive && keys.iter().any(|key| key != &prefix) {
                return Err(WorkspaceError::new(
                    WorkspaceErrorKind::Conflict,
                    "workspace_directory_not_empty",
                    "workspace directory is not empty",
                    false,
                ));
            }
            for key in keys {
                self.delete_key(&key).await?;
            }
            Ok(())
        })
    }

    fn rename(&self, request: RenameRequest) -> WorkspaceFuture<'_, FileMetadata> {
        Box::pin(async move {
            if request.from.is_root() || request.to.is_root() {
                return Err(WorkspaceError::invalid_path());
            }
            if !request.overwrite {
                match self.stat(request.to.clone()).await {
                    Ok(_) => {
                        return Err(WorkspaceError::new(
                            WorkspaceErrorKind::AlreadyExists,
                            "workspace_destination_exists",
                            "the destination already exists",
                            false,
                        ));
                    }
                    Err(error) if error.kind() == WorkspaceErrorKind::NotFound => {}
                    Err(error) => return Err(error),
                }
            }
            let metadata = self.stat(request.from.clone()).await?;
            if metadata.kind == FileKind::File {
                let from = self.key(&request.from);
                let to = self.key(&request.to);
                self.copy_object(&from, &to).await?;
                self.delete_key(&from).await?;
            } else {
                let from_prefix = self.directory_prefix(&request.from);
                let to_prefix = self.directory_prefix(&request.to);
                let keys = self.keys_under(&from_prefix).await?;
                for from in &keys {
                    let suffix = from.strip_prefix(&from_prefix).unwrap_or(from);
                    self.copy_object(from, &format!("{to_prefix}{suffix}"))
                        .await?;
                }
                for key in keys {
                    self.delete_key(&key).await?;
                }
            }
            self.stat(request.to).await
        })
    }
}

fn directory_metadata(path: WorkspacePath) -> FileMetadata {
    FileMetadata {
        path,
        kind: FileKind::Directory,
        size: 0,
        modified_at_ms: None,
        revision: None,
    }
}

fn non_negative_size(size: Option<i64>) -> u64 {
    size.and_then(|value| u64::try_from(value).ok())
        .unwrap_or_default()
}

fn encode_copy_source_key(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~' | b'/') {
            encoded.push(char::from(byte));
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

fn not_found() -> WorkspaceError {
    WorkspaceError::new(
        WorkspaceErrorKind::NotFound,
        "workspace_not_found",
        "workspace path was not found",
        false,
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

fn backend_error() -> WorkspaceError {
    WorkspaceError::new(
        WorkspaceErrorKind::Backend,
        "workspace_s3_failed",
        "S3 workspace operation failed",
        true,
    )
}
