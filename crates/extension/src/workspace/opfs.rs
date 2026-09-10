use js_sys::{Promise, Uint8Array};
use serde::Deserialize;
use wasm_bindgen::{JsCast, JsValue, prelude::wasm_bindgen};
use wasm_bindgen_futures::JsFuture;

use super::{
    CreateDirRequest, DirectoryEntry, DirectoryPage, FileContent, FileKind, FileMetadata,
    ListRequest, ReadRequest, RemoveRequest, RenameRequest, WorkspaceCapabilities,
    WorkspaceDescriptor, WorkspaceError, WorkspaceErrorKind, WorkspaceFs, WorkspaceFuture,
    WorkspacePath, WriteMode, WriteRequest,
};

#[wasm_bindgen(inline_js = r#"
async function minaOpfsRoot() {
  if (!globalThis.navigator?.storage?.getDirectory) {
    throw new Error("workspace_unsupported|OPFS is not available in this browser");
  }
  return navigator.storage.getDirectory();
}

function minaParts(path) {
  return path ? path.split("/").filter(Boolean) : [];
}

async function minaDirectory(path, create = false) {
  let directory = await minaOpfsRoot();
  for (const part of minaParts(path)) {
    directory = await directory.getDirectoryHandle(part, { create });
  }
  return directory;
}

async function minaParent(path, create = false) {
  const parts = minaParts(path);
  const name = parts.pop();
  if (!name) throw new Error("workspace_invalid_path|the workspace root is not a file");
  return { directory: await minaDirectory(parts.join("/"), create), name };
}

function minaWrap(error) {
  if (error?.message?.startsWith("workspace_")) return error;
  const name = error?.name;
  if (name === "NotFoundError") return new Error("workspace_not_found|workspace path was not found");
  if (name === "TypeMismatchError") return new Error("workspace_type_mismatch|workspace path has the wrong type");
  if (name === "InvalidModificationError") return new Error("workspace_conflict|workspace mutation conflicts with existing data");
  if (name === "NoModificationAllowedError" || name === "NotAllowedError") return new Error("workspace_permission_denied|workspace access was denied");
  if (name === "QuotaExceededError") return new Error("workspace_too_large|browser storage quota was exceeded");
  return new Error(`workspace_backend_failed|${error?.message || "OPFS operation failed"}`);
}

export async function mina_opfs_stat(path) {
  try {
    if (!path) return JSON.stringify({ kind: "directory", size: 0 });
    const { directory, name } = await minaParent(path, false);
    try {
      const handle = await directory.getFileHandle(name, { create: false });
      const file = await handle.getFile();
      return JSON.stringify({ kind: "file", size: file.size, modified_at_ms: file.lastModified, revision: `opfs:${file.size}:${file.lastModified}` });
    } catch (error) {
      if (error?.name !== "TypeMismatchError" && error?.name !== "NotFoundError") throw error;
      const handle = await directory.getDirectoryHandle(name, { create: false });
      return JSON.stringify({ kind: "directory", size: 0 });
    }
  } catch (error) { throw minaWrap(error); }
}

export async function mina_opfs_read(path, offset, length) {
  try {
    const { directory, name } = await minaParent(path, false);
    const handle = await directory.getFileHandle(name, { create: false });
    const file = await handle.getFile();
    const end = length < 0 ? file.size : Math.min(file.size, Number(offset) + Number(length));
    return new Uint8Array(await file.slice(Number(offset), end).arrayBuffer());
  } catch (error) { throw minaWrap(error); }
}

export async function mina_opfs_write(path, bytes, mode, createParents) {
  try {
    const { directory, name } = await minaParent(path, createParents);
    if (mode === "create_new") {
      try {
        await directory.getFileHandle(name, { create: false });
        throw new Error("workspace_already_exists|workspace path already exists");
      } catch (error) {
        if (error?.message?.startsWith("workspace_already_exists")) throw error;
        if (error?.name !== "NotFoundError") throw error;
      }
    }
    const handle = await directory.getFileHandle(name, { create: true });
    const keepExistingData = mode === "append";
    const writable = await handle.createWritable({ keepExistingData });
    if (mode === "append") {
      const file = await handle.getFile();
      await writable.seek(file.size);
    }
    await writable.write(bytes);
    await writable.close();
    return mina_opfs_stat(path);
  } catch (error) { throw minaWrap(error); }
}

export async function mina_opfs_list(path) {
  try {
    const directory = await minaDirectory(path, false);
    const entries = [];
    for await (const [name, handle] of directory.entries()) {
      if (handle.kind === "file") {
        const file = await handle.getFile();
        entries.push({ name, kind: "file", size: file.size, revision: `opfs:${file.size}:${file.lastModified}` });
      } else {
        entries.push({ name, kind: "directory", size: 0 });
      }
    }
    entries.sort((a, b) => a.name.localeCompare(b.name));
    return JSON.stringify(entries);
  } catch (error) { throw minaWrap(error); }
}

export async function mina_opfs_create_dir(path, recursive) {
  try {
    if (recursive) {
      await minaDirectory(path, true);
    } else {
      const { directory, name } = await minaParent(path, false);
      await directory.getDirectoryHandle(name, { create: true });
    }
    return mina_opfs_stat(path);
  } catch (error) { throw minaWrap(error); }
}

export async function mina_opfs_remove(path, recursive) {
  try {
    const { directory, name } = await minaParent(path, false);
    await directory.removeEntry(name, { recursive });
  } catch (error) { throw minaWrap(error); }
}

export async function mina_opfs_rename(from, to, overwrite) {
  try {
    const source = await minaParent(from, false);
    let handle;
    try { handle = await source.directory.getFileHandle(source.name, { create: false }); }
    catch (error) {
      if (error?.name !== "TypeMismatchError") throw error;
      handle = await source.directory.getDirectoryHandle(source.name, { create: false });
    }
    const destination = await minaParent(to, false);
    if (!overwrite) {
      for (const getter of ["getFileHandle", "getDirectoryHandle"]) {
        try {
          await destination.directory[getter](destination.name, { create: false });
          throw new Error("workspace_already_exists|workspace destination already exists");
        } catch (error) {
          if (error?.message?.startsWith("workspace_already_exists")) throw error;
          if (error?.name !== "NotFoundError" && error?.name !== "TypeMismatchError") throw error;
        }
      }
    }
    if (typeof handle.move === "function") {
      await handle.move(destination.directory, destination.name);
    } else if (handle.kind === "file") {
      const file = await handle.getFile();
      const target = await destination.directory.getFileHandle(destination.name, { create: true });
      const writable = await target.createWritable();
      await writable.write(await file.arrayBuffer());
      await writable.close();
      await source.directory.removeEntry(source.name);
    } else {
      throw new Error("workspace_unsupported|directory rename requires FileSystemHandle.move support");
    }
    return mina_opfs_stat(to);
  } catch (error) { throw minaWrap(error); }
}
"#)]
extern "C" {
    fn mina_opfs_stat(path: &str) -> Promise;
    fn mina_opfs_read(path: &str, offset: f64, length: f64) -> Promise;
    fn mina_opfs_write(path: &str, bytes: &[u8], mode: &str, create_parents: bool) -> Promise;
    fn mina_opfs_list(path: &str) -> Promise;
    fn mina_opfs_create_dir(path: &str, recursive: bool) -> Promise;
    fn mina_opfs_remove(path: &str, recursive: bool) -> Promise;
    fn mina_opfs_rename(from: &str, to: &str, overwrite: bool) -> Promise;
}

/// Browser workspace backed by the Origin Private File System.
#[derive(Debug, Clone)]
pub struct OpfsWorkspaceFs {
    prefix: WorkspacePath,
}

impl OpfsWorkspaceFs {
    pub fn new(prefix: impl AsRef<str>) -> Result<Self, WorkspaceError> {
        Ok(Self {
            prefix: WorkspacePath::parse(prefix)?,
        })
    }

    fn key(&self, path: &WorkspacePath) -> Result<String, WorkspaceError> {
        if self.prefix.is_root() {
            Ok(path.storage_key().to_owned())
        } else if path.is_root() {
            Ok(self.prefix.storage_key().to_owned())
        } else {
            Ok(self
                .prefix
                .join(path.storage_key())?
                .storage_key()
                .to_owned())
        }
    }
}

impl WorkspaceFs for OpfsWorkspaceFs {
    fn descriptor(&self) -> WorkspaceDescriptor {
        WorkspaceDescriptor {
            identity: format!("workspace:opfs:{}", self.prefix),
            kind: "opfs".into(),
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
            atomic_rename: false,
        }
    }

    fn stat(&self, path: WorkspacePath) -> WorkspaceFuture<'_, FileMetadata> {
        Box::pin(async move {
            let value = JsFuture::from(mina_opfs_stat(&self.key(&path)?))
                .await
                .map_err(map_js_error)?;
            metadata_from_json(path, value)
        })
    }

    fn read(&self, request: ReadRequest) -> WorkspaceFuture<'_, FileContent> {
        Box::pin(async move {
            let metadata = self.stat(request.path.clone()).await?;
            if metadata.kind != FileKind::File {
                return Err(type_error(WorkspaceErrorKind::NotFile));
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
            let value = JsFuture::from(mina_opfs_read(
                &self.key(&request.path)?,
                request.offset as f64,
                request.length.map_or(-1.0, |value| value as f64),
            ))
            .await
            .map_err(map_js_error)?;
            let array = value.dyn_into::<Uint8Array>().map_err(|_| {
                WorkspaceError::new(
                    WorkspaceErrorKind::Backend,
                    "workspace_invalid_result",
                    "OPFS returned an invalid byte payload",
                    false,
                )
            })?;
            Ok(FileContent {
                bytes: array.to_vec(),
                metadata,
            })
        })
    }

    fn write(&self, request: WriteRequest) -> WorkspaceFuture<'_, FileMetadata> {
        Box::pin(async move {
            let mode = match request.mode {
                WriteMode::CreateNew => "create_new",
                WriteMode::Truncate => "truncate",
                WriteMode::Append => "append",
            };
            let value = JsFuture::from(mina_opfs_write(
                &self.key(&request.path)?,
                &request.bytes,
                mode,
                request.create_parents,
            ))
            .await
            .map_err(map_js_error)?;
            metadata_from_json(request.path, value)
        })
    }

    fn list(&self, request: ListRequest) -> WorkspaceFuture<'_, DirectoryPage> {
        Box::pin(async move {
            let value = JsFuture::from(mina_opfs_list(&self.key(&request.path)?))
                .await
                .map_err(map_js_error)?;
            let json = value.as_string().ok_or_else(invalid_result)?;
            let raw: Vec<RawDirectoryEntry> =
                serde_json::from_str(&json).map_err(|_| invalid_result())?;
            let mut entries = raw
                .into_iter()
                .filter(|entry| {
                    request
                        .cursor
                        .as_ref()
                        .is_none_or(|cursor| entry.name > *cursor)
                })
                .map(|entry| {
                    Ok(DirectoryEntry {
                        path: request.path.join(&entry.name)?,
                        kind: entry.kind.into(),
                        size: entry.size,
                        revision: entry.revision,
                    })
                })
                .collect::<Result<Vec<_>, WorkspaceError>>()?;
            let limit = request.limit.clamp(1, 10_000);
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
            let value = JsFuture::from(mina_opfs_create_dir(
                &self.key(&request.path)?,
                request.recursive,
            ))
            .await
            .map_err(map_js_error)?;
            metadata_from_json(request.path, value)
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
            JsFuture::from(mina_opfs_remove(
                &self.key(&request.path)?,
                request.recursive,
            ))
            .await
            .map_err(map_js_error)?;
            Ok(())
        })
    }

    fn rename(&self, request: RenameRequest) -> WorkspaceFuture<'_, FileMetadata> {
        Box::pin(async move {
            let value = JsFuture::from(mina_opfs_rename(
                &self.key(&request.from)?,
                &self.key(&request.to)?,
                request.overwrite,
            ))
            .await
            .map_err(map_js_error)?;
            metadata_from_json(request.to, value)
        })
    }
}

#[derive(Deserialize)]
struct RawMetadata {
    kind: RawFileKind,
    size: u64,
    modified_at_ms: Option<i64>,
    revision: Option<String>,
}

#[derive(Deserialize)]
struct RawDirectoryEntry {
    name: String,
    kind: RawFileKind,
    size: u64,
    revision: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum RawFileKind {
    File,
    Directory,
}

impl From<RawFileKind> for FileKind {
    fn from(value: RawFileKind) -> Self {
        match value {
            RawFileKind::File => Self::File,
            RawFileKind::Directory => Self::Directory,
        }
    }
}

fn metadata_from_json(path: WorkspacePath, value: JsValue) -> Result<FileMetadata, WorkspaceError> {
    let json = value.as_string().ok_or_else(invalid_result)?;
    let raw: RawMetadata = serde_json::from_str(&json).map_err(|_| invalid_result())?;
    Ok(FileMetadata {
        path,
        kind: raw.kind.into(),
        size: raw.size,
        modified_at_ms: raw.modified_at_ms,
        revision: raw.revision,
    })
}

fn map_js_error(error: JsValue) -> WorkspaceError {
    let message = error
        .dyn_ref::<js_sys::Error>()
        .map(|error| error.message().into())
        .or_else(|| error.as_string())
        .unwrap_or_else(|| "OPFS operation failed".into());
    let (code, safe_message) = message
        .split_once('|')
        .unwrap_or(("workspace_backend_failed", "OPFS operation failed"));
    let kind = match code {
        "workspace_not_found" => WorkspaceErrorKind::NotFound,
        "workspace_already_exists" => WorkspaceErrorKind::AlreadyExists,
        "workspace_type_mismatch" => WorkspaceErrorKind::Conflict,
        "workspace_conflict" => WorkspaceErrorKind::Conflict,
        "workspace_permission_denied" => WorkspaceErrorKind::PermissionDenied,
        "workspace_too_large" => WorkspaceErrorKind::TooLarge,
        "workspace_unsupported" => WorkspaceErrorKind::Unsupported,
        "workspace_invalid_path" => WorkspaceErrorKind::InvalidPath,
        _ => WorkspaceErrorKind::Backend,
    };
    WorkspaceError::new(
        kind,
        code,
        safe_message,
        kind == WorkspaceErrorKind::Backend,
    )
}

fn type_error(kind: WorkspaceErrorKind) -> WorkspaceError {
    WorkspaceError::new(
        kind,
        "workspace_type_mismatch",
        "workspace path has the wrong type",
        false,
    )
}

fn invalid_result() -> WorkspaceError {
    WorkspaceError::new(
        WorkspaceErrorKind::Backend,
        "workspace_invalid_result",
        "OPFS returned invalid metadata",
        false,
    )
}
