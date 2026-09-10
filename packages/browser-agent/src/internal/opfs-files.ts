import {
  workspaceCapabilitiesJson,
  workspaceCreateDirectoryJson,
  workspaceDescriptorJson,
  workspaceListJson,
  workspaceReadBytes,
  workspaceRenameJson,
  workspaceRemove,
  workspaceStatJson,
  workspaceWriteJson,
} from "../pkg/mina_wasm_agent.js";

import { workspacePrefix, workspaceUrl } from "./storage-config.js";

const MAX_TEXT_BYTES = 1024 * 1024;
const MAX_BINARY_BYTES = 25 * 1024 * 1024;
const MAX_DOWNLOAD_BYTES = 64 * 1024 * 1024;
const MAX_DEBUG_ENTRIES = 500;

export type WorkspaceEntry = {
  path: string;
  name: string;
  kind: "file" | "directory";
  size: number;
  revision?: string;
};

export type StoredFile = WorkspaceEntry & {
  kind: "file";
  mediaType: string;
  lastModified: number;
};

type RawMetadata = {
  path: string;
  kind: "file" | "directory";
  size: number;
  modified_at_ms?: number;
  revision?: string;
};

type RawDirectoryPage = {
  entries: RawMetadata[];
  next_cursor?: string;
};

export class BrowserFileStore {
  descriptor() {
    return parseJson<{
      identity: string;
      kind: string;
      version: string;
      persistent: boolean;
      shared: boolean;
    }>(workspaceDescriptorJson(workspacePrefix()));
  }

  capabilities() {
    return parseJson<{
      readable: boolean;
      writable: boolean;
      directories: boolean;
      range_read: boolean;
      append: boolean;
      atomic_rename: boolean;
    }>(workspaceCapabilitiesJson(workspacePrefix()));
  }

  async ensureRoot(signal?: AbortSignal) {
    throwIfAborted(signal);
    await workspaceCall(() =>
      workspaceCreateDirectoryJson(workspacePrefix(), ".", true),
    );
    throwIfAborted(signal);
  }

  async stat(path: string, signal?: AbortSignal): Promise<WorkspaceEntry> {
    throwIfAborted(signal);
    const normalized = workspacePath(path, true);
    const source = await workspaceCall(() =>
      workspaceStatJson(workspacePrefix(), normalized),
    );
    throwIfAborted(signal);
    return entryFromRaw(parseJson<RawMetadata>(source));
  }

  async writeText(
    path: string,
    content: string,
    signal?: AbortSignal,
  ): Promise<{ file: StoredFile; created: boolean }> {
    throwIfAborted(signal);
    const bytes = new TextEncoder().encode(content);
    if (bytes.byteLength > MAX_TEXT_BYTES) {
      throw new FileStoreError("workspace_write_too_large", "文本文件不能超过 1 MiB");
    }
    const normalized = workspacePath(path);
    await this.ensureRoot(signal);
    const created = !(await this.exists(normalized, signal));
    const source = await workspaceCall(() =>
      workspaceWriteJson(workspacePrefix(), normalized, bytes, "truncate", false),
    );
    throwIfAborted(signal);
    return { file: storedFileFromRaw(parseJson<RawMetadata>(source)), created };
  }

  async saveBinary(
    path: string,
    data: Uint8Array,
    mediaType: string,
    signal?: AbortSignal,
  ): Promise<StoredFile> {
    throwIfAborted(signal);
    if (data.byteLength > MAX_BINARY_BYTES) {
      throw new FileStoreError("workspace_write_too_large", "图片文件不能超过 25 MiB");
    }
    const normalized = imagePath(path);
    await this.ensureRoot(signal);
    const unique = await this.availablePath(normalized, signal);
    const source = await workspaceCall(() =>
      workspaceWriteJson(workspacePrefix(), unique, data, "create_new", true),
    );
    throwIfAborted(signal);
    return {
      ...storedFileFromRaw(parseJson<RawMetadata>(source)),
      mediaType,
    };
  }

  async readText(path: string, signal?: AbortSignal) {
    const normalized = workspacePath(path);
    const bytes = await this.readBytes(normalized, MAX_TEXT_BYTES, signal);
    let content: string;
    try {
      content = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
    } catch {
      throw new FileStoreError("file_not_utf8", "文件不是有效的 UTF-8 文本");
    }
    const metadata = await this.stat(normalized, signal);
    if (metadata.kind !== "file") {
      throw new FileStoreError("workspace_not_file", "请求的路径不是文件");
    }
    return { file: storedFileFromEntry(metadata), content };
  }

  async readBinary(path: string, signal?: AbortSignal) {
    const normalized = imagePath(path);
    const bytes = await this.readBytes(normalized, MAX_BINARY_BYTES, signal);
    const metadata = await this.stat(normalized, signal);
    if (metadata.kind !== "file") {
      throw new FileStoreError("workspace_not_file", "请求的路径不是文件");
    }
    const mediaType = mediaTypeForPath(normalized);
    const copy = new Uint8Array(bytes.byteLength);
    copy.set(bytes);
    return {
      file: { ...storedFileFromEntry(metadata), mediaType },
      blob: new Blob([copy.buffer], { type: mediaType }),
    };
  }

  async readDownload(path: string, signal?: AbortSignal) {
    const normalized = workspacePath(path);
    const metadata = await this.stat(normalized, signal);
    if (metadata.kind !== "file") {
      throw new FileStoreError("workspace_not_file", "请求的路径不是文件");
    }
    if (metadata.size > MAX_DOWNLOAD_BYTES) {
      throw new FileStoreError(
        "workspace_download_too_large",
        "浏览器单次下载的文件不能超过 64 MiB",
      );
    }
    const bytes = await this.readBytes(normalized, MAX_DOWNLOAD_BYTES, signal);
    const copy = new Uint8Array(bytes.byteLength);
    copy.set(bytes);
    return {
      file: storedFileFromEntry(metadata),
      bytes: copy,
    };
  }

  async listDirectory(path = ".", signal?: AbortSignal) {
    throwIfAborted(signal);
    const normalized = workspacePath(path, true);
    await this.ensureRoot(signal);
    const source = await workspaceCall(() =>
      workspaceListJson(workspacePrefix(), normalized, undefined, MAX_DEBUG_ENTRIES),
    );
    throwIfAborted(signal);
    const page = parseJson<RawDirectoryPage>(source);
    return {
      entries: page.entries.map(entryFromRaw),
      truncated: Boolean(page.next_cursor),
    };
  }

  async debugSnapshot(signal?: AbortSignal) {
    await this.ensureRoot(signal);
    const entries: WorkspaceEntry[] = [];
    const pending = ["."];
    let truncated = false;
    while (pending.length > 0 && entries.length < MAX_DEBUG_ENTRIES) {
      throwIfAborted(signal);
      const path = pending.shift()!;
      const page = await this.listDirectory(path, signal);
      truncated ||= page.truncated;
      for (const entry of page.entries) {
        if (entries.length >= MAX_DEBUG_ENTRIES) {
          truncated = true;
          break;
        }
        entries.push(entry);
        if (entry.kind === "directory") pending.push(entry.path);
      }
    }
    if (pending.length > 0) truncated = true;
    return {
      descriptor: this.descriptor(),
      capabilities: this.capabilities(),
      root: workspaceUrl(),
      entries: entries.sort((left, right) => left.path.localeCompare(right.path)),
      truncated,
    };
  }

  async createDirectory(path: string, recursive = false, signal?: AbortSignal) {
    throwIfAborted(signal);
    const normalized = workspacePath(path);
    const source = await workspaceCall(() =>
      workspaceCreateDirectoryJson(workspacePrefix(), normalized, recursive),
    );
    throwIfAborted(signal);
    return entryFromRaw(parseJson<RawMetadata>(source));
  }

  async remove(path: string, recursive = false, signal?: AbortSignal) {
    throwIfAborted(signal);
    await workspaceCall(() =>
      workspaceRemove(workspacePrefix(), workspacePath(path), recursive),
    );
    throwIfAborted(signal);
  }

  async rename(from: string, to: string, overwrite = false, signal?: AbortSignal) {
    throwIfAborted(signal);
    const source = await workspaceCall(() =>
      workspaceRenameJson(
        workspacePrefix(),
        workspacePath(from),
        workspacePath(to),
        overwrite,
      ),
    );
    throwIfAborted(signal);
    return entryFromRaw(parseJson<RawMetadata>(source));
  }

  private async readBytes(path: string, maxBytes: number, signal?: AbortSignal) {
    throwIfAborted(signal);
    const bytes = await workspaceCall(() =>
      workspaceReadBytes(workspacePrefix(), path, 0, -1, maxBytes),
    );
    throwIfAborted(signal);
    return bytes;
  }

  private async exists(path: string, signal?: AbortSignal) {
    try {
      await this.stat(path, signal);
      return true;
    } catch (cause) {
      if (cause instanceof FileStoreError && cause.code.includes("not_found")) return false;
      throw cause;
    }
  }

  private async availablePath(path: string, signal?: AbortSignal) {
    const slash = path.lastIndexOf("/");
    const directory = slash >= 0 ? path.slice(0, slash + 1) : "";
    const filename = slash >= 0 ? path.slice(slash + 1) : path;
    const dot = filename.lastIndexOf(".");
    const stem = dot > 0 ? filename.slice(0, dot) : filename;
    const extension = dot > 0 ? filename.slice(dot) : "";
    for (let suffix = 0; suffix < 10_000; suffix += 1) {
      const candidate = `${directory}${suffix === 0 ? filename : `${stem}-${suffix}${extension}`}`;
      if (!(await this.exists(candidate, signal))) return candidate;
    }
    throw new FileStoreError("workspace_conflict", "无法为文件生成唯一名称");
  }
}

export class FileStoreError extends Error {
  constructor(
    readonly code: string,
    message: string,
    readonly retryable = false,
  ) {
    super(message);
  }
}

function entryFromRaw(value: RawMetadata): WorkspaceEntry {
  const path = value.path || ".";
  return {
    path,
    name: path === "." ? "files" : path.split("/").at(-1)!,
    kind: value.kind,
    size: value.size,
    ...(value.revision ? { revision: value.revision } : {}),
  };
}

function storedFileFromRaw(value: RawMetadata): StoredFile {
  return storedFileFromEntry(
    {
      ...entryFromRaw(value),
      kind: "file",
    },
    value.modified_at_ms,
  );
}

function storedFileFromEntry(entry: WorkspaceEntry, modifiedAt = Date.now()): StoredFile {
  return {
    ...entry,
    kind: "file",
    mediaType: mediaTypeForPath(entry.path),
    lastModified: modifiedAt,
  };
}

function workspacePath(path: string, allowRoot = false) {
  const normalized = path.trim().replaceAll("\\", "/").replace(/^\/+/, "");
  if ((normalized === "" || normalized === ".") && allowRoot) return ".";
  const segments = normalized.split("/");
  if (
    !normalized ||
    normalized.length > 512 ||
    segments.length > 20 ||
    segments.some(
      (segment) =>
        !segment ||
        segment === "." ||
        segment === ".." ||
        segment.length > 120 ||
        /[\u0000-\u001f]/.test(segment),
    )
  ) {
    throw new FileStoreError("workspace_invalid_path", "文件路径无效");
  }
  return normalized;
}

function imagePath(path: string) {
  const normalized = workspacePath(path);
  if (!/\.(?:png|jpe?g|webp)$/i.test(normalized)) {
    throw new FileStoreError(
      "invalid_file_type",
      "图片路径必须以 .png、.jpg、.jpeg 或 .webp 结尾",
    );
  }
  return normalized;
}

function mediaTypeForPath(path: string) {
  if (/\.txt$/i.test(path)) return "text/plain";
  if (/\.html?$/i.test(path)) return "text/html";
  if (/\.(?:md|markdown)$/i.test(path)) return "text/markdown";
  if (/\.json$/i.test(path)) return "application/json";
  if (/\.css$/i.test(path)) return "text/css";
  if (/\.(?:js|mjs|cjs)$/i.test(path)) return "text/javascript";
  if (/\.(?:ts|tsx)$/i.test(path)) return "text/typescript";
  if (/\.csv$/i.test(path)) return "text/csv";
  if (/\.svg$/i.test(path)) return "image/svg+xml";
  if (/\.png$/i.test(path)) return "image/png";
  if (/\.webp$/i.test(path)) return "image/webp";
  if (/\.gif$/i.test(path)) return "image/gif";
  if (/\.jpe?g$/i.test(path)) return "image/jpeg";
  if (/\.pdf$/i.test(path)) return "application/pdf";
  if (/\.zip$/i.test(path)) return "application/zip";
  if (/\.docx$/i.test(path)) {
    return "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
  }
  if (/\.pptx$/i.test(path)) {
    return "application/vnd.openxmlformats-officedocument.presentationml.presentation";
  }
  if (/\.xlsx$/i.test(path)) {
    return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
  }
  return "application/octet-stream";
}

async function workspaceCall<T>(operation: () => T | Promise<T>): Promise<T> {
  try {
    return await operation();
  } catch (cause) {
    if (cause instanceof FileStoreError) throw cause;
    const source = typeof cause === "string" ? cause : cause instanceof Error ? cause.message : "";
    try {
      const parsed = JSON.parse(source) as {
        code?: unknown;
        message?: unknown;
        retryable?: unknown;
      };
      if (typeof parsed.code === "string" && typeof parsed.message === "string") {
        throw new FileStoreError(parsed.code, parsed.message, parsed.retryable === true);
      }
    } catch (parsedCause) {
      if (parsedCause instanceof FileStoreError) throw parsedCause;
    }
    throw new FileStoreError("workspace_backend_failed", source || "OPFS 文件操作失败", true);
  }
}

function parseJson<T>(source: string): T {
  try {
    return JSON.parse(source) as T;
  } catch {
    throw new FileStoreError("workspace_invalid_result", "WASM FS 返回了无效数据");
  }
}

function throwIfAborted(signal?: AbortSignal) {
  if (signal?.aborted) throw signal.reason ?? new DOMException("Aborted", "AbortError");
}
