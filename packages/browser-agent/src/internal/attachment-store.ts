import { storageDirectory, validateNamespace } from "./storage-config.js";
const ATTACHMENTS_DIRECTORY = "attachments";
const METADATA_VERSION = 1;

export const MAX_ATTACHMENT_BYTES = 10 * 1024 * 1024;
export const MAX_ATTACHMENTS_PER_MESSAGE = 8;

export type BrowserAttachmentMetadata = {
  schema_version: 1;
  blob_id: string;
  media_type: string;
  name: string;
  size_bytes: number;
  created_at_ms: number;
};

export type BrowserAttachmentObject = {
  metadata: BrowserAttachmentMetadata;
  blob: Blob;
};

export type EncodedBrowserAttachment = {
  metadata: BrowserAttachmentMetadata;
  base64: string;
  dataUrl: string;
};



export class BrowserAttachmentStore {
  private disposed = false;
  private assertAlive() { if (this.disposed) throw new AttachmentStoreError("disposed", "Attachment store has been disposed"); }
  private readonly displayUrls = new Map<string, string>();
  constructor(private readonly namespace?: string, private readonly ready?: () => Promise<unknown>) { if (namespace) validateNamespace(namespace); }
  dispose() {
    this.disposed = true;
    for (const url of this.displayUrls.values()) URL.revokeObjectURL(url);
    this.displayUrls.clear();
  }
  async put(file: File, signal?: AbortSignal): Promise<BrowserAttachmentMetadata> {
    this.assertAlive();
    await this.ready?.();
    this.assertAlive();
    throwIfAborted(signal);
    if (file.size > MAX_ATTACHMENT_BYTES) {
      throw new AttachmentStoreError("attachment_too_large", "附件不能超过 10 MB");
    }
    const mediaType = inferMediaType(file.name, file.type);
    if (!isSupportedAttachmentType(mediaType)) {
      throw new AttachmentStoreError("unsupported_attachment", "该附件类型暂不受支持");
    }

    const blobId = crypto.randomUUID();
    const metadata: BrowserAttachmentMetadata = {
      schema_version: METADATA_VERSION,
      blob_id: blobId,
      media_type: mediaType,
      name: normalizedName(file.name),
      size_bytes: file.size,
      created_at_ms: Date.now(),
    };
    const directory = await this.directory(true);
    const data = await directory.getFileHandle(dataFilename(blobId), { create: true });
    const metadataFile = await directory.getFileHandle(metadataFilename(blobId), { create: true });
    await writeFile(data, file, signal);
    await writeFile(metadataFile, JSON.stringify(metadata), signal);
    return metadata;
  }

  async get(blobId: string, signal?: AbortSignal): Promise<BrowserAttachmentObject> {
    this.assertAlive();
    await this.ready?.();
    this.assertAlive();
    throwIfAborted(signal);
    requireBlobId(blobId);
    let directory: FileSystemDirectoryHandle;
    try {
      directory = await this.directory(false);
    } catch (cause) {
      if (isNotFound(cause)) throw attachmentNotFound();
      throw cause;
    }

    try {
      const metadataHandle = await directory.getFileHandle(metadataFilename(blobId));
      const parsed = JSON.parse(await (await metadataHandle.getFile()).text()) as unknown;
      const metadata = parseMetadata(parsed, blobId);
      const dataHandle = await directory.getFileHandle(dataFilename(blobId));
      const stored = await dataHandle.getFile();
      if (stored.size !== metadata.size_bytes) {
        throw new AttachmentStoreError("attachment_corrupt", "附件数据与元数据大小不一致");
      }
      throwIfAborted(signal);
      return {
        metadata,
        blob: stored.slice(0, stored.size, metadata.media_type),
      };
    } catch (cause) {
      if (isNotFound(cause)) throw attachmentNotFound();
      throw cause;
    }
  }

  async displayUrl(blobId: string, signal?: AbortSignal) {
    this.assertAlive();
    const cached = this.displayUrls.get(blobId);
    if (cached) return cached;
    const object = await this.get(blobId, signal);
    const objectUrl = URL.createObjectURL(object.blob);
    const displayUrl = `${objectUrl}#mina-attachment=${encodeURIComponent(blobId)}`;
    this.displayUrls.set(blobId, displayUrl);
    return displayUrl;
  }

  async encoded(blobId: string, signal?: AbortSignal): Promise<EncodedBrowserAttachment> {
    const object = await this.get(blobId, signal);
    const bytes = new Uint8Array(await object.blob.arrayBuffer());
    throwIfAborted(signal);
    const base64 = encodeBase64(bytes);
    return {
      metadata: object.metadata,
      base64,
      dataUrl: `data:${object.metadata.media_type};base64,${base64}`,
    };
  }

  private async directory(create: boolean) {
    if (!navigator.storage?.getDirectory) {
      throw new AttachmentStoreError("opfs_unavailable", "当前浏览器不支持 OPFS 附件存储");
    }
    const root = await navigator.storage.getDirectory();
    const app = await root.getDirectoryHandle(this.namespace ?? storageDirectory(), { create });
    return app.getDirectoryHandle(ATTACHMENTS_DIRECTORY, { create });
  }
}

export class AttachmentStoreError extends Error {
  readonly code: string;

  constructor(code: string, message: string) {
    super(message);
    this.code = code;
  }
}

export function stableAttachmentUrl(blobId: string) {
  requireBlobId(blobId);
  return `mina-attachment://local/${blobId}`;
}

export function attachmentIdFromUrl(url: string | undefined) {
  if (!url) return null;
  try {
    const parsed = new URL(url);
    if (parsed.protocol === "mina-attachment:" && parsed.hostname === "local") {
      const candidate = decodeURIComponent(parsed.pathname.replace(/^\/+/, ""));
      return isBlobId(candidate) ? candidate : null;
    }
    const candidate = new URLSearchParams(parsed.hash.replace(/^#/, "")).get("mina-attachment");
    return candidate && isBlobId(candidate) ? candidate : null;
  } catch {
    return null;
  }
}

export function inferMediaType(name: string, declared: string) {
  const normalized = declared.trim().toLowerCase().split(";", 1)[0] ?? "";
  if (normalized && normalized !== "application/octet-stream") return normalized;
  const extension = name.toLowerCase().split(".").at(-1);
  return MEDIA_TYPES_BY_EXTENSION[extension ?? ""] ?? "application/octet-stream";
}

export function isMessagesAttachmentType(mediaType: string) {
  return isSupportedImageType(mediaType) || mediaType === "application/pdf";
}

export function isSupportedImageType(mediaType: string) {
  return ["image/png", "image/jpeg", "image/webp", "image/gif"].includes(mediaType);
}

function isSupportedAttachmentType(mediaType: string) {
  return isSupportedImageType(mediaType) || SUPPORTED_DOCUMENT_TYPES.has(mediaType);
}

function parseMetadata(value: unknown, expectedBlobId: string): BrowserAttachmentMetadata {
  if (
    !isRecord(value) ||
    value.schema_version !== METADATA_VERSION ||
    value.blob_id !== expectedBlobId ||
    typeof value.media_type !== "string" ||
    !isSupportedAttachmentType(value.media_type) ||
    typeof value.name !== "string" ||
    typeof value.size_bytes !== "number" ||
    !Number.isSafeInteger(value.size_bytes) ||
    value.size_bytes < 0 ||
    value.size_bytes > MAX_ATTACHMENT_BYTES ||
    typeof value.created_at_ms !== "number"
  ) {
    throw new AttachmentStoreError("attachment_corrupt", "附件元数据无效");
  }
  return value as BrowserAttachmentMetadata;
}

async function writeFile(
  handle: FileSystemFileHandle,
  contents: FileSystemWriteChunkType,
  signal?: AbortSignal,
) {
  throwIfAborted(signal);
  const writable = await handle.createWritable();
  try {
    await writable.write(contents);
    throwIfAborted(signal);
    await writable.close();
  } catch (cause) {
    await writable.abort().catch(() => undefined);
    throw cause;
  }
}

function encodeBase64(bytes: Uint8Array) {
  let binary = "";
  const chunkSize = 32_768;
  for (let offset = 0; offset < bytes.length; offset += chunkSize) {
    binary += String.fromCharCode(...bytes.subarray(offset, offset + chunkSize));
  }
  return btoa(binary);
}

function normalizedName(name: string) {
  const normalized = name.trim().replace(/[\u0000-\u001f/\\]/g, "_").slice(0, 240);
  return normalized || "attachment";
}

function requireBlobId(blobId: string) {
  if (!isBlobId(blobId)) throw new AttachmentStoreError("invalid_blob_id", "附件标识无效");
}

function isBlobId(value: string) {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
    value,
  );
}

function dataFilename(blobId: string) {
  return `${blobId}.blob`;
}

function metadataFilename(blobId: string) {
  return `${blobId}.json`;
}

function attachmentNotFound() {
  return new AttachmentStoreError("attachment_not_found", "OPFS 中找不到这个附件");
}

function throwIfAborted(signal?: AbortSignal) {
  if (signal?.aborted) throw signal.reason ?? new DOMException("Aborted", "AbortError");
}

function isNotFound(cause: unknown) {
  return cause instanceof DOMException && cause.name === "NotFoundError";
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

const SUPPORTED_DOCUMENT_TYPES = new Set([
  "application/pdf",
  "text/plain",
  "text/markdown",
  "text/csv",
  "application/json",
  "application/msword",
  "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
  "application/vnd.ms-powerpoint",
  "application/vnd.openxmlformats-officedocument.presentationml.presentation",
  "application/vnd.ms-excel",
  "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
]);

const MEDIA_TYPES_BY_EXTENSION: Record<string, string> = {
  png: "image/png",
  jpg: "image/jpeg",
  jpeg: "image/jpeg",
  webp: "image/webp",
  gif: "image/gif",
  pdf: "application/pdf",
  txt: "text/plain",
  md: "text/markdown",
  markdown: "text/markdown",
  csv: "text/csv",
  json: "application/json",
  doc: "application/msword",
  docx: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
  ppt: "application/vnd.ms-powerpoint",
  pptx: "application/vnd.openxmlformats-officedocument.presentationml.presentation",
  xls: "application/vnd.ms-excel",
  xlsx: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
};

export const browserAttachmentStore = new BrowserAttachmentStore();
