import { executeJavaScript } from "./javascript-tool.js";
import { JavaScriptError, validateJavaScriptRequest } from "./javascript-contract.js";
import { workspaceUrl } from "./storage-config.js";
import { providerResourceUrl } from "./model-host.js";
import {
  BrowserFileStore,
  FileStoreError,
  type StoredFile,
} from "./opfs-files.js";
import { BrowserSkillStore, BrowserSkillStoreError } from "./opfs-skills.js";
import type {
  BrowserModelConfig,
  PortableToolError,
  SkillLock,
  ToolDefinition,
  ToolInvocationResult,
} from "./protocol.js";

type ToolName =
  | "javascript_eval"
  | "read"
  | "write"
  | "edit"
  | "list_directory"
  | "read_skill_resource"
  | "generate_image"
  | "edit_image";

const retry = { max_attempts: 1, initial_backoff_ms: 100, max_backoff_ms: 1_000 };
const readOnly = {
  idempotency: "read_only",
  concurrency: "parallel_safe",
  completion: "immediate",
  retry,
};
const exclusive = {
  idempotency: "unknown",
  concurrency: "exclusive",
  completion: "immediate",
  retry,
};

const localDefinitions: ToolDefinition[] = [
  {
    name: "javascript_eval",
    description: "Execute a self-contained JavaScript ES module in an isolated QuickJS WASM VM for JSON calculations and data transformations. Export main(input), e.g. export function main(input) { return input.map(x => x * 2); }. Optional input defaults to null; optional export selects a function. Returns value, console logs and usage. Pure async functions are supported. No browser/DOM, fetch, network, filesystem, Node.js, timers or package imports. Use read/write tools separately for files. Limits: 1 second, 16 MiB memory, 64 KiB source, 1 MiB input, 256 KiB result. Each call has fresh globals.",
    input_schema: {
      type: "object",
      properties: {
        source: { type: "string", minLength: 1, description: "ES module exporting main(input)" },
        input: { description: "JSON input passed to the exported function", default: null },
        export: { type: "string", minLength: 1, maxLength: 128, default: "main" },
      },
      required: ["source"],
      additionalProperties: false,
    },
    risk_level: "low",
    execution: { ...readOnly, concurrency: "exclusive" },
  },
  {
    name: "read",
    description:
      "Read one UTF-8 text file inside the browser OPFS workspace. Paths are relative to the workspace root.",
    input_schema: {
      type: "object",
      properties: {
        path: { type: "string", minLength: 1, description: "Workspace-relative file path" },
      },
      required: ["path"],
      additionalProperties: false,
    },
    execution: readOnly,
  },
  {
    name: "write",
    description:
      "Create or replace one UTF-8 text file inside the browser OPFS workspace. The parent directory must already exist.",
    input_schema: {
      type: "object",
      properties: {
        path: { type: "string", minLength: 1, description: "Workspace-relative destination path" },
        content: { type: "string", description: "Complete UTF-8 file content" },
      },
      required: ["path", "content"],
      additionalProperties: false,
    },
    risk_level: "medium",
    execution: exclusive,
  },
  {
    name: "edit",
    description:
      "Replace an exact UTF-8 text fragment in one existing browser OPFS workspace file. By default the old text must occur exactly once.",
    input_schema: {
      type: "object",
      properties: {
        path: { type: "string", minLength: 1, description: "Workspace-relative file path" },
        old_text: { type: "string", minLength: 1, description: "Exact text to replace" },
        new_text: { type: "string", description: "Replacement text" },
        replace_all: {
          type: "boolean",
          default: false,
          description: "Replace every exact occurrence instead of requiring one",
        },
      },
      required: ["path", "old_text", "new_text"],
      additionalProperties: false,
    },
    risk_level: "medium",
    execution: exclusive,
  },
  {
    name: "list_directory",
    description:
      "List direct children of one directory inside the browser OPFS workspace. Paths are relative to the workspace root.",
    input_schema: {
      type: "object",
      properties: {
        path: {
          type: "string",
          description: "Workspace-relative directory path; defaults to the root",
        },
      },
      additionalProperties: false,
    },
    execution: readOnly,
  },
];

const IMAGE_MODEL = "gpt-image-2";
const imageDefinitions: ToolDefinition[] = [
  {
    name: "generate_image",
    description: "Generate an image from a prompt using the configured Images API. Model and output settings are fixed by the SDK. Describe composition, style and background in the prompt. Saves a PNG to an automatically generated workspace path.",
    input_schema: {
      type: "object",
      properties: { prompt: { type: "string", minLength: 1, maxLength: 32000 } },
      required: ["prompt"],
      additionalProperties: false,
    },
    risk_level: "high",
    execution: exclusive,
  },
  {
    name: "edit_image",
    description: "Edit a workspace image using a prompt and optional PNG mask. Uses the same configured Images API and fixed model/settings as generate_image. Saves a new PNG to an automatically generated workspace path.",
    input_schema: {
      type: "object",
      properties: {
        source_path: { type: "string" },
        prompt: { type: "string", minLength: 1, maxLength: 32000 },
        mask_path: { type: "string", description: "Optional PNG mask path in OPFS" },
      },
      required: ["source_path", "prompt"],
      additionalProperties: false,
    },
    risk_level: "high",
    execution: exclusive,
  },
];

const skillResourceDefinition: ToolDefinition = {
  name: "read_skill_resource",
  description:
    "Read one UTF-8 resource from a Skill package locked to the current run. This cannot read the normal workspace or a different Skill version.",
  input_schema: {
    type: "object",
    properties: {
      skill_id: { type: "string", minLength: 1, description: "Active Skill id" },
      path: {
        type: "string",
        minLength: 1,
        description: "Package-relative resource path such as references/guide.md",
      },
    },
    required: ["skill_id", "path"],
    additionalProperties: false,
  },
  execution: readOnly,
};

export class BrowserToolRegistry {
  private readonly files = new BrowserFileStore();
  private readonly skills = new BrowserSkillStore();
  private readonly definitionsByName: Map<string, ToolDefinition>;

  constructor(
    private readonly config: BrowserModelConfig,
    private readonly skillLock?: SkillLock | null,
  ) {
    const definitions = [
      ...localDefinitions,
      ...(skillLock?.skills.length ? [skillResourceDefinition] : []),
      ...(config.protocol === "responses" ? imageDefinitions : []),
    ];
    this.definitionsByName = new Map(definitions.map((definition) => [definition.name, definition]));
  }

  snapshot() {
    const definitions = Array.from(this.definitionsByName.values());
    return {
      revision: 5,
      digest: `browser-tools-v5:${this.config.protocol}:${this.skillLock?.compiled_instruction_digest ?? "no-skills"}`,
      definitions,
      bindings: definitions.map((definition) => ({
        name: definition.name,
        kind: "static",
        argument_visibility: "full",
      })),
    };
  }

  validate(name: string, argumentsValue: unknown): PortableToolError | null {
    if (!this.definitionsByName.has(name)) return toolError("tool_not_found", "浏览器工具未注册", "not_found");
    if (!isRecord(argumentsValue)) return toolError("invalid_tool_arguments", "工具参数必须是 JSON object");
    try {
      validateArguments(name as ToolName, argumentsValue);
      return null;
    } catch (cause) {
      return toPortableError(cause);
    }
  }

  async invoke(
    name: string,
    argumentsValue: unknown,
    signal: AbortSignal,
  ): Promise<ToolInvocationResult> {
    const validation = this.validate(name, argumentsValue);
    if (validation) return { type: "failed", error: validation };
    const args = argumentsValue as Record<string, unknown>;
    try {
      let output: unknown;
      switch (name as ToolName) {
        case "javascript_eval":
          output = await executeJavaScript(args, signal);
          break;
        case "read": {
          const result = await this.files.readText(requiredString(args, "path"), signal);
          output = { path: result.file.path, content: result.content };
          break;
        }
        case "write": {
          const content = boundedString(args, "content", 0, 1024 * 1024);
          const result = await this.files.writeText(
            requiredString(args, "path"),
            content,
            signal,
          );
          output = {
            path: result.file.path,
            bytes_written: new TextEncoder().encode(content).byteLength,
            created: result.created,
            revision: result.file.revision,
          };
          break;
        }
        case "edit": {
          output = await this.editTextFile(args, signal);
          break;
        }
        case "list_directory": {
          const path = optionalString(args, "path") ?? ".";
          const page = await this.files.listDirectory(path, signal);
          output = {
            path,
            entries: page.entries.map((entry) => ({
              path: entry.path,
              kind: entry.kind,
              size: entry.size,
              revision: entry.revision,
            })),
            truncated: page.truncated,
          };
          break;
        }
        case "read_skill_resource": {
          const skillId = requiredString(args, "skill_id");
          const locked = this.skillLock?.skills.find((skill) => skill.skill_id === skillId);
          if (!locked) {
            throw new BrowserToolError(
              "skill_not_active",
              "请求的 Skill 没有锁定到当前 Run",
              "permission_denied",
            );
          }
          output = await this.skills.readText(locked, requiredString(args, "path"), signal);
          break;
        }
        case "generate_image":
          output = fileOutput(await this.generateImage(args, signal));
          break;
        case "edit_image":
          output = fileOutput(await this.editImage(args, signal));
          break;
      }
      return { type: "completed", content: JSON.stringify(output) };
    } catch (cause) {
      return { type: "failed", error: toPortableError(cause) };
    }
  }

  private async editTextFile(args: Record<string, unknown>, signal: AbortSignal) {
    const path = requiredString(args, "path");
    const oldText = boundedString(args, "old_text", 1, 1024 * 1024);
    const newText = boundedString(args, "new_text", 0, 1024 * 1024);
    const replaceAll = optionalBoolean(args, "replace_all") ?? false;
    const current = await this.files.readText(path, signal);
    const occurrences = countOccurrences(current.content, oldText);
    if (occurrences === 0) {
      throw new BrowserToolError("edit_match_not_found", "old_text 不存在于指定文件", "not_found");
    }
    if (occurrences > 1 && !replaceAll) {
      throw new BrowserToolError(
        "edit_match_ambiguous",
        "old_text 出现多次；请提供更多上下文或设置 replace_all",
        "conflict",
      );
    }
    const edited = replaceAll
      ? current.content.replaceAll(oldText, newText)
      : current.content.replace(oldText, newText);
    const result = await this.files.writeText(path, edited, signal);
    return {
      path: result.file.path,
      replacements: replaceAll ? occurrences : 1,
      bytes_written: new TextEncoder().encode(edited).byteLength,
      revision: result.file.revision,
    };
  }

  private async generateImage(args: Record<string, unknown>, signal: AbortSignal) {
    const format = "png";
    const response = await fetch(providerResourceUrl(this.config.baseUrl, "images/generations"), {
      method: "POST",
      headers: imageHeaders(this.config),
      body: JSON.stringify(
        compactObject({
          model: IMAGE_MODEL,
          prompt: requiredString(args, "prompt"),
          size: "auto",
          quality: "auto",
          background: "auto",
          output_format: format,
          n: 1,
        }),
      ),
      signal,
    });
    const image = await readImageResponse(response);
    const path = `images/generated-${crypto.randomUUID()}.${extension(format)}`;
    return this.files.saveBinary(path, image, mediaType(format), signal);
  }

  private async editImage(args: Record<string, unknown>, signal: AbortSignal) {
    const format = "png";
    const source = await this.files.readBinary(requiredString(args, "source_path"), signal);
    const form = new FormData();
    form.append("model", IMAGE_MODEL);
    form.append("prompt", requiredString(args, "prompt"));
    form.append("image[]", source.blob, source.file.name);
    form.append("output_format", format);
    form.append("size", "auto");
    form.append("quality", "auto");
    const maskPath = optionalString(args, "mask_path");
    if (maskPath) {
      const mask = await this.files.readBinary(maskPath, signal);
      form.append("mask", mask.blob, mask.file.name);
    }
    const response = await fetch(providerResourceUrl(this.config.baseUrl, "images/edits"), {
      method: "POST",
      headers: imageHeaders(this.config, false),
      body: form,
      signal,
    });
    const image = await readImageResponse(response);
    const path = `images/edited-${crypto.randomUUID()}.${extension(format)}`;
    return this.files.saveBinary(path, image, mediaType(format), signal);
  }
}

function validateArguments(name: ToolName, args: Record<string, unknown>) {
  switch (name) {
    case "javascript_eval":
      validateJavaScriptRequest(args);
      break;
    case "read":
      assertOnlyKeys(args, ["path"]);
      boundedString(args, "path", 1, 512);
      break;
    case "write":
      assertOnlyKeys(args, ["path", "content"]);
      boundedString(args, "path", 1, 512);
      boundedString(args, "content", 0, 1024 * 1024);
      break;
    case "edit":
      assertOnlyKeys(args, ["path", "old_text", "new_text", "replace_all"]);
      boundedString(args, "path", 1, 512);
      boundedString(args, "old_text", 1, 1024 * 1024);
      boundedString(args, "new_text", 0, 1024 * 1024);
      optionalBoolean(args, "replace_all");
      break;
    case "list_directory":
      assertOnlyKeys(args, ["path"]);
      if (args.path !== undefined) boundedString(args, "path", 1, 512);
      break;
    case "read_skill_resource":
      assertOnlyKeys(args, ["skill_id", "path"]);
      boundedString(args, "skill_id", 1, 128);
      boundedString(args, "path", 1, 512);
      break;
    case "generate_image":
      assertOnlyKeys(args, ["prompt"]);
      boundedString(args, "prompt", 1, 32_000);
      break;
    case "edit_image":
      assertOnlyKeys(args, ["source_path", "prompt", "mask_path"]);
      boundedString(args, "source_path", 1, 512);
      boundedString(args, "prompt", 1, 32_000);
      if (args.mask_path !== undefined) boundedString(args, "mask_path", 1, 512);
      break;
  }
}

async function readImageResponse(response: Response) {
  if (!response.ok) {
    throw new BrowserToolError(
      response.status === 429 ? "image_rate_limited" : "image_request_failed",
      `图片服务拒绝了请求（HTTP ${response.status}）`,
      response.status === 429 ? "resource_exhausted" : "unavailable",
      response.status === 429 || response.status >= 500,
    );
  }
  const payload = (await response.json()) as { data?: Array<{ b64_json?: unknown }> };
  const encoded = payload.data?.[0]?.b64_json;
  if (typeof encoded !== "string" || !encoded) {
    throw new BrowserToolError("invalid_image_response", "图片服务没有返回图片数据", "internal", false);
  }
  return decodeBase64(encoded);
}

function imageHeaders(config: BrowserModelConfig, json = true) {
  return Object.fromEntries(
    Object.entries({
      ...(json ? { "Content-Type": "application/json" } : {}),
      Authorization: `Bearer ${config.apiKey}`,
      "OpenAI-Organization": config.organization,
      "OpenAI-Project": config.project,
    }).filter((entry): entry is [string, string] => Boolean(entry[1])),
  );
}

function decodeBase64(source: string) {
  let binary: string;
  try {
    binary = atob(source);
  } catch {
    throw new BrowserToolError("invalid_image_response", "图片服务返回了无效的 Base64", "internal", false);
  }
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1) bytes[index] = binary.charCodeAt(index);
  return bytes;
}

function fileOutput(file: StoredFile) {
  return {
    path: file.path,
    url: `${workspaceUrl()}/${file.path}`,
    name: file.name,
    media_type: file.mediaType,
    bytes: file.size,
    last_modified_ms: file.lastModified,
  };
}

class BrowserToolError extends Error {
  constructor(
    readonly code: string,
    message: string,
    readonly category: PortableToolError["category"] = "invalid_request",
    readonly retryable = false,
  ) {
    super(message);
  }
}

function toPortableError(cause: unknown): PortableToolError {
  if (cause instanceof JavaScriptError) {
    return toolError(cause.code, cause.message, cause.category);
  }
  if (cause instanceof BrowserToolError) {
    return toolError(cause.code, cause.message, cause.category, cause.retryable);
  }
  if (cause instanceof FileStoreError) {
    return toolError(
      cause.code,
      cause.message,
      fileStoreErrorCategory(cause.code),
      cause.retryable,
    );
  }
  if (cause instanceof BrowserSkillStoreError) {
    return toolError(
      cause.code,
      cause.message,
      skillStoreErrorCategory(cause.code),
      cause.retryable,
    );
  }
  if (cause instanceof DOMException && cause.name === "NotFoundError") {
    return toolError("file_not_found", "OPFS 中找不到指定文件", "not_found");
  }
  if (cause instanceof DOMException && cause.name === "AbortError") {
    return toolError("tool_cancelled", "工具执行已取消", "cancelled");
  }
  if (cause instanceof TypeError) {
    return toolError("browser_fetch_failed", "浏览器无法连接图片服务，请检查网络和 CORS", "unavailable", true);
  }
  return toolError(
    "browser_tool_failed",
    cause instanceof Error ? cause.message : "浏览器工具执行失败",
    "internal",
  );
}

function toolError(
  code: string,
  message: string,
  category: PortableToolError["category"] = "invalid_request",
  retryable = false,
): PortableToolError {
  return { code, message, category, retryable };
}

function requiredString(args: Record<string, unknown>, key: string) {
  return boundedString(args, key, 1, key === "content" ? 2 * 1024 * 1024 : 32_000);
}

function optionalString(args: Record<string, unknown>, key: string) {
  if (args[key] === undefined) return undefined;
  return boundedString(args, key, 1, 32_000);
}

function boundedString(args: Record<string, unknown>, key: string, min: number, max: number) {
  const value = args[key];
  if (typeof value !== "string" || value.length < min || value.length > max) {
    throw new BrowserToolError("invalid_tool_arguments", `${key} 长度必须在 ${min} 到 ${max} 之间`);
  }
  return value;
}

function assertOnlyKeys(args: Record<string, unknown>, allowed: string[]) {
  const unexpected = Object.keys(args).find((key) => !allowed.includes(key));
  if (unexpected) {
    throw new BrowserToolError("invalid_tool_arguments", `不支持工具参数 ${unexpected}`);
  }
}

function optionalBoolean(args: Record<string, unknown>, key: string) {
  const value = args[key];
  if (value === undefined) return undefined;
  if (typeof value !== "boolean") {
    throw new BrowserToolError("invalid_tool_arguments", `${key} 必须是 boolean`);
  }
  return value;
}

function countOccurrences(content: string, search: string) {
  let count = 0;
  let offset = 0;
  while (offset <= content.length - search.length) {
    const index = content.indexOf(search, offset);
    if (index < 0) break;
    count += 1;
    offset = index + search.length;
  }
  return count;
}

function fileStoreErrorCategory(code: string): PortableToolError["category"] {
  if (code.includes("not_found")) return "not_found";
  if (code.includes("too_large")) return "resource_exhausted";
  if (code.includes("conflict") || code.includes("already_exists")) return "conflict";
  if (code.includes("permission") || code.includes("denied")) return "permission_denied";
  if (code.includes("unavailable")) return "unavailable";
  if (code.includes("backend") || code.includes("invalid_result")) return "internal";
  return "invalid_request";
}

function skillStoreErrorCategory(code: string): PortableToolError["category"] {
  if (code.includes("not_found") || code.includes("missing")) return "not_found";
  if (code.includes("too_large")) return "resource_exhausted";
  if (code.includes("permission") || code.includes("locked")) return "permission_denied";
  if (code.includes("conflict") || code.includes("digest")) return "conflict";
  if (code.includes("unavailable")) return "unavailable";
  return "invalid_request";
}

function compactObject(value: Record<string, unknown>) {
  return Object.fromEntries(Object.entries(value).filter(([, entry]) => entry !== undefined));
}

function extension(format: string) {
  return format === "jpeg" ? "jpg" : format;
}

function mediaType(format: string) {
  return `image/${format}`;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
