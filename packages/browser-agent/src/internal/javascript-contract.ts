import type { PortableToolError } from "./protocol.js";

export const JAVASCRIPT_LIMITS = Object.freeze({
  sourceBytes: 64 * 1024, inputBytes: 1024 * 1024, outputBytes: 256 * 1024,
  memoryBytes: 16 * 1024 * 1024, stackBytes: 512 * 1024,
  timeoutMs: 1000, logBytes: 16 * 1024, logCount: 100,
});
export type JavaScriptRequest = { source: string; input?: unknown; export?: string };
export type JavaScriptOutput = {
  value: unknown;
  logs: Array<{ level: string; text: string }>;
  logs_truncated: boolean;
  usage: { duration_ms: number; output_bytes: number };
};
export class JavaScriptError extends Error {
  constructor(readonly code: string, message: string, readonly category: PortableToolError["category"] = "invalid_request") {
    super(message); this.name = "JavaScriptError";
  }
}
export function validateJavaScriptRequest(value: unknown): JavaScriptRequest {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new JavaScriptError("javascript_invalid_arguments", "Expected a JavaScript tool request");
  const args = value as Record<string, unknown>;
  if (Object.keys(args).some((key) => !["source", "input", "export"].includes(key))) throw new JavaScriptError("javascript_invalid_arguments", "Only source, input and export are supported");
  if (typeof args.source !== "string" || !args.source.trim()) throw new JavaScriptError("javascript_invalid_source", "source must be a non-empty ES module");
  if (bytes(args.source) > JAVASCRIPT_LIMITS.sourceBytes) throw new JavaScriptError("javascript_source_too_large", "JavaScript source exceeds 64 KiB", "resource_exhausted");
  if (args.export !== undefined && (typeof args.export !== "string" || !args.export.length || args.export.length > 128)) throw new JavaScriptError("javascript_invalid_export", "export must be a function name of 1–128 characters");
  let input: string | undefined;
  try { input = JSON.stringify(args.input ?? null); } catch { throw new JavaScriptError("javascript_invalid_input", "input must be JSON serializable"); }
  if (input === undefined) throw new JavaScriptError("javascript_invalid_input", "input must be JSON serializable");
  if (bytes(input) > JAVASCRIPT_LIMITS.inputBytes) throw new JavaScriptError("javascript_input_too_large", "JavaScript input exceeds 1 MiB", "resource_exhausted");
  return { source: args.source, input: JSON.parse(input), export: args.export as string | undefined ?? "main" };
}
export function bytes(value: string) { return new TextEncoder().encode(value).byteLength; }
