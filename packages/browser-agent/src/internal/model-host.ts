import { readServerSentEvents } from "./sse.js";
import {
  ResponsesInputError,
  responsesInput,
  responsesStatelessContinuationInput,
} from "./responses-input.js";
import {
  BrowserResponsesContinuationStore,
  type ResponsesContinuation,
} from "./response-continuation-store.js";
import { ResponsesProviderToolTracker } from "./responses-provider-tools.js";
import {
  MessagesReasoningTracker,
  ResponsesReasoningTracker,
} from "./reasoning-events.js";
import {
  browserAttachmentStore,
  isMessagesAttachmentType,
  isSupportedImageType,
} from "./attachment-store.js";
import { messagesAttachmentBlock, responsesAttachmentPart } from "./multimodal-input.js";
import type {
  AgentEffect,
  BrowserModelConfig,
  ModelMessage,
  ModelRequest,
  ToolDefinition,
} from "./protocol.js";

export type PushReducerInput = (input: unknown) => Promise<void>;

type ModelEvent = Record<string, unknown> & { type: string };
type EmitModelEvent = (event: ModelEvent) => Promise<void>;

export class BrowserModelHost {
  private readonly continuations = new BrowserResponsesContinuationStore();
  private responseContinuation: ResponsesContinuation | null = null;

  constructor(private readonly config: BrowserModelConfig) {
    validateConfig(config);
  }

  async invoke(
    effect: AgentEffect,
    pushInput: PushReducerInput,
    runSignal: AbortSignal,
  ): Promise<void> {
    const request = effect.request;
    if (!request) throw new Error("invoke_model effect 缺少 request");

    const controller = new AbortController();
    let timedOut = false;
    const abort = () => controller.abort(runSignal.reason);
    runSignal.addEventListener("abort", abort, { once: true });
    const timer = setTimeout(() => {
      timedOut = true;
      controller.abort(new DOMException("Model timed out", "TimeoutError"));
    }, positiveTimeout(effect.timeout_ms, 300_000));

    const emit: EmitModelEvent = (event) =>
      pushInput({ type: "model_event", effect_id: effect.effect_id, event });

    try {
      if (this.config.protocol === "responses") {
        const continuation = await this.loadContinuation(request.run_id);
        const priorResponse =
          continuation && continuation.effectId !== effect.effect_id ? continuation : null;
        await streamResponses(
          this.config,
          request,
          effect.purpose,
          priorResponse?.history,
          async (responseId, history) => {
            const completed = { effectId: effect.effect_id, responseId, history };
            this.responseContinuation = completed;
            try {
              await this.continuations.save(request.run_id, completed);
            } catch (cause) {
              console.warn("Could not persist Responses continuation", cause);
            }
          },
          emit,
          controller.signal,
        );
      } else {
        await streamMessages(this.config, request, effect.purpose, emit, controller.signal);
      }
    } catch (cause) {
      if (runSignal.aborted) return;
      if (timedOut) {
        await pushInput({ type: "model_timed_out", effect_id: effect.effect_id });
        return;
      }
      const error = normalizeModelError(cause);
      await emit({
        type: "failed",
        error: { kind: error.kind, message: error.message, retryable: error.retryable },
      });
    } finally {
      clearTimeout(timer);
      runSignal.removeEventListener("abort", abort);
    }
  }

  private async loadContinuation(runId: string) {
    if (this.responseContinuation) return this.responseContinuation;
    try {
      this.responseContinuation = await this.continuations.load(runId);
    } catch (cause) {
      console.warn("Could not restore Responses continuation", cause);
    }
    return this.responseContinuation;
  }
}

async function streamResponses(
  config: BrowserModelConfig,
  request: ModelRequest,
  purpose: AgentEffect["purpose"],
  continuationHistory: unknown[] | undefined,
  rememberResponse: (responseId: string, history: unknown[]) => Promise<void>,
  emit: EmitModelEvent,
  signal: AbortSignal,
) {
  const body = await responsesRequest(
    config,
    request,
    purpose,
    continuationHistory,
    signal,
  );
  const response = await fetchResponses(config, body, signal);
  await requireStreamingResponse(response);

  let accepted = false;
  let terminal = false;
  let sawOutput = false;
  let responseId = "";
  const toolCalls = new Map<string, string>();
  const outputItems = new Map<number, unknown>();
  const providerTools = new ResponsesProviderToolTracker();
  const reasoning = new ResponsesReasoningTracker();
  for await (const message of readServerSentEvents(response.body!)) {
    if (!message.data.trim() || message.data.trim() === "[DONE]") continue;
    const value = parseEventJson(message.data, "Responses API 返回了无效的 SSE 事件");
    const eventType = stringAt(value, "type") || message.event;
    if (eventType === "response.output_item.done") {
      const outputIndex = numberAt(value, "output_index");
      const item = at(value, ["item"]);
      if (outputIndex !== undefined && item !== undefined) outputItems.set(outputIndex, item);
    }
    const providerToolEvents = providerTools.push(eventType, value);
    for (const event of providerToolEvents) {
      sawOutput = true;
      await emit(event);
    }
    for (const event of reasoning.push(eventType, value)) {
      sawOutput = true;
      await emit(event);
    }
    switch (eventType) {
      case "response.created":
      case "response.in_progress":
        responseId =
          stringAt(value, "response", "id") || stringAt(value, "response_id") || responseId;
        if (!accepted) {
          accepted = true;
          await emit({
            type: "accepted",
            provider_request_id: responseId || undefined,
          });
        }
        break;
      case "response.output_text.delta": {
        const delta = stringAt(value, "delta");
        if (delta) {
          sawOutput = true;
          await emit({ type: "text_delta", delta });
        }
        break;
      }
      case "response.reasoning_summary_text.delta":
      case "response.reasoning_text.delta": {
        const delta = stringAt(value, "delta");
        if (delta) {
          sawOutput = true;
          await emit({ type: "reasoning_delta", delta });
        }
        break;
      }
      case "response.output_item.added": {
        if (stringAt(value, "item", "type") !== "function_call") break;
        const callId = stringAt(value, "item", "call_id");
        const name = stringAt(value, "item", "name");
        if (!callId || !name) throw protocolError("Responses API 返回了不完整的 function call");
        const itemId = stringAt(value, "item", "id");
        if (itemId) toolCalls.set(itemId, callId);
        sawOutput = true;
        await emit({ type: "tool_call_started", call_id: callId, name });
        break;
      }
      case "response.function_call_arguments.delta": {
        const callId =
          stringAt(value, "call_id") || toolCalls.get(stringAt(value, "item_id")) || "";
        if (!callId) throw protocolError("Responses API 在 function call 之前返回了参数");
        const delta = stringAt(value, "delta");
        if (delta) {
          sawOutput = true;
          await emit({ type: "tool_call_arguments_delta", call_id: callId, delta });
        }
        break;
      }
      case "response.completed":
        responseId = stringAt(value, "response", "id") || responseId;
        if (!accepted) {
          accepted = true;
          await emit({
            type: "accepted",
            provider_request_id: stringAt(value, "response", "id") || undefined,
          });
        }
        if (responseId) {
          await rememberResponse(
            responseId,
            completedResponsesHistory(body, value, outputItems),
          );
        }
        await emitResponsesUsage(value, emit);
        await emit({
          type: "completed",
          finish_reason: toolCalls.size > 0 ? "tool_call" : "stop",
        });
        terminal = true;
        break;
      case "response.incomplete":
        responseId = stringAt(value, "response", "id") || responseId;
        if (responseId) {
          await rememberResponse(
            responseId,
            completedResponsesHistory(body, value, outputItems),
          );
        }
        await emitResponsesUsage(value, emit);
        await emit({
          type: "completed",
          finish_reason:
            stringAt(value, "response", "incomplete_details", "reason") === "max_output_tokens"
              ? "length"
              : "unknown",
        });
        terminal = true;
        break;
      case "response.failed":
      case "error":
        throw new BrowserModelError(
          "upstream_unavailable",
          "Responses API 报告模型请求失败",
          !sawOutput,
        );
    }
    if (terminal) return;
  }
  throw protocolError("Responses API 流在终态事件之前结束");
}

async function fetchResponses(
  config: BrowserModelConfig,
  body: Record<string, unknown>,
  signal: AbortSignal,
) {
  return fetch(providerResourceUrl(config.baseUrl, "responses"), {
    method: "POST",
    headers: compactHeaders({
      "Content-Type": "application/json",
      Authorization: `Bearer ${config.apiKey}`,
      "OpenAI-Organization": config.organization,
      "OpenAI-Project": config.project,
    }),
    body: JSON.stringify(body),
    signal,
  });
}

async function streamMessages(
  config: BrowserModelConfig,
  request: ModelRequest,
  purpose: AgentEffect["purpose"],
  emit: EmitModelEvent,
  signal: AbortSignal,
) {
  const response = await fetch(providerResourceUrl(config.baseUrl, "messages"), {
    method: "POST",
    headers: compactHeaders({
      "Content-Type": "application/json",
      "x-api-key": config.apiKey,
      "anthropic-version": config.anthropicVersion || "2023-06-01",
      "anthropic-dangerous-direct-browser-access": "true",
    }),
    body: JSON.stringify(await messagesRequest(config, request, purpose, signal)),
    signal,
  });
  await requireStreamingResponse(response);

  let accepted = false;
  let sawOutput = false;
  let inputTokens: number | undefined;
  let outputTokens: number | undefined;
  let finishReason = "unknown";
  const toolCalls = new Map<number, string>();
  const reasoning = new MessagesReasoningTracker();
  for await (const message of readServerSentEvents(response.body!)) {
    if (!message.data.trim()) continue;
    const value = parseEventJson(message.data, "Messages API 返回了无效的 SSE 事件");
    const eventType = stringAt(value, "type") || message.event;
    for (const event of reasoning.push(eventType, value)) {
      sawOutput = true;
      await emit(event);
    }
    switch (eventType) {
      case "message_start":
        if (!accepted) {
          accepted = true;
          inputTokens = numberAt(value, "message", "usage", "input_tokens");
          await emit({
            type: "accepted",
            provider_request_id: stringAt(value, "message", "id") || undefined,
          });
        }
        break;
      case "content_block_start": {
        const index = numberAt(value, "index") ?? 0;
        const blockType = stringAt(value, "content_block", "type");
        if (blockType === "tool_use") {
          const callId = stringAt(value, "content_block", "id");
          const name = stringAt(value, "content_block", "name");
          if (!callId || !name) throw protocolError("Messages API 返回了不完整的 tool use");
          toolCalls.set(index, callId);
          sawOutput = true;
          await emit({ type: "tool_call_started", call_id: callId, name });
        } else if (blockType === "text") {
          const delta = stringAt(value, "content_block", "text");
          if (delta) {
            sawOutput = true;
            await emit({ type: "text_delta", delta });
          }
        } else if (blockType === "thinking") {
          const delta = stringAt(value, "content_block", "thinking");
          if (delta) {
            sawOutput = true;
            await emit({ type: "reasoning_delta", delta });
          }
        }
        break;
      }
      case "content_block_delta": {
        const deltaType = stringAt(value, "delta", "type");
        if (deltaType === "text_delta") {
          const delta = stringAt(value, "delta", "text");
          if (delta) {
            sawOutput = true;
            await emit({ type: "text_delta", delta });
          }
        } else if (deltaType === "thinking_delta") {
          const delta = stringAt(value, "delta", "thinking");
          if (delta) {
            sawOutput = true;
            await emit({ type: "reasoning_delta", delta });
          }
        } else if (deltaType === "input_json_delta") {
          const callId = toolCalls.get(numberAt(value, "index") ?? 0);
          if (!callId) throw protocolError("Messages API 在 tool use 之前返回了参数");
          const delta = stringAt(value, "delta", "partial_json");
          if (delta) {
            sawOutput = true;
            await emit({ type: "tool_call_arguments_delta", call_id: callId, delta });
          }
        }
        break;
      }
      case "message_delta":
        outputTokens = numberAt(value, "usage", "output_tokens") ?? outputTokens;
        finishReason = mapMessagesStopReason(stringAt(value, "delta", "stop_reason"));
        break;
      case "message_stop":
        if (inputTokens !== undefined && outputTokens !== undefined) {
          await emit({
            type: "usage",
            usage: {
              input_tokens: inputTokens,
              output_tokens: outputTokens,
              total_tokens: inputTokens + outputTokens,
              source: "provider_reported",
            },
          });
        }
        await emit({ type: "completed", finish_reason: finishReason });
        return;
      case "error":
        throw new BrowserModelError(
          "upstream_unavailable",
          "Messages API 报告模型请求失败",
          !sawOutput,
        );
    }
  }
  throw protocolError("Messages API 流在终态事件之前结束");
}

async function responsesRequest(
  config: BrowserModelConfig,
  request: ModelRequest,
  purpose: AgentEffect["purpose"],
  continuationHistory: unknown[] | undefined,
  signal?: AbortSignal,
) {
  const tools: unknown[] = request.tools.map((tool) => ({
    type: "function",
    name: tool.name,
    description: tool.description,
    parameters: tool.input_schema,
    strict: false,
  }));
  if (config.enableWebSearch && purpose === "agent_step") tools.unshift({ type: "web_search" });
  const input = continuationHistory?.length
    ? responsesStatelessContinuationInput(continuationHistory, request.messages)
    : await responsesInputWithAttachments(request.messages, signal);
  return compactObject({
    model: request.model,
    input,
    tools,
    stream: true,
    max_output_tokens: request.max_output_tokens,
  });
}

async function messagesRequest(
  config: BrowserModelConfig,
  request: ModelRequest,
  purpose: AgentEffect["purpose"],
  signal?: AbortSignal,
) {
  const system: string[] = [];
  const messages: Array<{ role: "user" | "assistant"; content: unknown[] }> = [];
  for (const message of request.messages) {
    if (message.role === "system") {
      if ((message.attachments?.length ?? 0) > 0) {
        throw protocolError("system 消息不能包含附件");
      }
      if (message.content) system.push(message.content);
      continue;
    }
    let role: "user" | "assistant";
    const content: unknown[] = [];
    if (message.role === "tool") {
      if (!message.tool_call_id) throw protocolError("工具结果缺少 tool_call_id");
      role = "user";
      content.push({
        type: "tool_result",
        tool_use_id: message.tool_call_id,
        content: message.content,
      });
    } else {
      role = message.role;
      if (message.content) content.push({ type: "text", text: message.content });
      if (message.role === "user") {
        for (const attachment of message.attachments ?? []) {
          const encoded = await loadEncodedAttachment(attachment, signal);
          if (!isMessagesAttachmentType(encoded.metadata.media_type)) {
            throw new BrowserModelError(
              "invalid_request",
              "Messages API 仅支持 PNG、JPEG、WebP、GIF 图片和 PDF 附件",
              false,
            );
          }
          content.push(messagesAttachmentBlock(encoded, attachment.name));
        }
      } else if ((message.attachments?.length ?? 0) > 0) {
        throw protocolError("只有 user 消息可以包含附件");
      }
      for (const call of message.tool_calls ?? []) {
        let input: unknown;
        try {
          input = JSON.parse(call.arguments);
        } catch {
          throw protocolError("历史工具参数不是有效 JSON");
        }
        content.push({ type: "tool_use", id: call.id, name: call.name, input });
      }
    }
    if (content.length === 0) continue;
    const previous = messages.at(-1);
    if (previous?.role === role) previous.content.push(...content);
    else messages.push({ role, content });
  }

  const tools: unknown[] = request.tools.map(messagesTool);
  if (config.enableWebSearch && purpose === "agent_step") {
    tools.unshift({ type: "web_search_20250305", name: "web_search", max_uses: 5 });
  }
  return {
    model: request.model,
    system: system.join("\n\n"),
    messages,
    tools,
    max_tokens: request.max_output_tokens ?? 4_096,
    stream: true,
  };
}

async function responsesInputWithAttachments(
  messages: ModelMessage[],
  signal?: AbortSignal,
) {
  const input: unknown[] = [];
  for (const message of messages) {
    if (message.role === "tool") {
      input.push(...responsesInput([message]));
      continue;
    }

    const content: unknown[] = [];
    if (message.content) {
      content.push({
        type: message.role === "assistant" ? "output_text" : "input_text",
        text: message.content,
      });
    }
    if ((message.attachments?.length ?? 0) > 0 && message.role !== "user") {
      throw protocolError("只有 user 消息可以包含附件");
    }
    for (const attachment of message.attachments ?? []) {
      const encoded = await loadEncodedAttachment(attachment, signal);
      content.push(responsesAttachmentPart(encoded, attachment.name));
    }
    if (content.length > 0) input.push({ role: message.role, content });
    for (const call of message.tool_calls ?? []) {
      input.push({
        type: "function_call",
        call_id: call.id,
        name: call.name,
        arguments: call.arguments,
      });
    }
  }
  return input;
}

async function loadEncodedAttachment(
  attachment: NonNullable<ModelMessage["attachments"]>[number],
  signal?: AbortSignal,
) {
  try {
    return await browserAttachmentStore.encoded(attachment.blob_id, signal);
  } catch (cause) {
    if (cause instanceof DOMException && cause.name === "AbortError") throw cause;
    throw new BrowserModelError(
      "invalid_request",
      cause instanceof Error ? cause.message : "无法读取浏览器附件",
      false,
    );
  }
}

function messagesTool(tool: ToolDefinition) {
  return {
    name: tool.name,
    description: tool.description,
    input_schema: tool.input_schema,
  };
}

async function emitResponsesUsage(value: unknown, emit: EmitModelEvent) {
  const input = numberAt(value, "response", "usage", "input_tokens");
  const output = numberAt(value, "response", "usage", "output_tokens");
  if (input === undefined || output === undefined) return;
  await emit({
    type: "usage",
    usage: {
      input_tokens: input,
      output_tokens: output,
      total_tokens: numberAt(value, "response", "usage", "total_tokens") ?? input + output,
      source: "provider_reported",
    },
  });
}

function completedResponsesHistory(
  requestBody: Record<string, unknown>,
  value: unknown,
  streamedItems: Map<number, unknown>,
) {
  const input = Array.isArray(requestBody.input) ? requestBody.input : [];
  const output = at(value, ["response", "output"]);
  const completedOutput =
    Array.isArray(output) && output.length > 0
      ? output
      : [...streamedItems.entries()]
          .sort(([left], [right]) => left - right)
          .map(([, item]) => item);
  return [...input, ...completedOutput];
}

function validateConfig(config: BrowserModelConfig) {
  if (!config.model.trim()) throw new Error("请先配置模型名称");
  if (!config.apiKey.trim()) throw new Error("请先配置 API key");
  providerResourceUrl(config.baseUrl, config.protocol);
}

export function providerResourceUrl(baseUrl: string, resource: string) {
  let url: URL;
  try {
    url = new URL(baseUrl);
  } catch {
    throw new Error("模型 Base URL 无效");
  }
  if (url.protocol !== "https:" && !isLocalHttp(url)) {
    throw new Error("模型 Base URL 必须使用 HTTPS（localhost 除外）");
  }
  let path = url.pathname.replace(/\/+$/, "");
  path = path.replace(/\/(?:responses|messages)$/, "");
  const normalizedResource = resource.replace(/^\/+|\/+$/g, "");
  if (!normalizedResource) throw new Error("模型 resource path 无效");
  url.pathname = `${path}/${normalizedResource}`;
  return url.toString();
}

function isLocalHttp(url: URL) {
  return (
    url.protocol === "http:" &&
    (url.hostname === "127.0.0.1" || url.hostname === "localhost" || url.hostname === "[::1]")
  );
}

async function requireStreamingResponse(response: Response) {
  if (!response.ok) {
    const retryable = response.status === 429 || response.status >= 500;
    const kind =
      response.status === 401
        ? "authentication"
        : response.status === 403
          ? "permission_denied"
          : response.status === 429
            ? "rate_limited"
            : response.status >= 500
              ? "upstream_unavailable"
              : "invalid_request";
    const detail = await upstreamErrorMessage(response);
    throw new BrowserModelError(
      kind,
      `模型服务拒绝了请求（HTTP ${response.status}）${detail ? `：${detail}` : ""}`,
      retryable,
    );
  }
  if (!response.body) throw protocolError("模型服务没有返回可读取的流");
}

async function upstreamErrorMessage(response: Response) {
  try {
    const value = await response.clone().json();
    const message = stringAt(value, "error", "message").replace(/\s+/g, " ").trim();
    return Array.from(message).slice(0, 500).join("");
  } catch {
    return "";
  }
}

function parseEventJson(source: string, message: string): unknown {
  try {
    return JSON.parse(source);
  } catch {
    throw protocolError(message);
  }
}

function mapMessagesStopReason(reason: string) {
  if (reason === "end_turn" || reason === "stop_sequence") return "stop";
  if (reason === "max_tokens") return "length";
  if (reason === "tool_use") return "tool_call";
  return "unknown";
}

class BrowserModelError extends Error {
  constructor(
    readonly kind: string,
    message: string,
    readonly retryable: boolean,
  ) {
    super(message);
  }
}

function protocolError(message: string) {
  return new BrowserModelError("protocol_violation", message, false);
}

function normalizeModelError(cause: unknown) {
  if (cause instanceof BrowserModelError) return cause;
  if (cause instanceof ResponsesInputError) return protocolError(cause.message);
  if (cause instanceof DOMException && cause.name === "AbortError") {
    return new BrowserModelError("upstream_unavailable", "模型请求已中断", true);
  }
  if (cause instanceof TypeError) {
    return new BrowserModelError(
      "upstream_unavailable",
      "浏览器无法连接模型服务，请检查网络、Base URL 和服务端 CORS 设置",
      true,
    );
  }
  return new BrowserModelError(
    "internal",
    cause instanceof Error ? cause.message : "浏览器模型 Host 执行失败",
    false,
  );
}

function stringAt(value: unknown, ...path: Array<string | number>) {
  const found = at(value, path);
  return typeof found === "string" ? found : "";
}

function numberAt(value: unknown, ...path: Array<string | number>) {
  const found = at(value, path);
  return typeof found === "number" && Number.isFinite(found) ? found : undefined;
}

function at(value: unknown, path: Array<string | number>): unknown {
  let current = value;
  for (const key of path) {
    if (typeof current !== "object" || current === null) return undefined;
    current = (current as Record<string | number, unknown>)[key];
  }
  return current;
}

function compactHeaders(headers: Record<string, string | undefined>) {
  return Object.fromEntries(
    Object.entries(headers).filter((entry): entry is [string, string] => Boolean(entry[1])),
  );
}

function compactObject(value: Record<string, unknown>) {
  return Object.fromEntries(Object.entries(value).filter(([, entry]) => entry !== undefined));
}

function positiveTimeout(value: number | undefined, fallback: number) {
  return typeof value === "number" && Number.isFinite(value) && value > 0 ? value : fallback;
}
