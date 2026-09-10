import {
  TransportError,
  getMessageText,
  type ChatEvent,
  type FilePart,
  type ChatMessage,
  type ChatTransport,
  type SendRequest,
} from "@xinjiyuan97/chat-core";

import type {
  AgentEvent,
  AgentTransition,
  BrowserModelConfig,
  ModelAttachment,
} from "@mina/browser-agent";
import { attachmentIdFromUrl, inferMediaType } from "@mina/browser-agent";
import { updateSessionTitle } from "./session-store";
import { agent, wasmRuntime } from "./runtime";

type Usage = {
  input_tokens: number;
  output_tokens: number;
  total_tokens: number;
};

export function createWasmTransport(options: {
  sessionId: string;
  activeRunId?: string;
  modelConfig: BrowserModelConfig;
  onRunStarted?: (runId: string) => void;
  onRunFinished?: (sessionId: string) => void;
}): ChatTransport {
  return {
    async *send(request, context) {
      const resume = getResumeRun(request);
      const current = resume ? { input: "", attachments: [] } : latestUserInput(request);
      let activeOutput: "reasoning" | "text" | null = null;
      let usage: Usage | undefined;
      let runId: string | undefined;
      let finished = false;
      let suspended = false;

      function closeActiveOutput() {
        const events: ChatEvent[] = [];
        if (activeOutput === "reasoning") events.push({ type: "reasoning-end" });
        if (activeOutput === "text") events.push({ type: "text-end" });
        activeOutput = null;
        return events;
      }

      function mapEvent(event: AgentEvent): ChatEvent[] {
        switch (event.type) {
          case "reasoning_started": {
            if (activeOutput === "reasoning") return [];
            const events = closeActiveOutput();
            events.push({ type: "reasoning-start", redacted: event.redacted === true });
            activeOutput = "reasoning";
            return events;
          }

          case "reasoning_completed":
            if (activeOutput !== "reasoning") return [];
            activeOutput = null;
            return [{ type: "reasoning-end", redacted: event.redacted === true }];

          case "output_delta": {
            const delta = stringField(event, "delta");
            if (!delta) return [];
            if (event.channel === "assistant_reasoning") {
              const events = activeOutput === "reasoning" ? [] : closeActiveOutput();
              if (activeOutput !== "reasoning") {
                events.push({ type: "reasoning-start" });
                activeOutput = "reasoning";
              }
              events.push({ type: "reasoning-delta", delta });
              return events;
            }
            if (event.channel === "assistant_text") {
              const events = activeOutput === "text" ? [] : closeActiveOutput();
              if (activeOutput !== "text") {
                events.push({ type: "text-start" });
                activeOutput = "text";
              }
              events.push({ type: "text-delta", delta });
              return events;
            }
            return [];
          }

          case "usage_updated":
            if (isUsage(event.usage)) usage = event.usage;
            return [];

          case "tool_call_started":
            return [
              ...closeActiveOutput(),
              {
                type: "tool-input-start",
                toolCallId: stringField(event, "call_id"),
                name: stringField(event, "name"),
              },
            ];

          case "tool_call_arguments_delta":
            return [
              {
                type: "tool-input-delta",
                toolCallId: stringField(event, "call_id"),
                delta: stringField(event, "delta"),
              },
            ];

          case "tool_execution_started":
            return [
              {
                type: "tool-input-available",
                toolCallId: stringField(event, "call_id"),
                input: event.arguments,
              },
              { type: "tool-executing", toolCallId: stringField(event, "call_id") },
            ];

          case "tool_execution_completed":
            return [
              {
                type: "tool-output",
                toolCallId: stringField(event, "call_id"),
                output: parseOutput(stringField(event, "output")),
              },
            ];

          case "tool_execution_failed":
            return [
              {
                type: "tool-error",
                toolCallId: stringField(event, "call_id"),
                error: stringField(event, "message") || "工具执行失败",
              },
            ];

          case "approval_requested":
            return [
              {
                type: "permission-request",
                request: {
                  id: stringField(event, "approval_id"),
                  toolName: stringField(event, "tool_name"),
                  toolCallId: stringField(event, "call_id"),
                  title: `允许 ${stringField(event, "tool_name")} 执行？`,
                  detail: JSON.stringify(event.arguments, null, 2),
                  detailLanguage: "json",
                  risk: permissionRisk(event.risk_level),
                  options: [
                    { value: "allow-once", decision: "allow-once" },
                    { value: "deny", decision: "deny", promptForReason: true },
                  ],
                  metadata: { runId },
                },
              },
            ];

          case "approval_resolved": {
            const resolution = event.resolution;
            const decision = resolution.decision === "allow-once" ? "allow-once" : "deny";
            return [
              {
                type: "permission-resolved",
                requestId: stringField(event, "approval_id"),
                resolution: {
                  requestId: stringField(event, "approval_id"),
                  option: decision,
                  decision,
                  reason: typeof resolution.reason === "string" ? resolution.reason : undefined,
                },
              },
            ];
          }

          case "completed":
            finished = true;
            return [
              ...closeActiveOutput(),
              {
                type: "message-end",
                finishReason: stringField(event, "finish_reason") || "stop",
                usage: usage
                  ? {
                      inputTokens: usage.input_tokens,
                      outputTokens: usage.output_tokens,
                      totalTokens: usage.total_tokens,
                    }
                  : undefined,
              },
            ];

          case "cancelled":
            finished = true;
            return [
              ...closeActiveOutput(),
              { type: "message-end", finishReason: "cancelled" },
            ];

          case "failed":
            finished = true;
            throw new TransportError(stringField(event, "message") || "WASM Agent 执行失败");

          default:
            return [];
        }
      }

      try {
        for await (const message of wasmRuntime.run(
          {
            sessionId: options.sessionId,
            input: current.input,
            attachments: current.attachments,
            restoreRunId: resume?.runId,
            modelConfig: options.modelConfig,
          },
          context.signal,
        )) {
          if (message.type === "run_started") {
            runId = message.runId;
            const event: ChatEvent = { type: "message-start" };
            options.onRunStarted?.(runId);
            yield event;
            continue;
          }

          const transitionEvents = message.transition.events.flatMap(mapEvent);
          for (const event of transitionEvents) {
            yield event;
          }

          suspended = message.transition.outcome.status === "suspended";
          if (!finished && isTerminal(message.transition)) {
            const terminalEvents = terminalEventsFor(message.transition, closeActiveOutput, usage);
            finished = true;
            for (const event of terminalEvents) {
                yield event;
            }
          }
        }
      } finally {
        if (runId) {
          const session = await agent.sessions.get(options.sessionId);
          if (!resume && isFirstUserTurn(request.messages)) {
            const firstUser = request.messages.find((message) => message.role === "user");
            if (firstUser) {
              try {
                const generated = await wasmRuntime.generateTitle({
                  sessionId: options.sessionId,
                  firstMessageId: crypto.randomUUID(),
                  expectedRevision: session.revision,
                  sourceText: titleSource(firstUser),
                  modelConfig: options.modelConfig,
                });
                await updateSessionTitle(
                  options.sessionId,
                  generated.title,
                  generated.expectedRevision,
                );
              } catch (error) {
                console.warn("WASM title generation failed", error);
              }
            }
          }
          options.onRunFinished?.(options.sessionId);
        }
      }

      if (!finished && !suspended && !context.signal.aborted) {
        throw new TransportError("WASM Agent 在终态之前结束");
      }
    },
  };
}

function latestUserInput(request: SendRequest) {
  const latest = [...request.messages].reverse().find((message) => message.role === "user");
  const input = latest ? getMessageText(latest) : "";
  const attachments = latest ? modelAttachments(latest) : [];
  if (!input.trim() && attachments.length === 0) {
    throw new TransportError("没有可发送的消息或附件");
  }
  return { input, attachments };
}

function modelAttachments(message: ChatMessage): ModelAttachment[] {
  return message.parts
    .filter((part): part is FilePart => part.type === "file")
    .map((part) => {
      const blobId = attachmentIdFromUrl(part.url);
      if (!blobId) throw new TransportError(`${part.name ?? "附件"} 不是有效的本地 Blob`);
      return {
        blob_id: blobId,
        media_type: inferMediaType(part.name ?? "", part.mediaType),
        ...(part.name ? { name: part.name } : {}),
      };
    });
}

function titleSource(message: ChatMessage) {
  const text = getMessageText(message).trim();
  if (text) return text;
  const names = message.parts
    .filter((part): part is FilePart => part.type === "file")
    .map((part) => part.name ?? "附件");
  return names.length > 0 ? `分析附件：${names.join("、")}` : "新对话";
}

function getResumeRun(request: SendRequest) {
  const candidate = request.body?.minaResumeRun;
  if (!isRecord(candidate)) return null;
  return typeof candidate.runId === "string" ? { runId: candidate.runId } : null;
}

function terminalEventsFor(
  transition: AgentTransition,
  closeActiveOutput: () => ChatEvent[],
  usage?: Usage,
): ChatEvent[] {
  if (transition.outcome.status === "failed") {
    const error = isRecord(transition.outcome.error) ? transition.outcome.error : {};
    throw new TransportError(
      typeof error.message === "string" ? error.message : "WASM Agent 执行失败",
    );
  }
  return [
    ...closeActiveOutput(),
    {
      type: "message-end",
      finishReason: transition.outcome.status === "cancelled" ? "cancelled" : "stop",
      usage: usage
        ? {
            inputTokens: usage.input_tokens,
            outputTokens: usage.output_tokens,
            totalTokens: usage.total_tokens,
          }
        : undefined,
    },
  ];
}

function isTerminal(transition: AgentTransition) {
  return ["complete", "failed", "cancelled"].includes(transition.outcome.status);
}

function isFirstUserTurn(messages: ChatMessage[]) {
  return messages.filter((message) => message.role === "user").length === 1;
}

function stringField(value: Record<string, unknown>, key: string) {
  return typeof value[key] === "string" ? value[key] : "";
}

function permissionRisk(value: unknown): "low" | "medium" | "high" {
  return value === "high" || value === "medium" ? value : "low";
}

function isUsage(value: unknown): value is Usage {
  return (
    isRecord(value) &&
    typeof value.input_tokens === "number" &&
    typeof value.output_tokens === "number" &&
    typeof value.total_tokens === "number"
  );
}

function parseOutput(output: string) {
  try {
    return JSON.parse(output) as unknown;
  } catch {
    return output;
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}
