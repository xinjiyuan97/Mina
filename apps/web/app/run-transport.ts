import {
  TransportError,
  createSSETransport,
  getMessageText,
  type ChatEvent,
  type ChatTransport,
  type FilePart,
  type SSEMessage,
  type SendRequest,
} from "@xinjiyuan97/chat-core";

type RunUsage = {
  input_tokens: number;
  output_tokens: number;
  total_tokens: number;
  source?: "provider_reported" | "estimator_fallback" | "mixed";
};

export type RunTransportSession = {
  sessionId: string;
  revision: number;
  activeRunId?: string;
};

type RunAccepted = {
  session_id: string;
  run_id: string;
  session_revision: number;
  replayed: boolean;
  max_steps: number;
};

type ResumeRunRequest = {
  runId: string;
  follow: boolean;
};

type BlobInput = {
  blob_id: string;
  media_type: string;
  name?: string;
  size_bytes?: number;
};

type RunEvent = {
  run_id: string;
  seq: number;
  type: string;
  channel?: string;
  delta?: string;
  usage?: RunUsage;
  finish_reason?: string;
  call_id?: string;
  name?: string;
  approval_id?: string;
  tool_name?: string;
  risk_level?: string;
  arguments?: unknown;
  resolution?: {
    decision?: string;
    reason?: string;
  };
  output?: string;
  code?: string;
  message?: string;
  retryable?: boolean;
};

export function createRunTransport(
  options: {
    session: RunTransportSession;
    onRunStarted?: (runId: string) => void;
    onRunPaused?: (sessionId: string, runId: string) => void;
    onRunFinished?: (sessionId: string) => void;
    maxSteps?: number;
  },
): ChatTransport {
  let sessionState = options.session;

  return {
    async *send(request, context) {
      let messageStarted = false;
      let activeOutput: "reasoning" | "text" | null = null;
      let finished = false;
      let suspended = false;
      let lastSeq = 0;
      let usage: RunUsage | undefined;
      const resume = getResumeRunRequest(request);
      let runId: string;

      if (resume) {
        const expectedRunId = sessionState.activeRunId;
        const refreshed = await refreshSession(sessionState, context.signal);
        if (expectedRunId !== resume.runId && refreshed.activeRunId !== resume.runId) {
          throw protocolError("Session 当前没有这个可恢复的 Run");
        }
        sessionState = refreshed;
        runId = resume.runId;
      } else {
        const { input, attachments } = getRequestInput(request);
        const accepted = await submitSessionRun(
          sessionState,
          input,
          attachments,
          options.maxSteps,
          context.signal,
        );
        runId = accepted.run_id;
        sessionState = {
          sessionId: accepted.session_id,
          revision: accepted.session_revision,
          activeRunId: accepted.run_id,
        };
      }
      options.onRunStarted?.(runId);

      function closeActiveOutput(): ChatEvent[] {
        if (activeOutput === "reasoning") {
          activeOutput = null;
          return [{ type: "reasoning-end" }];
        }
        if (activeOutput === "text") {
          activeOutput = null;
          return [{ type: "text-end" }];
        }
        return [];
      }

      const mapEvent = (message: SSEMessage): ChatEvent | ChatEvent[] | null => {
          if (message.event === "run_stream_error") {
            throw parseStreamError(message);
          }
          const event = parseRunEvent(message);
          lastSeq = Math.max(lastSeq, event.seq);

          switch (event.type) {
            case "run_started":
              assertNotFinished(finished);
              if (messageStarted) {
                throw protocolError("后端重复发送了 run_started");
              }
              messageStarted = true;
              suspended = false;
              return { type: "message-start" };

            case "run_resumed":
              assertRunning(messageStarted, finished);
              suspended = false;
              return null;

            case "run_waiting":
              assertRunning(messageStarted, finished);
              suspended = true;
              return closeActiveOutput();

            case "output_delta": {
              assertRunning(messageStarted, finished);
              if (typeof event.delta !== "string") {
                throw protocolError("后端返回了无效的 output_delta");
              }
              if (!event.delta) return null;

              const events: ChatEvent[] = [];
              if (event.channel === "assistant_reasoning") {
                if (activeOutput !== "reasoning") {
                  events.push(...closeActiveOutput(), { type: "reasoning-start" });
                  activeOutput = "reasoning";
                }
                events.push({ type: "reasoning-delta", delta: event.delta });
                return events;
              }

              if (event.channel === "assistant_text") {
                if (activeOutput !== "text") {
                  events.push(...closeActiveOutput());
                  activeOutput = "text";
                  events.push({ type: "text-start" });
                }
                events.push({ type: "text-delta", delta: event.delta });
                return events;
              }

              throw protocolError("后端返回了未知的输出通道");
            }

            case "usage_updated":
              assertRunning(messageStarted, finished);
              if (!isUsage(event.usage)) {
                throw protocolError("后端返回了无效的 usage_updated");
              }
              usage = event.usage;
              return null;

            case "tool_call_started": {
              assertRunning(messageStarted, finished);
              if (typeof event.call_id !== "string" || typeof event.name !== "string") {
                throw protocolError("后端返回了无效的 tool_call_started");
              }
              return [
                ...closeActiveOutput(),
                {
                  type: "tool-input-start",
                  toolCallId: event.call_id,
                  name: event.name,
                },
              ];
            }

            case "tool_call_arguments_delta":
              assertRunning(messageStarted, finished);
              if (typeof event.call_id !== "string" || typeof event.delta !== "string") {
                throw protocolError("后端返回了无效的 tool_call_arguments_delta");
              }
              return {
                type: "tool-input-delta",
                toolCallId: event.call_id,
                delta: event.delta,
              };

            case "approval_requested": {
              assertRunning(messageStarted, finished);
              if (
                typeof event.approval_id !== "string" ||
                typeof event.call_id !== "string" ||
                typeof event.tool_name !== "string" ||
                !isPermissionRisk(event.risk_level) ||
                event.arguments === undefined
              ) {
                throw protocolError("后端返回了无效的 approval_requested");
              }
              return [
                {
                  type: "tool-input-available",
                  toolCallId: event.call_id,
                  input: event.arguments,
                },
                {
                  type: "permission-request",
                  request: {
                    id: event.approval_id,
                    toolName: event.tool_name,
                    toolCallId: event.call_id,
                    title: `允许 ${event.tool_name} 执行？`,
                    detail: JSON.stringify(event.arguments, null, 2),
                    detailLanguage: "json",
                    risk: event.risk_level,
                    options: [
                      { value: "allow-once", decision: "allow-once" },
                      {
                        value: "deny",
                        decision: "deny",
                        promptForReason: true,
                        requiresReason: false,
                      },
                    ],
                    metadata: { runId: event.run_id },
                  },
                },
              ];
            }

            case "approval_resolved":
              assertRunning(messageStarted, finished);
              if (
                typeof event.approval_id !== "string" ||
                !isApprovalDecision(event.resolution?.decision)
              ) {
                throw protocolError("后端返回了无效的 approval_resolved");
              }
              return {
                type: "permission-resolved",
                requestId: event.approval_id,
                resolution: {
                  requestId: event.approval_id,
                  option: event.resolution.decision,
                  decision: event.resolution.decision,
                  reason:
                    typeof event.resolution.reason === "string"
                      ? event.resolution.reason
                      : undefined,
                },
              };

            case "tool_execution_started":
              assertRunning(messageStarted, finished);
              if (typeof event.call_id !== "string" || event.arguments === undefined) {
                throw protocolError("后端返回了无效的 tool_execution_started");
              }
              return [
                {
                  type: "tool-input-available",
                  toolCallId: event.call_id,
                  input: event.arguments,
                },
                { type: "tool-executing", toolCallId: event.call_id },
              ];

            case "tool_execution_completed":
              assertRunning(messageStarted, finished);
              if (typeof event.call_id !== "string" || typeof event.output !== "string") {
                throw protocolError("后端返回了无效的 tool_execution_completed");
              }
              return {
                type: "tool-output",
                toolCallId: event.call_id,
                output: parseToolOutput(event.output),
              };

            case "tool_execution_failed":
              assertRunning(messageStarted, finished);
              if (typeof event.call_id !== "string") {
                throw protocolError("后端返回了无效的 tool_execution_failed");
              }
              return {
                type: "tool-error",
                toolCallId: event.call_id,
                error: event.message ?? "工具执行失败",
              };

            case "run_completed": {
              assertRunning(messageStarted, finished);
              finished = true;
              const events = closeActiveOutput();
              events.push({
                type: "message-end",
                finishReason:
                  typeof event.finish_reason === "string" ? event.finish_reason : "unknown",
                usage: usage
                  ? {
                      inputTokens: usage.input_tokens,
                      outputTokens: usage.output_tokens,
                      totalTokens: usage.total_tokens,
                    }
                  : undefined,
              });
              return events;
            }

            case "run_cancelled": {
              assertRunning(messageStarted, finished);
              finished = true;
              const events = closeActiveOutput();
              events.push({ type: "message-end", finishReason: "cancelled" });
              return events;
            }

            case "run_failed":
              assertRunning(messageStarted, finished);
              finished = true;
              throw new TransportError(event.message ?? "Agent 执行失败", {
                body: message.data,
              });

            default:
              // A newer server may add non-terminal event types. Ignore events
              // this client does not render, but still require a known terminal.
              return null;
          }
        };

      function eventTransport(afterSeq: number, follow: boolean) {
        return createSSETransport({
          url: runEventsUrl(runId, afterSeq, follow),
          method: "GET",
          fetch(input, init) {
            return globalThis.fetch(input, { ...init, method: "GET", body: undefined });
          },
          mapEvent,
        });
      }

      try {
        const initialFollow = resume?.follow ?? true;
        for await (const event of eventTransport(0, initialFollow).send(request, context)) {
          yield event;
        }

        // Hydration first performs a finite replay. If the persisted projection
        // says the run is currently executing, attach from the replay cursor;
        // a deliberately suspended run stays idle with its approval card visible.
        if (resume && !resume.follow && !finished && !suspended && !context.signal.aborted) {
          for await (const event of eventTransport(lastSeq, true).send(request, context)) {
            yield event;
          }
        }

        if (!finished && !suspended && !context.signal.aborted) {
          throw protocolError("Agent 事件流在终态之前结束");
        }
      } finally {
        if (finished) {
          const currentSession = sessionState;
          const refreshed = await refreshFinalizedSession(currentSession).catch(
            () => currentSession,
          );
          sessionState = refreshed;
          options.onRunFinished?.(refreshed.sessionId);
        } else if (suspended) {
          const refreshed = await refreshSession(sessionState).catch(() => sessionState);
          sessionState = refreshed;
          options.onRunPaused?.(refreshed.sessionId, runId);
        }
      }
    },
  };
}

function getResumeRunRequest(request: SendRequest): ResumeRunRequest | null {
  const candidate = request.body?.minaResumeRun;
  if (!isRecord(candidate)) return null;
  if (typeof candidate.runId !== "string" || typeof candidate.follow !== "boolean") {
    throw protocolError("恢复 Run 的请求参数无效");
  }
  return { runId: candidate.runId, follow: candidate.follow };
}

function runEventsUrl(runId: string, afterSeq: number, follow: boolean) {
  const query = new URLSearchParams({
    after_seq: String(afterSeq),
    follow: String(follow),
  });
  return `/api/v1/runs/${encodeURIComponent(runId)}/events?${query.toString()}`;
}

function getRequestInput(request: SendRequest) {
  const userMessage = request.messages.findLast((message) => message.role === "user");
  const input = userMessage ? getMessageText(userMessage) : "";
  const attachments = userMessage
    ? userMessage.parts.filter((part): part is FilePart => part.type === "file").map(toBlobInput)
    : [];

  if (!input.trim() && attachments.length === 0) {
    throw new TransportError("没有可发送的消息或附件");
  }

  return { input, attachments };
}

function toBlobInput(part: FilePart): BlobInput {
  if (!part.url) throw new TransportError("附件尚未上传完成");
  let pathname: string;
  try {
    pathname = new URL(part.url, "http://mina.local").pathname;
  } catch {
    throw new TransportError("附件地址无效");
  }
  const match = pathname.match(/^\/api\/v1\/blobs\/([^/]+)$/);
  if (!match?.[1]) throw new TransportError("附件不是 Mina Blob 引用");
  return {
    blob_id: decodeURIComponent(match[1]),
    media_type: part.mediaType,
    ...(part.name === undefined ? {} : { name: part.name }),
    ...(part.size === undefined ? {} : { size_bytes: part.size }),
  };
}

async function submitSessionRun(
  session: RunTransportSession,
  input: string,
  attachments: BlobInput[],
  maxSteps: number | undefined,
  signal: AbortSignal,
): Promise<RunAccepted> {
  const response = await fetch(
    `/api/v1/sessions/${encodeURIComponent(session.sessionId)}/runs`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        input,
        attachments,
        expected_revision: session.revision,
        idempotency_key: crypto.randomUUID(),
        ...(maxSteps === undefined ? {} : { max_steps: maxSteps }),
      }),
      signal,
    },
  );
  if (!response.ok) throw await responseError(response, "提交会话任务失败");
  const accepted = (await response.json()) as Partial<RunAccepted>;
  if (
    typeof accepted.session_id !== "string" ||
    typeof accepted.run_id !== "string" ||
    typeof accepted.session_revision !== "number" ||
    typeof accepted.replayed !== "boolean" ||
    typeof accepted.max_steps !== "number"
  ) {
    throw protocolError("后端返回了无效的 RunAccepted");
  }
  return accepted as RunAccepted;
}

async function refreshSession(
  session: RunTransportSession,
  signal?: AbortSignal,
): Promise<RunTransportSession> {
  const response = await fetch(
    `/api/v1/sessions/${encodeURIComponent(session.sessionId)}`,
    { signal },
  );
  if (!response.ok) throw await responseError(response, "读取会话失败");
  const payload = (await response.json()) as {
    session_id?: unknown;
    revision?: unknown;
    active_run_id?: unknown;
  };
  if (typeof payload.session_id !== "string" || typeof payload.revision !== "number") {
    throw protocolError("后端返回了无效的 Session 状态");
  }
  return {
    sessionId: payload.session_id,
    revision: payload.revision,
    activeRunId:
      typeof payload.active_run_id === "string" ? payload.active_run_id : undefined,
  };
}

async function refreshFinalizedSession(
  session: RunTransportSession,
): Promise<RunTransportSession> {
  let current = session;
  for (let attempt = 0; attempt < 20; attempt += 1) {
    current = await refreshSession(current);
    if (!current.activeRunId) return current;
    await new Promise<void>((resolve) => setTimeout(resolve, 25));
  }
  return current;
}

async function responseError(response: Response, fallback: string) {
  const body = await response.text();
  let message = fallback;
  try {
    const parsed = JSON.parse(body) as { message?: unknown };
    if (typeof parsed.message === "string") message = parsed.message;
  } catch {
    // Preserve the safe fallback for a non-JSON proxy error.
  }
  return new TransportError(message, { status: response.status, body });
}

function parseRunEvent(message: SSEMessage): RunEvent {
  let parsed: unknown;
  try {
    parsed = JSON.parse(message.data);
  } catch {
    throw protocolError("后端返回了无效的 SSE JSON");
  }

  if (!isRecord(parsed)) {
    throw protocolError("后端返回了无效的 Agent 事件");
  }
  if (
    typeof parsed.run_id !== "string" ||
    typeof parsed.seq !== "number" ||
    typeof parsed.type !== "string"
  ) {
    throw protocolError("Agent 事件缺少 run_id、seq 或 type");
  }

  return parsed as RunEvent;
}

function parseStreamError(message: SSEMessage) {
  try {
    const payload = JSON.parse(message.data) as { message?: unknown };
    if (typeof payload.message === "string") return new TransportError(payload.message);
  } catch {
    // Fall through to the stable error shown by the chat surface.
  }
  return new TransportError("Run 事件流读取失败");
}

function assertRunning(started: boolean, finished: boolean) {
  if (!started) throw protocolError("Agent 在 run_started 之前发送了业务事件");
  assertNotFinished(finished);
}

function assertNotFinished(finished: boolean) {
  if (finished) throw protocolError("Agent 在终态之后继续发送事件");
}

function protocolError(message: string) {
  return new TransportError(message);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function isUsage(value: unknown): value is RunUsage {
  if (!isRecord(value)) return false;
  return (
    typeof value.input_tokens === "number" &&
    typeof value.output_tokens === "number" &&
    typeof value.total_tokens === "number"
  );
}

function isPermissionRisk(value: unknown): value is "low" | "medium" | "high" {
  return value === "low" || value === "medium" || value === "high";
}

function isApprovalDecision(value: unknown): value is "allow-once" | "deny" {
  return value === "allow-once" || value === "deny";
}

function parseToolOutput(output: string): unknown {
  try {
    return JSON.parse(output) as unknown;
  } catch {
    return output;
  }
}
