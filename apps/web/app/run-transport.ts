import {
  TransportError,
  createSSETransport,
  getMessageText,
  type ChatEvent,
  type ChatTransport,
  type SSEMessage,
  type SendRequest,
} from "@xinjiyuan97/chat-core";

type RunUsage = {
  input_tokens: number;
  output_tokens: number;
  total_tokens: number;
  source?: "provider_reported" | "estimator_fallback" | "mixed";
};

type SessionState = {
  sessionId: string;
  revision: number;
  activeRunId?: string;
};

type RunAccepted = {
  session_id: string;
  run_id: string;
  session_revision: number;
  replayed: boolean;
};

const SESSION_STORAGE_KEY = "mina.chat.session.v1";

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

export function createRunTransport(options: { onRunFinished?: () => void } = {}): ChatTransport {
  let sessionState: SessionState | null = null;

  return {
    async *send(request, context) {
      let messageStarted = false;
      let activeOutput: "reasoning" | "text" | null = null;
      let finished = false;
      let usage: RunUsage | undefined;
      let activeRunId: string | null = null;
      let cancellationSent = false;

      function propagateCancellation() {
        if (!activeRunId || cancellationSent) return;
        cancellationSent = true;
        void fetch(`/api/v1/runs/${encodeURIComponent(activeRunId)}/cancel`, {
          method: "POST",
          keepalive: true,
        }).catch(() => {
          // The local abort still stops rendering. The server-side run timeout
          // remains the final safety net if this best-effort request cannot land.
        });
      }

      context.signal.addEventListener("abort", propagateCancellation, { once: true });

      const input = getRequestInput(request);
      sessionState = await ensureSession(sessionState, context.signal);
      const accepted = await submitSessionRun(sessionState, input, context.signal);
      activeRunId = accepted.run_id;
      sessionState = {
        sessionId: accepted.session_id,
        revision: accepted.session_revision,
      };
      saveSessionState(sessionState);
      if (context.signal.aborted) propagateCancellation();

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

      const transport = createSSETransport({
        url: `/api/v1/runs/${encodeURIComponent(accepted.run_id)}/events?after_seq=0`,
        method: "GET",
        fetch(input, init) {
          return globalThis.fetch(input, { ...init, method: "GET", body: undefined });
        },
        mapEvent(message) {
          const event = parseRunEvent(message);

          switch (event.type) {
            case "run_started":
              assertNotFinished(finished);
              if (messageStarted) {
                throw protocolError("后端重复发送了 run_started");
              }
              messageStarted = true;
              activeRunId = event.run_id;
              if (context.signal.aborted) propagateCancellation();
              // Keep the optimistic local id while streaming deltas.
              // message id while deltas are in flight, so keep the local id.
              return { type: "message-start" };

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
        },
      });

      try {
        for await (const event of transport.send(request, context)) {
          yield event;
        }

        if (!finished && !context.signal.aborted) {
          throw protocolError("Agent 事件流在终态之前结束");
        }
      } finally {
        context.signal.removeEventListener("abort", propagateCancellation);
        if (finished && sessionState) {
          const currentSession = sessionState;
          const refreshed = await refreshFinalizedSession(currentSession).catch(
            () => currentSession,
          );
          sessionState = refreshed;
          saveSessionState(refreshed);
          options.onRunFinished?.();
        }
      }
    },
  };
}

function getRequestInput(request: SendRequest) {
  const userMessage = request.messages.findLast((message) => message.role === "user");
  const input = userMessage ? getMessageText(userMessage) : "";

  if (!input.trim()) {
    throw new TransportError("没有可发送的文本消息");
  }

  return input;
}

async function ensureSession(
  current: SessionState | null,
  signal: AbortSignal,
): Promise<SessionState> {
  const stored = current ?? loadSessionState();
  if (stored) {
    const refreshed = await refreshSession(stored, signal).catch(() => null);
    if (refreshed) return refreshed;
  }

  const response = await fetch("/api/v1/sessions", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ agent_profile: "default" }),
    signal,
  });
  if (!response.ok) throw await responseError(response, "创建会话失败");
  const session = (await response.json()) as { session_id?: unknown; revision?: unknown };
  if (typeof session.session_id !== "string" || typeof session.revision !== "number") {
    throw protocolError("后端返回了无效的 Session");
  }
  const created = { sessionId: session.session_id, revision: session.revision };
  saveSessionState(created);
  return created;
}

async function submitSessionRun(
  session: SessionState,
  input: string,
  signal: AbortSignal,
): Promise<RunAccepted> {
  const response = await fetch(
    `/api/v1/sessions/${encodeURIComponent(session.sessionId)}/runs`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        input,
        expected_revision: session.revision,
        idempotency_key: crypto.randomUUID(),
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
    typeof accepted.replayed !== "boolean"
  ) {
    throw protocolError("后端返回了无效的 RunAccepted");
  }
  return accepted as RunAccepted;
}

async function refreshSession(
  session: SessionState,
  signal?: AbortSignal,
): Promise<SessionState> {
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

async function refreshFinalizedSession(session: SessionState): Promise<SessionState> {
  let current = session;
  for (let attempt = 0; attempt < 20; attempt += 1) {
    current = await refreshSession(current);
    if (!current.activeRunId) return current;
    await new Promise<void>((resolve) => setTimeout(resolve, 25));
  }
  return current;
}

function loadSessionState(): SessionState | null {
  if (typeof window === "undefined") return null;
  try {
    const parsed = JSON.parse(window.localStorage.getItem(SESSION_STORAGE_KEY) ?? "null") as {
      sessionId?: unknown;
      revision?: unknown;
    } | null;
    return parsed &&
      typeof parsed.sessionId === "string" &&
      typeof parsed.revision === "number"
      ? { sessionId: parsed.sessionId, revision: parsed.revision }
      : null;
  } catch {
    return null;
  }
}

function saveSessionState(session: SessionState) {
  if (typeof window === "undefined") return;
  window.localStorage.setItem(SESSION_STORAGE_KEY, JSON.stringify(session));
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
