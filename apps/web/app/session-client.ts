import type { ChatMessage, Conversation, MessagePart } from "@xinjiyuan97/chat-core";

export type SessionSnapshot = {
  session_id: string;
  agent_profile: string;
  status: "active" | "archived";
  title?: string;
  revision: number;
  next_message_ordinal: number;
  active_run_id?: string;
  created_at_ms: number;
  updated_at_ms: number;
};

type SessionContentPart =
  | { type: "text"; text: string }
  | { type: "reasoning"; text: string; duration_ms?: number }
  | {
      type: "tool_call";
      tool_call_id: string;
      name: string;
      state:
        | "input_streaming"
        | "input_available"
        | "executing"
        | "output_available"
        | "output_error";
      input?: unknown;
      input_text?: string;
      output?: string;
      error?: string;
      duration_ms?: number;
    }
  | {
      type: "permission";
      approval_id: string;
      call_id: string;
      tool_name: string;
      risk_level: "low" | "medium" | "high";
      arguments: unknown;
      requested_at_ms: number;
      resolution?: {
        decision: "allow-once" | "deny";
        reason?: string;
      };
      resolved_at_ms?: number;
    }
  | {
      type: "blob_ref";
      blob_id: string;
      media_type: string;
      name?: string;
      size_bytes?: number;
    };

type SessionMessage = {
  message_id: string;
  session_id: string;
  ordinal: number;
  role: "user" | "assistant" | "system_note";
  content: SessionContentPart[];
  source_run_id?: string;
  created_at_ms: number;
};

export async function listActiveSessions(signal?: AbortSignal): Promise<SessionSnapshot[]> {
  const response = await fetch("/api/v1/sessions?status=active&limit=100", { signal });
  if (!response.ok) throw await responseError(response, "读取会话列表失败");
  const payload = (await response.json()) as unknown;
  if (!Array.isArray(payload) || !payload.every(isSessionSnapshot)) {
    throw new Error("后端返回了无效的会话列表");
  }
  return payload;
}

export async function createSession(signal?: AbortSignal): Promise<SessionSnapshot> {
  const response = await fetch("/api/v1/sessions", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      agent_profile: "default",
      title: newSessionTitle(),
    }),
    signal,
  });
  if (!response.ok) throw await responseError(response, "创建会话失败");
  const payload = (await response.json()) as unknown;
  if (!isSessionSnapshot(payload)) throw new Error("后端返回了无效的会话");
  return payload;
}

export async function loadSessionMessages(
  sessionId: string,
  signal?: AbortSignal,
): Promise<ChatMessage[]> {
  const response = await fetch(
    `/api/v1/sessions/${encodeURIComponent(sessionId)}/messages?limit=1000`,
    { signal },
  );
  if (!response.ok) throw await responseError(response, "读取会话历史失败");
  const payload = (await response.json()) as unknown;
  if (!Array.isArray(payload) || !payload.every(isSessionMessage)) {
    throw new Error("后端返回了无效的会话历史");
  }
  return payload.map(toChatMessage);
}

export function toConversation(session: SessionSnapshot): Conversation {
  const messageCount = Math.max(0, session.next_message_ordinal - 1);
  return {
    id: session.session_id,
    title: session.title?.trim() || `会话 ${session.session_id.slice(0, 8)}`,
    preview: session.active_run_id ? "任务执行中" : `${messageCount} 条消息`,
    createdAt: session.created_at_ms,
    updatedAt: session.updated_at_ms,
    metadata: {
      revision: session.revision,
      messageCount,
    },
  };
}

function toChatMessage(message: SessionMessage): ChatMessage {
  const parts = message.content.map((part) => toChatPart(part, message));
  return {
    id: message.message_id,
    role: message.role === "system_note" ? "system" : message.role,
    parts: parts.length > 0 ? parts : [{ type: "text", text: "" }],
    createdAt: message.created_at_ms,
    status: "complete",
    metadata: {
      sessionId: message.session_id,
      ordinal: message.ordinal,
      sourceRunId: message.source_run_id,
    },
  };
}

function toChatPart(part: SessionContentPart, message: SessionMessage): MessagePart {
  switch (part.type) {
    case "text":
      return { type: "text", text: part.text };
    case "reasoning":
      return {
        type: "reasoning",
        text: part.text,
        durationMs: part.duration_ms,
      };
    case "tool_call":
      return {
        type: "tool",
        toolCallId: part.tool_call_id,
        name: part.name,
        state: sessionToolState(part.state),
        input: part.input,
        inputText: part.input_text,
        output:
          part.output === undefined ? undefined : parseStoredToolOutput(part.output),
        error: part.error,
        durationMs: part.duration_ms,
      };
    case "permission": {
      const decision = part.resolution?.decision;
      return {
        type: "permission",
        request: {
          id: part.approval_id,
          toolName: part.tool_name,
          toolCallId: part.call_id,
          title: `允许 ${part.tool_name} 执行？`,
          detail: JSON.stringify(part.arguments, null, 2),
          detailLanguage: "json",
          risk: part.risk_level,
          options: [
            { value: "allow-once", decision: "allow-once" },
            {
              value: "deny",
              decision: "deny",
              promptForReason: true,
              requiresReason: false,
            },
          ],
          createdAt: part.requested_at_ms,
          metadata: { runId: message.source_run_id },
        },
        resolution:
          decision === undefined
            ? undefined
            : {
                requestId: part.approval_id,
                option: decision,
                decision,
                reason: part.resolution?.reason,
                decidedAt: part.resolved_at_ms,
              },
      };
    }
    case "blob_ref":
      return {
        type: "file",
        id: part.blob_id,
        url: `/api/v1/blobs/${encodeURIComponent(part.blob_id)}`,
        mediaType: part.media_type,
        name: part.name,
        size: part.size_bytes,
        status: "ready",
      };
  }
}

function sessionToolState(state: Extract<SessionContentPart, { type: "tool_call" }>["state"]) {
  return state.replaceAll("_", "-") as
    | "input-streaming"
    | "input-available"
    | "executing"
    | "output-available"
    | "output-error";
}

function parseStoredToolOutput(output: string): unknown {
  try {
    return JSON.parse(output) as unknown;
  } catch {
    return output;
  }
}

function newSessionTitle() {
  const timestamp = new Intl.DateTimeFormat("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(Date.now());
  return `会话 ${timestamp}`;
}

function isSessionSnapshot(value: unknown): value is SessionSnapshot {
  if (!isRecord(value)) return false;
  return (
    typeof value.session_id === "string" &&
    typeof value.agent_profile === "string" &&
    (value.status === "active" || value.status === "archived") &&
    (value.title === undefined || typeof value.title === "string") &&
    typeof value.revision === "number" &&
    typeof value.next_message_ordinal === "number" &&
    (value.active_run_id === undefined || typeof value.active_run_id === "string") &&
    typeof value.created_at_ms === "number" &&
    typeof value.updated_at_ms === "number"
  );
}

function isSessionMessage(value: unknown): value is SessionMessage {
  if (!isRecord(value)) return false;
  return (
    typeof value.message_id === "string" &&
    typeof value.session_id === "string" &&
    typeof value.ordinal === "number" &&
    (value.role === "user" || value.role === "assistant" || value.role === "system_note") &&
    Array.isArray(value.content) &&
    value.content.every(isSessionContentPart) &&
    (value.source_run_id === undefined || typeof value.source_run_id === "string") &&
    typeof value.created_at_ms === "number"
  );
}

function isSessionContentPart(value: unknown): value is SessionContentPart {
  if (!isRecord(value) || typeof value.type !== "string") return false;
  switch (value.type) {
    case "text":
      return typeof value.text === "string";
    case "reasoning":
      return (
        typeof value.text === "string" &&
        (value.duration_ms === undefined || typeof value.duration_ms === "number")
      );
    case "tool_call":
      return (
        typeof value.tool_call_id === "string" &&
        typeof value.name === "string" &&
        isSessionToolState(value.state) &&
        (value.input_text === undefined || typeof value.input_text === "string") &&
        (value.output === undefined || typeof value.output === "string") &&
        (value.error === undefined || typeof value.error === "string") &&
        (value.duration_ms === undefined || typeof value.duration_ms === "number")
      );
    case "permission":
      return (
        typeof value.approval_id === "string" &&
        typeof value.call_id === "string" &&
        typeof value.tool_name === "string" &&
        isPermissionRisk(value.risk_level) &&
        "arguments" in value &&
        typeof value.requested_at_ms === "number" &&
        (value.resolution === undefined || isApprovalResolution(value.resolution)) &&
        (value.resolved_at_ms === undefined || typeof value.resolved_at_ms === "number")
      );
    case "blob_ref":
      return (
        typeof value.blob_id === "string" &&
        typeof value.media_type === "string" &&
        (value.name === undefined || typeof value.name === "string") &&
        (value.size_bytes === undefined || typeof value.size_bytes === "number")
      );
    default:
      return false;
  }
}

function isSessionToolState(
  value: unknown,
): value is Extract<SessionContentPart, { type: "tool_call" }>["state"] {
  return (
    value === "input_streaming" ||
    value === "input_available" ||
    value === "executing" ||
    value === "output_available" ||
    value === "output_error"
  );
}

function isPermissionRisk(value: unknown): value is "low" | "medium" | "high" {
  return value === "low" || value === "medium" || value === "high";
}

function isApprovalResolution(
  value: unknown,
): value is { decision: "allow-once" | "deny"; reason?: string } {
  return (
    isRecord(value) &&
    (value.decision === "allow-once" || value.decision === "deny") &&
    (value.reason === undefined || typeof value.reason === "string")
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

async function responseError(response: Response, fallback: string) {
  const payload = (await response.json().catch(() => null)) as { message?: unknown } | null;
  return new Error(typeof payload?.message === "string" ? payload.message : fallback);
}
