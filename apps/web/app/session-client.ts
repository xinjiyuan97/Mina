import type { ChatMessage, Conversation } from "@xinjiyuan97/chat-core";

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
  | { type: "blob_ref"; blob_id: string; media_type: string };

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
  const parts = message.content.map((part) => {
    if (part.type === "text") return { type: "text" as const, text: part.text };
    return {
      type: "text" as const,
      text: `[附件 ${part.media_type} · ${part.blob_id}]`,
    };
  });
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
  if (value.type === "text") return typeof value.text === "string";
  return (
    value.type === "blob_ref" &&
    typeof value.blob_id === "string" &&
    typeof value.media_type === "string"
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

async function responseError(response: Response, fallback: string) {
  const payload = (await response.json().catch(() => null)) as { message?: unknown } | null;
  return new Error(typeof payload?.message === "string" ? payload.message : fallback);
}
