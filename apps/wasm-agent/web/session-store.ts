import { applyEvents, type ChatEvent, type ChatMessage, type Conversation } from "@xinjiyuan97/chat-core";
import { type SessionSnapshot, type ModelMessage } from "@mina/browser-agent";
import { agent, browserAttachmentStore } from "./runtime";
export type { SessionSnapshot } from "@mina/browser-agent";

export async function listActiveSessions(signal?: AbortSignal) {
  throwIfAborted(signal);
  return agent.sessions.list();
}
export async function createSession(signal?: AbortSignal) {
  throwIfAborted(signal);
  return agent.sessions.create();
}
export async function loadSessionMessages(sessionId: string, signal?: AbortSignal) {
  const session = await agent.sessions.get(sessionId);
  const rendered: ChatMessage[] = [];
  for (const [index, message] of session.messages.entries()) {
    throwIfAborted(signal);
    if (message.role === "system") continue;
    if (message.role === "tool") {
      const owner = [...rendered].reverse().find((m) => m.role === "assistant");
      if (owner && message.tool_call_id) {
        const updated = applyEvents(owner, [{ type: "tool-output", toolCallId: message.tool_call_id, output: parseOutput(message.content) }]);
        rendered[rendered.indexOf(owner)] = updated;
      }
      continue;
    }
    let projected: ChatMessage = {
      id: `${sessionId}:${index}`, role: message.role, parts: [], status: "complete", createdAt: session.created_at_ms + index,
    };
    const events: ChatEvent[] = [];
    if (message.reasoning) events.push({ type: "reasoning-start" }, { type: "reasoning-delta", delta: message.reasoning }, { type: "reasoning-end" });
    if (message.content) events.push({ type: "text-start" }, { type: "text-delta", delta: message.content }, { type: "text-end" });
    for (const call of message.tool_calls ?? []) {
      events.push({ type: "tool-input-start", toolCallId: call.id, name: call.name }, { type: "tool-input-available", toolCallId: call.id, input: parseOutput(call.arguments) });
    }
    projected = applyEvents(projected, events);
    projected.status = "complete";
    for (const attachment of message.attachments ?? []) {
      try {
        const url = await browserAttachmentStore.displayUrl(attachment.blob_id, signal);
        projected.parts.push({ type: "file", id: attachment.blob_id, name: attachment.name, mediaType: attachment.media_type, url, status: "ready" });
      } catch (cause) {
        projected.parts.push({ type: "file", id: attachment.blob_id, name: attachment.name, mediaType: attachment.media_type, status: "error", error: cause instanceof Error ? cause.message : "无法读取附件" });
      }
    }
    rendered.push(projected);
  }
  return rendered;
}
export function updateSessionTitle(sessionId: string, title: string, expectedRevision: number) {
  return agent.sessions.updateTitle(sessionId, title, expectedRevision);
}
export function toConversation(session: SessionSnapshot): Conversation {
  const messageCount = session.messages.filter((m: ModelMessage) => m.role === "user" || m.role === "assistant").length;
  return {
    id: session.session_id, title: session.title?.trim() || `会话 ${session.session_id.slice(0, 8)}`,
    preview: session.active_run_id ? "浏览器任务待恢复" : `${messageCount} 条消息 · OPFS`,
    createdAt: session.created_at_ms, updatedAt: session.updated_at_ms,
    metadata: { revision: session.revision, messageCount },
  };
}
function parseOutput(content: string) { try { return JSON.parse(content); } catch { return content; } }
function throwIfAborted(signal?: AbortSignal) { if (signal?.aborted) throw signal.reason ?? new DOMException("Aborted", "AbortError"); }
