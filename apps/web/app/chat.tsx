"use client";

import { useMemo, useState } from "react";

import {
  isPermissionPart,
  useChat,
  type ChatMessage,
  type PermissionResolution,
} from "@xinjiyuan97/chat-core";
import {
  ChatContainer,
  ChatEmptyState,
  ChatMessageList,
  ChatThemeProvider,
  ChatViewport,
  Message,
  PromptInput,
  SuggestionChips,
} from "@xinjiyuan97/chat-ui";

import { createRunTransport } from "./run-transport";

export function Chat({ onRunFinished }: { onRunFinished?: () => void }) {
  const transport = useMemo(
    () => createRunTransport({ onRunFinished }),
    [onRunFinished],
  );
  const chat = useChat({ transport });
  const [permissionError, setPermissionError] = useState<string | null>(null);

  async function resolvePermission(
    resolution: PermissionResolution,
    message: ChatMessage,
  ) {
    setPermissionError(null);
    const permission = message.parts.find(
      (part) => isPermissionPart(part) && part.request.id === resolution.requestId,
    );
    if (!permission || !isPermissionPart(permission)) {
      setPermissionError("找不到对应的审批请求");
      return;
    }
    const runId = permission.request.metadata?.runId;
    if (typeof runId !== "string") {
      setPermissionError("审批请求缺少 run_id");
      return;
    }

    const response = await fetch(
      `/api/v1/runs/${encodeURIComponent(runId)}/approvals/${encodeURIComponent(resolution.requestId)}`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          decision: resolution.decision,
          reason: resolution.reason,
        }),
      },
    );
    if (!response.ok) {
      const error = (await response.json().catch(() => null)) as { message?: unknown } | null;
      setPermissionError(
        typeof error?.message === "string" ? error.message : "审批提交失败，请重试",
      );
    }
  }

  return (
    <ChatThemeProvider
      locale="zh-CN"
      onPermissionDecision={(resolution, message) => {
        void resolvePermission(resolution, message);
      }}
      asFragment
    >
      <ChatContainer className="chat-shell">
        <ChatViewport>
          {chat.messages.length === 0 ? (
            <ChatEmptyState title="开始对话" subtitle="无需登录，直接发送一条消息。">
              <SuggestionChips
                suggestions={["你好，Mina"]}
                onSelect={(text) => void chat.send(text)}
              />
            </ChatEmptyState>
          ) : (
            <ChatMessageList busy={chat.isLoading}>
              {chat.messages.map((message) => (
                <Message
                  key={message.id}
                  message={message}
                  hideActions={message.status === "streaming"}
                  onRegenerate={() => void chat.regenerate()}
                  onRetry={() => void chat.regenerate()}
                />
              ))}
            </ChatMessageList>
          )}
        </ChatViewport>

        <div className="composer">
          <div className="composer-inner">
            {(chat.error || permissionError) && (
              <p className="chat-error" role="alert">
                {permissionError ?? chat.error?.message}
              </p>
            )}
            <PromptInput
              onSubmit={(text) => void chat.send(text)}
              onStop={chat.stop}
              streaming={chat.isLoading}
              placeholder="给 Mina 发消息"
              autoFocus
              showHint
            />
          </div>
        </div>
      </ChatContainer>
    </ChatThemeProvider>
  );
}
