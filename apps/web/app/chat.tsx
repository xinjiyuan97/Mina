"use client";

import { useCallback, useEffect, useMemo, useState } from "react";

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
import type { SessionSnapshot } from "./session-client";

export function Chat({
  session,
  initialMessages,
  onRunFinished,
  onBusyChange,
  maxSteps,
}: {
  session: SessionSnapshot;
  initialMessages: ChatMessage[];
  onRunFinished?: (sessionId: string) => void;
  onBusyChange?: (busy: boolean) => void;
  maxSteps?: number;
}) {
  const [currentRunId, setCurrentRunId] = useState(session.active_run_id);
  const [runToHydrate] = useState(() => session.active_run_id);
  const [cancelling, setCancelling] = useState(false);
  const handleRunStarted = useCallback((runId: string) => {
    setCurrentRunId(runId);
  }, []);
  const handleRunFinished = useCallback(
    (sessionId: string) => {
      onBusyChange?.(false);
      setCurrentRunId(undefined);
      onRunFinished?.(sessionId);
    },
    [onBusyChange, onRunFinished],
  );
  const handleRunPaused = useCallback(
    (sessionId: string, runId: string) => {
      onBusyChange?.(false);
      setCurrentRunId(runId);
      onRunFinished?.(sessionId);
    },
    [onBusyChange, onRunFinished],
  );
  const transport = useMemo(
    () =>
      createRunTransport({
        session: {
          sessionId: session.session_id,
          revision: session.revision,
          activeRunId: session.active_run_id,
        },
        onRunStarted: handleRunStarted,
        onRunPaused: handleRunPaused,
        onRunFinished: handleRunFinished,
        maxSteps,
      }),
    [
      handleRunFinished,
      handleRunPaused,
      handleRunStarted,
      maxSteps,
      session.active_run_id,
      session.revision,
      session.session_id,
    ],
  );
  const chat = useChat({ transport, initialMessages });
  const submit = chat.submit;
  const [permissionError, setPermissionError] = useState<string | null>(null);

  useEffect(() => {
    if (!runToHydrate) return;
    const timer = window.setTimeout(() => {
      void submit({
        body: { minaResumeRun: { runId: runToHydrate, follow: false } },
      });
    }, 0);
    return () => window.clearTimeout(timer);
  }, [runToHydrate, submit]);

  useEffect(() => {
    onBusyChange?.(chat.isLoading);
    return () => onBusyChange?.(false);
  }, [chat.isLoading, onBusyChange]);

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

    try {
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
        return;
      }

      // A live request already owns an SSE connection and will receive the
      // resumed events. A replayed, idle request needs a fresh follow stream.
      if (!chat.isLoading) {
        chat.removeMessage(message.id);
        await chat.submit({
          body: { minaResumeRun: { runId, follow: true } },
        });
      }
    } catch (cause) {
      setPermissionError(cause instanceof Error ? cause.message : "审批提交失败，请重试");
    }
  }

  async function cancelCurrentRun() {
    const runId = currentRunId;
    if (!runId || cancelling) return;
    setCancelling(true);
    setPermissionError(null);
    try {
      const response = await fetch(`/api/v1/runs/${encodeURIComponent(runId)}/cancel`, {
        method: "POST",
      });
      if (!response.ok) {
        const error = (await response.json().catch(() => null)) as { message?: unknown } | null;
        setPermissionError(
          typeof error?.message === "string" ? error.message : "取消任务失败，请重试",
        );
        return;
      }

      if (!chat.isLoading) {
        const replayedMessage = chat.messages.findLast((message) => message.role === "assistant");
        if (replayedMessage) chat.removeMessage(replayedMessage.id);
        await chat.submit({
          body: { minaResumeRun: { runId, follow: true } },
        });
      }
    } catch (cause) {
      setPermissionError(cause instanceof Error ? cause.message : "取消任务失败，请重试");
    } finally {
      setCancelling(false);
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
            {currentRunId && !chat.isLoading && (
              <div className="paused-run-banner" role="status">
                <span>当前 Run 已挂起，等待审批或外部事件。</span>
                <button type="button" onClick={() => void cancelCurrentRun()} disabled={cancelling}>
                  {cancelling ? "取消中…" : "取消当前任务"}
                </button>
              </div>
            )}
            <PromptInput
              onSubmit={(text) => void chat.send(text)}
              onStop={() => void cancelCurrentRun()}
              streaming={chat.isLoading || cancelling}
              disabled={Boolean(currentRunId) && !chat.isLoading}
              placeholder={currentRunId ? "当前 Session 有任务正在执行" : "给 Mina 发消息"}
              autoFocus
              showHint
            />
          </div>
        </div>
      </ChatContainer>
    </ChatThemeProvider>
  );
}
