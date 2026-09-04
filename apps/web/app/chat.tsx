"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import {
  isPermissionPart,
  useAttachments,
  useChat,
  type ChatMessage,
  type PermissionResolution,
} from "@xinjiyuan97/chat-core";
import {
  ChatContainer,
  ChatDock,
  ChatEmptyState,
  ChatMessageList,
  ChatThemeProvider,
  ChatViewport,
  LoadingShimmer,
  Message,
  PromptInput,
  SuggestionChips,
  ThinkingDots,
} from "@xinjiyuan97/chat-ui";

import { createRunTransport } from "./run-transport";
import type { SessionSnapshot } from "./session-client";

export function Chat({
  session,
  initialMessages,
  onRunFinished,
  onBusyChange,
  maxSteps,
  inputModalities,
}: {
  session: SessionSnapshot;
  initialMessages: ChatMessage[];
  onRunFinished?: (sessionId: string) => void;
  onBusyChange?: (busy: boolean) => void;
  maxSteps?: number;
  inputModalities?: string[];
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
  const waitingForFirstOutput =
    chat.isLoading && isWaitingForFirstOutput(chat.messages);
  // Keep the composer multimodal while agent inspection is still loading. The
  // resolved model capabilities replace this fallback as soon as they arrive.
  const effectiveInputModalities = inputModalities ?? DEFAULT_INPUT_MODALITIES;
  const attachmentAccept = attachmentAcceptFor(effectiveInputModalities);
  const [attachmentError, setAttachmentError] = useState<string | null>(null);
  const attachments = useAttachments({
    accept: attachmentAccept,
    maxFiles: 8,
    maxSize: 10 * 1024 * 1024,
    onUpload: uploadAttachment,
    onError: (message, file) => {
      setAttachmentError(formatAttachmentError(message, file));
    },
  });
  const submit = chat.submit;
  const [permissionError, setPermissionError] = useState<string | null>(null);
  const chatRuntimeRef = useRef<HTMLDivElement>(null);
  const composerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const runtime = chatRuntimeRef.current;
    const composerLayer = composerRef.current?.parentElement;
    if (!runtime || !composerLayer) return;

    const updateComposerHeight = () => {
      runtime.style.setProperty(
        "--chat-composer-height",
        `${Math.ceil(composerLayer.getBoundingClientRect().height)}px`,
      );
    };
    updateComposerHeight();

    const observer = new ResizeObserver(updateComposerHeight);
    observer.observe(composerLayer);
    return () => observer.disconnect();
  }, []);

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
      toolVariant="compact"
      onPermissionDecision={(resolution, message) => {
        void resolvePermission(resolution, message);
      }}
      asFragment
    >
      <ChatContainer className="chat-shell">
        <div ref={chatRuntimeRef} className="chat-runtime">
          <ChatDock
            className="chat-dock"
            centered={chat.messages.length === 0}
            contentClassName="chat-dock-content"
            transcript={
              <ChatViewport
                className="chat-transcript"
                contentClassName="chat-transcript-content"
              >
                {chat.messages.length > 0 && (
                  <ChatMessageList busy={chat.isLoading}>
                    {chat.messages.map((message, index) =>
                      waitingForFirstOutput && index === chat.messages.length - 1 ? (
                        <div key={message.id} className="chat-response-loading" role="status">
                          <LoadingShimmer>正在思考</LoadingShimmer>
                          <ThinkingDots />
                        </div>
                      ) : (
                        <Message
                          key={message.id}
                          message={message}
                          hideActions={message.status === "streaming"}
                          onRegenerate={() => void chat.regenerate()}
                          onRetry={() => void chat.regenerate()}
                        />
                      ),
                    )}
                  </ChatMessageList>
                )}
              </ChatViewport>
            }
            intro={
              <div className="chat-intro">
                <ChatEmptyState
                  className="chat-empty-state"
                  title="开始对话"
                  subtitle="无需登录，直接发送一条消息。"
                />
                <SuggestionChips
                  suggestions={["你好，Mina"]}
                  onSelect={(text) => void chat.send(text)}
                  className="chat-suggestions"
                />
              </div>
            }
          >
            <div ref={composerRef} className="chat-composer-stack">
              {(chat.error || permissionError || attachmentError) && (
                <p className="chat-error" role="alert">
                  {attachmentError ?? permissionError ?? chat.error?.message}
                </p>
              )}
              {currentRunId && !chat.isLoading && (
                <div className="paused-run-banner" role="status">
                  <span>当前 Run 已挂起，等待审批或外部事件。</span>
                  <button
                    type="button"
                    onClick={() => void cancelCurrentRun()}
                    disabled={cancelling}
                  >
                    {cancelling ? "取消中…" : "取消当前任务"}
                  </button>
                </div>
              )}
              <PromptInput
                onSubmit={(text, options) => {
                  setAttachmentError(null);
                  void chat.send(text, { parts: options.parts });
                }}
                onStop={() => void cancelCurrentRun()}
                streaming={chat.isLoading || cancelling}
                disabled={Boolean(currentRunId) && !chat.isLoading}
                placeholder={currentRunId ? "当前 Session 有任务正在执行" : "给 Mina 发消息"}
                attachments={attachmentAccept ? attachments : undefined}
                showImageButton={effectiveInputModalities.includes("image")}
                autoFocus
                showHint
              />
            </div>
          </ChatDock>
        </div>
      </ChatContainer>
    </ChatThemeProvider>
  );
}

const DEFAULT_INPUT_MODALITIES = ["text", "image", "document"];

function isWaitingForFirstOutput(messages: ChatMessage[]) {
  const latest = messages.at(-1);
  if (latest?.role !== "assistant" || latest.status !== "streaming") return false;

  return latest.parts.every(
    (part) => part.type === "source" || (part.type === "text" && !part.text.trim()),
  );
}

async function uploadAttachment(file: File, context: { signal: AbortSignal }) {
  const query = new URLSearchParams({ name: file.name });
  const response = await fetch(`/api/v1/blobs?${query.toString()}`, {
    method: "POST",
    headers: { "Content-Type": file.type || "application/octet-stream" },
    body: file,
    signal: context.signal,
  });
  if (!response.ok) {
    const payload = (await response.json().catch(() => null)) as { message?: unknown } | null;
    throw new Error(
      typeof payload?.message === "string" ? payload.message : "附件上传失败，请重试",
    );
  }
  const payload = (await response.json()) as { url?: unknown };
  if (typeof payload.url !== "string") throw new Error("后端返回了无效的附件地址");
  return payload.url;
}

function attachmentAcceptFor(modalities: string[]) {
  const accepted: string[] = [];
  if (modalities.includes("image")) accepted.push("image/*");
  if (modalities.includes("audio")) accepted.push("audio/*");
  if (modalities.includes("video")) accepted.push("video/*");
  if (modalities.includes("document")) {
    accepted.push("application/pdf", "text/*", ".csv", ".json");
  }
  return accepted.join(",");
}

function formatAttachmentError(message: string, file: File) {
  if (message === "too-large") return `${file.name} 超过 10 MB，无法上传`;
  if (message === "too-many") return "一次最多上传 8 个文件";
  if (message === "wrong-type") return `${file.name} 的文件类型不受当前模型支持`;
  return `${file.name}：${message}`;
}
