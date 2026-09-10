import { browserAttachmentStore } from "./runtime";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import {
  isPermissionPart,
  useAttachments,
  useChat,
  type ChatMessage,
  type PermissionResolution,
  type ToolPart,
} from "@xinjiyuan97/chat-core";
import {
  ChatContainer,
  ChatDock,
  ChatEmptyState,
  ChatMessageList,
  ChatThemeProvider,
  ChatViewport,
  FileIcon,
  LoadingShimmer,
  Message,
  PromptInput,
  SearchIcon,
  SuggestionChips,
  ThinkingDots,
  type ToolDefinition,
} from "@xinjiyuan97/chat-ui";

import type { SessionSnapshot } from "./session-store";
import type { BrowserModelConfig } from "@mina/browser-agent";
import {
  MAX_ATTACHMENTS_PER_MESSAGE,
  MAX_ATTACHMENT_BYTES,
} from "@mina/browser-agent";
import { wasmRuntime } from "./runtime";
import { createWasmTransport } from "./wasm-transport";
import { downloadWorkspaceFile } from "./download-workspace";

const workspaceFileTools: Record<string, ToolDefinition> = {
  read: workspaceFileTool("正在读取文件", "已读取文件"),
  write: workspaceFileTool("正在保存文件", "已保存文件"),
  edit: workspaceFileTool("正在更新文件", "已更新文件"),
  generate_image: workspaceFileTool("正在生成图片", "已生成图片"),
  edit_image: workspaceFileTool("正在编辑图片", "已编辑图片"),
};

export function BrowserChat({
  session,
  initialMessages,
  onRunFinished,
  onBusyChange,
  modelConfig,
}: {
  session: SessionSnapshot;
  initialMessages: ChatMessage[];
  onRunFinished?: (sessionId: string) => void;
  onBusyChange?: (busy: boolean) => void;
  modelConfig: BrowserModelConfig;
}) {
  const [currentRunId, setCurrentRunId] = useState(session.active_run_id);
  const [runToRestore] = useState(() => session.active_run_id);
  const handleRunStarted = useCallback((runId: string) => setCurrentRunId(runId), []);
  const handleRunFinished = useCallback(
    (sessionId: string) => {
      setCurrentRunId(undefined);
      onBusyChange?.(false);
      onRunFinished?.(sessionId);
    },
    [onBusyChange, onRunFinished],
  );
  const transport = useMemo(
    () =>
      createWasmTransport({
        sessionId: session.session_id,
        activeRunId: session.active_run_id,
        modelConfig,
        onRunStarted: handleRunStarted,
        onRunFinished: handleRunFinished,
      }),
    [handleRunFinished, handleRunStarted, modelConfig, session.active_run_id, session.session_id],
  );
  const chat = useChat({ transport, initialMessages });
  const submit = chat.submit;
  const waitingForFirstOutput = chat.isLoading && isWaitingForFirstOutput(chat.messages);
  const [permissionError, setPermissionError] = useState<string | null>(null);
  const [attachmentError, setAttachmentError] = useState<string | null>(null);
  const attachments = useAttachments({
    accept: attachmentAccept(modelConfig.protocol),
    maxFiles: MAX_ATTACHMENTS_PER_MESSAGE,
    maxSize: MAX_ATTACHMENT_BYTES,
    onUpload: uploadAttachment,
    onError: (message, file) => setAttachmentError(formatAttachmentError(message, file)),
  });
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
    if (!runToRestore) return;
    const timer = window.setTimeout(() => {
      void submit({ body: { minaResumeRun: { runId: runToRestore } } });
    }, 0);
    return () => window.clearTimeout(timer);
  }, [runToRestore, submit]);

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
      setPermissionError("找不到对应的工具审批请求");
      return;
    }
    const runId = permission.request.metadata?.runId;
    if (typeof runId !== "string") {
      setPermissionError("工具审批请求缺少 run_id");
      return;
    }
    try {
      await wasmRuntime.resolveApproval({
        runId,
        approvalId: resolution.requestId,
        decision: resolution.decision === "allow-once" ? "allow-once" : "deny",
        reason: resolution.reason,
      });
    } catch (cause) {
      setPermissionError(cause instanceof Error ? cause.message : "提交工具审批失败");
    }
  }

  function dismissError() {
    setPermissionError(null);
    setAttachmentError(null);
    if (chat.error) {
      chat.store.setState({ error: null, status: "idle" });
    }
  }

  return (
    <ChatThemeProvider
      locale="zh-CN"
      toolVariant="compact"
      tools={{
        ...workspaceFileTools,
        web_search: {
          label: (part) =>
            part.state === "output-available" ? "已完成网页搜索" : "正在搜索网页",
          icon: SearchIcon,
          runningMotion: "pulse",
          summary: webSearchSummary,
          tone: "accent",
        },
      }}
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
                          <LoadingShimmer>正在浏览器中思考</LoadingShimmer>
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
                  subtitle="无需后端，会话和 checkpoint 只保存在当前浏览器。"
                />
                <SuggestionChips
                  suggestions={["你好，Mina", "验证一下浏览器 WASM Agent"]}
                  onSelect={(text) => void chat.send(text)}
                  className="chat-suggestions"
                />
              </div>
            }
          >
            <div ref={composerRef} className="chat-composer-stack">
              {(chat.error || permissionError || attachmentError) && (
                <div className="chat-error" role="alert">
                  <span>{attachmentError ?? permissionError ?? chat.error?.message}</span>
                  <button type="button" onClick={dismissError} aria-label="关闭错误提示">
                    关闭
                  </button>
                </div>
              )}
              <PromptInput
                onSubmit={(text, options) => {
                  setAttachmentError(null);
                  void chat.send(text, { parts: options.parts });
                }}
                onStop={() => chat.stop()}
                streaming={chat.isLoading}
                disabled={Boolean(currentRunId) && !chat.isLoading}
                placeholder={currentRunId ? "当前浏览器任务正在执行" : "给 Mina 发消息"}
                attachments={attachments}
                showImageButton
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

function isWaitingForFirstOutput(messages: ChatMessage[]) {
  const latest = messages.at(-1);
  if (latest?.role !== "assistant" || latest.status !== "streaming") return false;
  return latest.parts.every(
    (part) => part.type === "source" || (part.type === "text" && !part.text.trim()),
  );
}

function webSearchSummary(part: ToolPart) {
  const details = part.state === "output-available" ? part.output : part.input;
  if (!isRecord(details)) return undefined;
  const action = isRecord(details.action) ? details.action : details;
  if (typeof action.query === "string") return action.query;
  if (typeof action.url === "string") return action.url;
  return undefined;
}

function workspaceFileTool(runningLabel: string, completedLabel: string): ToolDefinition {
  return {
    render: ({ part }) => (
      <WorkspaceFileTool
        part={part}
        runningLabel={runningLabel}
        completedLabel={completedLabel}
      />
    ),
  };
}

function WorkspaceFileTool({
  part,
  runningLabel,
  completedLabel,
}: {
  part: ToolPart;
  runningLabel: string;
  completedLabel: string;
}) {
  const [downloading, setDownloading] = useState(false);
  const [downloadError, setDownloadError] = useState<string | null>(null);
  const path = filePath(part.output) ?? filePath(part.input);
  const running = ["input-streaming", "input-available", "executing"].includes(part.state);
  const failed = part.state === "output-error";
  const label = failed ? "文件操作失败" : running ? runningLabel : completedLabel;

  async function download() {
    if (!path || downloading) return;
    setDownloading(true);
    setDownloadError(null);
    try {
      await downloadWorkspaceFile(path);
    } catch (cause) {
      setDownloadError(cause instanceof Error ? cause.message : "无法下载文件");
    } finally {
      setDownloading(false);
    }
  }

  return (
    <div className={`workspace-file-tool${failed ? " is-error" : ""}`}>
      <FileIcon size={16} />
      <div className="workspace-file-tool-copy">
        <strong>{label}</strong>
        {path && <code title={path}>{path}</code>}
        {(part.error || downloadError) && (
          <span role="alert">{downloadError ?? part.error}</span>
        )}
      </div>
      {!failed && !running && path && (
        <button type="button" onClick={() => void download()} disabled={downloading}>
          {downloading ? "下载中" : "下载"}
        </button>
      )}
    </div>
  );
}

function filePath(value: unknown) {
  if (!isRecord(value)) return undefined;
  return typeof value.path === "string" && value.path ? value.path : undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

async function uploadAttachment(file: File, context: { signal: AbortSignal }) {
  const metadata = await browserAttachmentStore.put(file, context.signal);
  return browserAttachmentStore.displayUrl(metadata.blob_id, context.signal);
}

function attachmentAccept(protocol: BrowserModelConfig["protocol"]) {
  const images = ["image/png", "image/jpeg", "image/webp", "image/gif", ".png", ".jpg", ".jpeg", ".webp", ".gif"];
  if (protocol === "messages") return [...images, "application/pdf", ".pdf"].join(",");
  return [
    ...images,
    "application/pdf",
    "text/plain",
    "text/markdown",
    "text/csv",
    "application/json",
    ".pdf",
    ".txt",
    ".md",
    ".markdown",
    ".csv",
    ".json",
    ".doc",
    ".docx",
    ".ppt",
    ".pptx",
    ".xls",
    ".xlsx",
  ].join(",");
}

function formatAttachmentError(message: string, file: File) {
  if (message === "too-large" || message === "attachment_too_large") {
    return `${file.name} 超过 10 MB，无法添加`;
  }
  if (message === "too-many") return `一次最多添加 ${MAX_ATTACHMENTS_PER_MESSAGE} 个附件`;
  if (message === "wrong-type" || message === "unsupported_attachment") {
    return `${file.name} 的格式不受当前协议支持`;
  }
  return `${file.name}：${message}`;
}
