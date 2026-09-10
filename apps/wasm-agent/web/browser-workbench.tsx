import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import type { ChatMessage } from "@xinjiyuan97/chat-core";
import {
  ConversationSidebar,
  RegenerateIcon,
  ThinkingIcon,
  ToolIcon,
} from "@xinjiyuan97/chat-ui";

import { BrowserChat } from "./chat";
import { downloadWorkspaceFile } from "./download-workspace";
import { readSkillDirectory } from "@mina/browser-agent";
import {
  browserModelLabel,
  browserToolNames,
  isBrowserModelConfigured,
  loadBrowserModelConfig,
  saveBrowserModelConfig,
  switchBrowserModelProtocol,
} from "./model-config";
import type {
  BrowserModelConfig,
  SkillInspection,
  WorkspaceDebugEntry,
  WorkspaceDebugSnapshot,
} from "@mina/browser-agent";
import {
  createSession,
  listActiveSessions,
  loadSessionMessages,
  toConversation,
  type SessionSnapshot,
} from "./session-store";
import { wasmRuntime, type RuntimeInfo } from "./runtime";

type InspectorTab = "system" | "memory" | "skills" | "tools" | "opfs";

const tabs: Array<{ id: InspectorTab; label: string }> = [
  { id: "system", label: "System" },
  { id: "memory", label: "Memory" },
  { id: "skills", label: "Skills" },
  { id: "tools", label: "Tools" },
  { id: "opfs", label: "OPFS" },
];

const ACTIVE_SESSION_KEY = "mina.browser.active-session-id";

export function BrowserWorkbench() {
  const [runtime, setRuntime] = useState<RuntimeInfo | null>(null);
  const [runtimeError, setRuntimeError] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState(true);
  const [activeTab, setActiveTab] = useState<InspectorTab>("system");
  const [inspectorOpen, setInspectorOpen] = useState(false);
  const [sessions, setSessions] = useState<SessionSnapshot[]>([]);
  const [activeSessionId, setActiveSessionId] = useState<string | null>(null);
  const [sessionMessages, setSessionMessages] = useState<ChatMessage[]>([]);
  const [sessionsLoading, setSessionsLoading] = useState(true);
  const [sessionError, setSessionError] = useState<string | null>(null);
  const [chatBusy, setChatBusy] = useState(false);
  const [workspaceRevision, setWorkspaceRevision] = useState(0);
  const [skillInspection, setSkillInspection] = useState<SkillInspection | null>(null);
  const [skillsLoading, setSkillsLoading] = useState(true);
  const [skillError, setSkillError] = useState<string | null>(null);
  const [sessionsCollapsed, setSessionsCollapsed] = useState(false);
  const [sessionsOpen, setSessionsOpen] = useState(false);
  const [modelConfig, setModelConfig] = useState(loadBrowserModelConfig);
  const sessionSwitchGeneration = useRef(0);

  useEffect(() => {
    saveBrowserModelConfig(modelConfig);
  }, [modelConfig]);

  const refreshRuntime = useCallback(async (quiet = false) => {
    if (!quiet) setRefreshing(true);
    try {
      setRuntime(await wasmRuntime.ready());
      setRuntimeError(null);
    } catch (cause) {
      setRuntimeError(cause instanceof Error ? cause.message : "WASM Worker 启动失败");
    } finally {
      if (!quiet) setRefreshing(false);
    }
  }, []);

  useEffect(() => {
    void refreshRuntime();
  }, [refreshRuntime]);

  const refreshSkills = useCallback(async () => {
    setSkillsLoading(true);
    setSkillError(null);
    try {
      setSkillInspection(await wasmRuntime.inspectSkills());
    } catch (cause) {
      setSkillError(cause instanceof Error ? cause.message : "无法读取 OPFS Skills");
    } finally {
      setSkillsLoading(false);
    }
  }, []);

  useEffect(() => {
    if (runtime) void refreshSkills();
  }, [refreshSkills, runtime]);

  useEffect(() => {
    const controller = new AbortController();
    let cancelled = false;
    void (async () => {
      try {
        let available = await listActiveSessions(controller.signal);
        if (available.length === 0) available = [await createSession(controller.signal)];
        const remembered = window.localStorage.getItem(ACTIVE_SESSION_KEY);
        const active = available.find((session) => session.session_id === remembered) ?? available[0];
        const messages = await loadSessionMessages(active.session_id, controller.signal);
        if (cancelled) return;
        setSessions(available);
        setActiveSessionId(active.session_id);
        setSessionMessages(messages);
        setSessionError(null);
        window.localStorage.setItem(ACTIVE_SESSION_KEY, active.session_id);
      } catch (cause) {
        if (!cancelled && !controller.signal.aborted) {
          setSessionError(cause instanceof Error ? cause.message : "读取本地会话失败");
        }
      } finally {
        if (!cancelled) setSessionsLoading(false);
      }
    })();
    return () => {
      cancelled = true;
      controller.abort();
    };
  }, []);

  function openInspector(tab: InspectorTab) {
    setActiveTab(tab);
    setInspectorOpen(true);
    setSessionsOpen(false);
  }

  const selectSession = useCallback(
    async (sessionId: string) => {
      setSessionsOpen(false);
      if (sessionId === activeSessionId) return;
      if (chatBusy) {
        setSessionError("当前浏览器任务执行中，请停止后再切换会话");
        return;
      }
      const generation = sessionSwitchGeneration.current + 1;
      sessionSwitchGeneration.current = generation;
      setActiveSessionId(sessionId);
      setSessionMessages([]);
      setSessionsLoading(true);
      setSessionError(null);
      window.localStorage.setItem(ACTIVE_SESSION_KEY, sessionId);
      try {
        const messages = await loadSessionMessages(sessionId);
        if (sessionSwitchGeneration.current === generation) setSessionMessages(messages);
      } catch (cause) {
        if (sessionSwitchGeneration.current === generation) {
          setSessionError(cause instanceof Error ? cause.message : "读取会话历史失败");
        }
      } finally {
        if (sessionSwitchGeneration.current === generation) setSessionsLoading(false);
      }
    },
    [activeSessionId, chatBusy],
  );

  const newSession = useCallback(async () => {
    if (chatBusy) {
      setSessionError("当前浏览器任务执行中，请停止后再新建会话");
      return;
    }
    const generation = sessionSwitchGeneration.current + 1;
    sessionSwitchGeneration.current = generation;
    setSessionsLoading(true);
    setSessionError(null);
    try {
      const created = await createSession();
      if (sessionSwitchGeneration.current !== generation) return;
      setSessions((current) => [created, ...current]);
      setActiveSessionId(created.session_id);
      setSessionMessages([]);
      setSessionsOpen(false);
      window.localStorage.setItem(ACTIVE_SESSION_KEY, created.session_id);
    } catch (cause) {
      if (sessionSwitchGeneration.current === generation) {
        setSessionError(cause instanceof Error ? cause.message : "创建本地会话失败");
      }
    } finally {
      if (sessionSwitchGeneration.current === generation) setSessionsLoading(false);
    }
  }, [chatBusy]);

  const handleRunFinished = useCallback((sessionId: string) => {
    setWorkspaceRevision((revision) => revision + 1);
    void listActiveSessions()
      .then((available) => {
        setSessions(available);
        if (!available.some((session) => session.session_id === sessionId)) {
          setSessionError("完成的本地会话未写入 OPFS");
        }
      })
      .catch((cause: unknown) => {
        setSessionError(cause instanceof Error ? cause.message : "刷新本地会话失败");
      });
  }, []);

  const activeSession = sessions.find((session) => session.session_id === activeSessionId) ?? null;
  const conversations = useMemo(() => sessions.map(toConversation), [sessions]);
  const activeTitle = activeSession ? toConversation(activeSession).title : "Conversation";
  const statusError = sessionError ?? runtimeError;
  const modelConfigured = isBrowserModelConfigured(modelConfig);
  const modelLabel = browserModelLabel(modelConfig);
  const toolNames = browserToolNames(modelConfig);

  return (
    <main
      className="workbench-shell"
      data-session-sidebar-collapsed={sessionsCollapsed || undefined}
    >
      <header className="workbench-header">
        <div className="brand-lockup">
          <span className="brand-mark" aria-hidden="true">M</span>
          <div>
            <h1>Mina</h1>
            <p>Browser Agent</p>
          </div>
        </div>

        <div className="header-runtime" aria-label="当前运行时">
          <span className="status-dot" data-ready={modelConfigured || undefined} aria-hidden="true" />
          <span>{runtime ? "WASM Worker" : "connecting"}</span>
          <span className="header-divider" />
          <span className="header-model">{modelLabel}</span>
        </div>

        <div className="header-actions">
          <button
            className="mobile-session-trigger"
            type="button"
            onClick={() => {
              setSessionsCollapsed(false);
              setSessionsOpen((open) => !open);
              setInspectorOpen(false);
            }}
            aria-expanded={sessionsOpen}
          >
            Sessions <span>{sessions.length}</span>
          </button>
          <button
            className="mobile-inspector-trigger"
            type="button"
            onClick={() => setInspectorOpen((open) => !open)}
            aria-expanded={inspectorOpen}
          >
            Context <span>{runtime ? 1 : 0}</span>
          </button>
          <button
            className="refresh-button"
            type="button"
            onClick={() => void refreshRuntime()}
            disabled={refreshing}
            aria-label="刷新 WASM Agent 状态"
            title="刷新 WASM Agent 状态"
          >
            <RegenerateIcon className={refreshing ? "is-spinning" : undefined} />
          </button>
        </div>
      </header>

      <div className="workbench-body">
        <ConversationSidebar
          className={`agent-rail session-sidebar${sessionsOpen ? " is-open" : ""}`}
          conversations={conversations}
          activeId={activeSessionId ?? undefined}
          activeIndicator="none"
          loading={sessionsLoading && sessions.length === 0}
          collapsed={sessionsCollapsed}
          onCollapsedChange={setSessionsCollapsed}
          onNewChat={newSession}
          onSelect={(sessionId) => void selectSession(sessionId)}
          footer={
            sessionsCollapsed ? (
              <button
                className="collapsed-context-button"
                type="button"
                onClick={() => openInspector("system")}
                title="Browser Context"
                aria-label="打开 Browser Context"
              >
                <ThinkingIcon size={16} />
              </button>
            ) : (
              <div className="session-sidebar-footer">
                <div className="session-agent-summary">
                  <span className="status-dot" aria-hidden="true" />
                  <span>{runtime ? "WASM Worker" : "connecting"}</span>
                  <small>agent-core · OPFS</small>
                </div>
                <nav className="sidebar-context-nav" aria-label="Agent 上下文面板">
                  <button type="button" onClick={() => openInspector("system")}>
                    System <strong>1</strong>
                  </button>
                  <button type="button" onClick={() => openInspector("memory")}>
                    Memory <strong>0</strong>
                  </button>
                  <button type="button" onClick={() => openInspector("skills")}>
                    Skills <strong>{skillInspection?.packages.length ?? 0}</strong>
                  </button>
                  <button type="button" onClick={() => openInspector("tools")}>
                    Tools <strong>{toolNames.length}</strong>
                  </button>
                  <button type="button" onClick={() => openInspector("opfs")}>
                    OPFS <strong>debug</strong>
                  </button>
                </nav>
                {statusError && <p className="session-sidebar-error" role="alert">{statusError}</p>}
              </div>
            )
          }
        />

        <section className="playground-pane" aria-label="Agent 对话">
          <div className="pane-header">
            <div>
              <p className="eyebrow">PLAYGROUND</p>
              <h2>{activeTitle}</h2>
            </div>
            <span className="pane-status">
              {chatBusy ? "Running in Worker" : activeSession ? "OPFS persistent" : "Loading"}
            </span>
          </div>
          {activeSession && !sessionsLoading && modelConfigured ? (
            <BrowserChat
              key={activeSession.session_id}
              session={activeSession}
              initialMessages={sessionMessages}
              modelConfig={modelConfig}
              onRunFinished={handleRunFinished}
              onBusyChange={setChatBusy}
            />
          ) : activeSession && !sessionsLoading ? (
            <div className="session-loading-state model-setup-state" role="status">
              <ThinkingIcon size={20} />
              <strong>配置浏览器模型</strong>
              <p>选择 Responses 或 Messages，填写模型和 API key 后即可直接运行。</p>
              <button type="button" onClick={() => openInspector("system")}>打开模型配置</button>
            </div>
          ) : (
            <div className="session-loading-state" role="status">
              <ThinkingIcon size={20} />
              <strong>{sessionError ? "Session unavailable" : "Loading conversation"}</strong>
              <p>{sessionError ?? "正在从 OPFS 读取会话与消息…"}</p>
              {sessionError && <button type="button" onClick={() => void newSession()}>新建会话</button>}
            </div>
          )}
        </section>

        <aside
          className={`inspector-pane${inspectorOpen ? " is-open" : ""}`}
          aria-label="Agent 上下文检查器"
        >
          <div className="inspector-heading">
            <div>
              <p className="eyebrow">INSPECTOR</p>
              <h2>Browser Context</h2>
            </div>
            <button className="inspector-close" type="button" onClick={() => setInspectorOpen(false)}>
              Close
            </button>
          </div>
          <div className="inspector-tabs" role="tablist" aria-label="上下文类型">
            {tabs.map((tab) => (
              <button
                key={tab.id}
                type="button"
                role="tab"
                aria-selected={activeTab === tab.id}
                className={activeTab === tab.id ? "is-active" : undefined}
                onClick={() => setActiveTab(tab.id)}
              >
                {tab.label}
                <span>
                  {tab.id === "system"
                    ? 1
                    : tab.id === "tools"
                      ? toolNames.length
                      : tab.id === "skills"
                        ? skillInspection?.packages.length ?? 0
                      : tab.id === "opfs"
                        ? "FS"
                        : 0}
                </span>
              </button>
            ))}
          </div>
          <div className="inspector-content">
            <BrowserInspector
              tab={activeTab}
              runtime={runtime}
              modelConfig={modelConfig}
              toolNames={toolNames}
              workspaceRevision={workspaceRevision}
              skillInspection={skillInspection}
              skillsLoading={skillsLoading}
              skillError={skillError}
              skillManagementDisabled={chatBusy}
              modelConfigDisabled={chatBusy}
              onModelConfigChange={setModelConfig}
              onRefreshSkills={refreshSkills}
            />
          </div>
        </aside>
      </div>
    </main>
  );
}

function BrowserInspector({
  tab,
  runtime,
  modelConfig,
  toolNames,
  workspaceRevision,
  skillInspection,
  skillsLoading,
  skillError,
  skillManagementDisabled,
  modelConfigDisabled,
  onModelConfigChange,
  onRefreshSkills,
}: {
  tab: InspectorTab;
  runtime: RuntimeInfo | null;
  modelConfig: BrowserModelConfig;
  toolNames: string[];
  workspaceRevision: number;
  skillInspection: SkillInspection | null;
  skillsLoading: boolean;
  skillError: string | null;
  skillManagementDisabled: boolean;
  modelConfigDisabled: boolean;
  onModelConfigChange: (config: BrowserModelConfig) => void;
  onRefreshSkills: () => Promise<void>;
}) {
  if (tab === "system") {
    return (
      <section className="inspection-section">
        <div className="inspection-meta stacked">
          <span>Runtime</span><code>Dedicated Worker</code>
          <span>Reducer</span><code>agent-core / protocol v{runtime?.reducerProtocolVersion ?? "—"}</code>
          <span>Checkpoint</span><code>OPFS / schema v{runtime?.checkpointSchemaVersion ?? "—"}</code>
          <span>Title</span><code>WASM reducer / protocol v{runtime?.titleProtocolVersion ?? "—"}</code>
          <span>Skills</span><code>{runtime ? `${runtime.skillProvider} / compiler v${runtime.skillCompilerVersion}` : "—"}</code>
        </div>
        <p className="section-help">每次模型或工具副作用执行前，Rust reducer 状态都会先写入 OPFS。</p>
        <form className="model-config-form" onSubmit={(event) => event.preventDefault()}>
          <label>
            <span>Protocol</span>
            <select
              disabled={modelConfigDisabled}
              value={modelConfig.protocol}
              onChange={(event) =>
                onModelConfigChange(
                  switchBrowserModelProtocol(
                    modelConfig,
                    event.target.value as BrowserModelConfig["protocol"],
                  ),
                )
              }
            >
              <option value="responses">Responses</option>
              <option value="messages">Messages</option>
            </select>
          </label>
          <label>
            <span>Base URL</span>
            <input
              disabled={modelConfigDisabled}
              type="url"
              value={modelConfig.baseUrl}
              onChange={(event) =>
                onModelConfigChange({ ...modelConfig, baseUrl: event.target.value })
              }
              spellCheck={false}
            />
          </label>
          <label>
            <span>Model</span>
            <input
              disabled={modelConfigDisabled}
              value={modelConfig.model}
              onChange={(event) =>
                onModelConfigChange({ ...modelConfig, model: event.target.value })
              }
              spellCheck={false}
            />
          </label>
          <label>
            <span>API key</span>
            <input
              disabled={modelConfigDisabled}
              type="password"
              value={modelConfig.apiKey}
              onChange={(event) =>
                onModelConfigChange({ ...modelConfig, apiKey: event.target.value })
              }
              autoComplete="off"
              placeholder="只保留在当前页面内存"
            />
          </label>
          <label className="model-config-checkbox">
            <input
              disabled={modelConfigDisabled}
              type="checkbox"
              checked={modelConfig.enableWebSearch}
              onChange={(event) =>
                onModelConfigChange({ ...modelConfig, enableWebSearch: event.target.checked })
              }
            />
            <span>启用 provider 自带 Web Search</span>
          </label>
          <p className="model-config-note">
            API key 不会写入 OPFS、checkpoint 或 localStorage，刷新页面后需要重新填写。
          </p>
        </form>
        <pre className="system-prompt">You are Mina running locally inside a browser Worker.</pre>
      </section>
    );
  }
  if (tab === "tools") {
    return (
      <section className="inspection-section">
        <div className="runtime-boundaries">
          <div><span>Agent core</span><code>WASM</code><strong>isolated</strong></div>
          <div><span>Effect host</span><code>Browser JS</code><strong>local</strong></div>
        </div>
        <div className="browser-tool-list">
          {toolNames.map((name) => (
            <div className="browser-tool-row" key={name}>
              <ToolIcon size={16} />
              <code>{name}</code>
              <span>{browserToolDescription(name)}</span>
            </div>
          ))}
        </div>
        <p className="section-help">
          Provider Web Search 由模型 API 执行；文件写入 app-private OPFS，图片工具执行前需要用户批准。
        </p>
      </section>
    );
  }
  if (tab === "skills") {
    return (
      <SkillInspector
        inspection={skillInspection}
        loading={skillsLoading}
        error={skillError}
        disabled={skillManagementDisabled}
        onRefresh={onRefreshSkills}
      />
    );
  }
  if (tab === "opfs") return <OpfsInspector refreshToken={workspaceRevision} />;
  return (
    <div className="empty-inspector">
      <ThinkingIcon size={20} />
      <strong>Memory is local-only</strong>
      <p>会话历史与运行 checkpoint 已保存在当前浏览器的 OPFS。</p>
    </div>
  );
}

function SkillInspector({
  inspection,
  loading,
  error,
  disabled,
  onRefresh,
}: {
  inspection: SkillInspection | null;
  loading: boolean;
  error: string | null;
  disabled: boolean;
  onRefresh: () => Promise<void>;
}) {
  const inputRef = useRef<HTMLInputElement>(null);
  const [importing, setImporting] = useState(false);
  const [importError, setImportError] = useState<string | null>(null);
  const [importResult, setImportResult] = useState<string | null>(null);

  async function importDirectory(files: FileList | null) {
    if (!files?.length) return;
    setImporting(true);
    setImportError(null);
    setImportResult(null);
    try {
      const result = await wasmRuntime.installSkill(await readSkillDirectory(files));
      const label = `${result.descriptor.skill_id}@${result.descriptor.version}`;
      setImportResult(
        result.status === "installed" ? `已安装 ${label}` : `${label} 已安装，无需重复导入`,
      );
      await onRefresh();
    } catch (cause) {
      setImportError(cause instanceof Error ? cause.message : "Skill 导入失败");
    } finally {
      setImporting(false);
      if (inputRef.current) inputRef.current.value = "";
    }
  }

  return (
    <section className="inspection-section skill-inspector">
      <div className="skill-inspector-header">
        <div>
          <span>OpfsSkillStore</span>
          <code>{inspection?.root ?? "opfs://mina-browser-agent/skills"}</code>
        </div>
        <div className="skill-inspector-actions">
          <input
            ref={(node) => {
              inputRef.current = node;
              node?.setAttribute("webkitdirectory", "");
            }}
            type="file"
            multiple
            // Directory selection is intentionally management-only; selected
            // bytes are validated by WASM before the atomic OPFS install.
            onChange={(event) => void importDirectory(event.currentTarget.files)}
            disabled={disabled || importing}
            aria-label="选择 Skill 目录"
          />
          <button
            type="button"
            onClick={() => inputRef.current?.click()}
            disabled={disabled || importing}
          >
            {importing ? "导入中" : "导入目录"}
          </button>
          <button type="button" onClick={() => void onRefresh()} disabled={loading || importing}>
            {loading ? "读取中" : "刷新"}
          </button>
        </div>
      </div>
      <p className="section-help">
        Runtime 只读。导入目录需要包含 skill.toml 与 SKILL.md；WASM 完整校验通过后才会提交安装，新 Run 会执行默认和 hints 路由并锁定 digest。
      </p>
      {error && <p className="opfs-debug-error" role="alert">{error}</p>}
      {importError && <p className="opfs-debug-error" role="alert">{importError}</p>}
      {importResult && <p className="skill-import-success" role="status">{importResult}</p>}
      {!loading && inspection?.packages.length === 0 && (
        <div className="opfs-debug-empty">
          <strong>没有已安装的 Skill</strong>
          <p>点击“导入目录”，选择一个本地 Skill package。</p>
        </div>
      )}
      {inspection && inspection.packages.length > 0 && (
        <div className="skill-package-list">
          {inspection.packages.map((skill) => (
            <article key={`${skill.skill_id}@${skill.version}#${skill.digest}`}>
              <div>
                <strong>{skill.skill_id}</strong>
                <code>{skill.version}</code>
              </div>
              <p>{skill.description}</p>
              <div className="skill-package-meta">
                <span>{skillActivationLabel(skill.activation)}</span>
                <code title={skill.digest}>{shortDigest(skill.digest)}</code>
              </div>
              {skill.activation.type === "routable" && skill.activation.hints.length > 0 && (
                <small>{skill.activation.hints.join(" · ")}</small>
              )}
            </article>
          ))}
        </div>
      )}
    </section>
  );
}

function skillActivationLabel(activation: SkillInspection["packages"][number]["activation"]) {
  switch (activation.type) {
    case "profile_default": return "profile default";
    case "routable": return "routable";
    default: return "explicit only";
  }
}

function shortDigest(digest: string) {
  const value = digest.replace(/^sha256:/, "");
  return `sha256:${value.slice(0, 10)}…`;
}

function OpfsInspector({ refreshToken }: { refreshToken: number }) {
  const [snapshot, setSnapshot] = useState<WorkspaceDebugSnapshot | null>(null);
  const [selectedPath, setSelectedPath] = useState<string | null>(null);
  const [preview, setPreview] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [downloadingPath, setDownloadingPath] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const result = await wasmRuntime.inspectWorkspace();
      if (result.type !== "snapshot") throw new Error("OPFS Inspector 返回了错误的数据类型");
      setSnapshot(result.snapshot);
      if (
        selectedPath &&
        !result.snapshot.entries.some(
          (entry) => entry.kind === "file" && entry.path === selectedPath,
        )
      ) {
        setSelectedPath(null);
        setPreview(null);
      }
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "无法读取 OPFS workspace");
    } finally {
      setLoading(false);
    }
  }, [selectedPath]);

  useEffect(() => {
    void refresh();
  }, [refresh, refreshToken]);

  async function openEntry(entry: WorkspaceDebugEntry) {
    if (entry.kind !== "file") return;
    setSelectedPath(entry.path);
    setError(null);
    if (!isTextPreview(entry.path)) {
      setPreview("二进制文件只显示元数据，不读取正文。可由 Agent 的图片工具继续处理。");
      return;
    }
    setPreviewLoading(true);
    setPreview(null);
    try {
      const result = await wasmRuntime.inspectWorkspace(entry.path);
      if (result.type !== "file") throw new Error("OPFS Inspector 返回了错误的数据类型");
      setPreview(result.content);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "无法读取 OPFS 文件");
    } finally {
      setPreviewLoading(false);
    }
  }

  async function downloadEntry(path: string) {
    setDownloadingPath(path);
    setError(null);
    try {
      await downloadWorkspaceFile(path);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "无法下载 OPFS 文件");
    } finally {
      setDownloadingPath(null);
    }
  }

  return (
    <section className="inspection-section opfs-debugger">
      <div className="opfs-debug-header">
        <div>
          <span>WorkspaceFs</span>
          <code>{snapshot?.root ?? "opfs://mina-browser-agent/files"}</code>
        </div>
        <button type="button" onClick={() => void refresh()} disabled={loading}>
          {loading ? "读取中" : "刷新"}
        </button>
      </div>
      {snapshot && (
        <div className="opfs-debug-facts">
          <span>{snapshot.descriptor.kind}</span>
          <span>{snapshot.descriptor.persistent ? "persistent" : "ephemeral"}</span>
          <span>{snapshot.capabilities.writable ? "read / write" : "read only"}</span>
        </div>
      )}
      {error && <p className="opfs-debug-error" role="alert">{error}</p>}
      {!loading && snapshot?.entries.length === 0 && (
        <div className="opfs-debug-empty">
          <strong>Workspace 为空</strong>
          <p>让 Agent 使用 write 工具创建文件后，可在这里检查。</p>
        </div>
      )}
      {snapshot && snapshot.entries.length > 0 && (
        <div className="opfs-tree" role="tree" aria-label="OPFS workspace 文件">
          {snapshot.entries.map((entry) => {
            const content = (
              <>
                <span className="opfs-entry-kind">{entry.kind === "directory" ? "dir" : "file"}</span>
                <span className="opfs-entry-name">{entry.name}</span>
                <span className="opfs-entry-size">
                  {entry.kind === "file" ? formatBytes(entry.size) : ""}
                </span>
              </>
            );
            const style = { paddingLeft: `${0.55 + pathDepth(entry.path) * 0.75}rem` };
            return entry.kind === "file" ? (
              <div
                key={entry.path}
                role="treeitem"
                aria-selected={selectedPath === entry.path}
                className={`opfs-file-row${selectedPath === entry.path ? " is-selected" : ""}`}
                style={style}
              >
                <button
                  type="button"
                  className="opfs-entry-open"
                  onClick={() => void openEntry(entry)}
                >
                  {content}
                </button>
                <button
                  type="button"
                  className="opfs-entry-download"
                  onClick={() => void downloadEntry(entry.path)}
                  disabled={downloadingPath === entry.path}
                  aria-label={`下载 ${entry.name}`}
                >
                  {downloadingPath === entry.path ? "下载中" : "下载"}
                </button>
              </div>
            ) : (
              <div key={entry.path} role="treeitem" className="is-directory" style={style}>
                {content}
              </div>
            );
          })}
          {snapshot.truncated && <p className="opfs-debug-truncated">仅显示前 500 个条目</p>}
        </div>
      )}
      {selectedPath && (
        <div className="opfs-preview">
          <div>
            <span>Preview</span>
            <code>{selectedPath}</code>
            <button
              type="button"
              onClick={() => void downloadEntry(selectedPath)}
              disabled={downloadingPath === selectedPath}
            >
              {downloadingPath === selectedPath ? "下载中" : "下载文件"}
            </button>
          </div>
          <pre>{previewLoading ? "读取中…" : preview ?? ""}</pre>
        </div>
      )}
    </section>
  );
}

function browserToolDescription(name: string) {
  switch (name) {
    case "read": return "读取 Workspace 文本文件";
    case "write": return "创建或覆盖 Workspace 文件";
    case "edit": return "精确修改 Workspace 文件";
    case "javascript_eval": return "执行 JavaScript（QuickJS）";
    case "list_directory": return "列出 Workspace 目录";
    case "read_skill_resource": return "读取当前 Run 锁定的 Skill 资源";
    case "generate_image": return "生成图片并保存";
    case "edit_image": return "编辑 OPFS 图片";
    default: return "Browser handler";
  }
}

function pathDepth(path: string) {
  return Math.max(0, path.split("/").length - 1);
}

function isTextPreview(path: string) {
  return !/\.(?:png|jpe?g|webp|gif|pdf|zip|docx?|pptx?|xlsx?)$/i.test(path);
}

function formatBytes(bytes: number) {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}
