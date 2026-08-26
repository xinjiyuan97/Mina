"use client";

import { useCallback, useEffect, useState } from "react";
import { RegenerateIcon, ThinkingIcon, ToolIcon } from "@xinjiyuan97/chat-ui";

import { Chat } from "./chat";

type AgentMetadata = {
  name: string;
  version: string;
  protocol_version: string;
  capabilities: string[];
};

type ModelInfo = {
  profile: string;
  provider: string;
  model: string;
  input_modalities: string[];
  output_modalities: string[];
};

type ToolDefinition = {
  name: string;
  description: string;
  input_schema: Record<string, unknown>;
  risk_level: "low" | "medium" | "high";
};

type SkillDescriptor = {
  skill_id: string;
  version: string;
  description: string;
  digest: string;
  store_identity: string;
  activation:
    | { type: "explicit_only" }
    | { type: "profile_default" }
    | { type: "routable"; hints: string[] };
};

type ContentPart = {
  type: string;
  text?: string;
};

type MemoryRecord = {
  memory_id: string;
  scope: string;
  kind: "semantic" | "episodic";
  content: ContentPart[];
  confidence: number;
  salience: number;
  version: number;
  created_at_ms: number;
  source_refs: Array<Record<string, unknown>>;
};

type AgentInspection = {
  service: string;
  version: string;
  agent: AgentMetadata;
  model: ModelInfo | null;
  system: {
    source: string;
    content: string;
  };
  skills: {
    store: {
      identity: string;
      kind: string;
      version: string;
    };
    packages: SkillDescriptor[];
  };
  tools: ToolDefinition[];
  tool_runtime: {
    process_sandbox: {
      identity: string;
      kind: string;
      version: string;
      isolation: "none" | "process" | "container" | "virtual_machine";
      network_isolated: boolean;
      filesystem_isolated: boolean;
      resource_limited: boolean;
    };
    search_backend: {
      identity: string;
      kind: string;
      version: string;
      external_network: boolean;
    };
  } | null;
  memory: {
    enabled: boolean;
    scopes: string[];
    store: {
      identity: string;
      kind: string;
      version: string;
    };
    records: MemoryRecord[];
    has_more: boolean;
  };
};

type InspectorTab = "system" | "memory" | "skills" | "tools";

const tabs: Array<{ id: InspectorTab; label: string }> = [
  { id: "system", label: "System" },
  { id: "memory", label: "Memory" },
  { id: "skills", label: "Skills" },
  { id: "tools", label: "Tools" },
];

export function AgentWorkbench() {
  const [inspection, setInspection] = useState<AgentInspection | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState(true);
  const [activeTab, setActiveTab] = useState<InspectorTab>("skills");
  const [inspectorOpen, setInspectorOpen] = useState(false);

  const refresh = useCallback(async (quiet = false) => {
    if (!quiet) setRefreshing(true);
    try {
      setInspection(await fetchAgentInspection());
      setError(null);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "读取 Agent 状态失败");
    } finally {
      if (!quiet) setRefreshing(false);
    }
  }, []);

  useEffect(() => {
    let cancelled = false;
    void fetchAgentInspection()
      .then((result) => {
        if (!cancelled) {
          setInspection(result);
          setError(null);
        }
      })
      .catch((cause: unknown) => {
        if (!cancelled) {
          setError(cause instanceof Error ? cause.message : "读取 Agent 状态失败");
        }
      })
      .finally(() => {
        if (!cancelled) setRefreshing(false);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  function openInspector(tab: InspectorTab) {
    setActiveTab(tab);
    setInspectorOpen(true);
  }

  const modelLabel = inspection?.model?.model ?? "No model";
  const memoryCount = inspection?.memory.records.length ?? 0;
  const skillCount = inspection?.skills.packages.length ?? 0;
  const toolCount = inspection?.tools.length ?? 0;

  return (
    <main className="workbench-shell">
      <header className="workbench-header">
        <div className="brand-lockup">
          <span className="brand-mark" aria-hidden="true">M</span>
          <div>
            <h1>Mina</h1>
            <p>Agent Harness</p>
          </div>
        </div>

        <div className="header-runtime" aria-label="当前运行时">
          <span className="status-dot" aria-hidden="true" />
          <span>{inspection?.agent.name ?? "connecting"}</span>
          <span className="header-divider" />
          <span className="header-model">{modelLabel}</span>
        </div>

        <div className="header-actions">
          <button
            className="mobile-inspector-trigger"
            type="button"
            onClick={() => setInspectorOpen((open) => !open)}
            aria-expanded={inspectorOpen}
          >
            Context
            <span>{toolCount + memoryCount + skillCount}</span>
          </button>
          <button
            className="refresh-button"
            type="button"
            onClick={() => void refresh()}
            disabled={refreshing}
            aria-label="刷新 Agent 状态"
            title="刷新 Agent 状态"
          >
            <RegenerateIcon className={refreshing ? "is-spinning" : undefined} />
          </button>
        </div>
      </header>

      <div className="workbench-body">
        <aside className="agent-rail" aria-label="Agent 概览">
          <section className="rail-section agent-identity">
            <p className="eyebrow">ACTIVE AGENT</p>
            <div className="agent-glyph" aria-hidden="true">
              <ThinkingIcon size={20} />
            </div>
            <h2>{inspection?.agent.name ?? "Loading…"}</h2>
            <p className="agent-version">
              v{inspection?.agent.version ?? "—"} · protocol {inspection?.agent.protocol_version ?? "—"}
            </p>
          </section>

          <section className="rail-section">
            <p className="eyebrow">MODEL</p>
            <dl className="runtime-facts">
              <div>
                <dt>Profile</dt>
                <dd>{inspection?.model?.profile ?? "—"}</dd>
              </div>
              <div>
                <dt>Provider</dt>
                <dd>{inspection?.model?.provider ?? "—"}</dd>
              </div>
              <div>
                <dt>Model</dt>
                <dd title={modelLabel}>{modelLabel}</dd>
              </div>
            </dl>
          </section>

          <nav className="rail-section context-nav" aria-label="上下文面板">
            <p className="eyebrow">CONTEXT</p>
            <button type="button" onClick={() => openInspector("system")}>
              <span>System</span>
              <strong>{inspection?.system.content ? "1" : "0"}</strong>
            </button>
            <button type="button" onClick={() => openInspector("memory")}>
              <span>Memory</span>
              <strong>{memoryCount}</strong>
            </button>
            <button type="button" onClick={() => openInspector("skills")}>
              <span>Skills</span>
              <strong>{skillCount}</strong>
            </button>
            <button type="button" onClick={() => openInspector("tools")}>
              <span>Tools</span>
              <strong>{toolCount}</strong>
            </button>
          </nav>

          <section className="rail-section capability-section">
            <p className="eyebrow">CAPABILITIES</p>
            <ul>
              {(inspection?.agent.capabilities ?? []).map((capability) => (
                <li key={capability}>{formatLabel(capability)}</li>
              ))}
            </ul>
          </section>

          {error && <p className="inspection-error" role="alert">{error}</p>}
        </aside>

        <section className="playground-pane" aria-label="Agent 对话">
          <div className="pane-header">
            <div>
              <p className="eyebrow">PLAYGROUND</p>
              <h2>Conversation</h2>
            </div>
            <span className="pane-status">Session persistent</span>
          </div>
          <Chat onRunFinished={refresh} />
        </section>

        <aside
          className={`inspector-pane${inspectorOpen ? " is-open" : ""}`}
          aria-label="Agent 上下文检查器"
        >
          <div className="inspector-heading">
            <div>
              <p className="eyebrow">INSPECTOR</p>
              <h2>Agent Context</h2>
            </div>
            <button
              className="inspector-close"
              type="button"
              onClick={() => setInspectorOpen(false)}
            >
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
                <span>{tabCount(tab.id, inspection)}</span>
              </button>
            ))}
          </div>

          <div className="inspector-content">
            {!inspection ? (
              <InspectorLoading />
            ) : activeTab === "system" ? (
              <SystemPanel inspection={inspection} />
            ) : activeTab === "memory" ? (
              <MemoryPanel inspection={inspection} />
            ) : activeTab === "skills" ? (
              <SkillsPanel inspection={inspection} />
            ) : (
              <ToolsPanel inspection={inspection} />
            )}
          </div>
        </aside>
      </div>
    </main>
  );
}

function SystemPanel({ inspection }: { inspection: AgentInspection }) {
  return (
    <section className="inspection-section">
      <div className="inspection-meta">
        <span>Source</span>
        <code>{inspection.system.source}</code>
      </div>
      <p className="section-help">每次模型调用前注入的基础指令。</p>
      <pre className="system-prompt">{inspection.system.content || "No system prompt configured."}</pre>
    </section>
  );
}

function MemoryPanel({ inspection }: { inspection: AgentInspection }) {
  if (!inspection.memory.enabled) {
    return <EmptyInspector title="Memory is disabled" detail="在 orchestration 配置中启用后会显示持久化记忆。" />;
  }

  return (
    <section className="inspection-section">
      <div className="inspection-meta stacked">
        <span>Store</span>
        <code>{inspection.memory.store.identity}</code>
        <span>Scope</span>
        <code>{inspection.memory.scopes.join(", ") || "—"}</code>
      </div>
      {inspection.memory.records.length === 0 ? (
        <EmptyInspector title="No memories yet" detail="完成对话后，符合写入策略的内容会出现在这里。" />
      ) : (
        <div className="memory-list">
          {[...inspection.memory.records].reverse().map((memory) => (
            <article className="memory-item" key={memory.memory_id}>
              <header>
                <span className={`memory-kind is-${memory.kind}`}>{memory.kind}</span>
                <time dateTime={new Date(memory.created_at_ms).toISOString()}>
                  {formatTime(memory.created_at_ms)}
                </time>
              </header>
              <p>{memoryText(memory.content)}</p>
              <footer>
                <span>salience {formatScore(memory.salience)}</span>
                <span>confidence {formatScore(memory.confidence)}</span>
                <code>v{memory.version}</code>
              </footer>
            </article>
          ))}
          {inspection.memory.has_more && <p className="more-note">当前仅加载 100 条记录</p>}
        </div>
      )}
    </section>
  );
}

function SkillsPanel({ inspection }: { inspection: AgentInspection }) {
  return (
    <section className="inspection-section">
      <div className="inspection-meta stacked">
        <span>Store</span>
        <code>{inspection.skills.store.identity}</code>
        <span>Adapter</span>
        <code>{inspection.skills.store.kind}</code>
      </div>
      <p className="section-help">
        Store 中可被显式选择、Profile 默认启用或根据输入自动路由的 Skill。
      </p>
      {inspection.skills.packages.length === 0 ? (
        <EmptyInspector title="No skills installed" detail="在配置的 Skill 目录中加入 skill.toml 和 SKILL.md。" />
      ) : (
        <div className="skill-list">
          {inspection.skills.packages.map((skill) => (
            <article className="skill-item" key={`${skill.skill_id}@${skill.version}`}>
              <header>
                <div>
                  <code>{skill.skill_id}</code>
                  <span>v{skill.version}</span>
                </div>
                <span className={`activation-badge is-${skill.activation.type}`}>
                  {activationLabel(skill.activation.type)}
                </span>
              </header>
              <p>{skill.description}</p>
              {skill.activation.type === "routable" && skill.activation.hints.length > 0 && (
                <div className="skill-hints" aria-label="自动路由关键词">
                  {skill.activation.hints.map((hint) => <span key={hint}>{hint}</span>)}
                </div>
              )}
              <footer>
                <span title={skill.digest}>digest {shortDigest(skill.digest)}</span>
                <span>{skill.store_identity}</span>
              </footer>
            </article>
          ))}
        </div>
      )}
    </section>
  );
}

function ToolsPanel({ inspection }: { inspection: AgentInspection }) {
  if (inspection.tools.length === 0) {
    return <EmptyInspector title="No tools attached" detail="当前 Agent 模式不会向模型暴露工具。" />;
  }

  return (
    <section className="inspection-section">
      {inspection.tool_runtime && (
        <div className="runtime-boundaries">
          <div>
            <span>Terminal sandbox</span>
            <code>{inspection.tool_runtime.process_sandbox.kind}</code>
            <strong className={`is-${inspection.tool_runtime.process_sandbox.isolation}`}>
              {inspection.tool_runtime.process_sandbox.isolation}
            </strong>
          </div>
          <div>
            <span>Search backend</span>
            <code>{inspection.tool_runtime.search_backend.kind}</code>
            <strong>
              {inspection.tool_runtime.search_backend.external_network ? "external" : "local"}
            </strong>
          </div>
        </div>
      )}
      <p className="section-help">当前 Run 可用的工具定义。展开可查看参数契约。</p>
      <div className="tool-list">
        {inspection.tools.map((tool) => (
          <details className="tool-item" key={tool.name}>
            <summary>
              <span className="tool-icon"><ToolIcon size={15} /></span>
              <span className="tool-summary-copy">
                <code>{tool.name}</code>
                <small>{tool.description}</small>
              </span>
              <span className={`risk-badge is-${tool.risk_level}`}>{tool.risk_level}</span>
            </summary>
            <div className="tool-details">
              <p className="eyebrow">INPUT SCHEMA</p>
              <pre>{JSON.stringify(tool.input_schema, null, 2)}</pre>
            </div>
          </details>
        ))}
      </div>
    </section>
  );
}

function InspectorLoading() {
  return (
    <div className="inspector-loading" aria-label="正在读取 Agent 状态">
      <span />
      <span />
      <span />
    </div>
  );
}

function EmptyInspector({ title, detail }: { title: string; detail: string }) {
  return (
    <div className="empty-inspector">
      <ThinkingIcon size={20} />
      <strong>{title}</strong>
      <p>{detail}</p>
    </div>
  );
}

function tabCount(tab: InspectorTab, inspection: AgentInspection | null) {
  if (!inspection) return "—";
  if (tab === "system") return inspection.system.content ? 1 : 0;
  if (tab === "memory") return inspection.memory.records.length;
  if (tab === "skills") return inspection.skills.packages.length;
  return inspection.tools.length;
}

function formatLabel(value: string) {
  return value.replaceAll("_", " ");
}

function memoryText(parts: ContentPart[]) {
  const text = parts.map((part) => part.text).filter(Boolean).join("\n");
  return text || parts.map((part) => `[${part.type}]`).join(" ");
}

function formatScore(value: number) {
  return `${Math.round(value * 100)}%`;
}

function formatTime(timestamp: number) {
  return new Intl.DateTimeFormat("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  }).format(timestamp);
}

function activationLabel(type: SkillDescriptor["activation"]["type"]) {
  if (type === "routable") return "Auto route";
  if (type === "profile_default") return "Profile";
  return "Explicit";
}

function shortDigest(digest: string) {
  return digest.replace(/^sha256:/, "").slice(0, 10);
}

async function fetchAgentInspection() {
  const response = await fetch("/api/v1/debug/agent", { cache: "no-store" });
  if (!response.ok) throw new Error(`Agent inspection 返回 ${response.status}`);
  return (await response.json()) as AgentInspection;
}
