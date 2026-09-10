/// <reference lib="webworker" />

import init, {
  WasmAgent,
  startBrowserAgent,
  restoreBrowserAgent,
  checkpointSchemaVersion,
  reducerProtocolVersion,
  skillCompilerVersion,
  titleDispatchJson,
  titleProtocolVersion,
  titleStartJson,
} from "../pkg/mina_wasm_agent.js";

import { configureStorage, skillsPrefix } from "./storage-config.js";
import { BrowserSessionStore } from "./session-store.js";
import type { AgentRunConfig } from "./protocol.js";
import { BrowserCheckpointStore } from "./checkpoint-store.js";
import { BrowserHost } from "./browser-host.js";
import { BrowserFileStore } from "./opfs-files.js";
import { BrowserSkillInstaller } from "./opfs-skill-installer.js";
import { BrowserSkillStore } from "./opfs-skills.js";
import type {
  AgentEffect,
  AgentTransition,
  BrowserModelConfig,
  SkillLock,
  UiToWorker,
  WorkerToUi,
} from "./protocol.js";

const worker = self as DedicatedWorkerGlobalScope;
const sessions = new BrowserSessionStore();
let runConfig: AgentRunConfig = {};
let initialized = false;
const checkpoints = new BrowserCheckpointStore();
const workspaceFiles = new BrowserFileStore();
const browserSkills = new BrowserSkillStore();
const skillInstaller = new BrowserSkillInstaller();
let current: BrowserRun | null = null;
let messageTail = Promise.resolve();

async function initialize(namespace: string) {
  await acquireNamespace(namespace);
  configureStorage(namespace);
  await init();
  await workspaceFiles.ensureRoot();
  await browserSkills.ensureRoot();

  initialized = true;
  post({
    type: "ready",
    reducerProtocolVersion: reducerProtocolVersion(),
    checkpointSchemaVersion: checkpointSchemaVersion(),
    titleProtocolVersion: titleProtocolVersion(),
    skillCompilerVersion: skillCompilerVersion(),
    skillProvider: "opfs",
    supportedModelProtocols: ["responses", "messages"],
  });

}

// The lock lives with the Worker. Termination releases it, including tab crashes.
async function acquireNamespace(namespace: string) {
  if (!navigator.storage?.getDirectory || !navigator.locks) throw codedError("unsupported_browser", "OPFS and Web Locks are required");
  await new Promise<void>((resolve, reject) => {
    void navigator.locks.request(`mina-agent:${namespace}`, { ifAvailable: true }, async (lock) => {
      if (!lock) { reject(codedError("namespace_busy", "Another agent is using this namespace")); return; }
      resolve();
      await new Promise<void>(() => { });
    }).catch(reject);
  });
}

worker.addEventListener("message", (event: MessageEvent<UiToWorker>) => {
  const message = event.data;
  messageTail = messageTail
    .then(() => handleMessage(message))
    .catch(async (error: unknown) => {
      if ((message.type === "start" || message.type === "restore") && current?.requestId === message.requestId) {
        await current.shutdown().catch(() => {});
      }
      post({ type: "error", requestId: "requestId" in message ? message.requestId : undefined, code: errorCode(error), message: safeMessage(error) });
    });
});

async function handleMessage(message: UiToWorker) {
  if (message.type === "initialize") {
    if (initialized) throw codedError("already_initialized", "Agent already initialized");
    runConfig = message.config;
    await initialize(message.namespace);
    return;
  }
  if (!initialized) throw codedError("not_ready", "Agent is not initialized");
  switch (message.type) {
    case "session": {
      const result = message.operation === "list" ? await sessions.list()
        : message.operation === "create" ? await sessions.create()
          : message.operation === "get" ? await sessions.get(message.sessionId!)
            : await sessions.updateTitle(message.sessionId!, message.title!, message.expectedRevision!);
      post({ type: "session_result", requestId: message.requestId, result });
      return;
    }
    case "start": {
      if (current?.isActive()) throw codedError("busy", "Agent already has an active run");
      await current?.shutdown();
      const session = await sessions.get(message.sessionId);
      if (session.active_run_id) throw codedError("run_requires_restore", "Restore the unfinished run before starting another turn");
      const runId = crypto.randomUUID();
      const agent = await startBrowserAgent(skillsPrefix(), JSON.stringify({
        protocol_version: reducerProtocolVersion(),
        start: {
          run_id: runId,
          input: message.input,
          attachments: message.attachments,
          prior_messages: session.messages,
          config: {
            model: message.modelConfig.model,
            max_output_tokens: runConfig.maxOutputTokens ?? 1024,
            max_steps: runConfig.maxSteps ?? 32,
            allowed_tools: runConfig.allowedTools,
            allow_run_adf: false,
            approval_policy: runConfig.approvalPolicy ?? 50,
            tool_call_strategy: "parallel-safe",
            model_timeout_ms: runConfig.modelTimeoutMs ?? 300_000,
            tool_timeout_ms: runConfig.toolTimeoutMs ?? 120_000,
          },
        },
      }));
      const skillLock = JSON.parse(agent.skillLockJson()) as SkillLock | null;
      const host = new BrowserHost(message.modelConfig, skillLock);
      current = new BrowserRun(agent, host, message.requestId, message.sessionId, runId);
      post({ type: "run_started", requestId: message.requestId, runId, restored: false });
      await current.start();
      return;
    }

    case "restore": {
      if (current?.isActive()) throw codedError("busy", "Agent already has an active run");
      await current?.shutdown();
      const session = await sessions.get(message.sessionId);
      if (session.active_run_id !== message.runId) throw codedError("run_not_restorable", "Only the session's unfinished run can be restored");
      const checkpoint = await checkpoints.load(message.sessionId, message.runId);
      if (!checkpoint) throw new Error("OPFS 中找不到这个 Run 的 checkpoint");
      const agent = await restoreBrowserAgent(skillsPrefix(), checkpoint);
      const skillLock = JSON.parse(agent.skillLockJson()) as SkillLock | null;
      if (skillLock) await browserSkills.verifyLock(skillLock);
      current = new BrowserRun(
        agent,
        new BrowserHost(message.modelConfig, skillLock),
        message.requestId,
        message.sessionId,
        message.runId,
      );
      post({
        type: "run_started",
        requestId: message.requestId,
        runId: message.runId,
        restored: true,
      });
      await current.start();
      return;
    }

    case "cancel":
      if (current?.requestId === message.requestId) await current.cancel();
      return;

    case "resolve_approval":
      if (!current?.isActive() || current.runId !== message.runId) {
        throw new Error("找不到对应的浏览器 Run");
      }
      await current.resolveApproval(message.approvalId, message.decision, message.reason);
      post({ type: "approval_ack", requestId: message.requestId });
      return;

    case "generate_title": {
      if (current?.isActive()) throw codedError("busy", "Wait for the run before generating a title");
      const result = await generateTitle(message);
      post({ type: "title_generated", requestId: message.requestId, ...result });
      return;
    }

    case "inspect_workspace": {
      if (message.path) {
        const file = await workspaceFiles.readText(message.path);
        post({
          type: "workspace_inspected",
          requestId: message.requestId,
          result: { type: "file", path: file.file.path, content: file.content },
        });
      } else {
        post({
          type: "workspace_inspected",
          requestId: message.requestId,
          result: { type: "snapshot", snapshot: await workspaceFiles.debugSnapshot() },
        });
      }
      return;
    }

    case "inspect_skills": {
      post({
        type: "skills_inspected",
        requestId: message.requestId,
        result: await browserSkills.list(),
      });
      return;
    }

    case "install_skill": {
      if (current?.isActive()) throw new Error("当前 Run 执行中，不能导入 Skill");
      await current?.shutdown();
      current = null;
      post({
        type: "skill_installed",
        requestId: message.requestId,
        result: await skillInstaller.install(message.files),
      });
      return;
    }

    case "download_workspace": {
      const download = await workspaceFiles.readDownload(message.path);
      const bytes = download.bytes.buffer;
      post(
        {
          type: "workspace_downloaded",
          requestId: message.requestId,
          result: {
            path: download.file.path,
            name: download.file.name,
            size: download.file.size,
            mediaType: download.file.mediaType,
            bytes,
          },
        },
        [bytes],
      );
      return;
    }
  }
}

class BrowserRun {
  private active = true;
  private disposed = false;
  private readonly abortController = new AbortController();
  private dispatchTail = Promise.resolve();
  private readonly effects = new Set<Promise<void>>();

  constructor(
    private readonly agent: WasmAgent,
    private readonly host: BrowserHost,
    readonly requestId: string,
    private readonly sessionId: string,
    readonly runId: string,
  ) { }

  async start() {
    await this.accept(JSON.parse(this.agent.transitionJson()) as AgentTransition);
  }

  isActive() { return this.active && !this.disposed; }

  isTerminal() {
    return this.agent.isTerminal();
  }

  async cancel() {
    if (!this.active) return;
    await this.pushInput({ type: "cancelled" }, true);
    await this.shutdown();
  }

  async shutdown() {
    if (this.disposed) return;
    this.active = false;
    this.abortController.abort();
    await Promise.allSettled(this.effects);
    await this.dispatchTail.catch(() => {});
    this.agent.free();
    this.disposed = true;
  }

  async resolveApproval(
    approvalId: string,
    decision: "allow-once" | "deny",
    reason?: string,
  ) {
    const effect = this.pendingApprovals.get(approvalId);
    if (!effect) throw new Error("找不到待处理的浏览器工具审批");
    this.pendingApprovals.delete(approvalId);
    await this.pushInput({
      type: "approval_resolved",
      effect_id: effect.effect_id,
      resolution: { decision, reason },
    });
  }

  private pushInput(input: unknown, allowCancellation = false): Promise<void> {
    this.dispatchTail = this.dispatchTail.then(async () => {
      if (!this.active && !allowCancellation) return;
      const transition = JSON.parse(this.agent.dispatch(JSON.stringify(input))) as AgentTransition;
      await this.accept(transition);
    });
    return this.dispatchTail;
  }

  private async accept(transition: AgentTransition) {
    restoreApprovalEvents(transition);
    await checkpoints.save(this.sessionId, this.runId, this.agent.checkpointJson());
    await sessions.transition(this.sessionId, this.runId, transition);
    post({
      type: "transition",
      requestId: this.requestId,
      runId: this.runId,
      transition,
    });

    if (this.agent.isTerminal() || transition.outcome.status === "suspended") {
      this.active = false;
      this.abortController.abort();
      return;
    }

    for (const effect of transition.effects) this.startEffect(effect);
  }

  private startEffect(effect: AgentEffect) {
    if (effect.type === "request_approval") {
      if (!effect.approval_id) throw new Error("approval effect 缺少 approval_id");
      this.pendingApprovals.set(effect.approval_id, effect);
      return;
    }
    const execution = this.host
      .execute(effect, (input) => this.pushInput(input), this.abortController.signal)
      .catch((error: unknown) => {
        if (this.abortController.signal.aborted) return;
        this.active = false;
        this.abortController.abort();
        post({ type: "error", requestId: this.requestId, code: errorCode(error), message: safeMessage(error) });
      })
      .finally(() => this.effects.delete(execution));
    this.effects.add(execution);
  }

  private readonly pendingApprovals = new Map<string, AgentEffect>();
}

type TitleTransition = {
  state: unknown;
  effects: AgentEffect[];
  outcome: {
    status: "running" | "deferred" | "ready" | "fallback" | "user_locked" | "failed";
    title?: string;
    message?: string;
  };
};

async function generateTitle(message: Extract<UiToWorker, { type: "generate_title" }>) {
  const host = new BrowserHost(message.modelConfig);
  let transition = JSON.parse(
    titleStartJson(
      JSON.stringify({
        protocol_version: titleProtocolVersion(),
        start: {
          session_id: message.sessionId,
          first_message_id: message.firstMessageId,
          expected_session_revision: message.expectedRevision,
          model: message.modelConfig.model,
          source_text: message.sourceText,
          max_output_tokens: 32,
          timeout_ms: 60_000,
        },
      }),
    ),
  ) as TitleTransition;

  if (transition.outcome.status === "deferred") {
    return { title: fallbackTitle(message.sourceText), expectedRevision: message.expectedRevision };
  }

  const controller = new AbortController();
  for (const effect of transition.effects) {
    await host.execute(
      effect,
      async (input) => {
        transition = JSON.parse(
          titleDispatchJson(
            JSON.stringify({
              protocol_version: titleProtocolVersion(),
              state: transition.state,
              input,
            }),
          ),
        ) as TitleTransition;
      },
      controller.signal,
    );
  }

  if (
    (transition.outcome.status === "ready" || transition.outcome.status === "fallback") &&
    transition.outcome.title
  ) {
    return {
      title: transition.outcome.title,
      expectedRevision: message.expectedRevision,
    };
  }
  if (transition.outcome.status === "failed") {
    throw new Error(transition.outcome.message ?? "生成会话标题失败");
  }
  return { title: fallbackTitle(message.sourceText), expectedRevision: message.expectedRevision };
}

function fallbackTitle(source: string) {
  const normalized = source.replace(/\s+/g, " ").trim();
  return Array.from(normalized).slice(0, 48).join("") || "新对话";
}

function post(message: WorkerToUi, transfer: Transferable[] = []) {
  worker.postMessage(message, transfer);
}

function safeMessage(error: unknown) {
  return error instanceof Error ? error.message : String(error);
}

function restoreApprovalEvents(transition: AgentTransition) {
  for (const effect of transition.effects) {
    if (effect.type !== "request_approval" || !effect.approval_id || !effect.call) continue;
    if (
      transition.events.some(
        (event) => event.type === "approval_requested" && event.approval_id === effect.approval_id,
      )
    ) {
      continue;
    }
    const argumentsValue = effect.call.public_arguments ?? effect.call.arguments;
    transition.events.push(
      {
        type: "tool_call_started",
        call_id: effect.call.call_id,
        name: effect.call.name,
      },
      {
        type: "tool_call_arguments_delta",
        call_id: effect.call.call_id,
        delta: JSON.stringify(argumentsValue),
      },
      {
        type: "approval_requested",
        approval_id: effect.approval_id,
        call_id: effect.call.call_id,
        tool_name: effect.call.name,
        risk_level: effect.call.risk_level ?? "low",
        arguments: argumentsValue,
      },
    );
  }
}

function codedError(code: string, message: string) { return Object.assign(new Error(message), { code }); }
function errorCode(error: unknown) { return error && typeof error === "object" && "code" in error && typeof error.code === "string" ? error.code : "runtime_failed"; }
