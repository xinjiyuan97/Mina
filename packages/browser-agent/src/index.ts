import { WasmRuntimeClient, AgentError, type RuntimeOptions, type RunOptions } from "./internal/wasm-client.js";
import { BrowserAttachmentStore } from "./internal/attachment-store.js";
import type { AgentEvent, BrowserModelConfig, ModelAttachment } from "./internal/protocol.js";

export { AgentError };
export type { RuntimeInfo } from "./internal/wasm-client.js";
export type { SessionSnapshot } from "./internal/session-store.js";
export type { AgentEvent, AgentTransition, AgentRunConfig, BrowserModelConfig, ModelMessage, ModelAttachment, SkillDescriptor, SkillInspection, SkillInstallFile, SkillInstallResult, WorkspaceDebugEntry, WorkspaceDebugSnapshot, WorkspaceDownload, PortableToolError } from "./internal/protocol.js";
export { MAX_ATTACHMENT_BYTES, MAX_ATTACHMENTS_PER_MESSAGE, attachmentIdFromUrl, stableAttachmentUrl, inferMediaType } from "./internal/attachment-store.js";
export { readSkillDirectory } from "./internal/skill-input.js";

export type AgentOptions = RuntimeOptions & { model?: BrowserModelConfig };
export type AgentRunOptions = {
  sessionId: string;
  input?: string;
  attachments?: ModelAttachment[];
  restoreRunId?: string;
  model?: BrowserModelConfig;
  signal?: AbortSignal;
};
export type AgentStreamEvent =
  | { type: "run_started"; runId: string; restored: boolean }
  | (AgentEvent & { runId: string })
  | { type: "run_finished"; runId: string; status: "complete" | "failed" | "cancelled" | "suspended" };

/** One independent Worker and one OPFS namespace. Importing the package does no I/O. */
export class BrowserAgent {
  private readonly client: WasmRuntimeClient;
  readonly attachments: BrowserAttachmentStore;
  readonly sessions;
  readonly skills;
  readonly workspace;
  readonly debug;

  constructor(private readonly options: AgentOptions) {
    this.client = new WasmRuntimeClient(options);
    this.attachments = new BrowserAttachmentStore(options.namespace, () => this.client.ready());
    this.sessions = {
      list: () => this.client.session("list"),
      create: () => this.client.session("create"),
      get: (sessionId: string) => this.client.session("get", { sessionId }),
      updateTitle: (sessionId: string, title: string, expectedRevision: number) => this.client.session("update_title", { sessionId, title, expectedRevision }),
    };
    this.skills = { list: () => this.client.inspectSkills(), install: this.client.installSkill.bind(this.client) };
    this.workspace = { inspect: this.client.inspectWorkspace.bind(this.client), read: this.client.downloadWorkspace.bind(this.client) };
    // Raw reducer transitions are an explicit advanced surface, not the normal event API.
    this.debug = { runTransitions: (options: RunOptions, signal?: AbortSignal) => this.client.run(options, signal) };
  }
  ready() { return this.client.ready(); }
  async *run(options: AgentRunOptions): AsyncIterable<AgentStreamEvent> {
    const modelConfig = options.model ?? this.options.model;
    if (!modelConfig) throw new AgentError("model_required", "Configure a model before running the agent");
    for await (const message of this.client.run({ ...options, modelConfig }, options.signal)) {
      if (message.type === "run_started") {
        yield { type: "run_started", runId: message.runId, restored: message.restored };
      } else {
        for (const event of message.transition.events) yield { ...event, runId: message.runId };
        const status = message.transition.outcome.status;
        if (status !== "running") yield { type: "run_finished", runId: message.runId, status };
      }
    }
  }
  resolveApproval(options: Parameters<WasmRuntimeClient["resolveApproval"]>[0]) { return this.client.resolveApproval(options); }
  generateTitle(options: Parameters<WasmRuntimeClient["generateTitle"]>[0]) { return this.client.generateTitle(options); }
  dispose() { this.attachments.dispose(); this.client.dispose(); }
}

export async function createAgent(options: AgentOptions): Promise<BrowserAgent> {
  const agent = new BrowserAgent(options);
  try { await agent.ready(); return agent; }
  catch (error) { agent.dispose(); throw error; }
}
