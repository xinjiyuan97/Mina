import type { AgentRunConfig, BrowserModelConfig, ModelAttachment, RunWorkerMessage, SkillInstallFile, UiToWorker, WorkerToUi } from "./protocol.js";
import type { SessionSnapshot } from "./session-store.js";
import { validateNamespace } from "./storage-config.js";

export type RuntimeInfo = Extract<WorkerToUi, { type: "ready" }>;
export class AgentError extends Error {
  constructor(readonly code: string, message: string) { super(message); this.name = "AgentError"; }
}
export type RuntimeOptions = {
  namespace: string;
  config?: AgentRunConfig;
  workerFactory?: () => Worker;
  startupTimeoutMs?: number;
};
export type RunOptions = {
  sessionId: string;
  input?: string;
  attachments?: ModelAttachment[];
  restoreRunId?: string;
  modelConfig: BrowserModelConfig;
};

export class WasmRuntimeClient {
  private readonly worker: Worker;
  private readonly queues = new Map<string, AsyncQueue<WorkerToUi>>();
  private readonly activeRequestsByRun = new Map<string, string>();
  private activeRequest?: string;
  private stopped?: AgentError;
  private readonly readyPromise: Promise<RuntimeInfo>;
  private resolveReady!: (info: RuntimeInfo) => void;
  private rejectReady!: (error: Error) => void;
  private readonly startupTimer: ReturnType<typeof setTimeout>;

  constructor(options: RuntimeOptions) {
    validateNamespace(options.namespace);
    validateRunConfig(options.config ?? {});
    this.worker = options.workerFactory?.() ?? new Worker(new URL("./agent-worker.js", import.meta.url), { type: "module", name: `mina:${options.namespace}` });
    this.readyPromise = new Promise((resolve, reject) => { this.resolveReady = resolve; this.rejectReady = reject; });
    // A caller may dispose before awaiting ready().
    void this.readyPromise.catch(() => { });
    this.startupTimer = setTimeout(() => this.stop(new AgentError("startup_timeout", "Agent initialization timed out")), options.startupTimeoutMs ?? 30_000);
    this.worker.addEventListener("message", (event: MessageEvent<WorkerToUi>) => this.accept(event.data));
    this.worker.addEventListener("error", (event) => this.stop(new AgentError("worker_failed", event.message || "Worker failed")));
    this.worker.addEventListener("messageerror", () => this.stop(new AgentError("protocol_error", "Worker message could not be decoded")));
    this.post({ type: "initialize", namespace: options.namespace, config: options.config ?? {} });
  }
  async ready() { this.assertAlive(); return this.readyPromise; }
  dispose() { this.stop(new AgentError("disposed", "Agent has been disposed")); }

  async *run(options: RunOptions, signal?: AbortSignal): AsyncIterable<RunWorkerMessage> {
    throwIfAborted(signal);
    await this.ready();
    throwIfAborted(signal);
    this.assertAlive();
    if (this.activeRequest) throw new AgentError("busy", "This agent already has an active run");
    const requestId = crypto.randomUUID();
    const queue = new AsyncQueue<WorkerToUi>();
    this.activeRequest = requestId;
    this.queues.set(requestId, queue);
    let cancellationTimer: ReturnType<typeof setTimeout> | undefined;
    const cancel = () => {
      if (this.stopped || queue.isClosed || cancellationTimer) return;
      this.post({ type: "cancel", requestId });
      cancellationTimer = setTimeout(() => this.stop(new AgentError("cancel_timeout", "Agent did not acknowledge cancellation")), 5_000);
    };
    signal?.addEventListener("abort", cancel, { once: true });
    try {
      if (options.restoreRunId) this.post({ type: "restore", requestId, sessionId: options.sessionId, runId: options.restoreRunId, modelConfig: options.modelConfig });
      else this.post({ type: "start", requestId, sessionId: options.sessionId, input: options.input ?? "", attachments: options.attachments ?? [], modelConfig: options.modelConfig });
      for await (const message of queue) {
        if (message.type === "run_started" || message.type === "transition") yield message;
      }
    } finally {
      signal?.removeEventListener("abort", cancel);
      if (!queue.isClosed) {
        cancel();
        await queue.completed;
      }
      clearTimeout(cancellationTimer);
      this.queues.delete(requestId);
      if (this.activeRequest === requestId) this.activeRequest = undefined;
      for (const [run, request] of this.activeRequestsByRun) if (request === requestId) this.activeRequestsByRun.delete(run);
    }
  }
  async resolveApproval(options: { runId: string; approvalId: string; decision: "allow-once" | "deny"; reason?: string }) {
    await this.ready();
    const requestId = this.activeRequestsByRun.get(options.runId);
    if (!requestId) throw new AgentError("run_not_active", "No active run for this approval");
    const result = await this.request({ type: "resolve_approval", ...options });
    if (result.type !== "approval_ack") throw new AgentError("protocol_error", "Unexpected approval result");
  }
  async generateTitle(options: Omit<Extract<UiToWorker, { type: "generate_title" }>, "type" | "requestId">) {
    const result = await this.request({ type: "generate_title", ...options });
    if (result.type !== "title_generated") throw new AgentError("protocol_error", "Unexpected title result");
    return { title: result.title, expectedRevision: result.expectedRevision };
  }
  async inspectWorkspace(path?: string) {
    const result = await this.request({ type: "inspect_workspace", path });
    if (result.type !== "workspace_inspected") throw new AgentError("protocol_error", "Unexpected workspace result");
    return result.result;
  }
  async inspectSkills() {
    const result = await this.request({ type: "inspect_skills" });
    if (result.type !== "skills_inspected") throw new AgentError("protocol_error", "Unexpected skills result");
    return result.result;
  }
  async installSkill(files: SkillInstallFile[]) {
    const result = await this.request({ type: "install_skill", files });
    if (result.type !== "skill_installed") throw new AgentError("protocol_error", "Unexpected install result");
    return result.result;
  }
  async downloadWorkspace(path: string) {
    const result = await this.request({ type: "download_workspace", path });
    if (result.type !== "workspace_downloaded") throw new AgentError("protocol_error", "Unexpected download result");
    return result.result;
  }
  async session(operation: "list", options?: SessionArguments): Promise<SessionSnapshot[]>;
  async session(operation: "create" | "get" | "update_title", options?: SessionArguments): Promise<SessionSnapshot>;
  async session(operation: "list" | "create" | "get" | "update_title", options: SessionArguments = {}): Promise<SessionSnapshot | SessionSnapshot[]> {
    const result = await this.request({ type: "session", operation, ...options });
    if (result.type !== "session_result") throw new AgentError("protocol_error", "Unexpected session result");
    return result.result;
  }
  private async request(message: RequestMessage) {
    await this.ready();
    this.assertAlive();
    const requestId = crypto.randomUUID();
    const queue = new AsyncQueue<WorkerToUi>();
    this.queues.set(requestId, queue);
    try {
      this.post({ ...message, requestId } as UiToWorker);
      for await (const result of queue) return result;
      throw new AgentError("protocol_error", "Request ended without a result");
    } finally { this.queues.delete(requestId); }
  }
  private accept(message: WorkerToUi) {
    if (this.stopped) return;
    if (message.type === "ready") {
      clearTimeout(this.startupTimer);
      if (message.reducerProtocolVersion !== 1 || message.checkpointSchemaVersion !== 4) {
        this.stop(new AgentError("protocol_mismatch", "Incompatible WASM protocol")); return;
      }
      this.resolveReady(message); return;
    }
    if (!message.requestId) {
      if (message.type === "error") this.stop(new AgentError(message.code, message.message));
      return;
    }
    const queue = this.queues.get(message.requestId);
    if (!queue) return;
    if (message.type === "error") { queue.fail(new AgentError(message.code, message.message)); return; }
    if (message.type === "run_started") this.activeRequestsByRun.set(message.runId, message.requestId);
    queue.push(message);
    if ((message.type !== "run_started" && message.type !== "transition") || isTerminalTransition(message)) queue.close();
  }
  private assertAlive() { if (this.stopped) throw this.stopped; }
  private post(message: UiToWorker) { this.assertAlive(); this.worker.postMessage(message); }
  private stop(error: AgentError) {
    if (this.stopped) return;
    this.stopped = error;
    clearTimeout(this.startupTimer);
    this.worker.terminate();
    this.rejectReady(error);
    for (const queue of this.queues.values()) queue.fail(error);
    this.queues.clear();
    this.activeRequestsByRun.clear();
    this.activeRequest = undefined;
  }
}
type SessionArguments = { sessionId?: string; title?: string; expectedRevision?: number };
type RequestMessage = UiToWorker extends infer M ? M extends { requestId: string } ? Omit<M, "requestId"> : never : never;
function throwIfAborted(signal?: AbortSignal) { if (signal?.aborted) throw signal.reason ?? new DOMException("Aborted", "AbortError"); }
export function validateRunConfig(config: AgentRunConfig) {
  const allowed = new Set(["maxSteps", "maxOutputTokens", "modelTimeoutMs", "toolTimeoutMs", "approvalPolicy", "allowedTools"]);
  for (const [key, value] of Object.entries(config)) {
    if (!allowed.has(key)) throw new AgentError("invalid_config", `Unsupported agent setting: ${key}`);
    if (key === "allowedTools") {
      if (!Array.isArray(value) || value.some((v) => typeof v !== "string")) throw new AgentError("invalid_config", "allowedTools must be an array of names");
    } else if (!Number.isSafeInteger(value) || (value as number) < (key === "approvalPolicy" ? 0 : 1) || (value as number) > (key === "approvalPolicy" ? 100 : 2147483647)) {
      throw new AgentError("invalid_config", `Invalid ${key}`);
    }
  }
}

class AsyncQueue<T> implements AsyncIterable<T> {
  private readonly values: T[] = [];
  private readonly waiters: Array<{
    resolve: (result: IteratorResult<T>) => void;
    reject: (error: Error) => void;
  }> = [];
  private closed = false;
  readonly completed: Promise<void>;
  private finish!: () => void;
  constructor() { this.completed = new Promise((resolve) => { this.finish = resolve; }); }
  get isClosed() { return this.closed; }
  private error: Error | null = null;

  push(value: T) {
    if (this.closed) return;
    const waiter = this.waiters.shift();
    if (waiter) waiter.resolve({ value, done: false });
    else this.values.push(value);
  }

  close() {
    if (this.closed) return;
    this.closed = true;
    this.finish();
    for (const waiter of this.waiters.splice(0)) waiter.resolve({ value: undefined, done: true });
  }

  fail(error: Error) {
    if (this.closed) return;
    this.closed = true;
    this.finish();
    this.error = error;
    for (const waiter of this.waiters.splice(0)) waiter.reject(error);
  }

  [Symbol.asyncIterator](): AsyncIterator<T> {
    return {
      next: () => {
        const value = this.values.shift();
        if (value !== undefined) return Promise.resolve({ value, done: false });
        if (this.error) return Promise.reject(this.error);
        if (this.closed) return Promise.resolve({ value: undefined, done: true });
        return new Promise<IteratorResult<T>>((resolve, reject) => {
          this.waiters.push({ resolve, reject });
        });
      },
    };
  }
}

function isTerminalTransition(message: WorkerToUi) {
  return (
    message.type === "transition" &&
    ["complete", "failed", "cancelled", "suspended"].includes(message.transition.outcome.status)
  );
}
