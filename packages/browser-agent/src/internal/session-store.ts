import type { ModelMessage, AgentTransition } from "./protocol.js";
import { storageDirectory } from "./storage-config.js";

export type SessionSnapshot = {
  session_id: string;
  status: "active" | "archived";
  title?: string;
  revision: number;
  next_message_ordinal: number;
  active_run_id?: string;
  created_at_ms: number;
  updated_at_ms: number;
  messages: ModelMessage[];
};
type State = { schema_version: 1; sessions: SessionSnapshot[] };

/** Canonical model history. No rendered messages, UI state or credentials. */
export class BrowserSessionStore {
  private tail: Promise<unknown> = Promise.resolve();
  private state: State | undefined;

  async list() {
    await this.tail;
    return structuredClone((await this.load()).sessions)
      .filter((s) => s.status === "active")
      .sort((a, b) => b.updated_at_ms - a.updated_at_ms);
  }
  async get(id: string) {
    await this.tail;
    return structuredClone(this.require(await this.load(), id));
  }
  create() {
    return this.mutate((state) => {
      const now = Date.now();
      const session: SessionSnapshot = {
        session_id: crypto.randomUUID(), status: "active", title: "新对话",
        revision: 1, next_message_ordinal: 1, created_at_ms: now, updated_at_ms: now, messages: [],
      };
      state.sessions.push(session);
      return session;
    });
  }
  updateTitle(id: string, title: string, expectedRevision: number) {
    return this.mutate((state) => {
      const s = this.require(state, id);
      if (s.revision !== expectedRevision) return s;
      s.title = title.trim() || s.title;
      s.revision++;
      s.updated_at_ms = Date.now();
      return s;
    });
  }
  transition(id: string, runId: string, transition: AgentTransition) {
    return this.mutate((state) => {
      const s = this.require(state, id);
      const messages = transition.state.messages;
      if (!messages) throw new Error("Reducer transition is missing canonical messages");
      s.messages = structuredClone(messages.filter((m) => m.role !== "system"));
      s.next_message_ordinal = s.messages.length + 1;
      s.revision++;
      s.updated_at_ms = Date.now();
      if (["complete", "failed", "cancelled"].includes(transition.outcome.status)) delete s.active_run_id;
      else s.active_run_id = runId;
      return s;
    });
  }
  private require(state: State, id: string) {
    const session = state.sessions.find((s) => s.session_id === id);
    if (!session) throw new Error("Session not found");
    return session;
  }
  private mutate<T>(operation: (state: State) => T): Promise<T> {
    const result = this.tail.then(async () => {
      const draft = structuredClone(await this.load());
      const value = operation(draft);
      const file = await (await this.directory()).getFileHandle("sessions-v1.json", { create: true });
      const writable = await file.createWritable();
      try { await writable.write(JSON.stringify(draft)); await writable.close(); }
      catch (error) { await writable.abort().catch(() => { }); throw error; }
      this.state = draft;
      return structuredClone(value);
    });
    this.tail = result.catch(() => { });
    return result;
  }
  private async load(): Promise<State> {
    if (this.state) return this.state;
    let raw: string;
    try {
      const file = await (await this.directory()).getFileHandle("sessions-v1.json");
      raw = await (await file.getFile()).text();
    } catch (error) {
      if (!(error instanceof DOMException) || error.name !== "NotFoundError") throw error;
      return this.state = { schema_version: 1, sessions: [] };
    }
    const state = JSON.parse(raw) as State;
    if (state.schema_version !== 1 || !Array.isArray(state.sessions)) throw new Error("Unsupported session schema");
    return this.state = state;
  }
  private async directory() {
    const root = await navigator.storage.getDirectory();
    return root.getDirectoryHandle(storageDirectory(), { create: true });
  }
}
