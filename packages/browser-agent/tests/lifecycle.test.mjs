import assert from "node:assert/strict";
import test from "node:test";
import { createAgent, BrowserAgent } from "../dist/index.js";

const model = { protocol: "responses", baseUrl: "https://example.com/v1", model: "test", apiKey: "test", enableWebSearch: false };
class FakeWorker extends EventTarget {
  messages = [];
  terminated = false;
  postMessage(message) {
    this.messages.push(message);
    if (message.type === "initialize") queueMicrotask(() => this.emit({ type: "ready", reducerProtocolVersion: 1, checkpointSchemaVersion: 4 }));
    if (message.type === "start") queueMicrotask(() => this.emit({ type: "run_started", requestId: message.requestId, runId: "run-1", restored: false }));
    if (message.type === "cancel") queueMicrotask(() => this.emit({ type: "transition", requestId: message.requestId, runId: "run-1", transition: { protocol_version: 1, events: [{type: "cancelled"}], effects: [], state: { messages: [] }, outcome: { status: "cancelled" } } }));
  }
  emit(data) { this.dispatchEvent(new MessageEvent("message", { data })); }
  terminate() { this.terminated = true; }
}
async function fixture() {
  const worker = new FakeWorker();
  const agent = await createAgent({ namespace: "test", model, workerFactory: () => worker });
  return { agent, worker };
}
test("import does not require a browser or create a Worker", () => {
  assert.equal(typeof globalThis.Worker, "undefined");
  assert.equal(typeof createAgent, "function");
});
test("an already aborted signal never starts work", async () => {
  const { agent, worker } = await fixture();
  try {
    const controller = new AbortController(); controller.abort();
    await assert.rejects(agent.run({ sessionId: "s", input: "hello", signal: controller.signal }).next(), { name: "AbortError" });
    assert.equal(worker.messages.filter((m) => m.type === "start").length, 0);
  } finally { agent.dispose(); }
});
test("breaking a stream cancels work and waits for its terminal acknowledgement", async () => {
  const { agent, worker } = await fixture();
  try {
    for await (const event of agent.run({ sessionId: "s", input: "hello" })) { assert.equal(event.type, "run_started"); break; }
    assert.equal(worker.messages.filter((m) => m.type === "cancel").length, 1);
    const next = agent.run({ sessionId: "s", input: "next" });
    assert.equal((await next.next()).value.type, "run_started");
    await next.return();
  } finally { agent.dispose(); }
});
test("overlapping runs reject busy without cancelling the first", async () => {
  const { agent, worker } = await fixture();
  try {
    const first = agent.run({ sessionId: "s", input: "first" });
    await first.next();
    await assert.rejects(agent.run({ sessionId: "s", input: "second" }).next(), { code: "busy" });
    assert.equal(worker.messages.filter((m) => m.type === "cancel").length, 0);
    await first.return();
  } finally { agent.dispose(); }
});
test("dispose releases pending runs and RPCs and rejects subsequent calls", async () => {
  const { agent, worker } = await fixture();
  const run = agent.run({ sessionId: "s", input: "hello" });
  await run.next();
  const waiting = run.next();
  const session = agent.sessions.list();
  agent.dispose();
  await assert.rejects(waiting, { code: "disposed" });
  await assert.rejects(session, { code: "disposed" });
  await assert.rejects(agent.ready(), { code: "disposed" });
  assert.equal(worker.terminated, true);
});
test("worker failures fail pending requests and all subsequent work", async () => {
  const { agent, worker } = await fixture();
  const run = agent.run({ sessionId: "s", input: "hello" });
  await run.next();
  const waiting = run.next();
  worker.dispatchEvent(Object.assign(new Event("error"), { message: "crashed" }));
  await assert.rejects(waiting, { code: "worker_failed" });
  await assert.rejects(agent.sessions.create(), { code: "worker_failed" });
  assert.equal(worker.terminated, true);
});
test("startup failures terminate the worker", async () => {
  const worker = new FakeWorker(); worker.postMessage = () => {};
  await assert.rejects(createAgent({ namespace: "test", workerFactory: () => worker, startupTimeoutMs: 5 }), { code: "startup_timeout" });
  assert.equal(worker.terminated, true);
});
test("runtime rejects prompt configuration and unsafe namespaces before worker creation", () => {
  assert.throws(() => new BrowserAgent({ namespace: "../escape" }), /namespace/);
  assert.throws(() => new BrowserAgent({ namespace: "test", config: { systemPrompt: "injected" } }), { code: "invalid_config" });
});

test("approval RPC failures do not terminate the run stream", async () => {
  const { agent, worker } = await fixture();
  const post = worker.postMessage.bind(worker);
  worker.postMessage = (message) => {
    if (message.type !== "resolve_approval") return post(message);
    queueMicrotask(() => worker.emit(message.approvalId === "valid"
      ? { type: "approval_ack", requestId: message.requestId }
      : { type: "error", requestId: message.requestId, code: "approval_not_found", message: "Missing approval" }));
  };
  try {
    const run = agent.run({sessionId:"s", input:"hello"});
    await run.next();
    await assert.rejects(agent.resolveApproval({runId:"run-1",approvalId:"wrong",decision:"deny"}), {code:"approval_not_found"});
    await agent.resolveApproval({runId:"run-1",approvalId:"valid",decision:"deny"});
    assert.equal(worker.messages.filter((m) => m.type === "cancel").length, 0);
    await run.return();
  } finally { agent.dispose(); }
});
test("attachments respect initialization and disposal", async () => {
  const worker = new FakeWorker(); worker.postMessage = () => {};
  const agent = new BrowserAgent({namespace:"test",workerFactory:()=>worker});
  const pending = agent.attachments.put(new File(["test"], "a.txt"));
  agent.dispose();
  await assert.rejects(pending, {code:"disposed"});
  await assert.rejects(agent.attachments.get("unused"), {code:"disposed"});
});
