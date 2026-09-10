import { createAgent, type BrowserAgent, type AgentOptions } from "@mina/browser-agent";
const result = document.querySelector<HTMLPreElement>("#result")!;
const checks: string[] = [];
const agents = new Set<BrowserAgent>();
function check(value: unknown, label: string): asserts value { if (!value) throw new Error(label); checks.push(label); result.textContent = checks.join("\n"); }
const model = { protocol: "messages" as const, baseUrl: `${location.origin}/v1`, model: `mock-${crypto.randomUUID()}`, apiKey: "test-only-key", enableWebSearch: false };
async function open(namespace: string, extra: Partial<AgentOptions> = {}) {
  const a = await createAgent({ namespace, model, config: { approvalPolicy: 0 }, ...extra }); agents.add(a); return a;
}
async function collect(a: BrowserAgent, sessionId: string, input: string) {
  const events = [];
  for await (const event of a.run({sessionId, input})) {
    events.push(event);
    if (event.type === "approval_requested") await a.resolveApproval({ runId: event.runId, approvalId: event.approval_id, decision: "allow-once" });
  }
  check(events.some((e) => e.type === "run_finished" && e.status === "complete"), `completed: ${input}`);
  return events;
}
async function main() {
  const ns = `sdk-test-${crypto.randomUUID()}`;
  let a = await open(ns);
  const b = await open(`${ns}-other`);
  check((await a.ready()).reducerProtocolVersion === 1, "WASM loaded from installed package");
  try { const duplicate = await open(ns); duplicate.dispose(); throw new Error("duplicate accepted"); }
  catch (error) { check((error as {code?: string}).code === "namespace_busy", "same-namespace concurrent writers rejected"); }
  const session = await a.sessions.create();
  check((await b.sessions.list()).length === 0, "sessions isolated across namespaces");
  const blob = await a.attachments.put(new File(["attachment"], "note.txt", {type:"text/plain"}));
  check((await a.attachments.get(blob.blob_id)).metadata.name === "note.txt", "attachment round trip");
  try { await b.attachments.get(blob.blob_id); throw new Error("attachment leaked"); }
  catch (error) { check((error as {code?:string}).code === "attachment_not_found", "attachments isolated across namespaces"); }
  await collect(a, session.session_id, "first turn");
  await collect(a, session.session_id, "write-tool");
  const history = (await a.sessions.get(session.session_id)).messages;
  check(history.some((m) => m.role === "assistant" && m.tool_calls?.length) && history.some((m) => m.role === "tool"), "canonical history retains tool calls and tool results");
  check(new TextDecoder().decode((await a.workspace.read("proof.txt")).bytes) === "SDK tool result", "tool writes to namespaced workspace");
  const other = await b.workspace.inspect();
  check(other.type === "snapshot" && other.snapshot.entries.length === 0, "workspace isolated across namespaces");
  await collect(a, session.session_id, "next turn");
  const requests = (await (await fetch("/test/requests")).json()).filter((r: any) => r.model === model.model);
  check(requests.every((r: any) => r.system === "You are Mina running locally inside a browser Worker."), "all model requests use the WASM embedded prompt");
  check(requests.at(-1).messages.some((m: any) => m.content.some((p: any) => p.type === "tool_result")), "next turn receives full prior tool history without UI reconstruction");
  check((await a.sessions.get(session.session_id)).messages.at(-1)?.content === "Reply: next turn", "final assistant answer is part of canonical state");
  const expectedHistory = (await a.sessions.get(session.session_id)).messages;
  const skillFiles = [
    { path: "skill.toml", bytes: new TextEncoder().encode('skill_id = "smoke"\nversion = "1.0.0"\ndescription = "smoke"\nrequired_tools = []\nactivation = { type = "routable", hints = ["skill-smoke"] }\n').buffer },
    { path: "SKILL.md", bytes: new TextEncoder().encode("Use the locked smoke instruction.").buffer },
  ];
  await a.skills.install(skillFiles);
  check((await a.skills.list()).packages.length === 1 && (await b.skills.list()).packages.length === 0, "skills installed and isolated");
  const restoring = await a.sessions.create();
  let interruptedRun = "";
  try {
    for await (const event of a.run({sessionId: restoring.session_id, input: "skill-smoke write-tool"})) {
      if (event.type === "approval_requested") { interruptedRun = event.runId; a.dispose(); }
    }
  } catch (error) { check((error as {code?: string}).code === "disposed", "abrupt disposal ends pending approval stream"); }
  check(interruptedRun, "approval checkpoint captured");
  await new Promise((resolve) => setTimeout(resolve, 100));
  a = await open(ns);
  check(JSON.stringify((await a.sessions.get(session.session_id)).messages) === JSON.stringify(expectedHistory), "canonical conversation survives a new Worker");
  let completed = false;
  let approvalRestored = false;
  for await (const event of a.run({sessionId: restoring.session_id, restoreRunId: interruptedRun})) {
    if (event.type === "approval_requested") { approvalRestored = true; await a.resolveApproval({runId:event.runId, approvalId:event.approval_id, decision:"allow-once"}); }
    if (event.type === "run_finished") completed = event.status === "complete";
  }
  check(approvalRestored && completed, "restore verifies locked skills, restores approval, and completes");
  const cancelled = await a.sessions.create();
  for await (const event of a.run({sessionId: cancelled.session_id, input:"cancel me"})) { if (event.type === "run_started") break; }
  check(!(await a.sessions.get(cancelled.session_id)).active_run_id, "early iterator exit durably cancels the run");
  const jsSession = await a.sessions.create();
  const jsEvents = await collect(a, jsSession.session_id, "js-tool");
  const jsOutput = jsEvents.find((e) => e.type === "tool_execution_completed");
  check(jsOutput?.type === "tool_execution_completed" && JSON.parse(jsOutput.output).value === 21 && JSON.parse(jsOutput.output).logs[0].text === "count 3", "QuickJS nested Worker returns JSON and logs");
  const timeoutEvents = await collect(a, jsSession.session_id, "js-tool endless");
  check(timeoutEvents.some((e) => e.type === "tool_execution_failed" && e.code === "javascript_timeout"), "QuickJS infinite loop reports timeout");
  const abortSession = await a.sessions.create();
  for await (const event of a.run({sessionId: abortSession.session_id, input:"js-tool endless"})) {
    if (event.type === "approval_requested") await a.resolveApproval({runId:event.runId, approvalId:event.approval_id, decision:"allow-once"});
    if (event.type === "tool_execution_started") { await new Promise((resolve) => setTimeout(resolve, 100)); break; }
  }
  check(!(await a.sessions.get(abortSession.session_id)).active_run_id, "cancellation during JavaScript clears active run");
  await collect(a, jsSession.session_id, "js-tool recovery");
  const root = await navigator.storage.getDirectory();
  const dir = await root.getDirectoryHandle(ns);
  const state = await (await (await dir.getFileHandle("sessions-v1.json")).getFile()).text();
  check(!state.includes("test-only-key"), "credentials are not persisted in session state");
  result.textContent = `PASS (${checks.length})\n${checks.join("\n")}`;
}
main().catch((error) => { result.textContent = `FAIL\n${checks.join("\n")}\n${error.stack ?? error}`; }).finally(() => { for (const a of agents) a.dispose(); });
