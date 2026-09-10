import assert from "node:assert/strict";
import test from "node:test";
import { BrowserSessionStore } from "../dist/internal/session-store.js";
import { BrowserAttachmentStore } from "../dist/internal/attachment-store.js";
import { readFile, stat } from "node:fs/promises";

const files = new Map();
let failWrites = false;
function directory(path = "") {
  return {
    async getDirectoryHandle(name) { return directory(`${path}/${name}`); },
    async getFileHandle(name, options = {}) {
      const key = `${path}/${name}`;
      if (!files.has(key)) {
        if (!options.create) throw new DOMException("Missing", "NotFoundError");
        files.set(key, "");
      }
      return {
        async getFile() {
          const content = files.get(key);
          return typeof content === "string" ? new File([content], name) : content;
        },
        async createWritable() {
          let staged;
          return {
            async write(value) { if (failWrites) throw new Error("disk full"); staged = value; },
            async close() { files.set(key, staged); },
            async abort() {},
          };
        },
      };
    },
  };
}
Object.defineProperty(globalThis, "navigator", { configurable: true, value: { storage: { getDirectory: async () => directory() } } });

test("canonical history survives reloading and retains tools, reasoning and attachment IDs", async () => {
  const store = new BrowserSessionStore();
  const session = await store.create();
  const messages = [
    {role: "system", content: "embedded"},
    {role: "user", content: "read", attachments: [{blob_id:"blob-id", media_type:"image/png"}]},
    {role: "assistant", content: "", reasoning:"inspect", tool_calls:[{id:"call",name:"read",arguments:'{"path":"x"}'}]},
    {role: "tool", content:'{"content":"hello"}',tool_call_id:"call"},
    {role: "assistant", content:"hello"},
  ];
  await store.transition(session.session_id, "run", {state:{messages},outcome:{status:"running"}});
  assert.equal((await store.get(session.session_id)).active_run_id, "run");
  await store.transition(session.session_id, "run", {state:{messages},outcome:{status:"complete"}});
  const restored = await new BrowserSessionStore().get(session.session_id);
  assert.deepEqual(restored.messages, messages.slice(1));
  assert.equal(restored.active_run_id, undefined);
  restored.messages[0].content = "changed by caller";
  assert.equal((await store.get(session.session_id)).messages[0].content, "read");
});
test("failed writes do not mutate cached canonical state", async () => {
  const store = new BrowserSessionStore();
  const session = await store.create();
  failWrites = true;
  try { await assert.rejects(store.updateTitle(session.session_id, "lost", session.revision), /disk full/); }
  finally { failWrites = false; }
  assert.equal((await store.get(session.session_id)).title, session.title);
  await store.updateTitle(session.session_id, "saved", session.revision);
  assert.equal((await new BrowserSessionStore().get(session.session_id)).title, "saved");
});
test("main-thread attachment stores use the instance namespace", async () => {
  const a = new BrowserAttachmentStore("one");
  const b = new BrowserAttachmentStore("two");
  const metadata = await a.put(new File(["hello"], "test.txt", {type:"text/plain"}));
  assert.equal(await (await a.get(metadata.blob_id)).blob.text(), "hello");
  await assert.rejects(b.get(metadata.blob_id), {code:"attachment_not_found"});
});
test("distribution includes WASM and its OPFS bridge and has no UI runtime dependencies", async () => {
  const pkg = JSON.parse(await readFile(new URL("../package.json", import.meta.url), "utf8"));
  assert.deepEqual(Object.keys(pkg.dependencies ?? {}).sort(), ["@jitl/quickjs-singlefile-browser-release-sync", "quickjs-emscripten-core"]);
  assert.ok((await stat(new URL("../dist/pkg/mina_wasm_agent_bg.wasm", import.meta.url))).size > 0);
  const bridge = await readFile(new URL("../dist/pkg/mina_wasm_agent.js", import.meta.url), "utf8");
  const snippet = bridge.match(/from '\.\/(snippets\/[^']+)'/)[1];
  assert.ok((await stat(new URL(`../dist/pkg/${snippet}`, import.meta.url))).size > 0);
});
