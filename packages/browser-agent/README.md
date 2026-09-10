# @mina/browser-agent

A headless browser agent: TypeScript SDK + Dedicated Worker + Rust WASM. No React,
chat renderer, CSS, Mina server or Rust toolchain is needed by a consuming app.
The installed package contains the JavaScript, declarations, Worker, WASM and
OPFS bridge assets. It is an ESM package for modern browsers with OPFS and Web
Locks, served over HTTPS or localhost.

## Vite integration

Build and pack this package in the Mina workspace, then install that tarball in
any Vite app. No publication to a registry is required:

```sh
pnpm --filter @mina/browser-agent build
pnpm --filter @mina/browser-agent pack --pack-destination /tmp
# In the consuming app:
npm install /tmp/mina-browser-agent-0.1.0.tgz
```

Configure module Workers in Vite:

```ts
import { defineConfig } from "vite";
export default defineConfig({
  worker: { format: "es" },
  optimizeDeps: { exclude: ["@mina/browser-agent"] },
});
```

```ts
import { createAgent } from "@mina/browser-agent";

const agent = await createAgent({
  namespace: "my-app-agent",
  model: {
    protocol: "responses", // or "messages"
    baseUrl: "https://your-model-endpoint.example/v1",
    model: "your-model",
    apiKey: "your-runtime-token",
    enableWebSearch: false,
  },
  config: { maxSteps: 32, maxOutputTokens: 1024 },
});
const session = await agent.sessions.create();
const controller = new AbortController();

try {
  for await (const event of agent.run({
    sessionId: session.session_id,
    input: "List the workspace files",
    signal: controller.signal,
  })) {
    if (event.type === "output_delta" && event.channel === "assistant_text") {
      console.log(event.delta);
    }
    if (event.type === "approval_requested") {
      // Supply this decision from your application/user policy.
      await agent.resolveApproval({
        runId: event.runId,
        approvalId: event.approval_id,
        decision: "deny",
      });
    }
  }
} finally {
  // Dispose when your application is finished with this instance, not after
  // every turn if you intend to keep using it.
  agent.dispose();
}
```

The model endpoint must allow browser requests. The SDK does not persist model
credentials; supply them again when creating an instance or restoring a run.

## Ownership and public API

- `createAgent(options)` initializes a Worker and resolves after WASM and OPFS
  are ready. Importing the package starts no Worker and accesses no browser API.
  `new BrowserAgent(options)` plus `ready()` is available for loading/error UI.
- `sessions.create/list/get/updateTitle` manage framework-neutral metadata and
  canonical `ModelMessage[]`. The SDK saves assistant tool calls, tool results,
  reasoning and stable attachment references. A caller supplies only new input
  on each turn; it never reconstructs model history from rendered chat bubbles.
- `run({ sessionId, input, attachments?, signal?, model? })` streams typed agent
  events. `run_started` supplies the run ID; `run_finished` always describes the
  final status when the protocol completes, including suspension. Model/run
  failures are events; initialization, transport and storage failures reject
  with `AgentError` carrying a `code`.
- `run({ sessionId, restoreRunId, model? })` resumes a session's unfinished run.
  `sessions.get()` exposes `active_run_id` after interruption. A new turn is
  rejected until that run is restored and completed/cancelled.
- One instance runs one task at a time. A competing run returns `busy`, without
  replacing the existing task. Aborting or breaking the event iterator cancels
  the run and awaits acknowledgement. `dispose()` immediately terminates the
  Worker, rejects pending work and revokes attachment display URLs. It preserves
  checkpoints so interrupted work can be restored by another instance.
- Each instance has an explicit `namespace` (1–96 letters/digits/underscores/
  hyphens). All sessions, checkpoints, workspaces, attachments, skills, Responses
  continuation records and tool journals live below that OPFS directory.
  A Web Lock rejects simultaneous writers to the same namespace, including other
  tabs. Distinct namespaces can run concurrently.
- `attachments.put/get/displayUrl`, `skills.list/install` and
  `workspace.inspect/read` expose data operations. Workspace reads return bytes;
  DOM downloads, file pickers, dialogs and inspectors belong to the consuming UI.
- `generateTitle` is an optional explicit operation, not an automatic side effect
  of running an agent. `debug.runTransitions` exposes the lower-level reducer
  transitions for adapters and debugging; ordinary clients use `run`.

Run settings are `maxSteps`, `maxOutputTokens`, `modelTimeoutMs`, `toolTimeoutMs`,
`approvalPolicy` (0–100, matching the core review level) and `allowedTools`.
Unknown settings are rejected. Worker construction can be overridden using
`workerFactory` for a custom asset deployment; the default works with the Vite
configuration above. Initialization times out after 30 seconds by default.

## Image tools

For Responses configurations, `generate_image` accepts only `{prompt}`.
`edit_image` accepts `{prompt, source_path, mask_path?}`. Describe visual
requirements in the prompt. Both use the existing Images API endpoints under
the configured model `baseUrl`, with the same API key, organization and project.
The SDK fixes the image model to `gpt-image-2`, size/quality to `auto`, and
output to PNG (generation also uses background `auto` and one image).
Output paths are generated automatically under `images/` and returned in the
tool result. Transport/model/output overrides are rejected as tool arguments.

## JavaScript execution tool

The built-in `javascript_eval` tool runs ES modules in QuickJS. The agent can
call it for calculations and JSON transformations; include its name in
`allowedTools` if using an allowlist. Arguments:

```json
{
  "source": "export function main(input) { console.log('rows', input.length); return {sum: input.reduce((a, b) => a + b, 0)}; }",
  "input": [3, 7, 11],
  "export": "main"
}
```

The result contains `value` (here `{"sum":21}`), `logs`, `logs_truncated`, and
`usage` (`duration_ms`, `output_bytes`). Input defaults to `null` and the
export name defaults to `main`. Pure async functions and top-level await are
supported. The result must be JSON serializable.

QuickJS's browser WASM engine ships through the SDK dependencies and is loaded
in a separate execution Worker; it is not linked into the Rust agent WASM
binary. Vite bundles its assets locally, with no runtime CDN requirement.
Every call creates a fresh VM. No DOM, network, filesystem, Node APIs, module
imports or timers are exposed. Agent workspace operations remain separate tools.

Fixed limits: 1 second execution, 16 MiB QuickJS heap, 512 KiB stack, 64 KiB
source, 1 MiB input, 256 KiB result, and 16 KiB/100 entries of logs. The heap
limit covers guest allocations, not total browser Worker memory. A separate
watchdog bounds startup/execution, and cancellation terminates the execution
Worker without blocking the agent Worker. Callers cannot raise these limits.
Syntax/runtime errors, timeout and resource exhaustion become tool failure
events so the model can correct its code.

## Embedded system prompt

The base system prompt is compiled into WASM from
`crates/wasm-agent/src/system-prompt.md` using Rust `include_str!`. Change that
file and rebuild WASM to change the host prompt. There is no SDK `systemPrompt`
option, and the WASM start boundary rejects supplied system prompts and system
messages in prior history.

Rust resolves installed OPFS skills and composes their locked instructions with
the embedded base prompt. The Worker passes package locations and new user
input, not instruction text. Restore reconstructs the expected prompt from the
locked skill bytes and rejects a checkpoint with a mismatched prompt. The
skill installation API remains the deliberate way to add skill instructions.

This establishes API ownership; browser-delivered WASM and JavaScript remain
inspectable and replaceable by whoever controls the page. The prompt is not a
secret or an authentication boundary.

## Persistence and compatibility

OPFS is required; there is no silent localStorage or memory-only fallback.
Checkpoint writes and canonical session updates complete before effects execute
and before transitions reach the client. The SDK checks reducer/checkpoint
protocol versions on startup and Rust verifies restored state.

The previous demo's `mina-browser-agent-state-v1.json`/localStorage records hold
UI-specific messages, without complete canonical tool history. They are left
untouched and are not silently imported as SDK sessions. New canonical sessions
use `<namespace>/sessions-v1.json`. Existing workspace files, attachments and
installed skills remain accessible when the same namespace is selected.
Pre-SDK checkpoints whose prompt does not match the embedded/locked prompt are
rejected instead of silently changing their instructions.

Tool journals avoid replaying recorded effects; a crash between an external
side effect and its journal write still requires tool-specific idempotency.
Custom provider/tool plugins and alternative persistence adapters are later
extensions; this package includes the current Responses/Messages and OPFS tools.

## Verification

```sh
pnpm --filter @mina/browser-agent check
cargo test -p mina-wasm-agent
pnpm --filter @mina/wasm-demo build
node packages/browser-agent/scripts/prepare-vite-smoke.mjs
```

The last command creates a consumer outside the repository and packs the SDK
into it. Install its dependencies, run `pnpm dev`, and open `/embedded/` for the
browser integration suite. Run `pnpm build` and `pnpm preview` and open the same
path to test production with a non-root Vite base. It uses a local deterministic
model endpoint, with no credentials or external model requests. Success is
reported as `PASS` followed by each checked behavior.
