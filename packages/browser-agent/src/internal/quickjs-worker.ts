/// <reference lib="webworker" />
import { loadQuickJS, evaluateJavaScript } from "./quickjs-engine.js";
import { JavaScriptError, type JavaScriptRequest } from "./javascript-contract.js";
const worker = self as DedicatedWorkerGlobalScope;
// A new Worker handles exactly one invocation and is then terminated by its owner.
worker.addEventListener("message", async (event: MessageEvent<JavaScriptRequest>) => {
  try {
    const engine = await loadQuickJS();
    worker.postMessage({ type: "executing" });
    worker.postMessage({ type: "result", result: evaluateJavaScript(engine, event.data) });
  } catch (error) {
    worker.postMessage({ type: "error", code: error instanceof JavaScriptError ? error.code : "javascript_engine_failed", category: error instanceof JavaScriptError ? error.category : "internal", message: error instanceof Error ? error.message : String(error) });
  }
}, { once: true });
