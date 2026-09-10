import { JAVASCRIPT_LIMITS, JavaScriptError, validateJavaScriptRequest, type JavaScriptOutput } from "./javascript-contract.js";
import type { PortableToolError } from "./protocol.js";

/** Keep QuickJS off the agent event loop so cancellation can terminate it immediately. */
export function executeJavaScript(request: unknown, signal: AbortSignal): Promise<JavaScriptOutput> {
  const args = validateJavaScriptRequest(request);
  if (signal.aborted) return Promise.reject(new JavaScriptError("javascript_cancelled", "JavaScript execution cancelled", "cancelled"));
  return new Promise((resolve, reject) => {
    const worker = new Worker(new URL("./quickjs-worker.js", import.meta.url), { type: "module", name: "mina-javascript-eval" });
    let done = false;
    const finish = (error?: Error, result?: JavaScriptOutput) => {
      if (done) return;
      done = true;
      clearTimeout(timer);
      signal.removeEventListener("abort", abort);
      worker.terminate();
      if (error) reject(error); else resolve(result!);
    };
    const abort = () => finish(new JavaScriptError("javascript_cancelled", "JavaScript execution cancelled", "cancelled"));
    let timer = setTimeout(() => finish(new JavaScriptError("javascript_engine_timeout", "QuickJS engine initialization timed out", "timeout")), 15_000);
    signal.addEventListener("abort", abort, { once: true });
    worker.addEventListener("message", (event: MessageEvent<
      { type: "executing" } | { type: "result"; result: JavaScriptOutput } | { type: "error"; code: string; message: string; category: PortableToolError["category"] }
    >) => {
      const message = event.data;
      if (message.type === "executing") {
        clearTimeout(timer);
        timer = setTimeout(() => finish(new JavaScriptError("javascript_timeout", "JavaScript worker exceeded its execution deadline", "timeout")), JAVASCRIPT_LIMITS.timeoutMs + 1000);
      } else if (message.type === "result") finish(undefined, message.result);
      else finish(new JavaScriptError(message.code, message.message, message.category));
    });
    worker.addEventListener("error", () => finish(new JavaScriptError("javascript_engine_failed", "QuickJS Worker failed", "internal")));
    worker.addEventListener("messageerror", () => finish(new JavaScriptError("javascript_engine_failed", "Invalid QuickJS Worker response", "internal")));
    try { worker.postMessage(args); } catch (error) { finish(error instanceof Error ? error : new Error(String(error))); }
  });
}
