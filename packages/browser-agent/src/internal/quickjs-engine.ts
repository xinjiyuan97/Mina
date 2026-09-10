import variant from "@jitl/quickjs-singlefile-browser-release-sync";
import { newQuickJSWASMModuleFromVariant, type QuickJSHandle, type QuickJSContext, type QuickJSRuntime } from "quickjs-emscripten-core";
import { bytes, JAVASCRIPT_LIMITS as limits, JavaScriptError, validateJavaScriptRequest, type JavaScriptRequest, type JavaScriptOutput } from "./javascript-contract.js";

export async function loadQuickJS() { return newQuickJSWASMModuleFromVariant(variant); }

/** Every invocation gets an independent VM, without module loaders or host I/O. */
export function evaluateJavaScript(engine: Awaited<ReturnType<typeof loadQuickJS>>, request: JavaScriptRequest): JavaScriptOutput {
  const args = validateJavaScriptRequest(request);
  const runtime = engine.newRuntime();
  runtime.setMemoryLimit(limits.memoryBytes);
  runtime.setMaxStackSize(limits.stackBytes);
  const start = performance.now();
  const deadline = start + limits.timeoutMs;
  runtime.setInterruptHandler(() => performance.now() >= deadline);
  const context = runtime.newContext();
  const handles: QuickJSHandle[] = [];
  const own = (handle: QuickJSHandle) => { handles.push(handle); return handle; };
  const logs: JavaScriptOutput["logs"] = [];
  let logBytes = 0;
  let logsTruncated = false;
  try {
    // Capture serialization before guest code can replace JSON globals.
    const serialize = own(context.unwrapResult(context.evalCode("((stringify) => (value) => stringify(value))(JSON.stringify)")));
    const input = own(context.unwrapResult(context.evalCode(`JSON.parse(${JSON.stringify(JSON.stringify(args.input))})`)));
    const consoleObject = own(context.newObject());
    for (const level of ["log", "info", "warn", "error", "debug"]) {
      const fn = own(context.newFunction(level, (...values) => {
        if (logs.length >= limits.logCount || logBytes >= limits.logBytes) { logsTruncated = true; return; }
        const parts: string[] = [];
        for (const value of values) {
          if (context.typeof(value) === "string") parts.push(context.getString(value));
          else {
            const encoded = context.callFunction(serialize, context.undefined, value);
            try { parts.push(encoded.error ? "[unserializable]" : context.typeof(encoded.value) === "undefined" ? "undefined" : context.getString(encoded.value)); }
            finally { encoded.dispose(); }
          }
          // Avoid accumulating an unbounded argument list in the host.
          if (parts.join(" ").length > limits.logBytes) break;
        }
        let text = parts.join(" ");
        const remaining = limits.logBytes - logBytes;
        if (bytes(text) > remaining) { text = new TextDecoder().decode(new TextEncoder().encode(text).subarray(0, Math.max(0, remaining - 3))); logsTruncated = true; }
        logBytes += bytes(text);
        logs.push({ level, text });
      }));
      context.setProp(consoleObject, level, fn);
    }
    context.setProp(context.global, "console", consoleObject);
    const evaluated = own(context.unwrapResult(context.evalCode(args.source, "agent-script.mjs", { type: "module" })));
    const namespace = own(settle(context, runtime, evaluated, deadline));
    const fn = own(context.getProp(namespace, args.export!));
    if (context.typeof(fn) !== "function") throw new JavaScriptError("javascript_export_not_found", `Export ${args.export} is not a function`, "not_found");
    const called = own(context.unwrapResult(context.callFunction(fn, context.undefined, input)));
    const result = own(settle(context, runtime, called, deadline));
    const serialized = own(context.unwrapResult(context.callFunction(serialize, context.undefined, result)));
    if (context.typeof(serialized) !== "string") throw new JavaScriptError("javascript_invalid_result", "The exported function must return a JSON value");
    const json = context.getString(serialized);
    const outputBytes = bytes(json);
    if (outputBytes > limits.outputBytes) throw new JavaScriptError("javascript_output_too_large", "JavaScript output exceeds 256 KiB", "resource_exhausted");
    return { value: JSON.parse(json), logs, logs_truncated: logsTruncated, usage: { duration_ms: Math.round(performance.now() - start), output_bytes: outputBytes } };
  } catch (cause) {
    if (cause instanceof JavaScriptError) throw cause;
    if (performance.now() >= deadline) throw new JavaScriptError("javascript_timeout", "JavaScript exceeded its 1 second execution limit", "timeout");
    const message = cause instanceof Error ? cause.message : String(cause);
    if (/out of memory|stack overflow|allocation/i.test(message)) throw new JavaScriptError("javascript_resource_exhausted", message.slice(0, 2048), "resource_exhausted");
    throw new JavaScriptError("javascript_exception", message.slice(0, 2048));
  } finally {
    for (const handle of handles.reverse()) if (handle.alive) handle.dispose();
    context.dispose();
    runtime.dispose();
  }
}

function settle(context: QuickJSContext, runtime: QuickJSRuntime, value: QuickJSHandle, deadline: number): QuickJSHandle {
  while (performance.now() < deadline) {
    const state = context.getPromiseState(value);
    if (state.type === "fulfilled") return state.value;
    if (state.type === "rejected") return context.unwrapResult(state);
    const jobs = runtime.executePendingJobs(1);
    if (jobs.error) {
      try { throw new JavaScriptError("javascript_exception", String(context.dump(jobs.error)).slice(0, 2048)); }
      finally { jobs.error.dispose(); }
    }
    if (!jobs.value) throw new JavaScriptError("javascript_pending_promise", "Promise cannot resolve: timers and host I/O are unavailable");
  }
  throw new JavaScriptError("javascript_timeout", "JavaScript exceeded its 1 second execution limit", "timeout");
}
