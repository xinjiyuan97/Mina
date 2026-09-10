import { BrowserModelHost, type PushReducerInput } from "./model-host.js";
import { BrowserToolRegistry } from "./browser-tools.js";
import { BrowserToolJournal } from "./tool-journal.js";
import type {
  AgentEffect,
  BrowserModelConfig,
  PortableToolError,
  SkillLock,
  ToolInvocationResult,
} from "./protocol.js";

export class BrowserHost {
  private readonly model: BrowserModelHost;
  private readonly tools: BrowserToolRegistry;
  private readonly journal = new BrowserToolJournal();

  constructor(config: BrowserModelConfig, skillLock?: SkillLock | null) {
    this.model = new BrowserModelHost(config);
    this.tools = new BrowserToolRegistry(config, skillLock);
  }

  async execute(
    effect: AgentEffect,
    pushInput: PushReducerInput,
    signal: AbortSignal,
  ): Promise<void> {
    if (signal.aborted) return;
    switch (effect.type) {
      case "load_tool_set":
        await pushInput({
          type: "tool_set_loaded",
          effect_id: effect.effect_id,
          tool_set: this.tools.snapshot(),
        });
        return;
      case "invoke_model":
        await this.model.invoke(effect, pushInput, signal);
        return;
      case "validate_tools":
        await pushInput({
          type: "tools_validated",
          effect_id: effect.effect_id,
          results: (effect.calls ?? []).map((call) => {
            const error = this.tools.validate(call.name, call.arguments);
            return error ? { call_id: call.call_id, error } : { call_id: call.call_id };
          }),
        });
        return;
      case "request_approval":
        throw new Error("approval effect 必须由 BrowserRun 等待 UI 处理");
      case "invoke_tool":
        await this.invokeTool(effect, pushInput, signal);
        return;
      default:
        throw new Error(`不支持的 WASM effect：${effect.type}`);
    }
  }

  private async invokeTool(
    effect: AgentEffect,
    pushInput: PushReducerInput,
    runSignal: AbortSignal,
  ) {
    let cached: ToolInvocationResult | null;
    try {
      cached = await this.journal.load(effect.effect_id);
    } catch {
      await pushInput({
        type: "tool_finished",
        effect_id: effect.effect_id,
        result: failed({
          code: "tool_journal_unavailable",
          message: "浏览器无法读取 OPFS 工具日志，因此未执行工具",
          category: "unavailable",
          retryable: true,
        }),
      });
      return;
    }
    if (cached) {
      await pushInput({ type: "tool_finished", effect_id: effect.effect_id, result: cached });
      return;
    }
    const call = effect.call;
    if (!call) {
      await this.finishTool(effect, failed(browserToolUnavailable()), pushInput);
      return;
    }

    const controller = new AbortController();
    let timedOut = false;
    const abort = () => controller.abort(runSignal.reason);
    runSignal.addEventListener("abort", abort, { once: true });
    const timer = setTimeout(() => {
      timedOut = true;
      controller.abort(new DOMException("Tool timed out", "TimeoutError"));
    }, positiveTimeout(effect.timeout_ms, 120_000));
    let result: ToolInvocationResult;
    try {
      result = await this.tools.invoke(call.name, call.arguments, controller.signal);
      if (timedOut) result = failed(toolTimeout());
    } catch (cause) {
      result = failed(
        timedOut
          ? toolTimeout()
          : {
              code: "browser_tool_failed",
              message: cause instanceof Error ? cause.message : "浏览器工具执行失败",
              category: "internal",
              retryable: false,
            },
      );
    } finally {
      clearTimeout(timer);
      runSignal.removeEventListener("abort", abort);
    }
    if (runSignal.aborted) return;
    await this.finishTool(effect, result, pushInput);
  }

  private async finishTool(
    effect: AgentEffect,
    result: ToolInvocationResult,
    pushInput: PushReducerInput,
  ) {
    try {
      await this.journal.save(effect.effect_id, result);
    } catch (cause) {
      console.warn("Could not persist browser tool result journal", cause);
    }
    await pushInput({ type: "tool_finished", effect_id: effect.effect_id, result });
  }
}

function browserToolUnavailable(): PortableToolError {
  return {
    code: "browser_tool_unavailable",
    message: "当前 Browser Host 没有注册这个工具",
    category: "unavailable",
    retryable: false,
  };
}

function toolTimeout(): PortableToolError {
  return {
    code: "browser_tool_timeout",
    message: "浏览器工具执行超时",
    category: "timeout",
    retryable: true,
  };
}

function failed(error: PortableToolError): ToolInvocationResult {
  return { type: "failed", error };
}

function positiveTimeout(value: number | undefined, fallback: number) {
  return typeof value === "number" && Number.isFinite(value) && value > 0 ? value : fallback;
}
