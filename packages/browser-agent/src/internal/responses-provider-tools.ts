type ModelEvent = Record<string, unknown> & { type: string };

const PROVIDER_TOOL_NAMES = new Map([["web_search_call", "web_search"]]);

/**
 * Normalizes provider-executed Responses tools without turning them into
 * client-side function calls. The reducer can therefore expose their progress
 * while keeping local validation, approval, and invocation out of the path.
 */
export class ResponsesProviderToolTracker {
  private readonly started = new Set<string>();
  private readonly completed = new Set<string>();

  push(eventType: string, value: unknown): ModelEvent[] {
    if (eventType === "response.output_item.added") {
      const item = recordAt(value, "item");
      return item ? this.startFromItem(item, "in_progress") : [];
    }
    if (eventType === "response.output_item.done") {
      const item = recordAt(value, "item");
      return item ? this.completeFromItem(item) : [];
    }
    if (
      eventType === "response.web_search_call.in_progress" ||
      eventType === "response.web_search_call.searching" ||
      eventType === "response.web_search_call.completed"
    ) {
      const callId = stringAt(value, "item_id");
      if (!callId) return [];
      return this.start(callId, "web_search", {
        status: eventType.slice("response.web_search_call.".length),
      });
    }
    if (eventType === "response.completed" || eventType === "response.incomplete") {
      const events: ModelEvent[] = [];
      const output = at(value, ["response", "output"]);
      if (Array.isArray(output)) {
        for (const item of output) {
          if (isRecord(item)) events.push(...this.completeFromItem(item));
        }
      }
      for (const callId of this.started) {
        if (!this.completed.has(callId)) {
          events.push(...this.complete(callId, { status: "completed" }));
        }
      }
      return events;
    }
    return [];
  }

  private startFromItem(item: Record<string, unknown>, fallbackStatus: string) {
    const name = PROVIDER_TOOL_NAMES.get(stringAt(item, "type"));
    const callId = stringAt(item, "id");
    if (!name || !callId) return [];
    const action = at(item, ["action"]);
    return this.start(
      callId,
      name,
      isRecord(action) ? action : { status: stringAt(item, "status") || fallbackStatus },
    );
  }

  private completeFromItem(item: Record<string, unknown>) {
    const name = PROVIDER_TOOL_NAMES.get(stringAt(item, "type"));
    const callId = stringAt(item, "id");
    if (!name || !callId) return [];
    const events = this.startFromItem(item, "completed");
    const action = at(item, ["action"]);
    return [
      ...events,
      ...this.complete(callId, {
        status: stringAt(item, "status") || "completed",
        ...(isRecord(action) ? { action } : {}),
      }),
    ];
  }

  private start(callId: string, name: string, argumentsValue: unknown) {
    if (this.started.has(callId)) return [];
    this.started.add(callId);
    return [
      {
        type: "provider_tool_call_started",
        call_id: callId,
        name,
        arguments: argumentsValue,
      },
    ];
  }

  private complete(callId: string, output: unknown) {
    if (this.completed.has(callId)) return [];
    this.completed.add(callId);
    return [
      {
        type: "provider_tool_call_completed",
        call_id: callId,
        output: JSON.stringify(output),
      },
    ];
  }
}

function stringAt(value: unknown, ...path: Array<string | number>) {
  const found = at(value, path);
  return typeof found === "string" ? found : "";
}

function recordAt(value: unknown, ...path: Array<string | number>) {
  const found = at(value, path);
  return isRecord(found) ? found : undefined;
}

function at(value: unknown, path: Array<string | number>): unknown {
  let current = value;
  for (const key of path) {
    if (typeof current !== "object" || current === null) return undefined;
    current = (current as Record<string | number, unknown>)[key];
  }
  return current;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
