type ModelEvent = Record<string, unknown> & { type: string };

/** Maps provider lifecycle signals independently from optional reasoning text. */
export class ResponsesReasoningTracker {
  private readonly activeItems = new Set<string>();

  push(eventType: string, value: unknown): ModelEvent[] {
    if (eventType === "response.output_item.added" && itemType(value) === "reasoning") {
      const key = itemKey(value);
      this.activeItems.add(key);
      return [{ type: "reasoning_started", redacted: true }];
    }

    if (eventType === "response.output_item.done" && itemType(value) === "reasoning") {
      const key = itemKey(value);
      if (!this.activeItems.delete(key)) return [];
      return [{ type: "reasoning_completed", redacted: true }];
    }

    return [];
  }
}

export class MessagesReasoningTracker {
  private readonly blocks = new Map<number, boolean>();

  push(eventType: string, value: unknown): ModelEvent[] {
    const index = numberAt(value, "index") ?? 0;
    if (eventType === "content_block_start") {
      const type = stringAt(value, "content_block", "type");
      if (type !== "thinking" && type !== "redacted_thinking") return [];
      const redacted = type === "redacted_thinking";
      this.blocks.set(index, redacted);
      return [{ type: "reasoning_started", redacted }];
    }

    if (eventType === "content_block_stop") {
      const redacted = this.blocks.get(index);
      if (redacted === undefined) return [];
      this.blocks.delete(index);
      return [{ type: "reasoning_completed", redacted }];
    }

    return [];
  }
}

function itemType(value: unknown) {
  return stringAt(value, "item", "type");
}

function itemKey(value: unknown) {
  return (
    stringAt(value, "item", "id") ||
    `output:${numberAt(value, "output_index") ?? 0}`
  );
}

function stringAt(value: unknown, ...path: Array<string | number>) {
  const found = at(value, path);
  return typeof found === "string" ? found : "";
}

function numberAt(value: unknown, ...path: Array<string | number>) {
  const found = at(value, path);
  return typeof found === "number" && Number.isFinite(found) ? found : undefined;
}

function at(value: unknown, path: Array<string | number>): unknown {
  let current = value;
  for (const key of path) {
    if (typeof current !== "object" || current === null) return undefined;
    current = (current as Record<string | number, unknown>)[key];
  }
  return current;
}
