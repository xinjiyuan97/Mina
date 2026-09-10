import type { ToolInvocationResult } from "./protocol.js";

import { storageDirectory, validateNamespace } from "./storage-config.js";
const JOURNAL_DIRECTORY = "tool-results";

export class BrowserToolJournal {
  private readonly fallback = new Map<string, ToolInvocationResult>();

  async load(effectId: string): Promise<ToolInvocationResult | null> {
    const cached = this.fallback.get(effectId);
    if (cached) return structuredClone(cached);
    const file = await this.file(effectId, false).catch((cause) => {
      if (cause instanceof DOMException && cause.name === "NotFoundError") return null;
      throw cause;
    });
    if (!file) return null;
    const result = JSON.parse(await (await file.getFile()).text()) as ToolInvocationResult;
    this.fallback.set(effectId, result);
    return structuredClone(result);
  }

  async save(effectId: string, result: ToolInvocationResult) {
    this.fallback.set(effectId, structuredClone(result));
    const file = await this.file(effectId, true);
    const writable = await file.createWritable();
    await writable.write(JSON.stringify(result));
    await writable.close();
  }

  private async file(effectId: string, create: boolean) {
    if (!navigator.storage?.getDirectory) throw new Error("当前浏览器不支持 OPFS");
    const root = await navigator.storage.getDirectory();
    const app = await root.getDirectoryHandle(storageDirectory(), { create });
    const journal = await app.getDirectoryHandle(JOURNAL_DIRECTORY, { create });
    return journal.getFileHandle(`${await digest(effectId)}.json`, { create });
  }
}

async function digest(value: string) {
  const bytes = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(value));
  return Array.from(new Uint8Array(bytes), (byte) => byte.toString(16).padStart(2, "0")).join("");
}
