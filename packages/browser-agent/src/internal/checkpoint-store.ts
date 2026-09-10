import { storageDirectory, validateNamespace } from "./storage-config.js";
const CHECKPOINT_FILE = "checkpoint.json";

export class BrowserCheckpointStore {
  private readonly fallback = new Map<string, string>();

  async save(sessionId: string, runId: string, checkpoint: string): Promise<void> {
    const key = checkpointKey(sessionId, runId);
    this.fallback.set(key, checkpoint);
    const file = await this.fileHandle(sessionId, runId, true);
    if (!file) return;
    const writable = await file.createWritable();
    await writable.write(checkpoint);
    await writable.close();
  }

  async load(sessionId: string, runId: string): Promise<string | null> {
    const file = await this.fileHandle(sessionId, runId, false);
    if (!file) return this.fallback.get(checkpointKey(sessionId, runId)) ?? null;
    return (await file.getFile()).text();
  }

  private async fileHandle(
    sessionId: string,
    runId: string,
    create: boolean,
  ): Promise<FileSystemFileHandle | null> {
    if (!navigator.storage?.getDirectory) throw new Error("OPFS unavailable");
    const root = await navigator.storage.getDirectory();
    try {
      const app = await root.getDirectoryHandle(storageDirectory(), { create });
      const sessions = await app.getDirectoryHandle("sessions", { create });
      const session = await sessions.getDirectoryHandle(safeSegment(sessionId), { create });
      const runs = await session.getDirectoryHandle("runs", { create });
      const run = await runs.getDirectoryHandle(safeSegment(runId), { create });
      return await run.getFileHandle(CHECKPOINT_FILE, { create });
    } catch (error) {
      if (!create && error instanceof DOMException && error.name === "NotFoundError") return null;
      throw error;
    }
  }
}

function checkpointKey(sessionId: string, runId: string) {
  return `${sessionId}:${runId}`;
}

function safeSegment(value: string) {
  if (!/^[a-zA-Z0-9-]{1,128}$/.test(value)) throw new Error("checkpoint id 无效");
  return value;
}
