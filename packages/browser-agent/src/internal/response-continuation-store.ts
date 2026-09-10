import { storageDirectory, validateNamespace } from "./storage-config.js";
const CONTINUATIONS_DIRECTORY = "model-continuations";

export type ResponsesContinuation = {
  effectId: string;
  responseId: string;
  history?: unknown[];
};

export class BrowserResponsesContinuationStore {
  async load(runId: string): Promise<ResponsesContinuation | null> {
    const file = await this.file(runId, false).catch((cause) => {
      if (cause instanceof DOMException && cause.name === "NotFoundError") return null;
      throw cause;
    });
    if (!file) return null;
    const value = JSON.parse(await (await file.getFile()).text()) as Partial<ResponsesContinuation>;
    if (typeof value.effectId !== "string" || typeof value.responseId !== "string") {
      throw new Error("Responses continuation 文件格式无效");
    }
    if (value.history !== undefined && !Array.isArray(value.history)) {
      throw new Error("Responses continuation history 格式无效");
    }
    return {
      effectId: value.effectId,
      responseId: value.responseId,
      ...(value.history ? { history: value.history } : {}),
    };
  }

  async save(runId: string, continuation: ResponsesContinuation) {
    const file = await this.file(runId, true);
    const writable = await file.createWritable();
    try {
      await writable.write(JSON.stringify(continuation));
      await writable.close();
    } catch (cause) {
      await writable.abort().catch(() => undefined);
      throw cause;
    }
  }

  private async file(runId: string, create: boolean) {
    if (!navigator.storage?.getDirectory) throw new Error("当前浏览器不支持 OPFS");
    if (!/^[a-zA-Z0-9-]{1,128}$/.test(runId)) throw new Error("Responses run_id 无效");
    const root = await navigator.storage.getDirectory();
    const app = await root.getDirectoryHandle(storageDirectory(), { create });
    const continuations = await app.getDirectoryHandle(CONTINUATIONS_DIRECTORY, { create });
    return continuations.getFileHandle(`${runId}.json`, { create });
  }
}
