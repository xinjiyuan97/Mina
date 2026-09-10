import {
  skillReadResourceTextJson,
  skillsListJson,
  skillsResolveJson,
  skillsVerifyLockJson,
  workspaceCreateDirectoryJson,
} from "../pkg/mina_wasm_agent.js";

import type {
  LockedSkill,
  ResolvedSkillSet,
  SkillDescriptor,
  SkillInspection,
  SkillLock,
} from "./protocol.js";

import { skillsPrefix, skillsUrl } from "./storage-config.js";

const MAX_RESOURCE_TEXT_BYTES = 1024 * 1024;

type SkillResourceText = {
  skill_id: string;
  path: string;
  media_type?: string;
  content: string;
  digest: string;
};

export class BrowserSkillStore {
  async ensureRoot(signal?: AbortSignal) {
    throwIfAborted(signal);
    await skillCall(() =>
      workspaceCreateDirectoryJson(skillsPrefix(), ".", true),
    );
    throwIfAborted(signal);
  }

  async list(signal?: AbortSignal): Promise<SkillInspection> {
    await this.ensureRoot(signal);
    const source = await skillCall(() => skillsListJson(skillsPrefix()));
    throwIfAborted(signal);
    return {
      root: skillsUrl(),
      packages: parseJson<SkillDescriptor[]>(source),
    };
  }

  async resolve(input: string, signal?: AbortSignal): Promise<ResolvedSkillSet> {
    await this.ensureRoot(signal);
    const source = await skillCall(() =>
      skillsResolveJson(
        skillsPrefix(),
        JSON.stringify({
          current_input: input,
          explicit: [],
          profile_defaults: [],
          include_store_defaults: true,
          max_skills: 4,
        }),
      ),
    );
    throwIfAborted(signal);
    return parseJson<ResolvedSkillSet>(source);
  }

  async verifyLock(lock: SkillLock, signal?: AbortSignal) {
    await this.ensureRoot(signal);
    const source = await skillCall(() =>
      skillsVerifyLockJson(skillsPrefix(), JSON.stringify(lock)),
    );
    throwIfAborted(signal);
    return parseJson<SkillLock>(source);
  }

  async readText(
    locked: LockedSkill,
    path: string,
    signal?: AbortSignal,
  ): Promise<SkillResourceText> {
    throwIfAborted(signal);
    const source = await skillCall(() =>
      skillReadResourceTextJson(
        skillsPrefix(),
        JSON.stringify({
          locked,
          path,
          max_bytes: MAX_RESOURCE_TEXT_BYTES,
        }),
      ),
    );
    throwIfAborted(signal);
    return parseJson<SkillResourceText>(source);
  }
}

export class BrowserSkillStoreError extends Error {
  readonly code: string;
  readonly retryable: boolean;

  constructor(
    code: string,
    message: string,
    retryable = false,
  ) {
    super(message);
    this.code = code;
    this.retryable = retryable;
  }
}

async function skillCall<T>(operation: () => T | Promise<T>): Promise<T> {
  try {
    return await operation();
  } catch (cause) {
    if (cause instanceof BrowserSkillStoreError) throw cause;
    const source = typeof cause === "string" ? cause : cause instanceof Error ? cause.message : "";
    try {
      const parsed = JSON.parse(source) as {
        code?: unknown;
        message?: unknown;
        retryable?: unknown;
      };
      if (typeof parsed.code === "string" && typeof parsed.message === "string") {
        throw new BrowserSkillStoreError(
          parsed.code,
          parsed.message,
          parsed.retryable === true,
        );
      }
    } catch (parsedCause) {
      if (parsedCause instanceof BrowserSkillStoreError) throw parsedCause;
    }
    throw new BrowserSkillStoreError(
      "skill_store_failed",
      source || "OPFS Skill Store 操作失败",
      true,
    );
  }
}

function parseJson<T>(source: string): T {
  try {
    return JSON.parse(source) as T;
  } catch {
    throw new BrowserSkillStoreError("skill_invalid_result", "WASM Skill Store 返回了无效数据");
  }
}

function throwIfAborted(signal?: AbortSignal) {
  if (signal?.aborted) throw signal.reason ?? new DOMException("Aborted", "AbortError");
}
