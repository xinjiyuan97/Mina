import { validateInstallFiles, SkillInstallError } from "./skill-input.js";
import { skillsPrefix } from "./storage-config.js";
import {
  skillsListJson,
  workspaceCreateDirectoryJson,
  workspaceRemove,
  workspaceRenameJson,
  workspaceWriteJson,
} from "../pkg/mina_wasm_agent.js";

import { BrowserSkillStore } from "./opfs-skills.js";
import type {
  SkillDescriptor,
  SkillInstallFile,
  SkillInstallResult,
} from "./protocol.js";

const PENDING_MARKER = ".mina-skill-pending";

/**
 * Management-only installer for OPFS Skills. The Agent tool registry never
 * receives this capability; its SkillStore remains read-only.
 */
export class BrowserSkillInstaller {
  private readonly store = new BrowserSkillStore();

  async install(files: SkillInstallFile[]): Promise<SkillInstallResult> {
    validateInstallFiles(files);
    await this.store.ensureRoot();
    const installedBefore = (await this.store.list()).packages;
    const staging = `.installing-${crypto.randomUUID()}`;
    let stagingExists = false;
    let destinationCreated: string | null = null;
    let installationVerified = false;

    try {
      for (const file of files) {
        stagingExists = true;
        await workspaceWriteJson(
          skillsPrefix(),
          `${staging}/${file.path}`,
          new Uint8Array(file.bytes),
          "create_new",
          true,
        );
      }

      const staged = parseJson<SkillDescriptor[]>(
        await skillsListJson(`${skillsPrefix()}/${staging}`),
      );
      if (staged.length !== 1) {
        throw new SkillInstallError(
          "skill_package_invalid",
          "所选目录必须恰好包含一个 Skill package",
        );
      }
      const descriptor = staged[0];
      const existing = installedBefore.find(
        (skill) =>
          skill.skill_id === descriptor.skill_id && skill.version === descriptor.version,
      );
      if (existing) {
        if (existing.digest === descriptor.digest) {
          return { status: "already_installed", descriptor: existing };
        }
        throw new SkillInstallError(
          "skill_version_conflict",
          `${descriptor.skill_id}@${descriptor.version} 已存在且内容不同；请提升 version 后再导入`,
        );
      }

      const destination = packageDestination(descriptor);
      await workspaceCreateDirectoryJson(
        skillsPrefix(),
        destination.slice(0, destination.lastIndexOf("/")),
        true,
      );
      try {
        await workspaceRenameJson(skillsPrefix(), staging, destination, false);
        stagingExists = false;
        destinationCreated = destination;
      } catch (cause) {
        if (errorCode(cause) !== "workspace_unsupported") throw cause;
        destinationCreated = destination;
        await workspaceWriteJson(
          skillsPrefix(),
          `${destination}/${PENDING_MARKER}`,
          new Uint8Array([1]),
          "create_new",
          true,
        );
        for (const file of files) {
          await workspaceWriteJson(
            skillsPrefix(),
            `${destination}/${file.path}`,
            new Uint8Array(file.bytes),
            "create_new",
            true,
          );
        }
        await workspaceRemove(skillsPrefix(), `${destination}/${PENDING_MARKER}`, false);
        await workspaceRemove(skillsPrefix(), staging, true);
        stagingExists = false;
      }

      const installed = (await this.store.list()).packages.find(
        (skill) =>
          skill.skill_id === descriptor.skill_id &&
          skill.version === descriptor.version &&
          skill.digest === descriptor.digest,
      );
      if (!installed) {
        throw new SkillInstallError(
          "skill_install_verification_failed",
          "Skill 已写入 OPFS，但最终校验失败",
        );
      }
      installationVerified = true;
      return { status: "installed", descriptor: installed };
    } catch (cause) {
      if (cause instanceof SkillInstallError) throw cause;
      throw new SkillInstallError(
        "skill_install_failed",
        safeMessage(cause) || "Skill 导入失败",
      );
    } finally {
      if (stagingExists) {
        try {
          await workspaceRemove(skillsPrefix(), staging, true);
        } catch {
          // A failed best-effort cleanup must not hide the original error.
        }
      }
      if (destinationCreated && !installationVerified) {
        try {
          await workspaceRemove(skillsPrefix(), destinationCreated, true);
        } catch {
          // Keep the original validation/install error.
        }
      }
    }
  }
}

function packageDestination(descriptor: SkillDescriptor) {
  const digest = descriptor.digest.replace(/^sha256:/, "").slice(0, 16);
  return `packages/${digest}-${crypto.randomUUID()}`;
}

function parseJson<T>(source: string): T {
  try {
    return JSON.parse(source) as T;
  } catch {
    throw new SkillInstallError("skill_invalid_result", "WASM Skill Store 返回了无效数据");
  }
}

function safeMessage(cause: unknown) {
  if (typeof cause === "string") {
    try {
      const parsed = JSON.parse(cause) as { message?: unknown };
      if (typeof parsed.message === "string") return parsed.message;
    } catch {
      return cause;
    }
  }
  return cause instanceof Error ? cause.message : String(cause);
}

function errorCode(cause: unknown) {
  const source = typeof cause === "string" ? cause : cause instanceof Error ? cause.message : "";
  try {
    const parsed = JSON.parse(source) as { code?: unknown };
    return typeof parsed.code === "string" ? parsed.code : "";
  } catch {
    return "";
  }
}
