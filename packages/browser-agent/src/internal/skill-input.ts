import type { SkillInstallFile } from "./protocol.js";
const MAX_PACKAGE_BYTES = 2 * 1024 * 1024;
const MAX_PACKAGE_FILES = 512;
const MAX_RESOURCE_DEPTH = 8;
export async function readSkillDirectory(files: FileList): Promise<SkillInstallFile[]> {
  const selected = Array.from(files);
  if (selected.length === 0) {
    throw new SkillInstallError("skill_package_empty", "请选择一个 Skill 目录");
  }
  if (selected.length > MAX_PACKAGE_FILES) {
    throw new SkillInstallError(
      "skill_package_too_many_files",
      `Skill package 不能超过 ${MAX_PACKAGE_FILES} 个文件`,
    );
  }

  const paths = selected.map((file) => file.webkitRelativePath || file.name);
  const firstComponents = new Set(paths.map((path) => normalizedSlashes(path).split("/")[0]));
  const stripRoot = paths.every((path) => normalizedSlashes(path).includes("/"));
  if (stripRoot && firstComponents.size !== 1) {
    throw new SkillInstallError("skill_package_multiple_roots", "一次只能导入一个 Skill 目录");
  }

  let totalBytes = 0;
  const seen = new Set<string>();
  const prepared: SkillInstallFile[] = [];
  for (let index = 0; index < selected.length; index += 1) {
    const file = selected[index];
    const sourcePath = normalizedSlashes(paths[index]);
    const path = validatePackagePath(
      stripRoot ? sourcePath.slice(sourcePath.indexOf("/") + 1) : sourcePath,
    );
    if (seen.has(path)) {
      throw new SkillInstallError("skill_package_duplicate_path", `Skill 中存在重复路径：${path}`);
    }
    seen.add(path);
    totalBytes += file.size;
    if (totalBytes > MAX_PACKAGE_BYTES) {
      throw new SkillInstallError("skill_package_too_large", "Skill package 不能超过 2 MiB");
    }
    prepared.push({ path, bytes: await file.arrayBuffer() });
  }

  if (!seen.has("skill.toml") || !seen.has("SKILL.md")) {
    throw new SkillInstallError(
      "skill_package_missing_contract",
      "Skill 根目录必须包含 skill.toml 与 SKILL.md",
    );
  }
  return prepared;
}

export class SkillInstallError extends Error {
  readonly code: string;

  constructor(code: string, message: string) {
    super(message);
    this.code = code;
  }
}

export function validateInstallFiles(files: SkillInstallFile[]) {
  if (files.length === 0 || files.length > MAX_PACKAGE_FILES) {
    throw new SkillInstallError("skill_package_invalid", "Skill package 文件数量无效");
  }
  let totalBytes = 0;
  const seen = new Set<string>();
  for (const file of files) {
    const path = validatePackagePath(file.path);
    if (path !== file.path || seen.has(path)) {
      throw new SkillInstallError("skill_package_invalid", "Skill package 路径无效");
    }
    seen.add(path);
    totalBytes += file.bytes.byteLength;
  }
  if (totalBytes > MAX_PACKAGE_BYTES) {
    throw new SkillInstallError("skill_package_too_large", "Skill package 不能超过 2 MiB");
  }
  if (!seen.has("skill.toml") || !seen.has("SKILL.md")) {
    throw new SkillInstallError(
      "skill_package_missing_contract",
      "Skill 根目录必须包含 skill.toml 与 SKILL.md",
    );
  }
}

function validatePackagePath(source: string) {
  const path = normalizedSlashes(source).replace(/^\/+/, "");
  const segments = path.split("/");
  if (
    !path ||
    path.length > 512 ||
    segments.length > MAX_RESOURCE_DEPTH + 1 ||
    segments.some(
      (segment) =>
        !segment ||
        segment === "." ||
        segment === ".." ||
        segment.length > 120 ||
        /[\u0000-\u001f]/.test(segment),
    )
  ) {
    throw new SkillInstallError("skill_package_unsafe_path", `Skill 文件路径无效：${source}`);
  }
  return segments.join("/");
}


function normalizedSlashes(path: string) { return path.replaceAll("\\", "/"); }
