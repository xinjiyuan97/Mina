import assert from "node:assert/strict";
import test from "node:test";

import { readSkillDirectory } from "../dist/internal/skill-input.js";

function fixtureFile(path, content) {
  const bytes = new TextEncoder().encode(content);
  return {
    name: path.split("/").at(-1),
    webkitRelativePath: path,
    size: bytes.byteLength,
    arrayBuffer: async () => bytes.buffer.slice(0),
  };
}

test("prepares one selected Skill directory as package-relative bytes", async () => {
  const prepared = await readSkillDirectory([
    fixtureFile("browser-test/skill.toml", 'skill_id = "browser-test"'),
    fixtureFile("browser-test/SKILL.md", "Follow the guide."),
    fixtureFile("browser-test/references/guide.md", "Locked content"),
  ]);

  assert.deepEqual(
    prepared.map((file) => file.path),
    ["skill.toml", "SKILL.md", "references/guide.md"],
  );
  assert.equal(
    new TextDecoder().decode(prepared[2].bytes),
    "Locked content",
  );
});

test("rejects multiple roots and unsafe package paths before touching OPFS", async () => {
  await assert.rejects(
    readSkillDirectory([
      fixtureFile("one/skill.toml", "manifest"),
      fixtureFile("two/SKILL.md", "instructions"),
    ]),
    /一次只能导入一个 Skill 目录/,
  );
  await assert.rejects(
    readSkillDirectory([
      fixtureFile("browser-test/skill.toml", "manifest"),
      fixtureFile("browser-test/SKILL.md", "instructions"),
      fixtureFile("browser-test/references/../secret.md", "secret"),
    ]),
    /Skill 文件路径无效/,
  );
});
