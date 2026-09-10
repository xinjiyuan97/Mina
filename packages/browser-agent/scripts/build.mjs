import { spawnSync } from "node:child_process";
import { cp, mkdir, readdir, rm } from "node:fs/promises";
import { fileURLToPath } from "node:url";
const root = fileURLToPath(new URL("../", import.meta.url));
const pkg = fileURLToPath(new URL("../src/pkg", import.meta.url));
for (const [cmd, args] of [
  ["wasm-pack", ["build", "../../crates/wasm-agent", "--target", "web", "--out-dir", pkg, process.argv.includes("--dev") ? "--dev" : "--release"]],
  ["pnpm", ["exec", "tsc", "-p", "tsconfig.json"]],
]) {
  const result = spawnSync(cmd, args, { cwd: root, stdio: "inherit" });
  if (result.status !== 0) process.exit(result.status ?? 1);
}
await rm(new URL("../dist/pkg", import.meta.url), { recursive: true, force: true });
await mkdir(new URL("../dist/pkg", import.meta.url), { recursive: true });
for (const entry of await readdir(pkg)) {
  if (entry.endsWith(".js") || entry.endsWith(".wasm") || entry.endsWith(".ts") || entry === "snippets") {
    await cp(new URL(`../src/pkg/${entry}`, import.meta.url), new URL(`../dist/pkg/${entry}`, import.meta.url), { recursive: true });
  }
}
