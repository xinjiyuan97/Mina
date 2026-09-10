# Mina Browser Agent Demo

The React chat and inspectors are a consumer of `@mina/browser-agent`, the
headless SDK in `packages/browser-agent`. All runtime/model/tool/storage code
lives in that package. Rust bindings and the embedded system prompt live in
`crates/wasm-agent`.

See [the SDK README](../../packages/browser-agent/README.md) for independent Vite
integration, the public API, instance lifecycle, persistence and migration notes.
The demo owns only UI event projection, chat components, model-setting controls,
file download actions and its explicit application instance in `web/runtime.ts`.
The SDK owns canonical conversations; the chat renderer is never a source of
model history. Changing the host system prompt requires rebuilding WASM.

The demo uses namespace `mina-browser-agent`. Old UI-only conversation records
are preserved but not imported; existing workspace files, attachments and skills
under the same namespace remain available.

## OPFS Skills

The first browser release deliberately supports one runtime provider: OPFS.
HTTPS can later be added as an installation source without becoming a second
runtime provider. Import a local directory from the Skills inspector. A package
has this shape:

```text
my-skill/
├── skill.toml
├── SKILL.md
└── references/
    └── guide.md
```

Example manifest:

```toml
skill_id = "my-skill"
version = "1.0.0"
description = "A browser-local Skill"
required_tools = ["read_skill_resource"]
activation = { type = "routable", hints = ["my-skill"] }
resources = [{ path = "references/guide.md", media_type = "text/markdown" }]
```

`activation.type` may be `explicit_only`, `profile_default`, or `routable`.
At the start of a new Run, Rust resolves the matching packages, compiles their
instructions, and stores an exact `SkillLock` (package digests plus compiler
version and compiled-instruction digest) in the checkpoint. Restoring a Run
fails if a locked package is missing or its bytes have changed. Installed
`skill_id` + `version` pairs are immutable; changed content must use a new
version.

An active Skill adds `read_skill_resource` to that Run. It accepts only a
locked `skill_id` and package-relative UTF-8 resource path. It cannot read
another Skill, `skill.toml`, `SKILL.md`, or the normal workspace. Packages are
limited to 2 MiB and resource reads to 1 MiB.

A ready-to-import routable fixture is available at
`examples/skills/browser-test`; send a message containing `skill-test` to
exercise its routing and locked resource-read path.

The composer supports paste, drag-and-drop and file picking. Responses accepts
PNG/JPEG/WebP/GIF images plus PDF, text, JSON, CSV, Word, PowerPoint and Excel
documents through `input_image` / `input_file`. Messages accepts the image
formats and PDFs through native image/document content blocks. Each file is
limited to 10 MiB and each message to eight attachments.

## Run

```bash
pnpm install
pnpm dev:wasm
```

Open <http://127.0.0.1:3002>.

## Verify

```bash
pnpm check:wasm
pnpm build:wasm
cargo test -p mina-wasm-agent
```

Generated `pkg/` and `dist/` directories are intentionally ignored.

## 测试 JavaScript 工具

刷新页面后可以发送：“请使用 javascript_eval 计算 [3, 7, 11] 的总和，并输出一条日志。”
工具执行 ES module 的 main(input)，返回 JSON 和日志；每次使用独立 QuickJS VM，
限制 1 秒执行时间和 16 MiB 堆内存。QuickJS 浏览器 WASM 随 SDK 打包，由独立 Worker 执行。
