# P0 validation — 2026-09-10

- `cargo test -p agent-core -p mina-wasm-agent`: 75 tests passed.
- SDK Node tests: 37 tests passed, covering lifecycle, approval RPC isolation,
  canonical persistence, attachment isolation, provider event normalization and
  packaged WASM/OPFS assets.
- SDK and demo TypeScript checks passed. The external consumer also passed
  TypeScript checking with `strict` and without `skipLibCheck`.
- Rust boundary checks and formatting checks passed.
- Release WASM build and existing React demo production build passed. The demo
  retains dependency warnings about `use client` directives and large UI chunks.
- A tarball was installed in a temporary Vite 7.3.6 project outside the workspace,
  with no React, Mina source imports, Rust tooling or UI dependencies.
- Both Vite development mode and production preview passed the 27 browser checks
  in `tests/vite-consumer/main.ts` under the non-root `/embedded/` base path.

Browser checks exercised actual Dedicated Workers, release WASM, OPFS, Web Locks,
a local deterministic Messages API SSE endpoint, two independent namespaces,
QuickJS JSON/log output, infinite-loop timeout, cancellation and recovery,
tool execution and canonical multi-turn history, attachments, locked skill
installation/restoration, approval after interruption, and cancellation.
No external model service or real credential was used.

The release WASM asset is approximately 1.91 MB (592 KB gzip). The minimal
consumer build has approximately 19 KB of main JavaScript, 80 KB of agent Worker JavaScript, and 730 KB of
QuickJS execution Worker/engine chunks, before gzip. QuickJS WASM is embedded
in its engine chunk; these numbers include the integration fixture.

The tested tarball is `mina-browser-agent-0.1.0.tgz` alongside this file (ignored
by Git). Rebuild and repack before distributing subsequent source changes.

QuickJS unit checks exercise real engine execution, async jobs, VM isolation,
source/input/output/heap limits, bounded logs, malformed results and cancellation.

Image tool request tests verify prompt-only generation, content-only editing,
fixed model/output settings, inherited endpoint/credentials and rejection of
overrides using mocked HTTP and file storage; no paid image call was made.
