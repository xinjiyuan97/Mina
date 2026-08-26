# Rust Project Guide

Use this skill for tasks involving Rust crates, Cargo workspaces, compilation, tests, or dependency boundaries.

## Workflow

1. Inspect the relevant `Cargo.toml` files and module boundaries before proposing a change.
2. Preserve existing user changes and keep edits scoped to the requested crate or feature.
3. Prefer the smallest contract-preserving implementation that fits the current architecture.
4. Validate changes with formatting, targeted tests, and Clippy when appropriate.
5. Report which checks ran and distinguish verified facts from assumptions.

## Safety

- Treat `run_command` as an optional high-risk capability that still requires host approval.
- Do not read secret configuration values unless the user explicitly asks for them.
- Do not use destructive Git or filesystem operations to repair a build.
