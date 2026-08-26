#!/usr/bin/env bash
set -euo pipefail

mina_repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$mina_repo_root"

mina_forbidden_core_dependencies='^(agent-extension|axum|reqwest|rusqlite|tracing) '
if cargo tree -p agent-core --edges normal --prefix none \
    | rg -q "$mina_forbidden_core_dependencies"; then
  printf '%s\n' 'agent-core contains an external implementation dependency' >&2
  exit 1
fi

mina_retired_packages='(agent-tools|context-contract|context-engine|context-strategies|memory-contract|memory-runtime|memory-store|mina-harness|observability|observability-contract|openai-compatible|process-sandbox|run-store|skill-contract|skill-runtime|skill-store|tool-contract)\.workspace'
if rg -q "$mina_retired_packages" Cargo.toml crates/core/Cargo.toml crates/extension/Cargo.toml; then
  printf '%s\n' 'a retired internal package dependency was reintroduced' >&2
  exit 1
fi
