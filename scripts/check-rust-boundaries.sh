#!/usr/bin/env bash
set -euo pipefail

mina_repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$mina_repo_root"

mina_forbidden_core_dependencies='^(agent-extension|agent-harness|axum|reqwest|rusqlite) '
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

mina_removed_runtime='ToolsPipelineRuntime|SqlitePipelineStore|EffectWaitBridge|ApprovalIngress|agent::pipeline|agent::tool_preparation|agent::tool_operator'
if rg -q "$mina_removed_runtime" crates/*/src apps/*/src; then
  printf '%s\n' 'a removed fixed single-agent runtime was reintroduced' >&2
  exit 1
fi

mina_removed_core_paths='agent_core::(harness|context_operator|event_runtime|operator_fabric)::|agent_core::agent::(context_operator|operator_fabric|run_runtime|runtime)::'
if rg -q "$mina_removed_core_paths" crates/*/src apps/*/src; then
  printf '%s\n' 'an obsolete Core public module path was reintroduced' >&2
  exit 1
fi
