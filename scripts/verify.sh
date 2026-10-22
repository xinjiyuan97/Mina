#!/usr/bin/env bash
# 提交前本地闸门：fmt → 构建（含 agent-chat）→ 测试 → agent-chat/1 帧形状校验
# 用法: bash scripts/verify.sh            （全跑）
#       SKIP_FRAMES=1 bash scripts/verify.sh   （跳过 node 帧校验）
set -euo pipefail

mina_repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$mina_repo_root"

export PATH="/root/.cargo/bin:${PATH}"

step() { printf '\n== %s ==\n' "$1"; }

step 'cargo fmt --all -- --check'
cargo fmt --all -- --check

step 'cargo build -p agent-core -p agent-extension --features agent-chat'
cargo build -p agent-core -p agent-extension --features agent-chat

step 'cargo build -p agent-extension --example frame_shapes --features agent-chat'
cargo build -p agent-extension --example frame_shapes --features agent-chat

step 'cargo test -p agent-core -p agent-extension --features agent-chat'
cargo test -p agent-core -p agent-extension --features agent-chat

if [ "${SKIP_FRAMES:-0}" != "1" ]; then
  step 'node scripts/verify-frame-shapes.mjs'
  node scripts/verify-frame-shapes.mjs
fi

printf '\n本地闸门全部通过\n'
