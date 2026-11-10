#!/usr/bin/env bash
# Coordinator only: publish reviewed code, never checkpoint/stop/restart work.
# Prerequisite: all active schedulers understand hot_handoff=1 and the same VM
# lease protocol. This does NOT waive the first lease-protocol migration barrier.
# Usage: SC_ROLLOUT_PROTOCOL_COMPATIBLE=1 bash ROLLOUT.sh <merged-40-char-SHA>
# This lane never invokes this script (remote writes require explicit permission).
set -euo pipefail
REF=${1:-}
[[ $REF =~ ^[0-9a-f]{40}$ ]] || { echo 'Supply an immutable merged commit SHA.' >&2; exit 2; }
[[ ${SC_ROLLOUT_PROTOCOL_COMPATIBLE:-} == 1 ]] || {
  echo 'Refusing an unreviewed lease-protocol transition. Hot reload preserves identity; it cannot translate an incompatible protocol.' >&2
  exit 2
}
URL="git+https://github.com/monotykamary/supercomputer@$REF"
SC="$HOME/.local/bin/supercomputer"
# Stage reports installed + active hash. pending=true is expected: the active
# scheduler adopts it after its 60-second stable-code interval without a pause.
env -u GITHUB_TOKEN uv tool install --force --reinstall "$URL"
echo '== boat sandbox: installed / active / pending'
SUPERCOMPUTER_CONTROLLER=0 "$SC" scheduler stage

echo '== MacBook Air: installed / active / pending'
reflex home run --timeout 300 "set -e; cd ~/.local/share/supercomputer; git fetch origin '$REF'; git checkout --detach '$REF'; SUPERCOMPUTER_CONTROLLER=0 ~/.local/bin/supercomputer scheduler stage"
for machine in mini anh-1; do
  echo "== $machine: installed / active / pending"
  reflex on "$machine" run --timeout 300 "set -e; export PATH=\$HOME/.local/bin:\$PATH; uv tool install --force --reinstall '$URL'; SUPERCOMPUTER_CONTROLLER=0 supercomputer scheduler stage"
done
# Only ship code to the existing controller. No setup (pool transfer), service
# restart, treadmill restart, drain, or agent checkpoint is part of an update.
echo '== controller bx_gz4zdtbh: installed / active / pending'
SUPERCOMPUTER_CONTROLLER=1 "$SC" controller stage
printf '%s\n' 'Published to five machines; running agents finish their current items before upgrading.'
