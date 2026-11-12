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
here="$(cd "$(dirname "$0")" && pwd)"
SC="$HOME/.local/bin/supercomputer"
# Stage reports installed + active hash. pending=true is expected. Preparation
# can take minutes while the parent serves; the freeze starts only afterwards.
echo '== boat sandbox: installed / active / pending'
bash "$here/INSTALL_VERSION.sh" "$REF"

# Send this reviewed helper as a file, not a remote checkout/reinstall of a tree
# that running clients may still import. Base64 is only transport, not a secret.
installer_b64=$(python3 -c 'import base64,pathlib,sys; print(base64.b64encode(pathlib.Path(sys.argv[1]).read_bytes()).decode())' "$here/INSTALL_VERSION.sh")
remote_install="set -e; f=\$(mktemp); trap 'rm -f \"\$f\"' EXIT; printf '%s' '$installer_b64' | python3 -c 'import base64,sys; sys.stdout.buffer.write(base64.b64decode(sys.stdin.buffer.read()))' > \"\$f\"; SC_ROLLOUT_PROTOCOL_COMPATIBLE=1 bash \"\$f\" '$REF'"
# `here` is the Mac running Reflex (currently the mini), not this sandbox.
# Override only for different registered Reflex aliases; do not use `home`
# as a permanent alias for the Air.
read -r -a machines <<< "${SC_ROLLOUT_MACHINES:-here air anh-1}"
staged=1
skipped=()
for machine in "${machines[@]}"; do
  echo "== $machine: checking Reflex reachability"
  if ! reflex on "$machine" run --timeout 15 'true'; then
    echo "SKIP $machine: unreachable through Reflex; continuing to the controller." >&2
    skipped+=("$machine")
    continue
  fi
  echo "== $machine: installed / active / pending"
  if reflex on "$machine" run --timeout 600 "$remote_install"; then
    staged=$((staged + 1))
  else
    rc=$?
    if ! reflex on "$machine" run --timeout 15 'true'; then
      echo "SKIP $machine: lost Reflex connection during staging; install state unknown; continuing to the controller." >&2
      skipped+=("$machine")
      continue
    fi
    echo "ERROR $machine: reachable, but installation failed (exit $rc); refusing to hide an install error." >&2
    exit "$rc"
  fi
done
# Ship code and future-start descriptor limits to the existing controller. No setup (pool transfer), service
# restart, treadmill restart, drain, or agent checkpoint is part of an update.
echo '== controller bx_gz4zdtbh: installed / active / pending'
env -u PYTHONPATH -u SUPERCOMPUTER_SRC SUPERCOMPUTER_CONTROLLER=1 "$SC" controller stage
staged=$((staged + 1))
printf 'Staged on %s reachable machines; running agents finish their current items before upgrading.\n' "$staged"
if ((${#skipped[@]})); then
  printf 'Skipped unreachable Reflex machines: %s\n' "${skipped[*]}"
fi
