#!/usr/bin/env bash
# Coordinator-only immutable client install. Never modify or delete an old tree.
# Stage (including journal preparation) must succeed before the entry point moves.
set -euo pipefail
REF=${1:-}
[[ $REF =~ ^[0-9a-f]{40}$ ]] || { echo 'Supply an immutable merged commit SHA.' >&2; exit 2; }
[[ ${SC_ROLLOUT_PROTOCOL_COMPATIBLE:-} == 1 ]] || {
  echo 'Refusing an unreviewed lease-protocol transition.' >&2; exit 2;
}
export PATH="$HOME/.local/bin:$PATH"
root="${XDG_DATA_HOME:-$HOME/.local/share}/supercomputer/installs"
bin="$HOME/.local/bin"
mkdir -p "$root" "$bin"
[[ ! -d "$bin/supercomputer" ]] || { echo "Refusing a directory as the client entry point." >&2; exit 2; }
# The final path exists from venv creation onward: Python/console-script shebangs
# contain it. A fresh path per attempt avoids repairing or replacing a live tree.
release=$(mktemp -d "$root/$REF.XXXXXXXX")
printf 'Preparing immutable install %s\n' "$release" >&2
uv venv --python 3.12 "$release"
env -u GITHUB_TOKEN uv pip install --python "$release/bin/python" \
  "git+https://github.com/monotykamary/supercomputer@$REF"
printf '%s\n' "$REF" > "$release/.installed-commit"
# Neither a caller's PYTHONPATH nor an old source override may select mixed code.
env -u PYTHONPATH -u SUPERCOMPUTER_SRC SUPERCOMPUTER_CONTROLLER=0 \
  "$release/bin/supercomputer" scheduler stage
# The old executable remains selected on any failure above. Rename a symlink on
# the same filesystem; never move the venv or unlink an old version's files.
switch=$(mktemp -d "$bin/.sc-switch.XXXXXXXX")
trap 'rm -f "$switch/supercomputer"; rmdir "$switch"' EXIT
ln -s "$release/bin/supercomputer" "$switch/supercomputer"
mv -f "$switch/supercomputer" "$bin/supercomputer"
printf 'Client entry point -> %s\n' "$release/bin/supercomputer" >&2
