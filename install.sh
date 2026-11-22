#!/usr/bin/env bash
# Install this checkout's commit as the `supercomputer` command: its tree (git archive, so only what is committed)
# into ~/.local/share/supercomputer/installs/<sha>, then ~/.local/bin/supercomputer pointed at it. An install is
# never changed once made; installing another commit moves only the command.
set -euo pipefail
cd "$(dirname "$0")"
sha=$(git rev-parse HEAD)
[[ -z $(git status --porcelain -- src SKILL.md package.json bun.lock) ]] || { echo "commit src/, SKILL.md, package.json and bun.lock first" >&2; exit 2; }
command -v bun >/dev/null || { echo "supercomputer needs bun (https://bun.sh)" >&2; exit 2; }
root="${XDG_DATA_HOME:-$HOME/.local/share}/supercomputer/installs"
dest="$root/$sha"
if [[ ! -d $dest ]]; then
  mkdir -p "$root"
  tmp=$(mktemp -d "$root/.$sha.XXXXXX")
  git archive "$sha" src SKILL.md package.json bun.lock | tar -x -C "$tmp"
  # (the batch rule runs on gdp-wired: its runtime dependency, pinned by bun.lock)
  (cd "$tmp" && bun install --frozen-lockfile --production >/dev/null) || { rm -rf "$tmp"; echo "bun install failed" >&2; exit 1; }
  mv "$tmp" "$dest"
fi
mkdir -p "$HOME/.local/bin"
link="$HOME/.local/bin/supercomputer"
printf '#!/bin/sh\nexec bun %q "$@"\n' "$dest/src/cli.ts" > "$link.tmp"
chmod +x "$link.tmp"
mv "$link.tmp" "$link"
echo "supercomputer $sha installed: $link"
