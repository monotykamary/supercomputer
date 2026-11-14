#!/usr/bin/env bash
# Executed ONLY by supercomputer's native Boat image builder, never on the host.
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
[[ $(uname -m) == x86_64 ]]
[[ $HOME == /home/user ]]
sudo -n apt-get update -y -q
sudo -n apt-get install -y -q --no-install-recommends ca-certificates curl unzip zstd file python3 util-linux build-essential git
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
cd "$work"
curl --fail --location --retry 3 --max-time 600 -o bun.zip https://github.com/oven-sh/bun/releases/download/bun-v1.4.3/bun-linux-x64-baseline.zip
curl --fail --location --retry 3 --max-time 1200 -o lean.tar.zst https://github.com/leanprover/lean4/releases/download/v4.34.0/lean-4.34.0-linux.tar.zst
printf '%s\n' \
 '1fc2edac843102909e3a1be1d8d9802cc6071cf074e67e88231f4ffe0f8b397b  bun.zip' \
 'caaa98356098c85dc0fcbbd28e1ec66f39eb6551829972b752ff20e1286b646b  lean.tar.zst' | sha256sum --check --strict
unzip -q bun.zip
mkdir -p "$HOME/.bun/bin" "$HOME/.elan/toolchains" "$HOME/.elan/bin"
install -m 755 bun-linux-x64-baseline/bun "$HOME/.bun/bin/bun"
tar --zstd -xf lean.tar.zst
mv lean-4.34.0-linux "$HOME/.elan/toolchains/leanprover--lean4---v4.34.0"
leanbin="$HOME/.elan/toolchains/leanprover--lean4---v4.34.0/bin"
# Direct aliases to real distribution binaries, not Elan proxy executables.
for tool in lean lake leanc leanmake; do
  if [[ -f "$leanbin/$tool" ]]; then ln -sfn "../toolchains/leanprover--lean4---v4.34.0/bin/$tool" "$HOME/.elan/bin/$tool"; fi
done
export PATH="$HOME/.bun/bin:$leanbin:$HOME/.elan/bin:$PATH"
[[ $(bun --version) == 1.4.3 ]]
lean --version | grep -E '^Lean \(version 4\.34\.0,'
[[ $(lean --print-prefix) == "$HOME/.elan/toolchains/leanprover--lean4---v4.34.0" ]]
file "$HOME/.bun/bin/bun" "$leanbin/lean" | grep ELF
# Exercise real runtime/compiler functionality and installed Lean library files.
[[ $(bun -e 'console.log(6 * 7)') == 42 ]]
printf '#check Nat\nexample : 1 + 1 = 2 := rfl\n' > Smoke.lean
lean Smoke.lean
[[ -f "$leanbin/../lib/lean/Init.olean" ]]
receipt="$HOME/.local/share/sc-gate-image"
mkdir -p "$receipt"
sha256sum "$HOME/.bun/bin/bun" "$leanbin/lean" > "$receipt/executables.sha256"
printf '%s\n' \
 'bun-v1.4.3/bun-linux-x64-baseline.zip sha256:1fc2edac843102909e3a1be1d8d9802cc6071cf074e67e88231f4ffe0f8b397b' \
 'v4.34.0/lean-4.34.0-linux.tar.zst sha256:caaa98356098c85dc0fcbbd28e1ec66f39eb6551829972b752ff20e1286b646b' > "$receipt/archives.txt"
bun --version > "$receipt/bun.version"
lean --version > "$receipt/lean.version"
cat "$receipt/archives.txt" "$receipt/executables.sha256" "$receipt/bun.version" "$receipt/lean.version"
