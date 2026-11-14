#!/usr/bin/env bash
# Run only inside the dedicated Boat job; retain both files' independent results.
set -euo pipefail
: "${SUPERCOMPUTER_OUTPUT_DIR:?Boat output directory required}"
[[ ${OWNED_LIFECYCLE_BOAT_TEST:-} == 1 ]]
[[ $HOME == /home/user ]]
[[ $(command -v bun) == "$HOME/.bun/bin/bun" ]]
[[ $(command -v lean) == "$HOME/.elan/toolchains/leanprover--lean4---v4.34.0/bin/lean" ]]
[[ $(bun --version) == 1.4.3 ]]
lean --version | grep -E '^Lean \(version 4\.34\.0,'
sha256sum --check --strict "$HOME/.local/share/sc-gate-image/executables.sha256"
cp "$HOME/.local/share/sc-gate-image/"* "$SUPERCOMPUTER_OUTPUT_DIR/"
{
  printf 'HOME=%s\nPATH=%s\n' "$HOME" "$PATH"
  uname -a
  file "$HOME/.bun/bin/bun" "$(command -v lean)"
  lean --print-prefix
  sha256sum "$HOME/.elan/toolchains/leanprover--lean4---v4.34.0/lib/lean/Init.olean"
} > "$SUPERCOMPUTER_OUTPUT_DIR/environment.txt"
failed=0
for fixture in test_runtime_pins test_strict_environment; do
  set +e
  python3 "tests/$fixture.py" -v 2>&1 | tee "$SUPERCOMPUTER_OUTPUT_DIR/$fixture.log"
  status=${PIPESTATUS[0]}
  set -e
  printf '%s\t%s\n' "$fixture" "$status" | tee -a "$SUPERCOMPUTER_OUTPUT_DIR/status.tsv"
  if [[ $status != 0 ]] || grep -Eq 'skipped=|\.\.\. skipped' "$SUPERCOMPUTER_OUTPUT_DIR/$fixture.log"; then
    failed=1
  fi
done
exit "$failed"
