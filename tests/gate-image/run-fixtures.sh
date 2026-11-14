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
check_pipeline() {
  local status
  for status in "$@"; do
    if [[ $status != 0 ]]; then
      failed=1
    fi
  done
}
for fixture in test_runtime_pins test_strict_environment; do
  set +e
  python3 "tests/$fixture.py" -v 2>&1 | tee "$SUPERCOMPUTER_OUTPUT_DIR/$fixture.log"
  fixture_status=("${PIPESTATUS[@]}")
  set -e
  check_pipeline "${fixture_status[@]}"
  printf '%s pipeline exit codes: %s\n' "$fixture" "${fixture_status[*]}" >&2
  set +e
  printf '%s\t%s\n' "$fixture" "${fixture_status[0]}" | tee -a "$SUPERCOMPUTER_OUTPUT_DIR/status.tsv"
  record_status=("${PIPESTATUS[@]}")
  set -e
  check_pipeline "${record_status[@]}"
  if grep -Eq 'skipped=|\.\.\. skipped' "$SUPERCOMPUTER_OUTPUT_DIR/$fixture.log"; then
    failed=1
  else
    # grep status 1 means no skips; an unreadable/missing artifact is a failure.
    status=$?
    if [[ $status != 1 ]]; then failed=1; fi
  fi
done
exit "$failed"
