#!/usr/bin/env bash
# The real scheduler/client against simulated boat. Check the index in hooks;
# run all heavy work on Boat, including the Linux-only worker/namespace tests.
set -euo pipefail
here="$(cd "$(dirname "$0")/.." && pwd)"
py="${SC_PYTHON:-python3}"
[ -x "$py" ] || py=python3
if [ -n "${GIT_INDEX_FILE:-}" ] && [ -z "${CHECK_SCENARIOS_INDEX:-}" ]; then
  snap="$(mktemp -d)"
  trap 'rm -rf "$snap"' EXIT
  (cd "$here" && git checkout-index -a --prefix="$snap/")
  CHECK_SCENARIOS_INDEX=1 bash "$snap/tests/check_scenarios.sh" "$@"
  exit $?
fi
if [ -n "${CHECK_SCENARIOS_LOCAL:-}" ]; then
  for t in "$here"/tests/test_*.py; do
    nice -n 19 "$py" "$t"
  done
  [ -z "${CHECK_SCENARIOS_UNITS:-}" ] || exit 0
  exec nice -n 19 "$py" -P "$here/tests/scenarios.py" --code "$here" -j 1 "$@"
fi
# As with check_laws.sh, the installed client ships this candidate; it does not
# run the candidate scheduler against the live pool. A nonzero job is a failure
# even though `supercomputer run` itself exits successfully after collecting it.
out="$(mktemp -d)"
trap 'rm -rf "$out"' EXIT
printf '%s\n' '[{"name":"unit-tests","cmd":"OWNED_LIFECYCLE_BOAT_TEST=1 CHECK_SCENARIOS_LOCAL=1 CHECK_SCENARIOS_UNITS=1 SC_PYTHON=$(command -v python3) bash tests/check_scenarios.sh","timeout":1800,"cpu":2}]' > "$out/jobs.json"
img="${SC_LAWS_IMAGE:-$("$HOME/.local/bin/supercomputer" scheduler status 2>/dev/null | "$py" -I -c 'import json,sys; t=sys.stdin.read(); s=json.loads(t[t.index("{"):]); print(next((v["image"] for v in s.get("vms", [])), ""))' 2>/dev/null)}"
(cd "$here" && SUPERCOMPUTER_CONFIG="$here/tests/laws.toml" SUPERCOMPUTER_IMAGE="$img" \
  "$HOME/.local/bin/supercomputer" run --jobs "$out/jobs.json" --out "$out/run" --class gate --backend boat)
"$py" -I - "$out/run" <<'PY'
import pathlib, sys
p = pathlib.Path(sys.argv[1])
log = p / 'unit-tests.log'
if log.exists():
    print(log.read_text())
rows = (p / 'summary.tsv').read_text().splitlines()
if len(rows) != 1 or rows[0].split('\t')[1] != '0':
    raise SystemExit('unit gate failed on Boat')
PY
exec "$py" -P "$here/tests/scenarios.py" --code "$here" --boat "$@"
