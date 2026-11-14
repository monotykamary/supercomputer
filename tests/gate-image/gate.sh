#!/usr/bin/env bash
# Host entry point: orchestration only; all builds/tests execute on Boat.
set -euo pipefail
root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
if [[ -n ${SUPERCOMPUTER_IMAGE:-} || -n ${GDPW_BOAT_IMAGE:-} ]]; then
  echo 'Refusing an image override: this gate requires its dedicated image.' >&2
  exit 2
fi
export SUPERCOMPUTER_CONFIG="$root/tests/gate-image/supercomputer.toml"
case ${1:-} in
  build) exec supercomputer --repo "$root" image --build ;;
  run)
    # A fixture run must never trigger an implicit rebuild (including recovery
    # while an earlier provider snapshot is still saving).
    export SUPERCOMPUTER_NO_BUILD=1
    [[ $# == 2 ]] || { echo 'Usage: gate.sh run OUTPUT_DIRECTORY' >&2; exit 2; }
    exec supercomputer --repo "$root" run --jobs "$root/tests/gate-image/jobs.json" --out "$2" --backend boat --class gate --max-wait 600
    ;;
  *) echo 'Usage: gate.sh build | gate.sh run OUTPUT_DIRECTORY' >&2; exit 2 ;;
esac
