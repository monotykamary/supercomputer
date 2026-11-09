#!/usr/bin/env bash
# supercomputer's decisions held to their laws (supercomputer/rules/*.py's `# gdp: law` comments), by gdp-wired's CLI:
# every law proven and checked (the rule read from the code, checked against it by running it; from every state the
# laws allow, gdp.toml's from = "any"), none unknown, and every law of tests/broken_*.py broken, with the run that breaks
# it (the bugs they rule out, kept as regressions). Exit 0 only then.
#   tests/check_laws.sh                      # npx gdp-wired-cli@0.43.0 (pinned: the release with sets, Literal aliases, from any state)
#   GDP_WIRED_CLI="bun <your gdp-wired checkout>/core/cli.ts" tests/check_laws.sh
set -euo pipefail
here="$(cd "$(dirname "$0")/.." && pwd)"
# (in a git hook, what is being committed is checked: the index, written out whole, not the working tree around it)
if [ -n "${GIT_INDEX_FILE:-}" ] && [ -z "${CHECK_LAWS_INDEX:-}" ]; then
  snap="$(mktemp -d)"
  (cd "$here" && git checkout-index -a --prefix="$snap/")
  CHECK_LAWS_INDEX=1 bash "$snap/tests/check_laws.sh" "$@"; rc=$?
  rm -rf "$snap"
  exit $rc
fi
read -r -a cli <<< "${GDP_WIRED_CLI:-npx -y gdp-wired-cli@0.43.0}"
out="$(mktemp -d)"
trap 'rm -rf "$out"' EXIT
# (every module of supercomputer/rules/, every file of broken variants tests/broken_*.py; each one CLI run, at once,
# on boat: an interactive batch through the scheduler (the laptop runs only light work: three local CLI runs held its
# load at 100 for 3 minutes a commit, 11-01 07:02); CHECK_LAWS_LOCAL=1 runs them here instead, one at a time, niced)
good_files=($(ls "$here"/supercomputer/rules/*.py | grep -v "/__init__.py$")) broken_files=("$here"/tests/broken_*.py)
if [ -z "${CHECK_LAWS_LOCAL:-}" ]; then
  py="${SC_PYTHON:-python3}"
  "$py" -I - "$here" "$out" "${cli[*]}" "${good_files[@]}" -- "${broken_files[@]}" <<'PY'
import json, pathlib, sys
here, out, cli = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2]), sys.argv[3].replace("npx -y ", "bunx ")
args = sys.argv[4:]
k = args.index("--")
jobs = []
for kind, files, ms in (("good", args[:k], ""), ("broken", args[k + 1:], " --ms 90000")):
    for f in files:
        rel = pathlib.Path(f).relative_to(here)
        jobs.append({"name": f"{kind}.{rel.name}", "cmd": f"{cli} check {rel} --root . --no-propose --json{ms} 2>/dev/null", "timeout": 1200, "cpu": 2})
(out / "jobs.json").write_text(json.dumps(jobs))
PY
  img="${SC_LAWS_IMAGE:-$("$HOME/.local/bin/supercomputer" scheduler status 2>/dev/null | "$py" -I -c 'import json,sys; t=sys.stdin.read(); s=json.loads(t[t.index("{"):]); print(next((v["image"] for v in s.get("vms", [])), ""))' 2>/dev/null)}"
  (cd "$here" && SUPERCOMPUTER_CONFIG="$here/tests/laws.toml" SUPERCOMPUTER_IMAGE="$img" \
     "$HOME/.local/bin/supercomputer" run --jobs "$out/jobs.json" --out "$out/run" --class interactive --backend boat >&2) || true
  for j in "$out"/run/*.log; do
    [ -f "$j" ] || continue
    n="$(basename "$j" .log)"
    sed '/^--- stderr ---$/,$d' "$j" > "$out/$n.json"
  done
else
  for f in "${good_files[@]}"; do nice -n 19 "${cli[@]}" check "$f" --root "$here" --no-propose --json > "$out/good.$(basename "$f").json"; done
  for f in "${broken_files[@]}"; do nice -n 19 "${cli[@]}" check "$f" --root "$here" --no-propose --ms 90000 --json > "$out/broken.$(basename "$f").json"; done
fi
python3 -I - "$out" <<'PY'
import glob, json, os, sys
def rules(f):
    try:
        d = json.load(open(f))
    except Exception as e:
        return [{"entry": os.path.basename(f), "ok": False, "refusals": [f"no output: {e}"], "laws": []}]
    return d if isinstance(d, list) else d.get("rules", [])
bad = []
good = [r for f in sorted(glob.glob(os.path.join(sys.argv[1], "good.*.json"))) for r in rules(f)]
broken = [r for f in sorted(glob.glob(os.path.join(sys.argv[1], "broken.*.json"))) for r in rules(f)]
for want, rs in (("proven", good), ("broken", broken)):
    if not rs: bad.append(f"no rule read ({want})")
    for r in rs:
        if not r["ok"]: bad.append(f"{r.get('entry')}: refused: {r['refusals'][:1]}")
        if not r["laws"]: bad.append(f"{r.get('entry')}: no law")
        for l in r["laws"]:
            ok = l["verdict"] == want and (want == "proven" or l.get("run"))
            print(f"  {'ok ' if ok else 'BAD'} {r.get('entry')}: {l['name']}: {l['verdict']}")
            if not ok: bad.append(f"{r.get('entry')}: {l['name']}: {l['verdict']} (want {want}) {l.get('why', '')}")
n = sum(len(r["laws"]) for r in good + broken)
if bad:
    print("\n".join(["laws: FAILED"] + bad)); sys.exit(1)
print(f"laws: {sum(len(r['laws']) for r in good)} proven, {sum(len(r['laws']) for r in broken)} broken variants refuted ({n} of {n})")
PY
