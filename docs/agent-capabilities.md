# Agent protocol requirements

A controller capability is not an agent capability. A current controller can own
legacy workers. Agent source hashes identify code; they do **not** imply protocol
support. Workers advertise an `agent_capabilities` map in their actual `ping`
response. Missing/malformed inventory means unknown (`{}`), never support.
Published status exposes the last observed map as `vms[].agent_capabilities`
and the admission fence as `vms[].agent_upgrading`; its snapshot age applies.

## One artifact-job recipe

Create `jobs.json`:

```json
[
  {
    "name": "proof",
    "cmd": "test -n \"$SUPERCOMPUTER_OUTPUT_DIR\" && mkdir -p \"$SUPERCOMPUTER_OUTPUT_DIR/proofs\" && printf 'proof artifact\\n' > \"$SUPERCOMPUTER_OUTPUT_DIR/proofs/proof.txt\"",
    "required_agent_capabilities": {
      "output_files_protocol": 1,
      "result_files_protocol": 1
    }
  }
]
```

Run with the normal native client:

```sh
supercomputer run --jobs jobs.json --out runs/proof --backend boat
```

The artifact is downloaded and SHA-256 verified at
`runs/proof/proof.files/proofs/proof.txt`. Use `result_files_protocol: 1` also
when relying on large Python return values/rows being offloaded to result files.
See [result-files.md](result-files.md) for transport and reference formats.

`required_agent_capabilities` must be an object mapping nonblank capability names
to positive **integer** minimum versions (not booleans, strings, zero or negative
numbers). Every entry is enforced: extra/unknown names are not silently discarded.
There are no implicit `output_files` or `result_files` job-file switches: declare
these requirements explicitly. The scheduler does not inspect shell text or guess
whether a command intends to write artifacts. Jobs with no field or an empty map
retain their existing placement and launch behavior.

## Placement and failure behavior

* Before tree upload/submission, the client requires the controller to advertise
  `required_agent_capabilities_protocol: 1`. An old controller is refused, rather
  than allowed to ignore a new job field.
* Placement and every fit-cache key include the complete requirement map. A worker
  must advertise every requested protocol at that version or higher. A VM being
  upgraded takes no new work, including ordinary legacy work.
* An empty legacy worker can upgrade first. Under the scheduler lock, the upgrade
  takes an exclusive fence only if there are no running, admitted/starting or
  queued-start jobs (and no foreign work). Network I/O is outside that lock; the
  fence remains until the attempt finishes and is released on failure too.
  Busy legacy workers keep running their existing jobs unchanged. A read-only
  ping does not fence placement: if admission or handoff wins during that probe,
  the second atomic check refuses the upgrade.
* Capability-requiring launches ping again before preparation **and immediately
  before launch**. Missing, insufficient, unreachable or changed agent inventory
  fails closed, before sending the payload. No Modal or legacy-client fallback
  is allowed for these jobs. Generic requirements that no available agent can
  advertise remain unplaceable; they never authorize a blind launch. The growth
  planner does not start new VMs when this build cannot supply a requested
  capability/version. Already-held agents with sufficient actual advertisements
  remain eligible; the build map is only a growth guard, never placement evidence.
* Native output support creates a private reserved `$SUPERCOMPUTER_OUTPUT_DIR`;
  user environment values cannot override it. Use the explicit declaration above
  to avoid silently landing on a worker that predates that contract.

## Receipts and recovery

The flat placement row (including `placement.json`/the persisted `.placed` row)
records actual `agent_version` and `agent_capabilities` alongside VM/image/type/CPU
identity **before** payload launch. Unknown legacy values stay empty; recovery
never fills old receipts with invented capabilities. Completion placement metadata
and handoff snapshots preserve the evidence, including the VM's last observed map.
A recovered starting job still re-verifies the live agent before launching.

A controller handoff with live capability-requiring jobs refuses any successor or
rollback target without the placement-enforcement protocol. Handoff waits for
in-progress fenced upgrades; an upgrade fence is not transferred as if it were a
settled worker.

## Regression checks

`tests/test_agent_capabilities.py` exercises version matching, validation,
old-controller preflight, old-worker refusal, idle upgrade/placement races,
prelaunch failure, private output-directory setup, receipts and handoff refusal.
`supercomputer/rules/agent_capabilities.py` contains production-used pure version
and upgrade-admission rules and their laws; `tests/broken_agent_capabilities.py`
retains the unsafe variants. Run the native gate through the normal supercomputer
workflow; do not upgrade or roll out live workers merely to test these changes.
