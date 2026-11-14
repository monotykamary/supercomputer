# Scenario simulation contracts

`tests/scenarios.py` drives real scheduler/client code through a virtual clock.
`world.py` replaces provider CLI calls, agent transport, and scheduler subprocesses;
`BOAT_BIN` points at a nonexistent executable. Simulated VM creation never creates
real provider resources. Run the simulation *inside* a Boat job, not on the host.

## Fixture boundaries

- Named snapshots are independent of VM inventory. `Sim` seeds the default image
  in `World.images`; warming/creating a fixture VM records its image as well.
  Deleting all VMs does not delete a named snapshot. `cfg={"images": []}` can model
  a provider with no snapshots. Do not bypass `Scheduled._check_image`: a missing
  image is different from an empty pool. Alternate images used by a cold
  scenario must be declared in `cfg["images"]` before client preflight.
- `gate_68_slot_hours` tests gate-deadline sizing with fixed 90-second VM boot
  supply, keeping bootstrap RPC overhead, the workload, horizon and assertions.
  Its zero hindsight-unused-start assertion is not a universal claim under
  random future boot latency: the measured arrival estimate can legitimately
  buy a worker which later arrives after faster workers clear the tail. Keep
  default-jitter diagnostic failures as evidence rather than claiming that a
  fixed-supply pass proves a production cost improvement. Other growth/bringup
  scenarios continue to use the default variable boot times.
- A blocked-extra handoff fixture must first have a settled required pool. A
  request correctly deferred during bootstrap is not a committed handoff. Create
  the unoffered extra only after that prerequisite; continue blocking its execs
  before *and* after commit, so an early adoption still fails the assertion.
- Waste counts placeable idle slots, not bootstrap or dispatched-start latency.
  `_waste_watch` uses readiness and allocated starts but **not reservations** to
  determine availability. A ghost reservation on a ready idle VM must still be
  detected. `test_sim_world.py` includes this negative control.

## Bounded remote validation

Example jobs file (place it outside the uploaded repository):

```json
[
  {
    "name": "sim-contracts",
    "cmd": "python3 tests/test_sim_world.py",
    "cpu": 2,
    "timeout": 60
  },
  {
    "name": "handoff-extra",
    "cmd": "SIM_LOG=\"$SUPERCOMPUTER_OUTPUT_DIR/events\" python3 tests/scenarios.py handoff_blocked_extra --out \"$SUPERCOMPUTER_OUTPUT_DIR/scenario\"",
    "cpu": 2,
    "timeout": 600,
    "required_agent_capabilities": {
      "output_files_protocol": 1,
      "result_files_protocol": 1
    }
  }
]
```

Submit with `supercomputer run --jobs JOBS --out NEW_RECEIPT_DIRECTORY --class
submission --backend boat --strict-once --max-attempts 1`, under a finite process
wrapper timeout. Use one waiter, preserve failed receipts, and inspect per-job
exit codes **and** each `scenario/results.json` verdict: the batch command's exit
status alone does not establish a pass. Do not increase scenario watchdogs to
conceal blocked real operations.

`tests/sim/diagnose_gate.py` exports read-only planning snapshots to the job's
output directory. `SIM_VARIABLE_BOOT=1` replays the old variable-supply case for
comparison. `SIM_GATE_ECONOMIC_ONLY=1` installs a test-process-only mutant which
removes gate-deadline growth and requires the original horizon assertion to
fail. Its successful exit means **the negative control was detected**, not that
the mutated scheduler passed the scenario. Full verdicts remain in `result.json`.
