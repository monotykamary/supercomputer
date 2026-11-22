// The ledger as the batch's one log: of two appends at the same version exactly one wins, and the loser, asking the
// rule again on what won, is refused what the winner took (one job on two boxes at once, a box past the cap).
import {expect, test} from 'bun:test'
import {mkdtempSync} from 'node:fs'
import {tmpdir} from 'node:os'
import {join} from 'node:path'
import {Engine} from '../src/engine.ts'
import {openLedger, type State} from '../src/ledger.ts'
import {claimBox, propose, type Shared} from '../src/peer.ts'

const limits = {maxBoxes: 2, maxAttempts: 3, strictOnce: false, timing: false}
const job = (name: string) => ({name, cmd: 'true', cwd: '.', cpu: 1, memory: 512, timeout: 60, env: {}})
const shared = (n: number): Shared => ({
  id: 'b', owner: 'o', tree: 't', caches: [], limits, groups: [], kinds: ['||'],
  jobs: Array.from({length: n}, (_, i) => job(`j${i}`)) as never,
  cfg: {ignore: [], pre: '', image: {template: 'ci', path: [], env: {}, data: {}}, images: {}, caches: [], size: {vcpus: 2, memMiB: 4096}} as never,
})
async function fresh(n: number) {
  const e = new Engine(limits)
  for (let i = 0; i < n; i++) e.step({op: 'add', job: i, cpu: 1, mem: 512, group: 0, kind: 1})
  const l = openLedger(`dir:${mkdtempSync(join(tmpdir(), 'sc-ledger-'))}#b`)
  await l.create({'batch.json': '{}'}, {log: e.log, boxes: {}, times: {}})
  return l
}

test('of two appends at one version, one wins', async () => {
  const l = await fresh(1)
  const {head, state} = await l.read()
  const [a, b] = await Promise.all([l.append(head, state), l.append(head, state)])
  expect([a, b].filter(Boolean).length).toBe(1)
})

test('two boxes racing for one job: one starts it, the other is refused by the rule on reading again', async () => {
  const l = await fresh(1)
  const s = shared(1)
  let r = await l.read()
  // (two boxes open: slots 0 and 1)
  expect(await propose(l, r.head, s, r.state, [{op: 'open', box: 0, cpu: 2, mem: 3584, kind: 1}, {op: 'open', box: 1, cpu: 2, mem: 3584, kind: 1}])).toHaveLength(2)
  r = await l.read()
  const [a, b] = await Promise.all([0, 1].map((box) => propose(l, r.head, s, r.state, [{op: 'start', job: 0, box}])))
  expect([a, b].filter((x) => x === undefined)).toHaveLength(1) // the loser lost the race
  const again = await l.read()
  const loser = a === undefined ? 0 : 1
  expect(await propose(l, again.head, s, again.state, [{op: 'start', job: 0, box: loser}])).toEqual([]) // and the rule refuses it now
  expect(new Engine(limits, again.state.log).job(0)!.box).toBe(1 - loser)
})

test('boxes for queued work: packed onto open room first, never past the cap', async () => {
  const l = await fresh(5) // 5 one-vCPU jobs, 2-vCPU boxes, at most 2 boxes
  const s = shared(5)
  const slots: number[] = []
  for (;;) {
    const r = await l.read()
    const c = await claimBox(l, r.head, s, r.state)
    if (!c) break
    slots.push(c.slot)
  }
  expect(slots).toEqual([0, 1])
  const st = (await l.read()).state as State
  expect(Object.keys(st.boxes)).toEqual(['0', '1'])
  expect(new Engine(limits, st.log).boxes().length).toBe(2)
})
