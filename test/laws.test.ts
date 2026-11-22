// The batch rule's laws: proven from every state they allow (a batch is picked up from batch.json), checked by the
// kernel, nothing assumed; each bug they rule out refuted by a run from the start; the builders cross-checked.
import {expect, test} from 'bun:test'
import {proveDecision, proveSteps} from 'gdp-wired'
import {letGoLaw} from '../src/pool.ts'
import {batch, type Bug, BUGS, laws, lawsOf, makeBatch, stepLaws, stepLawsOf} from '../src/batch.ts'

test('every law of the batch is proven from every state the laws allow', () => {
  const v = proveSteps(batch, laws, stepLaws, {from: 'any'})
  expect({proven: v.proven, checked: v.checked, assumes: v.assumes, goal: v.goal}).toEqual({proven: true, checked: true, assumes: undefined, goal: undefined})
}, 600_000)

// Runs the search does not reach in its time (ten steps and more), written out: each replayed on the variant's own
// compiled machine from the start, then the law's breach read off the state it reaches.
const P = {cap: 2, maxAttempts: 3, strictOnce: false, timing: false}
const replay = (bug: Bug, reqs: Record<string, unknown>[]) => {
  const r = makeBatch(bug), m = r.compiled(), params = r.encodeParams(P)
  let st = m.init(params)
  for (const q of reqs) st = m.step(params, st, r.encodeInput({op: 'add', job: 0, box: 0, cpu: 0, mem: 0, code: 0, group: 0, kind: 0, ...q} as never, P as never))
  return {st, d: r.decode(st) as unknown as {Jobs: Record<number, {st: string}>}}
}
const twoOnABox = (group: number) => [{op: 'add', job: 0, cpu: 1, mem: 1, group}, {op: 'add', job: 1, cpu: 1, mem: 1, group}, {op: 'open', box: 0, cpu: 4, mem: 100}, {op: 'start', job: 0, box: 0}, {op: 'start', job: 1, box: 0}]
const written: Partial<Record<Bug, () => boolean>> = {
  // two jobs end on one box, one is fetched (its answer), then retried as if lost
  'retries an answer': () => {
    const before = replay('retries an answer', [...twoOnABox(0), {op: 'exit', job: 0}, {op: 'exit', job: 1}, {op: 'fetch', job: 0}])
    const after = replay('retries an answer', [...twoOnABox(0), {op: 'exit', job: 0}, {op: 'exit', job: 1}, {op: 'fetch', job: 0}, {op: 'retry', job: 0}])
    return before.d.Jobs[0]?.st === 'done' && after.d.Jobs[0]?.st === 'queued'
  },
  // two members of group 1 started on box 0
  'puts a group twice on a box': () => (replay('puts a group twice on a box', twoOnABox(1)).st.maps.Groups?.get(1) ?? 0) > 1,
}

for (const bug of BUGS)
  test(`refuted by a run: the batch that ${bug}`, () => {
    const r = makeBatch(bug)
    const v = proveSteps(r, lawsOf(r), stepLawsOf(r))
    expect(v.proven).toBe(false)
    if (written[bug]) expect(written[bug]!()).toBe(true)
    else expect(v.run?.trace.length ?? 0).toBeGreaterThan(0) // the search's own run from the start
  }, 600_000)

test("the builders' callbacks agree with the proven expressions on concrete runs", () => {
  const c = batch.crossCheck(laws, stepLaws) as {checked: number}
  expect(c.checked).toBeGreaterThan(0)
}, 600_000)

test("the warm pool's decision: a paused box is deleted only past its keep", () => {
  const v = proveDecision(letGoLaw)
  expect({proven: v.proven, checked: v.checked}).toEqual({proven: true, checked: true})
})

