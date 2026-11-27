// The batch rule's laws: proven from every state they allow (a batch is picked up from batch.json), checked by the
// kernel, nothing assumed; each bug they rule out refuted by a run from the start; the builders cross-checked.
import {expect, test} from 'bun:test'
import {crossCheckDecision, proveDecision, proveSteps} from 'gdp-wired'
import {decideIdle, idleLaws} from '../src/idle.ts'
import {boxSizeLaws} from '../src/size.ts'
import {decideLetGo, fitsLifetimeLaws, hasEnoughLifetime, LET_GO_BUGS, letGoLaws, letGoLawsOf, makeLetGo} from '../src/pool.ts'
import {batch, type Bug, BUGS, laws, lawsOf, makeBatch, stepLaws, stepLawsOf} from '../src/batch.ts'

test('every law of the batch is proven from every state the laws allow', () => {
  const v = proveSteps(batch, laws, stepLaws, {from: 'any'})
  expect({proven: v.proven, checked: v.checked, assumes: v.assumes, goal: v.goal}).toEqual({proven: true, checked: true, assumes: undefined, goal: undefined})
}, 600_000)

// Runs the search does not reach in its time (ten steps and more), written out: each replayed on the variant's own
// compiled machine from the start, then the law's breach read off the state it reaches.
const P = {cap: 2, maxAttempts: 3, strictOnce: false, timing: false, peers: false}
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
  // a job ends on box 0, its outputs still there, and the box closes
  'closes a box with outputs on it': () => {
    const {d} = replay('closes a box with outputs on it', [{op: 'add', job: 0, cpu: 1, mem: 1}, {op: 'open', box: 0, cpu: 4, mem: 100}, {op: 'start', job: 0, box: 0}, {op: 'exit', job: 0}, {op: 'close', box: 0}])
    const b = (d as unknown as {Boxes: Record<number, {open: boolean; unfetched: number}>}).Boxes[0]
    return b?.open === false && b.unfetched === 1
  },
  // a job answers 3, and the batch finishes passed
  'passes with a failure': () => {
    const {d} = replay('passes with a failure', [{op: 'add', job: 0, cpu: 1, mem: 1}, {op: 'open', box: 0, cpu: 4, mem: 100}, {op: 'start', job: 0, box: 0}, {op: 'exit', job: 0, code: 3}, {op: 'fetch', job: 0}, {op: 'finish'}])
    return (d as unknown as {verdict: string}).verdict === 'passed' && (d.Jobs[0] as unknown as {exit: number}).exit === 3
  },
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

for (const law of letGoLaws)
  test(`the warm pool's decision: ${law.name}`, () => {
    const v = proveDecision(law)
    expect({proven: v.proven, checked: v.checked}).toEqual({proven: true, checked: true})
    expect(crossCheckDecision(law).failure).toBeUndefined()
  })

for (const bug of LET_GO_BUGS)
  test(`refuted: warm pool decision that ${bug}`, () => {
    const broken = letGoLawsOf(makeLetGo(bug))
    const refuted = broken.some((law) => !proveDecision(law).proven)
    expect(refuted).toBe(true)
  })

test("the warm pool decision's compiled runner answers as its laws say", () => {
  const p = {keep: 1800, holdKeep: 21600}
  expect(decideLetGo({pausedFor: 1799, held: false, live: false, pulled: false}, p)).toBe('keep')
  expect(decideLetGo({pausedFor: 1800, held: false, live: false, pulled: false}, p)).toBe('delete')
  expect(decideLetGo({pausedFor: 99999, held: false, live: true, pulled: false}, p)).toBe('keep')
  expect(decideLetGo({pausedFor: 21599, held: true, live: false, pulled: false}, p)).toBe('keep')
  expect(decideLetGo({pausedFor: 21600, held: true, live: false, pulled: false}, p)).toBe('pull')
  expect(decideLetGo({pausedFor: 21600, held: true, live: false, pulled: true}, p)).toBe('delete')
  expect(decideLetGo({pausedFor: 21600, held: true, live: true, pulled: true}, p)).toBe('keep')
  expect(decideLetGo({pausedFor: 21600, held: true, live: true, pulled: false}, p)).toBe('keep')
})

for (const law of fitsLifetimeLaws)
  test(`the box lifetime decision: ${law.name}`, () => {
    const v = proveDecision(law)
    expect({proven: v.proven, checked: v.checked}).toEqual({proven: true, checked: true})
    expect(crossCheckDecision(law).failure).toBeUndefined()
  })

test("the box lifetime runner rejects boxes near delete-after and allows fresh or unbounded ones", () => {
  const now = Date.UTC(2026, 10, 27, 12, 0, 0)
  expect(hasEnoughLifetime(undefined, 3600, now, 600)).toBe(true)
  expect(hasEnoughLifetime(new Date(now + 4199_000).toISOString(), 3600, now, 600)).toBe(false)
  expect(hasEnoughLifetime(new Date(now + 4200_000).toISOString(), 3600, now, 600)).toBe(true)
  expect(hasEnoughLifetime(new Date(now - 10_000).toISOString(), 60, now, 600)).toBe(false)
})


for (const law of idleLaws)
  test(`the idle box's decision: ${law.name ?? 'a law'}`, () => {
    const v = proveDecision(law)
    expect({proven: v.proven, checked: v.checked}).toEqual({proven: true, checked: true})
  })

test("the idle decision's compiled runner answers as its laws say", () => {
  const base = {left: 3, others: 2, busy: 1, young: false, blocked: false, lowest: true}
  expect(decideIdle({...base, left: 0})).toBe('finish')
  expect(decideIdle({...base, others: 0})).toBe('hand')
  expect(decideIdle(base)).toBe('stay')
  expect(decideIdle({...base, blocked: true})).toBe('close')
  expect(decideIdle({...base, busy: 0, blocked: true})).toBe('stay') // (the others may never have come up)
  expect(decideIdle({...base, busy: 2})).toBe('close')
  expect(decideIdle({...base, busy: 2, young: true})).toBe('stay')
  expect(decideIdle({...base, lowest: false})).toBe('close')
})

test('with peers, the last box with work left is refused its close, and replaced instead', async () => {
  const {Engine} = await import('../src/engine.ts')
  const e = new Engine({maxBoxes: 1, maxAttempts: 3, strictOnce: false, timing: false, peers: true})
  expect(e.step({op: 'add', job: 0, cpu: 1, mem: 1, kind: 1})).toBe(true)
  expect(e.step({op: 'open', box: 0, cpu: 2, mem: 100, kind: 2})).toBe(true)
  expect(e.step({op: 'add', job: 1, cpu: 1, mem: 1, kind: 1})).toBe(false) // (no work added once a box is open)
  expect(e.step({op: 'close', box: 0})).toBe(false)
  expect(e.step({op: 'replace', box: 0, cpu: 2, mem: 100, kind: 1})).toBe(true)
  expect(e.step({op: 'start', job: 0, box: 0})).toBe(true)
  const lone = new Engine({maxBoxes: 1, maxAttempts: 3, strictOnce: false, timing: false})
  lone.step({op: 'add', job: 0, cpu: 1, mem: 1, kind: 1})
  lone.step({op: 'open', box: 0, cpu: 2, mem: 100, kind: 2})
  expect(lone.step({op: 'close', box: 0})).toBe(true) // (a client-run batch: the client is there to open another)
})

for (const law of boxSizeLaws)
  test(`the size a job's box gets: ${law.name ?? 'a law'}`, () => {
    const v = proveDecision(law)
    expect({proven: v.proven, checked: v.checked}).toEqual({proven: true, checked: true})
  })
