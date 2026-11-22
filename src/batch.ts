// A batch as a gdp-wired rule: its jobs and boxes, and every step the runner may take. The runner (src/run.ts) steps
// this rule's compiled machine and acts only on what the step accepted; there is no other copy of these decisions.
// The laws below are proven from every state they allow (test/laws.test.ts), since a batch is picked up from
// <out>/batch.json, and test/broken.test.ts keeps the bugs they rule out, each refuted by a run.
//
// A job is none (not in the batch), queued, running (on a box), ended (its exit known, its log and outputs still on
// the box), done (fetched and checked) or lost (no attempt left without a native answer). Only a lost attempt is
// tried again: a native exit, zero or not, is the job's answer. A box is a slot of the batch, open while the batch
// holds it; it holds the jobs it runs and the outputs of those that ended until they are fetched.
//
// The runner's names map onto naturals: a job is its index in the jobs file, a box its slot, a group and a box's
// kind (image, GPU, named size) small numbers the runner assigns (0: none).

import {bool, forall, nat, oneOf, record, rule, when, yes} from 'gdp-wired'

// Groups per batch: a (box, group) pair is the key box * GROUPS + group.
export const GROUPS = 4096

export const Op = oneOf('add', 'open', 'start', 'exit', 'fetch', 'retry', 'drop', 'close', 'finish')
export const Status = oneOf('none', 'queued', 'running', 'ended', 'done', 'lost')
export const Job = record({st: Status, attempts: nat, cpu: nat, mem: nat, box: nat, exit: nat, group: nat, need: nat})
export const Box = record({open: bool, vcpus: nat, mem: nat, cpu: nat, memUsed: nat, running: nat, unfetched: nat, starts: nat, kind: nat})

// The bugs the laws rule out, each a variant of the same rule (test/broken.test.ts refutes every one by a run).
export const BUGS = ['opens past the cap', 'packs by vCPUs alone', 'retries an answer', 'tries strict-once work twice',
  'closes a box with outputs on it', 'puts a group twice on a box', 'passes with a failure'] as const
export type Bug = (typeof BUGS)[number]

export const makeBatch = (bug?: Bug) => rule(bug ? `batch (${bug})` : 'batch')
  .params({cap: nat, maxAttempts: nat, strictOnce: bool, timing: bool})
  .inputs({op: Op, job: nat, box: nat, cpu: nat, mem: nat, code: nat, group: nat, kind: nat})
  .state({live: 0, verdict: oneOf('open', 'passed', 'failed').from('open'), ok: bool.from(false)})
  .maps({
    Jobs: Job.from({st: 'none', attempts: 0, cpu: 0, mem: 0, box: 0, exit: 0, group: 0, need: 0}),
    Boxes: Box.from({open: false, vcpus: 0, mem: 0, cpu: 0, memUsed: 0, running: 0, unfetched: 0, starts: 0, kind: 0}),
    Groups: 0,
  })
  .assume(({group}) => group.lt(GROUPS))
  .flags(({op, job, box, group, Jobs, Boxes, Groups, live, verdict, cap, maxAttempts, strictOnce, timing}) => {
    const j = Jobs.at(job)
    const b = Boxes.at(box)
    const on = Boxes.at(j.box) // the box the job is on
    const holds = bug === 'retries an answer' ? j.st.is('running').or(j.st.is('ended'), j.st.is('done')) : j.st.is('running').or(j.st.is('ended'))
    const attemptLeft = bug === 'tries strict-once work twice' ? j.attempts.lt(maxAttempts) : j.attempts.lt(maxAttempts).and(strictOnce.not().or(j.attempts.eq(0)))
    const open = verdict.is('open')
    return {
      adds: op.is('add').and(open, j.st.is('none'), group.lt(GROUPS)),
      opens: op.is('open').and(open, b.open.not(), bug === 'opens past the cap' ? yes : live.lt(cap)),
      starts: op.is('start').and(
        j.st.is('queued'),
        b.open,
        b.cpu.plus(j.cpu).atMost(b.vcpus),
        bug === 'packs by vCPUs alone' ? yes : b.memUsed.plus(j.mem).atMost(b.mem),
        attemptLeft,
        timing.not().or(b.starts.eq(0)),
        j.need.eq(0).or(j.need.eq(b.kind)),
        bug === 'puts a group twice on a box' ? yes : j.group.eq(0).or(Groups.at(box.times(GROUPS).plus(j.group)).eq(0)),
      ),
      exits: op.is('exit').and(j.st.is('running'), on.open, on.running.atLeast(1), on.cpu.atLeast(j.cpu), on.memUsed.atLeast(j.mem)),
      fetches: op.is('fetch').and(j.st.is('ended'), on.open, on.unfetched.atLeast(1)),
      retries: op.is('retry').and(holds, on.open, j.attempts.lt(maxAttempts), bug === 'tries strict-once work twice' ? yes : strictOnce.not(),
        j.st.is('running').not().or(on.running.atLeast(1).and(on.cpu.atLeast(j.cpu), on.memUsed.atLeast(j.mem))),
        j.st.is('ended').not().or(on.unfetched.atLeast(1))),
      drops: op.is('drop').and(holds, on.open, j.attempts.lt(maxAttempts).and(strictOnce.not()).not(),
        j.st.is('ended').or(on.running.atLeast(1).and(on.cpu.atLeast(j.cpu), on.memUsed.atLeast(j.mem))),
        j.st.is('running').or(on.unfetched.atLeast(1))),
      closes: op.is('close').and(b.open, b.running.eq(0), bug === 'closes a box with outputs on it' ? yes : b.unfetched.eq(0)),
    }
  })
  .step(({op, job, box, cpu, mem, code, group, kind, Jobs, Boxes, Groups, live, verdict, adds, opens, starts, exits, fetches, retries, drops, closes}) => {
    const j = Jobs.at(job)
    const leaves = retries.or(drops) // an attempt ends without an answer
    const wasRunning = j.st.is('running')
    // The one box this step touches: the request's (open, start, close) or the job's (exit, fetch, retry, drop).
    const target = when(op.is('open').or(op.is('start'), op.is('close')), box, j.box)
    const b = Boxes.at(target)
    const freed = exits.or(leaves.and(wasRunning)) // the job's vCPUs and memory come back
    const answered = fetches.or(leaves.and(wasRunning.not())) // its outputs leave the box
    const gkey = target.times(GROUPS).plus(j.group)
    const g = Groups.at(gkey)
    const finishing = op.is('finish').and(verdict.is('open'),
      Jobs.sum((c) => when(c.st.is('queued').or(c.st.is('running'), c.st.is('ended')), 1, 0)).eq(0))
    const failed = Jobs.sum((c) => when(c.st.is('done').and(c.exit.gt(0)).or(c.st.is('lost')), 1, 0))
    return {
      ok: adds.or(opens, starts, exits, fetches, retries, drops, closes, finishing),
      live: when(opens, live.plus(1), when(closes, live.minus(1), live)),
      verdict: when(finishing.and(bug === 'passes with a failure' ? yes : failed.eq(0)), 'passed', when(finishing, 'failed', verdict)),
      Jobs: Jobs.set(job, {
        st: when(adds, 'queued', when(starts, 'running', when(exits, 'ended', when(fetches, 'done', when(retries, 'queued', when(drops, 'lost', j.st)))))),
        attempts: when(starts, j.attempts.plus(1), j.attempts),
        cpu: when(adds, cpu, j.cpu),
        mem: when(adds, mem, j.mem),
        box: when(starts, box, j.box),
        exit: when(exits, code, j.exit),
        group: when(adds, group, j.group),
        need: when(adds, kind, j.need),
      }),
      Boxes: Boxes.set(target, {
        open: opens.or(closes.not().and(b.open)),
        vcpus: when(opens, cpu, b.vcpus),
        mem: when(opens, mem, b.mem),
        cpu: when(opens, 0, when(starts, b.cpu.plus(j.cpu), when(freed, b.cpu.minus(j.cpu), b.cpu))),
        memUsed: when(opens, 0, when(starts, b.memUsed.plus(j.mem), when(freed, b.memUsed.minus(j.mem), b.memUsed))),
        running: when(opens, 0, when(starts, b.running.plus(1), when(freed, b.running.minus(1), b.running))),
        unfetched: when(opens, 0, when(exits, b.unfetched.plus(1), when(answered, b.unfetched.minus(1), b.unfetched))),
        starts: when(opens, 0, when(starts, b.starts.plus(1), b.starts)),
        kind: when(opens, kind, b.kind),
      }),
      Groups: Groups.set(gkey, when(j.group.eq(0), g, when(starts, g.plus(1), when(fetches.or(leaves), g.minus(1), g)))),
    }
  })

export const batch = makeBatch()
export type Batch = ReturnType<typeof makeBatch>

export const lawsOf = (batch: Batch) => [
  batch.law('live boxes stay within the cap', ({live, cap}) => live.atMost(cap)),
  batch.law('live counts the open boxes', ({live, Boxes}) => live.eq(Boxes.sum((b) => when(b.open, 1, 0)))),
  batch.law('no box runs past its vCPUs', ({Boxes}) => forall((k) => Boxes.at(k).cpu.atMost(Boxes.at(k).vcpus))),
  batch.law('no box runs past its memory', ({Boxes}) => forall((k) => Boxes.at(k).memUsed.atMost(Boxes.at(k).mem))),
  batch.law('attempts stay within their budget', ({Jobs, maxAttempts}) => forall((k) => Jobs.at(k).attempts.atMost(maxAttempts))),
  batch.law('strict-once work is tried at most once', ({Jobs, strictOnce}) => forall((k) => strictOnce.implies(Jobs.at(k).attempts.atMost(1)))),
  batch.law('a timing box runs one job in its life', ({Boxes, timing}) => forall((k) => timing.implies(Boxes.at(k).starts.atMost(1)))),
  batch.law("a group's members run on different boxes", ({Groups}) => forall((k) => Groups.at(k).atMost(1))),
  batch.law('a closed box holds nothing', ({Boxes}) => forall((k) => Boxes.at(k).open.or(Boxes.at(k).running.eq(0).and(Boxes.at(k).unfetched.eq(0))))),
  batch.law('the batch passes only with nothing left to run or fetch', ({verdict, Jobs}) =>
    verdict.is('passed').implies(Jobs.sum((c) => when(c.st.is('queued').or(c.st.is('running'), c.st.is('ended')), 1, 0)).eq(0))),
  batch.law('the batch passes only with no job failed or lost', ({verdict, Jobs}) =>
    verdict.is('passed').implies(Jobs.sum((c) => when(c.st.is('done').and(c.exit.gt(0)).or(c.st.is('lost')), 1, 0)).eq(0))),
]

export const stepLawsOf = (batch: Batch) => [
  batch.stepLaw('an answer is never taken back', ({Jobs}, after) =>
    forall((k) => Jobs.at(k).st.is('done').implies(after.Jobs.at(k).st.is('done').and(after.Jobs.at(k).exit.eq(Jobs.at(k).exit))))),
  batch.stepLaw('a lost job stays lost', ({Jobs}, after) => forall((k) => Jobs.at(k).st.is('lost').implies(after.Jobs.at(k).st.is('lost')))),
  batch.stepLaw('a job is tried again only with an attempt left', ({Jobs, maxAttempts, strictOnce}, after) =>
    forall((k) => Jobs.at(k).st.is('queued').not().and(after.Jobs.at(k).st.is('queued'), Jobs.at(k).st.is('none').not())
      .implies(Jobs.at(k).attempts.lt(maxAttempts).and(strictOnce.not())))),
  batch.stepLaw('a verdict is final', ({verdict}, after) => verdict.is('open').or(after.verdict.eq(verdict))),
]

export const laws = lawsOf(batch)
export const stepLaws = stepLawsOf(batch)
