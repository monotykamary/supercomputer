// The batch rule running: src/batch.ts's compiled machine, stepped by the runner for every decision. A request the
// rule accepts (its `ok`) is kept in the log; <out>/batch.json holds that log, and picking a batch up replays it from
// the start, so a state read back is one the rule reached (and the laws are proven from every state they allow too).

import {batch, GROUPS, type Op} from './batch.ts'

export type Limits = {maxBoxes: number; maxAttempts: number; strictOnce: boolean; timing: boolean}
type OpName = typeof Op extends {values: readonly (infer V)[]} ? V : string
export type Req = {op: OpName; job?: number; box?: number; cpu?: number; mem?: number; code?: number; group?: number; kind?: number}
export type JobView = {st: 'none' | 'queued' | 'running' | 'ended' | 'done' | 'lost'; attempts: number; cpu: number; mem: number; box: number; exit: number; group: number; need: number}
export type BoxView = {open: boolean; vcpus: number; mem: number; cpu: number; memUsed: number; running: number; unfetched: number; starts: number; kind: number}
type View = {live: number; verdict: 'open' | 'passed' | 'failed'; ok: boolean; Jobs: Record<string, JobView>; Boxes: Record<string, BoxView>}

const machine = batch.compiled()
const blank = {op: 'add', job: 0, box: 0, cpu: 0, mem: 0, code: 0, group: 0, kind: 0} as const

export class Engine {
  readonly log: Req[] = []
  private readonly params: Record<string, number>
  private readonly values: Record<string, unknown>
  private st: ReturnType<typeof machine.init>
  private view?: View

  constructor(readonly limits: Limits, log: Req[] = []) {
    this.values = {cap: limits.maxBoxes, maxAttempts: limits.maxAttempts, strictOnce: limits.strictOnce, timing: limits.timing}
    this.params = batch.encodeParams(this.values as never)
    this.st = machine.init(this.params)
    for (const [i, r] of log.entries()) if (!this.step(r)) throw new Error(`batch.json's log: request ${i} (${r.op}) is one the batch rule refuses`)
  }

  private input(r: Req) {
    if ((r.group ?? 0) >= GROUPS) throw new Error(`at most ${GROUPS - 1} groups`)
    return batch.encodeInput({...blank, ...r} as never, this.values as never)
  }

  // Take the request: true when the rule accepted it (and the state moved), false when it refused (nothing moved).
  step(r: Req): boolean {
    const next = machine.step(this.params, machine.fork(this.st), this.input(r))
    if (next.scalars.ok !== 1) return false
    this.st = next
    this.view = undefined
    this.log.push(r)
    return true
  }

  // Whether the rule would accept it now, the state left as it is.
  would(r: Req): boolean {
    return machine.step(this.params, machine.fork(this.st), this.input(r)).scalars.ok === 1
  }

  private read(): View {
    return (this.view ??= batch.decode(this.st) as unknown as View)
  }
  job(j: number): JobView | undefined {
    return this.read().Jobs[j]
  }
  box(b: number): BoxView | undefined {
    return this.read().Boxes[b]
  }
  jobs(): [number, JobView][] {
    return Object.entries(this.read().Jobs).map(([k, v]) => [Number(k), v])
  }
  boxes(): [number, BoxView][] {
    return Object.entries(this.read().Boxes).filter(([, b]) => b.open).map(([k, v]) => [Number(k), v])
  }
  get verdict() {
    return this.read().verdict
  }
  count(st: JobView['st']): number {
    return this.jobs().filter(([, j]) => j.st === st).length
  }
}
