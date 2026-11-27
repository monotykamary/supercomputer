// The warm pool's decisions, as proven gdp-wired decisions (test/laws.test.ts); reap and the runner call their
// compiled runners directly.
//   - letGo: an unheld paused box is deleted once paused past `keep`; a held paused box whose batch has ended is
//     pulled past `holdKeep` and deleted only once its outputs are pulled; nothing a live batch can pick up is ever
//     pulled or deleted.
//   - fitsLifetime: a box with a server-side delete deadline is reused or given a job only while its remaining
//     lifetime covers the job's timeout plus a margin.
import {bool, fun, nat, no, yes} from 'gdp-wired'

export const DEFAULT_KEEP_S = 30 * 60
export const DEFAULT_HOLD_KEEP_S = 6 * 3600
export const LIFETIME_MARGIN_S = 10 * 60

export const LET_GO_BUGS = [
  'deletes a held box without pulling its outputs',
  'deletes a held box at normal keep',
  'deletes a box of a live batch',
  'pulls a box of a live batch',
] as const
export type LetGoBug = (typeof LET_GO_BUGS)[number]

export const makeLetGo = (bug?: LetGoBug) =>
  fun(bug ? `let a paused box go (${bug})` : 'let a paused box go')
    .params({keep: nat, holdKeep: nat})
    .inputs({pausedFor: nat, held: bool, live: bool, pulled: bool})
    .returns(({pausedFor, held, live, pulled, keep, holdKeep}) => {
      const pull = (bug === 'pulls a box of a live batch' ? yes : live.not())
        .and(held, pulled.not(), pausedFor.atLeast(holdKeep))
      const heldGo = held.and(
        bug === 'deletes a held box without pulling its outputs' ? yes : pulled,
        pausedFor.atLeast(bug === 'deletes a held box at normal keep' ? keep : holdKeep),
      )
      const go = (bug === 'deletes a box of a live batch' ? yes : live.not())
        .and(held.not().and(pausedFor.atLeast(keep)).or(heldGo))
      return {pull, go}
    })

export const letGo = makeLetGo()

export const letGoLawsOf = (f: ReturnType<typeof makeLetGo>) => [
  f.law('an unheld box is deleted only past its keep', (i, o) => [i.held.not(), o.go], (_, i) => i.pausedFor.atLeast(i.keep)),
  f.law('a held box is deleted only after its outputs are pulled and past hold-keep', (i, o) => [i.held, o.go], (_, i) => i.pulled.and(i.pausedFor.atLeast(i.holdKeep))),
  f.law('nothing a live batch can pick up is ever deleted', (i, o) => [i.live, o.go], () => no),
  f.law('nothing a live batch can pick up is ever pulled', (i, o) => [i.live, o.pull], () => no),
  f.law('a held box is pulled only when non-live, not yet pulled and past hold-keep', (_, o) => [o.pull], (_, i) => i.held.and(i.live.not(), i.pulled.not(), i.pausedFor.atLeast(i.holdKeep))),
  f.law('a box is never pulled and deleted at once', (_, o) => [o.pull, o.go], () => no),
  f.law('an unheld non-live box past its keep is deleted', (i) => [i.live.not(), i.held.not(), i.pausedFor.atLeast(i.keep)], (o) => o.go),
  f.law('a held non-live box past hold-keep with outputs pulled is deleted', (i) => [i.live.not(), i.held, i.pulled, i.pausedFor.atLeast(i.holdKeep)], (o) => o.go),
  f.law('a held non-live box past hold-keep without outputs pulled is pulled', (i) => [i.live.not(), i.held, i.pulled.not(), i.pausedFor.atLeast(i.holdKeep)], (o) => o.pull),
]

export const letGoLaws = letGoLawsOf(letGo)
export const letGoLaw = letGoLaws[0]!

const letGoRunner = letGo.compiled()
export type LetGoInput = {pausedFor: number; held: boolean; live: boolean; pulled: boolean}
export type LetGoAct = 'delete' | 'pull' | 'keep'

export function decideLetGo(i: LetGoInput, p: {keep: number; holdKeep: number}): LetGoAct {
  const o = letGoRunner(i, p) as {pull: boolean; go: boolean}
  return o.go ? 'delete' : o.pull ? 'pull' : 'keep'
}

export const deletes = (
  pausedFor: number,
  keep: number,
  o: {held?: boolean; live?: boolean; pulled?: boolean; holdKeep?: number} = {},
): boolean =>
  decideLetGo(
    {pausedFor, held: o.held ?? false, live: o.live ?? false, pulled: o.pulled ?? false},
    {keep, holdKeep: o.holdKeep ?? DEFAULT_HOLD_KEEP_S},
  ) === 'delete'

export const fitsLifetime = fun('a box has enough lifetime left for a job')
  .params({margin: nat})
  .inputs({remaining: nat, timeout: nat})
  .returns(({remaining, timeout, margin}) => ({ok: remaining.atLeast(timeout.plus(margin))}))

export const fitsLifetimeLaws = [
  fitsLifetime.law('a box is used only when its remaining lifetime covers the timeout plus margin', (_, o) => [o.ok], (_, i) => i.remaining.atLeast(i.timeout.plus(i.margin))),
  fitsLifetime.law('a box with enough remaining lifetime is allowed', (i) => [i.remaining.atLeast(i.timeout.plus(i.margin))], (o) => o.ok),
]

const lifetimeRunner = fitsLifetime.compiled()
export function hasEnoughLifetime(deleteAt: string | null | undefined, timeout: number, nowMs = Date.now(), margin = LIFETIME_MARGIN_S): boolean {
  if (!deleteAt) return true
  const at = Date.parse(deleteAt)
  if (!Number.isFinite(at)) return true
  const remaining = Math.max(0, Math.floor((at - nowMs) / 1000))
  return lifetimeRunner({remaining, timeout: Math.max(0, Math.ceil(timeout))}, {margin}).ok === true
}
