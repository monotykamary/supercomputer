// The warm pool's one decision, as a gdp-wired decision: a paused box is deleted once it has been paused past its
// keep. (A batch pauses a box only once the batch rule let it go: no job and no outputs on it.) Proven in
// test/laws.test.ts; reap runs it.
import {fun, nat} from 'gdp-wired'

export const letGo = fun('let a paused box go').params({keep: nat}).inputs({pausedFor: nat})
  .returns(({pausedFor, keep}) => ({go: pausedFor.atLeast(keep)}))

export const letGoLaw = letGo.law('a paused box is deleted only past its keep', (_, {go}) => go, (_, {pausedFor, keep}) => pausedFor.atLeast(keep))

const runner = letGo.compiled()
export const deletes = (pausedFor: number, keep: number): boolean => runner({pausedFor}, {keep}).go === true
