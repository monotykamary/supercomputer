// What a controller-less box does once nothing is left for it (src/peer.ts): a gdp-wired decision, proven
// (test/laws.test.ts), and its compiled runner is what the peer calls. Its inputs are read off the batch rule's state:
//   left     jobs not answered yet (queued, running, or ended with outputs still on a box)
//   others   other open boxes
//   busy     other boxes running a job or holding outputs
//   young    another open box was claimed lately (it may still be booting, or never come up)
//   blocked  the boxes are at the cap and queued work fits on none of them (its slot is needed for a box that can)
//   lowest   this is the lowest-numbered idle box
// It answers one act at most: finish the batch, hand its slot to a box for the work left (the rule's replace: the
// last box never just closes), or stay as the one watching the others; none of them is to close and go.

import {bool, fun, nat, no} from 'gdp-wired'

export const idle = fun('what a box with nothing left for it does')
  .inputs({left: nat, others: nat, busy: nat, young: bool, blocked: bool, lowest: bool})
  .returns(({left, others, busy, young, blocked, lowest}) => {
    const work = left.gt(0)
    return {
      finish: work.not(),
      hand: work.and(others.eq(0)),
      // (blocked gives the slot up only while another box is busy: with none busy, the others may be boxes that never
      // came up, and going would leave no box working)
      stay: work.and(others.gt(0), blocked.not().or(busy.eq(0)), busy.lt(2).or(young), lowest),
    }
  })

export const idleLaws = [
  idle.law('it finishes the batch only when nothing is left', (_, o) => [o.finish], (_, i) => i.left.eq(0)),
  idle.law('nothing left, it finishes the batch', (i) => [i.left.eq(0)], (o) => o.finish),
  idle.law('the last box hands the work left on, and never just goes', (i) => [i.left.gt(0), i.others.eq(0)], (o) => o.hand),
  idle.law('it never holds a slot that queued work needs at the cap while another box is busy', (i, o) => [i.blocked, i.busy.gt(0), o.stay], () => no),
  idle.law('while work is left and fewer than two others watch each other (or one may never come up), the lowest idle box stays',
    (i) => [i.left.gt(0), i.others.gt(0), i.blocked.not().or(i.busy.eq(0)), i.busy.lt(2).or(i.young), i.lowest], (o) => o.stay),
  idle.law('while work is left and no other box is busy, the lowest idle box never goes: it stays or hands its slot on',
    (i) => [i.left.gt(0), i.busy.eq(0), i.lowest], (o) => o.stay.or(o.hand)),
  idle.law('one act at most', () => [], (o) => o.finish.and(o.stay).not().and(o.finish.and(o.hand).not(), o.stay.and(o.hand).not())),
]

const runner = idle.compiled()
export type IdleAct = 'finish' | 'hand' | 'stay' | 'close'
export const decideIdle = (i: {left: number; others: number; busy: number; young: boolean; blocked: boolean; lowest: boolean}): IdleAct => {
  const o = runner(i, {}) as {finish: boolean; hand: boolean; stay: boolean}
  return o.finish ? 'finish' : o.hand ? 'hand' : o.stay ? 'stay' : 'close'
}
