// The batch rule as the runner drives it (src/engine.ts): a batch's life, a box lost mid-job, and batch.json's log
// replayed.
import {expect, test} from 'bun:test'
import {Engine} from '../src/engine.ts'

const limits = (o: Partial<{maxBoxes: number; maxAttempts: number; strictOnce: boolean; timing: boolean}> = {}) =>
  ({maxBoxes: 2, maxAttempts: 3, strictOnce: false, timing: false, ...o})

test('jobs pack onto a box by vCPUs and memory, end, are fetched, and the box closes; the batch passes', () => {
  const e = new Engine(limits())
  expect(e.step({op: 'add', job: 0, cpu: 4, mem: 4000, kind: 1}) && e.step({op: 'add', job: 1, cpu: 4, mem: 4000, kind: 1}) && e.step({op: 'add', job: 2, cpu: 1, mem: 100, kind: 1})).toBe(true)
  expect(e.step({op: 'open', box: 0, cpu: 8, mem: 15872, kind: 1})).toBe(true)
  expect(e.step({op: 'start', job: 0, box: 0}) && e.step({op: 'start', job: 1, box: 0})).toBe(true)
  expect(e.would({op: 'start', job: 2, box: 0})).toBe(false) // 8 of 8 vCPUs taken
  expect(e.step({op: 'close', box: 0})).toBe(false) // jobs on it
  expect(e.step({op: 'exit', job: 0, code: 0}) && e.step({op: 'exit', job: 1, code: 0})).toBe(true)
  expect(e.step({op: 'start', job: 2, box: 0})).toBe(true)
  expect(e.step({op: 'fetch', job: 0}) && e.step({op: 'fetch', job: 1})).toBe(true)
  expect(e.step({op: 'close', box: 0})).toBe(false) // job 2 still on it
  expect(e.step({op: 'exit', job: 2, code: 0}) && e.step({op: 'fetch', job: 2}) && e.step({op: 'close', box: 0})).toBe(true)
  expect(e.step({op: 'finish'})).toBe(true)
  expect(e.verdict).toBe('passed')
})

test("a box lost mid-job: its attempts go elsewhere; a native exit is the answer; a failure fails the batch", () => {
  const e = new Engine(limits({maxAttempts: 2}))
  e.step({op: 'add', job: 0, cpu: 1, mem: 1, kind: 1})
  e.step({op: 'add', job: 1, cpu: 1, mem: 1, kind: 1})
  e.step({op: 'open', box: 0, cpu: 2, mem: 3584, kind: 1})
  e.step({op: 'start', job: 0, box: 0})
  e.step({op: 'start', job: 1, box: 0})
  e.step({op: 'exit', job: 1, code: 3})
  expect(e.step({op: 'retry', job: 0})).toBe(true) // lost while running: queued again
  expect(e.step({op: 'retry', job: 1})).toBe(true) // its outputs gone with the box: queued again
  expect(e.step({op: 'close', box: 0})).toBe(true)
  expect(e.step({op: 'open', box: 1, cpu: 2, mem: 3584, kind: 1}) && e.step({op: 'start', job: 0, box: 1}) && e.step({op: 'exit', job: 0, code: 3}) && e.step({op: 'fetch', job: 0})).toBe(true)
  expect(e.step({op: 'retry', job: 0})).toBe(false) // answered: never again
  expect(e.step({op: 'start', job: 1, box: 1}) && e.step({op: 'retry', job: 1})).toBe(false) // second attempt of two: none left
  expect(e.step({op: 'drop', job: 1})).toBe(true)
  expect(e.step({op: 'close', box: 1}) && e.step({op: 'finish'})).toBe(true)
  expect([e.count('done'), e.count('lost'), e.verdict]).toEqual([1, 1, 'failed'])
})

test('kinds, groups, the cap, strict-once and timing', () => {
  const e = new Engine(limits({maxBoxes: 1, timing: true}))
  e.step({op: 'add', job: 0, cpu: 1, mem: 1, kind: 2, group: 1})
  e.step({op: 'add', job: 1, cpu: 1, mem: 1, kind: 1, group: 1})
  e.step({op: 'open', box: 0, cpu: 8, mem: 1000, kind: 1})
  expect(e.step({op: 'open', box: 1, cpu: 8, mem: 1000, kind: 1})).toBe(false) // the cap
  expect(e.would({op: 'start', job: 0, box: 0})).toBe(false) // another image
  expect(e.step({op: 'start', job: 1, box: 0})).toBe(true)
  e.step({op: 'exit', job: 1, code: 0})
  e.step({op: 'fetch', job: 1})
  expect(e.would({op: 'start', job: 1, box: 0})).toBe(false) // done
  const s = new Engine(limits({strictOnce: true, maxAttempts: 1}))
  s.step({op: 'add', job: 0, cpu: 1, mem: 1, kind: 1})
  s.step({op: 'open', box: 0, cpu: 2, mem: 100, kind: 1})
  s.step({op: 'start', job: 0, box: 0})
  expect(s.step({op: 'retry', job: 0})).toBe(false)
  expect(s.step({op: 'drop', job: 0})).toBe(true)
})

test("batch.json's log replays to the same batch; a log the rule refuses is refused", () => {
  const e = new Engine(limits())
  e.step({op: 'add', job: 0, cpu: 1, mem: 1, kind: 1})
  e.step({op: 'open', box: 0, cpu: 2, mem: 100, kind: 1})
  e.step({op: 'start', job: 0, box: 0})
  const again = new Engine(limits(), JSON.parse(JSON.stringify(e.log)))
  expect(again.jobs()).toEqual(e.jobs())
  expect(again.boxes()).toEqual(e.boxes())
  expect(() => new Engine(limits(), [{op: 'start', job: 0, box: 0}])).toThrow(/refuses/)
})
