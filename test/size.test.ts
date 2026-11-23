// The size a job's box gets (src/size.ts), its compiled runner: a locked image's size always, a job it can't hold
// refused when the batch starts (with what it asks and the image's size), the other sizes as before 1.0.1.
import {expect, test} from 'bun:test'
import {mkdtempSync, writeFileSync} from 'node:fs'
import {tmpdir} from 'node:os'
import {join} from 'node:path'
import {readConfig, readJobs} from '../src/jobs.ts'
import {planSize, refuseUnfit} from '../src/size.ts'

const cfg = (toml = '') => {
  const dir = mkdtempSync(join(tmpdir(), 'sc-size-'))
  writeFileSync(join(dir, 'supercomputer.toml'), `name = "t"\n[instabox]\nsnapshot = "sc-t"\n${toml}`)
  return readConfig(dir)
}
const jobs = (list: unknown[]) => {
  const f = join(mkdtempSync(join(tmpdir(), 'sc-size-')), 'jobs.json')
  writeFileSync(f, JSON.stringify(list))
  return readJobs(f)
}
const locked = {'': {vcpus: 8, memMiB: 16384}}

test("a locked image's box is its own size: never a bigger one, never a smaller one", () => {
  const c = cfg('size = "small"')
  const [big, small] = jobs([{name: 'big', cmd: 'x', cpu: 8, memory: 15360}, {name: 'small', cmd: 'x', cpu: 1, memory: 512}])
  expect(planSize(big!, c, locked, false)).toEqual({vcpus: 8, memMiB: 16384, holds: true})
  expect(planSize(small!, c, locked, false)).toEqual({vcpus: 8, memMiB: 16384, holds: true})
})

test('a job its locked image cannot hold is refused when the batch starts, naming it, what it asks and the size', () => {
  const [j] = jobs([{name: 'check', cmd: 'x', cpu: 8, memory: 16384}]) // (16384 + the system's 512 > 16384)
  expect(planSize(j!, cfg(), locked, false).holds).toBe(false)
  expect(() => refuseUnfit([j!], cfg(), locked, false)).toThrow(/check: asks 8 vCPUs and 16384 MiB .* locked snapshot of 8 vCPUs \/ 16384 MiB/)
  const [k] = jobs([{name: 'wide', cmd: 'x', cpu: 12, memory: 1024}])
  expect(() => refuseUnfit([k!], cfg(), locked, false)).toThrow(/wide: asks 12 vCPUs/)
})

test('unlocked images keep the sizes they had: named, else the batch size when bigger, else the smallest that holds it', () => {
  const c = cfg('size = "large"')
  const [a, b, n] = jobs([{name: 'a', cmd: 'x', cpu: 1, memory: 512}, {name: 'b', cmd: 'x', cpu: 12, memory: 1024}, {name: 'n', cmd: 'x', cpu: 1, memory: 512, vm_type: 'small'}])
  expect(planSize(a!, c, {}, false)).toEqual({vcpus: 8, memMiB: 16384, holds: true}) // (the batch's size, bigger)
  expect(planSize(a!, c, {}, true)).toEqual({vcpus: 2, memMiB: 4096, holds: true}) // (--timing: the smallest)
  expect(planSize(b!, c, {}, false)).toEqual({vcpus: 16, memMiB: 32768, holds: true})
  expect(planSize(n!, c, {}, false)).toEqual({vcpus: 2, memMiB: 4096, holds: true})
  const [huge] = jobs([{name: 'huge', cmd: 'x', cpu: 64, memory: 1024}])
  expect(() => refuseUnfit([huge!], c, {}, false)).toThrow(/huge: .*no instabox size holds it/)
})

test("a named image's lock is its own; GPU jobs are not sized here", () => {
  const c = cfg('[instabox.images.chc]\nsnapshot = "sc-chc"\n')
  const [j, g] = jobs([{name: 'j', cmd: 'x', cpu: 2, memory: 2048, image: 'chc'}, {name: 'g', cmd: 'x', cpu: 64, memory: 1024, gpu: 'l4'}])
  expect(planSize(j!, c, {chc: {vcpus: 4, memMiB: 8192}}, false)).toEqual({vcpus: 4, memMiB: 8192, holds: true})
  expect(() => refuseUnfit([g!], c, {}, false)).not.toThrow()
})
