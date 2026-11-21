import {expect, test} from 'bun:test'
import {mkdtempSync, writeFileSync} from 'node:fs'
import {tmpdir} from 'node:os'
import {join} from 'node:path'
import {fits, readConfig, readJobs, sizeOf} from '../src/jobs.ts'

const jobsFile = (jobs: unknown) => {
  const f = join(mkdtempSync(join(tmpdir(), 'sc-jobs-')), 'jobs.json')
  writeFileSync(f, JSON.stringify(jobs))
  return f
}

test("a job's defaults: the tree's root, one vCPU, 1 GiB, an hour", () => {
  const [j] = readJobs(jobsFile([{name: 'a', cmd: 'true'}]))
  expect(j).toMatchObject({cwd: '.', cpu: 1, memory: 1024, timeout: 3600, env: {}})
  expect(fits(j!)).toEqual({vcpus: 2, memMiB: 4096})
})

test('the smallest instabox size that holds the job, the system left its 512 MiB', () => {
  const size = (cpu: number, memory: number) => fits({...readJobs(jobsFile([{name: 'a', cmd: 'true'}]))[0]!, cpu, memory})
  expect(size(3, 1024)).toEqual({vcpus: 4, memMiB: 8192})
  expect(size(1, 4096)).toEqual({vcpus: 4, memMiB: 8192})
  expect(size(16, 30000)).toEqual({vcpus: 16, memMiB: 32768})
  expect(() => size(64, 1024)).toThrow(/no instabox size/)
})

test("boat's vm_type names and NxM sizes", () => {
  expect(sizeOf('large')).toEqual({vcpus: 8, memMiB: 16384})
  expect(sizeOf('8x16')).toEqual({vcpus: 8, memMiB: 16384})
  expect(sizeOf('xlarge')).toEqual({vcpus: 16, memMiB: 32768})
  expect(() => sizeOf('8x15')).toThrow(/unknown size/)
  const [j] = readJobs(jobsFile([{name: 'a', cmd: 'true', vm_type: 'xlarge'}]))
  expect(fits(j!)).toEqual({vcpus: 16, memMiB: 32768})
})

test("what instabox can't honour is refused, not ignored", () => {
  expect(() => readJobs(jobsFile([{name: 'a', cmd: 'true', vm_type: 'xlarge', cpu_model: 'AMD Ryzen 9 9950X 16-Core Processor'}]))).toThrow(/CPU model/)
  expect(() => readJobs(jobsFile([{name: 'a', cmd: 'true', backend: 'runpod'}]))).toThrow(/backend/)
  expect(() => readJobs(jobsFile([{name: 'a', cmd: 'true'}, {name: 'a', cmd: 'true'}]))).toThrow(/two jobs named a/)
  expect(() => readJobs(jobsFile([{name: 'a b', cmd: 'true'}]))).toThrow(/letters, digits/)
})

test('GPUs as instabox names them', () => {
  const [a, b] = readJobs(jobsFile([{name: 'a', cmd: 'nvidia-smi', gpu: 'a100'}, {name: 'b', cmd: 'nvidia-smi', gpu: {type: 'l4', count: 2}}]))
  expect(a!.gpu).toBe('a100')
  expect(b!.gpu).toBe('l4:2')
})

test("supercomputer.toml: [boat]'s tree, PATH and env carry over, [instabox] overrides", () => {
  const dir = mkdtempSync(join(tmpdir(), 'sc-cfg-'))
  writeFileSync(join(dir, 'supercomputer.toml'), [
    'name = "demo"',
    '[run]', 'pre = "export X=1; "',
    '[boat]', 'vm_type = "large"', 'path = ["/opt/node/bin"]', 'env = { A = "1" }', 'ignore = ["runs/**"]',
    '[instabox]', 'snapshot = "sc-demo-abc"', 'env = { B = "2" }', 'ignore = ["private/**"]',
  ].join('\n'))
  expect(readConfig(dir)).toEqual({
    name: 'demo', pre: 'export X=1; ', size: {vcpus: 8, memMiB: 16384}, path: ['/opt/node/bin'],
    env: {A: '1', B: '2'}, ignore: ['runs/**', 'private/**'], snapshot: 'sc-demo-abc', template: 'universal', setup: undefined,
  })
})
