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

test('supercomputer.toml: [instabox] for the boxes, [run] pre, images and caches', () => {
  const dir = mkdtempSync(join(tmpdir(), 'sc-cfg-'))
  writeFileSync(join(dir, 'supercomputer.toml'), [
    'name = "demo"',
    '[run]', 'pre = "export X=1; "',
    '[instabox]', 'size = "large"', 'snapshot = "sc-demo-abc"', 'path = ["/opt/node/bin"]', 'env = { A = "1" }', 'ignore = ["runs/**"]',
    '[instabox.images.runner]', 'path = ["/usr/lib/llvm-18/bin"]', 'env = { B = "2" }',
    '[instabox.images.chc]', 'setup = "sc/chc.sh"', 'data = { "/bench" = "data/bench", "/data/m.jsonl" = "runs/m.jsonl" }',
    '[[cache]]', 'name = "olean"', 'dir = "lean"', 'modules = ["A", "B"]', 'inputs = "{m}.lean"', 'outputs = "{m}.olean"', 'salt = "v1"',
  ].join('\n'))
  expect(readConfig(dir)).toEqual({
    name: 'demo', pre: 'export X=1; ', size: {vcpus: 8, memMiB: 16384}, setup: undefined, ignore: ['runs/**'],
    image: {snapshot: 'sc-demo-abc', template: 'universal', path: ['/opt/node/bin'], env: {A: '1'}, setup: undefined, data: {}},
    images: {
      runner: {snapshot: 'sc-demo-abc', template: 'universal', ownTemplate: false, path: ['/usr/lib/llvm-18/bin', '/opt/node/bin'], env: {A: '1', B: '2'}, setup: undefined, data: {}},
      // (an image with its own setup is its own snapshot: none until `image build chc`, never the repo's)
      chc: {snapshot: undefined, template: 'universal', ownTemplate: false, path: ['/opt/node/bin'], env: {A: '1'}, setup: 'sc/chc.sh', data: {'/bench': 'data/bench', '/data/m.jsonl': 'runs/m.jsonl'}},
    },
    caches: [{name: 'olean', dir: 'lean', inputs: ['A.lean', 'B.lean'], outputs: ['A.olean', 'B.olean'], salt: 'v1'}],
  })
})

test("an image's data: absolute box paths, and a setup to build it", () => {
  const cfg = (body: string) => {
    const dir = mkdtempSync(join(tmpdir(), 'sc-cfg-'))
    writeFileSync(join(dir, 'supercomputer.toml'), body)
    return () => readConfig(dir)
  }
  expect(cfg('[instabox.images.x]\nsetup = "s.sh"\ndata = { "bench" = "b" }')).toThrow(/absolute/)
  expect(cfg('[instabox.images.x]\nsetup = "s.sh"\ndata = { "/a/../etc" = "b" }')).toThrow(/absolute/)
  expect(cfg('[instabox.images.x]\ndata = { "/bench" = "b" }')).toThrow(/no setup/)
  expect(cfg('[instabox.images.Big]\nsetup = "s.sh"')).toThrow(/lowercase/)
  expect(cfg('[instabox.images.x]\nsetup = "s.sh"\ndata = { "/" = "b" }')).toThrow(/not "\/" itself/)
  expect(cfg('[instabox.images.x]\nsetup = "s.sh"\ndata = { "/bench/" = "b" }')().images.x!.data).toEqual({'/bench': 'b'})
})

test("the old supercomputer's sections are refused, not ignored", () => {
  const dir = mkdtempSync(join(tmpdir(), 'sc-cfg-'))
  writeFileSync(join(dir, 'supercomputer.toml'), '[boat]\nvm_type = "large"\n')
  expect(() => readConfig(dir)).toThrow(/\[boat\] is the old supercomputer's/)
})

test('a job names an image of supercomputer.toml, or the default', () => {
  const [a, b] = readJobs(jobsFile([{name: 'a', cmd: 'true', image: 'runner'}, {name: 'b', cmd: 'true', image: 'default'}]))
  expect(a!.image).toBe('runner')
  expect(b!.image).toBeUndefined()
})
