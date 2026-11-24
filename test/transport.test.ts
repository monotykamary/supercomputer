// instabox calls failing in transport (a timeout, a connection reset or refused, an EOF, the gateway's 502, a stalled
// or hung transfer): a call that is safe to make twice is waited out and made again, a create is made again under its
// idempotency key or reconciled by its name (never a second box, never one left behind), and an exec that may have
// run is made again only when its script is idempotent.
import {expect, test} from 'bun:test'
import {existsSync, mkdtempSync, readdirSync, readFileSync, writeFileSync} from 'node:fs'
import {tmpdir} from 'node:os'
import {join} from 'node:path'
import * as ib from '../src/instabox.ts'

const fake = (faults: Record<string, string[]> = {}, env: Record<string, string> = {}) => {
  const dir = mkdtempSync(join(tmpdir(), 'sc-fake-'))
  process.env.INSTABOX_BIN = `bun ${join(import.meta.dir, 'fake-instabox.ts')}`
  process.env.FAKE_INSTABOX_DIR = dir
  process.env.SC_REFUSAL_BACKOFF_MS = '5'
  delete process.env.SC_RETRY_BUDGET_MS
  delete process.env.SC_DEADLINE_MS
  delete process.env.FAKE_FORGET_KEYS
  Object.assign(process.env, env)
  writeFileSync(join(dir, 'faults.json'), JSON.stringify(faults))
  return dir
}
const left = (dir: string) => Object.values(JSON.parse(readFileSync(join(dir, 'faults.json'), 'utf8')) as Record<string, string[]>).flat()
const boxes = (dir: string) => readdirSync(dir).filter((f) => f.startsWith('m_') && existsSync(join(dir, f, 'box.json')))
const created = (dir: string) => readFileSync(join(dir, 'calls.log'), 'utf8').split('\n').filter((l) => l.startsWith('created')).length
const EVERY = ['timeout', 'reset', 'refused', 'eof', '502']

test('reads through every transport error: waited out, answered', async () => {
  const dir = fake({snapshot: EVERY, ls: EVERY, templates: ['reset', 'timeout']})
  writeFileSync(join(dir, 'snapshots.json'), JSON.stringify([{name: 's', defaultSpec: {vcpus: 4, memMiB: 8192}}]))
  expect((await ib.snapshots()).map((s) => s.name)).toEqual(['s'])
  expect(await ib.list({sc: '1'})).toEqual([])
  expect((await ib.gpuBox('l4')).template).toBe('gpu')
  expect(left(dir)).toEqual([])
})

test('a stalled push, and one that hangs past its deadline, are made again', async () => {
  const dir = fake({}, {SC_DEADLINE_MS: '1500'})
  const b = await ib.create({name: 'p1', vcpus: 2, memMiB: 4096, meta: {}})
  const file = join(dir, 'tree.tgz')
  writeFileSync(file, 'bytes')
  writeFileSync(join(dir, 'faults.json'), JSON.stringify({push: ['stall', 'hang', 'reset']}))
  await ib.push(b.id, file, '/data/home/sc/tree.tgz')
  expect(readFileSync(join(dir, b.id, 'home', 'sc', 'tree.tgz'), 'utf8')).toBe('bytes')
  expect(left(dir)).toEqual([])
}, 30_000)

test('a create that timed out after making its box: made again under its key, the same box, nothing more made', async () => {
  const dir = fake({create: ['reset', 'timeout-after', 'eof']})
  const b = await ib.create({name: 'c1', vcpus: 2, memMiB: 4096, meta: {sc: '1'}})
  expect(b.name).toBe('c1')
  expect([created(dir), boxes(dir).length]).toEqual([1, 1])
})

test('a create whose key instabox forgot: its box found by name and metadata, and used', async () => {
  const dir = fake({create: ['timeout-after']}, {FAKE_FORGET_KEYS: '1'})
  const b = await ib.create({name: 'c2', vcpus: 2, memMiB: 4096, meta: {sc: '1', sc_owner: 'o'}})
  expect(b).toMatchObject({name: 'c2', state: 'running'})
  expect([created(dir), boxes(dir).length]).toEqual([1, 1])
})

test('a timed-out create whose box then fails is deleted, not leaked, and the create raised', async () => {
  const dir = fake({create: ['broken-after']}, {FAKE_FORGET_KEYS: '1'})
  await expect(ib.create({name: 'c3', vcpus: 2, memMiB: 4096, meta: {sc: '1'}})).rejects.toThrow(/Client\.Timeout|already exists/)
  expect(created(dir)).toBe(1)
  expect(boxes(dir)).toEqual([])
})

test("a create that never answers: the box it made after all is used once it runs, deleted when it never comes up", async () => {
  const many = (first: string) => [first, ...Array(12).fill('timeout')]
  const up = fake({create: many('timeout-after')}, {SC_RETRY_BUDGET_MS: '30'})
  expect(await ib.create({name: 'c4', vcpus: 2, memMiB: 4096, meta: {sc: '1'}})).toMatchObject({name: 'c4', state: 'running'})
  expect(boxes(up)).toHaveLength(1)
  const never = fake({create: many('pending-after')}, {SC_RETRY_BUDGET_MS: '30'})
  await expect(ib.create({name: 'c4', vcpus: 2, memMiB: 4096, meta: {sc: '1'}})).rejects.toThrow(/Client\.Timeout/)
  expect(boxes(never)).toEqual([])
})

test('another owner\'s box of the name is never taken nor touched', async () => {
  const dir = fake()
  await ib.create({name: 'c5', vcpus: 2, memMiB: 4096, meta: {sc_owner: 'someone'}})
  await expect(ib.create({name: 'c5', vcpus: 2, memMiB: 4096, meta: {sc_owner: 'me'}})).rejects.toThrow(/already exists/)
  expect(boxes(dir)).toHaveLength(1)
})

test('an exec that may have run is not made again blindly: only an idempotent one is', async () => {
  const dir = fake()
  const b = await ib.create({name: 'e1', vcpus: 2, memMiB: 4096, meta: {}})
  const count = `echo x >> ${join(dir, 'ran')}; echo hi`
  writeFileSync(join(dir, 'faults.json'), JSON.stringify({exec: ['reset-after']}))
  await expect(ib.exec(b.id, count)).rejects.toThrow(/connection reset by peer/)
  expect(readFileSync(join(dir, 'ran'), 'utf8')).toBe('x\n') // (it ran once, and was not made again)
  writeFileSync(join(dir, 'faults.json'), JSON.stringify({exec: ['reset', 'timeout-after', 'eof']}))
  expect(await ib.exec(b.id, 'echo again', {idempotent: true})).toMatchObject({stdout: 'again\n', exitCode: 0})
  expect(left(dir)).toEqual([])
})

test('suspend, resume and rm made again after a transport failure; one that took effect is taken as made', async () => {
  const dir = fake()
  const b = await ib.create({name: 's1', vcpus: 2, memMiB: 4096, meta: {}})
  writeFileSync(join(dir, 'faults.json'), JSON.stringify({suspend: ['reset-after'], resume: ['timeout'], rm: ['timeout-after']}))
  await ib.suspend(b.id)
  expect((await ib.get(b.id)).state).toBe('suspended')
  await ib.resume(b.id)
  expect((await ib.get(b.id)).state).toBe('running')
  await ib.remove(b.id)
  expect(boxes(dir)).toEqual([])
})

test('what counts as a transport failure: the gate runs\' own errors, and never a command\'s answer', () => {
  const t = (stderr: string, stdout = '', code = 1) => ib.transportFailure({stdout, stderr, code})
  expect(t('Downloading your snapshot... instabox: error cli_error: Get "https://api.instabox.sh/v1/operations/op_a?wait=25s": context deadline exceeded (Client.Timeout exceeded while awaiting headers)')).toBe(true)
  expect(t('instabox: error cli_error: Get "https://api.instabox.sh/v1/snapshots": read tcp 1.2.3.4:5->6.7.8.9:443: read: connection reset by peer')).toBe(true)
  expect(t('', JSON.stringify({error: {message: "the connection to the API closed after 46s, before the command's result came (Post \"https://api.instabox.sh/v1/machines/m_a/exec\": read tcp 1.2.3.4:5->6.7.8.9:443: read: connection reset by peer)"}}))).toBe(true)
  expect(t('Post "https://api.instabox.sh/v1/machines/m_a/exec": read tcp 1.2.3.4:5->6.7.8.9:443: read: connection timed out')).toBe(true)
  expect(t('instabox: push /tmp/t.tgz: the upload stalled: no bytes went out for 2m; nothing was written in the sandbox')).toBe(true)
  expect(t('instabox: error http_503: 503 Service Unavailable')).toBe(true)
  expect(t('instabox: error not_found: no such machine')).toBe(false)
  expect(t('instabox: error spec_not_allowed: snapshot is size-locked')).toBe(false)
  // (an exec that answered, its command printing the words and failing: the command's answer, never retried)
  expect(t('', JSON.stringify({stdout: 'connection reset by peer', stderr: '', exitCode: 1}))).toBe(false)
  expect(ib.refusedForLoad({stdout: JSON.stringify({stdout: 'rate limit', stderr: '', exitCode: 1}), stderr: '', code: 1})).toBe(false)
})
