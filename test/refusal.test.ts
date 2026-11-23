// instabox refusing a call for load (too many execs at once across the account): the call ran nothing, so it is made
// again a little later, never an attempt lost for it; any other error is raised at once.
import {expect, test} from 'bun:test'
import {mkdtempSync, readFileSync, writeFileSync} from 'node:fs'
import {tmpdir} from 'node:os'
import {join} from 'node:path'
import * as ib from '../src/instabox.ts'

const fake = () => {
  const dir = mkdtempSync(join(tmpdir(), 'sc-fake-'))
  process.env.INSTABOX_BIN = `bun ${join(import.meta.dir, 'fake-instabox.ts')}`
  process.env.FAKE_INSTABOX_DIR = dir
  process.env.SC_REFUSAL_BACKOFF_MS = '5'
  return dir
}

test('an exec refused for load is made again, and answers', async () => {
  const dir = fake()
  const b = await ib.create({name: 'r1', vcpus: 2, memMiB: 4096, meta: {}})
  writeFileSync(join(dir, 'refuse-execs'), '4')
  const r = await ib.exec(b.id, 'echo hi')
  expect(r).toMatchObject({stdout: 'hi\n', exitCode: 0})
  expect(readFileSync(join(dir, 'refuse-execs'), 'utf8')).toBe('0')
})

test('any other error is raised at once', async () => {
  fake()
  await expect(ib.exec('no-such-box', 'true')).rejects.toThrow(/no such machine/)
})

test('what counts as refused for load', () => {
  expect(ib.refusedForLoad({stdout: '{"error":{"message":"too many concurrent execs (at most 256 per user)"}}', stderr: '', code: 5})).toBe(true)
  expect(ib.refusedForLoad({stdout: '', stderr: 'HTTP 429 Too Many Requests', code: 1})).toBe(true)
  expect(ib.refusedForLoad({stdout: '{"error":{"message":"no such machine"}}', stderr: '', code: 1})).toBe(false)
  expect(ib.refusedForLoad({stdout: 'rate limit is a phrase in a log', stderr: '', code: 0})).toBe(false)
})
