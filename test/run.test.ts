// The runner end to end on the stand-in instabox (test/fake-instabox.ts): a batch's answers and outputs, a box lost
// mid-job, a batch picked up after its client died, the warm pool, and the Lean-style cache. Linux only (setsid).
import {expect, test} from 'bun:test'
import {existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, writeFileSync} from 'node:fs'
import {tmpdir} from 'node:os'
import {join} from 'node:path'
import {Engine} from '../src/engine.ts'
import {owner} from '../src/run.ts'

const here = join(import.meta.dir, '..')
const linux = process.platform === 'linux'

function repo(files: Record<string, string>, toml = '') {
  const dir = mkdtempSync(join(tmpdir(), 'sc-repo-'))
  for (const [f, body] of Object.entries(files)) {
    mkdirSync(join(dir, f, '..'), {recursive: true})
    writeFileSync(join(dir, f), body)
  }
  writeFileSync(join(dir, 'supercomputer.toml'), `name = "t"\n[instabox]\ntemplate = "universal"\n${toml}`)
  Bun.spawnSync(['git', 'init', '-q'], {cwd: dir})
  Bun.spawnSync(['git', 'add', '-A'], {cwd: dir})
  const fake = mkdtempSync(join(tmpdir(), 'sc-fake-'))
  return {dir, fake, out: join(dir, 'runs', 'x')}
}

function sc(r: {dir: string; fake: string}, args: string[], env: Record<string, string> = {}) {
  return Bun.spawn(['bun', join(here, 'src/cli.ts'), ...args], {
    cwd: r.dir,
    stdout: 'pipe',
    stderr: 'pipe',
    env: {...process.env, INSTABOX_BIN: `bun ${join(here, 'test/fake-instabox.ts')}`, FAKE_INSTABOX_DIR: r.fake, SC_STATE_DIR: join(r.fake, 'state'), SC_REAPER_RECORD: join(r.fake, 'reaper-spawns.jsonl'), ...env},
  })
}

async function done(p: ReturnType<typeof sc>) {
  const [err, code] = await Promise.all([new Response(p.stderr).text(), p.exited])
  return {err, code}
}

const jobs = (r: {dir: string}, list: unknown[]) => {
  writeFileSync(join(r.dir, 'jobs.json'), JSON.stringify(list))
  return 'jobs.json'
}
const summary = (out: string) => readFileSync(join(out, 'summary.tsv'), 'utf8').trim().split('\n').slice(1).map((l) => l.split('\t').slice(0, 2))
const boxes = (fake: string) => readdirSync(fake).filter((f) => f.startsWith('m_')).map((id) => JSON.parse(readFileSync(join(fake, id, 'box.json'), 'utf8')))

test.skipIf(!linux)('a batch: answers, outputs fetched and checked, failures not retried, boxes paused', async () => {
  const r = repo({'src/a.txt': 'hello'})
  const f = jobs(r, [
    {name: 'ok', cmd: 'cat src/a.txt > "$SUPERCOMPUTER_OUTPUT_DIR/copy.txt"', cpu: 1},
    {name: 'fails', cmd: 'echo no; exit 3', cpu: 1},
    {name: 'big', cmd: 'nproc >/dev/null; echo fine', cpu: 3, memory: 2000},
  ])
  const {err, code} = await done(sc(r, ['run', '--jobs', f, '--out', r.out, '--poll', '1s']))
  expect(code, err).toBe(3)
  expect(summary(r.out).sort()).toEqual([['big', '0'], ['fails', '3'], ['ok', '0']])
  expect(readFileSync(join(r.out, 'ok.files', 'copy.txt'), 'utf8')).toBe('hello')
  expect(readFileSync(join(r.out, 'fails.log'), 'utf8')).toBe('no\n')
  expect(err).not.toContain('queued again')
  expect(boxes(r.fake).every((b) => b.state === 'suspended')).toBe(true)
  const batch = JSON.parse(readFileSync(join(r.out, 'batch.json'), 'utf8'))
  const e = new Engine(batch.limits, batch.log) // batch.json's log, replayed through the rule
  expect([e.count('done'), e.boxes().length, e.verdict]).toEqual([3, 0, 'failed'])
}, 60_000)

test.skipIf(!linux)('a box lost mid-job: the attempt is tried again on a new box', async () => {
  const r = repo({'x': ''})
  const f = jobs(r, [{name: 'once', cmd: `if [ ! -f "$FAKE_INSTABOX_DIR/killed" ]; then touch "$FAKE_INSTABOX_DIR/killed"; rm "$HOME/../box.json"; sleep 3; fi; echo second > "$SUPERCOMPUTER_OUTPUT_DIR/o"`}])
  const {err, code} = await done(sc(r, ['run', '--jobs', f, '--out', r.out, '--poll', '1s']))
  expect(code, err).toBe(0)
  expect(err).toContain('queued again')
  expect(readFileSync(join(r.out, 'once.files', 'o'), 'utf8')).toBe('second\n')
}, 60_000)

test.skipIf(!linux)('strict-once: a lost attempt is not tried again', async () => {
  const r = repo({'x': ''})
  const f = jobs(r, [{name: 'once', cmd: `rm "$HOME/../box.json"; sleep 3`}])
  const {err, code} = await done(sc(r, ['run', '--jobs', f, '--out', r.out, '--poll', '1s', '--strict-once']))
  expect(code, err).toBe(3)
  expect(summary(r.out)).toEqual([['once', 'lost']])
}, 60_000)

test.skipIf(!linux)("a batch picked up after its client died: the finished job fetched, not run again", async () => {
  const r = repo({'x': ''})
  const f = jobs(r, [{name: 'slow', cmd: `echo run >> "$FAKE_INSTABOX_DIR/runs"; sleep 4; echo out > "$SUPERCOMPUTER_OUTPUT_DIR/o"`}])
  const first = sc(r, ['run', '--jobs', f, '--out', r.out, '--poll', '1s'])
  for (let i = 0; i < 100 && !existsSync(join(r.fake, 'runs')); i++) await Bun.sleep(100)
  first.kill(9)
  await first.exited
  await Bun.sleep(5000) // the job finishes on its box meanwhile
  const {err, code} = await done(sc(r, ['batch', 'fetch', r.out, '--poll', '1s']))
  expect(code, err).toBe(0)
  expect(readFileSync(join(r.fake, 'runs'), 'utf8')).toBe('run\n')
  expect(readFileSync(join(r.out, 'slow.files', 'o'), 'utf8')).toBe('out\n')
}, 60_000)

test.skipIf(!linux)('the warm pool: the next batch resumes a paused box; the cache seeds what an earlier job built', async () => {
  const r = repo({'lean/A.lean': 'def a := 1'}, '[[cache]]\nname = "olean"\ndir = "lean"\nmodules = ["A"]\ninputs = "{m}.lean"\noutputs = "{m}.olean"\nsalt = "v1"\n')
  const build = 'cd lean && if [ -f A.olean ]; then echo seeded > "$SUPERCOMPUTER_OUTPUT_DIR/how"; else echo built > A.olean; echo built > "$SUPERCOMPUTER_OUTPUT_DIR/how"; fi'
  const f = jobs(r, [{name: 'lean', cmd: build}])
  expect((await done(sc(r, ['run', '--jobs', f, '--out', r.out, '--poll', '1s']))).code).toBe(0)
  expect(readFileSync(join(r.out, 'lean.files', 'how'), 'utf8')).toBe('built\n')
  const again = await done(sc(r, ['run', '--jobs', f, '--out', join(r.dir, 'runs', 'y'), '--poll', '1s']))
  expect(again.code, again.err).toBe(0)
  expect(again.err).toContain('resumed from the warm pool')
  expect(readFileSync(join(r.dir, 'runs', 'y', 'lean.files', 'how'), 'utf8')).toBe('seeded\n')
  expect(boxes(r.fake).length).toBe(1)
}, 60_000)

test.skipIf(!linux)('a box gone with outputs on it: the attempt is tried again, not fetched forever', async () => {
  const r = repo({'x': ''})
  // (the first attempt writes its exit, then its box goes before the outputs are fetched)
  const f = jobs(r, [{name: 'gone', cmd: `if [ ! -f "$FAKE_INSTABOX_DIR/once" ]; then touch "$FAKE_INSTABOX_DIR/once"; (sleep 0.5; rm "$HOME/../box.json") & fi; echo ok > "$SUPERCOMPUTER_OUTPUT_DIR/o"`}])
  const {err, code} = await done(sc(r, ['run', '--jobs', f, '--out', r.out, '--poll', '2s']))
  expect(code, err).toBe(0)
  expect(err).toContain('queued again')
  expect(readFileSync(join(r.out, 'gone.files', 'o'), 'utf8')).toBe('ok\n')
}, 60_000)

test.skipIf(!linux)("GPU jobs: a box of the type's own size and template, deleted (not paused) when the batch ends", async () => {
  const r = repo({'x': ''})
  const f = jobs(r, [
    {name: 'l4', cmd: 'nproc >/dev/null; echo ok', gpu: 'l4', cpu: 10, memory: 30000},
    {name: 'h100', cmd: 'echo ok', gpu: 'h100'},
  ])
  const {err, code} = await done(sc(r, ['run', '--jobs', f, '--out', r.out, '--poll', '1s']))
  expect(code, err).toBe(0)
  const created = readFileSync(join(r.fake, 'calls.log'), 'utf8').split('\n').filter((l) => l.startsWith('created')).map((l) => l.split(' ').slice(2).join(' ')).sort()
  expect(created).toEqual(['template=gpu gpu=l4 11x36864', 'template=gpu-metal gpu=h100 44x229376'])
  expect(err).toContain('deleted (a GPU box is not kept)')
  expect(boxes(r.fake)).toEqual([])
}, 60_000)

test.skipIf(!linux)('a GPU job its box cannot hold is refused with the reason', async () => {
  const r = repo({'x': ''})
  const f = jobs(r, [{name: 'big', cmd: 'true', gpu: 't4', cpu: 8}])
  const {err, code} = await done(sc(r, ['run', '--jobs', f, '--out', r.out, '--poll', '1s']))
  expect(code).not.toBe(0)
  expect(err).toContain('a t4 box has 7 vCPUs')
}, 60_000)

test.skipIf(!linux)("a size-locked snapshot: its boxes are its size; a job it can't hold is refused before any box is made", async () => {
  const r = repo({'x': ''}, 'snapshot = "sc-locked"\nsize = "small"\n')
  writeFileSync(join(r.fake, 'snapshots.json'), JSON.stringify([{name: 'sc-locked', sizeLocked: true, defaultSpec: {vcpus: 8, memMiB: 16384}}]))
  // (a 4096 MiB job and a batch size of 2x4: still the locked 8x16, which instabox would refuse to change)
  const ok = jobs(r, [{name: 'fits', cmd: 'echo ok', cpu: 2, memory: 4096}])
  const a = await done(sc(r, ['run', '--jobs', ok, '--out', r.out, '--poll', '1s']))
  expect(a.code, a.err).toBe(0)
  expect(boxes(r.fake).map((b) => `${b.vcpus}x${b.memMiB}`)).toEqual(['8x16384'])
  // (16384 MiB and the box's system's 512 don't fit in 16384: refused at once, nothing made, nothing lost)
  const big = jobs(r, [{name: 'check', cmd: 'echo never', cpu: 8, memory: 16384}])
  const b = await done(sc(r, ['run', '--jobs', big, '--out', join(r.dir, 'runs', 'big'), '--poll', '1s']))
  expect(b.code).toBe(1)
  expect(b.err).toMatch(/check: asks 8 vCPUs and 16384 MiB .* locked snapshot of 8 vCPUs \/ 16384 MiB/)
  expect(existsSync(join(r.dir, 'runs', 'big', 'batch.json'))).toBe(false)
  expect(boxes(r.fake)).toHaveLength(1)
}, 60_000)

test.skipIf(!linux)('execs refused for load (too many at once across the account): strict-once work still runs, nothing lost', async () => {
  const r = repo({'x': ''})
  const f = jobs(r, [{name: 'a', cmd: 'echo a > "$SUPERCOMPUTER_OUTPUT_DIR/o"'}, {name: 'b', cmd: 'echo b > "$SUPERCOMPUTER_OUTPUT_DIR/o"'}])
  writeFileSync(join(r.fake, 'refuse-execs'), '8')
  const p = Bun.spawn(['bun', join(here, 'src/cli.ts'), 'run', '--jobs', f, '--out', r.out, '--poll', '1s', '--strict-once'], {
    cwd: r.dir, stdout: 'pipe', stderr: 'pipe',
    env: {...process.env, INSTABOX_BIN: `bun ${join(here, 'test/fake-instabox.ts')}`, FAKE_INSTABOX_DIR: r.fake, SC_STATE_DIR: join(r.fake, 'state'), SC_REAPER_RECORD: join(r.fake, 'reaper-spawns.jsonl'), SC_REFUSAL_BACKOFF_MS: '20'},
  })
  const {err, code} = await done(p)
  expect(code, err).toBe(0)
  expect(summary(r.out).sort()).toEqual([['a', '0'], ['b', '0']])
  expect(err).not.toContain('lost on') // (no attempt lost; the summary's "0 lost" aside)
}, 60_000)

// Each job counts its runs in the stand-in's folder: a job run twice is seen.
const counted = (name: string) => `echo run >> "$FAKE_INSTABOX_DIR/ran-${name}"; echo ${name} > "$SUPERCOMPUTER_OUTPUT_DIR/o"`
const ran = (fake: string, name: string) => (existsSync(join(fake, `ran-${name}`)) ? readFileSync(join(fake, `ran-${name}`), 'utf8') : '')
const faultsLeft = (fake: string) => Object.values(JSON.parse(readFileSync(join(fake, 'faults.json'), 'utf8')) as Record<string, string[]>).flat()
const made = (fake: string) => readFileSync(join(fake, 'calls.log'), 'utf8').split('\n').filter((l) => l.startsWith('created')).length

test.skipIf(!linux)('instabox failing in transport on every kind of call: a strict-once batch answers, nothing lost, nothing run twice, no box left behind', async () => {
  const r = repo({'x': ''}, 'snapshot = "sc-snap"\n')
  writeFileSync(join(r.fake, 'snapshots.json'), JSON.stringify([{name: 'sc-snap', defaultSpec: {vcpus: 4, memMiB: 8192}}]))
  writeFileSync(join(r.fake, 'faults.json'), JSON.stringify({
    snapshot: ['reset', 'timeout'],
    create: ['timeout-after', 'reset', 'refused', 'timeout-after'],
    ls: ['502'],
    'exec:test -s': ['eof'],
    push: ['stall', 'reset'],
    'exec:job.sh': ['reset-after', 'timeout', 'reset-after'],
    'exec:/exit ]': ['reset', 'timeout'],
    pull: ['reset'],
    suspend: ['timeout-after'],
  }))
  const f = jobs(r, ['a', 'b', 'c'].map((n) => ({name: n, cmd: counted(n), cpu: 3})))
  const {err, code} = await done(sc(r, ['run', '--jobs', f, '--out', r.out, '--poll', '1s', '--strict-once'], {SC_REFUSAL_BACKOFF_MS: '20'}))
  expect(code, err).toBe(0)
  expect(summary(r.out).sort()).toEqual([['a', '0'], ['b', '0'], ['c', '0']])
  for (const n of ['a', 'b', 'c']) expect(ran(r.fake, n)).toBe('run\n')
  expect(err).not.toContain('lost on')
  expect(faultsLeft(r.fake)).toEqual([])
  // (every box made is the batch's, paused into the pool: the timed-out creates made one box each, none leaked)
  expect(boxes(r.fake)).toHaveLength(made(r.fake))
  expect(boxes(r.fake).every((b) => b.state === 'suspended')).toBe(true)
}, 90_000)

test.skipIf(!linux)('a launch that ran but never answered: made again, its job claimed once on the box, so it runs once', async () => {
  const r = repo({'x': ''})
  writeFileSync(join(r.fake, 'faults.json'), JSON.stringify({'exec:job.sh': ['reset-after', 'reset-after', 'timeout-after']}))
  const f = jobs(r, [{name: 'once', cmd: counted('once')}])
  const {err, code} = await done(sc(r, ['run', '--jobs', f, '--out', r.out, '--poll', '1s', '--strict-once'], {SC_REFUSAL_BACKOFF_MS: '20'}))
  expect(code, err).toBe(0)
  expect(ran(r.fake, 'once')).toBe('run\n')
}, 60_000)

test.skipIf(!linux)("an ambiguous launch is not made again blindly: past its retries, the box's claim decides started or lost", async () => {
  // (the launch ran, its answer lost, no retry left: the job claimed its start, so it is running, not lost)
  const r = repo({'x': ''})
  writeFileSync(join(r.fake, 'faults.json'), JSON.stringify({'exec:job.sh': ['reset-after']}))
  const f = jobs(r, [{name: 'claimed', cmd: `sleep 4; ${counted('claimed')}`}])
  const a = await done(sc(r, ['run', '--jobs', f, '--out', r.out, '--poll', '1s', '--strict-once'], {SC_RETRY_BUDGET_MS: '0'}))
  expect(a.code, a.err).toBe(0)
  expect(a.err).toContain('its launch call failed after the job claimed its start')
  expect(ran(r.fake, 'claimed')).toBe('run\n')
  // (the launch failed before it ran: no claim on the box, so the attempt is lost, as strict-once says, and never run)
  const s = repo({'x': ''})
  writeFileSync(join(s.fake, 'faults.json'), JSON.stringify({'exec:job.sh': ['reset']}))
  const g = jobs(s, [{name: 'never', cmd: counted('never')}])
  const b = await done(sc(s, ['run', '--jobs', g, '--out', s.out, '--poll', '1s', '--strict-once'], {SC_RETRY_BUDGET_MS: '0'}))
  expect(b.code).toBe(3)
  expect(summary(s.out)).toEqual([['never', 'lost']])
  expect(ran(s.fake, 'never')).toBe('')
}, 60_000)

test.skipIf(!linux)("the warm pool reaps itself when a run starts: this owner's paused boxes past their keep, never another's, never one a batch holds", async () => {
  const r = repo({'x': ''})
  const me = owner(), long = new Date(Date.now() - 2 * 3600_000).toISOString(), lately = new Date(Date.now() - 60_000).toISOString()
  const put = (id: string, meta: Record<string, string>, at: string, state = 'suspended') => {
    mkdirSync(join(r.fake, id, 'home'), {recursive: true})
    writeFileSync(join(r.fake, id, 'box.json'), JSON.stringify({id, name: `sc-${id}`, state, vcpus: 2, memMiB: 4096, metadata: {sc: '1', ...meta}, updatedAt: at, autoSuspendedAt: at}))
  }
  const pool = {sc_owner: me, sc_image: 'other'}
  put('m_old', pool, long)
  put('m_fresh', pool, lately)
  put('m_theirs', {sc_owner: 'someone', sc_image: 'other'}, long)
  put('m_peer', {...pool, sc_batch: 'B0'}, long)
  put('m_running', pool, long, 'running')
  // (paused mid-batch with its client gone: held for that batch while its batch.json can pick it up)
  put('m_held', pool, long)
  const other = join(r.dir, 'runs', 'earlier')
  mkdirSync(other, {recursive: true})
  writeFileSync(join(other, 'batch.json'), JSON.stringify({id: 'B1'}))
  mkdirSync(join(r.fake, 'state', 'held'), {recursive: true})
  writeFileSync(join(r.fake, 'state', 'held', 'm_held'), JSON.stringify({batch: 'B1', out: other}))
  // (a record whose batch can't be picked up any more holds nothing)
  put('m_stale', pool, long)
  writeFileSync(join(r.fake, 'state', 'held', 'm_stale'), JSON.stringify({batch: 'B2', out: join(r.dir, 'runs', 'gone')}))
  const f = jobs(r, [{name: 'j', cmd: 'echo ok'}])
  const {err, code} = await done(sc(r, ['run', '--jobs', f, '--out', r.out, '--poll', '1s']))
  expect(code, err).toBe(0)
  const ids = new Set(boxes(r.fake).map((b) => b.id))
  expect(['m_old', 'm_stale'].filter((id) => ids.has(id))).toEqual([])
  expect(['m_fresh', 'm_theirs', 'm_peer', 'm_running', 'm_held'].filter((id) => !ids.has(id))).toEqual([])
  // (reaped before the batch made its first box: at the start, not only on the way out)
  const calls = readFileSync(join(r.fake, 'calls.log'), 'utf8').split('\n')
  expect(calls.findIndex((l) => l.startsWith('rm --yes m_old'))).toBeLessThan(calls.findIndex((l) => l.startsWith('create')))
}, 60_000)
