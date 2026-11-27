// Background reaper, hold expiry (pull before delete, keep on pull failure), single-instance PID lock, and the
// server-side --delete-after 24h backstop (no resume or placement near expiry).
// All reaper scheduling, lock and spawner tests inject the clock, liveness check and spawner (no real setsid daemon,
// no real sleep waits).
import {afterEach, expect, test} from 'bun:test'
import {existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, writeFileSync} from 'node:fs'
import {tmpdir} from 'node:os'
import {join} from 'node:path'
import type * as ib from '../src/instabox.ts'
import {
  acquireReaperLock,
  computeNextWakeMs,
  ensureReaper,
  owner,
  readReaperLock,
  reaperLockPath,
  reaperWantPath,
  reapUntilEmpty,
  releaseReaperLock,
} from '../src/reaper.ts'

const here = join(import.meta.dir, '..')
const linux = process.platform === 'linux'

const envKeys = ['SC_STATE_DIR', 'INSTABOX_BIN', 'FAKE_INSTABOX_DIR', 'SC_RETRY_BUDGET_MS', 'SC_REFUSAL_BACKOFF_MS'] as const
const savedEnv: Partial<Record<(typeof envKeys)[number], string | undefined>> = {}
for (const k of envKeys) savedEnv[k] = process.env[k]

afterEach(() => {
  for (const k of envKeys) {
    if (savedEnv[k] === undefined) delete process.env[k]
    else process.env[k] = savedEnv[k]
  }
})

function setup(files: Record<string, string> = {'x': 'hi'}, toml = '') {
  const dir = mkdtempSync(join(tmpdir(), 'sc-reap-repo-'))
  for (const [f, body] of Object.entries(files)) {
    mkdirSync(join(dir, f, '..'), {recursive: true})
    writeFileSync(join(dir, f), body)
  }
  writeFileSync(join(dir, 'supercomputer.toml'), `name = "t"\n[instabox]\ntemplate = "universal"\n${toml}`)
  Bun.spawnSync(['git', 'init', '-q'], {cwd: dir})
  Bun.spawnSync(['git', 'add', '-A'], {cwd: dir})
  const fake = mkdtempSync(join(tmpdir(), 'sc-reap-fake-'))
  const state = join(fake, 'state')
  const record = join(fake, 'reaper-spawns.jsonl')
  return {dir, fake, state, record, out: join(dir, 'runs', 'x')}
}

function sc(r: {dir: string; fake: string; state: string; record: string}, args: string[], env: Record<string, string> = {}) {
  return Bun.spawn(['bun', join(here, 'src/cli.ts'), ...args], {
    cwd: r.dir,
    stdout: 'pipe',
    stderr: 'pipe',
    env: {
      ...process.env,
      INSTABOX_BIN: `bun ${join(here, 'test/fake-instabox.ts')}`,
      FAKE_INSTABOX_DIR: r.fake,
      SC_STATE_DIR: r.state,
      SC_REAPER_RECORD: r.record,
      ...env,
    },
  })
}

async function done(p: ReturnType<typeof sc>) {
  const [out, err, code] = await Promise.all([new Response(p.stdout).text(), new Response(p.stderr).text(), p.exited])
  return {out, err, code}
}

const jobs = (r: {dir: string}, list: unknown[]) => {
  writeFileSync(join(r.dir, 'jobs.json'), JSON.stringify(list))
  return 'jobs.json'
}
const boxes = (fake: string) =>
  readdirSync(fake)
    .filter((f) => f.startsWith('m_') && existsSync(join(fake, f, 'box.json')))
    .map((id) => JSON.parse(readFileSync(join(fake, id, 'box.json'), 'utf8')) as ib.Box)
const argvLog = (fake: string): string[][] =>
  existsSync(join(fake, 'argv.log'))
    ? readFileSync(join(fake, 'argv.log'), 'utf8').trim().split('\n').filter(Boolean).map((l) => JSON.parse(l) as string[])
    : []
const recordedSpawns = (record: string): {cmd: string[]; keep: number; holdKeep: number; logPath: string}[] =>
  existsSync(record) ? readFileSync(record, 'utf8').trim().split('\n').filter(Boolean).map((l) => JSON.parse(l)) : []

function putBox(
  fake: string,
  id: string,
  opts: {owner?: string; image?: string; state?: string; pausedAtMs?: number; deleteAtMs?: number; vcpus?: number; memMiB?: number; extraMeta?: Record<string, string>} = {},
) {
  const at = new Date(opts.pausedAtMs ?? Date.now()).toISOString()
  mkdirSync(join(fake, id, 'home'), {recursive: true})
  writeFileSync(
    join(fake, id, 'box.json'),
    JSON.stringify({
      id,
      name: `sc-${id}`,
      state: opts.state ?? 'suspended',
      vcpus: opts.vcpus ?? 2,
      memMiB: opts.memMiB ?? 4096,
      metadata: {sc: '1', sc_owner: opts.owner ?? owner(), sc_image: opts.image ?? 'template_universal', ...(opts.extraMeta ?? {})},
      createdAt: at,
      updatedAt: at,
      autoSuspendedAt: (opts.state ?? 'suspended') === 'suspended' ? at : null,
      deleteAt: opts.deleteAtMs !== undefined ? new Date(opts.deleteAtMs).toISOString() : null,
    }),
  )
}

async function waitFor(fn: () => boolean, timeoutMs = 10_000, stepMs = 50): Promise<boolean> {
  const end = Date.now() + timeoutMs
  while (Date.now() < end) {
    if (fn()) return true
    await Bun.sleep(stepMs)
  }
  return fn()
}

test('reaper schedule: next wake equals the earliest keep/hold-keep expiry, and null when empty', () => {
  const me = 'me123456'
  const now = Date.UTC(2026, 10, 27, 12, 0, 0)
  const mk = (id: string, agoS: number, extra: Partial<ib.Box> = {}): ib.Box => ({
    id,
    name: id,
    state: 'suspended',
    metadata: {sc: '1', sc_owner: me},
    autoSuspendedAt: new Date(now - agoS * 1000).toISOString(),
    updatedAt: new Date(now - agoS * 1000).toISOString(),
    ...extra,
  })
  // Empty (only running unheld, another owner's, or controller-less sc_batch boxes) -> null
  const empty = computeNextWakeMs(
    [
      mk('m_run', 0, {state: 'running'}),
      mk('m_other', 100, {metadata: {sc: '1', sc_owner: 'other'}}),
      mk('m_peer', 100, {metadata: {sc: '1', sc_owner: me, sc_batch: 'B0'}}),
    ],
    {me, keep: 1800, holdKeep: 21600, holding: new Map(), nowMs: now},
  )
  expect(empty).toEqual({remainingPaused: 0, remainingHeld: 0, nextWakeMs: null})

  // Multiple paused boxes: m_a paused 1000s ago (800s left), m_b paused 1700s ago (100s left), m_held paused 21550s ago (50s left)
  const holding = new Map([['m_held', {rec: {batch: 'B1', out: '/tmp/b1'}, live: false}]])
  const sched = computeNextWakeMs(
    [mk('m_a', 1000), mk('m_b', 1700), mk('m_held', 21550)],
    {me, keep: 1800, holdKeep: 21600, holding, nowMs: now},
  )
  expect(sched.remainingPaused).toBe(2)
  expect(sched.remainingHeld).toBe(1)
  expect(sched.nextWakeMs).toBe(50_100)

  // When m_held still has 20000s left, earliest is m_b (100s + 100ms slack = 100100ms)
  const sched2 = computeNextWakeMs(
    [mk('m_a', 1000), mk('m_b', 1700), mk('m_held', 1600)],
    {me, keep: 1800, holdKeep: 21600, holding, nowMs: now},
  )
  expect(sched2.nextWakeMs).toBe(100_100)
})

test('single-instance PID lock and injected spawner: blocks second live reaper, signals/tightens when alive, takes over stale dead PID', () => {
  const dir = mkdtempSync(join(tmpdir(), 'sc-lock-'))
  process.env.SC_STATE_DIR = dir
  const livePids = new Set<number>([424242])
  const alive = (pid: number) => livePids.has(pid)

  const first = acquireReaperLock(1800, 21600, 424242, {alive})
  expect(first.acquired).toBe(true)
  expect(readReaperLock()).toEqual({pid: 424242, keep: 1800, holdKeep: 21600})

  // A second contender cannot take over while 424242 is alive
  const second = acquireReaperLock(1800, 21600, 515151, {alive})
  expect(second.acquired).toBe(false)
  expect(second.owner?.pid).toBe(424242)

  // ensureReaper does NOT spawn a second reaper while 424242 is alive; tightens keep via reaper.want and sends SIGUSR1
  const spawns: {cmd: string[]; logPath: string}[] = []
  const signals: {pid: number; sig: string}[] = []
  const r1 = ensureReaper(120, 21600, undefined, {
    alive,
    spawn: (cmd, logPath) => (spawns.push({cmd, logPath}), 9001),
    signal: (pid, sig) => signals.push({pid, sig}),
  })
  expect(r1).toEqual({started: false, pid: 424242})
  expect(spawns).toHaveLength(0)
  expect(signals).toEqual([{pid: 424242, sig: 'SIGUSR1'}])
  expect(JSON.parse(readFileSync(reaperWantPath(), 'utf8'))).toEqual({keep: 120, holdKeep: 21600})

  // Mark 424242 dead -> stale lock is taken over cleanly by both ensureReaper and acquireReaperLock
  livePids.delete(424242)
  const r2 = ensureReaper(120, 3600, undefined, {
    alive,
    spawn: (cmd, logPath) => (spawns.push({cmd, logPath}), 9002),
    signal: (pid, sig) => signals.push({pid, sig}),
  })
  expect(r2).toEqual({started: true, pid: 9002})
  expect(spawns).toHaveLength(1)
  expect(spawns[0]!.cmd).toContain('--until-empty')
  expect(spawns[0]!.cmd).toContain('120s')
  expect(spawns[0]!.logPath).toBe(join(dir, 'reaper.log'))

  const takeover = acquireReaperLock(120, 3600, 515151, {alive})
  expect(takeover.acquired).toBe(true)
  expect(readReaperLock()).toEqual({pid: 515151, keep: 120, holdKeep: 3600})

  // Releasing with another PID does not remove 515151's lock; releasing with 515151 does
  releaseReaperLock(424242)
  expect(existsSync(reaperLockPath())).toBe(true)
  releaseReaperLock(515151)
  expect(existsSync(reaperLockPath())).toBe(false)
})

test('reapUntilEmpty with virtual clock: wakes at staggered earliest expiries, reaps each box, exits when empty and cleans lock', async () => {
  const r = setup()
  process.env.INSTABOX_BIN = `bun ${join(here, 'test/fake-instabox.ts')}`
  process.env.FAKE_INSTABOX_DIR = r.fake
  process.env.SC_STATE_DIR = r.state

  let now = Date.UTC(2026, 10, 27, 12, 0, 0)
  // Two paused boxes with staggered pause timestamps under keep=1800s (30m):
  //   m_first paused 25m ago -> expires in 5m (300s)
  //   m_second paused 10m ago -> expires in 20m (1200s, i.e. 900s after m_first)
  // Plus a running unheld box and another owner's box that must never be touched.
  putBox(r.fake, 'm_first', {pausedAtMs: now - 25 * 60_000})
  putBox(r.fake, 'm_second', {pausedAtMs: now - 10 * 60_000})
  putBox(r.fake, 'm_running', {state: 'running', pausedAtMs: now - 3600_000})
  putBox(r.fake, 'm_other', {owner: 'other_owner', pausedAtMs: now - 3600_000})

  const sleeps: number[] = []
  const logs: string[] = []
  const deleted = await reapUntilEmpty(1800, 21600, (s) => logs.push(s), {
    now: () => now,
    sleep: async (ms) => {
      sleeps.push(ms)
      now += ms
    },
    alive: () => false,
  })
  expect(deleted).toBe(2)
  // First wake at 300s (+100ms slack), second wake 900s later (total 1200.1s), then empty -> exits!
  expect(sleeps).toEqual([300_100, 900_000])
  expect(boxes(r.fake).map((b) => b.id).sort()).toEqual(['m_other', 'm_running'])
  expect(existsSync(join(r.state, 'reaper.pid'))).toBe(false)
  expect(logs.some((l) => l.includes('no paused or held boxes left'))).toBe(true)
})

test('reapUntilEmpty exits immediately when another live reaper holds the lock, or when no paused/held boxes exist', async () => {
  const r = setup()
  process.env.INSTABOX_BIN = `bun ${join(here, 'test/fake-instabox.ts')}`
  process.env.FAKE_INSTABOX_DIR = r.fake
  process.env.SC_STATE_DIR = r.state

  // 1) Another live PID holds the lock -> exits immediately without touching anything
  acquireReaperLock(1800, 21600, 777777, {alive: (pid) => pid === 777777})
  const logs1: string[] = []
  const n1 = await reapUntilEmpty(1800, 21600, (s) => logs1.push(s), {alive: (pid) => pid === 777777})
  expect(n1).toBe(0)
  expect(logs1.some((l) => l.includes('reaper already running (pid 777777)'))).toBe(true)
  expect(readReaperLock()?.pid).toBe(777777)
  releaseReaperLock(777777)

  // 2) No paused or held boxes of this client -> exits immediately with 0 sleeps and removes lock
  putBox(r.fake, 'm_run', {state: 'running'})
  putBox(r.fake, 'm_theirs', {owner: 'someone', state: 'suspended'})
  const sleeps: number[] = []
  const logs2: string[] = []
  const n2 = await reapUntilEmpty(1800, 21600, (s) => logs2.push(s), {
    sleep: async (ms) => void sleeps.push(ms),
    alive: () => false,
  })
  expect(n2).toBe(0)
  expect(sleeps).toEqual([])
  expect(existsSync(join(r.state, 'reaper.pid'))).toBe(false)
  expect(boxes(r.fake)).toHaveLength(2)
})

test.skipIf(!linux)('run and reap trigger the background reaper when paused boxes stay inside keep', async () => {
  const r = setup()
  const f = jobs(r, [{name: 'ok', cmd: 'echo hi > "$SUPERCOMPUTER_OUTPUT_DIR/o"'}])
  const res = await done(sc(r, ['run', '--jobs', f, '--out', r.out, '--poll', '1s', '--keep', '2m']))
  expect(res.code, res.err).toBe(0)
  const spawns = recordedSpawns(r.record)
  expect(spawns.length).toBeGreaterThanOrEqual(1)
  expect(spawns.at(-1)!.cmd).toContain('--until-empty')
  expect(spawns.at(-1)!.keep).toBe(120)
  expect(spawns.at(-1)!.holdKeep).toBe(21600)
}, 30_000)

test.skipIf(!linux)('hold expiry: pulls outputs into recorded out dir without re-running, then unholds and deletes; live batch hold is never touched', async () => {
  const r = setup({'src/msg.txt': 'from-held-box'})
  // Start a real run and SIGKILL the client after the job starts (leaving batch.json open + held box)
  const f = jobs(r, [{name: 'slow', cmd: 'echo ran >> "$FAKE_INSTABOX_DIR/runs"; sleep 1; cat src/msg.txt > "$SUPERCOMPUTER_OUTPUT_DIR/out.txt"; echo done-log'}])
  const first = sc(r, ['run', '--jobs', f, '--out', r.out, '--poll', '1s'])
  expect(await waitFor(() => existsSync(join(r.fake, 'runs')), 10_000, 50)).toBe(true)
  first.kill(9)
  await first.exited
  const [box] = boxes(r.fake)
  expect(box).toBeDefined()
  const batchId = (JSON.parse(readFileSync(join(r.out, 'batch.json'), 'utf8')) as {id: string}).id
  expect(await waitFor(() => existsSync(join(r.fake, box!.id, 'home', 'sc', 'run', batchId, 'slow', 'exit')), 10_000, 50)).toBe(true)

  // Mark the held box suspended 7 hours ago (past default 6h hold-keep)
  const sevenHoursAgo = new Date(Date.now() - 7 * 3600_000).toISOString()
  writeFileSync(
    join(r.fake, box!.id, 'box.json'),
    JSON.stringify({...box!, state: 'suspended', updatedAt: sevenHoursAgo, autoSuspendedAt: sevenHoursAgo}),
  )

  // Add a held box whose client PID IS ALIVE (use current test runner process.pid, guaranteed alive): never touched!
  putBox(r.fake, 'm_live_held', {pausedAtMs: Date.now() - 7 * 3600_000})
  const liveOut = join(r.dir, 'runs', 'live')
  mkdirSync(liveOut, {recursive: true})
  writeFileSync(join(liveOut, 'batch.json'), JSON.stringify({id: 'LIVE1'}))
  writeFileSync(join(r.state, 'held', 'm_live_held'), JSON.stringify({batch: 'LIVE1', out: liveOut, pid: process.pid}))

  // Inside hold-keep (8h), neither held box is touched
  const inside = await done(sc(r, ['reap', '--keep', '30m', '--hold-keep', '8h']))
  expect(inside.code, inside.err).toBe(0)
  expect(boxes(r.fake).map((b) => b.id).sort()).toEqual([box!.id, 'm_live_held'].sort())
  expect(existsSync(join(r.out, 'slow.files', 'out.txt'))).toBe(false)

  // Past hold-keep (6h): expired held box pulls outputs, unholds and deletes; live held box stays
  const expired = await done(sc(r, ['reap', '--keep', '30m', '--hold-keep', '6h']))
  expect(expired.code, expired.err).toBe(0)
  expect(readFileSync(join(r.out, 'slow.files', 'out.txt'), 'utf8')).toBe('from-held-box')
  expect(readFileSync(join(r.out, 'slow.log'), 'utf8')).toBe('done-log\n')
  expect(readFileSync(join(r.fake, 'runs'), 'utf8')).toBe('ran\n') // no re-run
  expect(existsSync(join(r.state, 'held', box!.id))).toBe(false)
  expect(boxes(r.fake).map((b) => b.id)).toEqual(['m_live_held'])
  expect(existsSync(join(r.state, 'held', 'm_live_held'))).toBe(true)
  expect(existsSync(join(r.out, 'summary.tsv'))).toBe(true)
}, 30_000)

test.skipIf(!linux)('hold expiry keeps the box when outputs cannot be pulled (missing exit or pull failure)', async () => {
  // 1) Missing exit file -> kept, still held, re-suspended
  const r = setup()
  const f = jobs(r, [{name: 'stuck', cmd: 'echo ran >> "$FAKE_INSTABOX_DIR/runs"; sleep 60'}])
  const first = sc(r, ['run', '--jobs', f, '--out', r.out, '--poll', '1s'])
  expect(await waitFor(() => existsSync(join(r.fake, 'runs')), 10_000, 50)).toBe(true)
  first.kill(9)
  await first.exited
  const [box] = boxes(r.fake)
  expect(box).toBeDefined()
  const batchId = (JSON.parse(readFileSync(join(r.out, 'batch.json'), 'utf8')) as {id: string}).id
  const pidFile = join(r.fake, box!.id, 'home', 'sc', 'run', batchId, 'stuck', 'pid')
  if (existsSync(pidFile)) {
    const jpid = Number(readFileSync(pidFile, 'utf8').trim())
    if (jpid > 0) try { process.kill(jpid, 'SIGKILL') } catch {}
  }
  const sevenHoursAgo = new Date(Date.now() - 7 * 3600_000).toISOString()
  writeFileSync(
    join(r.fake, box!.id, 'box.json'),
    JSON.stringify({...box!, state: 'suspended', updatedAt: sevenHoursAgo, autoSuspendedAt: sevenHoursAgo}),
  )

  const res = await done(sc(r, ['reap', '--keep', '30m', '--hold-keep', '6h']))
  expect(res.code, res.err).toBe(0)
  expect(res.err).toContain('has no exit file yet; keeping box')
  expect(boxes(r.fake).map((b) => b.id)).toEqual([box!.id])
  expect(boxes(r.fake)[0]!.state).toBe('suspended')
  expect(existsSync(join(r.state, 'held', box!.id))).toBe(true)

  // 2) Pull transport failure on a finished job -> kept, still held, re-suspended
  const r2 = setup()
  const f2 = jobs(r2, [{name: 'donejob', cmd: 'echo ran >> "$FAKE_INSTABOX_DIR/runs"; echo hi > "$SUPERCOMPUTER_OUTPUT_DIR/o"'}])
  const p2 = sc(r2, ['run', '--jobs', f2, '--out', r2.out, '--poll', '1s'])
  expect(await waitFor(() => existsSync(join(r2.fake, 'runs')), 10_000, 50)).toBe(true)
  p2.kill(9)
  await p2.exited
  const [box2] = boxes(r2.fake)
  const batch2Id = (JSON.parse(readFileSync(join(r2.out, 'batch.json'), 'utf8')) as {id: string}).id
  expect(await waitFor(() => existsSync(join(r2.fake, box2!.id, 'home', 'sc', 'run', batch2Id, 'donejob', 'exit')), 10_000, 50)).toBe(true)
  writeFileSync(
    join(r2.fake, box2!.id, 'box.json'),
    JSON.stringify({...box2!, state: 'suspended', updatedAt: sevenHoursAgo, autoSuspendedAt: sevenHoursAgo}),
  )
  writeFileSync(join(r2.fake, 'faults.json'), JSON.stringify({pull: ['reset']}))
  const res2 = await done(sc(r2, ['reap', '--keep', '30m', '--hold-keep', '6h'], {SC_RETRY_BUDGET_MS: '0'}))
  expect(res2.code, res2.err).toBe(0)
  expect(res2.err).toContain('could not pull outputs for donejob')
  expect(boxes(r2.fake).map((b) => b.id)).toEqual([box2!.id])
  expect(boxes(r2.fake)[0]!.state).toBe('suspended')
  expect(existsSync(join(r2.state, 'held', box2!.id))).toBe(true)
}, 30_000)

test.skipIf(!linux)('server-side backstop: create passes --delete-after 24h and pool never resumes or places a job near delete-after', async () => {
  const r = setup()
  const in30m = Date.now() + 30 * 60_000
  putBox(r.fake, 'm_dying', {pausedAtMs: Date.now() - 60_000, deleteAtMs: in30m, vcpus: 2, memMiB: 4096})

  // 1) timeout=3600s MUST NOT resume m_dying (30m < 60m+10m): creates a new box with --delete-after 24h instead.
  const f1 = jobs(r, [{name: 'long', cmd: 'echo ok > "$SUPERCOMPUTER_OUTPUT_DIR/o"', cpu: 1, memory: 1024, timeout: 3600}])
  const a = await done(sc(r, ['run', '--jobs', f1, '--out', r.out, '--poll', '1s']))
  expect(a.code, a.err).toBe(0)
  expect(a.err).not.toContain('resumed from the warm pool')
  const creates = argvLog(r.fake).filter((args) => args[0] === 'create')
  expect(creates).toHaveLength(1)
  const delIdx = creates[0]!.indexOf('--delete-after')
  expect(delIdx).toBeGreaterThan(-1)
  expect(creates[0]![delIdx + 1]).toBe('24h')
  const createdBox = boxes(r.fake).find((b) => b.id !== 'm_dying')!
  expect(createdBox.deleteAt).toBeTruthy()
  const hours = (Date.parse(createdBox.deleteAt!) - Date.now()) / 3600_000
  expect(hours).toBeGreaterThan(23.5)
  expect(hours).toBeLessThanOrEqual(24.1)

  // 2) Open box near delete-after: j_short (60s) resumes m_shortlife (30m left), then queued j_long (3600s) MUST NOT
  //    be placed on m_shortlife — m_shortlife pauses and a new box is created for j_long.
  const r2 = setup()
  putBox(r2.fake, 'm_shortlife', {pausedAtMs: Date.now() - 60_000, deleteAtMs: Date.now() + 30 * 60_000, vcpus: 2, memMiB: 4096})
  const f2 = jobs(r2, [
    {name: 'j_short', cmd: 'echo s > "$SUPERCOMPUTER_OUTPUT_DIR/o"', cpu: 2, memory: 1024, timeout: 60},
    {name: 'j_long', cmd: 'echo l > "$SUPERCOMPUTER_OUTPUT_DIR/o"', cpu: 2, memory: 1024, timeout: 3600},
  ])
  const b = await done(sc(r2, ['run', '--jobs', f2, '--out', r2.out, '--max-boxes', '1', '--poll', '1s']))
  expect(b.code, b.err).toBe(0)
  expect(b.err).toContain('box sc-m_shortlife: resumed from the warm pool')
  const rows = readFileSync(join(r2.out, 'summary.tsv'), 'utf8').trim().split('\n').slice(1).map((l) => l.split('\t'))
  const where = Object.fromEntries(rows.map((cols) => [cols[0], cols[3]]))
  expect(where.j_short).toBe('sc-m_shortlife')
  expect(where.j_long).not.toBe('sc-m_shortlife')
  expect(boxes(r2.fake).length).toBe(2)
}, 30_000)
