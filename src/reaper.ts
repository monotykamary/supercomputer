// Warm-pool reaping, hold expiry (pulling outputs before deleting), and the single-instance detached background
// reaper (`supercomputer reap --until-empty`). Every delete/pull decision comes from the proven gdp-wired rule in
// src/pool.ts.

import {closeSync, existsSync, mkdirSync, mkdtempSync, openSync, readdirSync, readFileSync, renameSync, rmSync, statSync, writeFileSync} from 'node:fs'
import {homedir, hostname, tmpdir, userInfo} from 'node:os'
import {join} from 'node:path'
import {Engine, type Limits, type Req} from './engine.ts'
import * as ib from './instabox.ts'
import type {Cache, Config, Job, Size} from './jobs.ts'
import {decideLetGo, DEFAULT_HOLD_KEEP_S} from './pool.ts'
import type {Locked} from './size.ts'

// A box of a batch, by its slot: what instabox knows it as.
export type BoxRec = {slot: number; id: string; name: string; size: Size; gpu?: string; image?: string; tree?: string; deleteAt?: string}

// What <out>/batch.json holds: enough to pick the batch up with no jobs file and no memory of this client.
export type Batch = {
  id: string
  owner: string
  tree: string
  // Each [[cache]] with its key for this tree: the hash of its salt and its inputs.
  caches: (Cache & {key: string})[]
  cfg: Config
  limits: Limits
  jobs: Job[]
  // The rule's numbers for what it compares: a job's group and what a box must be for it (its image, GPU, named size).
  groups: string[]
  kinds: string[]
  boxes: BoxRec[]
  // The requests the batch rule accepted, in order: replayed, they are the batch's state.
  log: Req[]
  // Each job's last attempt: when it started and ended, and the box it ran on (summary.tsv).
  times: Record<string, {started?: number; ended?: number; seconds?: number; where?: string}>
  nextSlot: number
  // Each image's locked size (an instabox snapshot saved with sizeLocked), by image name ('' the repo's): its boxes
  // are that size, never another. (Absent in a batch.json from before 1.0.1: none locked.)
  locked?: Locked
}

export const owner = () => new Bun.CryptoHasher('sha256').update(`${userInfo().username}@${hostname()}`).digest('hex').slice(0, 8)
export const stateDir = () => process.env.SC_STATE_DIR ?? join(process.env.XDG_STATE_HOME ?? join(homedir(), '.local', 'state'), 'supercomputer')
export const heldDir = () => join(stateDir(), 'held')
export const reaperLockPath = () => join(stateDir(), 'reaper.pid')
export const reaperWantPath = () => join(stateDir(), 'reaper.want')
export const reaperLogPath = () => join(stateDir(), 'reaper.log')

export function pidAlive(pid: number): boolean {
  if (!Number.isInteger(pid) || pid <= 0) return false
  try {
    process.kill(pid, 0)
  } catch (e: unknown) {
    return (e as NodeJS.ErrnoException)?.code === 'EPERM'
  }
  // On Linux, process.kill(pid, 0) still succeeds on a defunct (zombie) process; treat 'Z' as dead.
  try {
    const stat = readFileSync(`/proc/${pid}/stat`, 'utf8')
    const afterName = stat.slice(stat.lastIndexOf(')') + 2)
    if (afterName.startsWith('Z')) return false
  } catch {}
  return true
}

export type ReaperDeps = {
  now?: () => number
  sleep?: (ms: number) => Promise<void>
  alive?: (pid: number) => boolean
  spawn?: (cmd: string[], logPath: string) => number
  signal?: (pid: number, sig: NodeJS.Signals) => void
}

export function shaFile(file: string): string {
  return new Bun.CryptoHasher('sha256').update(readFileSync(file)).digest('hex')
}

export function readBatch(out: string): Batch | undefined {
  const f = join(out, 'batch.json')
  return existsSync(f) ? (JSON.parse(readFileSync(f, 'utf8')) as Batch) : undefined
}

export function saveBatch(out: string, batch: Batch) {
  const file = join(out, 'batch.json')
  writeFileSync(`${file}.tmp`, JSON.stringify(batch, null, 1))
  renameSync(`${file}.tmp`, file)
}

export function writeSummary(out: string, batch: Batch, rule: Engine) {
  const rows = batch.jobs.map((j, i) => {
    const st = rule.job(i)!
    const t = batch.times[j.name] ?? {}
    return [j.name, st.st === 'done' ? String(st.exit) : st.st, String(st.st === 'done' ? (t.seconds ?? '') : ''), t.where ?? '']
  })
  writeFileSync(join(out, 'summary.tsv'), ['name\texit\tseconds\twhere', ...rows.map((r) => r.join('\t'))].join('\n') + '\n')
}

// The boxes this client's batches hold (a job or outputs on them, or about to be): one file per box, naming its
// batch's --out and the client PID, from the moment the batch has the box until it pauses it empty (or finds it gone).
export type HoldRec = {batch: string; out: string; pid?: number}
export type HeldEntry = {rec?: HoldRec; live: boolean}

export function hold(id: string, batch: string, out: string, pid = process.pid) {
  if (!id) return
  mkdirSync(heldDir(), {recursive: true})
  const file = join(heldDir(), id)
  writeFileSync(`${file}.tmp.${process.pid}`, JSON.stringify({batch, out, pid}))
  renameSync(`${file}.tmp.${process.pid}`, file)
}

export const unhold = (id: string) => {
  if (id) rmSync(join(heldDir(), id), {force: true})
}

function readHoldFile(id: string): HoldRec | undefined | 'corrupt' {
  const f = join(heldDir(), id)
  if (!existsSync(f)) return undefined
  try {
    const r = JSON.parse(readFileSync(f, 'utf8')) as Partial<HoldRec>
    if (typeof r.batch !== 'string' || typeof r.out !== 'string') return 'corrupt'
    return {batch: r.batch, out: r.out, pid: typeof r.pid === 'number' ? r.pid : undefined}
  } catch {
    return 'corrupt'
  }
}

export function heldMap(alive: (pid: number) => boolean = pidAlive): Map<string, HeldEntry> {
  const entries = new Map<string, HeldEntry>()
  if (!existsSync(heldDir())) return entries
  for (const id of readdirSync(heldDir())) {
    if (id.includes('.tmp.')) continue
    const rec = readHoldFile(id)
    if (rec === undefined) continue
    if (rec === 'corrupt') {
      entries.set(id, {live: true})
      continue
    }
    try {
      const b = readBatch(rec.out)
      if (b?.id === rec.batch) {
        const live = rec.pid !== undefined && alive(rec.pid)
        entries.set(id, {rec, live})
      } else {
        // (its batch can no longer be picked up: nothing would ever take what the box holds)
        unhold(id)
      }
    } catch {
      entries.set(id, {rec, live: true})
    }
  }
  return entries
}

export const heldIds = (alive: (pid: number) => boolean = pidAlive): Set<string> => new Set(heldMap(alive).keys())

// Pull one ended job's log and output files from its box, verifying sha256 hashes before unpacking.
// Returns 'missing' when the job folder is gone from the box; throws on transport/verification failures.
export async function pullJobOutputs(
  boxId: string,
  batchId: string,
  jobName: string,
  outDir: string,
  runApi: <T>(f: () => Promise<T>) => Promise<T>,
): Promise<{status: 'missing'} | {status: 'ok'; count: number; started: number}> {
  const J = `~/sc/run/${batchId}/${jobName}`
  const local = mkdtempSync(join(tmpdir(), 'sc-out-'))
  try {
    const pack = await runApi(() =>
      ib.exec(boxId, `cd ${J} 2>/dev/null || exit 7; tar -czf out.tgz -C out . && sha256sum log out.tgz && find out -type f | wc -l && cat started`, {idempotent: true}),
    )
    if (pack.exitCode === 7) return {status: 'missing'}
    if (pack.exitCode !== 0) throw new Error(`packing outputs exited ${pack.exitCode}: ${pack.stderr.trim()}`)
    const lines = pack.stdout.trim().split('\n')
    const sums = Object.fromEntries(lines.slice(0, 2).map((l) => l.split(/\s+/).reverse()))
    const count = Number(lines[2])
    const started = Number(lines[3])
    mkdirSync(outDir, {recursive: true})
    const logFile = join(outDir, `${jobName}.log`)
    await runApi(() => ib.pull(boxId, `${J.replace(/^~/, '/data/home')}/log`, logFile))
    let verified = shaFile(logFile) === sums.log
    if (count > 0) {
      const tgz = join(local, 'out.tgz')
      await runApi(() => ib.pull(boxId, `${J.replace(/^~/, '/data/home')}/out.tgz`, tgz))
      verified &&= shaFile(tgz) === sums['out.tgz']
      const files = join(outDir, `${jobName}.files`)
      rmSync(files, {recursive: true, force: true})
      mkdirSync(files, {recursive: true})
      if (verified && Bun.spawnSync(['tar', '-xzf', tgz, '-C', files]).exitCode !== 0) verified = false
    }
    if (!verified) throw new Error('fetched outputs did not match their sha256')
    return {status: 'ok', count, started}
  } finally {
    rmSync(local, {recursive: true, force: true})
  }
}

// Pull every unfetched job's outputs from an expired held box into its batch's recorded out dir (never re-running any
// job). Returns true only once nothing unfetched remains on the box; on any failure, logs, re-suspends if resumed,
// and returns false so the box is kept until the server-side backstop.
async function pullHeldBox(box: ib.Box, rec: HoldRec, log: (s: string) => void, alive: (pid: number) => boolean = pidAlive): Promise<boolean> {
  // Re-check hold file right before touching the box in case a live `batch fetch` took it over.
  const fresh = readHoldFile(box.id)
  if (!fresh || fresh === 'corrupt' || (fresh.pid !== undefined && alive(fresh.pid))) return false
  let batch: Batch | undefined
  try {
    batch = readBatch(rec.out)
  } catch (e) {
    log(`box ${box.name}: could not read ${join(rec.out, 'batch.json')}: ${e instanceof Error ? e.message : e}; keeping box`)
    return false
  }
  if (!batch || batch.id !== rec.batch) return true
  if (!batch.limits || !Array.isArray(batch.jobs) || !Array.isArray(batch.boxes) || !Array.isArray(batch.log)) {
    log(`box ${box.name}: batch ${rec.batch} in ${rec.out} has incomplete batch.json; keeping box`)
    return false
  }
  let rule: Engine
  try {
    rule = new Engine(batch.limits, batch.log)
  } catch (e) {
    log(`box ${box.name}: could not replay batch ${batch.id}: ${e instanceof Error ? e.message : e}; keeping box`)
    return false
  }
  const boxRecs = batch.boxes.filter((x) => x.id === box.id)
  const pending = boxRecs.flatMap((br) =>
    rule.jobs().filter(([, j]) => j.box === br.slot && (j.st === 'running' || j.st === 'ended')).map(([i]) => ({slot: br.slot, i})),
  )
  const closeAndSave = () => {
    for (const br of boxRecs) if (rule.box(br.slot)?.open) rule.step({op: 'close', box: br.slot})
    batch!.boxes = batch!.boxes.filter((x) => x.id !== box.id)
    if (rule.verdict === 'open' && rule.count('queued') + rule.count('running') + rule.count('ended') === 0) {
      if (rule.step({op: 'finish'})) writeSummary(rec.out, batch!, rule)
    }
    batch!.log = rule.log
    saveBatch(rec.out, batch!)
  }
  if (!pending.length) {
    closeAndSave()
    return true
  }
  let resumed = false
  try {
    await ib.resume(box.id)
    resumed = true
    // Re-check once resumed in case a live client picked the batch up meanwhile.
    const nowHold = readHoldFile(box.id)
    if (!nowHold || nowHold === 'corrupt' || (nowHold.pid !== undefined && alive(nowHold.pid))) return false
    let allOk = true
    const direct = <T>(f: () => Promise<T>) => f()
    for (const {i} of pending) {
      const job = batch.jobs[i]!
      const J = `~/sc/run/${batch.id}/${job.name}`
      try {
        if (rule.job(i)?.st === 'running') {
          const ex = await ib.exec(box.id, `[ -f ${J}/exit ] && cat ${J}/exit || true`, {timeout: 60, idempotent: true})
          const out = ex.stdout.trim()
          if (!out) {
            log(`box ${box.name}: ${job.name} has no exit file yet; keeping box until server-side backstop`)
            allOk = false
            continue
          }
          const [code, ended] = out.split(/\s+/).map(Number) as [number, number?]
          if (rule.step({op: 'exit', job: i, code: Number.isInteger(code) && code >= 0 ? code : 255})) {
            if (Number.isFinite(ended)) (batch.times[job.name] ??= {}).ended = ended
            batch.log = rule.log
            saveBatch(rec.out, batch)
          }
        }
        if (rule.job(i)?.st === 'ended') {
          const pulled = await pullJobOutputs(box.id, batch.id, job.name, rec.out, direct)
          if (pulled.status === 'missing') {
            log(`box ${box.name}: ${job.name} outputs missing on box; keeping box until server-side backstop`)
            allOk = false
            continue
          }
          const t = (batch.times[job.name] ??= {})
          t.seconds = Math.round((t.ended ?? Date.now() / 1000) - (Number.isFinite(pulled.started) ? pulled.started : (t.started ?? Date.now() / 1000)))
          if (rule.step({op: 'fetch', job: i})) {
            batch.log = rule.log
            saveBatch(rec.out, batch)
            log(`${job.name}: pulled outputs from expired held box ${box.name} (exit ${rule.job(i)!.exit}${pulled.count ? `, ${pulled.count} output files` : ''})`)
            await ib.exec(box.id, `rm -rf ${J} ${J}.lock`, {idempotent: true}).catch(() => undefined)
          }
        }
      } catch (e) {
        log(`box ${box.name}: could not pull outputs for ${job.name}: ${e instanceof Error ? e.message : e}; keeping box until server-side backstop`)
        allOk = false
      }
    }
    if (allOk) {
      closeAndSave()
      return true
    }
    await ib.suspend(box.id).catch(() => undefined)
    return false
  } catch (e) {
    log(`box ${box.name}: could not pull expired held box: ${e instanceof Error ? e.message : e}; keeping box until server-side backstop`)
    if (resumed) await ib.suspend(box.id).catch(() => undefined)
    return false
  }
}

export type ReapOptions = {
  holdKeep?: number
  nowMs?: number
  failedPulls?: Map<string, number>
  alive?: (pid: number) => boolean
}

export type ReapReport = {
  deleted: number
  pulled: number
  pullFailed: number
  remainingPaused: number
  remainingHeld: number
  nextWakeMs: number | null
}

export function pausedSinceMs(b: Pick<ib.Box, 'autoSuspendedAt' | 'updatedAt'>): number {
  return Date.parse(b.autoSuspendedAt ?? b.updatedAt ?? '')
}

// Pure schedule helper: compute the earliest expiry (ms from `nowMs`) across all paused/held boxes of `me` that
// still remain after a reap pass. Returns null when no paused or held box of this client is left.
export function computeNextWakeMs(
  boxes: ib.Box[],
  opts: {me: string; keep: number; holdKeep: number; holding: Map<string, HeldEntry>; spare?: Set<string>; nowMs?: number; failedPulls?: Map<string, number>},
): {remainingPaused: number; remainingHeld: number; nextWakeMs: number | null} {
  const now = opts.nowMs ?? Date.now()
  const spare = opts.spare ?? new Set<string>()
  const idlePollMs = Math.max(1000, Math.min(opts.keep, opts.holdKeep, 300) * 1000)
  let remainingPaused = 0
  let remainingHeld = 0
  let nextWakeMs: number | null = null
  const bump = (ms: number) => {
    const clamped = Math.max(250, Math.ceil(ms))
    nextWakeMs = nextWakeMs === null ? clamped : Math.min(nextWakeMs, clamped)
  }
  for (const b of boxes) {
    if (b.metadata?.sc_owner !== opts.me || b.metadata?.sc_batch) continue
    const h = opts.holding.get(b.id)
    const isHeld = !!h
    const isLive = spare.has(b.id) || (h?.live ?? false)
    if (b.state !== 'suspended') {
      if (b.state === 'suspending') {
        if (isHeld) remainingHeld++
        else remainingPaused++
        bump(Math.min(idlePollMs, 5000))
      } else if (isHeld) {
        remainingHeld++
        bump(idlePollMs)
      }
      continue
    }
    if (isHeld) remainingHeld++
    else remainingPaused++
    if (isLive) {
      bump(idlePollMs)
      continue
    }
    const retryAt = opts.failedPulls?.get(b.id)
    if (isHeld && retryAt !== undefined && retryAt > now) {
      bump(retryAt - now)
      continue
    }
    const since = pausedSinceMs(b)
    const limitS = isHeld ? opts.holdKeep : opts.keep
    if (!Number.isFinite(since)) {
      bump(limitS * 1000)
      continue
    }
    // +100ms slack so when the timer wakes, Math.floor((Date.now() - since) / 1000) >= limitS on the first wake.
    const leftMs = since + limitS * 1000 - now + 100
    bump(leftMs)
  }
  return {remainingPaused, remainingHeld, nextWakeMs}
}

// One reap pass: delete every unheld paused box past `keep`, and for every non-live held paused box past `holdKeep`,
// pull its outputs into its batch's out dir first, then unhold and delete only when pulling succeeded.
export async function reapOnce(
  keep: number,
  log: (s: string) => void,
  spare: Set<string> = new Set(),
  o: ReapOptions = {},
): Promise<ReapReport> {
  const me = owner()
  const alive = o.alive ?? pidAlive
  const holdKeep = o.holdKeep ?? DEFAULT_HOLD_KEEP_S
  const now = o.nowMs ?? Date.now()
  const failedPulls = o.failedPulls ?? new Map<string, number>()
  const pool = await ib.list({sc: '1', sc_owner: me})
  const holding = heldMap(alive)
  const presentIds = new Set(pool.map((b) => b.id))
  // Drop orphan hold files whose boxes are already gone from instabox (and whose batch is not live).
  for (const [id, entry] of holding) {
    if (!presentIds.has(id) && !entry.live) {
      unhold(id)
      holding.delete(id)
      failedPulls.delete(id)
    }
  }
  const surviving: ib.Box[] = []
  let deleted = 0, pulled = 0, pullFailed = 0
  for (const b of pool) {
    if (b.metadata?.sc_owner !== me || b.metadata?.sc_batch) continue
    if (b.state !== 'suspended') {
      surviving.push(b)
      continue
    }
    const h = holding.get(b.id)
    const isHeld = !!h
    const isLive = spare.has(b.id) || (h?.live ?? false)
    const since = pausedSinceMs(b)
    const pausedFor = Number.isFinite(since) ? Math.max(0, Math.floor((now - since) / 1000)) : 0
    const act = decideLetGo({pausedFor, held: isHeld, live: isLive, pulled: false}, {keep, holdKeep})
    if (act === 'delete') {
      await ib.remove(b.id)
      log(`box ${b.name}: deleted (paused ${Math.round(pausedFor / 60)} min)`)
      deleted++
      continue
    }
    if (act === 'pull') {
      const retryAt = failedPulls.get(b.id)
      if (retryAt !== undefined && retryAt > now) {
        surviving.push(b)
        continue
      }
      const ok = h?.rec ? await pullHeldBox(b, h.rec, log, alive) : false
      const after = decideLetGo({pausedFor, held: isHeld, live: isLive, pulled: ok}, {keep, holdKeep})
      if (after === 'delete') {
        unhold(b.id)
        holding.delete(b.id)
        failedPulls.delete(b.id)
        await ib.remove(b.id)
        log(`box ${b.name}: unheld and deleted (hold expired after ${Math.round(pausedFor / 60)} min)`)
        pulled++
        deleted++
        continue
      }
      pullFailed++
      // Back off before retrying a failed pull (never busy-loop); server-side 24h backstop bounds total lifetime.
      failedPulls.set(b.id, now + Math.max(1000, Math.min(holdKeep, 1800) * 1000))
      surviving.push(b)
      continue
    }
    surviving.push(b)
  }
  const sched = computeNextWakeMs(surviving, {me, keep, holdKeep, holding, spare, nowMs: now, failedPulls})
  return {deleted, pulled, pullFailed, ...sched}
}

export async function reap(keep: number, log: (s: string) => void, spare: Set<string> = new Set(), o: ReapOptions = {}): Promise<number> {
  return (await reapOnce(keep, log, spare, o)).deleted
}

// Single-instance PID lock for the background reaper. Uses O_EXCL (`wx`) for atomic creation and a takeover directory
// lock to safely reclaim stale locks whose PID is dead.
export type ReaperLock = {pid: number; keep: number; holdKeep: number}

export function readReaperLock(): ReaperLock | undefined {
  const f = reaperLockPath()
  if (!existsSync(f)) return undefined
  try {
    const raw = readFileSync(f, 'utf8').trim()
    if (!raw) return undefined
    if (/^\d+$/.test(raw)) return {pid: Number(raw), keep: Number.POSITIVE_INFINITY, holdKeep: Number.POSITIVE_INFINITY}
    const d = JSON.parse(raw) as Partial<ReaperLock>
    if (typeof d.pid !== 'number') return undefined
    return {
      pid: d.pid,
      keep: typeof d.keep === 'number' ? d.keep : Number.POSITIVE_INFINITY,
      holdKeep: typeof d.holdKeep === 'number' ? d.holdKeep : Number.POSITIVE_INFINITY,
    }
  } catch {
    return undefined
  }
}

function writeReaperLockPayload(lock: ReaperLock) {
  const f = reaperLockPath()
  const tmp = `${f}.tmp.${process.pid}`
  writeFileSync(tmp, JSON.stringify(lock))
  renameSync(tmp, f)
}

export function acquireReaperLock(
  keep: number,
  holdKeep: number,
  pid = process.pid,
  deps: Pick<ReaperDeps, 'alive'> = {},
): {acquired: boolean; owner?: ReaperLock} {
  const alive = deps.alive ?? pidAlive
  mkdirSync(stateDir(), {recursive: true})
  const f = reaperLockPath()
  const payload = JSON.stringify({pid, keep, holdKeep})
  const takeover = `${f}.takeover`
  for (let attempt = 0; attempt < 20; attempt++) {
    try {
      writeFileSync(f, payload, {flag: 'wx'})
      return {acquired: true, owner: {pid, keep, holdKeep}}
    } catch (e: unknown) {
      if ((e as NodeJS.ErrnoException)?.code !== 'EEXIST') throw e
    }
    const cur = readReaperLock()
    if (cur && cur.pid !== pid && alive(cur.pid)) return {acquired: false, owner: cur}
    if (cur && cur.pid === pid) {
      writeReaperLockPayload({pid, keep, holdKeep})
      return {acquired: true, owner: {pid, keep, holdKeep}}
    }
    // Stale lock (dead PID or corrupt): take over atomically.
    try {
      mkdirSync(takeover)
    } catch {
      // Remove an abandoned takeover dir older than 10s; otherwise yield briefly and re-check.
      try {
        if (Date.now() - statSync(takeover).mtimeMs > 10_000) rmSync(takeover, {recursive: true, force: true})
      } catch {}
      Bun.sleepSync(25)
      continue
    }
    try {
      const again = readReaperLock()
      if (again && again.pid !== pid && alive(again.pid)) return {acquired: false, owner: again}
      rmSync(f, {force: true})
      writeFileSync(f, payload, {flag: 'wx'})
      return {acquired: true, owner: {pid, keep, holdKeep}}
    } finally {
      rmSync(takeover, {recursive: true, force: true})
    }
  }
  const cur = readReaperLock()
  return {acquired: false, owner: cur}
}

export function releaseReaperLock(pid = process.pid) {
  const cur = readReaperLock()
  if (cur?.pid === pid) rmSync(reaperLockPath(), {force: true})
}

function cliEntry(): string {
  const ts = join(import.meta.dir, 'cli.ts')
  return existsSync(ts) ? ts : process.argv[1]!
}

function spawnDetached(cmd: string[], logPath: string): number {
  const fd = openSync(logPath, 'a')
  try {
    const p = Bun.spawn(cmd, {stdin: 'ignore', stdout: fd, stderr: fd, detached: true, env: process.env})
    p.unref()
    return p.pid
  } finally {
    closeSync(fd)
  }
}

// Start ONE detached background reaper (`supercomputer reap --until-empty`) if one is not already alive. If a live
// reaper already exists, tighten its keep/holdKeep if needed and send SIGUSR1 so it recalculates its next wake.
export function ensureReaper(
  keep: number,
  holdKeep: number,
  log?: (s: string) => void,
  deps: Pick<ReaperDeps, 'alive' | 'spawn' | 'signal'> = {},
): {started: boolean; pid: number} {
  const alive = deps.alive ?? pidAlive
  const sendSignal = deps.signal ?? ((pid: number, sig: NodeJS.Signals) => { try { process.kill(pid, sig) } catch {} })
  mkdirSync(stateDir(), {recursive: true})
  const cur = readReaperLock()
  if (cur && alive(cur.pid)) {
    const nextKeep = Math.min(cur.keep, keep)
    const nextHold = Math.min(cur.holdKeep, holdKeep)
    if (nextKeep < cur.keep || nextHold < cur.holdKeep) {
      const want = reaperWantPath()
      const tmp = `${want}.tmp.${process.pid}`
      writeFileSync(tmp, JSON.stringify({keep: nextKeep, holdKeep: nextHold}))
      renameSync(tmp, want)
    }
    sendSignal(cur.pid, 'SIGUSR1')
    return {started: false, pid: cur.pid}
  }
  const cmd = [process.execPath, cliEntry(), 'reap', '--until-empty', '--keep', `${keep}s`, '--hold-keep', `${holdKeep}s`]
  const logPath = reaperLogPath()
  // Tests running CLI subprocesses set SC_REAPER_RECORD to capture spawn intent without starting a detached daemon.
  if (!deps.spawn && process.env.SC_REAPER_RECORD) {
    writeFileSync(process.env.SC_REAPER_RECORD, JSON.stringify({cmd, keep, holdKeep, logPath}) + '\n', {flag: 'a'})
    log?.(`background reaper recorded (log ${logPath})`)
    return {started: true, pid: 0}
  }
  const pid = (deps.spawn ?? spawnDetached)(cmd, logPath)
  log?.(`background reaper started (pid ${pid}, log ${logPath})`)
  return {started: true, pid}
}

// Background reaper loop (`supercomputer reap --until-empty`): holds the single-instance PID lock, sleeps until the
// earliest paused/held box's expiry, reaps, and exits when no paused or held box of this client is left.
export async function reapUntilEmpty(
  initialKeep: number,
  initialHoldKeep: number,
  log: (s: string) => void,
  deps: Pick<ReaperDeps, 'now' | 'sleep' | 'alive'> = {},
): Promise<number> {
  const now = deps.now ?? (() => Date.now())
  const alive = deps.alive ?? pidAlive
  let keep = initialKeep
  let holdKeep = initialHoldKeep
  const got = acquireReaperLock(keep, holdKeep, process.pid, {alive})
  if (!got.acquired) {
    log(`reaper already running (pid ${got.owner?.pid}); exiting`)
    return 0
  }
  let stopped = false
  const wake = {woke: false, onWake: undefined as (() => void) | undefined}
  const trigger = () => {
    wake.woke = true
    wake.onWake?.()
  }
  const onHup = () => {} // nohup: survive terminal hangup
  const onUsr1 = () => trigger()
  const onStop = () => {
    stopped = true
    trigger()
  }
  process.on('SIGHUP', onHup)
  process.on('SIGUSR1', onUsr1)
  process.on('SIGTERM', onStop)
  process.on('SIGINT', onStop)

  const readWant = () => {
    const f = reaperWantPath()
    if (!existsSync(f)) return
    try {
      const w = JSON.parse(readFileSync(f, 'utf8')) as Partial<{keep: number; holdKeep: number}>
      rmSync(f, {force: true})
      let changed = false
      if (typeof w.keep === 'number' && w.keep < keep) (keep = w.keep), (changed = true)
      if (typeof w.holdKeep === 'number' && w.holdKeep < holdKeep) (holdKeep = w.holdKeep), (changed = true)
      if (changed) writeReaperLockPayload({pid: process.pid, keep, holdKeep})
    } catch {}
  }

  const sleepUntil = async (waitMs: number) => {
    if (deps.sleep) {
      await deps.sleep(waitMs)
      return
    }
    const deadline = now() + waitMs
    while (!wake.woke && !stopped) {
      const left = deadline - now()
      if (left <= 0) return
      const chunk = Math.min(left, 60_000)
      await new Promise<void>((resolve) => {
        const t = setTimeout(resolve, chunk)
        wake.onWake = () => {
          clearTimeout(t)
          resolve()
        }
      })
      wake.onWake = undefined
    }
  }

  const failedPulls = new Map<string, number>()
  let totalDeleted = 0
  try {
    log(`reaper started (pid ${process.pid}, keep=${keep}s, hold-keep=${holdKeep}s)`)
    while (!stopped) {
      wake.woke = false
      readWant()
      let report: ReapReport
      try {
        report = await reapOnce(keep, log, new Set(), {holdKeep, nowMs: now(), failedPulls, alive})
      } catch (e) {
        log(`reap pass failed: ${e instanceof Error ? e.message : e}; retrying`)
        await sleepUntil(Math.max(1000, Math.min(keep, 30) * 1000))
        continue
      }
      totalDeleted += report.deleted
      if (report.remainingPaused === 0 && report.remainingHeld === 0) {
        log(`no paused or held boxes left (${totalDeleted} deleted); exiting`)
        return totalDeleted
      }
      const waitMs = report.nextWakeMs ?? Math.max(1000, Math.min(keep, holdKeep, 300) * 1000)
      log(`${report.remainingPaused} paused, ${report.remainingHeld} held; next wake in ${Math.max(1, Math.round(waitMs / 1000))} s`)
      await sleepUntil(waitMs)
    }
    return totalDeleted
  } finally {
    process.off('SIGHUP', onHup)
    process.off('SIGUSR1', onUsr1)
    process.off('SIGTERM', onStop)
    process.off('SIGINT', onStop)
    releaseReaperLock()
  }
}
