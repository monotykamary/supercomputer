// A batch on instabox: the I/O around the batch rule (src/batch.ts), which src/engine.ts runs. No controller: the
// boxes hold the work and this client only places it. Every act is first a request to the rule (add a job, open a
// box, start a job on it, take its exit, take its fetched outputs, retry or drop an attempt, close a box, finish), and
// the client acts only when the rule accepts it; nothing here decides on its own.
//
// - A job is a detached process on its box (setsid): its log, exit code and $SUPERCOMPUTER_OUTPUT_DIR under
//   ~/sc/run/<batch>/<job>. This client going away (a laptop closing) stops nothing: the box finishes the job, idles
//   and pauses itself (--auto-suspend), and `supercomputer batch fetch <out>` (or `run` again with the same --out)
//   picks the batch up from <out>/batch.json: the jobs and the log of requests the rule accepted, replayed.
// - A box with nothing on it that no queued job can start on (as the rule says) is paused into the warm pool; the next
//   batch resumes a paused box (seconds) before it creates one, provided its server-side delete deadline covers the
//   job's timeout plus a margin.

import {existsSync, mkdirSync} from 'node:fs'
import {join} from 'node:path'
import {Engine, type Limits, type Req} from './engine.ts'
import * as ib from './instabox.ts'
import {type Cache, type Config, type Image, type Job, type Size} from './jobs.ts'
import {DEFAULT_HOLD_KEEP_S, hasEnoughLifetime} from './pool.ts'
import {
  type Batch,
  type BoxRec,
  ensureReaper,
  heldIds,
  hold,
  owner,
  pullJobOutputs,
  readBatch,
  reap,
  reapOnce,
  saveBatch,
  shaFile,
  unhold,
  writeSummary,
} from './reaper.ts'
import {type Locked, planSize, refuseUnfit, SYSTEM_MIB} from './size.ts'
import type {Tree} from './tree.ts'

export type {Limits} from './engine.ts'
export type {Batch} from './reaper.ts'
export {owner, readBatch, reap} from './reaper.ts'
export {SYSTEM_MIB} from './size.ts'

export type Options = {
  out: string
  poll: number // seconds
  keep: number // seconds a paused box stays in the warm pool
  holdKeep?: number // seconds a held box whose batch has ended stays before outputs are pulled and it is deleted
  log: (line: string) => void
}

// A box as this client holds it: missed polls, and when it is ready for jobs.
type Live = BoxRec & {misses: number; ready: Promise<void>}

const q = (s: string) => `'${s.replaceAll("'", `'\\''`)}'`
const sleep = (s: number) => new Promise((r) => setTimeout(r, s * 1000))

// What a box must be for the job: its image, its GPU and, when it names one, its size.
export const kindOf = (j: Job) => [j.image ?? '', j.gpu ?? '', j.size ? `${j.size.vcpus}x${j.size.memMiB}` : ''].join('|')

// At most n instabox calls at once.
function limiter(n: number) {
  let active = 0
  const waiting: (() => void)[] = []
  return async <T>(f: () => Promise<T>): Promise<T> => {
    while (active >= n) await new Promise<void>((r) => waiting.push(r))
    active++
    try {
      return await f()
    } finally {
      active--
      waiting.shift()?.()
    }
  }
}

export function newBatch(jobs: Job[], cfg: Config, tree: Tree, limits: Limits, locked: Locked = {}): Batch {
  for (const j of jobs) {
    const img = j.image ? cfg.images[j.image] : undefined
    if (j.image && !img) throw new Error(`${j.name}: no image ${j.image} in supercomputer.toml's [instabox.images]`)
    // (an image of its own setup has no snapshot until it is built: its boxes would be the bare template)
    if (img?.setup && !img.snapshot) throw new Error(`${j.name}: image ${j.image} has no snapshot yet: \`supercomputer image build ${j.image}\`, then set its snapshot in supercomputer.toml`)
  }
  // (a job no box can hold, its image's locked size or any size instabox has: refused now, never lost later)
  refuseUnfit(jobs, cfg, locked, limits.timing)
  const id = `${new Date().toISOString().replace(/[-:]/g, '').replace('T', '-').slice(0, 15)}-${Math.random().toString(36).slice(2, 8)}`
  if (limits.strictOnce) limits = {...limits, maxAttempts: 1}
  const groups = [...new Set(jobs.flatMap((j) => (j.group ? [j.group] : [])))]
  const kinds = [...new Set(jobs.map(kindOf))]
  const engine = new Engine(limits)
  for (const [i, j] of jobs.entries()) {
    const req: Req = {op: 'add', job: i, cpu: Math.ceil(j.cpu), mem: j.memory, group: j.group ? groups.indexOf(j.group) + 1 : 0, kind: kinds.indexOf(kindOf(j)) + 1}
    if (!engine.step(req)) throw new Error(`${j.name}: the batch rule refused it`)
  }
  const caches = cfg.caches.map((c) => ({...c, key: cacheKey(tree.root, c)}))
  return {id, owner: owner(), tree: tree.hash, caches, cfg, limits, jobs, groups, kinds, boxes: [], log: engine.log, times: {}, nextSlot: 0, locked}
}

// A cache's key: its salt and every input file's path and content (a missing one as such).
export function cacheKey(root: string, c: Cache): string {
  const h = new Bun.CryptoHasher('sha256').update(c.salt + '\0')
  for (const f of [...c.inputs].sort()) {
    const file = join(root, c.dir, f)
    h.update(f + '\0' + (existsSync(file) ? shaFile(file) : '-') + '\0')
  }
  return h.digest('hex').slice(0, 16)
}

// Run (or pick up) the batch in `out` until the rule gives it a verdict; 0 only when it passed.
export async function run(batch: Batch, tree: Tree | undefined, o: Options): Promise<number> {
  mkdirSync(o.out, {recursive: true})
  const holdKeep = o.holdKeep ?? DEFAULT_HOLD_KEEP_S
  const rule = new Engine(batch.limits, batch.log)
  const timing = batch.limits.timing
  const jobs = batch.jobs
  const live = new Map<number, Live>()
  const recs = () => (batch.boxes = [...live.values()].map(({slot, id, name, size, gpu, image, tree, deleteAt}) => ({slot, id, name, size, gpu, image, tree, deleteAt})))
  const save = () => {
    batch.log = rule.log
    saveBatch(o.out, batch)
  }
  // A request to the rule, kept in batch.json when accepted.
  const ask = (r: Req) => {
    const yes = rule.step(r)
    if (yes) save()
    return yes
  }
  const time = (n: string) => (batch.times[n] ??= {})
  const imageOf = (name?: string): Image => (name ? batch.cfg.images[name]! : batch.cfg.image)
  const imageKey = (name?: string) => {
    const i = imageOf(name)
    return (i.snapshot ?? `template:${i.template}`).replace(/[^A-Za-z0-9._-]/g, '_')
  }
  const api = limiter(16)
  const meta = (image?: string) => ({sc: '1', sc_owner: batch.owner, sc_image: imageKey(image)})
  const dir = (name: string) => `~/sc/run/${batch.id}/${name}`
  const on = (b: Live, st: string) => rule.jobs().filter(([, j]) => j.box === b.slot && j.st === st).map(([i]) => i)
  let pausedAny = false

  const sendTree = async (b: Live) => {
    if (b.tree === batch.tree) return
    if (!tree) throw new Error(`box ${b.name} lacks tree ${batch.tree}; pick the batch up from its repo to send it again`)
    const has = await api(() => ib.exec(b.id, `mkdir -p ~/sc/trees ~/sc/run && test -s ~/sc/trees/${tree.hash}.tgz`, {idempotent: true}))
    if (has.exitCode !== 0) await api(() => ib.push(b.id, tree.tarball, `/data/home/sc/trees/${tree.hash}.tgz`))
    b.tree = tree.hash
    recs()
    save()
  }

  // An attempt with no native answer: tried again elsewhere if the rule gives it another, else lost.
  const lose = (b: Live, i: number) => {
    const again = ask({op: 'retry', job: i})
    if (!again && !ask({op: 'drop', job: i})) return
    o.log(`${jobs[i]!.name}: lost on ${b.name}${again ? ', queued again' : ' (no attempt left)'}`)
  }

  // A box found gone (or never made): its jobs' attempts lost, then the box closed.
  const gone = (b: Live) => {
    for (const [i, j] of rule.jobs()) if (j.box === b.slot && (j.st === 'running' || j.st === 'ended')) lose(b, i)
    if (!ask({op: 'close', box: b.slot})) return
    live.delete(b.slot)
    unhold(b.id)
    recs()
    save()
  }

  // A box this batch made that never became ready (its create or its tree failed): deleted, so it is never left
  // running or paused with nothing to show for it. Known by its id, else by its name (a create that did not answer).
  const discard = async (b: Live) => {
    const id = b.id || (await api(() => ib.get(b.name)).then((x) => (x.metadata?.sc_owner === batch.owner ? x.id : ''), () => ''))
    if (id) await api(() => ib.remove(id)).then(() => o.log(`box ${b.name}: deleted (it never became ready)`), () => undefined)
  }

  // A GPU job's box, as instabox makes it (asked once per type): one the job doesn't fit in is the job's error.
  const gpus = new Map<string, Promise<ib.GpuBox>>()
  const gpuBox = async (job: Job): Promise<ib.GpuBox> => {
    if (!gpus.has(job.gpu!)) gpus.set(job.gpu!, api(() => ib.gpuBox(job.gpu!)))
    const g = await gpus.get(job.gpu!)!
    if (job.cpu > g.vcpus || job.memory > g.memMiB - SYSTEM_MIB)
      throw new Error(`${job.name}: a ${job.gpu} box has ${g.vcpus} vCPUs and ${g.memMiB} MiB, less than the job asks`)
    return g
  }

  // A box for this job: a paused one of the warm pool of its size, image and sufficient remaining lifetime, else a new
  // one; only in a slot the rule opens for it.
  const acquire = async (i: number): Promise<Live | undefined> => {
    const job = jobs[i]!
    // (a GPU box's size is the GPU type's, not one asked for)
    const gpu = job.gpu ? await gpuBox(job) : undefined
    const size: Size = gpu ?? planSize(job, batch.cfg, batch.locked ?? {}, timing)
    const slot = batch.nextSlot
    if (!ask({op: 'open', box: slot, cpu: size.vcpus, mem: size.memMiB - SYSTEM_MIB, kind: batch.kinds.indexOf(kindOf(job)) + 1})) return undefined
    batch.nextSlot++
    let warm: ib.Box | undefined
    if (!timing && !job.gpu) {
      const pool = await api(() => ib.list(meta(job.image))).catch(() => [] as ib.Box[])
      const taken = new Set([...live.values()].map((b) => b.id))
      const holding = heldIds()
      warm = pool.find(
        (b) =>
          b.state === 'suspended' &&
          b.vcpus === size.vcpus &&
          b.memMiB === size.memMiB &&
          !taken.has(b.id) &&
          !holding.has(b.id) &&
          hasEnoughLifetime(b.deleteAt, job.timeout),
      )
    }
    const name = warm?.name ?? `sc-${batch.owner}-${Math.random().toString(36).slice(2, 10)}`
    let resolve!: () => void, reject!: (e: unknown) => void
    const ready = new Promise<void>((a, z) => ((resolve = a), (reject = z)))
    const b: Live = {slot, id: warm?.id ?? '', name, size, gpu: job.gpu, image: job.image, deleteAt: warm?.deleteAt ?? undefined, misses: 0, ready}
    live.set(slot, b)
    recs()
    save()
    hold(b.id, batch.id, o.out)
    let made = false
    ;(async () => {
      if (warm) {
        o.log(`box ${name}: resumed from the warm pool`)
        // (a warm box that won't resume, deleted meanwhile or broken: a new box in its place, the slot kept)
        const back = await api(() => ib.resume(warm!.id)).then(() => true, (e) => {
          o.log(`box ${name}: not resumed (${e instanceof Error ? e.message : e}); a new box instead`)
          return false
        })
        if (!back) {
          unhold(b.id)
          b.id = ''
          b.deleteAt = undefined
          b.name = `sc-${batch.owner}-${Math.random().toString(36).slice(2, 10)}`
          recs()
          save()
        }
      }
      if (!b.id) {
        o.log(`box ${b.name}: creating ${size.vcpus} vCPU / ${size.memMiB / 1024} GiB${job.gpu ? ` with ${job.gpu}` : ''}`)
        const img = imageOf(job.image)
        made = true
        const box = await api(() =>
          ib.create({
            name: b.name,
            vcpus: size.vcpus,
            memMiB: size.memMiB,
            snapshot: gpu ? undefined : img.snapshot,
            template: gpu?.template ?? img.template,
            gpu: job.gpu,
            meta: meta(job.image),
            deleteAfter: ib.DEFAULT_DELETE_AFTER,
          }),
        )
        b.id = box.id
        b.deleteAt = box.deleteAt ?? undefined
        hold(b.id, batch.id, o.out)
        recs()
        save()
      }
      await sendTree(b)
    })().then(resolve, (e) => {
      o.log(`box ${b.name}: ${e instanceof Error ? e.message : e}`)
      reject(e)
    })
    ready.catch(async () => {
      if (made) await discard(b)
      gone(b)
    })
    return b
  }

  const start = async (b: Live, i: number) => {
    const job = jobs[i]!
    if (!ask({op: 'start', job: i, box: b.slot})) return
    batch.times[job.name] = {started: Date.now() / 1000, where: b.name}
    save()
    const {script, launch, claimed} = jobScript(batch, job)
    try {
      await b.ready
      // (made again through a transport failure: the launch is idempotent, its job claimed once on the box)
      const r = await api(() => ib.exec(b.id, launch, {stdin: script, idempotent: true}))
      if (r.exitCode !== 0) throw new Error(`launch exited ${r.exitCode}: ${r.stderr.trim()}`)
      o.log(`${job.name}: started on ${b.name}`)
    } catch (e) {
      // (a launch call that never answered may still have started the job: the box's claim says whether it did)
      const ran = !!b.id && e instanceof ib.InstaboxError && ib.transportFailure({stdout: '', stderr: e.stderr, code: e.code}) &&
        (await sleep(2).then(() => api(() => ib.exec(b.id, claimed, {idempotent: true}))).then((x) => x.exitCode === 0, () => false))
      if (ran) return o.log(`${job.name}: started on ${b.name} (its launch call failed after the job claimed its start)`)
      if (rule.job(i)?.st !== 'running' || rule.job(i)?.box !== b.slot) return // (answered meanwhile: it had started)
      o.log(`${job.name}: not started on ${b.name}: ${e instanceof Error ? e.message : e}`)
      lose(b, i)
    }
  }

  const collect = async (b: Live, i: number) => {
    const job = jobs[i]!
    const J = dir(job.name)
    const pulled = await pullJobOutputs(b.id, batch.id, job.name, o.out, api)
    // (its folder gone from the box: the attempt's answer is gone with it)
    if (pulled.status === 'missing') return lose(b, i)
    const t = time(job.name)
    t.seconds = Math.round((t.ended ?? Date.now() / 1000) - (Number.isFinite(pulled.started) ? pulled.started : t.started!))
    // (the answer kept in batch.json before its folder on the box goes)
    if (!ask({op: 'fetch', job: i})) return
    o.log(`${job.name}: exit ${rule.job(i)!.exit} in ${t.seconds} s${pulled.count ? `, ${pulled.count} output files` : ''}`)
    await api(() => ib.exec(b.id, `rm -rf ${J} ${J}.lock`, {idempotent: true})).catch(() => undefined)
  }

  // A box's running jobs in one call (the ones that wrote their exit code), then the outputs of every ended one.
  const poll = async (b: Live) => {
    const running = on(b, 'running')
    // (asked even with only outputs on the box: a box gone with them is found gone, its attempts lost)
    if (running.length || on(b, 'ended').length) {
      const script = running.map((i) => `[ -f ${dir(jobs[i]!.name)}/exit ] && echo ${i} "$(cat ${dir(jobs[i]!.name)}/exit)"`).join('; ') + '; true'
      let r: ib.Exec
      try {
        r = await api(() => ib.exec(b.id, script, {timeout: 60, idempotent: true}))
        b.misses = 0
      } catch {
        if (++b.misses < 4) return
        const state = await api(() => ib.get(b.id)).then((x) => x.state, () => 'gone')
        if (state === 'suspended' || state === 'stopped') {
          await api(async () => { if (state === 'suspended') await ib.resume(b.id); else await ib.exec(b.id, 'true', {idempotent: true}) }).catch(() => undefined)
          b.misses = 0
          return
        }
        if (state === 'running' || state === 'starting' || state === 'resuming') return
        o.log(`box ${b.name}: ${state}`)
        gone(b)
        return
      }
      for (const line of r.stdout.trim().split('\n').filter(Boolean)) {
        const [i, code, ended] = line.split(' ').map(Number) as [number, number, number?]
        if (!ask({op: 'exit', job: i, code: Number.isInteger(code) && code! >= 0 ? code : 255})) continue
        if (Number.isFinite(ended)) time(jobs[i]!.name).ended = ended
        save()
      }
    }
    await Promise.all(on(b, 'ended').map((i) => collect(b, i).catch((e) => {
      o.log(`${jobs[i]!.name}: outputs not fetched yet: ${e instanceof Error ? e.message : e}`)
    })))
  }

  const pause = async (b: Live) => {
    if (!ask({op: 'close', box: b.slot})) return
    live.delete(b.slot)
    unhold(b.id)
    recs()
    save()
    // (a GPU box is deleted, not paused: no batch resumes one, and paused it would keep its 200 GiB disk until a reap)
    if (b.id && b.gpu) await api(() => ib.remove(b.id)).then(() => o.log(`box ${b.name}: deleted (a GPU box is not kept)`), () => undefined)
    else if (b.id) {
      pausedAny = true
      await api(() => ib.suspend(b.id)).then(() => o.log(`box ${b.name}: paused into the warm pool`), () => undefined)
    }
  }

  // The boxes this batch held before (a batch picked up): theirs again, resumed if they paused meanwhile.
  for (const rec of batch.boxes) {
    const b: Live = {...rec, misses: 0, ready: Promise.resolve()}
    live.set(rec.slot, b)
    hold(rec.id, batch.id, o.out)
    // (no id: its create never answered, so no job was launched there; a box it made after all is deleted)
    if (!rec.id) await discard(b)
    const info = rec.id ? await api(() => ib.get(rec.id)).catch(() => undefined) : undefined
    const state = info?.state ?? 'gone'
    if (info?.deleteAt) b.deleteAt = info.deleteAt
    if (state === 'gone' || state === 'error' || state === 'deleted') {
      o.log(`box ${rec.name}: ${state}`)
      gone(b)
      continue
    }
    if (state === 'suspended') {
      b.ready = api(() => ib.resume(rec.id)).then(() => undefined)
      b.ready.catch(() => gone(b))
    }
  }
  // (a slot the rule holds open that no record names, a box whose create never answered: its jobs' attempts lost)
  for (const [slot] of rule.boxes())
    if (!live.has(slot)) gone({slot, id: '', name: `slot ${slot}`, size: {vcpus: 0, memMiB: 0}, misses: 0, ready: Promise.resolve()})
  recs()
  save()
  // (the warm pool reaps itself: this owner's paused boxes past their keep, none this or another live batch holds)
  const before = await reapOnce(o.keep, o.log, new Set(batch.boxes.map((b) => b.id)), {holdKeep}).catch(() => undefined)
  if (before && (before.remainingPaused > 0 || before.remainingHeld > 0)) ensureReaper(o.keep, holdKeep, o.log)
  const out = () => rule.count('queued') + rule.count('running') + rule.count('ended')
  o.log(`batch ${batch.id}: ${jobs.length} jobs, ${rule.count('done')} answered, ${rule.count('queued')} queued, ${rule.count('running') + rule.count('ended')} out; ${imageKey()}`)

  let finished = false
  try {
    // Place, poll, until nothing is left to run or fetch.
    while (out() > 0) {
      for (const [i, j] of rule.jobs()) {
        if (j.st !== 'queued') continue
        const b =
          [...live.values()].find((b) => hasEnoughLifetime(b.deleteAt, jobs[i]!.timeout) && rule.would({op: 'start', job: i, box: b.slot})) ??
          (await acquire(i))
        if (!b) break // the rule opens no box: at the cap, the rest wait for a slot
        void start(b, i)
      }
      // A box with nothing on it that no queued job can start on goes back to the pool.
      const queued = rule.jobs().filter(([, j]) => j.st === 'queued').map(([i]) => i)
      for (const b of [...live.values()]) {
        const pb = rule.box(b.slot)
        if (!b.id || !pb || pb.running || pb.unfetched) continue
        if (!queued.some((i) => hasEnoughLifetime(b.deleteAt, jobs[i]!.timeout) && rule.would({op: 'start', job: i, box: b.slot}))) await pause(b)
      }
      if (out() === 0) break
      await sleep(o.poll)
      await Promise.all([...live.values()].filter((b) => b.id).map((b) => b.ready.then(() => poll(b), () => undefined)))
    }
    await Promise.all([...live.values()].map((b) => pause(b)))
    ask({op: 'finish'})

    // The summary: name, exit (or lost), seconds, the box it ran on.
    writeSummary(o.out, batch, rule)
    const failed = rule.jobs().filter(([, j]) => j.st === 'done' && j.exit !== 0).length
    o.log(`batch ${batch.id}: ${rule.verdict}: ${rule.count('done') - failed} passed, ${failed} failed, ${rule.count('lost')} lost; ${join(o.out, 'summary.tsv')}`)
    finished = true
    const after = await reapOnce(o.keep, o.log, new Set(), {holdKeep}).catch(() => undefined)
    if ((after && (after.remainingPaused > 0 || after.remainingHeld > 0)) || (!after && pausedAny)) ensureReaper(o.keep, holdKeep, o.log)
    return rule.verdict === 'passed' ? 0 : 3
  } finally {
    if (!finished && (live.size > 0 || pausedAny)) ensureReaper(o.keep, holdKeep, o.log)
  }
}

// A job's script on its box, and the command that starts it detached (setsid) with the script on its stdin: it unpacks
// the tree, seeds the caches, runs the command under its timeout, keeps what it built, and writes its exit code last.
export function jobScript(batch: Pick<Batch, 'id' | 'tree' | 'cfg' | 'caches'>, job: Job, attempt?: number): {script: string; launch: string; claimed: string} {
  const J = `~/sc/run/${batch.id}/${job.name}${attempt ? `.${attempt}` : ''}`
  const img = job.image ? batch.cfg.images[job.image]! : batch.cfg.image
  const path = img.path.map((p) => p.replace(/^~/, '$HOME')).join(':')
  const env = Object.entries({...img.env, ...job.env}).map(([k, v]) => `export ${k}=${q(String(v))}`).join('\n')
  // Each cache: its outputs copied into the tree when the box has them for this key; kept after the job when it
  // built all of them and the box has none yet.
  const caches = batch.caches.map((c) => {
    const C = `"$HOME/sc/cache/${c.name}/${c.key}"`, D = `"$J/tree"/${q(c.dir)}`
    const outs = c.outputs.map(q).join(' ')
    return {
      seed: `[ -d ${C} ] && cp -a ${C}/. ${D}/ 2>/dev/null`,
      keep: `( cd ${D} 2>/dev/null && [ ! -d ${C} ] && ls ${outs} >/dev/null 2>&1 && T=${C}.$$ && mkdir -p "$T" && cp --parents ${outs} "$T"/ && mv -T "$T" ${C} 2>/dev/null || rm -rf "$T" ) || true`,
    }
  })
  // The launch's own token: a launch made again with it (its call failed in transport) keeps what the first one
  // wrote, and the job's claim (an atomic mkdir, its first act) lets one start through, ever; a new launch (another
  // token, a new attempt) starts the folder over. The lock keeps two launches of the job on the box from interleaving.
  const T = Math.random().toString(36).slice(2, 12)
  const D = J.replace(/^~/, '$HOME')
  const script = [
    'set -u',
    `J=${D}`,
    'mkdir "$J/claim" 2>/dev/null || exit 0',
    'echo $$ > "$J/pid"',
    `tar -xzf "$HOME/sc/trees/${batch.tree}.tgz" -C "$J/tree" || { echo "the tree did not unpack" > "$J/log"; echo 125 > "$J/exit"; exit; }`,
    `cd "$J/tree"/${q(job.cwd)} || { echo "no ${job.cwd} in the tree" > "$J/log"; echo 126 > "$J/exit"; exit; }`,
    path ? `export PATH="${path}:$PATH"` : '',
    env,
    `export SUPERCOMPUTER_OUTPUT_DIR="$J/out" SUPERCOMPUTER_JOB=${q(job.name)} SUPERCOMPUTER_BATCH=${q(batch.id)}`,
    ...caches.map((c) => c.seed),
    'date +%s > "$J/started"',
    `timeout -k 30 ${job.timeout} bash -c ${q(batch.cfg.pre + job.cmd)} > "$J/log" 2>&1 < /dev/null`,
    'code=$?',
    ...caches.map((c) => c.keep),
    'echo "$code $(date +%s)" > "$J/exit.tmp" && mv "$J/exit.tmp" "$J/exit"',
  ].filter(Boolean).join('\n') + '\n'
  const launch = `J=${D}; T=${T}; mkdir -p "$(dirname "$J")" && exec 9>"$J.lock" && { ! command -v flock >/dev/null || flock 9; } &&` +
    ` { [ "$(cat "$J/launch" 2>/dev/null)" = "$T" ] || { rm -rf "$J" && mkdir -p "$J/tree" "$J/out" && echo "$T" > "$J/launch"; }; } &&` +
    ` cat > "$J/job.sh.$$" && mv "$J/job.sh.$$" "$J/job.sh" && (setsid nohup bash "$J/job.sh" >> "$J/runner.log" 2>&1 < /dev/null 9>&- &)`
  const claimed = `J=${D}; test -d "$J/claim" && [ "$(cat "$J/launch" 2>/dev/null)" = ${T} ]`
  return {script, launch, claimed}
}

// Each image the jobs use, its locked size, if its snapshot was saved with sizeLocked: asked of instabox once per
// batch. A snapshot instabox doesn't have is refused now, not when its first box can't be made.
export async function lockedSizes(cfg: Config, jobs: Job[]): Promise<Locked> {
  const used = new Set(jobs.filter((j) => !j.gpu).map((j) => j.image ?? ''))
  const want = [...used].map((name) => ({name, snap: (name ? cfg.images[name] : cfg.image)?.snapshot})).filter((x): x is {name: string; snap: string} => !!x.snap)
  if (!want.length) return {}
  const all = await ib.snapshots()
  const locked: Locked = {}
  for (const {name, snap} of want) {
    const s = all.find((x) => x.name === snap)
    if (!s) throw new Error(`${name ? `image ${name}` : "the repo's image"}: instabox has no snapshot ${snap} (instabox snapshot ls)`)
    if (s.sizeLocked && s.defaultSpec) locked[name] = {vcpus: s.defaultSpec.vcpus, memMiB: s.defaultSpec.memMiB}
  }
  return locked
}
