// A batch on instabox. No controller: the boxes hold the work and this client only places it.
//
// - A job is a detached process on a box (setsid), its log, exit code and $SUPERCOMPUTER_OUTPUT_DIR under
//   ~/sc/run/<batch>/<job>. This client going away (a laptop closing) stops nothing: the box finishes the job,
//   idles and pauses itself (--auto-suspend), and `run` again with the same --out picks the batch up from
//   <out>/batch.json, fetching what finished meanwhile.
// - Jobs pack onto a box by its vCPUs and memory, as boat's slots did; a box takes the next queued job that fits
//   once a slot frees, and pauses into the warm pool when no queued job can use it. The next batch resumes a
//   paused box (seconds) before it creates one.
// - What it decides is src/boxes.ts and src/decide.ts, proven: at most --max-boxes live; only a lost attempt
//   retried, within its budget, never strict-once; a box reused only once its job's outputs are fetched and
//   verified; a paused box deleted only past its keep and never with outputs on it.

import {existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync} from 'node:fs'
import {hostname, tmpdir, userInfo} from 'node:os'
import {join} from 'node:path'
import {Boxes} from './boxes.ts'
import {letGo, retry, reusable, succeeded} from './decide.ts'
import * as ib from './instabox.ts'
import {type Config, fits, type Job, type Size} from './jobs.ts'
import type {Tree} from './tree.ts'

export type Options = {
  out: string
  maxBoxes: number
  maxAttempts: number
  strictOnce: boolean
  timing: boolean
  poll: number // seconds
  keep: number // seconds a paused box stays in the warm pool
  log: (line: string) => void
}

type JobState = {
  status: 'queued' | 'running' | 'done' | 'lost'
  attempts: number
  box?: string
  where?: string // the box's name
  started?: number
  seconds?: number
  exit?: number
}

type BoxRec = {id: string; name: string; size: Size; gpu?: string; tree?: string}

type Batch = {id: string; owner: string; image: string; tree: string; jobs: Record<string, JobState>; boxes: Record<string, BoxRec>}

type Live = BoxRec & {cpu: number; mem: number; running: Set<string>; groups: Set<string>; misses: number; ready: Promise<void>}

const q = (s: string) => `'${s.replaceAll("'", `'\\''`)}'`
const sleep = (s: number) => new Promise((r) => setTimeout(r, s * 1000))

export const owner = () => new Bun.CryptoHasher('sha256').update(`${userInfo().username}@${hostname()}`).digest('hex').slice(0, 8)

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

export async function run(jobs: Job[], cfg: Config, tree: Tree, image: {snapshot?: string; template: string}, o: Options): Promise<number> {
  mkdirSync(o.out, {recursive: true})
  const file = join(o.out, 'batch.json')
  const byName = new Map(jobs.map((j) => [j.name, j]))
  const imageKey = image.snapshot ?? `template:${image.template}`
  let batch: Batch
  if (existsSync(file)) {
    batch = JSON.parse(readFileSync(file, 'utf8')) as Batch
    for (const j of jobs) batch.jobs[j.name] ??= {status: 'queued', attempts: 0}
    batch.tree = tree.hash
    o.log(`batch ${batch.id}: picked up from ${file}`)
  } else {
    const id = `${new Date().toISOString().replace(/[-:]/g, '').replace('T', '-').slice(0, 15)}-${Math.random().toString(36).slice(2, 8)}`
    batch = {id, owner: owner(), image: imageKey, tree: tree.hash, jobs: Object.fromEntries(jobs.map((j) => [j.name, {status: 'queued', attempts: 0}])), boxes: {}}
    o.log(`batch ${id}: ${jobs.length} jobs, tree ${tree.hash} (${tree.files} files, ${(tree.bytes / 1e6).toFixed(1)} MB), ${imageKey}`)
  }
  const save = () => writeFileSync(file, JSON.stringify(batch, null, 1))
  save()

  const api = limiter(16)
  const boxes = new Boxes(o.maxBoxes)
  const live = new Map<string, Live>()
  const meta = {sc: '1', sc_owner: batch.owner, sc_image: imageKey.replace(/[^A-Za-z0-9._-]/g, '_')}
  const dir = (name: string) => `~/sc/run/${batch.id}/${name}`

  const sendTree = async (b: Live) => {
    if (b.tree === tree.hash) return
    const has = await api(() => ib.exec(b.id, `mkdir -p ~/sc/trees ~/sc/run && test -s ~/sc/trees/${tree.hash}.tgz`))
    if (has.exitCode !== 0) await api(() => ib.push(b.id, tree.tarball, `/data/home/sc/trees/${tree.hash}.tgz`))
    b.tree = tree.hash
    batch.boxes[b.id] = {id: b.id, name: b.name, size: b.size, gpu: b.gpu, tree: b.tree}
    save()
  }

  const adopt = (rec: BoxRec, ready: Promise<void>): Live => {
    const b: Live = {...rec, cpu: 0, mem: 0, running: new Set(), groups: new Set(), misses: 0, ready}
    live.set(rec.id, b)
    batch.boxes[rec.id] = rec
    save()
    return b
  }

  // A box for this job: a paused one of the warm pool of the same size and image, else a new one.
  const acquire = async (job: Job): Promise<Live | undefined> => {
    if (!boxes.open()) return undefined
    const size = job.size ?? (o.timing ? fits(job) : biggest(fits(job), cfg.size))
    let warm: ib.Box | undefined
    if (!o.timing && !job.gpu) {
      const pool = await api(() => ib.list(meta)).catch(() => [] as ib.Box[])
      warm = pool.find((b) => b.state === 'suspended' && b.vcpus === size.vcpus && b.memMiB === size.memMiB && !live.has(b.id))
    }
    const name = warm?.name ?? `sc-${batch.owner}-${Math.random().toString(36).slice(2, 10)}`
    const placeholder = `pending:${name}`
    let resolve!: () => void, reject!: (e: unknown) => void
    const ready = new Promise<void>((a, b) => ((resolve = a), (reject = b)))
    const b = adopt({id: warm?.id ?? placeholder, name, size, gpu: job.gpu}, ready)
    ;(async () => {
      if (warm) {
        o.log(`box ${name}: resumed from the warm pool`)
        await api(() => ib.resume(warm!.id))
      } else {
        o.log(`box ${name}: creating ${size.vcpus} vCPU / ${size.memMiB / 1024} GiB${job.gpu ? ` with ${job.gpu}` : ''}`)
        const made = await api(() => ib.create({name, vcpus: size.vcpus, memMiB: size.memMiB, snapshot: image.snapshot, template: job.gpu ? 'gpu' : image.template, gpu: job.gpu, meta}))
        live.delete(placeholder)
        delete batch.boxes[placeholder]
        b.id = made.id
        live.set(made.id, b)
        batch.boxes[made.id] = {id: made.id, name, size, gpu: job.gpu}
        save()
      }
      await sendTree(b)
    })().then(resolve, (e) => {
      o.log(`box ${name}: ${e instanceof Error ? e.message : e}`)
      reject(e)
    })
    ready.catch(() => {
      live.delete(b.id)
      delete batch.boxes[b.id]
      boxes.close()
      save()
    })
    return b
  }

  const fitsOn = (b: Live, job: Job) =>
    (b.gpu ?? '') === (job.gpu ?? '') &&
    (!job.size || (b.size.vcpus === job.size.vcpus && b.size.memMiB === job.size.memMiB)) &&
    b.cpu + Math.ceil(job.cpu) <= b.size.vcpus &&
    b.mem + job.memory <= b.size.memMiB - 512 &&
    (!job.group || !b.groups.has(job.group)) &&
    (!o.timing || b.groups.size + b.running.size === 0)

  const take = (b: Live, job: Job) => {
    b.cpu += Math.ceil(job.cpu)
    b.mem += job.memory
    b.running.add(job.name)
    if (job.group) b.groups.add(job.group)
    if (o.timing) b.groups.add(`timing:${job.name}`)
  }
  const free = (b: Live, job: Job) => {
    b.cpu -= Math.ceil(job.cpu)
    b.mem -= job.memory
    b.running.delete(job.name)
  }

  const start = async (b: Live, job: Job) => {
    const st = batch.jobs[job.name]!
    st.status = 'running'
    st.attempts++
    st.started = Date.now() / 1000
    save()
    try {
      await b.ready
      st.box = b.id
      st.where = b.name
      save()
      const J = dir(job.name)
      const path = cfg.path.map((p) => p.replace(/^~/, '$HOME')).join(':')
      const env = Object.entries({...cfg.env, ...job.env}).map(([k, v]) => `export ${k}=${q(String(v))}`).join('\n')
      const script = [
        'set -u',
        `J=${J.replace(/^~/, '$HOME')}`,
        `tar -xzf "$HOME/sc/trees/${tree.hash}.tgz" -C "$J/tree" || { echo "the tree did not unpack" > "$J/log"; echo 125 > "$J/exit"; exit; }`,
        `cd "$J/tree"/${q(job.cwd)} || { echo "no ${job.cwd} in the tree" > "$J/log"; echo 126 > "$J/exit"; exit; }`,
        path ? `export PATH="${path}:$PATH"` : '',
        env,
        `export SUPERCOMPUTER_OUTPUT_DIR="$J/out" SUPERCOMPUTER_JOB=${q(job.name)} SUPERCOMPUTER_BATCH=${q(batch.id)}`,
        'date +%s > "$J/started"',
        `timeout -k 30 ${job.timeout} bash -c ${q(cfg.pre + job.cmd)} > "$J/log" 2>&1 < /dev/null`,
        'echo "$? $(date +%s)" > "$J/exit.tmp" && mv "$J/exit.tmp" "$J/exit"',
      ].filter(Boolean).join('\n')
      const launch = `J=${J.replace(/^~/, '$HOME')}; rm -rf "$J" && mkdir -p "$J/tree" "$J/out" && cat > "$J/job.sh" && (setsid nohup bash "$J/job.sh" > "$J/runner.log" 2>&1 < /dev/null &)`
      const r = await api(() => ib.exec(b.id, launch, {stdin: script + '\n'}))
      if (r.exitCode !== 0) throw new Error(`launch exited ${r.exitCode}: ${r.stderr.trim()}`)
      o.log(`${job.name}: started on ${b.name}`)
    } catch (e) {
      o.log(`${job.name}: not started on ${b.name}: ${e instanceof Error ? e.message : e}`)
      lost(b, job)
    }
  }

  // An attempt that got no native answer: tried again elsewhere while retry says so, else terminal.
  const lost = (b: Live, job: Job) => {
    const st = batch.jobs[job.name]!
    free(b, job)
    const again = retry(false, o.strictOnce, st.attempts, o.maxAttempts)
    st.status = again ? 'queued' : 'lost'
    st.box = undefined
    o.log(`${job.name}: lost on ${b.name}${again ? ', queued again' : ' (attempts spent)'}`)
    save()
  }

  const collect = async (b: Live, job: Job, exit: number, ended: number) => {
    const st = batch.jobs[job.name]!
    const J = dir(job.name)
    const local = mkdtempSync(join(tmpdir(), 'sc-out-'))
    try {
      const pack = await api(() => ib.exec(b.id, `cd ${J} && tar -czf out.tgz -C out . && sha256sum log out.tgz && find out -type f | wc -l && cat started`))
      if (pack.exitCode !== 0) throw new Error(`packing outputs exited ${pack.exitCode}: ${pack.stderr.trim()}`)
      const lines = pack.stdout.trim().split('\n')
      const sums = Object.fromEntries(lines.slice(0, 2).map((l) => l.split(/\s+/).reverse()))
      const count = Number(lines[2])
      const started = Number(lines[3])
      const logFile = join(o.out, `${job.name}.log`)
      await api(() => ib.pull(b.id, `${J.replace(/^~/, '/data/home')}/log`, logFile))
      let verified = sha(logFile) === sums.log
      if (count > 0) {
        const tgz = join(local, 'out.tgz')
        await api(() => ib.pull(b.id, `${J.replace(/^~/, '/data/home')}/out.tgz`, tgz))
        verified &&= sha(tgz) === sums['out.tgz']
        const files = join(o.out, `${job.name}.files`)
        rmSync(files, {recursive: true, force: true})
        mkdirSync(files, {recursive: true})
        if (verified && Bun.spawnSync(['tar', '-xzf', tgz, '-C', files]).exitCode !== 0) verified = false
      }
      if (!reusable(true, verified)) throw new Error('fetched outputs did not match their sha256')
      st.status = 'done'
      st.exit = exit
      st.seconds = Math.round((Number.isFinite(ended) ? ended : Date.now() / 1000) - (Number.isFinite(started) ? started : st.started!))
      await api(() => ib.exec(b.id, `rm -rf ${J}`)).catch(() => undefined)
      free(b, job)
      o.log(`${job.name}: exit ${exit} in ${st.seconds} s${count ? `, ${count} output files` : ''}`)
      save()
    } finally {
      rmSync(local, {recursive: true, force: true})
    }
  }

  // Every running job of a box, in one call: the ones that wrote their exit code.
  const poll = async (b: Live) => {
    const names = [...b.running].filter((n) => batch.jobs[n]!.status === 'running')
    if (!names.length) return
    const script = names.map((n) => `[ -f ${dir(n)}/exit ] && echo ${q(n)} "$(cat ${dir(n)}/exit)"`).join('; ') + '; true'
    let r: ib.Exec
    try {
      r = await api(() => ib.exec(b.id, script, {timeout: 60}))
      b.misses = 0
    } catch {
      if (++b.misses < 4) return
      const state = await api(() => ib.get(b.id)).then((x) => x.state, () => 'gone')
      if (state === 'suspended' || state === 'stopped') {
        await api(async () => { if (state === 'suspended') await ib.resume(b.id); else await ib.exec(b.id, 'true') }).catch(() => undefined)
        b.misses = 0
        return
      }
      if (state === 'running' || state === 'starting' || state === 'resuming') return
      o.log(`box ${b.name}: ${state}`)
      for (const n of names) lost(b, byName.get(n)!)
      retire(b)
      return
    }
    const done = r.stdout.trim().split('\n').filter(Boolean).map((l) => l.split(' ') as [string, string, string?])
    await Promise.all(done.map(([n, code, ended]) => collect(b, byName.get(n)!, Number(code), Number(ended)).catch((e) => {
      o.log(`${n}: outputs not fetched yet: ${e instanceof Error ? e.message : e}`)
    })))
  }

  const retire = (b: Live) => {
    live.delete(b.id)
    delete batch.boxes[b.id]
    boxes.close()
    save()
  }

  const pause = async (b: Live) => {
    retire(b)
    await api(() => ib.suspend(b.id)).then(() => o.log(`box ${b.name}: paused into the warm pool`), () => undefined)
  }

  // Boxes this batch held before (a run picked up): theirs again, resumed if they paused meanwhile.
  for (const rec of Object.values(batch.boxes)) {
    if (rec.id.startsWith('pending:')) {
      delete batch.boxes[rec.id]
      continue
    }
    if (!boxes.open()) break
    const state = await api(() => ib.get(rec.id)).then((x) => x.state, () => 'gone')
    if (state === 'gone' || state === 'error' || state === 'deleted') {
      boxes.close()
      delete batch.boxes[rec.id]
      continue
    }
    const b = adopt(rec, state === 'suspended' ? api(() => ib.resume(rec.id)).then(() => undefined) : Promise.resolve())
    for (const [n, st] of Object.entries(batch.jobs))
      if (st.status === 'running' && st.box === rec.id && byName.has(n)) take(b, byName.get(n)!)
  }
  for (const [n, st] of Object.entries(batch.jobs))
    if (st.status === 'running' && (!st.box || !live.has(st.box))) {
      st.status = retry(false, o.strictOnce, st.attempts, o.maxAttempts) ? 'queued' : 'lost'
      st.box = undefined
      if (!byName.has(n)) st.status = 'lost'
    }
  save()

  // Place, poll, until every job is done or lost.
  for (;;) {
    const queued = jobs.filter((j) => batch.jobs[j.name]!.status === 'queued')
    for (const job of queued) {
      const b = [...live.values()].find((b) => fitsOn(b, job)) ?? (await acquire(job))
      if (!b) break // at the cap: the rest wait for a slot
      take(b, job)
      void start(b, job)
    }
    const open = jobs.filter((j) => ['queued', 'running'].includes(batch.jobs[j.name]!.status))
    if (!open.length) break
    // A box no queued job can use, with nothing running, goes back to the pool.
    const waiting = jobs.filter((j) => batch.jobs[j.name]!.status === 'queued')
    for (const b of [...live.values()])
      if (b.running.size === 0 && !b.id.startsWith('pending:') && !waiting.some((j) => fitsOn({...b, cpu: 0, mem: 0}, j))) await pause(b)
    await sleep(o.poll)
    await Promise.all([...live.values()].filter((b) => !b.id.startsWith('pending:')).map((b) => b.ready.then(() => poll(b), () => undefined)))
  }
  await Promise.all([...live.values()].map((b) => pause(b)))

  // The summary, as the Python supercomputer writes it.
  const rows = jobs.map((j) => {
    const st = batch.jobs[j.name]!
    const where = st.where ?? ''
    return [j.name, st.status === 'done' ? String(st.exit) : st.status, String(st.seconds ?? ''), where]
  })
  writeFileSync(join(o.out, 'summary.tsv'), ['name\texit\tseconds\twhere', ...rows.map((r) => r.join('\t'))].join('\n') + '\n')
  const answered = jobs.filter((j) => batch.jobs[j.name]!.status === 'done').length
  const failed = jobs.filter((j) => batch.jobs[j.name]!.status === 'done' && batch.jobs[j.name]!.exit !== 0).length
  o.log(`batch ${batch.id}: ${answered - failed} passed, ${failed} failed, ${jobs.length - answered} lost; ${join(o.out, 'summary.tsv')}`)
  await reap(o.keep, o.log).catch(() => undefined)
  return succeeded(jobs.length, answered, failed) ? 0 : 3
}

function biggest(a: Size, b?: Size): Size {
  return b && b.vcpus > a.vcpus ? b : a
}

function sha(file: string): string {
  return new Bun.CryptoHasher('sha256').update(readFileSync(file)).digest('hex')
}

// The warm pool's paused boxes past their keep, deleted (never one this client could still be fetching from:
// only paused boxes, and a batch pauses a box only after its jobs' outputs are fetched).
export async function reap(keep: number, log: (s: string) => void): Promise<number> {
  const pool = await ib.list({sc: '1', sc_owner: owner()})
  let n = 0
  for (const b of pool) {
    if (b.state !== 'suspended') continue
    const since = Date.parse(b.autoSuspendedAt ?? b.updatedAt ?? '')
    const pausedFor = Number.isFinite(since) ? Math.max(0, Math.floor((Date.now() - since) / 1000)) : 0
    if (!letGo(pausedFor, keep, false)) continue
    await ib.remove(b.id)
    log(`box ${b.name}: deleted (paused ${Math.round(pausedFor / 60)} min)`)
    n++
  }
  return n
}
