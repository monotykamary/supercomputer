// A batch with no controller. Every box of the batch runs this loop (`supercomputer peer`); the laptop that launches
// the batch runs it once to open the first boxes, then may go. No participant is special and none has to stay: each
// turn reads the batch's ledger (src/ledger.ts), replays its log through the batch rule, and asks the rule for the next
// act this participant may take:
//
//   - its own jobs: report the exit of one that ended, its log and outputs stored in the ledger with the answer;
//   - its own box: start the queued jobs that fit on it;
//   - another box found gone (instabox says so): its attempts lost, tried again where the rule gives another;
//   - work no open box can take: a box opened for it, and made;
//   - nothing left for its box: the box closed and deleted, unless it is the last one watching boxes that still run;
//   - nothing left at all: the batch finished.
//
// Each act is appended to the ledger before it is done. An append another participant beat is not done: the loop
// reads again and asks the rule again, so whatever the participants do at once, the ledger is one run of the rule.

import {existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync} from 'node:fs'
import {homedir, tmpdir} from 'node:os'
import {join} from 'node:path'
import {Engine, type Limits, type Req} from './engine.ts'
import * as ib from './instabox.ts'
import {type Cache, type Config, fits, type Job} from './jobs.ts'
import {type BoxRec, type Files, type Ledger, openLedger, type State} from './ledger.ts'
import {biggest, jobScript, kindOf, newBatch, SYSTEM_MIB} from './run.ts'
import type {Tree} from './tree.ts'

// batch.json in the ledger: what never changes.
export type Shared = {id: string; owner: string; tree: string; caches: (Cache & {key: string})[]; cfg: Config; limits: Limits; jobs: Job[]; groups: string[]; kinds: string[]}

export type PeerOptions = {log: (line: string) => void; poll: number}

// (a box claimed this long ago that instabox doesn't know yet is gone: its create never came, or failed)
const GRACE_MS = 5 * 60_000
const SECRETS = ['SC_LEDGER_TOKEN', 'SC_INSTABOX_KEY']
const sleep = (s: number) => new Promise((r) => setTimeout(r, s * 1000))

// Ask the rule for these requests in order and append those it accepts, as one version: the requests taken (none
// accepted: []), or undefined when another participant appended first.
export async function propose(ledger: Ledger, head: string, shared: Shared, state: State, reqs: Req[], edit?: (s: State) => void, files?: Files): Promise<Req[] | undefined> {
  const e = new Engine(shared.limits, state.log)
  const took = reqs.filter((r) => e.step(r))
  if (!took.length) return []
  const next: State = {...structuredClone(state), log: e.log} // (a copy: a lost append leaves the state read as it was)
  edit?.(next)
  const h = await ledger.append(head, next, files, took.map((r) => `${r.op}${r.job !== undefined ? ` ${shared.jobs[r.job]?.name}` : ''}${r.box !== undefined ? ` box ${r.box}` : ''}`).join(', '))
  return h ? took : undefined
}

// The request that ends an attempt with no native answer: tried again when the rule gives it another, else lost.
const lose = (e: Engine, i: number): Req => (e.would({op: 'retry', job: i}) ? {op: 'retry', job: i} : {op: 'drop', job: i})

// A box for the first queued job no open box can take, opened in the ledger (with its name, claimed now); the slot
// and record, or undefined when the rule opens none or another participant appended first.
export async function claimBox(ledger: Ledger, head: string, shared: Shared, state: State): Promise<{slot: number; rec: BoxRec} | undefined> {
  const e = new Engine(shared.limits, state.log)
  // (the queued jobs packed, as if started, onto the room the open boxes have: a box only for one left over)
  const sim = new Engine(shared.limits, state.log)
  for (const [i, j] of e.jobs()) {
    if (j.st !== 'queued') continue
    if (sim.boxes().some(([b]) => sim.step({op: 'start', job: i, box: b}))) continue
    const job = shared.jobs[i]!
    const size = job.size ?? (shared.limits.timing ? fits(job) : biggest(fits(job), shared.cfg.size))
    const slot = Object.keys(state.boxes).length
    const rec: BoxRec = {name: `sc-${shared.owner}-${Math.random().toString(36).slice(2, 10)}`, at: Date.now(), size, image: job.image}
    const open: Req = {op: 'open', box: slot, cpu: size.vcpus, mem: size.memMiB - SYSTEM_MIB, kind: shared.kinds.indexOf(kindOf(job)) + 1}
    if (!e.would(open)) return undefined // (at the cap: the job waits for a slot)
    const took = await propose(ledger, head, shared, state, [open], (s) => (s.boxes[slot] = rec))
    return took?.length ? {slot, rec} : undefined
  }
  return undefined
}

// The box itself: made from the batch's image with the ledger's Secrets, its peer started by its setup script.
async function makeBox(ledger: Ledger, shared: Shared, slot: number, rec: BoxRec, log: (s: string) => void): Promise<void> {
  const [repo, batch] = ledger.where.split('#') as [string, string]
  const img = rec.image ? shared.cfg.images[rec.image]! : shared.cfg.image
  const boot = `#!/usr/bin/env bash
# supercomputer's peer for box ${slot} of batch ${batch}: this program and the tree from the ledger, then the loop.
set -euo pipefail
export PATH="$HOME/.bun/bin:$PATH"
command -v bun >/dev/null || { curl -fsSL https://bun.sh/install | bash >/dev/null 2>&1; }
command -v instabox >/dev/null || sudo npm i -g instabox@latest >/dev/null 2>&1
mkdir -p ~/sc/trees ~/sc/peer
api() { curl -fsSL --retry 5 --retry-all-errors -H "Authorization: Bearer $SC_LEDGER_TOKEN" -H 'User-Agent: supercomputer' "$@"; }
head=$(api "https://api.github.com/repos/${repo}/git/ref/heads/sc/${batch}" | python3 -c 'import json, sys; print(json.load(sys.stdin)["object"]["sha"])')
api -H 'Accept: application/vnd.github.raw' -o ~/sc/peer/sc.js "https://api.github.com/repos/${repo}/contents/sc.js?ref=$head"
api -H 'Accept: application/vnd.github.raw' -o ~/sc/trees/${shared.tree}.tgz "https://api.github.com/repos/${repo}/contents/tree.tgz?ref=$head"
cat > ~/sc/peer/run.sh <<'EOF'
export PATH="$HOME/.bun/bin:$PATH" INSTABOX_API_KEY="$SC_INSTABOX_KEY" INSTABOX_NO_UPDATE_CHECK=1
until [ -f ~/sc/peer/done ]; do bun ~/sc/peer/sc.js peer --ledger '${ledger.where}' --slot ${slot} --name '${rec.name}' >> ~/sc/peer/peer.log 2>&1; sleep 5; done
EOF
(setsid nohup bash ~/sc/peer/run.sh > /dev/null 2>&1 < /dev/null &)
echo "peer started"
`
  const dir = mkdtempSync(join(tmpdir(), 'sc-boot-'))
  const file = join(dir, 'boot.sh')
  writeFileSync(file, boot)
  try {
    log(`box ${rec.name}: creating (slot ${slot}, ${rec.size.vcpus} vCPU / ${rec.size.memMiB / 1024} GiB)`)
    await ib.create({
      name: rec.name, vcpus: rec.size.vcpus, memMiB: rec.size.memMiB, snapshot: img.snapshot, template: img.template,
      meta: {sc: '1', sc_owner: shared.owner, sc_batch: shared.id},
      // (no idle pause: its peer decides when it is done; a backstop against a peer that never does)
      autoSuspend: '0', deleteAfter: '6h', secrets: SECRETS, setupFile: file,
    })
    log(`box ${rec.name}: up, its peer running`)
  } finally {
    rmSync(dir, {recursive: true, force: true})
  }
}

// Whether instabox says the box is gone for good: deleted, never made, broken or stopped (its processes ended).
async function isGone(name: string): Promise<boolean> {
  try {
    const b = await ib.get(name)
    if (b.state === 'suspended') await ib.resume(name).catch(() => undefined)
    return ['deleted', 'error', 'stopped'].includes(b.state)
  } catch (e) {
    return e instanceof ib.InstaboxError && /not.?found|404|no (such )?sandbox/i.test(e.stderr)
  }
}

// The laptop's part: the batch written to a new ledger, its first boxes opened and made. Then it may go.
export async function launch(jobs: Job[], cfg: Config, tree: Tree, limits: Limits, repo: string, log: (s: string) => void): Promise<string> {
  for (const j of jobs) if (j.gpu) throw new Error(`${j.name}: GPU jobs don't run controller-less yet`)
  const b = newBatch(jobs, cfg, tree, limits)
  const shared: Shared = {id: b.id, owner: b.owner, tree: b.tree, caches: b.caches, cfg: b.cfg, limits: b.limits, jobs: b.jobs, groups: b.groups, kinds: b.kinds}
  const out = join(mkdtempSync(join(tmpdir(), 'sc-js-')), 'sc.js')
  const built = Bun.spawnSync(['bun', 'build', join(import.meta.dir, 'cli.ts'), '--target', 'bun', '--outfile', out], {stdout: 'pipe', stderr: 'pipe'})
  if (built.exitCode !== 0) throw new Error(`bundling the peer: ${built.stderr}`)
  const ledger = openLedger(`${repo}#${b.id}`)
  await ledger.create({'batch.json': JSON.stringify(shared, null, 1), 'tree.tgz': new Uint8Array(readFileSync(tree.tarball)), 'sc.js': new Uint8Array(readFileSync(out))}, {log: b.log, boxes: {}, times: {}})
  log(`batch ${b.id}: ${jobs.length} jobs in the ledger ${ledger.where}`)
  const making: Promise<void>[] = []
  for (;;) {
    const {head, state} = await ledger.read()
    const c = await claimBox(ledger, head, shared, state)
    if (!c) break
    making.push(makeBox(ledger, shared, c.slot, c.rec, log))
  }
  const made = await Promise.allSettled(making)
  for (const m of made) if (m.status === 'rejected') log(`a box was not made: ${m.reason instanceof Error ? m.reason.message : m.reason} (its slot is closed by a peer once it is found gone)`)
  return ledger.where
}

// A box's part, until the batch has a verdict or its box is closed: then its box is deleted.
export async function peer(spec: string, me: {slot: number; name: string}, o: PeerOptions): Promise<void> {
  const ledger = openLedger(spec)
  const shared = JSON.parse(new TextDecoder().decode(await ledger.file('batch.json'))) as Shared
  const run = join(homedir(), 'sc', 'run', shared.id)
  const jobDir = (i: number) => join(run, shared.jobs[i]!.name)
  const making = new Set<Promise<void>>()
  const lastProbe = new Map<number, number>()
  o.log(`peer of box ${me.slot} (${me.name}) in batch ${shared.id}`)

  const leave = async (why: string) => {
    o.log(`leaving: ${why}`)
    await Promise.allSettled([...making])
    writeFileSync(join(homedir(), 'sc', 'peer', 'done'), why)
    await ib.remove(me.name).catch((e) => o.log(`deleting this box: ${e instanceof Error ? e.message : e}`))
  }

  for (;;) {
    const {head, state} = await ledger.read()
    const e = new Engine(shared.limits, state.log)
    const mine = e.box(me.slot)
    if (e.verdict !== 'open') {
      if (mine?.open) await propose(ledger, head, shared, state, [{op: 'close', box: me.slot}])
      return leave(`the batch ${e.verdict}`)
    }
    if (!mine?.open) return leave('this box is no longer open in the batch')
    // (a slot is its claimer's box alone: one whose record names another box is not this one's to act for)
    if (state.boxes[me.slot]?.name !== me.name) return leave(`slot ${me.slot} is ${state.boxes[me.slot]?.name ?? 'no box'}'s, not this one's`)
    const onMe = (st: string) => e.jobs().filter(([, j]) => j.box === me.slot && j.st === st).map(([i]) => i)

    // Its jobs that ended: their exits. (One claimed here but never started, a peer that died in between: lost.)
    const exits: Req[] = []
    const ended: Record<string, number> = {}
    for (const i of onMe('running')) {
      const d = jobDir(i)
      if (existsSync(join(d, 'exit'))) {
        const [code, at] = readFileSync(join(d, 'exit'), 'utf8').trim().split(' ').map(Number) as [number, number]
        exits.push({op: 'exit', job: i, code: Number.isInteger(code) && code >= 0 ? code : 255})
        ended[shared.jobs[i]!.name] = at
      } else if (!existsSync(join(d, 'job.sh'))) exits.push(lose(e, i))
    }
    if (exits.length) {
      const took = await propose(ledger, head, shared, state, exits, (s) => {
        for (const [n, at] of Object.entries(ended)) s.times[n] = {...s.times[n], ended: at}
      })
      if (took?.length) for (const r of took) o.log(`${shared.jobs[r.job!]!.name}: ${r.op === 'exit' ? `exit ${r.code}` : `${r.op} (never started)`}`)
      continue
    }

    // An ended job's log and outputs, stored in the ledger with its answer; then its folder here goes.
    const fetchable = onMe('ended')
    if (fetchable.length) {
      const i = fetchable[0]!
      const d = jobDir(i)
      const t = Bun.spawnSync(['tar', '-czf', '-', '-C', d, 'log', 'out'], {stdout: 'pipe', stderr: 'pipe'})
      const took = t.exitCode === 0
        ? await propose(ledger, head, shared, state, [{op: 'fetch', job: i}], undefined, {[`out/${shared.jobs[i]!.name}.tgz`]: new Uint8Array(t.stdout)})
        : await propose(ledger, head, shared, state, [lose(e, i)]) // (its folder gone: the answer with it)
      if (took?.length) {
        o.log(`${shared.jobs[i]!.name}: ${t.exitCode === 0 ? `stored (${t.stdout.length} bytes)` : 'its outputs are gone, the attempt lost'}`)
        rmSync(d, {recursive: true, force: true})
      }
      continue
    }

    // The queued jobs that fit on this box, claimed together, then started here.
    const c = new Engine(shared.limits, state.log)
    const starts = e.jobs().filter(([i, j]) => j.st === 'queued' && c.step({op: 'start', job: i, box: me.slot})).map(([i]): Req => ({op: 'start', job: i, box: me.slot}))
    if (starts.length) {
      const now = Math.floor(Date.now() / 1000)
      const took = await propose(ledger, head, shared, state, starts, (s) => {
        for (const r of starts) s.times[shared.jobs[r.job!]!.name] = {started: now, where: me.name}
      })
      for (const r of took ?? []) {
        const job = shared.jobs[r.job!]!
        const {script, launch} = jobScript(shared, job)
        const p = Bun.spawnSync(['bash', '-c', launch], {stdin: new Blob([script]), stderr: 'pipe'})
        o.log(`${job.name}: ${p.exitCode === 0 ? 'started' : `not started: ${p.stderr}`}`)
      }
      continue
    }

    // Another box gone: its attempts lost (tried again where the rule allows), then its slot closed.
    let acted = false
    for (const [b, box] of e.boxes()) {
      if (b === me.slot) continue
      const rec = state.boxes[b]
      if (rec && Date.now() - rec.at < GRACE_MS) continue
      if (Date.now() - (lastProbe.get(b) ?? 0) < 20_000) continue
      lastProbe.set(b, Date.now())
      if (rec && !(await isGone(rec.name))) continue
      const reqs = e.jobs().filter(([, j]) => j.box === b && (j.st === 'running' || j.st === 'ended')).map(([i]) => lose(e, i))
      const took = await propose(ledger, head, shared, state, [...reqs, {op: 'close', box: b}])
      if (took?.length) o.log(`box ${rec?.name ?? `slot ${b}`} gone: ${took.filter((r) => r.op !== 'close').map((r) => `${shared.jobs[r.job!]!.name} ${r.op === 'retry' ? 'queued again' : 'lost'}`).join(', ') || 'nothing on it'}${box.running || box.unfetched ? '' : ''}`)
      acted = true
      break
    }
    if (acted) continue

    // Work no open box can take: a box opened for it.
    const claimed = await claimBox(ledger, head, shared, state)
    if (claimed) {
      const m = makeBox(ledger, shared, claimed.slot, claimed.rec, o.log).catch((err) => o.log(`box ${claimed.rec.name}: ${err instanceof Error ? err.message : err}`))
      making.add(m)
      void m.finally(() => making.delete(m))
      continue
    }

    // Nothing left for this box: the batch finished when nothing is left at all; else the box closes, unless it is
    // the last idle one while others still run (someone must be there to find a box gone).
    const queued = e.jobs().filter(([, j]) => j.st === 'queued').map(([i]) => i)
    if (mine.running === 0 && mine.unfetched === 0 && !queued.some((i) => e.would({op: 'start', job: i, box: me.slot}))) {
      const left = e.count('queued') + e.count('running') + e.count('ended')
      if (left === 0) {
        const took = await propose(ledger, head, shared, state, [{op: 'finish'}])
        if (took?.length) o.log(`the batch finished: ${new Engine(shared.limits, [...state.log, ...took]).verdict}`)
        continue
      }
      const idle = e.boxes().filter(([, b]) => b.running === 0 && b.unfetched === 0).map(([b]) => b)
      const othersRun = e.boxes().some(([b, x]) => b !== me.slot && (x.running > 0 || x.unfetched > 0))
      if (!(othersRun && Math.min(...idle) === me.slot)) {
        const took = await propose(ledger, head, shared, state, [{op: 'close', box: me.slot}])
        if (took === undefined) continue
        // (closed: the slot it frees opened for queued work another kind of box must take, before this one goes)
        for (;;) {
          const r = await ledger.read()
          const c2 = await claimBox(ledger, r.head, shared, r.state)
          if (!c2) break
          await makeBox(ledger, shared, c2.slot, c2.rec, o.log).catch((err) => o.log(`box ${c2.rec.name}: ${err instanceof Error ? err.message : err}`))
        }
        return leave('nothing left for this box')
      }
    }
    await sleep(o.poll)
  }
}

// A batch's results from its ledger, with no box needed: each answered job's log and outputs, and summary.tsv.
export async function fetchLedger(spec: string, out: string, log: (s: string) => void): Promise<number> {
  const ledger = openLedger(spec)
  const shared = JSON.parse(new TextDecoder().decode(await ledger.file('batch.json'))) as Shared
  const {head, state} = await ledger.read()
  const e = new Engine(shared.limits, state.log)
  mkdirSync(out, {recursive: true})
  const rows: string[][] = []
  for (const [i, job] of shared.jobs.entries()) {
    const st = e.job(i)!
    const t = state.times[job.name] ?? {}
    if (st.st === 'done') {
      const files = join(out, `${job.name}.files`)
      const tmp = mkdtempSync(join(tmpdir(), 'sc-fetch-'))
      try {
        writeFileSync(join(tmp, 'o.tgz'), await ledger.file(`out/${job.name}.tgz`, head))
        if (Bun.spawnSync(['tar', '-xzf', join(tmp, 'o.tgz'), '-C', tmp]).exitCode !== 0) throw new Error(`${job.name}: its outputs did not unpack`)
        writeFileSync(join(out, `${job.name}.log`), readFileSync(join(tmp, 'log')))
        rmSync(files, {recursive: true, force: true})
        Bun.spawnSync(['cp', '-R', join(tmp, 'out'), files])
      } finally {
        rmSync(tmp, {recursive: true, force: true})
      }
    }
    rows.push([job.name, st.st === 'done' ? String(st.exit) : st.st, st.st === 'done' && t.started && t.ended ? String(t.ended - t.started) : '', t.where ?? ''])
  }
  writeFileSync(join(out, 'summary.tsv'), ['name\texit\tseconds\twhere', ...rows.map((r) => r.join('\t'))].join('\n') + '\n')
  const failed = e.jobs().filter(([, j]) => j.st === 'done' && j.exit !== 0).length
  const open = e.boxes().length
  log(`batch ${shared.id}: ${e.verdict}: ${e.count('done') - failed} passed, ${failed} failed, ${e.count('lost')} lost, ${e.count('queued') + e.count('running') + e.count('ended')} not answered yet; ${open} boxes open; ${Object.keys(state.boxes).length} made in all`)
  return e.verdict === 'passed' ? 0 : e.verdict === 'failed' ? 3 : 4
}
