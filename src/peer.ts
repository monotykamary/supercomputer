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
import type {Cache, Config, Job} from './jobs.ts'
import {type BoxRec, type Files, type Ledger, openLedger, type State} from './ledger.ts'
import {decideIdle} from './idle.ts'
import {jobScript, kindOf, lockedSizes, newBatch, SYSTEM_MIB} from './run.ts'
import {type Locked, planSize} from './size.ts'
import type {Tree} from './tree.ts'

// batch.json in the ledger: what never changes.
export type Shared = {id: string; owner: string; tree: string; caches: (Cache & {key: string})[]; cfg: Config; limits: Limits; jobs: Job[]; groups: string[]; kinds: string[]; locked?: Locked}

export type PeerOptions = {log: (line: string) => void; poll: number}

// (a box claimed this long ago that instabox doesn't know yet is gone: its create never came, or failed)
const GRACE_MS = 5 * 60_000
const SECRETS = ['SC_LEDGER_TOKEN', 'SC_INSTABOX_KEY']
const sleep = (s: number) => new Promise((r) => setTimeout(r, s * 1000))
const alive = (pid: number) => {
  if (!Number.isInteger(pid) || pid <= 0) return true // (no pid to tell by: not taken for dead)
  try {
    process.kill(pid, 0)
    return true
  } catch {
    return false
  }
}

// Ask the rule for these requests in order and append those it accepts, as one version: the requests taken (none
// accepted: []), or undefined when another participant appended first.
export async function propose(ledger: Ledger, head: string, shared: Shared, state: State, reqs: Req[], edit?: (s: State) => void, files?: Files): Promise<Req[] | undefined> {
  const e = new Engine(shared.limits, state.log)
  const took = reqs.filter((r) => e.step(r))
  if (!took.length) return []
  const next: State = {...structuredClone(state), log: e.log} // (a copy: a lost append leaves the state read as it was)
  edit?.(next)
  const h = await ledger.append(head, next, files, took.map((r) => `${r.op}${r.job !== undefined ? ` ${shared.jobs[r.job]?.name}` : ''}${r.box !== undefined ? ` box ${r.box}` : ''}`).join(', '))
  // (beaten: a moment's random wait, so participants that raced don't race again in step)
  if (!h) await sleep(0.2 + Math.random() * 1.3)
  return h ? took : undefined
}

// The request that ends an attempt with no native answer: tried again when the rule gives it another, else lost.
const lose = (e: Engine, i: number): Req => (e.would({op: 'retry', job: i}) ? {op: 'retry', job: i} : {op: 'drop', job: i})

// A box for the first queued job no open box can take, opened in the ledger (with its name, claimed now by `by`),
// after `before` (a box closing itself to free the slot): the slot and record; 'none' when no job needs one or the
// rule opens none; 'beaten' when another participant appended first.
export type Claim = {slot: number; rec: BoxRec} | 'none' | 'beaten'
export async function claimBox(ledger: Ledger, head: string, shared: Shared, state: State, by: string, before: Req[] = []): Promise<Claim> {
  const e = new Engine(shared.limits, state.log)
  // (the queued jobs packed, as if started, onto the room the open boxes have: a box only for one left over)
  const sim = new Engine(shared.limits, state.log)
  for (const r of before) if (!e.step(r) || !sim.step(r)) return 'none'
  for (const [i, j] of e.jobs()) {
    if (j.st !== 'queued') continue
    if (sim.boxes().some(([b]) => sim.step({op: 'start', job: i, box: b}))) continue
    const job = shared.jobs[i]!
    const size = planSize(job, shared.cfg, shared.locked ?? {}, shared.limits.timing)
    const slot = Object.keys(state.boxes).length
    const rec: BoxRec = {name: `sc-${shared.owner}-${Math.random().toString(36).slice(2, 10)}`, at: Date.now(), size, image: job.image, by}
    const open: Req = {op: 'open', box: slot, cpu: size.vcpus, mem: size.memMiB - SYSTEM_MIB, kind: shared.kinds.indexOf(kindOf(job)) + 1}
    if (!e.would(open)) return 'none' // (at the cap: the job waits for a slot)
    const took = await propose(ledger, head, shared, state, [...before, open], (s) => (s.boxes[slot] = rec))
    return took === undefined ? 'beaten' : took.length === before.length + 1 ? {slot, rec} : 'none'
  }
  return 'none'
}

// Claim and make the boxes queued work needs, until none is needed: how many were made. A box that could not be made
// has its slot closed again (nothing is on it), so the work it was for is claimed anew; the last open box, which the
// rule keeps while work is left, has its slot replaced by a fresh box instead.
async function grow(ledger: Ledger, shared: Shared, by: string, log: (s: string) => void, before: Req[] = []): Promise<number> {
  let made = 0
  // (three boxes in a row that could not be made: instabox is refusing them, and it stops trying)
  let failed = 0
  while (failed < 3) {
    const {head, state} = await ledger.read()
    const c = await claimBox(ledger, head, shared, state, by, before)
    if (c === 'beaten') continue
    if (c === 'none') return made
    before = []
    for (let rec = c.rec; ; ) {
      if (await makeBox(ledger, shared, c.slot, rec, (await ledger.read()).head, log)) {
        made++
        failed = 0
        break
      }
      const closed = await closeSlot(ledger, shared, c.slot)
      if (++failed >= 3 || closed) break
      rec = await replaceSlot(ledger, shared, c.slot, rec)
    }
  }
  return made
}

// A slot closed in the ledger (a box never made), whatever else moved meanwhile: false when the rule refuses it (the
// last open box, while work is left).
async function closeSlot(ledger: Ledger, shared: Shared, slot: number): Promise<boolean> {
  for (;;) {
    const {head, state} = await ledger.read()
    const took = await propose(ledger, head, shared, state, [{op: 'close', box: slot}])
    if (took !== undefined) return took.length > 0
  }
}

// A slot's box that could not be made, replaced in the ledger by a fresh one of the same size and kind (a new name):
// the record to make.
async function replaceSlot(ledger: Ledger, shared: Shared, slot: number, old: BoxRec): Promise<BoxRec> {
  const rec: BoxRec = {...old, name: `sc-${shared.owner}-${Math.random().toString(36).slice(2, 10)}`, at: Date.now()}
  for (;;) {
    const {head, state} = await ledger.read()
    const e = new Engine(shared.limits, state.log)
    const kind = e.box(slot)?.kind ?? 0
    const took = await propose(ledger, head, shared, state, [{op: 'replace', box: slot, cpu: rec.size.vcpus, mem: rec.size.memMiB - SYSTEM_MIB, kind}], (s) => (s.boxes[slot] = rec))
    if (took !== undefined) return took.length ? rec : old
  }
}

// The box itself: made from the batch's image with the ledger's Secrets, its peer started by its setup script.
async function makeBox(ledger: Ledger, shared: Shared, slot: number, rec: BoxRec, head: string, log: (s: string) => void): Promise<boolean> {
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
# (a version known to hold them: GitHub's answer for the branch's head may lag behind it)
head=${head}
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
    for (let attempt = 1; attempt <= 3; attempt++) {
      log(`box ${rec.name}: creating (slot ${slot}, ${rec.size.vcpus} vCPU / ${rec.size.memMiB / 1024} GiB)${attempt > 1 ? `, attempt ${attempt}` : ''}`)
      try {
        await ib.create({
          name: rec.name, vcpus: rec.size.vcpus, memMiB: rec.size.memMiB, snapshot: img.snapshot, template: img.template,
          meta: {sc: '1', sc_owner: shared.owner, sc_batch: shared.id},
          // (no idle pause: its peer decides when it is done; a backstop against a peer that never does)
          autoSuspend: '0', deleteAfter: '6h', secrets: SECRETS, setupFile: file,
        })
        log(`box ${rec.name}: up, its peer running`)
        return true
      } catch (e) {
        log(`box ${rec.name}: ${e instanceof Error ? e.message : e}`)
        const b = await ib.get(rec.name).catch(() => undefined)
        // (it exists and booted: made after all, by this try or another participant's)
        if (b && b.state === 'running' && b.metadata?.setupStatus === 'succeeded') return true
        // (made, but its boot failed: deleted before the next try, so it bills nothing)
        if (b) await ib.remove(rec.name).catch(() => undefined)
      }
    }
    return false
  } finally {
    rmSync(dir, {recursive: true, force: true})
  }
}

// Whether instabox says, for certain, that there is no such box (an error of any other kind is no answer).
async function notFound(name: string): Promise<boolean> {
  return ib.get(name).then(
    () => false,
    (e) => e instanceof ib.InstaboxError && /not.?found|404|no (such )?sandbox/i.test(e.stderr),
  )
}

// Whether instabox says the box is gone for good: deleted, never made, broken or stopped (its processes ended).
async function isGone(name: string): Promise<boolean> {
  try {
    const b = await ib.get(name)
    // (its boot failed: no peer will ever run there, so it goes, and its slot with it)
    if (b.metadata?.setupStatus === 'failed') {
      await ib.remove(name).catch(() => undefined)
      return true
    }
    if (b.state === 'suspended') await ib.resume(name).catch(() => undefined)
    return ['deleted', 'error', 'stopped'].includes(b.state)
  } catch (e) {
    return e instanceof ib.InstaboxError && /not.?found|404|no (such )?sandbox/i.test(e.stderr)
  }
}

// The laptop's part: the batch written to a new ledger, its first boxes opened and made. Then it may go.
export async function launch(jobs: Job[], cfg: Config, tree: Tree, limits: Limits, repo: string, log: (s: string) => void): Promise<string> {
  for (const j of jobs) if (j.gpu) throw new Error(`${j.name}: GPU jobs don't run controller-less yet`)
  // (peers: the rule keeps the last box while work is left, and hands its slot on with replace)
  const locked = await lockedSizes(cfg, jobs)
  const b = newBatch(jobs, cfg, tree, {...limits, peers: true}, locked)
  const shared: Shared = {id: b.id, owner: b.owner, tree: b.tree, caches: b.caches, cfg: b.cfg, limits: b.limits, jobs: b.jobs, groups: b.groups, kinds: b.kinds, locked: b.locked}
  const out = join(mkdtempSync(join(tmpdir(), 'sc-js-')), 'sc.js')
  const built = Bun.spawnSync(['bun', 'build', join(import.meta.dir, 'cli.ts'), '--target', 'bun', '--outfile', out], {stdout: 'pipe', stderr: 'pipe'})
  if (built.exitCode !== 0) throw new Error(`bundling the peer: ${built.stderr}`)
  const ledger = openLedger(`${repo}#${b.id}`)
  await ledger.create({'batch.json': JSON.stringify(shared, null, 1), 'tree.tgz': new Uint8Array(readFileSync(tree.tarball)), 'sc.js': new Uint8Array(readFileSync(out))}, {log: b.log, boxes: {}, times: {}})
  log(`batch ${b.id}: ${jobs.length} jobs in the ledger ${ledger.where}`)
  const made = await grow(ledger, shared, 'launch', log)
  // (no box made: nobody is left to run the batch, so it is not launched)
  if (!made) throw new Error(`no box could be made for batch ${b.id}; its ledger ${ledger.where} holds it, and nothing runs it`)
  return ledger.where
}

// A box's part, until the batch has a verdict or its box is closed: then its box is deleted.
export async function peer(spec: string, me: {slot: number; name: string}, o: PeerOptions): Promise<void> {
  const ledger = openLedger(spec)
  const shared = JSON.parse(new TextDecoder().decode(await ledger.file('batch.json'))) as Shared
  const run = join(homedir(), 'sc', 'run', shared.id)
  // (a folder per attempt: a retried job on the same box never reads the attempt before's exit)
  const jobDir = (i: number, attempt: number) => join(run, `${shared.jobs[i]!.name}.${attempt}`)
  const making = new Set<Promise<unknown>>()
  const lastProbe = new Map<number, number>()
  o.log(`peer of box ${me.slot} (${me.name}) in batch ${shared.id}`)

  // Going: first the boxes this one claimed that aren't up (a peer that died between claiming and making one), then
  // this box deleted. Marked done for the supervisor only once deleting failed for good; else it starts again.
  const leave = async (why: string) => {
    o.log(`leaving: ${why}`)
    await Promise.allSettled([...making])
    const {state} = await ledger.read()
    const e = new Engine(shared.limits, state.log)
    // (only while the batch is open, only a slot claimed lately with nothing on it, only one instabox says for certain
    // has no box: never a box that is up, or one that went away on its own)
    for (const [b, x] of e.verdict === 'open' ? e.boxes() : []) {
      const rec = state.boxes[b]
      if (rec?.by !== me.name || rec.name === me.name || x.running || x.unfetched || Date.now() - rec.at > GRACE_MS) continue
      if ((await notFound(rec.name)) && !(await makeBox(ledger, shared, b, rec, (await ledger.read()).head, o.log)) && !(await closeSlot(ledger, shared, b)))
        o.log(`slot ${b}: its box could not be made, and it is the last one: the batch waits for \`batch fetch\` or a new launch`)
    }
    for (let attempt = 1; attempt <= 3; attempt++) {
      try {
        await ib.remove(me.name)
        return
      } catch (err) {
        o.log(`deleting this box: ${err instanceof Error ? err.message : err}`)
        await sleep(5 * attempt)
      }
    }
    // (the supervisor starts this peer again, which comes back here and tries again)
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
      const d = jobDir(i, e.job(i)!.attempts)
      const exitOf = () => (existsSync(join(d, 'exit')) ? readFileSync(join(d, 'exit'), 'utf8').trim().split(' ').map(Number) as [number, number] : undefined)
      let x = exitOf()
      // (no exit and its process gone, killed past its own timeout's reach: the attempt is lost)
      if (!x && existsSync(join(d, 'pid')) && !alive(Number(readFileSync(join(d, 'pid'), 'utf8')))) x = exitOf() ?? [-1, 0]
      if (x && x[0] >= 0) {
        exits.push({op: 'exit', job: i, code: Number.isInteger(x[0]) ? x[0] : 255})
        ended[shared.jobs[i]!.name] = x[1]
      } else if (x || !existsSync(join(d, 'job.sh'))) exits.push(lose(e, i))
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
      const d = jobDir(i, e.job(i)!.attempts)
      // (a job that removed its own output folder, or wrote no log, still has its answer)
      if (existsSync(d)) {
        mkdirSync(join(d, 'out'), {recursive: true})
        if (!existsSync(join(d, 'log'))) writeFileSync(join(d, 'log'), '')
      }
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
      const failed: Req[] = []
      for (const r of took ?? []) {
        const job = shared.jobs[r.job!]!
        const {script, launch} = jobScript(shared, job, e.job(r.job!)!.attempts + 1)
        const p = Bun.spawnSync(['bash', '-c', launch], {stdin: new Blob([script]), stderr: 'pipe'})
        o.log(`${job.name}: ${p.exitCode === 0 ? 'started' : `not started: ${p.stderr}`}`)
        if (p.exitCode !== 0) failed.push({op: 'retry', job: r.job!})
      }
      // (a launch that failed: its attempt lost now, not found later)
      for (let i = 0; failed.length && i < 5; i++) {
        const r = await ledger.read()
        const e2 = new Engine(shared.limits, r.state.log)
        if ((await propose(ledger, r.head, shared, r.state, failed.map((f) => lose(e2, f.job!)))) !== undefined) break
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

    // Work no open box can take: a box opened for it, made meanwhile (its slot closed again if it can't be).
    const claimed = await claimBox(ledger, head, shared, state, me.name)
    if (claimed === 'beaten') continue
    if (claimed !== 'none') {
      // (its slot is never the last open one: this box is open too, so closing it is never refused)
      const m = makeBox(ledger, shared, claimed.slot, claimed.rec, head, o.log).then((ok) => (ok ? undefined : closeSlot(ledger, shared, claimed.slot)))
      making.add(m)
      void m.finally(() => making.delete(m))
      continue
    }

    // Nothing left for this box: the proven idle decision says whether the batch finishes, the box stays to watch
    // the others, hands its slot on (the last box), or closes and goes.
    const queued = e.jobs().filter(([, j]) => j.st === 'queued').map(([i]) => i)
    if (mine.running === 0 && mine.unfetched === 0 && !queued.some((i) => e.would({op: 'start', job: i, box: me.slot}))) {
      const left = e.count('queued') + e.count('running') + e.count('ended')
      // What to do is the proven idle decision's (src/idle.ts); its inputs are read off the rule's state.
      const idleBoxes = e.boxes().filter(([, b]) => b.running === 0 && b.unfetched === 0).map(([b]) => b)
      const sim = new Engine(shared.limits, state.log)
      const act = decideIdle({
        left,
        others: e.boxes().length - 1,
        busy: e.boxes().filter(([b, x]) => b !== me.slot && (x.running > 0 || x.unfetched > 0)).length,
        young: e.boxes().some(([b]) => b !== me.slot && Date.now() - (state.boxes[b]?.at ?? 0) < GRACE_MS),
        blocked: e.boxes().length >= shared.limits.maxBoxes && queued.some((i) => !e.boxes().some(([b]) => sim.would({op: 'start', job: i, box: b}))),
        lowest: Math.min(...idleBoxes) === me.slot,
      })
      if (act === 'finish') {
        const took = await propose(ledger, head, shared, state, [{op: 'finish'}])
        if (took?.length) o.log(`the batch finished: ${new Engine(shared.limits, [...state.log, ...took]).verdict}`)
        continue
      }
      if (act === 'close') {
        if ((await propose(ledger, head, shared, state, [{op: 'close', box: me.slot}])) === undefined) continue
        return leave('nothing left for this box')
      }
      if (act === 'hand') {
        if (making.size) {
          await sleep(o.poll)
          continue
        }
        // (the last box: its slot replaced by a box for the first queued job it can't take, the rule's replace; if
        // that box can't be made, the slot is replaced back by this one, which stays; not under --timing, where a box
        // runs one job in its life)
        const i = queued[0]!
        const job = shared.jobs[i]!
        const size = planSize(job, shared.cfg, shared.locked ?? {}, shared.limits.timing)
        const rec: BoxRec = {name: `sc-${shared.owner}-${Math.random().toString(36).slice(2, 10)}`, at: Date.now(), size, image: job.image, by: me.name}
        const own = state.boxes[me.slot]!
        const took = await propose(ledger, head, shared, state, [{op: 'replace', box: me.slot, cpu: size.vcpus, mem: size.memMiB - SYSTEM_MIB, kind: shared.kinds.indexOf(kindOf(job)) + 1}], (st) => (st.boxes[me.slot] = rec))
        if (!took?.length) continue
        o.log(`slot ${me.slot} handed to ${rec.name} for ${job.name}`)
        if (await makeBox(ledger, shared, me.slot, rec, head, o.log)) return leave(`its slot handed to ${rec.name}`)
        if (shared.limits.timing) return leave('no box could be made for the work left, and under --timing this one cannot take it')
        for (;;) {
          const r = await ledger.read()
          // (only its own hand-off taken back: never a slot someone else replaced meanwhile)
          if (r.state.boxes[me.slot]?.name !== rec.name) break
          const back = await propose(ledger, r.head, shared, r.state, [{op: 'replace', box: me.slot, cpu: own.size.vcpus, mem: own.size.memMiB - SYSTEM_MIB, kind: mine.kind}], (st) => (st.boxes[me.slot] = {...own, at: Date.now()}))
          if (back !== undefined) break
        }
        await sleep(30)
        continue
      }
      // (stay: the one watching the others)
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
