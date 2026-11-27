#!/usr/bin/env bun
// supercomputer: a repo's jobs on instabox boxes.
//   supercomputer run --jobs jobs.json --out runs/x [--max-boxes 50] [--size large] [--timing] [--strict-once]
//   supercomputer batch fetch runs/x   pick a batch up from runs/x/batch.json: fetch what finished, run the rest
//   supercomputer status               the warm pool and the boxes running for this client
//   supercomputer reap [--keep 30m] [--hold-keep 6h] [--until-empty]
//                                      delete paused boxes of the warm pool past their keep (and expired holds after pulling outputs)
//   supercomputer image build [NAME]   the repo's snapshot, from supercomputer.toml's [instabox] setup (NAME: one of
//                                      [instabox.images], its data and setup on the repo's image)
//   supercomputer --skill              how to use it, for an agent

import {existsSync, readFileSync, readdirSync, rmSync, statSync} from 'node:fs'
import {tmpdir} from 'node:os'
import {basename, dirname, join, resolve} from 'node:path'
import {parseArgs} from 'node:util'
import * as ib from './instabox.ts'
import {readConfig, readJobs, sizeOf} from './jobs.ts'
import {ensureReaper, reapOnce, reapUntilEmpty} from './reaper.ts'
import {lockedSizes, newBatch, owner, readBatch, run} from './run.ts'
import {fetchLedger, launch, peer} from './peer.ts'
import {pack, root} from './tree.ts'

const log = (s: string) => console.error(`${new Date().toISOString().slice(11, 19)} ${s}`)
const USAGE = 'usage: supercomputer run --jobs FILE --out DIR | launch --jobs FILE --ledger OWNER/REPO | batch fetch DIR | batch fetch --ledger OWNER/REPO#BATCH --out DIR | status | reap [--keep 30m] [--hold-keep 6h] [--until-empty] | image build [NAME] | --skill'

function seconds(s: string): number {
  const m = /^(\d+)(s|m|h)?$/.exec(s)
  if (!m) throw new Error(`a duration: 90s, 30m, 2h (got ${s})`)
  return Number(m[1]) * {s: 1, m: 60, h: 3600}[(m[2] ?? 's') as 's' | 'm' | 'h']
}

function count(s: string, what: string): number {
  const n = Number(s)
  if (!Number.isInteger(n) || n < 1) throw new Error(`${what}: a whole number from 1 (got ${s})`)
  return n
}

const q = (s: string) => `'${s.replaceAll("'", `'\\''`)}'`

// A local file's or folder's content as one sha256 (each file's path, mode and bytes, in order).
async function digest(p: string): Promise<string> {
  const h = new Bun.CryptoHasher('sha256')
  const walk = (d: string, rel: string) => {
    const st = statSync(d)
    if (!st.isDirectory()) return void h.update(`${rel}\0${st.mode & 0o111 ? 'x' : '-'}\0`).update(readFileSync(d))
    for (const e of readdirSync(d).sort()) walk(join(d, e), `${rel}/${e}`)
  }
  walk(p, '.')
  return h.digest('hex')
}

const loop = {poll: {type: 'string', default: '5s'}, keep: {type: 'string', default: '30m'}, 'hold-keep': {type: 'string', default: '6h'}} as const

async function main(argv: string[]): Promise<number> {
  const [cmd, ...rest] = argv
  if (cmd === '--skill' || cmd === 'skill') {
    console.log(readFileSync(join(import.meta.dir, '..', 'SKILL.md'), 'utf8'))
    return 0
  }
  if (cmd === 'run') {
    const {values: v} = parseArgs({
      args: rest,
      options: {
        jobs: {type: 'string'},
        out: {type: 'string'},
        'max-boxes': {type: 'string', default: '50'},
        'max-attempts': {type: 'string', default: '3'},
        size: {type: 'string'},
        timing: {type: 'boolean', default: false},
        'strict-once': {type: 'boolean', default: false},
        env: {type: 'string', multiple: true, default: []},
        ...loop,
      },
    })
    if (!v.jobs || !v.out) throw new Error('run needs --jobs FILE and --out DIR')
    const out = resolve(v.out)
    const repo = await root(process.cwd())
    const jobs = readJobs(resolve(v.jobs))
    const tree = await pack(repo, readConfig(repo).ignore)
    let batch = readBatch(out)
    if (batch) {
      const had = batch.jobs.map((j) => j.name).join('\n'), has = jobs.map((j) => j.name).join('\n')
      if (had !== has) throw new Error(`${out} holds batch ${batch.id} of other jobs: pick it up with \`supercomputer batch fetch ${v.out}\`, or use a new --out`)
      log(`batch ${batch.id}: picked up from ${join(out, 'batch.json')}`)
    } else {
      const cfg = readConfig(repo)
      if (v.size) cfg.size = sizeOf(v.size)
      for (const kv of v.env!) {
        const i = kv.indexOf('=')
        if (i < 1) throw new Error(`--env K=V (got ${kv})`)
        cfg.image.env[kv.slice(0, i)] = kv.slice(i + 1)
      }
      batch = newBatch(jobs, cfg, tree, {
        maxBoxes: count(v['max-boxes']!, '--max-boxes'),
        maxAttempts: count(v['max-attempts']!, '--max-attempts'),
        strictOnce: v['strict-once']!,
        timing: v.timing!,
      }, await lockedSizes(cfg, jobs))
      log(`batch ${batch.id}: ${jobs.length} jobs, tree ${tree.hash} (${tree.files} files, ${(tree.bytes / 1e6).toFixed(1)} MB); results in ${out}`)
    }
    return run(batch, tree.hash === batch.tree ? tree : undefined, {out, poll: seconds(v.poll!), keep: seconds(v.keep!), holdKeep: seconds(v['hold-keep']!), log})
  }
  // Controller-less (src/peer.ts): the batch in a ledger, its boxes running it with no client.
  if (cmd === 'launch') {
    const {values: v} = parseArgs({
      args: rest,
      options: {
        jobs: {type: 'string'}, ledger: {type: 'string'},
        'max-boxes': {type: 'string', default: '50'}, 'max-attempts': {type: 'string', default: '3'},
        size: {type: 'string'}, timing: {type: 'boolean', default: false}, 'strict-once': {type: 'boolean', default: false},
        keep: loop.keep, 'hold-keep': loop['hold-keep'],
      },
    })
    if (!v.jobs || !v.ledger) throw new Error('launch needs --jobs FILE and --ledger OWNER/REPO')
    // (the warm pool reaps itself on the way in and out, as a run does; a controller-less batch's boxes are never its)
    const keep = seconds(v.keep!), holdKeep = seconds(v['hold-keep']!)
    await reapOnce(keep, log, new Set(), {holdKeep}).catch(() => undefined)
    const repo = await root(process.cwd())
    const cfg = readConfig(repo)
    if (v.size) cfg.size = sizeOf(v.size)
    const tree = await pack(repo, cfg.ignore)
    const where = await launch(readJobs(resolve(v.jobs)), cfg, tree, {
      maxBoxes: count(v['max-boxes']!, '--max-boxes'), maxAttempts: count(v['max-attempts']!, '--max-attempts'),
      strictOnce: v['strict-once']!, timing: v.timing!,
    }, v.ledger, log)
    console.log(where)
    const after = await reapOnce(keep, log, new Set(), {holdKeep}).catch(() => undefined)
    if (after && (after.remainingPaused > 0 || after.remainingHeld > 0)) ensureReaper(keep, holdKeep, log)
    log(`launched: the boxes run the batch now, this machine may go; results: supercomputer batch fetch --ledger ${where} --out DIR`)
    return 0
  }
  if (cmd === 'peer') {
    const {values: v} = parseArgs({args: rest, options: {ledger: {type: 'string'}, slot: {type: 'string'}, name: {type: 'string'}, poll: {type: 'string', default: '3s'}}})
    if (!v.ledger || v.slot === undefined || !v.name) throw new Error('peer needs --ledger, --slot and --name')
    await peer(v.ledger, {slot: Number(v.slot), name: v.name}, {log, poll: seconds(v.poll!)})
    return 0
  }
  if (cmd === 'batch' && rest[0] === 'fetch' && rest.includes('--ledger')) {
    const {values: v} = parseArgs({args: rest.slice(1), options: {ledger: {type: 'string'}, out: {type: 'string'}}})
    if (!v.ledger || !v.out) throw new Error('batch fetch --ledger OWNER/REPO#BATCH needs --out DIR')
    return fetchLedger(v.ledger, resolve(v.out), log)
  }
  if (cmd === 'batch' && rest[0] === 'fetch') {
    const {values: v, positionals: p} = parseArgs({args: rest.slice(1), allowPositionals: true, options: loop})
    if (p.length !== 1) throw new Error("batch fetch needs the batch's --out directory")
    const out = resolve(p[0]!)
    const batch = readBatch(out)
    if (!batch) throw new Error(`no batch.json in ${out}`)
    // (its tree again, to send to a box that lacks it, when run from the repo it was packed from)
    const repo = await root(process.cwd()).catch(() => undefined)
    const tree = repo ? await pack(repo, batch.cfg.ignore).catch(() => undefined) : undefined
    log(`batch ${batch.id}: picked up from ${join(out, 'batch.json')}`)
    return run(batch, tree?.hash === batch.tree ? tree : undefined, {out, poll: seconds(v.poll!), keep: seconds(v.keep!), holdKeep: seconds(v['hold-keep']!), log})
  }
  if (cmd === 'status') {
    const pool = await ib.list({sc: '1', sc_owner: owner()})
    const by = Map.groupBy(pool, (b) => `${b.state.padEnd(10)} ${b.vcpus}x${(b.memMiB ?? 0) / 1024}  ${b.metadata?.sc_image ?? ''}`)
    console.log(`client ${owner()}: ${pool.length} boxes`)
    for (const [k, bs] of by) console.log(`  ${String(bs.length).padStart(4)}  ${k}`)
    return 0
  }
  if (cmd === 'reap') {
    const {values: v} = parseArgs({args: rest, options: {keep: loop.keep, 'hold-keep': loop['hold-keep'], 'until-empty': {type: 'boolean', default: false}}})
    const keep = seconds(v.keep!), holdKeep = seconds(v['hold-keep']!)
    if (v['until-empty']) {
      await reapUntilEmpty(keep, holdKeep, log)
      return 0
    }
    const r = await reapOnce(keep, log, new Set(), {holdKeep})
    log(`${r.deleted} paused boxes deleted`)
    if (r.remainingPaused > 0 || r.remainingHeld > 0) ensureReaper(keep, holdKeep, log)
    return 0
  }
  if (cmd === 'image' && rest[0] === 'build') {
    const repo = await root(process.cwd())
    const cfg = readConfig(repo)
    const which = rest[1]
    const img = which ? cfg.images[which] : cfg.image
    if (!img) throw new Error(`no [instabox.images.${which}] in supercomputer.toml`)
    if (!img.setup) throw new Error(`supercomputer.toml has no ${which ? `[instabox.images.${which}]` : '[instabox]'} setup = "path/to/setup.sh"`)
    // (a named image is built on the repo's image, its snapshot, unless it names a template of its own)
    const base = which && !img.ownTemplate ? cfg.image.snapshot ?? `template:${cfg.image.template}` : `template:${img.template}`
    if (which && !img.ownTemplate && !cfg.image.snapshot) log(`image ${which}: the repo has no [instabox] snapshot yet, so it builds on the ${cfg.image.template} template alone`)
    const setup = join(repo, img.setup)
    if (!existsSync(setup)) throw new Error(`no ${img.setup}`)
    const h = new Bun.CryptoHasher('sha256').update(base.replace(/^template:/, '') + '\0').update(readFileSync(setup))
    for (const [to, from] of Object.entries(img.data).sort()) {
      if (!existsSync(join(repo, from))) throw new Error(`[instabox.images.${which}] data: no ${from} in ${repo}`)
      h.update(`\0${to}\0${await digest(join(repo, from))}`)
    }
    const hash = h.digest('hex').slice(0, 12)
    const name = `sc-${(cfg.name ?? 'repo').toLowerCase().replace(/[^a-z0-9-]/g, '-')}${which ? `-${which}` : ''}-${hash}`
    const size = cfg.size ?? sizeOf('large')
    const from = base.startsWith('template:') ? {template: base.slice(9)} : {snapshot: base}
    const box = await ib.create({name: `${name}-build`.slice(0, 63), vcpus: size.vcpus, memMiB: size.memMiB, ...from, meta: {sc: '1', sc_build: name}, autoSuspend: '30m'})
    log(`image ${name}: building on ${box.name} from ${base.replace(/^template:/, 'the template ')}`)
    try {
      // (the data first, at its paths, so the setup can use it; each sent as one tarball, since instabox push copies only
      // what git tracks from a folder in a repo, and data is usually ignored; unpacked under ~/.sc-data, moved there by root)
      for (const [i, [to, from]] of Object.entries(img.data).sort().entries()) {
        const src = join(repo, from), tgz = join(tmpdir(), `sc-data-${process.pid}-${i}.tgz`)
        const dir = statSync(src).isDirectory()
        // (COPYFILE_DISABLE: macOS's tar would add a ._ file of extended attributes beside each file)
        const t = Bun.spawnSync(['tar', '-czf', tgz, '-C', dir ? src : dirname(src), dir ? '.' : basename(src)], {stderr: 'pipe', env: {...process.env, COPYFILE_DISABLE: '1'}})
        if (t.exitCode !== 0) throw new Error(`tar ${from}: ${t.stderr}`)
        log(`image ${name}: ${from} -> ${to} (${(statSync(tgz).size / 1e6).toFixed(1)} MB)`)
        const tmp = `/data/home/.sc-data/${i}`
        try {
          const r = await ib.exec(box.id, `rm -rf ${q(tmp)} && mkdir -p ${q(tmp)}`)
          if (r.exitCode !== 0) throw new Error(r.stderr)
          await ib.push(box.id, tgz, `${tmp}.tgz`)
        } finally {
          rmSync(tgz, {force: true})
        }
        const what = dir ? q(tmp) : `${q(tmp)}/${q(basename(src))}`
        const mv = await ib.exec(box.id, `tar -xzf ${q(tmp + '.tgz')} -C ${q(tmp)} && rm ${q(tmp + '.tgz')} && sudo mkdir -p "$(dirname ${q(to)})" && sudo rm -rf ${q(to)} && sudo mv ${what} ${q(to)} && sudo chown -R "$(id -un)" ${q(to)} && find ${q(to)} -type f | wc -l`, {timeout: 600})
        if (mv.exitCode !== 0) throw new Error(`unpacking ${from} at ${to}: ${mv.stderr}`)
        log(`image ${name}: ${to}: ${mv.stdout.trim()} files`)
      }
      const p = Bun.spawn(['instabox', 'ssh', box.id, '--', 'bash', '-s'], {stdin: Bun.file(setup), stdout: 'inherit', stderr: 'inherit'})
      if ((await p.exited) !== 0) throw new Error(`${img.setup} exited ${p.exitCode}`)
      await ib.exec(box.id, 'rm -rf /data/home/.sc-data')
      const s = Bun.spawn(['instabox', 'snapshot', 'save', box.id, '--name', name], {stdout: 'inherit', stderr: 'inherit'})
      if ((await s.exited) !== 0) throw new Error('snapshot save failed')
    } finally {
      await ib.remove(box.id).catch(() => undefined)
    }
    console.log(name)
    log(`image ${name}: saved; set ${which ? `[instabox.images.${which}]` : '[instabox]'} snapshot = "${name}" in supercomputer.toml`)
    return 0
  }
  console.error(USAGE)
  return 2
}

main(process.argv.slice(2)).then(
  (code) => process.exit(code),
  (e) => {
    console.error(`supercomputer: ${e instanceof Error ? e.message : e}`)
    process.exit(1)
  },
)
