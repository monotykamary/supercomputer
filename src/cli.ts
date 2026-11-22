#!/usr/bin/env bun
// supercomputer: a repo's jobs on instabox boxes.
//   supercomputer run --jobs jobs.json --out runs/x [--max-boxes 50] [--size large] [--timing] [--strict-once]
//   supercomputer batch fetch runs/x   pick a batch up from runs/x/batch.json: fetch what finished, run the rest
//   supercomputer status               the warm pool and the boxes running for this client
//   supercomputer reap [--keep 30m]    delete paused boxes of the warm pool past their keep
//   supercomputer image build          the repo's snapshot, from supercomputer.toml's [instabox] setup
//   supercomputer --skill              how to use it, for an agent

import {existsSync, readFileSync} from 'node:fs'
import {join, resolve} from 'node:path'
import {parseArgs} from 'node:util'
import * as ib from './instabox.ts'
import {readConfig, readJobs, sizeOf} from './jobs.ts'
import {newBatch, owner, readBatch, reap, run} from './run.ts'
import {pack, root} from './tree.ts'

const log = (s: string) => console.error(`${new Date().toISOString().slice(11, 19)} ${s}`)
const USAGE = 'usage: supercomputer run --jobs FILE --out DIR | batch fetch DIR | status | reap [--keep 30m] | image build | --skill'

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

const loop = {poll: {type: 'string', default: '5s'}, keep: {type: 'string', default: '30m'}} as const

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
      })
      log(`batch ${batch.id}: ${jobs.length} jobs, tree ${tree.hash} (${tree.files} files, ${(tree.bytes / 1e6).toFixed(1)} MB); results in ${out}`)
    }
    return run(batch, tree.hash === batch.tree ? tree : undefined, {out, poll: seconds(v.poll!), keep: seconds(v.keep!), log})
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
    return run(batch, tree?.hash === batch.tree ? tree : undefined, {out, poll: seconds(v.poll!), keep: seconds(v.keep!), log})
  }
  if (cmd === 'status') {
    const pool = await ib.list({sc: '1', sc_owner: owner()})
    const by = Map.groupBy(pool, (b) => `${b.state.padEnd(10)} ${b.vcpus}x${(b.memMiB ?? 0) / 1024}  ${b.metadata?.sc_image ?? ''}`)
    console.log(`client ${owner()}: ${pool.length} boxes`)
    for (const [k, bs] of by) console.log(`  ${String(bs.length).padStart(4)}  ${k}`)
    return 0
  }
  if (cmd === 'reap') {
    const {values: v} = parseArgs({args: rest, options: {keep: loop.keep}})
    const n = await reap(seconds(v.keep!), log)
    log(`${n} paused boxes deleted`)
    return 0
  }
  if (cmd === 'image' && rest[0] === 'build') {
    const repo = await root(process.cwd())
    const cfg = readConfig(repo)
    if (!cfg.setup) throw new Error('supercomputer.toml has no [instabox] setup = "path/to/setup.sh"')
    const setup = join(repo, cfg.setup)
    if (!existsSync(setup)) throw new Error(`no ${cfg.setup}`)
    const script = readFileSync(setup)
    const hash = new Bun.CryptoHasher('sha256').update(cfg.image.template + '\0').update(script).digest('hex').slice(0, 12)
    const name = `sc-${(cfg.name ?? 'repo').toLowerCase().replace(/[^a-z0-9-]/g, '-')}-${hash}`
    const size = cfg.size ?? sizeOf('large')
    const box = await ib.create({name: `${name}-build`.slice(0, 63), vcpus: size.vcpus, memMiB: size.memMiB, template: cfg.image.template, meta: {sc: '1', sc_build: name}, autoSuspend: '30m'})
    log(`image ${name}: building on ${box.name} from ${cfg.image.template}`)
    try {
      const p = Bun.spawn(['instabox', 'ssh', box.id, '--', 'bash', '-s'], {stdin: Bun.file(setup), stdout: 'inherit', stderr: 'inherit'})
      if ((await p.exited) !== 0) throw new Error(`${cfg.setup} exited ${p.exitCode}`)
      const s = Bun.spawn(['instabox', 'snapshot', 'save', box.id, '--name', name], {stdout: 'inherit', stderr: 'inherit'})
      if ((await s.exited) !== 0) throw new Error('snapshot save failed')
    } finally {
      await ib.remove(box.id).catch(() => undefined)
    }
    console.log(name)
    log(`image ${name}: saved; set [instabox] snapshot = "${name}" in supercomputer.toml`)
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
