#!/usr/bin/env bun
// supercomputer on instabox.
//   supercomputer run --jobs jobs.json --out runs/x [--max-boxes 50] [--size large] [--timing] [--strict-once]
//   supercomputer status            the warm pool and the boxes running for this client
//   supercomputer reap [--keep 30m] delete paused boxes of the warm pool past their keep
//   supercomputer image build       the repo's snapshot, from supercomputer.toml's [instabox] setup

import {existsSync, readFileSync} from 'node:fs'
import {join, resolve} from 'node:path'
import {parseArgs} from 'node:util'
import * as ib from './instabox.ts'
import {readConfig, readJobs, sizeOf} from './jobs.ts'
import {owner, reap, run} from './run.ts'
import {pack, root} from './tree.ts'

const log = (s: string) => console.error(`${new Date().toISOString().slice(11, 19)} ${s}`)

function seconds(s: string): number {
  const m = /^(\d+)(s|m|h)?$/.exec(s)
  if (!m) throw new Error(`a duration: 90s, 30m, 2h (got ${s})`)
  return Number(m[1]) * {s: 1, m: 60, h: 3600}[(m[2] ?? 's') as 's' | 'm' | 'h']
}

async function main(argv: string[]): Promise<number> {
  const [cmd, ...rest] = argv
  if (cmd === 'run') {
    const {values: v} = parseArgs({
      args: rest,
      options: {
        jobs: {type: 'string'},
        out: {type: 'string'},
        'max-boxes': {type: 'string', default: '50'},
        'max-attempts': {type: 'string', default: '3'},
        size: {type: 'string'},
        'vm-type': {type: 'string'},
        snapshot: {type: 'string'},
        template: {type: 'string'},
        timing: {type: 'boolean', default: false},
        'strict-once': {type: 'boolean', default: false},
        poll: {type: 'string', default: '5s'},
        keep: {type: 'string', default: '30m'},
        env: {type: 'string', multiple: true, default: []},
        class: {type: 'string'},
      },
    })
    if (!v.jobs || !v.out) throw new Error('run needs --jobs FILE and --out DIR')
    const repo = await root(process.cwd())
    const cfg = readConfig(repo)
    const size = v.size ?? v['vm-type']
    if (size) cfg.size = sizeOf(size)
    for (const kv of v.env!) {
      const i = kv.indexOf('=')
      if (i < 1) throw new Error(`--env K=V (got ${kv})`)
      cfg.env[kv.slice(0, i)] = kv.slice(i + 1)
    }
    const jobs = readJobs(resolve(v.jobs))
    if (v['strict-once']) v['max-attempts'] = '1'
    const tree = await pack(repo, cfg.ignore)
    return run(jobs, cfg, tree, {snapshot: v.snapshot ?? cfg.snapshot, template: v.template ?? cfg.template}, {
      out: resolve(v.out),
      maxBoxes: Number(v['max-boxes']),
      maxAttempts: Number(v['max-attempts']),
      strictOnce: v['strict-once']!,
      timing: v.timing!,
      poll: seconds(v.poll!),
      keep: seconds(v.keep!),
      log,
    })
  }
  if (cmd === 'status') {
    const pool = await ib.list({sc: '1', sc_owner: owner()})
    const by = Map.groupBy(pool, (b) => `${b.state.padEnd(10)} ${b.vcpus}x${(b.memMiB ?? 0) / 1024}  ${b.metadata?.sc_image ?? ''}`)
    console.log(`client ${owner()}: ${pool.length} boxes`)
    for (const [k, bs] of by) console.log(`  ${String(bs.length).padStart(4)}  ${k}`)
    return 0
  }
  if (cmd === 'reap') {
    const {values: v} = parseArgs({args: rest, options: {keep: {type: 'string', default: '30m'}}})
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
    const hash = new Bun.CryptoHasher('sha256').update(cfg.template + '\0').update(script).digest('hex').slice(0, 12)
    const name = `sc-${(cfg.name ?? 'repo').toLowerCase().replace(/[^a-z0-9-]/g, '-')}-${hash}`
    const size = cfg.size ?? sizeOf('large')
    const box = await ib.create({name: `${name}-build`.slice(0, 63), vcpus: size.vcpus, memMiB: size.memMiB, template: cfg.template, meta: {sc: '1', sc_build: name}, autoSuspend: '30m'})
    log(`image ${name}: building on ${box.name} from ${cfg.template}`)
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
  console.error('usage: supercomputer run --jobs FILE --out DIR | status | reap [--keep 30m] | image build')
  return 2
}

main(process.argv.slice(2)).then(
  (code) => process.exit(code),
  (e) => {
    console.error(`supercomputer: ${e instanceof Error ? e.message : e}`)
    process.exit(1)
  },
)
