#!/usr/bin/env bun
// A stand-in for the instabox CLI, for test/run.test.ts: each box is a directory under $FAKE_INSTABOX_DIR (its home
// at <box>/home, /data/home in paths), `exec` runs bash there. A box whose box.json is gone is gone.
import {cpSync, existsSync, mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync} from 'node:fs'
import {dirname, join} from 'node:path'

const root = process.env.FAKE_INSTABOX_DIR!
type Box = {id: string; name: string; state: string; vcpus: number; memMiB: number; metadata: Record<string, string>; updatedAt: string; autoSuspendedAt: string | null}
const at = (id: string) => join(root, id)
const read = (id: string): Box | undefined => (existsSync(join(at(id), 'box.json')) ? JSON.parse(readFileSync(join(at(id), 'box.json'), 'utf8')) : undefined)
const write = (b: Box) => writeFileSync(join(at(b.id), 'box.json'), JSON.stringify(b))
const all = () => (existsSync(root) ? readdirSync(root).map(read).filter((b): b is Box => !!b) : [])
const find = (key: string) => all().find((b) => b.id === key || b.name === key)
const local = (b: Box, p: string) => join(at(b.id), 'home', p.replace(/^\/data\/home\/?/, ''))
const fail = (msg: string, code = 1): never => {
  console.error(`instabox: error ${msg}`)
  process.exit(code)
}
const log = (line: string) => writeFileSync(join(root, 'calls.log'), line + '\n', {flag: 'a'})

const [cmd, ...args] = process.argv.slice(2)
log([cmd, ...args.slice(0, 2)].join(' '))
const flag = (name: string) => {
  const i = args.indexOf(name)
  return i < 0 ? undefined : args[i + 1]
}
const metas = () => args.flatMap((a, i) => (args[i - 1] === '--meta' ? [a.split('=') as [string, string]] : []))

if (cmd === 'create') {
  mkdirSync(root, {recursive: true})
  const name = flag('--name')!
  if (find(name)) fail(`operation_conflict: a machine named "${name}" already exists`)
  const id = `m_${Math.random().toString(36).slice(2, 12)}`
  mkdirSync(join(at(id), 'home'), {recursive: true})
  const b: Box = {id, name, state: 'running', vcpus: Number(flag('--vcpus') ?? 4), memMiB: Number(flag('--mem-mib') ?? 8192), metadata: Object.fromEntries(metas()), updatedAt: new Date().toISOString(), autoSuspendedAt: null}
  write(b)
  console.log(JSON.stringify(b))
} else if (cmd === 'ls') {
  const want = metas()
  console.log(JSON.stringify(all().filter((b) => want.every(([k, v]) => b.metadata[k] === v))))
} else if (cmd === 'get') {
  const b = find(args[0]!) ?? fail('not_found: no such machine')
  console.log(JSON.stringify(b))
} else if (cmd === 'exec') {
  const b = find(args[0]!)
  if (!b) {
    console.log(JSON.stringify({error: {code: 'not_found', message: 'no such machine'}}))
    process.exit(1)
  }
  if (b.state !== 'running') {
    console.log(JSON.stringify({error: {code: 'not_running', message: `machine is ${b.state}`}}))
    process.exit(1)
  }
  const script = args.slice(args.indexOf('--') + 1)
  const home = join(at(b.id), 'home')
  const p = Bun.spawnSync(script, {cwd: home, env: {...process.env, HOME: home}, stdin: args.includes('--stdin') ? Bun.stdin : 'ignore'})
  console.log(JSON.stringify({stdout: p.stdout.toString(), stderr: p.stderr.toString(), exitCode: p.exitCode}))
} else if (cmd === 'push' || cmd === 'pull') {
  const b = find(args[0]!) ?? fail('not_found: no such machine')
  const [from, to] = cmd === 'push' ? [args[1]!, local(b, args[2]!)] : [local(b, args[1]!), args[2]!]
  if (!existsSync(from)) fail(`no ${from}`)
  mkdirSync(dirname(to), {recursive: true})
  cpSync(from, to, {recursive: true})
} else if (cmd === 'suspend' || cmd === 'resume') {
  const b = find(args[0]!) ?? fail('not_found: no such machine')
  write({...b, state: cmd === 'suspend' ? 'suspended' : 'running', updatedAt: new Date().toISOString(), autoSuspendedAt: cmd === 'suspend' ? new Date().toISOString() : null})
} else if (cmd === 'rm') {
  const b = find(args.find((a) => !a.startsWith('--'))!) ?? fail('not_found: no such machine')
  rmSync(at(b.id), {recursive: true, force: true})
} else {
  fail(`the stand-in has no ${cmd}`, 2)
}
