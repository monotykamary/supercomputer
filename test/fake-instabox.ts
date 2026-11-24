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

// instabox's GPU templates, as `instabox templates --json` lists them (the parts the runner reads)
const GPU_TYPES = [
  {id: 'gpu', gpuTypes: [{type: 'l4', sizes: [{vcpus: 11, memMiB: 36864}], counts: [1, 2, 4]}, {type: 't4', sizes: [{vcpus: 7, memMiB: 24576}], counts: [1, 2, 4]}]},
  {id: 'gpu-metal', gpuTypes: [{type: 'h100', sizes: [{vcpus: 44, memMiB: 229376}], counts: [1]}]},
]

// The account's snapshots: $FAKE_INSTABOX_DIR/snapshots.json, as `instabox snapshot ls --json` lists them (none when absent)
type Snap = {name: string; sizeLocked?: boolean; defaultSpec: {vcpus: number; memMiB: number}}
const snaps = (): Snap[] => (existsSync(join(root, 'snapshots.json')) ? JSON.parse(readFileSync(join(root, 'snapshots.json'), 'utf8')) : [])

const [cmd, ...args] = process.argv.slice(2)
log([cmd, ...args.slice(0, 2)].join(' '))
const flag = (name: string) => {
  const i = args.indexOf(name)
  return i < 0 ? undefined : args[i + 1]
}

// Transport failures on request: $FAKE_INSTABOX_DIR/faults.json, {"<kind>": [mode, ...]}, the next calls of that
// kind failing in those modes, one each, in order. A kind is a command (create, exec, push, pull, get, ls, snapshot,
// suspend, resume, rm) or "exec:<text>", an exec whose script holds the text (checked first). A mode is how the call
// fails, as instabox's CLI says it: before it does anything (timeout, reset, refused, eof, 502, stall, hang: no
// answer at all, until the caller's deadline kills it) or after it did what it was asked (timeout-after,
// reset-after; broken-after: a create whose box then fails; pending-after: one whose box never comes up). Each failing call is logged to calls.log as "fault".
const MODES: Record<string, string> = {
  timeout: 'instabox: error cli_error: Get "https://api.instabox.sh/v1/operations/op_fake?wait=25s": context deadline exceeded (Client.Timeout exceeded while awaiting headers)',
  reset: 'instabox: error cli_error: Get "https://api.instabox.sh/v1/snapshots": read tcp 10.0.0.1:5000->10.0.0.2:443: read: connection reset by peer',
  refused: 'instabox: error cli_error: Post "https://api.instabox.sh/v1/machines": dial tcp 10.0.0.2:443: connect: connection refused',
  eof: 'instabox: error cli_error: Post "https://api.instabox.sh/v1/machines/m_x/exec": EOF',
  '502': 'instabox: error http_502: 502 Bad Gateway',
  stall: 'instabox: push: the upload stalled: no bytes went out for 2m; nothing was written in the sandbox (a file there is only replaced once every byte has arrived), so it is safe to run the same command again',
  'reset-after': "the connection to the API closed after 0s, before the command's result came (Post \"https://api.instabox.sh/v1/machines/m_x/exec\": read tcp 10.0.0.1:5000->10.0.0.2:443: read: connection reset by peer); the server stops a command whose request is gone, so it may not have finished",
}
MODES['timeout-after'] = MODES.timeout!
MODES['broken-after'] = MODES.timeout!
MODES['pending-after'] = MODES.timeout!
function nextFault(): string | undefined {
  const f = join(root, 'faults.json'), lock = join(root, 'faults.lock')
  if (!existsSync(f)) return undefined
  for (let i = 0; ; i++) {
    try {
      mkdirSync(lock)
      break
    } catch {
      if (i > 2000) return undefined
      Bun.sleepSync(2)
    }
  }
  try {
    const faults = JSON.parse(readFileSync(f, 'utf8')) as Record<string, string[]>
    const script = cmd === 'exec' ? args.slice(args.indexOf('--') + 1).join(' ') : ''
    const kind = Object.keys(faults).find((k) => k.startsWith(`${cmd}:`) && script.includes(k.slice(cmd!.length + 1)) && faults[k]!.length) ??
      (faults[cmd!]?.length ? cmd : undefined)
    if (!kind) return undefined
    const mode = faults[kind]!.shift()!
    writeFileSync(f, JSON.stringify(faults))
    log(`fault ${kind} ${mode}`)
    return mode
  } finally {
    rmSync(lock, {recursive: true, force: true})
  }
}
const fault = root ? nextFault() : undefined
const failWith = (mode: string): never => {
  if (cmd === 'exec' && mode === 'reset-after') {
    process.stdout.write(JSON.stringify({error: {code: 'cli_error', message: MODES[mode]}}) + '\n')
    process.exit(1)
  }
  process.stderr.write(`${MODES[mode]}\n`)
  process.exit(1)
}
if (fault === 'hang') Bun.sleepSync(3_600_000)
else if (fault && !fault.endsWith('-after')) failWith(fault)
// (a fault after the act: the act done, its answer never reaching the caller)
const after = fault?.endsWith('-after') ? fault : undefined
if (after) console.log = () => {}
const metas = () => args.flatMap((a, i) => (args[i - 1] === '--meta' ? [a.split('=') as [string, string]] : []))

if (cmd === 'create') {
  mkdirSync(root, {recursive: true})
  const name = flag('--name')!
  // (an idempotency key seen before: the first create's box again, nothing more made; FAKE_FORGET_KEYS: instabox
  // no longer knows the key, so a create made again finds its name taken)
  const key = flag('--idempotency-key'), keyFile = key ? join(root, 'keys', key) : ''
  const first = key && !process.env.FAKE_FORGET_KEYS && existsSync(keyFile) ? read(readFileSync(keyFile, 'utf8')) : undefined
  if (first) {
    log(`replayed ${name}`)
    console.log(JSON.stringify(first))
    if (after) failWith(after)
    process.exit(0)
  }
  if (find(name)) fail(`operation_conflict: a machine named "${name}" already exists`)
  const id = `m_${Math.random().toString(36).slice(2, 12)}`
  mkdirSync(join(at(id), 'home'), {recursive: true})
  // (a GPU box: the size of its type's, as instabox's templates say, times the count)
  const gpu = flag('--gpu'), [type, n] = (gpu ?? '').split(':')
  const g = gpu ? GPU_TYPES.flatMap((t) => t.gpuTypes).find((x) => x.type === type) : undefined
  if (gpu && (!g || flag('--template') !== GPU_TYPES.find((t) => t.gpuTypes.includes(g))!.id)) fail(`bad_request: no ${type} GPUs on template ${flag('--template')}`)
  const count = Number(n ?? 1)
  // (a snapshot saved with sizeLocked makes boxes of its size only, as instabox's create refuses any other)
  const snap = flag('--snapshot') ? snaps().find((x) => x.name === flag('--snapshot')) : undefined
  if (flag('--snapshot') && !snap) fail(`not_found: no snapshot ${flag('--snapshot')}`)
  if (snap?.sizeLocked && (Number(flag('--vcpus')) !== snap.defaultSpec.vcpus || Number(flag('--mem-mib')) !== snap.defaultSpec.memMiB))
    fail(`spec_not_allowed: snapshot ${snap.name} is size-locked to ${snap.defaultSpec.vcpus} vCPU / ${snap.defaultSpec.memMiB} MiB`, 5)
  const vcpus = g ? g.sizes[0]!.vcpus * count : Number(flag('--vcpus') ?? 4), memMiB = g ? g.sizes[0]!.memMiB * count : Number(flag('--mem-mib') ?? 8192)
  const b: Box = {id, name, state: after === 'broken-after' ? 'error' : after === 'pending-after' ? 'starting' : 'running', vcpus, memMiB, metadata: Object.fromEntries(metas()), updatedAt: new Date().toISOString(), autoSuspendedAt: null}
  write(b)
  if (key) mkdirSync(join(root, 'keys'), {recursive: true}), writeFileSync(keyFile, id)
  log(`created ${name} template=${flag('--template') ?? '-'} gpu=${gpu ?? '-'} ${vcpus}x${memMiB}`)
  console.log(JSON.stringify(b))
} else if (cmd === 'snapshot' && args[0] === 'ls') {
  console.log(JSON.stringify(snaps()))
} else if (cmd === 'templates') {
  console.log(JSON.stringify({templates: [{id: 'universal'}, ...GPU_TYPES]}))
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
  // ($FAKE_INSTABOX_DIR/refuse-execs holding N: the next N execs refused for load, as instabox refuses past 256 at once)
  const rf = join(root, 'refuse-execs')
  const left = existsSync(rf) ? Number(readFileSync(rf, 'utf8')) : 0
  if (left > 0) {
    writeFileSync(rf, String(left - 1))
    console.log(JSON.stringify({error: {code: 'too_many_execs', message: 'too many concurrent execs (at most 256 per user)'}}))
    process.exit(5)
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
if (after) failWith(after)
