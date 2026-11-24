// The instabox CLI, typed: every call JSON in, JSON out. INSTABOX_BIN names another binary (the tests' stand-in).

export type Box = {
  id: string
  name: string
  state: string
  vcpus?: number
  memMiB?: number
  metadata?: Record<string, string>
  updatedAt?: string
  autoSuspendedAt?: string | null
}

export type Exec = {stdout: string; stderr: string; exitCode: number}

export class InstaboxError extends Error {
  constructor(readonly args: string[], readonly code: number, readonly stderr: string) {
    super(`instabox ${args.slice(0, 2).join(' ')} exited ${code}: ${stderr.trim().split('\n').slice(-2).join(' ')}`)
  }
}

const bin = () => (process.env.INSTABOX_BIN ?? 'instabox').split(' ')

type Result = {stdout: string; stderr: string; code: number}

// One call, killed (its own process, by PID) when it outlives its deadline: a stalled call is an answer of its own.
async function once(args: string[], stdin: Blob | string | undefined, deadline: number): Promise<Result> {
  const p = Bun.spawn([...bin(), ...args], {
    stdin: stdin === undefined ? 'ignore' : typeof stdin === 'string' ? new Blob([stdin]) : stdin,
    stdout: 'pipe',
    stderr: 'pipe',
    env: {...process.env, INSTABOX_NO_UPDATE_CHECK: '1'},
  })
  let stalled = false
  const t = setTimeout(() => ((stalled = true), p.kill('SIGKILL')), deadline)
  try {
    const [stdout, stderr, code] = await Promise.all([new Response(p.stdout).text(), new Response(p.stderr).text(), p.exited])
    if (stalled) return {stdout: '', stderr: `${stderr}\ninstabox ${args[0]} stalled: no answer in ${Math.round(deadline / 1000)} s, killed`, code: 124}
    return {stdout, stderr, code}
  } finally {
    clearTimeout(t)
  }
}

// Whether instabox answered: it exited 0 with no error, or (exec) it brought the command's own exit code, whatever
// that is. Only a call instabox did not answer is ever made again.
function answered(r: Result): boolean {
  let d: unknown
  try {
    d = JSON.parse(r.stdout)
  } catch {
    return r.code === 0
  }
  const o = d && typeof d === 'object' && !Array.isArray(d) ? (d as Record<string, unknown>) : undefined
  if (o?.error) return false
  return r.code === 0 || typeof o?.exitCode === 'number'
}

const said = (r: Result) => r.stderr + r.stdout

// instabox refusing a call for load (too many execs at once across the account, a rate limit): it ran nothing.
export const refusedForLoad = (r: Result) =>
  !answered(r) && /too many concurrent|rate.?limit|too many requests|\b429\b|try again (later|in)/i.test(said(r))

// The call's way to instabox failing (a timeout, a connection reset, refused or closed, an EOF, the gateway's 5xx, a
// stalled transfer): no answer, so whether the call ran on instabox's side is not known from it.
export const transportFailure = (r: Result) =>
  !answered(r) && /Client\.Timeout|deadline exceeded|i\/o timeout|(connection|operation) timed out|connection (was )?(reset|refused|closed)|connection to the API closed|reset by peer|broken pipe|\bEOF\b|TLS handshake timeout|no such host|name resolution|network is unreachable|stalled|\b50[234]\b|bad gateway|service unavailable|gateway time-?out/i.test(said(r))

// How a call may be made again. 'load': only when refused for load (it ran nothing). 'transport': also on a transport
// failure, for a call that is safe to make twice (a read, a push that replaces its file whole, a create under its
// idempotency key, an exec its script makes idempotent); an exec that is not stays 'load'.
type Again = 'load' | 'transport'

// The deadline of a call (ms): SC_DEADLINE_MS for all (the tests), else by what the call does.
const deadlineOf = (cmd: string, exec = 120) =>
  Number(process.env.SC_DEADLINE_MS ?? 0) ||
  1000 * (cmd === 'exec' ? exec + 120 : cmd === 'push' || cmd === 'pull' ? Number(process.env.SC_TRANSFER_DEADLINE_S ?? 900) : cmd === 'create' ? 1500 : 180)

// A call made until instabox answers it, or for up to about five minutes of waiting (2 s doubling to 60 s, jittered)
// while it is refused for load or, when the call allows it, fails in transport; never an attempt lost for either.
async function call(args: string[], stdin?: Blob | string, o: {again?: Again; deadline?: number} = {}): Promise<Result> {
  const base = Number(process.env.SC_REFUSAL_BACKOFF_MS ?? 2000)
  const budget = Number(process.env.SC_RETRY_BUDGET_MS ?? 300_000)
  const deadline = o.deadline ?? deadlineOf(args[0]!)
  for (let attempt = 0, waited = 0; ; attempt++) {
    const r = await once(args, stdin, deadline)
    if (!(refusedForLoad(r) || (o.again === 'transport' && transportFailure(r))) || waited >= budget) return r
    const wait = Math.min(base * 2 ** attempt, 60_000) * (0.75 + Math.random() / 2)
    await Bun.sleep(wait)
    waited += wait
  }
}

async function ok(args: string[], o: {stdin?: Blob | string; again?: Again} = {}): Promise<string> {
  const r = await call(args, o.stdin, {again: o.again ?? 'transport'})
  if (r.code !== 0) throw new InstaboxError(args, r.code, r.stderr || r.stdout)
  return r.stdout
}

export type CreateOptions = {
  name: string
  vcpus: number
  memMiB: number
  snapshot?: string
  template?: string
  gpu?: string
  meta: Record<string, string>
  autoSuspend?: string
  secrets?: string[] // instabox Secrets the box gets (a placeholder each; the proxy swaps the value in for its hosts)
  setupFile?: string // run in the box once it is up; create returns when it has
  deleteAfter?: string
}

// A box made under an idempotency key of its own (one per create, kept across the call's retries): a create that did
// not answer is made again and instabox gives back the first one's operation, never a second box. One that still did not answer (or a name
// already taken) is reconciled: the box of that name and this metadata, if instabox made it after all, is this
// create's once it runs, and is deleted (never left behind) when it doesn't come up.
export async function create(o: CreateOptions): Promise<Box> {
  const args = ['create', '--json', '--name', o.name, '--auto-suspend', o.autoSuspend ?? '5m', '--idempotency-key', `sc-${o.name}-${crypto.randomUUID().slice(0, 8)}`]
  if (o.snapshot) args.push('--snapshot', o.snapshot)
  else args.push('--template', o.template ?? 'universal')
  if (o.gpu) args.push('--gpu', o.gpu, '--queue')
  else args.push('--vcpus', String(o.vcpus), '--mem-mib', String(o.memMiB))
  for (const [k, v] of Object.entries(o.meta)) args.push('--meta', `${k}=${v}`)
  if (o.secrets?.length) args.push('--secrets', o.secrets.join(','))
  if (o.setupFile) args.push('--setup-file', o.setupFile)
  if (o.deleteAfter) args.push('--delete-after', o.deleteAfter)
  const r = await call(args, undefined, {again: 'transport'})
  if (r.code === 0) return JSON.parse(r.stdout) as Box
  if (transportFailure(r) || /already exists|operation_conflict/i.test(said(r))) {
    const b = await reconcile(o)
    if (b) return b
  }
  throw new InstaboxError(args, r.code, r.stderr || r.stdout)
}

const sameMeta = (b: Box, meta: Record<string, string>) => Object.entries(meta).every(([k, v]) => b.metadata?.[k] === v)
const absent = (e: unknown) => e instanceof InstaboxError && /not.?found|404|no such (machine|sandbox)/i.test(e.stderr)

// The box a create that did not answer may have made: waited for while it comes up (as long as a call is retried),
// then this create's; one that fails or never comes up is deleted. None of that name and metadata: undefined.
async function reconcile(o: CreateOptions): Promise<Box | undefined> {
  const budget = Number(process.env.SC_RETRY_BUDGET_MS ?? 300_000), step = Number(process.env.SC_REFUSAL_BACKOFF_MS ?? 2000)
  for (let waited = 0; ; ) {
    let b: Box
    try {
      b = await get(o.name)
    } catch (e) {
      if (absent(e)) return undefined
      throw e
    }
    if (!sameMeta(b, o.meta)) return undefined // (another's box of that name: not this create's, never touched)
    const setup = b.metadata?.setupStatus
    if (b.state === 'running' && (!o.setupFile || setup === 'succeeded')) return b
    const failed = ['error', 'failed', 'deleted', 'stopped'].includes(b.state) || setup === 'failed'
    if (failed || waited >= budget) {
      await remove(b.id).catch(() => undefined)
      return undefined
    }
    await Bun.sleep(Math.min(step * 2, 10_000))
    waited += Math.min(step * 2, 10_000)
  }
}

export async function list(meta: Record<string, string>): Promise<Box[]> {
  const args = ['ls', '--json']
  for (const [k, v] of Object.entries(meta)) args.push('--meta', `${k}=${v}`)
  return JSON.parse(await ok(args)) as Box[]
}

export async function get(box: string): Promise<Box> {
  return JSON.parse(await ok(['get', box, '--json'])) as Box
}

// One command in the box (bash -lc as its user), its exit code returned rather than thrown. An exec that fails in
// transport may have run (instabox stops a command whose request is gone, but not before it began): it is made
// again only when the caller says its script is idempotent, that is, safe to run twice; any other is raised.
export async function exec(box: string, script: string, o: {stdin?: Blob | string; timeout?: number; idempotent?: boolean} = {}): Promise<Exec> {
  const timeout = o.timeout ?? 120
  const args = ['exec', box, '--json', '--timeout', String(timeout)]
  if (o.stdin !== undefined) args.push('--stdin')
  args.push('--', 'bash', '-c', script)
  const r = await call(args, o.stdin, {again: o.idempotent ? 'transport' : 'load', deadline: deadlineOf('exec', timeout)})
  let d: Partial<Exec> & {error?: {code?: string; message?: string}}
  try {
    d = JSON.parse(r.stdout)
  } catch {
    throw new InstaboxError(args, r.code, r.stderr || r.stdout)
  }
  // (the box gone, not running, the call refused: instabox's error, never the command's exit)
  if (d.error || typeof d.exitCode !== 'number') throw new InstaboxError(args, r.code, d.error?.message ?? (r.stderr || r.stdout))
  return {stdout: d.stdout ?? '', stderr: d.stderr ?? '', exitCode: d.exitCode}
}

// A GPU box as instabox makes it: the template that has the type (h100 is gpu-metal's, the others gpu's) and its size,
// the type's vCPUs and memory per GPU times the count (`instabox templates --json`).
export type GpuBox = {template: string; vcpus: number; memMiB: number}
type Template = {id: string; gpuTypes?: {type: string; sizes: {vcpus: number; memMiB: number}[]; counts: number[]}[]}
// A snapshot as `instabox snapshot ls --json` lists it (the parts the runner reads): one saved with sizeLocked makes
// boxes of its defaultSpec's size only.
export type Snapshot = {name: string; status?: string; sizeLocked?: boolean; defaultSpec?: {vcpus: number; memMiB: number}}

export async function snapshots(): Promise<Snapshot[]> {
  return JSON.parse(await ok(['snapshot', 'ls', '--json'])) as Snapshot[]
}

export async function gpuBox(gpu: string): Promise<GpuBox> {
  const [type, n] = gpu.split(':')
  const count = Number(n ?? 1)
  const {templates} = JSON.parse(await ok(['templates', '--json'])) as {templates: Template[]}
  // (the plain template before its -inference twin, which has the same types)
  for (const t of [...templates].sort((a, b) => a.id.length - b.id.length)) {
    const g = t.gpuTypes?.find((x) => x.type === type)
    if (!g || !g.sizes[0]) continue
    if (!g.counts.includes(count)) throw new Error(`${gpu}: instabox makes ${type} boxes of ${g.counts.join(', ')} GPUs`)
    return {template: t.id, vcpus: g.sizes[0].vcpus * count, memMiB: g.sizes[0].memMiB * count}
  }
  const types = templates.flatMap((t) => t.gpuTypes?.map((x) => x.type) ?? [])
  throw new Error(`${gpu}: instabox has no ${type} GPUs (${[...new Set(types)].join(', ')})`)
}

// (a push replaces its file only once every byte has arrived, a pull writes a local file: both safe to make again)
export const push = (box: string, local: string, remote: string) => ok(['push', box, local, remote])
export const pull = (box: string, remote: string, local: string) => ok(['pull', box, remote, local])

// A change of state made again after a transport failure, and taken as made when the box is in the state it asks
// for (an earlier call that did not answer may have made it): the box running, paused, or gone.
async function settle(args: string[], done: (b: Box | undefined) => boolean): Promise<string> {
  try {
    return await ok(args)
  } catch (e) {
    const now = await get(args.at(-1)!).catch((x) => (absent(x) ? undefined : null))
    if (now !== null && done(now)) return ''
    throw e
  }
}
export const suspend = (box: string) => settle(['suspend', box], (b) => b?.state === 'suspended')
export const resume = (box: string) => settle(['resume', box], (b) => b?.state === 'running')
export const remove = (box: string) => settle(['rm', '--yes', box], (b) => !b || b.state === 'deleted')
