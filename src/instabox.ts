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

async function call(args: string[], stdin?: Blob | string): Promise<{stdout: string; stderr: string; code: number}> {
  const p = Bun.spawn([...bin(), ...args], {
    stdin: stdin === undefined ? 'ignore' : typeof stdin === 'string' ? new Blob([stdin]) : stdin,
    stdout: 'pipe',
    stderr: 'pipe',
    env: {...process.env, INSTABOX_NO_UPDATE_CHECK: '1'},
  })
  const [stdout, stderr, code] = await Promise.all([new Response(p.stdout).text(), new Response(p.stderr).text(), p.exited])
  return {stdout, stderr, code}
}

async function ok(args: string[], stdin?: Blob | string): Promise<string> {
  const r = await call(args, stdin)
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
}

export async function create(o: CreateOptions): Promise<Box> {
  const args = ['create', '--json', '--name', o.name, '--auto-suspend', o.autoSuspend ?? '5m']
  if (o.snapshot) args.push('--snapshot', o.snapshot)
  else args.push('--template', o.template ?? 'universal')
  if (o.gpu) args.push('--gpu', o.gpu, '--queue')
  else args.push('--vcpus', String(o.vcpus), '--mem-mib', String(o.memMiB))
  for (const [k, v] of Object.entries(o.meta)) args.push('--meta', `${k}=${v}`)
  return JSON.parse(await ok(args)) as Box
}

export async function list(meta: Record<string, string>): Promise<Box[]> {
  const args = ['ls', '--json']
  for (const [k, v] of Object.entries(meta)) args.push('--meta', `${k}=${v}`)
  return JSON.parse(await ok(args)) as Box[]
}

export async function get(box: string): Promise<Box> {
  return JSON.parse(await ok(['get', box, '--json'])) as Box
}

// One command in the box (bash -lc as its user), its exit code returned rather than thrown.
export async function exec(box: string, script: string, o: {stdin?: Blob | string; timeout?: number} = {}): Promise<Exec> {
  const args = ['exec', box, '--json', '--timeout', String(o.timeout ?? 120)]
  if (o.stdin !== undefined) args.push('--stdin')
  args.push('--', 'bash', '-c', script)
  const r = await call(args, o.stdin)
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

export const push = (box: string, local: string, remote: string) => ok(['push', box, local, remote])
export const pull = (box: string, remote: string, local: string) => ok(['pull', box, remote, local])
export const suspend = (box: string) => ok(['suspend', box])
export const resume = (box: string) => ok(['resume', box])
export const remove = (box: string) => ok(['rm', '--yes', box])
