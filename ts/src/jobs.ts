// A jobs file (the same as the Python supercomputer's) and a repo's supercomputer.toml, read for instabox.

import {existsSync, readFileSync} from 'node:fs'
import {join} from 'node:path'

export type Job = {
  name: string
  cmd: string
  cwd: string
  cpu: number
  memory: number // MiB
  timeout: number // seconds
  env: Record<string, string>
  gpu?: string // a100, a100:2, l4, t4, h100
  size?: Size // vm_type's, when the job names one
  group?: string // distinct_vm_group: each member on a different box
}

export type Size = {vcpus: number; memMiB: number}

// instabox's sizes (Team: all of them), smallest first; boat's vm_type names mapped onto them.
export const SIZES: Size[] = [
  {vcpus: 2, memMiB: 4096},
  {vcpus: 4, memMiB: 8192},
  {vcpus: 8, memMiB: 16384},
  {vcpus: 16, memMiB: 32768},
  {vcpus: 32, memMiB: 65536},
]
const VM_TYPES: Record<string, Size> = {small: SIZES[0]!, default: SIZES[1]!, large: SIZES[2]!, xlarge: SIZES[3]!, '2xlarge': SIZES[4]!}

export function sizeOf(name: string): Size {
  const m = /^(\d+)x(\d+)$/.exec(name)
  const s = m ? SIZES.find((s) => s.vcpus === Number(m[1]) && s.memMiB === Number(m[2]) * 1024) : VM_TYPES[name]
  if (!s) throw new Error(`unknown size ${name}: one of ${Object.keys(VM_TYPES).join(', ')} or ${SIZES.map((s) => `${s.vcpus}x${s.memMiB / 1024}`).join(', ')}`)
  return s
}

// The smallest size that holds the job (a few hundred MiB of the box's memory left to the system).
export function fits(job: Job): Size {
  if (job.size) return job.size
  const s = SIZES.find((s) => s.vcpus >= Math.ceil(job.cpu) && s.memMiB - 512 >= job.memory)
  if (!s) throw new Error(`${job.name}: no instabox size holds ${job.cpu} vCPUs and ${job.memory} MiB (largest 32 x 64 GiB)`)
  return s
}

const REFUSED = ['backend', 'runtime', 'cpu_model', 'image']

export function readJobs(file: string): Job[] {
  const raw = JSON.parse(readFileSync(file, 'utf8')) as Record<string, unknown>[]
  if (!Array.isArray(raw)) throw new Error(`${file}: a jobs file is a JSON array`)
  const seen = new Set<string>()
  return raw.map((j, i) => {
    const name = String(j.name ?? '')
    if (!/^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$/.test(name)) throw new Error(`${file}[${i}]: name ${JSON.stringify(name)}: letters, digits, . _ -`)
    if (seen.has(name)) throw new Error(`${file}: two jobs named ${name}`)
    seen.add(name)
    if (typeof j.cmd !== 'string' || !j.cmd) throw new Error(`${name}: no cmd`)
    for (const k of REFUSED)
      if (j[k] !== undefined && !(k === 'image' && j[k] === 'default'))
        throw new Error(`${name}: "${k}" is boat's or RunPod's; on instabox the image is the repo's snapshot, a CPU model is not exposed (lscpu says "AMD EPYC"), and GPUs are "gpu"`)
    const vm = j.vm_type === undefined ? undefined : sizeOf(String(j.vm_type))
    return {
      name,
      cmd: j.cmd,
      cwd: String(j.cwd ?? '.'),
      cpu: Number(j.cpu ?? 1),
      memory: Number(j.memory ?? 1024),
      timeout: Number(j.timeout ?? 3600),
      env: (j.env ?? {}) as Record<string, string>,
      gpu: j.gpu === undefined ? undefined : typeof j.gpu === 'string' ? j.gpu : `${(j.gpu as {type: string}).type}:${(j.gpu as {count?: number}).count ?? 1}`,
      size: vm,
      group: j.distinct_vm_group === undefined ? undefined : String(j.distinct_vm_group),
    }
  })
}

export type Config = {
  name?: string
  ignore: string[]
  path: string[]
  env: Record<string, string>
  pre: string
  size?: Size
  snapshot?: string
  template: string
  setup?: string
}

// supercomputer.toml at the repo's root: [boat]'s tree, PATH and env carry over; [instabox] names the snapshot
// (`supercomputer image build` makes it from `setup`) and may override the size.
export function readConfig(root: string): Config {
  const f = join(root, 'supercomputer.toml')
  const t = (existsSync(f) ? Bun.TOML.parse(readFileSync(f, 'utf8')) : {}) as Record<string, any>
  const boat = t.boat ?? {}
  const ib = t.instabox ?? {}
  const size = ib.size ?? ib.vm_type ?? boat.vm_type
  return {
    name: t.name,
    ignore: [...(boat.ignore ?? []), ...(ib.ignore ?? [])],
    path: ib.path ?? boat.path ?? [],
    env: {...(boat.env ?? {}), ...(ib.env ?? {})},
    pre: t.run?.pre ?? '',
    size: size ? sizeOf(String(size)) : undefined,
    snapshot: ib.snapshot,
    template: ib.template ?? 'universal',
    setup: ib.setup,
  }
}
