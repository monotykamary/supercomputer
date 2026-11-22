// A jobs file and a repo's supercomputer.toml, read for instabox.

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
  image?: string // one of supercomputer.toml's [instabox.images]; else the repo's snapshot
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

const REFUSED = ['backend', 'runtime', 'cpu_model']

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
      if (j[k] !== undefined)
        throw new Error(`${name}: "${k}" is not instabox's: a box is the repo's snapshot (or one of [instabox.images]), a CPU model is not exposed (lscpu says "AMD EPYC"), and GPUs are "gpu"`)
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
      image: j.image === undefined || j.image === 'default' ? undefined : String(j.image),
    }
  })
}

// A box's image: an instabox snapshot (or a template), with what jobs on it add to PATH and the environment. A named
// image may have its own `setup` and `data` (a box path for each local file or folder, relative to the repo):
// `supercomputer image build NAME` puts the data at its paths on a box of the repo's image, runs the setup after it,
// and saves the snapshot.
export type Image = {snapshot?: string; template: string; path: string[]; env: Record<string, string>; setup?: string; data: Record<string, string>; ownTemplate?: boolean}

// Build outputs kept on each box between jobs and batches (a Lean library's .olean files): a job finds `outputs` in
// `dir` already there when the cache's key (its salt and every input) matches, and what it built is kept for the next.
export type Cache = {name: string; dir: string; inputs: string[]; outputs: string[]; salt: string}

export type Config = {
  name?: string
  ignore: string[]
  pre: string
  size?: Size
  setup?: string
  image: Image
  images: Record<string, Image>
  caches: Cache[]
}

// supercomputer.toml at the repo's root:
//   name = "repo"
//   [run] pre = "export X=1; "            (before every job's cmd)
//   [instabox]
//   template = "universal"                 (what `image build` starts from, and boxes when there is no snapshot)
//   setup = "supercomputer/setup.sh"       (`supercomputer image build` runs it and saves the snapshot)
//   snapshot = "sc-repo-0123456789ab"      (what boxes start from)
//   size = "large"                         (the smallest box a batch makes; a job's own size wins when bigger)
//   path = ["~/.bun/bin"], env = {A = "1"}, ignore = ["runs/**"]
//   [instabox.images.runner]               (a job's "image": "runner": another snapshot, its own path and env)
//   [instabox.images.chc] setup = "...", data = {"/bench" = "data/bench"}   (built on the repo's image by `image build chc`)
//   [[cache]] name, dir, modules, inputs = "{m}.lean", outputs = "{m}.olean", salt
export function readConfig(root: string): Config {
  const f = join(root, 'supercomputer.toml')
  const t = (existsSync(f) ? Bun.TOML.parse(readFileSync(f, 'utf8')) : {}) as Record<string, any>
  for (const k of ['boat', 'modal', 'treadmill', 'gpu', 'runpod'])
    if (t[k] !== undefined) throw new Error(`${f}: [${k}] is the old supercomputer's; this one runs on instabox only ([instabox])`)
  const ib = t.instabox ?? {}
  const image: Image = {snapshot: ib.snapshot, template: ib.template ?? 'universal', path: ib.path ?? [], env: ib.env ?? {}, setup: ib.setup, data: {}}
  const images: Record<string, Image> = {}
  for (const [k, v] of Object.entries((ib.images ?? {}) as Record<string, any>)) {
    if (!/^[a-z0-9][a-z0-9-]*$/.test(k)) throw new Error(`${f}: [instabox.images.${k}]: a name of lowercase letters, digits and dashes`)
    const data: Record<string, string> = {}
    for (const [path, from] of Object.entries((v.data ?? {}) as Record<string, unknown>)) {
      // (a trailing slash dropped: "/bench/" is /bench; never / itself, which the copy would replace)
      const to = path.replace(/\/+$/, '')
      if (!path.startsWith('/') || !to || to.split('/').some((p) => p === '..' || p === '.') || typeof from !== 'string')
        throw new Error(`${f}: [instabox.images.${k}] data: "/absolute/box/path" = "path/in/the/repo" (not "/" itself)`)
      data[to] = from
    }
    if (Object.keys(data).length && !v.setup) throw new Error(`${f}: [instabox.images.${k}] has data but no setup: its snapshot is built by \`supercomputer image build ${k}\``)
    images[k] = {snapshot: v.snapshot ?? (v.setup ? undefined : image.snapshot), template: v.template ?? image.template, ownTemplate: v.template !== undefined, path: [...(v.path ?? []), ...image.path], env: {...image.env, ...(v.env ?? {})}, setup: v.setup, data}
  }
  const caches = ((t.cache ?? []) as Record<string, any>[]).map((c): Cache => {
    const mods: string[] = c.modules ?? ['']
    const each = (p: string | string[] | undefined) => [p ?? []].flat().flatMap((x) => mods.map((m) => String(x).replaceAll('{m}', m)))
    if (!c.name || !c.dir || !c.inputs || !c.outputs) throw new Error(`${f}: a [[cache]] needs name, dir, inputs and outputs`)
    return {name: String(c.name), dir: String(c.dir), inputs: each(c.inputs), outputs: each(c.outputs), salt: String(c.salt ?? '')}
  })
  return {
    name: t.name,
    ignore: ib.ignore ?? [],
    pre: t.run?.pre ?? '',
    size: ib.size ? sizeOf(String(ib.size)) : undefined,
    setup: ib.setup,
    image,
    images,
    caches,
  }
}
