// The size a job's box gets, as a gdp-wired decision (proven in test/laws.test.ts); the runner and `launch` call its
// compiled runner, and a batch refuses, when it starts, every job its box can't hold.
//   - A box of a locked image (an instabox snapshot saved with sizeLocked) is that size, never another: instabox
//     refuses any other, and the jobs packed on a box it refused would be lost.
//   - Else the job's named size (vm_type); else the batch's size when it is bigger than the smallest size that holds
//     the job (not under --timing); else that smallest size.
//   - It holds the job when the job's vCPUs and its memory, with what the box's system keeps, fit in it.
// Inputs, all naturals or yes/no: the job's cpu (whole vCPUs) and mem (MiB); the image's locked size (locked, lv, lm);
// the job's named size (named, nv, nm); the batch's size (batch, bv, bm); timing; the smallest size that holds the job
// (fv, fm; 0 when none does).

import {bool, fun, nat, no, when} from 'gdp-wired'
import type {Config, Job, Size} from './jobs.ts'
import {SIZES} from './jobs.ts'

export const SYSTEM_MIB = 512 // a box's memory left to its system

export const boxSize = fun('the size a job box gets')
  .inputs({cpu: nat, mem: nat, locked: bool, lv: nat, lm: nat, named: bool, nv: nat, nm: nat, batch: bool, bv: nat, bm: nat, timing: bool, fv: nat, fm: nat})
  .returns(({cpu, mem, locked, lv, lm, named, nv, nm, batch, bv, bm, timing, fv, fm}) => {
    const bigger = named.not().and(timing.not(), batch, bv.gt(fv))
    const vcpus = when(locked, lv, when(named, nv, when(bigger, bv, fv)))
    const memMiB = when(locked, lm, when(named, nm, when(bigger, bm, fm)))
    return {vcpus, memMiB, holds: cpu.atMost(vcpus).and(mem.plus(SYSTEM_MIB).atMost(memMiB))}
  })

export const boxSizeLaws = [
  boxSize.law("a locked image's box is its own size, never another", (i) => [i.locked], (o, i) => o.vcpus.eq(i.lv).and(o.memMiB.eq(i.lm))),
  boxSize.law('a box is planned only when it holds the job', (_, o) => [o.holds], (o, i) => i.cpu.atMost(o.vcpus).and(i.mem.plus(SYSTEM_MIB).atMost(o.memMiB))),
  boxSize.law('a job its locked image holds is never refused', (i) => [i.locked, i.cpu.atMost(i.lv), i.mem.plus(SYSTEM_MIB).atMost(i.lm)], (o) => o.holds),
  boxSize.law('a job its locked image cannot hold is refused', (i, o) => [i.locked, i.cpu.gt(i.lv).or(i.mem.plus(SYSTEM_MIB).gt(i.lm)), o.holds], () => no),
  // (unlocked, unnamed: the smallest size that holds it, or a bigger batch size, which holds it too when it is as big
  // in memory as in vCPUs, as instabox's sizes are)
  boxSize.law('an unlocked job the smallest size holds is never refused',
    (i) => [i.locked.not(), i.named.not(), i.cpu.atMost(i.fv), i.mem.plus(SYSTEM_MIB).atMost(i.fm), i.batch.not().or(i.bv.atMost(i.fv), i.bm.atLeast(i.fm))],
    (o) => o.holds),
]

const runner = boxSize.compiled()

// The smallest of instabox's sizes that holds the job (undefined when none does).
export const smallest = (job: Job): Size | undefined =>
  SIZES.find((s) => s.vcpus >= Math.ceil(job.cpu) && s.memMiB - SYSTEM_MIB >= job.memory)

// Each image's locked size, by image name ('' the repo's): what `instabox snapshot ls` says of its snapshot.
export type Locked = Record<string, Size>

// The box a (non-GPU) job gets, and whether it holds the job.
export function planSize(job: Job, cfg: Config, locked: Locked, timing: boolean): Size & {holds: boolean} {
  const l = locked[job.image ?? '']
  const f = smallest(job)
  const o = runner({
    cpu: Math.ceil(job.cpu), mem: job.memory,
    locked: !!l, lv: l?.vcpus ?? 0, lm: l?.memMiB ?? 0,
    named: !!job.size, nv: job.size?.vcpus ?? 0, nm: job.size?.memMiB ?? 0,
    batch: !!cfg.size, bv: cfg.size?.vcpus ?? 0, bm: cfg.size?.memMiB ?? 0,
    timing, fv: f?.vcpus ?? 0, fm: f?.memMiB ?? 0,
  }, {}) as {vcpus: number; memMiB: number; holds: boolean}
  return {vcpus: o.vcpus, memMiB: o.memMiB, holds: o.holds}
}

// Every job its box can't hold, said when the batch starts (never a box instabox refuses, its jobs lost later).
export function refuseUnfit(jobs: Job[], cfg: Config, locked: Locked, timing: boolean): void {
  const bad = jobs.filter((j) => !j.gpu).flatMap((j) => {
    const s = planSize(j, cfg, locked, timing)
    if (s.holds) return []
    const l = locked[j.image ?? '']
    const where = l ? `image ${j.image ?? '(the repo\'s)'} is a locked snapshot of ${l.vcpus} vCPUs / ${l.memMiB} MiB` : j.size ? `its vm_type is ${s.vcpus} vCPUs / ${s.memMiB} MiB` : 'no instabox size holds it (largest 32 vCPUs / 64 GiB)'
    return [`${j.name}: asks ${Math.ceil(j.cpu)} vCPUs and ${j.memory} MiB (plus ${SYSTEM_MIB} MiB for the box's system), but ${where}`]
  })
  if (bad.length) throw new Error(`${bad.length} job${bad.length > 1 ? 's' : ''} no box can hold:\n  ${bad.join('\n  ')}`)
}
