// The runner's decisions, as plain code with its laws (`// gdp: law`), proven by gdp-wired
// (test/check_laws.ts: `gdp-wired-cli check`); test/broken_decide.ts keeps the bugs they rule out, each refuted.
//
// On boat, most of the old scheduler existed to ration starts and keep VMs busy (holds, filler, the treadmill).
// instabox bills by the second while a box runs and only disk while it is paused, and a paused box resumes in
// seconds, so what is left to decide is small: how many boxes may be live, when a job is tried again, when a
// box may take its next job, and when a paused box is let go.

// Whether a job that ended is tried again on another box. A native exit, zero or not, is the job's answer and
// never retried (a failing test is data); only a lost attempt (the box gone, the runner not answering) is, and
// only while attempts are left; strict-once work is never tried twice.
// gdp: law a native exit is never retried: !result || !exited
// gdp: law strict-once is never retried: !result || !strictOnce
// gdp: law attempts stay within their budget: !result || attempts < maxAttempts
export function retry(exited: boolean, strictOnce: boolean, attempts: number, maxAttempts: number): boolean {
  return !exited && !strictOnce && attempts < maxAttempts
}

// Whether a box may take another job (or be paused into the warm pool): only once the job it ran has ended and
// its log, exit and output files are fetched and verified; a box whose job is still out is never reused.
// gdp: law a box is reused only after its job ended: !result || ended
// gdp: law a box is reused only after its outputs are verified: !result || verified
export function reusable(ended: boolean, verified: boolean): boolean {
  return ended && verified
}

// Whether a paused box of the warm pool is deleted: once it has been paused past keep seconds, and never while
// it holds a job's unfetched outputs.
// gdp: law a box holding unfetched outputs is never deleted: !result || !holdsOutputs
// gdp: law a box is deleted only past its keep: !result || pausedFor >= keep
export function letGo(pausedFor: number, keep: number, holdsOutputs: boolean): boolean {
  return !holdsOutputs && pausedFor >= keep
}

// Whether the batch's command exits 0: every job answered with a native 0, none lost.
// gdp: law success needs every job answered: !result || answered == jobs
// gdp: law success needs no failed job: !result || failed == 0
export function succeeded(jobs: number, answered: number, failed: number): boolean {
  return answered == jobs && failed == 0
}
