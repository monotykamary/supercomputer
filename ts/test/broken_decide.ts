// The bugs src/decide.ts rules out, kept as regressions: every law here must be refuted (test/check_laws.ts).

// A failing test retried as if its box were lost.
// gdp: law a native exit is never retried: !result || !exited
export function retryAnyFailure(exited: boolean, attempts: number, maxAttempts: number): boolean {
  return attempts < maxAttempts
}

// Strict-once work tried twice.
// gdp: law strict-once is never retried: !result || !strictOnce
export function retryStrict(exited: boolean, strictOnce: boolean, attempts: number, maxAttempts: number): boolean {
  return !exited && attempts < maxAttempts
}

// A box handed its next job before the last one's outputs were fetched.
// gdp: law a box is reused only after its outputs are verified: !result || verified
export function reuseOnExit(ended: boolean, verified: boolean): boolean {
  return ended
}

// A paused box deleted with its outputs still on it.
// gdp: law a box holding unfetched outputs is never deleted: !result || !holdsOutputs
export function letGoByAge(pausedFor: number, keep: number, holdsOutputs: boolean): boolean {
  return pausedFor >= keep
}

// A batch reported a success with a job never answered.
// gdp: law success needs every job answered: !result || answered == jobs
export function succeededOnNoFailure(jobs: number, answered: number, failed: number): boolean {
  return failed == 0
}
