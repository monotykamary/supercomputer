// A batch's shared log: what lets a batch go on with no client and no controller. Anyone with the batch's ledger (its
// boxes, the laptop that launched it, a later `batch fetch`) reads it, replays its requests through the batch rule
// (src/batch.ts), and adds what the rule accepts. An append names the version it read; it is refused when another
// came first, so of two appends at the same point exactly one wins, and the other reads again and asks the rule anew.
// Every interleaving of participants is then one sequence of rule steps, and the rule's laws hold of it.
//
// On GitHub, a batch is the branch sc/<batch> of a repo: its commit's tree holds batch.json (what never changes: the
// jobs, the config, the limits), state.json (the log of accepted requests, the boxes' names, the jobs' times),
// tree.tgz (the repo's tree), sc.js (this program, for the boxes) and out/<job>.tgz (each answered job's log and
// outputs). An append is a commit made with createCommitOnBranch, which lands only while the branch still points at
// the commit read.

import {existsSync, mkdirSync, readFileSync, renameSync, rmdirSync, writeFileSync} from 'node:fs'
import {dirname, join} from 'node:path'
import type {Req} from './engine.ts'
import type {Size} from './jobs.ts'

// A box of the batch, by its slot: its instabox name, when it was claimed (ms), and what it was made as.
export type BoxRec = {name: string; at: number; size: Size; image?: string; gpu?: string}
export type Time = {started?: number; ended?: number; where?: string}
export type State = {log: Req[]; boxes: Record<string, BoxRec>; times: Record<string, Time>}
export type Files = Record<string, string | Uint8Array>

export interface Ledger {
  readonly where: string
  // The batch's first version: its files and its state; refused when the batch exists.
  create(files: Files, state: State): Promise<void>
  // The latest version: its id and state.
  read(): Promise<{head: string; state: State}>
  // A file of a version (the latest when none is named).
  file(path: string, head?: string): Promise<Uint8Array>
  // The next version after `head`: the new state and any files it adds; undefined when `head` is no longer the latest.
  append(head: string, state: State, files?: Files, message?: string): Promise<string | undefined>
}

const text = (b: Uint8Array) => new TextDecoder().decode(b)
const bytes = (f: string | Uint8Array) => (typeof f === 'string' ? new TextEncoder().encode(f) : f)

// "owner/repo#batch" (GitHub) or "dir:/path#batch" (a folder, for tests and one machine).
export function openLedger(spec: string, token?: string): Ledger {
  const i = spec.lastIndexOf('#')
  if (i < 1) throw new Error(`a ledger is owner/repo#batch or dir:/path#batch (got ${spec})`)
  const [where, batch] = [spec.slice(0, i), spec.slice(i + 1)]
  if (!/^[A-Za-z0-9._-]+$/.test(batch)) throw new Error(`a batch id of letters, digits, dots and dashes (got ${batch})`)
  if (where.startsWith('dir:')) return new DirLedger(where.slice(4), batch)
  if (!/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/.test(where)) throw new Error(`a GitHub repo is owner/repo (got ${where})`)
  return new GitHubLedger(where, batch, token ?? githubToken())
}

// The token: SC_LEDGER_TOKEN (on a box, the placeholder instabox's proxy swaps for the real one), else gh's.
function githubToken(): string {
  if (process.env.SC_LEDGER_TOKEN) return process.env.SC_LEDGER_TOKEN
  const p = Bun.spawnSync(['gh', 'auth', 'token'], {stdout: 'pipe', stderr: 'pipe'})
  const t = p.stdout.toString().trim()
  if (p.exitCode !== 0 || !t) throw new Error('no GitHub token for the ledger: set SC_LEDGER_TOKEN or sign in with `gh auth login`')
  return t
}

class GitHubLedger implements Ledger {
  readonly where: string
  private readonly base: string
  private readonly branch: string
  private etag?: string
  private beaten = false
  private cached?: {head: string; state: State}

  constructor(readonly repo: string, readonly batch: string, private readonly token: string) {
    this.where = `${repo}#${batch}`
    this.base = `https://api.github.com/repos/${repo}`
    this.branch = `sc/${batch}`
  }

  private async call(url: string, init: {method: string; body?: unknown; headers?: Record<string, string>}): Promise<Response> {
    for (let attempt = 0; ; attempt++) {
      const r = await fetch(url, {
        method: init.method,
        headers: {
          Authorization: `Bearer ${this.token}`,
          Accept: 'application/vnd.github+json',
          'X-GitHub-Api-Version': '2022-11-28',
          'User-Agent': 'supercomputer',
          ...(init.body === undefined ? {} : {'Content-Type': 'application/json'}),
          ...init.headers,
        },
        body: init.body === undefined ? undefined : JSON.stringify(init.body),
      }).catch((e: unknown) => e as Error)
      // (a network error or GitHub's 5xx: tried again, up to 5 times, a little later each time)
      if (r instanceof Error || r.status >= 500) {
        if (attempt >= 4) throw r instanceof Error ? r : new Error(`GitHub ${init.method} ${url}: ${r.status}`)
        await Bun.sleep(500 * 2 ** attempt)
        continue
      }
      return r
    }
  }

  private api = (method: string, path: string, body?: unknown, headers?: Record<string, string>) => this.call(`${this.base}${path}`, {method, body, headers})

  // A commit on the batch's branch after `expected`, or undefined when the branch no longer points there. GitHub's
  // createCommitOnBranch checks expectedHeadOid where it moves the branch: of several at one head exactly one lands
  // (the REST API's ref update does not: it takes a non-fast-forward even with force false).
  private async commitOn(expected: string, files: Files, message: string): Promise<string | undefined> {
    const additions = Object.entries(files).map(([path, f]) => ({path, contents: Buffer.from(bytes(f)).toString('base64')}))
    const query = `mutation($input: CreateCommitOnBranchInput!) { createCommitOnBranch(input: $input) { commit { oid } } }`
    const input = {branch: {repositoryNameWithOwner: this.repo, branchName: this.branch}, expectedHeadOid: expected, message: {headline: message}, fileChanges: {additions}}
    const r = await this.call('https://api.github.com/graphql', {method: 'POST', body: {query, variables: {input}}})
    const d = (await r.json()) as {data?: {createCommitOnBranch?: {commit: {oid: string}} | null}; errors?: {type?: string; message: string}[]}
    if (d.errors?.some((e) => e.type === 'STALE_DATA')) return undefined
    const oid = d.data?.createCommitOnBranch?.commit.oid
    if (!oid) throw new Error(`the ledger ${this.where}: ${r.status} ${d.errors?.map((e) => e.message).join('; ') ?? 'no commit'}`)
    return oid
  }

  async create(files: Files, state: State): Promise<void> {
    // (the git database of an empty repo takes nothing: its first commit, a README, made once)
    let main = await this.api('GET', '/git/ref/heads/main')
    if (main.status !== 200) {
      await this.api('PUT', '/contents/README.md', {message: 'supercomputer batch ledgers', content: Buffer.from('Batch ledgers of supercomputer: a branch sc/<batch> each.\n').toString('base64')})
      main = await this.api('GET', '/git/ref/heads/main')
    }
    const root = ((await main.json()) as {object: {sha: string}}).object.sha
    const r = await this.api('POST', '/git/refs', {ref: `refs/heads/${this.branch}`, sha: root})
    if (r.status !== 201) throw new Error(`the ledger ${this.where}: ${r.status} ${(await r.text()).slice(0, 200)}`)
    const sha = await this.commitOn(root, {...files, 'state.json': JSON.stringify(state)}, `batch ${this.batch}`)
    if (!sha) throw new Error(`the ledger ${this.where}: another made it first`)
    this.cached = {head: sha, state}
  }

  async read(): Promise<{head: string; state: State}> {
    let head: string
    if (this.beaten) {
      // (just beaten by another append: the head from GraphQL, which answers from where the branch moved; the REST
      // answer may still be the old one for a while)
      const [owner, name] = this.repo.split('/')
      const query = `query($o: String!, $n: String!, $q: String!) { repository(owner: $o, name: $n) { ref(qualifiedName: $q) { target { oid } } } }`
      const r = await this.call('https://api.github.com/graphql', {method: 'POST', body: {query, variables: {o: owner, n: name, q: `refs/heads/${this.branch}`}}})
      const d = (await r.json()) as {data?: {repository?: {ref?: {target: {oid: string}} | null}}}
      const oid = d.data?.repository?.ref?.target.oid
      if (!oid) throw new Error(`the ledger ${this.where}: no branch ${this.branch}`)
      head = oid
      this.beaten = false
    } else {
      // (unchanged since the last read: GitHub answers 304, which costs no rate limit)
      const r = await this.api('GET', `/git/ref/heads/${this.branch}`, undefined, this.etag && this.cached ? {'If-None-Match': this.etag} : {})
      if (r.status === 304 && this.cached) return {head: this.cached.head, state: structuredClone(this.cached.state)}
      if (r.status !== 200) throw new Error(`the ledger ${this.where}: ${r.status} ${(await r.text()).slice(0, 200)}`)
      head = ((await r.json()) as {object: {sha: string}}).object.sha
      this.etag = r.headers.get('etag') ?? undefined
    }
    if (this.cached?.head !== head) this.cached = {head, state: JSON.parse(text(await this.file('state.json', head))) as State}
    return {head, state: structuredClone(this.cached.state)}
  }

  async file(path: string, head?: string): Promise<Uint8Array> {
    const ref = head ?? (await this.read()).head
    const r = await this.api('GET', `/contents/${path}?ref=${ref}`, undefined, {Accept: 'application/vnd.github.raw'})
    if (r.status !== 200) throw new Error(`the ledger ${this.where}: ${path}: ${r.status}`)
    return new Uint8Array(await r.arrayBuffer())
  }

  async append(head: string, state: State, files: Files = {}, message = 'step'): Promise<string | undefined> {
    const sha = await this.commitOn(head, {...files, 'state.json': JSON.stringify(state)}, message)
    if (sha) this.cached = {head: sha, state: structuredClone(state)}
    else this.beaten = true // (the next read asks where the branch is now)
    return sha
  }
}

// The same in a folder: versions as numbered state files, an append made under a lock (mkdir is atomic).
class DirLedger implements Ledger {
  readonly where: string
  private readonly dir: string

  constructor(root: string, batch: string) {
    this.dir = join(root, batch)
    this.where = `dir:${root}#${batch}`
  }

  private headFile = () => join(this.dir, 'HEAD')
  private head = () => readFileSync(this.headFile(), 'utf8').trim()

  private put(n: string, state: State, files: Files) {
    for (const [p, f] of Object.entries(files)) {
      const f2 = join(this.dir, 'files', p)
      mkdirSync(dirname(f2), {recursive: true})
      writeFileSync(f2, bytes(f))
    }
    writeFileSync(join(this.dir, `state-${n}.json`), JSON.stringify(state))
    writeFileSync(`${this.headFile()}.tmp`, n)
    renameSync(`${this.headFile()}.tmp`, this.headFile())
  }

  private async locked<T>(f: () => T): Promise<T> {
    const lock = join(this.dir, 'lock')
    for (let i = 0; ; i++) {
      try {
        mkdirSync(lock)
        break
      } catch {
        if (i > 2000) throw new Error(`the ledger ${this.where} stays locked`)
        await Bun.sleep(5)
      }
    }
    try {
      return f()
    } finally {
      rmdirSync(lock)
    }
  }

  async create(files: Files, state: State): Promise<void> {
    if (existsSync(this.headFile())) throw new Error(`the ledger ${this.where} exists`)
    mkdirSync(this.dir, {recursive: true})
    this.put('1', state, files)
  }

  async read(): Promise<{head: string; state: State}> {
    const head = this.head()
    return {head, state: JSON.parse(readFileSync(join(this.dir, `state-${head}.json`), 'utf8')) as State}
  }

  async file(path: string): Promise<Uint8Array> {
    return new Uint8Array(readFileSync(join(this.dir, 'files', path)))
  }

  append(head: string, state: State, files: Files = {}): Promise<string | undefined> {
    return this.locked(() => {
      if (this.head() !== head) return undefined
      const next = String(Number(head) + 1)
      this.put(next, state, files)
      return next
    })
  }
}
