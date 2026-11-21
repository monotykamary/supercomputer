// The repo's tree as one tarball named by the hash of its files: what git tracks (submodules too) and the untracked
// files it does not ignore, less supercomputer.toml's ignore globs. A box keeps each tree it was sent, so a batch on
// a warm box sends nothing again.

import {mkdtempSync, writeFileSync} from 'node:fs'
import {tmpdir} from 'node:os'
import {join} from 'node:path'

async function git(root: string, args: string[]): Promise<string[]> {
  const p = Bun.spawn(['git', ...args], {cwd: root, stdout: 'pipe', stderr: 'pipe'})
  const out = await new Response(p.stdout).text()
  if ((await p.exited) !== 0) throw new Error(`git ${args.join(' ')}: ${await new Response(p.stderr).text()}`)
  return out.split('\0').filter(Boolean)
}

export async function root(dir: string): Promise<string> {
  const p = Bun.spawn(['git', 'rev-parse', '--show-toplevel'], {cwd: dir, stdout: 'pipe', stderr: 'pipe'})
  const out = (await new Response(p.stdout).text()).trim()
  if ((await p.exited) !== 0) throw new Error(`${dir} is not in a git repository`)
  return out
}

export type Tree = {hash: string; tarball: string; files: number; bytes: number}

export async function pack(rootDir: string, ignore: string[]): Promise<Tree> {
  const globs = ignore.map((g) => new Bun.Glob(g))
  const tracked = await git(rootDir, ['ls-files', '-z', '--recurse-submodules'])
  const untracked = await git(rootDir, ['ls-files', '-z', '--others', '--exclude-standard'])
  const files = [...new Set([...tracked, ...untracked])].filter((f) => !globs.some((g) => g.match(f))).sort()
  const h = new Bun.CryptoHasher('sha256')
  let bytes = 0
  const kept: string[] = []
  for (const f of files) {
    const file = Bun.file(join(rootDir, f))
    if (!(await file.exists())) continue // deleted in the working tree, still in the index
    const body = await file.arrayBuffer()
    bytes += body.byteLength
    h.update(f + '\0')
    h.update(new Bun.CryptoHasher('sha256').update(body).digest('hex') + '\0')
    kept.push(f)
  }
  const hash = h.digest('hex').slice(0, 16)
  const dir = mkdtempSync(join(tmpdir(), 'sc-tree-'))
  const list = join(dir, 'files')
  writeFileSync(list, kept.join('\0'))
  const tarball = join(dir, `${hash}.tgz`)
  const p = Bun.spawn(['tar', '--null', '-T', list, '-czf', tarball], {cwd: rootDir, stderr: 'pipe', env: {...process.env, COPYFILE_DISABLE: '1'}})
  if ((await p.exited) !== 0) throw new Error(`tar: ${await new Response(p.stderr).text()}`)
  return {hash, tarball, files: kept.length, bytes}
}
