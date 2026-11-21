// The runner's decisions held to their laws by gdp-wired's CLI: every law of src/boxes.ts and src/decide.ts proven
// and checked from every state the laws allow, none unknown or unread; every law of test/broken_*.ts broken by a
// run (the bugs they rule out, kept as regressions). Exits 0 only then.
//   bun test/check_laws.ts
//   GDP_WIRED_CLI="bun <gdp-wired checkout>/core/cli.ts" bun test/check_laws.ts
import {readFileSync} from 'node:fs'
import {join} from 'node:path'

const here = join(import.meta.dir, '..')
const cli = (process.env.GDP_WIRED_CLI ?? 'bunx gdp-wired-cli').split(' ')
type Law = {name: string; where: string; verdict: string; why?: string}

// A file of classes is read whole (each class a rule); a file of plain functions one exported function at a time.
function units(file: string): string[][] {
  const src = readFileSync(join(here, file), 'utf8')
  if (/^export class /m.test(src)) return [[]]
  return [...src.matchAll(/^export function (\w+)/gm)].map((m) => ['--mode', 'standalone', '--entry', m[1]!])
}

async function verdicts(file: string): Promise<Map<string, Law>> {
  const laws = new Map<string, Law>()
  await Promise.all(units(file).map(async (extra) => {
    const p = Bun.spawn([...cli, 'check', file, '--from', 'any', '--no-propose', '--json', ...extra], {cwd: here, stdout: 'pipe', stderr: 'pipe'})
    const out = await new Response(p.stdout).text()
    await p.exited
    const d = JSON.parse(out) as {rules: {laws: Law[]}[]}
    for (const r of d.rules) for (const l of r.laws) if (l.verdict !== 'not read' || !laws.has(l.where)) laws.set(l.where, l)
  }))
  return laws
}

let bad = 0
for (const [file, want] of [['src/boxes.ts', 'proven'], ['src/decide.ts', 'proven'], ['test/broken_boxes.ts', 'broken'], ['test/broken_decide.ts', 'broken']] as const) {
  for (const l of (await verdicts(file)).values()) {
    const ok = l.verdict === want
    if (!ok) bad++
    console.log(`${ok ? 'ok  ' : 'FAIL'} ${l.verdict.padEnd(8)} ${l.name}  (${file.split('/')[0]}/${l.where})${ok || !l.why ? '' : `\n       ${l.why}`}`)
  }
}
process.exit(bad ? 1 : 0)
