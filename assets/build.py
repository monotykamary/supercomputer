"""The README's animated SVGs, in boat.dev's palette, light and dark: python3 assets/build.py writes assets/*.svg.

Stdlib only. Every animation is CSS or SMIL (GitHub shows SVGs as images: no scripts), its resting state is its final
frame, and prefers-reduced-motion stops it there.
"""
import html
import pathlib
import random

HERE = pathlib.Path(__file__).resolve().parent

# boat.dev's tokens: a neutral scale from --n-1 #fefefe to --n-10 #060606, one accent (--ocean #263fa9).
THEMES = {
    "light": dict(bg="#fefefe", surface="#f2f2f2", line="#e0e0e0", line2="#c4c4c4", muted="#767676", text2="#4d4d4d",
                  ink="#060606", accent="#263fa9", accent_soft="#e3e7f6", ok="#263fa9", warn="#8a5300"),
    "dark": dict(bg="#060606", surface="#1a1a1a", line="#2e2e2e", line2="#4d4d4d", muted="#9e9e9e", text2="#c4c4c4",
                 ink="#fefefe", accent="#7189f0", accent_soft="#141a33", ok="#7189f0", warn="#e0a64a"),
}
DISPLAY = "'Funnel Display','Inter',ui-sans-serif,system-ui,-apple-system,'Segoe UI',Helvetica,Arial,sans-serif"
TEXT = "'Inter',ui-sans-serif,system-ui,-apple-system,'Segoe UI',Helvetica,Arial,sans-serif"
MONO = "'IBM Plex Mono',ui-monospace,SFMono-Regular,Menlo,Consolas,'Liberation Mono',monospace"


def esc(s: str) -> str:
    return html.escape(s, quote=False)


def svg(w: int, h: int, t: dict, body: str, css: str = "", label: str = "") -> str:
    return f'''<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" viewBox="0 0 {w} {h}" role="img" aria-label="{html.escape(label)}">
<style>
.d{{font-family:{DISPLAY}}} .t{{font-family:{TEXT}}} .m{{font-family:{MONO}}}
.ink{{fill:{t["ink"]}}} .t2{{fill:{t["text2"]}}} .mu{{fill:{t["muted"]}}} .ac{{fill:{t["accent"]}}}
{css}
@media (prefers-reduced-motion: reduce){{*{{animation:none!important}}}}
</style>
<rect x="0.5" y="0.5" width="{w - 1}" height="{h - 1}" rx="14" fill="{t["bg"]}" stroke="{t["line"]}"/>
{body}
</svg>
'''


# ---------------------------------------------------------------------------------------------------------- hero
def hero(t: dict) -> str:
    w, h = 880, 330
    rnd = random.Random(7)
    cols, rows = 12, 2
    cw, ch, gx, gy = 58, 40, 9, 12
    x0 = (w - (cols * cw + (cols - 1) * gx)) // 2
    y0 = 214
    cards, css = [], []
    starting = {(3, 1), (9, 0)}
    stopped = {(11, 1), (0, 1)}
    n = 0
    for r in range(rows):
        for c in range(cols):
            x, y = x0 + c * (cw + gx), y0 + r * (ch + gy)
            if (c, r) in stopped:
                cards.append(f'<rect x="{x + .5}" y="{y + .5}" width="{cw - 1}" height="{ch - 1}" rx="7" fill="none" '
                             f'stroke="{t["line2"]}" stroke-dasharray="3 4"/>')
                continue
            cls = ' class="boot"' if (c, r) in starting else ""
            cards.append(f'<rect{cls} x="{x + .5}" y="{y + .5}" width="{cw - 1}" height="{ch - 1}" rx="7" '
                         f'fill="{t["surface"]}" stroke="{t["line"]}"/>')
            for s in range(4):
                sx, sy = x + 8 + s * 11, y + 10
                cards.append(f'<rect x="{sx}" y="{sy}" width="7" height="{ch - 20}" rx="2" fill="{t["line"]}"/>')
                if (c, r) in starting:
                    continue
                dur = rnd.uniform(3.2, 8.5)
                delay = -rnd.uniform(0, dur)
                on = rnd.uniform(0.35, 0.8)
                css.append(f'.s{n}{{animation:fill {dur:.2f}s {delay:.2f}s infinite}}')
                cards.append(f'<rect class="s{n}" x="{sx}" y="{sy}" width="7" height="{ch - 20}" rx="2" fill="{t["accent"]}" '
                             f'style="--on:{on:.2f}"/>')
                n += 1
    css.append("@keyframes fill{0%{opacity:0}8%{opacity:1}60%{opacity:1}72%{opacity:0}100%{opacity:0}}")
    css.append(f"@keyframes boot{{0%,100%{{stroke:{t['line']}}}50%{{stroke:{t['accent']}}}}}")
    css.append(".boot{animation:boot 1.6s ease-in-out infinite}")
    css.append(f"@keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:.25}}}} .pulse{{animation:pulse 2.4s ease-in-out infinite}}")
    body = f'''
<g>
  <circle class="pulse" cx="44" cy="45" r="4" fill="{t["accent"]}"/>
  <text class="m mu" x="56" y="49.5" font-size="13" letter-spacing=".02em">boat.dev · RunPod · Modal</text>
  <text class="d ink" x="38" y="112" font-size="56" font-weight="650" letter-spacing="-.025em">supercomputer</text>
  <text class="t t2" x="40" y="148" font-size="18">One job runner for every repo on your machine: a warm pool of boat.dev VMs</text>
  <text class="t t2" x="40" y="173" font-size="18">first, RunPod for GPU jobs, Modal as the fallback of both.</text>
</g>
{"".join(cards)}
<text class="m mu" x="{x0}" y="{y0 - 12}" font-size="11.5" letter-spacing=".04em">THE POOL · BUSY, NEVER PARKED</text>
<text class="m mu" x="{w - x0}" y="{y0 - 12}" font-size="11.5" text-anchor="end" letter-spacing=".04em">a slot lit = an item running</text>
'''
    return svg(w, h, t, body, "\n".join(css), "supercomputer: one job runner over boat.dev, RunPod and Modal")


# ---------------------------------------------------------------------------------------------------------- flow
def flow(t: dict) -> str:
    w, h = 880, 360

    def box(x, y, bw, bh, title, subs, strong=False):
        stroke = t["accent"] if strong else t["line"]
        sw = 1.5 if strong else 1
        out = [f'<rect x="{x + .5}" y="{y + .5}" width="{bw}" height="{bh}" rx="10" fill="{t["surface"] if not strong else t["accent_soft"]}" '
               f'stroke="{stroke}" stroke-width="{sw}"/>',
               f'<text class="d ink" x="{x + 16}" y="{y + 28}" font-size="17" font-weight="600">{esc(title)}</text>']
        for i, s in enumerate(subs):
            out.append(f'<text class="m mu" x="{x + 16}" y="{y + 49 + i * 17}" font-size="11.5">{esc(s)}</text>')
        return "".join(out)

    paths = {
        "in": "M170 180 L232 180",
        "boat": "M412 168 C462 168 462 92 520 92",
        "gpu": "M412 186 C462 186 462 212 520 212",
        "modal": "M412 198 C470 198 452 300 520 300",
    }
    lines = []
    for k, d in paths.items():
        dash = ' stroke-dasharray="4 5"' if k == "modal" else ""
        col = t["accent"] if k in ("in", "boat") else t["line2"]
        lines.append(f'<path id="p-{k}" d="{d}" fill="none" stroke="{col}" stroke-width="1.5"{dash}/>')

    def packets(k, n, dur, color, begin0=0.0, opacity_keys=None):
        out = []
        for i in range(n):
            b = begin0 - i * dur / n
            op = ""
            if opacity_keys:
                op = f'<animate attributeName="opacity" values="{opacity_keys[0]}" keyTimes="{opacity_keys[1]}" dur="{dur}s" begin="{b:.2f}s" repeatCount="indefinite"/>'
            out.append(f'<circle r="3.6" fill="{color}">{op}<animateMotion dur="{dur}s" begin="{b:.2f}s" repeatCount="indefinite" '
                       f'rotate="auto"><mpath href="#p-{k}"/></animateMotion></circle>')
        return "".join(out)

    chips = []
    rnd = random.Random(3)
    css = []
    for i in range(8):
        cx = 536 + i * 38
        chips.append(f'<rect x="{cx}" y="104" width="30" height="20" rx="4" fill="{t["bg"]}" stroke="{t["line"]}"/>')
        for s in range(3):
            sx = cx + 5 + s * 7.5
            chips.append(f'<rect x="{sx}" y="109" width="4.5" height="10" rx="1.2" fill="{t["line"]}"/>')
            dur, j = rnd.uniform(2.5, 6), len(css)
            css.append(f'.c{j}{{animation:fill {dur:.2f}s {-rnd.uniform(0, dur):.2f}s infinite}}')
            chips.append(f'<rect class="c{j}" x="{sx}" y="109" width="4.5" height="10" rx="1.2" fill="{t["accent"]}"/>')
    css.append("@keyframes fill{0%{opacity:0}10%{opacity:1}62%{opacity:1}74%{opacity:0}100%{opacity:0}}")

    body = f'''
{"".join(lines)}
{box(30, 150, 140, 60, "jobs.json", ["any repo"])}
{box(232, 130, 180, 100, "scheduler", ["on your machine", "one per pool", "127.0.0.1:8790"])}
{box(520, 34, 330, 104, "boat.dev VMs", ["a warm pool shared by every repo"], strong=True)}
{"".join(chips)}
{box(520, 172, 330, 74, "RunPod", ["GPU jobs: a pod per batch"])}
{box(520, 262, 330, 74, "Modal", ["the fallback of both"])}
<text class="m mu" x="248" y="258" font-size="11.5">Modal: after 3 boat failures,</text>
<text class="m mu" x="248" y="275" font-size="11.5">each on another VM</text>
{packets("in", 3, 2.4, t["accent"])}
{packets("boat", 4, 2.0, t["accent"])}
{packets("gpu", 1, 4.5, t["muted"], -1.0, ("0;1;1;0;0", "0;.05;.9;.95;1"))}
{packets("modal", 1, 9.0, t["muted"], -6.0, ("0;0;1;1;0", "0;.62;.66;.95;1"))}
'''
    return svg(w, h, t, body, "\n".join(css),
               "jobs.json goes to the scheduler on your machine, which places it on boat.dev VMs, RunPod for GPU jobs, or Modal after three boat failures")


# ---------------------------------------------------------------------------------------------------------- terminal
def terminal(t: dict, cmd: str, out: list[tuple[str, str]]) -> str:
    """`cmd` typed, then each (style, text) line of `out` appears; a 12 s loop."""
    w = 880
    lh, top = 22, 74
    h = top + (len(out) + 1) * lh + 34
    period = 12.0
    type_end = 2.6
    css = [f".cur{{animation:blink 1s steps(1) infinite}} @keyframes blink{{50%{{opacity:0}}}}"]
    n = len(cmd)
    # the command typed: a mask widening one character at a time
    cw = 8.4  # IBM Plex Mono / SF Mono at 14px: ~0.6em
    css.append(f"@keyframes type{{0%{{width:0}}{type_end / period * 100:.1f}%{{width:{n * cw + 4:.0f}px}}100%{{width:{n * cw + 4:.0f}px}}}}")
    css.append(f".typed{{animation:type {period}s steps({n}) infinite}}")
    css.append(f"@keyframes caret{{0%{{transform:translateX(-{n * cw:.0f}px)}}{type_end / period * 100:.1f}%,100%{{transform:translateX(0)}}}}")
    css.append(f".caret{{animation:caret {period}s steps({n}) infinite}}")
    rows = []
    for i, (style, text) in enumerate(out):
        at = (type_end + 0.5 + i * 0.55) / period * 100
        css.append(f"@keyframes l{i}{{0%,{at:.1f}%{{opacity:0}}{at + .1:.1f}%,96%{{opacity:1}}100%{{opacity:0}}}} .l{i}{{animation:l{i} {period}s infinite}}")
        cls = {"ok": "ac", "mu": "mu", "": "t2", "ink": "ink"}[style]
        rows.append(f'<text class="m {cls} l{i}" x="28" y="{top + (i + 1) * lh}" font-size="14" xml:space="preserve">{esc(text)}</text>')
    body = f'''
<rect x="0.5" y="0.5" width="{w - 1}" height="38" rx="14" fill="{t["surface"]}"/>
<rect x="0.5" y="24" width="{w - 1}" height="15" fill="{t["surface"]}"/>
<line x1="0" y1="38.5" x2="{w}" y2="38.5" stroke="{t["line"]}"/>
<circle cx="24" cy="19.5" r="5" fill="{t["line2"]}"/><circle cx="42" cy="19.5" r="5" fill="{t["line2"]}"/><circle cx="60" cy="19.5" r="5" fill="{t["line2"]}"/>
<text class="m mu" x="{w / 2}" y="24" font-size="12" text-anchor="middle">~/my-repo</text>
<defs><clipPath id="typed"><rect class="typed" x="44" y="{top - 16}" width="{n * cw + 4:.0f}" height="22"/></clipPath></defs>
<text class="m ac" x="28" y="{top}" font-size="14">$</text>
<text class="m ink" x="46" y="{top}" font-size="14" clip-path="url(#typed)" xml:space="preserve">{esc(cmd)}</text>
<g class="caret"><rect class="cur" x="{46 + n * cw + 3:.0f}" y="{top - 13}" width="8" height="16" fill="{t["accent"]}" opacity=".85"/></g>
{"".join(rows)}
'''
    return svg(w, h, t, body, "\n".join(css), f"a terminal running: {cmd}")


# ---------------------------------------------------------------------------------------------------------- laws
def laws(t: dict, stats: list[tuple[str, str]], note: str) -> str:
    w, h = 880, 128
    cell = (w - 40) / len(stats)
    css = ["@keyframes draw{0%,8%{stroke-dashoffset:24}30%,100%{stroke-dashoffset:0}}",
           "@keyframes ring{0%,8%{stroke-dashoffset:76}26%,100%{stroke-dashoffset:0}}"]
    parts = []
    for i, (num, label) in enumerate(stats):
        x = 20 + i * cell
        d = 0.35 * i
        css.append(f".k{i}{{animation:draw 6s {d:.2f}s infinite both}} .r{i}{{animation:ring 6s {d:.2f}s infinite both}}")
        if i:
            parts.append(f'<line x1="{x:.0f}" y1="28" x2="{x:.0f}" y2="{h - 28}" stroke="{t["line"]}"/>')
        cx, cy = x + 34, 58
        parts.append(f'<circle class="r{i}" cx="{cx:.0f}" cy="{cy}" r="12" fill="none" stroke="{t["accent"]}" stroke-width="1.6" '
                     f'stroke-dasharray="76" transform="rotate(-90 {cx:.0f} {cy})"/>')
        parts.append(f'<path class="k{i}" d="M{cx - 5.5:.1f} {cy + .5} l3.8 3.8 l7 -7.6" fill="none" stroke="{t["accent"]}" stroke-width="1.8" '
                     f'stroke-linecap="round" stroke-linejoin="round" stroke-dasharray="24"/>')
        parts.append(f'<text class="d ink" x="{x + 58:.0f}" y="68" font-size="32" font-weight="650" letter-spacing="-.02em">{esc(num)}</text>')
        parts.append(f'<text class="t mu" x="{x + 60:.0f}" y="92" font-size="13">{esc(label)}</text>')
    parts.append(f'<text class="m mu" x="{w - 20}" y="{h - 12}" font-size="10.5" text-anchor="end">{esc(note)}</text>')
    return svg(w, h, t, "".join(parts), "\n".join(css), ", ".join(f"{n} {l}" for n, l in stats))


# The terminal shows a real run's output, verbatim: a three-job batch on a warm VM (2026-11-09, v0.1.0).
RUN_CMD = "supercomputer run --jobs jobs.json --out runs/x"
RUN_OUT = [
    ("mu", "[boat] batch 20261109-190534-9oib2o: demo, image demo-img-bced87332328, tree b838901c3100 (2 files)"),
    ("mu", "[boat] 3 jobs to the scheduler (interactive) (times from the last run)"),
    ("", "hello                                    exit   0      0.03 s"),
    ("", "nproc                                    exit   0      0.01 s"),
    ("", "tests                                    exit   0      0.02 s"),
    ("ok", "3 jobs, 0 failed, 11.7 s wall, 0.1 s of job time; 3 on boat"),
    ("mu", "boat (the scheduler, class interactive): 1 VMs, 0 starts charged to this batch"),
]
LAWS = [("471", "laws proven"), ("115", "broken variants refuted"), ("72", "scenarios on simulated boat")]
LAWS_NOTE = "tests/check_laws.sh · tests/check_scenarios.sh · v0.1.0"


def main():
    for name, t in THEMES.items():
        (HERE / f"hero-{name}.svg").write_text(hero(t))
        (HERE / f"flow-{name}.svg").write_text(flow(t))
        (HERE / f"run-{name}.svg").write_text(terminal(t, RUN_CMD, RUN_OUT))
        (HERE / f"laws-{name}.svg").write_text(laws(t, LAWS, LAWS_NOTE))
    print("wrote", ", ".join(sorted(p.name for p in HERE.glob("*.svg"))))


if __name__ == "__main__":
    main()
