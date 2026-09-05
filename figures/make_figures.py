"""Article figures -> SVG, then PNG via rsvg-convert.

Caught/missed is encoded by glyph shape and fill weight rather than hue:
green and red measure dE 4.1 under deuteranopia.
"""
from __future__ import annotations

import subprocess
from pathlib import Path

OUT = Path(__file__).resolve().parent
FONT = "Helvetica Neue, Helvetica, Arial, sans-serif"

# --- light surface tokens (see drill palette notes) ---
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK2 = "#52514e"
MUTED = "#898781"
GRID = "#e1e0d9"
AXIS = "#c3c2b7"
RED = "#d03b3b"
BLUE = "#2a78d6"


def esc(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def text(x, y, s, size=15, fill=INK2, weight="400", anchor="start", mono=False):
    # Single quotes: nested double quotes would truncate the XML attribute.
    fam = "'SFMono-Regular', Menlo, Consolas, monospace" if mono else FONT
    return (f'<text x="{x:.1f}" y="{y:.1f}" font-family="{fam}" font-size="{size}" '
            f'fill="{fill}" font-weight="{weight}" text-anchor="{anchor}">{esc(s)}</text>')


def check(cx, cy, color, w=2.4):
    return (f'<path d="M {cx-6:.1f} {cy-0.5:.1f} L {cx-2:.1f} {cy+4:.1f} L {cx+6.5:.1f} {cy-5.5:.1f}" '
            f'fill="none" stroke="{color}" stroke-width="{w}" stroke-linecap="round" '
            f'stroke-linejoin="round"/>')


def cross(cx, cy, color, w=2.6, r=5.5):
    return (f'<path d="M {cx-r:.1f} {cy-r:.1f} L {cx+r:.1f} {cy+r:.1f} '
            f'M {cx+r:.1f} {cy-r:.1f} L {cx-r:.1f} {cy+r:.1f}" '
            f'fill="none" stroke="{color}" stroke-width="{w}" stroke-linecap="round"/>')


# ---------------------------------------------------------------- figure 1
def fig1() -> str:
    W, H = 1240, 700
    PX0, PX1 = 168, 726          # timeline plot area
    MX0 = 790                    # matrix area
    COLW = 140
    ROW_Y = [176, 248, 320, 392, 464]
    AXIS_Y = 520
    WM_T = 55                    # watermark read instant, in time units

    def tx(t: float) -> float:
        return PX0 + (t / 100.0) * (PX1 - PX0)

    wmx = tx(WM_T)

    # seq, label, statement t, commit t, caught by [timestamp, max(seq), xmin], note
    txns = [
        (49, "T1", 8, 30, [1, 1, 1], ""),
        (50, "T2", 20, 72, [0, 0, 1], ""),
        (51, "T3", 34, 88, [0, 0, 1], ""),
        (52, "T4", 42, 50, [1, 1, 1], ""),
        (53, "T5", 64, 82, [1, 1, 1], ""),
    ]
    cols = [
        ("timestamp", "updated_at > W"),
        ("MAX(seq)", "seq > 52"),
        ("snapshot xmin", "xact_id >= xmin"),
    ]

    s = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" '
         f'viewBox="0 0 {W} {H}"><rect width="{W}" height="{H}" fill="{SURFACE}"/>']

    s.append(text(40, 52, "Three watermarks on a commit timeline", 25, INK, "600"))
    s.append(text(40, 80, "Each watermark is read at the same instant. Only the snapshot xmin "
                          "covers transactions that were still open.", 15.5, INK2))

    # region after the watermark
    s.append(f'<rect x="{wmx:.1f}" y="146" width="{PX1-wmx:.1f}" height="{AXIS_Y-146:.1f}" '
             f'fill="{INK}" opacity="0.030"/>')

    # matrix column bands + headers
    for i, (name, cond) in enumerate(cols):
        cx = MX0 + i * COLW
        if i == 2:
            s.append(f'<rect x="{cx:.1f}" y="146" width="{COLW}" height="{AXIS_Y-146:.1f}" '
                     f'fill="{BLUE}" opacity="0.055" rx="4"/>')
        s.append(text(cx + COLW / 2, 128, name, 14.5, INK, "600", "middle"))
        s.append(text(cx + COLW / 2, 148, cond, 12, MUTED, "400", "middle", mono=True))

    # watermark line
    s.append(f'<line x1="{wmx:.1f}" y1="146" x2="{wmx:.1f}" y2="{AXIS_Y:.1f}" '
             f'stroke="{INK}" stroke-width="1.8" stroke-dasharray="6 4"/>')
    s.append(text(wmx, 138, "watermark read (W)", 13, INK, "600", "middle"))

    for (seq, label, t0, t1, caught, note), y in zip(txns, ROW_Y):
        crosses = t0 < WM_T < t1
        bar = RED if crosses else MUTED
        op = 1.0 if crosses else 0.75
        x0, x1 = tx(t0), tx(t1)

        s.append(text(40, y + 5, label, 15.5, INK, "600"))
        s.append(text(72, y + 5, f"seq {seq}", 12.5, MUTED, "400", mono=True))

        # span from statement to commit
        s.append(f'<line x1="{x0:.1f}" y1="{y}" x2="{x1:.1f}" y2="{y}" stroke="{bar}" '
                 f'stroke-width="{3.0 if crosses else 2.0}" opacity="{op}"/>')
        # statement marker (open circle) and commit marker (filled square)
        s.append(f'<circle cx="{x0:.1f}" cy="{y}" r="4.6" fill="{SURFACE}" stroke="{bar}" '
                 f'stroke-width="2.2" opacity="{op}"/>')
        s.append(f'<rect x="{x1-4.6:.1f}" y="{y-4.6:.1f}" width="9.2" height="9.2" rx="1.6" '
                 f'fill="{bar}" opacity="{op}"/>')

        if note:
            s.append(text(x1 + 16, y + 4, note, 11.5, MUTED))

        for i, ok in enumerate(caught):
            cx = MX0 + i * COLW + COLW / 2
            if ok:
                s.append(check(cx, y, INK2))
            else:
                s.append(f'<rect x="{cx-COLW/2+8:.1f}" y="{y-17:.1f}" width="{COLW-16}" '
                         f'height="34" rx="5" fill="{RED}" opacity="0.10"/>')
                s.append(cross(cx, y, RED))

    # time axis
    s.append(f'<line x1="{PX0}" y1="{AXIS_Y}" x2="{PX1}" y2="{AXIS_Y}" stroke="{AXIS}" '
             f'stroke-width="1.5"/>')
    s.append(text(PX1, AXIS_Y + 22, "time", 13, MUTED, "400", "end"))

    # legend - x slots measured at ~6.8px/char so nothing collides
    ly = 586
    s.append(f'<circle cx="{PX0+6}" cy="{ly-4}" r="4.6" fill="{SURFACE}" stroke="{MUTED}" '
             f'stroke-width="2.2"/>')
    s.append(text(PX0 + 20, ly, "UPDATE runs (updated_at stamped, seq allocated)", 13, INK2))
    s.append(f'<rect x="520" y="{ly-8.6:.1f}" width="9.2" height="9.2" rx="1.6" fill="{MUTED}"/>')
    s.append(text(540, ly, "COMMIT (row becomes visible)", 13, INK2))
    s.append(cross(778, ly - 4, RED))
    s.append(text(794, ly, "never replayed", 13, RED, "600"))
    s.append(check(938, ly - 4, INK2))
    s.append(text(954, ly, "correct", 13, INK2))

    s.append(text(40, 640, "T1 and T4 committed before W, so the bulk scan already has them. "
                           "T5 started after W, so all three watermarks replay it.", 13, INK2))
    s.append(text(40, 662, "T2 and T3 stamped their rows before W and committed after it. The "
                           "scan's snapshot predates both commits, and updated_at < W excludes "
                           "them from the replay.", 13, INK2))
    s.append(text(40, 684, "T4 allocated seq 52 and committed first, so MAX(seq) = 52 also "
                           "excludes seq 50 and 51 - it only works when nothing allocated later "
                           "commits ahead of you.", 13, MUTED))

    s.append("</svg>")
    return "\n".join(s)


# ---------------------------------------------------------------- figure 2
def fig2(N=None, s_stale=None, n_rec=None) -> str:
    """Detection curve. Parameters are read from the naive results so the
    figure cannot state a corpus size the runs did not produce."""
    import json
    if N is None:
        files = sorted((OUT.parent / "results").glob("naive_*.json"))
        if not files:
            raise SystemExit("fig2 needs at least one naive_*.json in results/")
        runs = [json.loads(f.read_text())["checks"] for f in files]
        # Medians over sorted files, so the figure is machine-independent.
        N = int(_median([c["stale_exact"]["compared"] for c in runs]))
        s_stale = int(_median([c["stale_exact"]["count"] for c in runs]))
        n_rec = int(_median([c["stale"]["sampled"] for c in runs]))

    W, H = 1000, 620
    PX0, PX1 = 92, 940
    PY0, PY1 = 132, 486
    XMAX = 0.62

    def px(frac):
        return PX0 + (frac / XMAX) * (PX1 - PX0)

    def py(p):
        return PY1 - p * (PY1 - PY0)

    def detect(frac):
        """P(at least one of s_stale docs lands in a sample of n).

        P(none) = C(N-s, n) / C(N, n) = prod (N-n-i)/(N-i) for i in 0..s-1.
        """
        n = frac * N
        p = 1.0
        for i in range(s_stale):
            p *= (N - n - i) / (N - i)
        return 1 - max(0.0, p)

    out = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" '
           f'viewBox="0 0 {W} {H}"><rect width="{W}" height="{H}" fill="{SURFACE}"/>']
    out.append(text(40, 52, f"A {n_rec:,}-ID sample cannot see {s_stale} stale documents",
                    25, INK, "600"))
    out.append(text(40, 80, f"Probability the sampled check finds at least one defect. "
                            f"Corpus {N:,} documents, {s_stale} stale.", 15.5, INK2))
    out.append(text(40, 102, "Hypergeometric, exact.", 13, MUTED))

    # y grid + labels
    for p in (0, .25, .5, .75, 1.0):
        y = py(p)
        out.append(f'<line x1="{PX0}" y1="{y:.1f}" x2="{PX1}" y2="{y:.1f}" stroke="{GRID}" '
                   f'stroke-width="1"/>')
        out.append(text(PX0 - 12, y + 4.5, f"{p*100:.0f}%", 13, MUTED, "400", "end"))
    # x ticks
    for f in (0, .1, .2, .3, .4, .5, .6):
        x = px(f)
        out.append(f'<line x1="{x:.1f}" y1="{PY1}" x2="{x:.1f}" y2="{PY1+6}" stroke="{AXIS}" '
                   f'stroke-width="1.2"/>')
        out.append(text(x, PY1 + 26, f"{f*100:.0f}%", 13, MUTED, "400", "middle"))
    out.append(text((PX0 + PX1) / 2, PY1 + 56, "share of the corpus verified",
                    14, INK2, "500", "middle"))

    # 95% reference
    y95 = py(0.95)
    out.append(f'<line x1="{PX0}" y1="{y95:.1f}" x2="{PX1}" y2="{y95:.1f}" stroke="{MUTED}" '
               f'stroke-width="1.3" stroke-dasharray="5 4"/>')
    out.append(text(PX0 + 12, y95 - 10, "95% detection", 12.5, MUTED))

    # curve
    pts = []
    steps = 500
    for i in range(steps + 1):
        f = XMAX * i / steps
        pts.append(f"{px(f):.2f},{py(detect(f)):.2f}")
    out.append(f'<polyline points="{" ".join(pts)}" fill="none" stroke="{BLUE}" '
               f'stroke-width="2.4" stroke-linejoin="round"/>')

    # the recommended sample
    f_rec = n_rec / N
    p_rec = detect(f_rec)
    xr, yr = px(f_rec), py(p_rec)
    out.append(f'<line x1="{xr:.1f}" y1="{yr:.1f}" x2="{xr:.1f}" y2="{PY1:.1f}" '
               f'stroke="{RED}" stroke-width="1.4" stroke-dasharray="4 3"/>')
    out.append(f'<circle cx="{xr:.1f}" cy="{yr:.1f}" r="6.5" fill="{RED}" stroke="{SURFACE}" '
               f'stroke-width="2"/>')
    out.append(text(xr + 62, yr - 46, f"{n_rec:,}-ID sample", 15, RED, "600"))
    out.append(text(xr + 62, yr - 26, f"{p_rec*100:.2f}% chance of detection", 14, RED))
    out.append(text(xr + 62, yr - 6, "the check reports clean", 13, MUTED))

    # 95% crossing
    lo, hi = 0.0, XMAX
    for _ in range(60):
        mid = (lo + hi) / 2
        if detect(mid) >= 0.95: hi = mid
        else: lo = mid
    x95 = px(hi)
    out.append(f'<circle cx="{x95:.1f}" cy="{y95:.1f}" r="6.5" fill="{BLUE}" '
               f'stroke="{SURFACE}" stroke-width="2"/>')
    out.append(f'<line x1="{x95:.1f}" y1="{y95:.1f}" x2="{x95:.1f}" y2="{PY1:.1f}" '
               f'stroke="{BLUE}" stroke-width="1.4" stroke-dasharray="4 3"/>')
    out.append(text(x95 - 16, y95 - 36, f"{hi*N:,.0f} documents", 15, BLUE, "600", "end"))
    out.append(text(x95 - 16, y95 - 16, f"{hi*100:.1f}% of the corpus", 14, BLUE, "400", "end"))

    out.append(text(40, 560, "Past a certain rarity, sampling is not a cheaper approximation of "
                             "exact verification. It is not verification at all:", 13.5, INK2))
    out.append(text(40, 582, "once the sample has to cover 45% of the corpus to be trustworthy, "
                             "its only advantage is gone.", 13.5, INK2))
    out.append("</svg>")
    return "\n".join(out)


# ---------------------------------------------------------------- figure 3
def _median(vals):
    s = sorted(v for v in vals if v is not None)
    if not s:
        return 0.0
    n = len(s)
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2


def fig3(batch=2000) -> str:
    """All four cells of the replicas x refresh 2x2, at one batch size.

    Comparing replicas 0 / refresh -1 against replicas 1 / refresh 1s changes
    both variables at once, so it cannot separate their effects. Each variable
    is held constant across a pair here.

    Reads medians from results/, so the figure cannot drift from the runs.
    """
    import json
    rd = OUT.parent / "results"
    cells: dict = {}
    for f in rd.glob("*.json"):
        if f.name == "rejection_probe.json":
            continue
        d = json.loads(f.read_text())
        if d["recovery"] != "corrected" or d["batch_size"] != batch:
            continue
        cells.setdefault((d["replicas"], d["refresh"]), []).append(d)

    missing = [c for c in ((0, "-1"), (0, "1s"), (1, "-1"), (1, "1s")) if c not in cells]
    if missing:
        raise SystemExit(f"fig3 needs all four 2x2 cells; missing {missing}")
    counts = {len(v) for v in cells.values()}

    def m(cell, fld):
        vals = [r[fld] for r in cells[cell] if r.get(fld) is not None]
        return _median(vals) if vals else 0.0

    # Every phase that cutover_ready_seconds sums, in order.
    PHASES = [
        ("bulk scan", "load_seconds", "#2a78d6", True),
        ("live drain", "live_drain_seconds", "#008300", True),
        ("barrier", "barrier_seconds", "#eb6834", False),
        ("drain", "replay_seconds", "#1baf7a", False),
        ("shard settle", "settle_seconds", "#eda100", False),
        ("verify", "verify_seconds", "#e87ba4", False),
    ]
    order = [(0, "-1"), (0, "1s"), (1, "-1"), (1, "1s")]

    W, H = 1180, 640
    PX0, PX1 = 300, 1020
    totals = {c: sum(m(c, f) for _, f, _, _ in PHASES) for c in order}
    scale = (PX1 - PX0) / max(totals.values())

    o = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" '
         f'viewBox="0 0 {W} {H}"><rect width="{W}" height="{H}" fill="{SURFACE}"/>']
    o.append(text(40, 50, "The replica costs throughput, but not time-to-verified",
                  24, INK, "600"))
    n_desc = (f"{counts.pop()}" if len(counts) == 1
              else f"{min(counts)}-{max(counts)}")
    o.append(text(40, 78, f"Time to an index that has passed the cutover gate. Median of "
                          f"{n_desc} runs per cell, "
                          f"{cells[order[0]][0]['docs_loaded']:,} docs, batch {batch}. "
                          f"Reads continue throughout.", 15.5, INK2))

    BAR_H = 50
    ys = [150, 232, 330, 412]
    for cell, y in zip(order, ys):
        repl, refr = cell
        o.append(text(280, y - 2, f"replicas {repl} / refresh {refr}", 15.5, INK, "600", "end"))
        o.append(text(280, y + 18, f"{m(cell, 'docs_per_sec'):,.0f} docs/sec", 12.5, MUTED,
                      "400", "end"))
        x = PX0
        stop_x = None
        for name, fld, colour, is_live in PHASES:
            w = m(cell, fld) * scale
            if stop_x is None and not is_live:
                stop_x = x
            if w > 0.6:
                o.append(f'<rect x="{x:.1f}" y="{y - BAR_H/2:.1f}" '
                         f'width="{max(w - 2, 0.8):.1f}" height="{BAR_H}" '
                         f'fill="{colour}" rx="3"/>')
            x += w
        o.append(text(x + 12, y + 5, f"{totals[cell]:.1f}s", 16, INK, "600"))
        o.append(f'<line x1="{stop_x:.1f}" y1="{y - BAR_H/2 - 8:.1f}" '
                 f'x2="{stop_x:.1f}" y2="{y + BAR_H/2 + 8:.1f}" stroke="{INK}" '
                 f'stroke-width="1.6" stroke-dasharray="4 3"/>')
        o.append(text(x + 12, y + 22, f"finalization {totals[cell] - m(cell, 'load_seconds'):.1f}s",
                      11.5, MUTED))

    # bracket the two pairs so the held-constant variable is visible
    for y0, y1, label in ((ys[0], ys[1], "refresh -1 vs 1s, replicas 0"),
                          (ys[2], ys[3], "refresh -1 vs 1s, replicas 1")):
        o.append(f'<path d="M 292 {y0 - 26:.0f} L 286 {y0 - 26:.0f} L 286 {y1 + 30:.0f} '
                 f'L 292 {y1 + 30:.0f}" fill="none" stroke="{GRID}" stroke-width="1.5"/>')

    ly = 486
    o.append(text(40, ly - 24, "Phases", 13, INK, "600"))
    for i, (name, fld, colour, _) in enumerate(PHASES):
        cx = 40 + (i % 3) * 350
        cy = ly + (i // 3) * 26
        o.append(f'<rect x="{cx}" y="{cy - 9}" width="12" height="12" rx="2" fill="{colour}"/>')
        o.append(text(cx + 20, cy + 2, name, 13.5, INK2))

    r0m, r1m = totals[(0, "1s")], totals[(1, "1s")]
    d0, d1 = m((0, "1s"), "docs_per_sec"), m((1, "1s"), "docs_per_sec")
    o.append(text(40, 562, f"Holding refresh at 1s, the replica costs "
                           f"{abs(d1/d0-1)*100:.0f}% of indexing throughput "
                           f"({d0:,.0f} -> {d1:,.0f} docs/sec) - but that work overlaps the "
                           f"scan instead of", 13.5, INK2))
    n0, n1 = totals[(0, "-1")], totals[(1, "-1")]
    o.append(text(40, 584, f"deferring to shard settlement, so time-to-verified is "
                           f"{r0m:.1f}s vs {r1m:.1f}s. At refresh -1 it is "
                           f"{n0:.1f}s vs {n1:.1f}s, a {abs(n1/n0-1)*100:.1f}% difference. "
                           f"Disabling refresh was slower on both axes.", 13.5, INK2))
    o.append(text(40, 612, "The dashed line marks where writes stop. The synthetic writer had "
                           "finished its plan by then, so this is finalization duration, not "
                           "measured latency for blocked writes.", 13.5, MUTED))
    o.append("</svg>")
    return "\n".join(o)


def render(name: str, svg: str, zoom: float = 2.0) -> None:
    """SVG -> PNG, quantized to land in the 100-200 KB band. Flat vector art
    palettes down to nothing; JPEG rings around thin strokes, so PNG."""
    sp = OUT / f"{name}.svg"
    pp = OUT / f"{name}.png"
    sp.write_text(svg)
    subprocess.run(["rsvg-convert", "-z", str(zoom), "-o", str(pp), str(sp)], check=True)
    raw_kb = pp.stat().st_size / 1024

    try:
        from PIL import Image
    except ImportError:
        print(f"  {pp.name:34} {raw_kb:7.1f} KB  (install pillow to shrink)")
        return

    img = Image.open(pp).convert("RGB")
    for colors in (64, 32, 16):
        img.quantize(colors=colors, method=Image.MEDIANCUT, dither=Image.NONE) \
           .save(pp, optimize=True)
        kb = pp.stat().st_size / 1024
        if kb <= 200:
            break
    flag = "" if 100 <= kb <= 200 else "  <-- outside DZone's 100-200 KB band"
    print(f"  {pp.name:34} {kb:7.1f} KB  ({raw_kb:.0f} KB truecolour, "
          f"{colors}-colour palette){flag}")


if __name__ == "__main__":
    OUT.mkdir(parents=True, exist_ok=True)
    print("rendering figures:")
    render("fig1-three-watermarks", fig1(), zoom=3.2)
    render("fig2-detection-curve", fig2(), zoom=3.6)
    render("fig3-cutover-timeline", fig3(), zoom=3.0)
