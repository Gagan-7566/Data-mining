"""
Render every captured console run as a PNG for the submission.

These are not photographs of a screen.  Each script in src/ tees its stdout to
logs/<script>.log while it runs (see config.Tee), and this renders those exact
bytes into a terminal-styled image, one or more pages per run.  Nothing is
retyped, reformatted or abridged: if a number appears in a shot, it appeared in
the run, and the matching .log file in logs/ is the plain-text original.
"""
from __future__ import annotations

import pathlib
import sys
import textwrap

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import config as C

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

BG      = "#14140f"
CHROME  = "#24241f"
FG      = "#e6e5dd"
DIM     = "#9a9990"
GREEN   = "#7fd07f"
BLUE    = "#7fb8f0"
YELLOW  = "#f0c674"
RED     = "#e88a8a"

COLS = 112
ROWS = 58
CW   = 0.0088     # character cell width  (figure fraction)
CH   = 0.0205     # character cell height


def colour_for(line: str) -> str:
    s = line.strip()
    if s.startswith("=") and len(set(s)) == 1:
        return YELLOW
    if s.startswith("-") and len(set(s)) == 1:
        return DIM
    low = s.lower()
    if "verdict:" in low or "identical" in low or " True" in line or "PASS" in s:
        return GREEN
    if "not idempotent" in low or "unstable" in low or "error" in low:
        return RED
    if s.startswith(("(a)", "(b)", "(c)", "(d)", "(e)", "(f)")) or s.startswith("witness"):
        return BLUE
    return FG


def wrap(lines: list[str]) -> list[str]:
    out = []
    for ln in lines:
        ln = ln.rstrip("\n").replace("\t", "    ")
        if len(ln) <= COLS:
            out.append(ln)
        else:
            out.extend(textwrap.wrap(ln, COLS, subsequent_indent="    ",
                                     drop_whitespace=False) or [""])
    return out


def render(lines: list[str], caption: str, out: pathlib.Path, page: int, pages: int):
    # Size the image to the lines actually on this page.  A fixed height leaves
    # a short final page as mostly empty terminal, which looks like a crop.
    n = max(len(lines), 8)
    fig = plt.figure(figsize=(COLS * CW * 13, (n + 4) * CH * 13), dpi=110)
    fig.patch.set_facecolor(BG)
    ax = fig.add_axes((0, 0, 1, 1))
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")

    # window chrome
    bar = min(0.06, 2.4 / (n + 4))
    ax.add_patch(plt.Rectangle((0, 1 - bar), 1, bar, facecolor=CHROME, lw=0))
    for i, c in enumerate(("#e06c60", "#e0b060", "#78c078")):
        ax.add_patch(plt.Circle((0.014 + i * 0.019, 1 - bar / 2), bar / 7,
                                facecolor=c, lw=0, transform=ax.transAxes))
    head = f"{caption}"
    if pages > 1:
        head += f"   —   page {page} of {pages}"
    ax.text(0.5, 1 - bar / 2, head, color=DIM, fontsize=10.5, family="monospace",
            ha="center", va="center")

    step = 0.90 / max(n, 1)
    y = 1 - bar - 0.012
    for ln in lines:
        ax.text(0.012, y, ln, color=colour_for(ln), fontsize=9.2,
                family="monospace", va="top", ha="left")
        y -= step
    fig.savefig(out, facecolor=BG, bbox_inches="tight", pad_inches=0.06)
    plt.close(fig)


SHOTS = [
    ("s01_stack_up.log",        "01", "(a) stack up  -- object store + PostgreSQL + DuckDB"),
    ("s02_ingest_runs3.log",    "02", "(b) load three times  -- same rows, same checksum"),
    ("s02b_resends.log",        "03", "(b) the re-sends  -- what each de-dup rule is worth"),
    ("s03_layout.log",          "04", "(a) layout  -- 4,389 objects down to 1"),
    ("s04_model.log",           "05", "(c) the star  -- and the two traps in the source"),
    ("s08_dashboard.log",       "06", "(c) the dashboard  -- the four slices"),
    ("s05_asof_prices.log",     "07", "(d) same query, different period"),
    ("s06_federated.log",       "08", "(e) one query, two systems, three witnesses"),
    ("s07_reconcile.log",       "09", "(f) reconciliation against finance_monthly.csv"),
]


def main() -> None:
    print("rendering console captures ->", C.SCREENSHOTS)
    made = 0
    for logname, num, caption in SHOTS:
        src = C.LOGS / logname
        if not src.exists():
            print(f"  SKIP {logname} (not run yet)")
            continue
        lines = wrap(src.read_text(encoding="utf-8", errors="replace").splitlines())
        pages = [lines[i:i + ROWS] for i in range(0, len(lines), ROWS)] or [[""]]
        for pi, page in enumerate(pages, 1):
            name = (f"console_{num}_{src.stem}_p{pi}.png" if len(pages) > 1
                    else f"console_{num}_{src.stem}.png")
            render(page, caption, C.SCREENSHOTS / name, pi, len(pages))
            made += 1
        print(f"  {logname:<26} -> {len(pages)} page(s)")
    print(f"done, {made} images.")


if __name__ == "__main__":
    main()
