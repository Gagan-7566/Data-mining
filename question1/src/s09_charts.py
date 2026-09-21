"""
Charts for the report.

Colour is the validated reference palette (see the dataviz reference):
categorical slots in fixed order, a single blue hue for sequential magnitude,
and blue<->red with a neutral grey midpoint for the signed variance.  Palette
checked with the validator at three slots -- all gates pass; the aqua slot sits
below 3:1 on the light surface, so every chart that uses it carries visible
direct labels, which is the documented relief.

These are static PNGs for a printed submission, so there is no hover layer.
The table view the accessibility pass requires is the matching CSV in
results/ -- every chart here is a rendering of one of those files and nothing
else.
"""
from __future__ import annotations

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import config as C

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

import matplotlib.ticker as mticker

# ---- the palette, by role -------------------------------------------------
SURFACE = "#fcfcfb"
INK     = "#0b0b0b"
INK2    = "#52514e"
GRID    = "#e3e2de"
S1      = "#2a78d6"   # categorical slot 1 / sequential hue
S2      = "#eb6834"   # categorical slot 2
NEUTRAL = "#c9c8c4"
POS     = "#2a78d6"   # diverging: cool pole
NEG     = "#d03b3b"   # diverging: warm pole
SEQ = ["#cde2fb", "#b7d3f6", "#9ec5f4", "#86b6ef", "#6da7ec", "#5598e7",
       "#3987e5", "#2a78d6", "#256abf", "#1c5cab", "#184f95", "#104281"]

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE,
    "savefig.facecolor": SURFACE,
    "font.family": "DejaVu Sans", "font.size": 10,
    "text.color": INK, "axes.labelcolor": INK2, "axes.edgecolor": GRID,
    "xtick.color": INK2, "ytick.color": INK2,
    "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8,
    "axes.axisbelow": True, "figure.dpi": 150,
})


def lakhs(x, _pos=None):
    """Indian readers count in lakh and crore; 5.6 Cr is legible, 56359195.92 is not."""
    if abs(x) >= 1e7:
        return f"{x/1e7:,.2f} Cr"
    if abs(x) >= 1e5:
        return f"{x/1e5:,.1f} L"
    return f"{x:,.0f}"


def title(ax, head, sub=None):
    """Title above, subtitle under it, neither colliding with the other or with
    the plot.  The subtitle is where the chart says what it is NOT showing."""
    if sub:
        ax.set_title(head, loc="left", fontsize=13, fontweight="bold", color=INK,
                     pad=30)
        ax.text(0, 1.035, sub, transform=ax.transAxes, fontsize=9.5, color=INK2,
                va="bottom")
    else:
        ax.set_title(head, loc="left", fontsize=13, fontweight="bold", color=INK,
                     pad=14)


def save(fig, name):
    p = C.SCREENSHOTS / name
    fig.savefig(p, bbox_inches="tight", pad_inches=0.3)
    plt.close(fig)
    print(f"  wrote {p.name}")
    return p


def main() -> None:
    con = C.connect_duckdb(memory=True)
    con.execute(f"CREATE VIEW agg AS SELECT * FROM read_parquet('{C.P_MART}/agg_revenue_day.parquet')")
    con.execute(f"CREATE VIEW dim_store AS SELECT * FROM read_parquet('{C.P_MART}/dim_store.parquet')")
    con.execute(f"CREATE VIEW dim_category AS SELECT * FROM read_parquet('{C.P_MART}/dim_category.parquet')")
    con.execute(f"CREATE VIEW fin AS SELECT * FROM read_csv('{C.FINANCE.as_posix()}')")
    print("rendering charts ->", C.SCREENSHOTS)

    # ---------------------------------------------------------------- 1. months
    d = con.execute("""
        SELECT a.business_month AS m, sum(a.revenue) AS rev, f.revenue_inr AS fin
        FROM agg a JOIN fin f ON f.month = a.business_month
        GROUP BY 1, f.revenue_inr ORDER BY 1""").fetchdf()
    fig, ax = plt.subplots(figsize=(10, 4.4))
    xs = range(len(d))
    ax.bar(xs, d.rev, width=0.58, color=S1, zorder=3)
    for i, (v, f) in enumerate(zip(d.rev, d.fin)):
        if abs(v - f) > 0.005:
            ax.plot([i], [f], marker="_", markersize=22, markeredgewidth=2.2,
                    color=NEG, zorder=5)
    ax.set_xticks(list(xs))
    ax.set_xticklabels([x[5:] for x in d.m])
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lakhs))
    ax.set_xlabel("month of 2024")
    title(ax, "Revenue by month",
          "bars: the platform.  red dash: the finance figure, drawn only on the three "
          "months where it differs -- the other nine match to the paisa")
    save(fig, "chart_01_revenue_by_month.png")

    # ------------------------------------------------------------- 2. variance
    d2 = con.execute("""
        SELECT f.month AS m, sum(a.revenue) - f.revenue_inr AS diff
        FROM agg a JOIN fin f ON f.month = a.business_month
        GROUP BY 1, f.revenue_inr ORDER BY 1""").fetchdf()
    # Nine of the twelve differences are exactly zero and the other three span
    # four orders of magnitude (486,250 against 50.48).  No single linear axis
    # shows that honestly, and a log axis cannot render a zero at all -- so this
    # is a status strip with the numbers written on it, not a bar chart.
    causes = {"2024-03": ("source scope", "not in the till data at all"),
              "2024-07": ("source data", "3 store-days never exported"),
              "2024-12": ("immaterial", "0.0001% -- a hand-keyed close")}
    fig, ax = plt.subplots(figsize=(12, 3.4))
    for i, r in d2.iterrows():
        v = float(r["diff"])
        agree = abs(v) < 0.005
        col = NEUTRAL if agree else (NEG if v < 0 else "#ec835a")
        ax.add_patch(plt.Rectangle((i - 0.42, 0), 0.84, 0.5, facecolor=col,
                                   edgecolor=SURFACE, linewidth=2, zorder=3))
        ax.text(i, 0.60, r["m"][5:], ha="center", va="bottom", fontsize=10, color=INK)
        if agree:
            ax.text(i, 0.25, "0.00", ha="center", va="center", fontsize=8.6,
                    color=INK, zorder=4)
        else:
            lab, why = causes[r["m"]]
            # the value goes UNDER the cell: 486,250.00 does not fit inside one
            ax.text(i, -0.12, f"{v:,.2f}", ha="center", va="top", fontsize=10,
                    color=INK, fontweight="bold")
            ax.text(i, -0.34, f"{lab}\n{why}", ha="center", va="top",
                    fontsize=8.6, color=INK2)
    ax.set_xlim(-0.6, len(d2) - 0.4)
    ax.set_ylim(-0.78, 0.95)
    ax.axis("off")
    fig.suptitle("Platform minus finance, month by month", x=0.045, y=1.13,
                 ha="left", fontsize=13, fontweight="bold", color=INK)
    fig.text(0.045, 1.00, "grey = agrees to the paisa (9 of 12).  the three "
                           "exceptions carry their value and their cause",
             ha="left", fontsize=9.5, color=INK2)
    save(fig, "chart_02_variance_vs_finance.png")

    # ---------------------------------------------------------------- 3. stores
    d3 = con.execute("""
        SELECT s.store_name || '  (' || s.city || ')' AS label, sum(a.revenue) AS rev
        FROM agg a JOIN dim_store s USING (store_id)
        GROUP BY 1 ORDER BY rev""").fetchdf()
    fig, ax = plt.subplots(figsize=(9, 5.2))
    ys = list(range(len(d3)))
    ax.barh(ys, d3.rev, height=0.62, color=S1, zorder=3)
    ax.set_yticks(ys)
    ax.set_yticklabels(d3.label)
    ax.set_xlim(0, d3.rev.max() * 1.16)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lakhs))
    ax.grid(axis="y", visible=False)
    for y, v in zip(ys, d3.rev):
        ax.text(v * 1.012, y, lakhs(v), va="center", fontsize=9, color=INK)
    title(ax, "Revenue by store, full year 2024")
    save(fig, "chart_03_revenue_by_store.png")

    # ------------------------------------------------------------ 4. categories
    d4 = con.execute("""
        SELECT c.category_name AS label, sum(a.revenue) AS rev
        FROM agg a JOIN dim_category c USING (category_id)
        GROUP BY 1 ORDER BY rev""").fetchdf()
    fig, ax = plt.subplots(figsize=(9, 5.6))
    ys = list(range(len(d4)))
    cols = [NEG if v < 0 else S1 for v in d4.rev]
    ax.barh(ys, d4.rev, height=0.62, color=cols, zorder=3)
    ax.axvline(0, color=INK2, linewidth=1.1, zorder=4)
    ax.set_yticks(ys)
    ax.set_yticklabels(d4.label)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lakhs))
    ax.grid(axis="y", visible=False)
    for y, v in zip(ys, d4.rev):
        ax.text(v + (0.01 if v >= 0 else -0.01) * abs(d4.rev).max(), y, lakhs(v),
                va="center", ha="left" if v >= 0 else "right", fontsize=9, color=INK)
    ax.set_xlim(min(d4.rev) * 3.2, max(d4.rev) * 1.16)   # room for the
    # negative bar's outboard label, which otherwise runs into the tick text
    title(ax, "Revenue by product category, full year 2024",
          "'Bill-level discount' is negative by construction: it is the discount "
          "that belongs to a bill, not to a product")
    save(fig, "chart_04_revenue_by_category.png")

    # ---------------------------------------------------------- 5. day of week
    d5 = con.execute("""
        SELECT a.day_of_week AS dow, a.dow_no,
               sum(a.revenue)/count(DISTINCT a.business_date) AS avg_day
        FROM agg a GROUP BY 1,2 ORDER BY a.dow_no""").fetchdf()
    fig, ax = plt.subplots(figsize=(8.6, 4.2))
    mx = d5.avg_day.max()
    cols = [S1 if v == mx else "#9ec5f4" for v in d5.avg_day]
    ax.bar(range(len(d5)), d5.avg_day, width=0.6, color=cols, zorder=3)
    ax.set_xticks(range(len(d5)))
    ax.set_xticklabels([x[:3] for x in d5.dow])
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lakhs))
    for i, v in enumerate(d5.avg_day):
        ax.annotate(lakhs(v), (i, v), textcoords="offset points", xytext=(0, 6),
                    ha="center", fontsize=9, color=INK)
    ax.set_ylim(0, mx * 1.16)
    title(ax, "Average revenue per trading day, by day of the week",
          "all 12 stores, full year")
    save(fig, "chart_05_day_of_week.png")

    # ------------------------------------------------------- 6. store x month
    d6 = con.execute("""
        SELECT s.store_id, s.city, a.business_month AS m, sum(a.revenue) AS rev
        FROM agg a JOIN dim_store s USING (store_id)
        GROUP BY 1,2,3 ORDER BY 1,3""").fetchdf()
    piv = d6.pivot(index="store_id", columns="m", values="rev")
    labels = (d6[["store_id", "city"]].drop_duplicates()
              .set_index("store_id").loc[piv.index, "city"])
    from matplotlib.colors import LinearSegmentedColormap
    cmap = LinearSegmentedColormap.from_list("seq_blue", SEQ)
    fig, ax = plt.subplots(figsize=(10, 4.8))
    im = ax.imshow(piv.values, aspect="auto", cmap=cmap)
    ax.set_xticks(range(piv.shape[1]))
    ax.set_xticklabels([c[5:] for c in piv.columns])
    ax.set_yticks(range(piv.shape[0]))
    ax.set_yticklabels([f"{i}  {c}" for i, c in zip(piv.index, labels)])
    ax.grid(False)
    cb = fig.colorbar(im, ax=ax, pad=0.015)
    cb.ax.yaxis.set_major_formatter(mticker.FuncFormatter(lakhs))
    cb.outline.set_visible(False)
    cb.ax.tick_params(color=GRID, labelcolor=INK2)
    title(ax, "Revenue by store and month",
          "one hue, light to dark.  the October column is Diwali, not a bug")
    ax.set_xlabel("month of 2024")
    save(fig, "chart_06_store_month_heatmap.png")

    # ----------------------------------------------------------- 7. the 2.17x
    oct_naive = con.execute(f"""
        SELECT sum(qty*unit_price) FROM read_parquet('{C.P_RAW}/**/*.parquet',
        hive_partitioning=1) WHERE business_month='2024-10'""").fetchone()[0]
    oct_notender = con.execute(f"""
        SELECT sum(qty*unit_price) FROM read_parquet('{C.P_RAW}/**/*.parquet',
        hive_partitioning=1) WHERE business_month='2024-10'
          AND line_type <> 'TENDER'""").fetchone()[0]
    oct_rev = con.execute(
        "SELECT sum(revenue) FROM agg WHERE business_month='2024-10'").fetchone()[0]
    names = ["sum every row\nin the file",
             "drop TENDER,\nkeep the GST",
             "revenue\n(as defined)",
             "finance,\nsigned off"]
    vals = [float(oct_naive), float(oct_notender), float(oct_rev), 56359195.92]
    cols = [NEG, "#ec835a", S1, NEUTRAL]
    fig, ax = plt.subplots(figsize=(8.4, 4.4))
    ax.bar(range(4), vals, width=0.56, color=cols, zorder=3)
    for i, v in enumerate(vals):
        ax.annotate(f"{v:,.0f}", (i, v), textcoords="offset points", xytext=(0, 7),
                    ha="center", fontsize=9.5, color=INK)
    ax.set_xticks(range(4))
    ax.set_xticklabels(names)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lakhs))
    ax.set_ylim(0, max(vals) * 1.17)
    title(ax, "October 2024, four ways of adding it up",
          f"the left-hand bar is {vals[0]/vals[2]:.2f}x the right-hand one, and it is the "
          "easy mistake to make")
    save(fig, "chart_07_october_trap.png")

    # ------------------------------------------------------------- 8. layout
    import json
    lay = json.load(open(C.RESULTS / "a_layout.json"))
    labels = ["shared folder today\n(4,457 CSV files)",
              "same rows, Parquet,\none flat prefix",
              "partitioned lake,\nwhole year",
              "partitioned lake,\npruned to S03/2024-10"]
    files = [lay["source_csv_files"], lay["flat_objects"],
             lay["partitioned_objects_total"], lay["pruned_objects"]]
    byts = [lay["source_csv_bytes"], lay["flat_bytes"],
            lay["partitioned_bytes_total"], lay["pruned_bytes"]]
    fig, axes = plt.subplots(1, 2, figsize=(11.5, 4.2))
    for ax, vals, head, fmt in (
        (axes[0], files, "Files the engine could have to open", lambda v: f"{v:,}"),
        (axes[1], byts, "Bytes the engine could have to open", lakhs),
    ):
        cols = [NEUTRAL, "#9ec5f4", "#5598e7", S1]
        ax.bar(range(4), vals, width=0.56, color=cols, zorder=3)
        ax.set_yscale("log")
        ax.set_xticks(range(4))
        ax.set_xticklabels(labels, fontsize=8.4)
        for i, v in enumerate(vals):
            ax.annotate(f"{v:,}", (i, v), textcoords="offset points", xytext=(0, 6),
                        ha="center", fontsize=9, color=INK)
        title(ax, head, "log scale -- the range is four orders of magnitude")
    fig.suptitle("(a)  one store, one month:  4,389 objects -> 1",
                 x=0.005, ha="left", fontsize=13, fontweight="bold", color=INK)
    fig.tight_layout(rect=(0, 0, 1, 0.93))
    save(fig, "chart_08_layout_pruning.png")

    print("done.")


if __name__ == "__main__":
    main()
