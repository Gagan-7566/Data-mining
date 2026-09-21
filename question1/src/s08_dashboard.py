"""
(c) The dashboard, answering the four questions the CFO asked for.

    "slice revenue by store, by product category, by day of the week and by
     month -- and the same number every time I ask"

Every slice below comes from agg_revenue_day, the aggregate built in
s04_model.py.  It is 65,685 rows -- store x category x day -- which is the
finest grain any of the four slices needs, and it is small enough that all of
them answer in milliseconds.

The last section is the part the CFO actually cares about: asking for October
twenty times, from four differently-written queries, and getting one number.
"""
from __future__ import annotations

import json
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import config as C


def views(con):
    con.execute(f"CREATE OR REPLACE VIEW agg  AS SELECT * FROM read_parquet('{C.P_MART}/agg_revenue_day.parquet')")
    con.execute(f"CREATE OR REPLACE VIEW fact AS SELECT * FROM read_parquet('{C.P_MART}/fact_sale_line/**/*.parquet', hive_partitioning=1)")
    con.execute(f"CREATE OR REPLACE VIEW dim_store    AS SELECT * FROM read_parquet('{C.P_MART}/dim_store.parquet')")
    con.execute(f"CREATE OR REPLACE VIEW dim_category AS SELECT * FROM read_parquet('{C.P_MART}/dim_category.parquet')")
    con.execute(f"CREATE OR REPLACE VIEW dim_product  AS SELECT * FROM read_parquet('{C.P_MART}/dim_product.parquet')")
    con.execute(f"CREATE OR REPLACE VIEW dim_date     AS SELECT * FROM read_parquet('{C.P_MART}/dim_date.parquet')")


def timed(con, sql, reps=5):
    con.execute(sql).fetchdf()
    best = min((lambda t0: (con.execute(sql).fetchall(), time.perf_counter() - t0)[1])
               (time.perf_counter()) for _ in range(reps))
    return con.execute(sql).fetchdf(), best * 1000


def main() -> None:
    with C.Tee(C.LOGS / "s08_dashboard.log"):
        C.rule("(c) THE DASHBOARD -- the four slices, from agg_revenue_day")
        con = C.connect_duckdb(memory=True)
        views(con)

        out = {}

        # ---------------------------------------------------------- by store
        C.rule("slice 1 -- revenue by store", "-")
        df, ms = timed(con, """
            SELECT s.store_id, s.store_name, s.city, s.region,
                   round(sum(a.revenue),2) AS revenue,
                   sum(a.bills)            AS bills,
                   round(sum(a.revenue)/sum(a.bills),2) AS avg_bill,
                   round(sum(a.revenue)/max(s.floor_area_sqft),2) AS revenue_per_sqft
            FROM agg a JOIN dim_store s USING (store_id)
            GROUP BY 1,2,3,4 ORDER BY revenue DESC""")
        print(df.to_string(index=False))
        print(f"\n  {ms:.1f} ms")
        out["by_store"] = df

        # ------------------------------------------------------- by category
        C.rule("slice 2 -- revenue by product category", "-")
        df, ms = timed(con, """
            SELECT c.category_id, c.category_name, c.department,
                   round(sum(a.revenue),2) AS revenue,
                   round(100*sum(a.revenue)/sum(sum(a.revenue)) OVER (),2) AS pct_of_total,
                   sum(a.units) AS units
            FROM agg a JOIN dim_category c USING (category_id)
            GROUP BY 1,2,3 ORDER BY revenue DESC""")
        print(df.to_string(index=False))
        print(f"\n  {ms:.1f} ms")
        print("  C00 is the bill-level discount bucket: discounts that belong to a bill")
        print("  rather than to any product.  It is shown rather than hidden so the")
        print("  category column still adds up to total revenue.")
        out["by_category"] = df

        # ---------------------------------------------------- by day of week
        C.rule("slice 3 -- revenue by day of the week", "-")
        df, ms = timed(con, """
            SELECT a.day_of_week,
                   round(sum(a.revenue),2)                       AS revenue,
                   count(DISTINCT a.business_date)               AS days,
                   round(sum(a.revenue)/count(DISTINCT a.business_date),2) AS avg_per_day,
                   sum(a.bills)                                  AS bills
            FROM agg a GROUP BY 1, a.dow_no ORDER BY a.dow_no""")
        print(df.to_string(index=False))
        print(f"\n  {ms:.1f} ms")
        out["by_dow"] = df

        # ----------------------------------------------------------- by month
        C.rule("slice 4 -- revenue by month", "-")
        df, ms = timed(con, """
            SELECT a.business_month,
                   round(sum(a.revenue),2) AS revenue,
                   sum(a.bills) AS bills,
                   count(DISTINCT a.business_date) AS trading_days
            FROM agg a GROUP BY 1 ORDER BY 1""")
        print(df.to_string(index=False))
        print(f"\n  {ms:.1f} ms")
        out["by_month"] = df

        # ------------------------------------------------- the cross-slice
        C.rule("all four at once -- store x category x day-of-week x month", "-")
        df, ms = timed(con, """
            SELECT s.store_name, c.category_name, a.day_of_week, a.business_month,
                   round(sum(a.revenue),2) AS revenue
            FROM agg a
            JOIN dim_store s    USING (store_id)
            JOIN dim_category c USING (category_id)
            WHERE a.business_month='2024-10' AND s.store_id='S03'
              AND c.category_id IN ('C01','C04')
            GROUP BY ALL ORDER BY 2,3""")
        print(df.to_string(index=False))
        print(f"\n  {ms:.1f} ms")
        out["cross"] = df

        # ------------------------------------------------ the same number
        C.rule("'the same number every time I ask'", "=")
        print("""
Four differently-written queries for October revenue, run five times each.
Two read the aggregate, one reads the fact table, one goes all the way back to
the raw landed lines in the object store.  If the model is sound they cannot
disagree.
""")
        variants = {
            "agg, filtered on business_month":
                "SELECT round(sum(revenue),2) FROM agg WHERE business_month='2024-10'",
            "agg, rolled up from store and category":
                """SELECT round(sum(v),2) FROM (
                     SELECT store_id, category_id, sum(revenue) v FROM agg
                     WHERE business_month='2024-10' GROUP BY 1,2)""",
            "fact table, joined to dim_date":
                """SELECT round(sum(f.line_amount),2) FROM fact f
                   JOIN dim_date d USING (business_date)
                   WHERE d.business_month='2024-10'""",
            "raw landed lines in the object store":
                f"""SELECT round(sum(qty*unit_price),2)
                    FROM read_parquet('{C.P_RAW}/**/*.parquet', hive_partitioning=1)
                    WHERE business_month='2024-10' AND line_type NOT IN ('TAX','TENDER')""",
        }
        answers = []
        for name, sql in variants.items():
            vals = [con.execute(sql).fetchone()[0] for _ in range(5)]
            v = float(vals[0])
            answers.append(v)
            print(f"  {name:<42} {v:>18,.2f}   "
                  f"{'all 5 runs identical' if len(set(map(float, vals)))==1 else 'UNSTABLE'}")
        print(f"\n  {'finance, signed off 2024-11-09':<42} {56359195.92:>18,.2f}")
        same = len({round(a, 2) for a in answers}) == 1
        print(f"\n  all four routes agree : {same}")
        print(f"  agrees with finance   : {abs(answers[0]-56359195.92) < 0.005}")

        C.rule("and the number the old spreadsheet gives", "-")
        naive = con.execute(f"""
            SELECT round(sum(qty*unit_price),2)
            FROM read_parquet('{C.P_RAW}/**/*.parquet', hive_partitioning=1)
            WHERE business_month='2024-10'""").fetchone()[0]
        print(f"\n  sum of every row in October's files : {float(naive):>18,.2f}")
        print(f"  revenue                             : {answers[0]:>18,.2f}")
        print(f"  inflation                           : {float(naive)/answers[0]:>18.2f}x")
        print("\n  That is the surprisingly healthy October.  It is TENDER -- the bill")
        print("  total, written into the same file as another row -- plus the GST.")

        for k, v in out.items():
            v.to_csv(C.RESULTS / f"c_dashboard_{k}.csv", index=False)
        json.dump(dict(
            october_all_routes=answers, october_finance=56359195.92,
            all_routes_agree=bool(same), naive_october=float(naive),
            inflation=float(naive) / answers[0],
        ), open(C.RESULTS / "c_dashboard.json", "w"), indent=2)
        print("\nwritten -> results/c_dashboard_*.csv, c_dashboard.json")


if __name__ == "__main__":
    main()
