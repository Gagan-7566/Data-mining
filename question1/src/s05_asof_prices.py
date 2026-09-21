"""
(d) Make March use March's prices.

Two SQL files, both parameterised, neither edited between runs:

    sql/d_period_report.sql        the period report
    sql/d_what_did_it_sell_for.sql the category manager's question

Each is read from disk, printed verbatim once, and then executed twice with
nothing changed but the value bound to $period.  If the two runs give
different prices, the period -- not the code -- is what decided them.
"""
from __future__ import annotations

import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import config as C

PERIOD_A = "2024-03"          # "last March"
PERIOD_B = "2024-11"          # "last month"


def setup_views(con) -> None:
    C.attach_postgres(con)
    con.execute(f"CREATE OR REPLACE VIEW fact_sale_line AS SELECT * FROM read_parquet('{C.P_MART}/fact_sale_line/**/*.parquet', hive_partitioning=1)")
    con.execute(f"CREATE OR REPLACE VIEW dim_product    AS SELECT * FROM read_parquet('{C.P_MART}/dim_product.parquet')")
    con.execute(f"CREATE OR REPLACE VIEW dim_store      AS SELECT * FROM read_parquet('{C.P_MART}/dim_store.parquet')")
    # price_revisions is NOT copied -- it is read live from PostgreSQL
    con.execute("CREATE OR REPLACE VIEW price_revisions AS SELECT * FROM pg.price_revisions")


def main() -> None:
    with C.Tee(C.LOGS / "s05_asof_prices.log"):
        C.rule("(d) SAME QUERY, DIFFERENT PERIOD -- prices follow the period")
        con = C.connect_duckdb(memory=True)
        setup_views(con)

        report_sql = (C.SQL_DIR / "d_period_report.sql").read_text(encoding="utf-8")
        lookup_sql = (C.SQL_DIR / "d_what_did_it_sell_for.sql").read_text(encoding="utf-8")

        # ------------------------------------------------------------------
        # 1. the category manager's question, asked twice
        # ------------------------------------------------------------------
        C.rule("1. 'What did that biscuit pack sell for in March?'", "-")

        # pick a biscuit pack whose price actually moved between the two periods
        code = con.execute(f"""
            SELECT dp.product_code
            FROM dim_product dp
            JOIN price_revisions a ON a.product_sk=dp.product_sk
                 AND last_day(DATE '{PERIOD_A}-01') BETWEEN a.effective_from AND a.effective_to
            JOIN price_revisions b ON b.product_sk=dp.product_sk
                 AND last_day(DATE '{PERIOD_B}-01') BETWEEN b.effective_from AND b.effective_to
            WHERE dp.category_id='C01' AND a.selling_price <> b.selling_price
            ORDER BY abs(b.selling_price-a.selling_price) DESC
            LIMIT 1""").fetchone()[0]

        print(f"\nthe query (sql/d_what_did_it_sell_for.sql), unchanged between runs:\n")
        print(lookup_sql)
        print(f"\nbound values: $code = '{code}'\n")
        for per in (PERIOD_A, PERIOD_B, "2024-12"):
            df = con.execute(lookup_sql, {"period": per, "code": code}).fetchdf()
            print(f"  $period = '{per}'")
            print("   ", df.to_string(index=False).replace("\n", "\n    "))
            print()

        a = con.execute(lookup_sql, {"period": PERIOD_A, "code": code}).fetchdf().iloc[0]
        b = con.execute(lookup_sql, {"period": PERIOD_B, "code": code}).fetchdf().iloc[0]
        print(f"  ask about {PERIOD_A} -> {a.selling_price_then}")
        print(f"  ask about {PERIOD_B} -> {b.selling_price_then}")
        print(f"  the shelf price today is {b.selling_price_then}, which is the number "
              f"the spreadsheet\n  gave the category manager when they asked about March. "
              f"March's price was {a.selling_price_then}.")

        # ------------------------------------------------------------------
        # 2. the harder case -- a code that was reissued
        # ------------------------------------------------------------------
        C.rule("2. the same question on a code that was REISSUED in June 2024", "-")
        reissued = con.execute("""
            SELECT product_code FROM dim_product
            WHERE product_code IN (SELECT product_code FROM dim_product
                                   GROUP BY 1 HAVING count(*)>1)
            ORDER BY 1 LIMIT 1""").fetchone()[0]
        print(f"\n  $code = '{reissued}'  -- one code, two different products\n")
        for per in ("2024-03", "2024-11"):
            df = con.execute(lookup_sql, {"period": per, "code": reissued}).fetchdf()
            r = df.iloc[0]
            print(f"  $period = '{per}':  {r.what_that_code_meant_then}")
            print(f"{'':20}category {r.category_then}, sold at {r.selling_price_then}")
        print("\n  Same code, same query, different period -- and it is not merely a"
              "\n  different price, it is a different product in a different category."
              "\n  A report that joins on the code alone files March's coriander powder"
              "\n  under Confectionery.")

        # ------------------------------------------------------------------
        # 3. the period report, run twice
        # ------------------------------------------------------------------
        C.rule("3. the period report -- one query text, two periods", "-")
        print("\nthe query (sql/d_period_report.sql), unchanged between runs:\n")
        print(report_sql)

        out = {}
        for per in (PERIOD_A, PERIOD_B):
            df = con.execute(report_sql, {"period": per}).fetchdf()
            out[per] = df
            print(f"\n--- EXECUTE with $period = '{per}'  (nothing else changed) ---")
            print(df.to_string(index=False))
            print(f"    total revenue as billed        : "
                  f"{df.revenue_as_billed.sum():,.2f}")
            print(f"    total at the price then in force: "
                  f"{df.revenue_at_price_in_force.sum():,.2f}")
            print(f"    till-sync drift                 : "
                  f"{df.till_sync_drift.sum():,.2f}")

        # ------------------------------------------------------------------
        # 4. proof the price actually used varies by period
        # ------------------------------------------------------------------
        C.rule("4. proof: the price the report used, per period", "-")
        proof = con.execute(f"""
            WITH x AS (
              SELECT '{PERIOD_A}' AS period, dp.product_code, dp.product_name,
                     pr.selling_price, pr.effective_from, pr.effective_to
              FROM dim_product dp JOIN price_revisions pr ON pr.product_sk=dp.product_sk
              WHERE dp.product_code='{code}'
                AND last_day(DATE '{PERIOD_A}-01') BETWEEN pr.effective_from AND pr.effective_to
              UNION ALL
              SELECT '{PERIOD_B}', dp.product_code, dp.product_name,
                     pr.selling_price, pr.effective_from, pr.effective_to
              FROM dim_product dp JOIN price_revisions pr ON pr.product_sk=dp.product_sk
              WHERE dp.product_code='{code}'
                AND last_day(DATE '{PERIOD_B}-01') BETWEEN pr.effective_from AND pr.effective_to)
            SELECT * FROM x ORDER BY period""").fetchdf()
        print(proof.to_string(index=False))
        print("\n  full revision history for that code, from PostgreSQL:")
        hist = con.execute(f"""
            SELECT pr.selling_price, pr.mrp, pr.effective_from, pr.effective_to
            FROM dim_product dp JOIN price_revisions pr ON pr.product_sk=dp.product_sk
            WHERE dp.product_code='{code}' ORDER BY pr.effective_from""").fetchdf()
        print("   ", hist.to_string(index=False).replace("\n", "\n    "))

        for per, df in out.items():
            df.to_csv(C.RESULTS / f"d_period_report_{per}.csv", index=False)
        json.dump(dict(
            code=code, reissued_code=reissued,
            price_march=float(a.selling_price_then),
            price_november=float(b.selling_price_then),
            product_march=str(a.what_that_code_meant_then),
            totals={p: float(df.revenue_as_billed.sum()) for p, df in out.items()},
            drift={p: float(df.till_sync_drift.sum()) for p, df in out.items()},
        ), open(C.RESULTS / "d_asof.json", "w"), indent=2)
        print("\nwritten -> results/d_period_report_*.csv, d_asof.json")


if __name__ == "__main__":
    main()
