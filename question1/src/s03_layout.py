"""
(a) evidence -- does the layout actually stop the engine opening the other
    eleven stores and the other eleven months?

Builds a CONTROL copy of exactly the same rows in a single flat prefix
(s3://annapurna-lake/flat/sales/, one object per store-day, the shape the
shared folder is in today), then runs the identical question against both
layouts and reports, for each:

    * how many objects the engine could possibly have to open
    * how many bytes those objects are
    * what the engine actually did, from EXPLAIN ANALYZE
    * wall clock

Nothing here is asserted from documentation: the file counts come from an S3
LIST, the byte counts from the object sizes MinIO reports, and the scan detail
from DuckDB's own profile.
"""
from __future__ import annotations

import json
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import config as C

# The question the CFO actually asks: one store, one month.
Q_STORE = "S03"
Q_MONTH = "2024-10"


def list_objects(prefix: str) -> list[dict]:
    s3 = C.s3_client()
    out = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=C.BUCKET, Prefix=prefix):
        out.extend(page.get("Contents", []))
    return out


def build_flat_control(con) -> None:
    """Same rows, one object per store-day, all in ONE prefix -- i.e. what you
    get if you just dump the nightly exports somewhere and point a query at
    them.  Built from the lake so the row set is identical by construction."""
    existing = list_objects("flat/sales/")
    if existing:
        print(f"      control layout already present ({len(existing)} objects)")
        return
    print("      building the flat control layout (one object per store-day) ...")
    t0 = time.time()
    con.execute(
        f"""
        COPY (
          SELECT bill_no, line_no, store_id, business_date, business_month,
                 raw_code, qty, unit_price, line_type, txn_ts, source_file,
                 store_id || '_' || strftime(business_date,'%Y%m%d') AS daykey
          FROM read_parquet('{C.P_RAW}/**/*.parquet', hive_partitioning=1)
        )
        TO '{C.P_FLAT}'
        (FORMAT parquet, COMPRESSION zstd, PARTITION_BY (daykey),
         OVERWRITE_OR_IGNORE true, FILENAME_PATTERN 'data_{{i}}');
        """
    )
    print(f"      built in {time.time()-t0:.1f}s")


def flatten_keys(objs: list[dict]) -> tuple[int, int]:
    return len(objs), sum(o["Size"] for o in objs)


def timed(con, sql: str, reps: int = 3) -> tuple[float, object]:
    con.execute(sql).fetchall()          # warm the connection, not a cache
    best = None
    res = None
    for _ in range(reps):
        t0 = time.perf_counter()
        res = con.execute(sql).fetchall()
        dt = time.perf_counter() - t0
        best = dt if best is None else min(best, dt)
    return best, res


def explain(con, sql: str) -> str:
    return con.execute("EXPLAIN ANALYZE " + sql).fetchall()[0][1]


def main() -> None:
    with C.Tee(C.LOGS / "s03_layout.log"):
        C.rule("(a) LAYOUT -- what the engine can possibly have to open")

        con = C.connect_duckdb(memory=True)
        build_flat_control(con)

        # ---------------------------------------------------------------
        # what is physically there
        # ---------------------------------------------------------------
        part_all = list_objects("raw/sales/")
        part_one = list_objects(f"raw/sales/store_id={Q_STORE}/business_month={Q_MONTH}/")
        flat_all = list_objects("flat/sales/")
        src_files = list(C.SALES_DIR.iterdir())
        src_bytes = sum(p.stat().st_size for p in src_files if p.is_file())

        print(f"\nThe question: revenue for store {Q_STORE} in {Q_MONTH}.\n")
        rows = [
            ("today: shared folder, raw CSV, one folder",
             len([p for p in src_files if p.suffix == '.csv']), src_bytes),
            ("control: same rows as Parquet, ONE flat prefix",
             *flatten_keys(flat_all)),
            (f"chosen: store_id=/business_month=  (whole lake)",
             *flatten_keys(part_all)),
            (f"chosen: partition pruned to {Q_STORE}/{Q_MONTH}",
             *flatten_keys(part_one)),
        ]
        print(f"{'layout':<48}{'objects':>10}{'bytes':>16}")
        print("-" * 74)
        for name, n, b in rows:
            print(f"{name:<48}{n:>10,}{b:>16,}")

        n_flat, b_flat = flatten_keys(flat_all)
        n_one, b_one = flatten_keys(part_one)
        print("-" * 74)
        print(f"\nfiles the engine could have to open : {n_flat:,}  ->  {n_one:,}"
              f"   ({n_flat / max(n_one,1):,.0f}x fewer)")
        print(f"bytes the engine could have to open : {b_flat:,}  ->  {b_one:,}"
              f"   ({b_flat / max(b_one,1):,.0f}x fewer)")
        print(f"against the shared folder as it stands today: "
              f"{len([p for p in src_files if p.suffix=='.csv']):,} files / {src_bytes:,} bytes"
              f"  ->  {n_one:,} file / {b_one:,} bytes")

        # ---------------------------------------------------------------
        # the same question, both layouts, engine evidence
        # ---------------------------------------------------------------
        q_part = f"""
            SELECT store_id, business_month,
                   round(sum(qty*unit_price),2) AS revenue,
                   count(DISTINCT bill_no) AS bills
            FROM read_parquet('{C.P_RAW}/**/*.parquet', hive_partitioning=1)
            WHERE store_id = '{Q_STORE}'
              AND business_month = '{Q_MONTH}'
              AND line_type NOT IN ('TAX','TENDER')
            GROUP BY 1,2"""

        q_flat = f"""
            SELECT store_id, business_month,
                   round(sum(qty*unit_price),2) AS revenue,
                   count(DISTINCT bill_no) AS bills
            FROM read_parquet('{C.P_FLAT}/**/*.parquet')
            WHERE store_id = '{Q_STORE}'
              AND business_month = '{Q_MONTH}'
              AND line_type NOT IN ('TAX','TENDER')
            GROUP BY 1,2"""

        C.rule("the identical question, run against each layout", "-")
        t_part, r_part = timed(con, q_part)
        t_flat, r_flat = timed(con, q_flat)
        print(f"\npartitioned : {t_part*1000:9.1f} ms   result {r_part}")
        print(f"flat folder : {t_flat*1000:9.1f} ms   result {r_flat}")
        print(f"\nsame answer from both layouts: {r_part == r_flat}")
        print(f"speed-up: {t_flat/t_part:,.1f}x")

        C.rule("EXPLAIN ANALYZE -- partitioned layout", "-")
        ep = explain(con, q_part)
        print(ep)
        C.rule("EXPLAIN ANALYZE -- flat layout", "-")
        ef = explain(con, q_flat)
        print(ef)

        json.dump(
            dict(
                query_store=Q_STORE, query_month=Q_MONTH,
                source_csv_files=len([p for p in src_files if p.suffix == ".csv"]),
                source_csv_bytes=src_bytes,
                flat_objects=n_flat, flat_bytes=b_flat,
                partitioned_objects_total=len(part_all),
                partitioned_bytes_total=sum(o["Size"] for o in part_all),
                pruned_objects=n_one, pruned_bytes=b_one,
                ms_partitioned=round(t_part * 1000, 1),
                ms_flat=round(t_flat * 1000, 1),
                same_answer=bool(r_part == r_flat),
            ),
            open(C.RESULTS / "a_layout.json", "w"), indent=2,
        )
        (C.RESULTS / "a_explain_partitioned.txt").write_text(ep, encoding="utf-8")
        (C.RESULTS / "a_explain_flat.txt").write_text(ef, encoding="utf-8")
        print(f"\nwritten -> results/a_layout.json, a_explain_*.txt")


if __name__ == "__main__":
    main()
