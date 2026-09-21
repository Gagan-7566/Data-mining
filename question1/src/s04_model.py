"""
(c) The tables behind the dashboard.

The dashboard has to slice revenue by store, by product category, by day of
week and by month, and it must not carry the store's name and address on every
one of the sale lines.  So: a star.

    dim_store     12 rows      published from PostgreSQL
    dim_category  15 rows      from PostgreSQL + one 'unknown' member
    dim_product   1,225 rows   from PostgreSQL + one 'unknown' member, SCD-2
    dim_date      366 rows     generated -- carries day-of-week and month
    fact_sale_line             one row per REVENUE-BEARING line, keys only
    agg_revenue_day            the table the dashboard actually reads

TWO THINGS ABOUT THE SOURCE, BOTH OF WHICH CHANGE THE ANSWER
------------------------------------------------------------
1. Not every line is a sale.  TENDER is the bill total written as another row
   in the same file, and TAX is GST.  Summing every row counts each bill about
   twice and adds the GST on top.  That is the "October looks surprisingly
   healthy" bug, and it is worth 2.17x on this dataset.  The fact table admits
   only SALE / RETURN / DISCOUNT / VOID.  RETURN, DISCOUNT and VOID carry their
   own sign and subtract; VOID lines are kept alongside the SALE lines they
   mirror so a cancelled bill nets to zero, which is the half of the rule that
   is usually dropped.

2. A product code does not identify a product.  In June 2024 two dozen retired
   codes were reissued to different products, several in a different category.
   Joining on product_code alone returns two master rows for those codes: the
   line is duplicated and half of them land in the wrong category.  The fact
   is keyed on product_sk, resolved by (product_code, business_date) against
   products.valid_from / valid_to.

Bill-level DISCOUNT rows carry no product code, so they resolve to an explicit
unknown member (product_sk = -1, category C00 'Bill-level discount') rather
than being dropped.  Revenue by category therefore still adds up to total
revenue.  Allocating those discounts pro-rata across the bill's items is the
other defensible choice; it is not the default because it invents a product
attribution the till never recorded.
"""
from __future__ import annotations

import json
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import config as C

LAKE = f"read_parquet('{C.P_RAW}/**/*.parquet', hive_partitioning=1)"


def build(con) -> None:
    # ----------------------------------------------------------------- dims
    print("  dim_store      ", end="", flush=True)
    con.execute(f"""
        COPY (SELECT store_id, store_name, address_line, city, state, region,
                     floor_area_sqft, opened_on
              FROM pg.stores ORDER BY store_id)
        TO '{C.P_MART}/dim_store.parquet' (FORMAT parquet, OVERWRITE_OR_IGNORE true);""")
    print("ok")

    print("  dim_category   ", end="", flush=True)
    con.execute(f"""
        COPY (
          SELECT category_id, category_name, department, gst_rate
          FROM pg.product_categories
          UNION ALL
          -- explicit unknown member: bill-level discounts belong to no product
          SELECT 'C00', 'Bill-level discount', 'Unallocated', 0.0
        ORDER BY 1)
        TO '{C.P_MART}/dim_category.parquet' (FORMAT parquet, OVERWRITE_OR_IGNORE true);""")
    print("ok")

    print("  dim_product    ", end="", flush=True)
    con.execute(f"""
        COPY (
          SELECT p.product_sk, p.product_code, p.product_name, p.brand, p.pack_size,
                 p.uom, p.category_id, c.category_name, c.department, c.gst_rate,
                 p.valid_from, p.valid_to, p.is_current
          FROM pg.products p
          JOIN pg.product_categories c USING (category_id)
          UNION ALL
          SELECT -1, 'DISC', 'Bill-level discount', NULL, NULL, NULL,
                 'C00', 'Bill-level discount', 'Unallocated', 0.0,
                 DATE '1900-01-01', DATE '9999-12-31', TRUE
        ORDER BY 1)
        TO '{C.P_MART}/dim_product.parquet' (FORMAT parquet, OVERWRITE_OR_IGNORE true);""")
    print("ok")

    print("  dim_date       ", end="", flush=True)
    con.execute(f"""
        COPY (
          SELECT CAST(d AS DATE)                              AS business_date,
                 CAST(strftime(d,'%Y%m%d') AS INTEGER)        AS date_key,
                 year(d)                                      AS year,
                 month(d)                                     AS month_no,
                 strftime(d,'%Y-%m')                          AS business_month,
                 monthname(d)                                 AS month_name,
                 dayofweek(d)                                 AS dow_no,     -- 0=Sun
                 dayname(d)                                   AS day_of_week,
                 (dayofweek(d) IN (0,6))                      AS is_weekend,
                 week(d)                                      AS iso_week
          FROM (SELECT unnest(generate_series(DATE '2024-01-01', DATE '2024-12-31',
                                              INTERVAL 1 DAY)) AS d)
          ORDER BY 1)
        TO '{C.P_MART}/dim_date.parquet' (FORMAT parquet, OVERWRITE_OR_IGNORE true);""")
    print("ok")

    # ----------------------------------------------------------------- fact
    print("  fact_sale_line ", end="", flush=True)
    t0 = time.time()
    con.execute(f"""
        COPY (
          SELECT
              l.bill_no,
              l.line_no,
              l.store_id,                                     -- key only, no name/address
              l.business_date,
              l.business_month,
              CAST(strftime(l.business_date,'%Y%m%d') AS INTEGER) AS date_key,
              -- Bill-level discounts carry the literal code 'DISC', not a
              -- product code.  So do the VOID rows that MIRROR a discount on
              -- a cancelled bill -- 117 of them, line_type='VOID',
              -- raw_code='DISC'.  Keying on line_type alone would leave those
              -- with a null product_sk and drop them out of any inner join to
              -- the product dimension, quietly losing revenue.  Key on the
              -- code instead, which is what actually identifies them.
              CASE WHEN l.raw_code = 'DISC' THEN -1 ELSE p.product_sk END
                                                              AS product_sk,
              l.raw_code                                      AS product_code_as_billed,
              l.line_type,
              l.qty,
              l.unit_price,
              CAST(l.qty * l.unit_price AS DECIMAL(18,4))     AS line_amount
          FROM {LAKE} l
          -- the as-of join: code AND date.  Joining on code alone fans out the
          -- 24 reissued codes into two rows each.
          LEFT JOIN pg.products p
                 ON p.product_code = l.raw_code
                AND l.business_date BETWEEN p.valid_from AND p.valid_to
          WHERE l.line_type IN ('SALE','RETURN','DISCOUNT','VOID')
          ORDER BY l.store_id, l.business_date, l.bill_no, l.line_no
        )
        TO '{C.P_MART}/fact_sale_line'
        (FORMAT parquet, COMPRESSION zstd, PARTITION_BY (store_id, business_month),
         OVERWRITE_OR_IGNORE true, FILENAME_PATTERN 'part-{{i}}');""")
    print(f"ok  ({time.time()-t0:.1f}s)")

    # ------------------------------------------------------------ aggregate
    # This is what the dashboard reads.  Store x category x day, which is the
    # finest grain any of the four requested slices needs.  ~60k rows.
    print("  agg_revenue_day ", end="", flush=True)
    con.execute(f"""
        COPY (
          SELECT f.store_id,
                 coalesce(dp.category_id,'C00')   AS category_id,
                 f.business_date,
                 f.business_month,
                 dd.day_of_week,
                 dd.dow_no,
                 dd.is_weekend,
                 sum(f.line_amount)               AS revenue,
                 sum(CASE WHEN f.line_type IN ('SALE','VOID') THEN f.qty ELSE 0 END) AS units,
                 count(*)                         AS lines,
                 count(DISTINCT f.bill_no)        AS bills
          FROM read_parquet('{C.P_MART}/fact_sale_line/**/*.parquet', hive_partitioning=1) f
          LEFT JOIN read_parquet('{C.P_MART}/dim_product.parquet') dp USING (product_sk)
          JOIN      read_parquet('{C.P_MART}/dim_date.parquet')    dd USING (business_date)
          GROUP BY ALL
          ORDER BY 1,2,3
        )
        TO '{C.P_MART}/agg_revenue_day.parquet'
        (FORMAT parquet, COMPRESSION zstd, OVERWRITE_OR_IGNORE true);""")
    print("ok")


def views(con) -> None:
    con.execute(f"CREATE OR REPLACE VIEW dim_store    AS SELECT * FROM read_parquet('{C.P_MART}/dim_store.parquet')")
    con.execute(f"CREATE OR REPLACE VIEW dim_category AS SELECT * FROM read_parquet('{C.P_MART}/dim_category.parquet')")
    con.execute(f"CREATE OR REPLACE VIEW dim_product  AS SELECT * FROM read_parquet('{C.P_MART}/dim_product.parquet')")
    con.execute(f"CREATE OR REPLACE VIEW dim_date     AS SELECT * FROM read_parquet('{C.P_MART}/dim_date.parquet')")
    con.execute(f"CREATE OR REPLACE VIEW fact_sale_line AS SELECT * FROM read_parquet('{C.P_MART}/fact_sale_line/**/*.parquet', hive_partitioning=1)")
    con.execute(f"CREATE OR REPLACE VIEW agg_revenue_day AS SELECT * FROM read_parquet('{C.P_MART}/agg_revenue_day.parquet')")


def main() -> None:
    with C.Tee(C.LOGS / "s04_model.log"):
        C.rule("(c) BUILD THE STAR")
        con = C.connect_duckdb(memory=True)
        C.attach_postgres(con)
        build(con)
        views(con)

        def q(sql):
            return con.execute(sql).fetchdf()

        C.rule("trap 1 -- not every line is a sale", "-")
        print(q(f"""
          SELECT
            round(sum(qty*unit_price),2)                                   AS every_row_in_the_file,
            round(sum(qty*unit_price) FILTER (WHERE line_type<>'TENDER'),2) AS drop_tender_only,
            round(sum(qty*unit_price)
                  FILTER (WHERE line_type NOT IN ('TAX','TENDER')),2)       AS revenue_as_defined
          FROM {LAKE} WHERE business_month='2024-10'
        """).to_string(index=False))
        naive, _, correct = q(f"""
          SELECT sum(qty*unit_price) a,
                 sum(qty*unit_price) FILTER (WHERE line_type<>'TENDER') b,
                 sum(qty*unit_price) FILTER (WHERE line_type NOT IN ('TAX','TENDER')) c
          FROM {LAKE} WHERE business_month='2024-10'""").iloc[0]
        print(f"\n  October, every row summed : {naive:,.2f}")
        print(f"  October, revenue as defined: {correct:,.2f}")
        print(f"  inflation if you skip this : {naive/correct:.2f}x   "
              f"(the 'surprisingly healthy October')")
        print(f"  finance's signed-off October: 56,359,195.92")

        C.rule("trap 2 -- a product code does not identify a product", "-")
        print(q("""
          SELECT p.product_code, p.product_sk, p.product_name, p.category_id,
                 p.valid_from, p.valid_to
          FROM pg.products p
          WHERE p.product_code IN (SELECT product_code FROM pg.products
                                   GROUP BY 1 HAVING count(*)>1)
          ORDER BY p.product_code, p.valid_from LIMIT 6""").to_string(index=False))
        fan = q(f"""
          SELECT
            (SELECT count(*) FROM {LAKE}
              WHERE line_type IN ('SALE','RETURN','VOID'))                      AS item_lines,
            (SELECT count(*) FROM {LAKE} l JOIN pg.products p
                    ON p.product_code=l.raw_code
              WHERE l.line_type IN ('SALE','RETURN','VOID'))                    AS join_on_code_only,
            (SELECT count(*) FROM {LAKE} l JOIN pg.products p
                    ON p.product_code=l.raw_code
                   AND l.business_date BETWEEN p.valid_from AND p.valid_to
              WHERE l.line_type IN ('SALE','RETURN','VOID'))                    AS join_on_code_and_date
        """).iloc[0]
        print(f"\n  item lines in the lake            : {fan.item_lines:,}")
        print(f"  rows after join on code only      : {fan.join_on_code_only:,}"
              f"   (+{fan.join_on_code_only-fan.item_lines:,} phantom rows)")
        print(f"  rows after join on code AND date  : {fan.join_on_code_and_date:,}")

        C.rule("the star, as built", "-")
        print(q(f"""
          SELECT 'dim_store' AS tbl, count(*) AS n_rows FROM dim_store UNION ALL
          SELECT 'dim_category', count(*) FROM dim_category UNION ALL
          SELECT 'dim_product', count(*) FROM dim_product UNION ALL
          SELECT 'dim_date', count(*) FROM dim_date UNION ALL
          SELECT 'fact_sale_line', count(*) FROM fact_sale_line UNION ALL
          SELECT 'agg_revenue_day', count(*) FROM agg_revenue_day""").to_string(index=False))

        C.rule("integrity assertions", "-")
        chk = q(f"""
          SELECT
            (SELECT count(*) FROM {LAKE}
              WHERE line_type IN ('SALE','RETURN','DISCOUNT','VOID'))  AS source_revenue_lines,
            (SELECT count(*) FROM fact_sale_line)                      AS fact_rows,
            (SELECT count(*) FROM fact_sale_line WHERE product_sk IS NULL) AS unresolved_sk,
            (SELECT round(sum(line_amount),2) FROM fact_sale_line)      AS fact_revenue,
            (SELECT round(sum(revenue),2) FROM agg_revenue_day)         AS agg_revenue,
            (SELECT round(sum(qty*unit_price),2) FROM {LAKE}
              WHERE line_type NOT IN ('TAX','TENDER'))                  AS lake_revenue""").iloc[0]
        ok_rows = chk.source_revenue_lines == chk.fact_rows
        ok_rev = abs(float(chk.fact_revenue) - float(chk.lake_revenue)) < 0.01
        ok_agg = abs(float(chk.agg_revenue) - float(chk.fact_revenue)) < 0.01
        print(f"  source revenue-bearing lines : {int(chk.source_revenue_lines):,}")
        print(f"  fact_sale_line rows          : {int(chk.fact_rows):,}      no fan-out: {ok_rows}")
        print(f"  rows with unresolved product : {int(chk.unresolved_sk):,}"
              f"   must be 0: {int(chk.unresolved_sk) == 0}")
        # A null product_sk would silently vanish from any inner join to the
        # product dimension, so this is an assertion and not a report.
        assert int(chk.unresolved_sk) == 0, "unresolved product_sk would lose revenue"
        star_ties = con.execute(f"""
          SELECT round(sum(f.line_amount),2)
          FROM fact_sale_line f JOIN dim_product dp USING (product_sk)""").fetchone()[0]
        print(f"  revenue surviving the product join: {float(star_ties):,.2f}   "
              f"ties: {abs(float(star_ties)-float(chk.lake_revenue)) < 0.01}")
        assert abs(float(star_ties) - float(chk.lake_revenue)) < 0.01
        print(f"  lake revenue                 : {float(chk.lake_revenue):,.2f}")
        print(f"  fact revenue                 : {float(chk.fact_revenue):,.2f}   ties: {ok_rev}")
        print(f"  agg  revenue                 : {float(chk.agg_revenue):,.2f}   ties: {ok_agg}")

        C.rule("why the fact carries store_id and not the store's name", "-")
        # Measured like-for-like: same rows, same sort order, ONE file each,
        # so the only difference is the five repeated store columns.
        for name, extra, join in [
            ("norm",   "",                                                             ""),
            ("denorm", ", s.store_name, s.address_line, s.city, s.state, s.region",
             "JOIN dim_store s USING (store_id)"),
        ]:
            con.execute(f"""
              COPY (SELECT f.* {extra} FROM fact_sale_line f {join}
                    ORDER BY f.store_id, f.business_date, f.bill_no, f.line_no)
              TO 's3://{C.BUCKET}/tmp/fact_{name}.parquet'
              (FORMAT parquet, COMPRESSION zstd, OVERWRITE_OR_IGNORE true);""")

        sizes = con.execute(f"""
          SELECT 'keys only (store_id)' AS shape,
                 sum(total_compressed_size)   AS on_disk,
                 sum(total_uncompressed_size) AS logical
          FROM parquet_metadata('s3://{C.BUCKET}/tmp/fact_norm.parquet')
          UNION ALL
          SELECT 'store name+address on every row',
                 sum(total_compressed_size), sum(total_uncompressed_size)
          FROM parquet_metadata('s3://{C.BUCKET}/tmp/fact_denorm.parquet')
        """).fetchdf()
        print(sizes.to_string(index=False))

        n_disk, n_log = int(sizes.on_disk[0]), int(sizes.logical[0])
        d_disk, d_log = int(sizes.on_disk[1]), int(sizes.logical[1])
        s3 = C.s3_client()
        dims = sum(s3.head_object(Bucket=C.BUCKET, Key=f"mart/{k}.parquet")["ContentLength"]
                   for k in ("dim_store", "dim_category", "dim_product", "dim_date"))
        # Materialised size: what the five store columns weigh once they stop
        # being dictionary indexes -- a CSV extract, a row store, or strings in
        # memory.  This is where the repetition is actually paid for.
        mat = con.execute("""
          SELECT sum(strlen(s.store_name) + strlen(s.address_line)
                   + strlen(s.city) + strlen(s.state)
                   + strlen(s.region) + 5) AS on_every_fact_row
          FROM fact_sale_line f JOIN dim_store s USING (store_id)""").fetchone()[0]
        mat_dim = con.execute("""
          SELECT sum(strlen(store_name) + strlen(address_line)
                   + strlen(city) + strlen(state)
                   + strlen(region) + 5) FROM dim_store""").fetchone()[0]

        print(f"\n  all four dimensions together : {dims:,} bytes on disk")
        print(f"  repeating the store costs    : {d_disk-n_disk:,} bytes on disk "
              f"({d_disk/n_disk:.2f}x)")
        print(f"  the same five columns as text: {int(mat):,} bytes on the fact")
        print(f"                       vs        {int(mat_dim):,} bytes in dim_store "
              f"({mat/mat_dim:,.0f}x)")
        print(f"""
  Read that honestly.  Parquet dictionary-encodes a column with twelve distinct
  values down to almost nothing, so ON DISK the denormalised fact is {d_disk/n_disk:.2f}x --
  the storage argument people usually lead with barely exists here, and it
  would be dishonest to claim a 10x saving.

  The cost shows up in two other places.  Materialised as text -- a CSV
  extract, a row store, strings in memory -- those five columns are
  {int(mat):,} bytes on the fact against {int(mat_dim):,} in dim_store, {mat/mat_dim:,.0f}x.

  And the argument that actually decides it is maintenance: the address exists
  in exactly one row.  When Koramangala moves, one UPDATE in PostgreSQL
  republishes a 12-row dimension.  The alternative is rewriting {int(chk.fact_rows):,}
  fact rows -- which is the same class of mistake as the three different
  October figures: the same fact stored in many places, drifting apart.""")
        for k in ("fact_norm", "fact_denorm"):
            s3.delete_object(Bucket=C.BUCKET, Key=f"tmp/{k}.parquet")

        json.dump(dict(
            october_every_row=float(naive), october_revenue=float(correct),
            october_inflation=float(naive / correct),
            item_lines=int(fan.item_lines), join_code_only=int(fan.join_on_code_only),
            join_code_and_date=int(fan.join_on_code_and_date),
            fact_rows=int(chk.fact_rows), fact_revenue=float(chk.fact_revenue),
            agg_revenue=float(chk.agg_revenue),
            bytes_fact_keys_only=n_disk, bytes_fact_denormalised=d_disk,
            bytes_fact_keys_only_decoded=n_log, bytes_fact_denorm_decoded=d_log,
            bytes_dims=dims,
            assertions_passed=bool(ok_rows and ok_rev and ok_agg),
        ), open(C.RESULTS / "c_model.json", "w"), indent=2)
        print("\nwritten -> results/c_model.json")


if __name__ == "__main__":
    main()
