"""
(a) Land the daily files in the object store, partitioned.
(b) Make the load safe to run twice.

WHAT THE SOURCE ACTUALLY LOOKS LIKE  (billing_notes.md, verified against the files)
  * three CSV dialects -- comma/ISO, semicolon/dd-mm-yyyy, comma+BOM/epoch-seconds
    with the columns in a different order
  * the BUSINESS DATE is the date in the FILE NAME, never the timestamp inside:
    bills punched after midnight carry the next calendar date
  * re-sends land beside the original as __R1 / __R2.  Most are byte-identical
    replacements; some are PARTIAL (only the bills committed at that moment),
    so "newest file wins" would silently drop bills.  The safe unit is the
    LINE, keyed by (bill_no, line_no).

LAYOUT CHOSEN
    s3://annapurna-lake/raw/sales/store_id=<S>/business_month=<YYYY-MM>/part-0.parquet

  Hive-style, store first then month.  A dashboard question is always "this
  store, this month" or "all stores, this month", and both prune on a
  directory name without opening a byte of data.  business_date stays as a
  COLUMN so day-of-week still works; Parquet row-group min/max on that column
  prunes inside the file.  Grain is one file per store-month (144 files) --
  partitioning by day as well would give 4,392 tiny files and cost more in
  request overhead than it saves in scanning.

IDEMPOTENCE
  Each partition is rebuilt from its full set of source files and written with
  PUT-overwrite semantics.  There is no append anywhere.  Row order is fixed
  by an ORDER BY, so the bytes are reproducible, not merely the row set.
  Run it once or ten times: same rows, same checksum.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import time
import pathlib
from collections import defaultdict

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import config as C

FILE_RE = re.compile(
    r"^SALES_(?P<store>S\d{2})_(?P<date>\d{8})(?:__R(?P<resend>\d+))?\.(?P<ext>csv|parquet)$",
    re.IGNORECASE,
)


def dialect_of(store: str) -> str:
    """store_id -> dialect.  Straight out of billing_notes.md, File dialects."""
    n = int(store[1:])
    if n <= 5:
        return "A"
    if n <= 9:
        return "B"
    return "C"


# --------------------------------------------------------------------------
# discovery
# --------------------------------------------------------------------------
def discover(sales_dir: pathlib.Path) -> list[dict]:
    """One record per physical export file, with its contract fields parsed
    out of the name -- the name is the contract (billing_notes.md)."""
    out = []
    for p in sorted(sales_dir.iterdir()):
        m = FILE_RE.match(p.name)
        if not m:
            continue
        d = m.group("date")
        out.append(
            dict(
                path=p,
                name=p.name,
                store_id=m.group("store").upper(),
                business_date=f"{d[:4]}-{d[4:6]}-{d[6:]}",
                business_month=f"{d[:4]}-{d[4:6]}",
                # 0 = original, 1 = __R1, 2 = __R2 ... higher wins on a tie
                resend_seq=int(m.group("resend") or 0),
                ext=m.group("ext").lower(),
                size=p.stat().st_size,
            )
        )
    return out


# --------------------------------------------------------------------------
# dialect-aware reader -> one canonical projection
# --------------------------------------------------------------------------
def reader_sql(rec: dict) -> str:
    """SQL returning the canonical seven columns from one export file,
    whatever dialect it happens to be written in."""
    f = rec["path"].as_posix()
    dia = dialect_of(rec["store_id"])

    if rec["ext"] == "parquet":
        # the parquet experiment kept the dialect-A column names
        return f"""
            SELECT CAST(bill_no AS VARCHAR)           AS bill_no,
                   CAST(line_no AS INTEGER)           AS line_no,
                   CAST(product_code AS VARCHAR)      AS raw_code,
                   CAST(qty AS DOUBLE)                AS qty,
                   CAST(unit_price AS DOUBLE)         AS unit_price,
                   upper(CAST(line_type AS VARCHAR))  AS line_type,
                   CAST(ts AS TIMESTAMP)              AS txn_ts
            FROM read_parquet('{f}')"""

    if dia == "A":
        src = (
            f"read_csv('{f}', delim=',', header=true, "
            "columns={'bill_no':'VARCHAR','line_no':'INTEGER','product_code':'VARCHAR',"
            "'qty':'DOUBLE','unit_price':'DOUBLE','line_type':'VARCHAR','ts':'VARCHAR'})"
        )
        return f"""
            SELECT bill_no, line_no, product_code AS raw_code, qty, unit_price,
                   upper(line_type) AS line_type,
                   strptime(ts, '%Y-%m-%dT%H:%M:%S') AS txn_ts
            FROM {src}"""

    if dia == "B":
        # SEMICOLON separated, dd-mm-yyyy.  The format is given explicitly so
        # that 05-03-2024 can never be read as the 3rd of May.
        src = (
            f"read_csv('{f}', delim=';', header=true, "
            "columns={'bill_no':'VARCHAR','line_no':'INTEGER','item_code':'VARCHAR',"
            "'quantity':'DOUBLE','rate':'DOUBLE','type':'VARCHAR','txn_time':'VARCHAR'})"
        )
        return f"""
            SELECT bill_no, line_no, item_code AS raw_code, quantity AS qty,
                   rate AS unit_price, upper(type) AS line_type,
                   strptime(txn_time, '%d-%m-%Y %H:%M:%S') AS txn_ts
            FROM {src}"""

    # dialect C: UTF-8 BOM, epoch seconds UTC, columns in a different order
    src = (
        f"read_csv('{f}', delim=',', header=true, "
        "columns={'ts':'BIGINT','bill_no':'VARCHAR','line_no':'INTEGER',"
        "'line_type':'VARCHAR','product_code':'VARCHAR','unit_price':'DOUBLE','qty':'DOUBLE'})"
    )
    return f"""
        SELECT bill_no, line_no, product_code AS raw_code, qty, unit_price,
               upper(line_type) AS line_type,
               CAST(to_timestamp(ts) AS TIMESTAMP) AS txn_ts
        FROM {src}"""


# --------------------------------------------------------------------------
# the load
# --------------------------------------------------------------------------
def build_partition_sql(recs: list[dict]) -> str:
    """UNION every file of one store-month, then de-duplicate at LINE level.

    Why line level and not file level: a re-send may be a partial roll.  Taking
    the newest file alone would lose the bills that were not yet committed when
    the re-send was triggered.  The UNION plus one row per (bill_no, line_no)
    is correct for the byte-identical replacements and the partial ones alike.
    Ties break on resend_seq then file name, so the choice is deterministic --
    which is exactly what makes run 2 equal run 1.
    """
    parts = []
    for r in recs:
        parts.append(
            f"""
            SELECT *, '{r['name']}' AS source_file, {r['resend_seq']} AS resend_seq
            FROM ({reader_sql(r)})"""
        )
    union = "\nUNION ALL\n".join(parts)

    store = recs[0]["store_id"]
    month = recs[0]["business_month"]
    return f"""
    WITH raw AS (
        {union}
    ),
    -- The business date is the date in the FILE NAME.  bill_no carries the
    -- same contract ('S01/20241014/00001'), so it is re-derived from there and
    -- the timestamp inside the file is deliberately NOT used for dating.
    typed AS (
        SELECT
            bill_no,
            line_no,
            '{store}'                                                   AS store_id,
            CAST(strptime(split_part(bill_no,'/',2),'%Y%m%d') AS DATE)   AS business_date,
            '{month}'                                                   AS business_month,
            nullif(raw_code,'')                                         AS raw_code,
            qty,
            unit_price,
            line_type,
            txn_ts,
            source_file,
            resend_seq
        FROM raw
    ),
    ranked AS (
        SELECT *, row_number() OVER (
                    PARTITION BY bill_no, line_no
                    ORDER BY resend_seq DESC, source_file DESC
                  ) AS rn
        FROM typed
    )
    SELECT bill_no, line_no, store_id, business_date, business_month,
           raw_code, qty, unit_price, line_type, txn_ts, source_file
    FROM ranked
    WHERE rn = 1
    ORDER BY bill_no, line_no      -- deterministic bytes, not just a deterministic row set
    """


def partition_checksum(con, store: str, month: str) -> tuple[int, str]:
    """Logical checksum of one partition: md5 over every row, concatenated in
    key order.  Independent of file layout, compression and write time."""
    q = f"""
        SELECT count(*),
               md5(string_agg(h, '' ORDER BY bill_no, line_no))
        FROM (
          SELECT bill_no, line_no,
                 md5(concat_ws('|', bill_no, CAST(line_no AS VARCHAR), store_id,
                               CAST(business_date AS VARCHAR), coalesce(raw_code,''),
                               printf('%.4f', qty), printf('%.4f', unit_price),
                               line_type, CAST(txn_ts AS VARCHAR))) AS h
          FROM read_parquet('{C.P_RAW}/store_id={store}/business_month={month}/*.parquet')
        )"""
    n, h = con.execute(q).fetchone()
    return n, (h or "empty")


def run_once(run_no: int, verbose: bool = True) -> dict:
    con = C.connect_duckdb(memory=True)
    files = discover(C.SALES_DIR)

    groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for r in files:
        groups[(r["store_id"], r["business_month"])].append(r)

    t0 = time.time()
    written = 0
    for i, (key, recs) in enumerate(sorted(groups.items()), 1):
        store, month = key
        target = f"{C.P_RAW}/store_id={store}/business_month={month}/part-0.parquet"
        con.execute(
            f"""
            COPY ({build_partition_sql(recs)})
            TO '{target}'
            (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 100000, OVERWRITE_OR_IGNORE true);
        """
        )
        written += 1
        if verbose and i % 24 == 0:
            print(f"      ... {i}/{len(groups)} partitions   ({time.time()-t0:5.1f}s)")

    elapsed = time.time() - t0

    # ---- logical checksum, Merkle style: a digest per partition, then a
    #      digest of the sorted partition digests.  Cheap and reproducible.
    per_part = []
    total_rows = 0
    for (store, month) in sorted(groups):
        n, h = partition_checksum(con, store, month)
        total_rows += n
        per_part.append(f"{store}/{month}:{n}:{h}")
    logical = hashlib.sha256("\n".join(per_part).encode()).hexdigest()

    # ---- physical checksum: sha256 over the actual objects sitting in MinIO
    s3 = C.s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    objs = []
    for page in paginator.paginate(Bucket=C.BUCKET, Prefix="raw/sales/"):
        objs.extend(page.get("Contents", []))
    objs.sort(key=lambda o: o["Key"])
    hsh = hashlib.sha256()
    nbytes = 0
    for o in objs:
        body = s3.get_object(Bucket=C.BUCKET, Key=o["Key"])["Body"].read()
        hsh.update(o["Key"].encode())
        hsh.update(body)
        nbytes += len(body)
    physical = hsh.hexdigest()

    con.close()
    return dict(
        run=run_no,
        source_files=len(files),
        partitions=written,
        objects=len(objs),
        rows=total_rows,
        bytes=nbytes,
        logical_checksum=logical,
        physical_checksum=physical,
        seconds=round(elapsed, 1),
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=1)
    args = ap.parse_args()

    with C.Tee(C.LOGS / f"s02_ingest_runs{args.runs}.log"):
        C.rule("(a)+(b) INGEST -- 3 dialects -> partitioned Parquet in MinIO")

        files = discover(C.SALES_DIR)
        resends = [f for f in files if f["resend_seq"] > 0]
        print(f"\nsource files discovered          : {len(files):,}")
        print(f"  of which re-sends (__Rn)       : {len(resends):,}")
        print(f"  distinct store-days            : "
              f"{len({(f['store_id'], f['business_date']) for f in files}):,}")
        print(f"  store-month partitions to write: "
              f"{len({(f['store_id'], f['business_month']) for f in files}):,}")
        print(f"  raw bytes on the shared folder : {sum(f['size'] for f in files):,}")

        results = []
        for n in range(1, args.runs + 1):
            print(f"\n--- RUN {n} of {args.runs} " + "-" * 50)
            r = run_once(n)
            results.append(r)
            print(f"      rows              : {r['rows']:,}")
            print(f"      objects written   : {r['objects']}  ({r['bytes']:,} bytes)")
            print(f"      logical checksum  : {r['logical_checksum']}")
            print(f"      physical checksum : {r['physical_checksum']}")
            print(f"      elapsed           : {r['seconds']}s")

        if args.runs > 1:
            C.rule("IDEMPOTENCE VERDICT", "=")
            print(f"{'run':<5}{'rows':>12}  {'logical checksum (sha256)':<66}{'objects sha256'}")
            for r in results:
                print(f"{r['run']:<5}{r['rows']:>12,}  {r['logical_checksum']}  "
                      f"{r['physical_checksum'][:16]}...")
            same_rows = len({r["rows"] for r in results}) == 1
            same_log = len({r["logical_checksum"] for r in results}) == 1
            same_phy = len({r["physical_checksum"] for r in results}) == 1
            print()
            print(f"  identical row count      : {same_rows}")
            print(f"  identical logical digest : {same_log}")
            print(f"  identical object bytes   : {same_phy}")
            print("\n  VERDICT:", "IDEMPOTENT" if (same_rows and same_log) else "NOT IDEMPOTENT")

        (C.RESULTS / "b_idempotency_runs.json").write_text(json.dumps(results, indent=2))
        print(f"\nwritten -> {C.RESULTS / 'b_idempotency_runs.json'}")


if __name__ == "__main__":
    main()
