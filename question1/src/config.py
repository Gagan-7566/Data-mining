"""
Central configuration + helpers for the Annapurna analytics platform.

Three systems:
  * MinIO      -- S3-compatible object store, holds every sale line as Parquet
  * PostgreSQL -- operational masters (stores, products, categories, prices)
  * DuckDB     -- the analytical engine.  Embedded, runs on the host, reads
                  MinIO over `httpfs` and PostgreSQL over `postgres_scanner`.
"""
from __future__ import annotations

import os
import pathlib
import sys

# --------------------------------------------------------------------------
# paths
# --------------------------------------------------------------------------
PROJECT   = pathlib.Path(__file__).resolve().parents[1]
DATA_DIR  = pathlib.Path(os.environ.get("ANNAPURNA_DATA", PROJECT.parent / "data"))
SALES_DIR = DATA_DIR / "sales"
MASTERS   = DATA_DIR / "masters.sql"
FINANCE   = DATA_DIR / "finance_monthly.csv"

# DuckDB's EXPLAIN ANALYZE draws box characters; the Windows console defaults
# to cp1252 and would raise on them.  Force UTF-8 once, here.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

RESULTS     = PROJECT / "results"
SCREENSHOTS = PROJECT / "screenshots"
LOGS        = PROJECT / "logs"
SQL_DIR     = PROJECT / "sql"
for _d in (RESULTS, SCREENSHOTS, LOGS):
    _d.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------------------------------
# object store (MinIO)
# --------------------------------------------------------------------------
S3_ENDPOINT   = os.environ.get("ANNAPURNA_S3_ENDPOINT", "localhost:9000")
S3_KEY        = os.environ.get("ANNAPURNA_S3_KEY", "annapurna")
S3_SECRET     = os.environ.get("ANNAPURNA_S3_SECRET", "annapurna_secret")
BUCKET        = "annapurna-lake"

# logical prefixes inside the bucket
P_RAW     = f"s3://{BUCKET}/raw/sales"            # normalised sale lines, partitioned
P_FLAT    = f"s3://{BUCKET}/flat/sales"           # same rows, ONE folder (part (a) control)
P_MART    = f"s3://{BUCKET}/mart"                 # star-schema fact + aggregate

# --------------------------------------------------------------------------
# relational database (PostgreSQL)
# --------------------------------------------------------------------------
PG_HOST = os.environ.get("ANNAPURNA_PG_HOST", "localhost")
PG_PORT = int(os.environ.get("ANNAPURNA_PG_PORT", "5433"))
PG_DB   = "annapurna"
PG_USER = "annapurna"
PG_PW   = "annapurna_secret"
PG_DSN  = f"host={PG_HOST} port={PG_PORT} dbname={PG_DB} user={PG_USER} password={PG_PW}"
PG_URI  = f"postgresql://{PG_USER}:{PG_PW}@{PG_HOST}:{PG_PORT}/{PG_DB}"

# --------------------------------------------------------------------------
# analytical engine (DuckDB)
# --------------------------------------------------------------------------
DUCKDB_FILE = PROJECT / "warehouse.duckdb"

# Revenue definition, stated once and reused everywhere.
# See billing_notes.md: TENDER is the bill total written as another row, and
# TAX is GST.  Counting either of them is the "October is twice as healthy as
# it should be" bug.
REVENUE_LINE_TYPES = ("SALE", "RETURN", "DISCOUNT", "VOID")
NON_REVENUE_LINE_TYPES = ("TAX", "TENDER")


def connect_duckdb(read_only: bool = False, memory: bool = False):
    """Open DuckDB with httpfs (-> MinIO) and postgres_scanner (-> PostgreSQL)."""
    import duckdb

    con = duckdb.connect(":memory:" if memory else str(DUCKDB_FILE), read_only=read_only)
    con.execute("INSTALL httpfs;   LOAD httpfs;")
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"""
        CREATE OR REPLACE PERSISTENT SECRET minio (
            TYPE       s3,
            KEY_ID     '{S3_KEY}',
            SECRET     '{S3_SECRET}',
            ENDPOINT   '{S3_ENDPOINT}',
            URL_STYLE  'path',
            USE_SSL    false
        );
    """)
    con.execute("SET preserve_insertion_order = false;")
    return con


def attach_postgres(con, alias: str = "pg", read_only: bool = True) -> None:
    """Attach the live PostgreSQL database to DuckDB.  No data is copied."""
    existing = {r[0] for r in con.execute("SELECT database_name FROM duckdb_databases()").fetchall()}
    if alias in existing:
        return
    ro = ", READ_ONLY" if read_only else ""
    con.execute(f"ATTACH '{PG_DSN}' AS {alias} (TYPE postgres{ro});")


def s3_client():
    import boto3
    return boto3.client(
        "s3",
        endpoint_url=f"http://{S3_ENDPOINT}",
        aws_access_key_id=S3_KEY,
        aws_secret_access_key=S3_SECRET,
        region_name="us-east-1",
    )


class Tee:
    """Write stdout to the console *and* to a log file, so every figure that
    appears in the report is backed by a captured run."""

    def __init__(self, path: pathlib.Path):
        self.path = pathlib.Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.fh = open(self.path, "w", encoding="utf-8")
        self._stdout = sys.stdout
        sys.stdout = self

    def write(self, s):
        self._stdout.write(s)
        self.fh.write(s)

    def flush(self):
        self._stdout.flush()
        self.fh.flush()

    def close(self):
        sys.stdout = self._stdout
        self.fh.close()

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()


def rule(title: str, ch: str = "=", width: int = 78) -> None:
    print(ch * width)
    print(title)
    print(ch * width)
