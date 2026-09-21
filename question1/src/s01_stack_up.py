"""
(a) Stand the platform up.

Verifies the three systems are reachable, creates the bucket, loads
masters.sql into PostgreSQL, and prints what each system reports about
itself.  Nothing here is taken on trust from the compose file -- every
line printed is the system answering for itself.
"""
from __future__ import annotations

import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent))

import psycopg2
import config as C


def main() -> None:
    with C.Tee(C.LOGS / "s01_stack_up.log"):
        C.rule("(a) PLATFORM -- three systems, each asked to identify itself")

        # ---------------- object store ----------------
        print("\n[1/3] OBJECT STORE  MinIO  @ http://%s" % C.S3_ENDPOINT)
        s3 = C.s3_client()
        buckets = {b["Name"] for b in s3.list_buckets()["Buckets"]}
        if C.BUCKET not in buckets:
            s3.create_bucket(Bucket=C.BUCKET)
            print(f"      created bucket s3://{C.BUCKET}")
        else:
            print(f"      bucket s3://{C.BUCKET} already present")
        print("      buckets now:", sorted({b['Name'] for b in s3.list_buckets()['Buckets']}))

        # ---------------- relational database ----------------
        print("\n[2/3] RELATIONAL DB  PostgreSQL @ %s:%s/%s" % (C.PG_HOST, C.PG_PORT, C.PG_DB))
        con = psycopg2.connect(C.PG_DSN)
        con.autocommit = True
        cur = con.cursor()
        cur.execute("SELECT version();")
        print("     ", cur.fetchone()[0].split(" on ")[0])

        sql_text = C.MASTERS.read_text(encoding="utf-8")
        print(f"      loading {C.MASTERS.name} ({len(sql_text):,} bytes) ...")
        cur.execute(sql_text)
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements;")

        for t in ("stores", "product_categories", "products", "price_revisions"):
            cur.execute(f"SELECT count(*) FROM {t};")
            print(f"        {t:<20} {cur.fetchone()[0]:>8,} rows")

        # the two facts from billing_notes.md that drive part (c)
        cur.execute("""
            SELECT count(*) FROM (
              SELECT product_code FROM products GROUP BY product_code HAVING count(*) > 1
            ) x;""")
        print(f"\n      product codes reissued to a different product : {cur.fetchone()[0]}")
        cur.execute("SELECT count(*) FROM products WHERE NOT is_current;")
        print(f"      product rows no longer current                : {cur.fetchone()[0]}")

        # ---------------- analytical engine ----------------
        print("\n[3/3] ANALYTICAL ENGINE  DuckDB (embedded, on the host)")
        d = C.connect_duckdb()
        print("      version:", d.execute("SELECT version();").fetchone()[0])
        exts = d.execute("""
            SELECT extension_name, installed, loaded
            FROM duckdb_extensions()
            WHERE extension_name IN ('httpfs','postgres_scanner','parquet')
            ORDER BY 1;""").fetchall()
        for name, inst, load in exts:
            print(f"        {name:<18} installed={inst!s:<5} loaded={load}")

        C.attach_postgres(d)
        print("      ATTACHed PostgreSQL as 'pg' -- DuckDB now sees:")
        rows = d.execute("""
            SELECT table_name FROM duckdb_tables()
            WHERE database_name='pg' AND schema_name='public' ORDER BY 1;""").fetchall()
        print("       ", [r[0] for r in rows])
        print("      cross-check through DuckDB -> PostgreSQL:",
              d.execute("SELECT count(*) FROM pg.stores;").fetchone()[0], "stores")
        d.close()

        print("\nPLATFORM UP.  object store + relational db + analytical engine all live.")


if __name__ == "__main__":
    main()
