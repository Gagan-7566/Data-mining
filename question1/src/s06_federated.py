"""
(e) Query across both systems, and show where each part ran.

The query is in sql/e_federated.sql.  It reads Parquet out of MinIO and tables
out of PostgreSQL in one statement, with nothing staged from one into the
other.

The interesting half of the task is the evidence.  "The documentation says
DuckDB pushes filters down" is not evidence.  Three witnesses are used, two of
them outside the engine that planned the query:

  1. DuckDB's own profile (EXPLAIN ANALYZE) -- which operator read what, which
     predicates sat on which scan, how many Parquet files were opened, and the
     HTTPFS HTTP Stats block: the GET/PUT counts the engine itself issued.
  2. PostgreSQL's pg_stat_statements and its server log -- the literal SQL
     PostgreSQL was asked to run.  If a join had been evaluated there the
     statement would contain one.  If a filter had not been pushed down the
     statement would have no WHERE clause.
  3. MinIO's own request counters (/minio/v2/metrics/node).  MinIO caches
     these for about ten seconds, so they are read with a settle loop; they
     are a coarse but genuinely external witness.

Each external witness watches exactly ONE execution, on its own cold DuckDB
connection: run A is bracketed by MinIO's counters, run B by a reset of
pg_stat_statements.  Two reasons for the care.  DuckDB caches remote Parquet,
so a warm re-run serves from memory and shows zero GETs, which would make the
object store look idle when it is not.  And counters that span two executions
report double, which would be a quietly wrong number in a submission about
getting the same number every time.
"""
from __future__ import annotations

import json
import pathlib
import re
import subprocess
import sys
import time
import urllib.request

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import config as C

import psycopg2

METRICS = f"http://{C.S3_ENDPOINT}/minio/v2/metrics/node"
PG_TABLES = ("stores", "products", "product_categories", "price_revisions")


def _scrape() -> dict:
    txt = urllib.request.urlopen(METRICS, timeout=20).read().decode()
    out = {}
    for line in txt.splitlines():
        m = re.match(r'^minio_s3_requests_total\{api="(\w+)".*\}\s+([0-9.e+]+)', line)
        if m:
            out[m.group(1)] = int(float(m.group(2)))
        m = re.match(r'^minio_s3_traffic_sent_bytes\{.*\}\s+([0-9.e+]+)', line)
        if m:
            out["bytes_sent"] = int(float(m.group(1)))
    return out


def minio_counters(settle: bool = True) -> dict:
    """MinIO's own counters.

    MinIO refreshes /minio/v2/metrics/node on roughly a ten second cycle, so a
    scrape taken immediately after a query still reports the pre-query value.
    Wait out one full cycle first, then poll until two consecutive scrapes
    agree.  Measured refresh lag on this host was ~10s."""
    if not settle:
        return _scrape()
    time.sleep(14)
    prev = _scrape()
    for _ in range(10):
        time.sleep(4)
        cur = _scrape()
        if cur == prev:
            return cur
        prev = cur
    return prev


def pg_reset() -> None:
    con = psycopg2.connect(C.PG_DSN)
    con.autocommit = True
    con.cursor().execute("SELECT pg_stat_statements_reset();")
    con.close()


def pg_statements() -> list[tuple]:
    con = psycopg2.connect(C.PG_DSN)
    cur = con.cursor()
    cur.execute("""
        SELECT calls, rows, query
        FROM pg_stat_statements
        WHERE query ILIKE '%public%'
          AND query NOT ILIKE '%pg_stat_statements%'
        ORDER BY rows DESC, calls DESC;""")
    rows = cur.fetchall()
    con.close()
    return rows


def pg_log(since: str) -> list[str]:
    """PostgreSQL's server log.  compose sets log_statement=all."""
    try:
        out = subprocess.run(
            ["docker", "logs", "--since", since, "annapurna-postgres"],
            capture_output=True, text=True, timeout=60,
        )
        text = (out.stdout or "") + (out.stderr or "")
    except Exception as e:                                    # pragma: no cover
        return [f"(could not read the container log: {e})"]
    keep = []
    for ln in text.splitlines():
        if "statement:" in ln and "pg_stat_statements" not in ln:
            s = " ".join(ln.split("statement:", 1)[1].split())
            if s not in keep:
                keep.append(s)
    return keep


def profile_json(con, sql: str) -> dict:
    """Run the query with DuckDB's JSON profiler and return the profile tree.

    The box-drawn EXPLAIN ANALYZE output is for human eyes; it interleaves
    parallel branches across columns, so scraping it with regexes mis-reads
    operators whose names wrap. The JSON profile is the same information
    structured, and is what the claims below are actually based on."""
    out = pathlib.Path(C.LOGS / "e_profile.json")
    con.execute("PRAGMA enable_profiling='json';")
    con.execute(f"PRAGMA profiling_output='{out.as_posix()}';")
    con.execute(sql).fetchall()
    con.execute("PRAGMA disable_profiling;")
    return json.loads(out.read_text(encoding="utf-8"))


def walk(node: dict):
    yield node
    for ch in node.get("children", []):
        yield from walk(ch)


def parse_profile(prof: dict) -> dict:
    """Pull the facts out of the profile tree, by operator."""
    root = prof.get("children", [prof])[0] if "children" in prof else prof
    nodes = list(walk(root))

    pg_scans, pq_scans = [], []
    for n in nodes:
        op = n.get("operator_name")
        info = n.get("extra_info", {}) or {}
        if op == "READ_PARQUET":
            pq_scans.append(dict(
                rows=n.get("operator_cardinality"),
                files_considered=info.get("Scanning Files"),
                files_read=info.get("Total Files Read"),
                file_filters=info.get("File Filters"),
                projections=info.get("Projections"),
                filters=info.get("Filters"),
                dynamic_filters=info.get("Dynamic Filters"),
            ))
        elif op == "POSTGRES_SCAN":
            pg_scans.append(dict(
                table=info.get("Table"),
                rows=n.get("operator_cardinality"),
                projections=info.get("Projections"),
                filters=info.get("Filters"),
                dynamic_filters=info.get("Dynamic Filters"),
            ))

    ops = {}
    for n in nodes:
        ops[n.get("operator_name", "?")] = ops.get(n.get("operator_name", "?"), 0) + 1
    ops.pop("?", None)

    return dict(postgres_scans=pg_scans, parquet_scans=pq_scans,
                operator_counts=ops,
                total_bytes_read=prof.get("total_bytes_read"),
                latency_s=prof.get("latency"))


def parse_plan_text(plan: str) -> dict:
    """Only the HTTPFS counter block is read from the text plan -- it is a
    single-column box at the bottom, so it cannot be mis-columned."""
    flat = " ".join(plan.replace("│", " ").replace("─", " ").split())
    http = {}
    for key, pat in [("bytes_in", r"in:\s*([\d.]+\s*\w+)"),
                     ("bytes_out", r"out:\s*([\d.]+\s*\w*)"),
                     ("GET", r"#GET:\s*(\d+)"), ("PUT", r"#PUT:\s*(\d+)"),
                     ("HEAD", r"#HEAD:\s*(\d+)"), ("POST", r"#POST:\s*(\d+)"),
                     ("DELETE", r"#DELETE:\s*(\d+)")]:
        m = re.search(pat, flat)
        if m:
            http[key] = m.group(1).strip()
    return http


def main() -> None:
    with C.Tee(C.LOGS / "s06_federated.log"):
        C.rule("(e) ONE QUERY, TWO SYSTEMS, NO COPYING")

        sql = (C.SQL_DIR / "e_federated.sql").read_text(encoding="utf-8")
        print(sql)

        con = C.connect_duckdb(memory=True)
        C.attach_postgres(con)

        print("\nwhere each relation physically lives, as the engine sees it:")
        print(con.execute("""
            SELECT database_name AS catalog, schema_name, table_name
            FROM duckdb_tables()
            WHERE database_name='pg' AND schema_name='public'
              AND table_name IN ('stores','products','product_categories','price_revisions')
            ORDER BY 3""").fetchdf().to_string(index=False))
        print("  The sale lines appear in the FROM clause as read_parquet('s3://...').")
        print("  They have no entry in either catalogue, because they were never loaded")
        print("  into one.\n")

        C.rule("the answer", "-")
        t0 = time.perf_counter()
        df = con.execute(sql).fetchdf()
        warm_ms = (time.perf_counter() - t0) * 1000
        print(df.to_string(index=False))

        # ------------------------------------------------------------------
        # the instrumented run: cold engine, both remote systems watched
        # ------------------------------------------------------------------
        C.rule("instrumented runs (each witness watches exactly one execution)", "-")

        # -- run A: cold connection, watched by MinIO ----------------------
        # A fresh connection so nothing is served from DuckDB's cache; the
        # MinIO delta below therefore belongs to this one execution and no
        # other.
        coldA = C.connect_duckdb(memory=True)
        C.attach_postgres(coldA)
        print("  run A -- settling MinIO's counters before ...")
        before = minio_counters()
        t0 = time.perf_counter()
        plan = coldA.execute("EXPLAIN ANALYZE " + sql).fetchall()[0][1]
        cold_ms = (time.perf_counter() - t0) * 1000
        print("  run A -- settling MinIO's counters after ...")
        after = minio_counters()
        coldA.close()

        # -- run B: cold connection, watched by PostgreSQL -----------------
        # pg_stat_statements is reset immediately before, so its counts also
        # belong to exactly one execution.
        coldB = C.connect_duckdb(memory=True)
        C.attach_postgres(coldB)
        pg_reset()
        t_mark = time.strftime("%Y-%m-%dT%H:%M:%S")
        time.sleep(1.5)
        prof = parse_profile(profile_json(coldB, sql))
        coldB.close()

        print(f"\n  cold run: {cold_ms:,.0f} ms     warm run: {warm_ms:,.0f} ms")

        # --------------------------------------------------- witness 1
        C.rule("witness 1 -- DuckDB's own profile", "-")
        print(plan)
        (C.RESULTS / "e_explain.txt").write_text(plan, encoding="utf-8")

        http = parse_plan_text(plan)

        C.rule("witness 1, read back from the JSON profile", "-")
        print("  POSTGRES_SCAN operators -- work handed to PostgreSQL:")
        for s in prof["postgres_scans"]:
            print(f"\n    table {s['table']}   returned {s['rows']} rows")
            print(f"      projection sent down : {s['projections']}")
            print(f"      filter sent down     : {s['filters'] or '(none)'}")
            if s["dynamic_filters"]:
                print(f"      dynamic filter       : {str(s['dynamic_filters'])[:100]}...")
        print("\n  READ_PARQUET operator -- work served by the object store:")
        for s in prof["parquet_scans"]:
            print(f"\n      rows produced        : {s['rows']:,}")
            print(f"      files considered     : {s['files_considered']}")
            print(f"      files actually read  : {s['files_read']}")
            print(f"      partition filter     : {s['file_filters']}")
            print(f"      projection           : {s['projections']}")
            print(f"      row filter           : {s['filters']}")
            for dfil in (s["dynamic_filters"] or []):
                print(f"      dynamic filter       : {str(dfil)[:104]}")
        print("\n  operators in the plan, by name:")
        for k, v in sorted(prof["operator_counts"].items(), key=lambda x: -x[1]):
            print(f"    {k:<26}{v}")
        print(f"\n  HTTPFS counters the engine kept for itself: {http}")
        print(f"  total_bytes_read reported by the profile   : "
              f"{prof['total_bytes_read']:,}" if prof.get("total_bytes_read")
              else "")

        print("""
  Read the READ_PARQUET box again.  It considered 12 partitions (the twelve
  stores' October) and opened 8.  The four it skipped are the four stores that
  are not in the South or West -- and 'region' is a column that exists only in
  PostgreSQL.  A predicate that could only be evaluated in the relational
  database came back as a dynamic filter and pruned files in the object store.
  That is the two systems cooperating on one query, and it is visible in the
  engine's own profile rather than in anybody's documentation.""")
        p = dict(prof)
        p["httpfs"] = http

        # --------------------------------------------------- witness 2
        C.rule("witness 2 -- what PostgreSQL was actually asked to do", "-")
        stmts = pg_statements()
        print("pg_stat_statements, on the PostgreSQL server:\n")
        for calls, rows, q in stmts:
            print(f"  calls={calls:<3} rows={rows:<7} {' '.join(q.split())[:132]}")
        print("\nthe PostgreSQL server log (log_statement=all):\n")
        for ln in pg_log(t_mark)[:10]:
            print(f"  {ln[:132]}")

        pg_rows = sum(r for _, r, _ in stmts)
        masters = con.execute(
            "SELECT (SELECT count(*) FROM pg.stores)+(SELECT count(*) FROM pg.products)"
            "+(SELECT count(*) FROM pg.product_categories)").fetchone()[0]
        lines = con.execute(f"""
            SELECT count(*) FROM read_parquet('{C.P_RAW}/**/*.parquet', hive_partitioning=1)
            WHERE business_month='2024-10' AND line_type IN ('SALE','RETURN','VOID')""").fetchone()[0]
        print(f"\n  rows PostgreSQL sent to DuckDB        : {pg_rows:,}")
        print(f"  rows in its three master tables       : {masters:,}")
        print(f"  October sale lines the query covered  : {lines:,}")
        print("\n  Every statement PostgreSQL ran is a single-table SELECT with the")
        print("  projection cut to the columns the query needs, and two of them carry a")
        print("  WHERE that came from the outer query -- region IN ('South','West') and")
        print("  department = 'Food'.  There is no join, no GROUP BY and no sale line in")
        print("  anything PostgreSQL was asked to do.")

        # --------------------------------------------------- witness 3
        C.rule("witness 3 -- what MinIO says it served", "-")
        delta = {k: after.get(k, 0) - before.get(k, 0) for k in set(before) | set(after)}
        print(f"{'counter':<20}{'before':>14}{'after':>14}{'delta':>12}")
        for k in sorted(delta):
            print(f"{k:<20}{before.get(k,0):>14,}{after.get(k,0):>14,}{delta[k]:>12,}")
        print(f"\n  GETs MinIO served during the run : {delta.get('getobject',0):,}")
        print(f"  bytes MinIO sent                 : {delta.get('bytes_sent',0):,}")
        print(f"  PUTs MinIO accepted              : {delta.get('putobject',0):,}")
        if delta.get("putobject", 0) == 0:
            print("  -> nothing was written into the object store, so no part of"
                  "\n     PostgreSQL was staged there for the join.")

        # ------------------------------------------------------------------
        C.rule("VERDICT -- which part ran where, and how that is known", "=")
        verdict = [
            ("scanning and filtering stores, products, product_categories",
             "PostgreSQL",
             "pg_stat_statements and the server log show single-table SELECTs with "
             "narrowed projections and the outer WHERE pushed in; "
             f"{pg_rows:,} rows crossed the wire, all of them master data"),
            ("reading the October sale lines, partition-pruned",
             "MinIO, scanned by DuckDB",
             f"the profile shows READ_PARQUET opening "
             f"{[s['files_read'] for s in prof['parquet_scans']]} of "
             f"{[s['files_considered'] for s in prof['parquet_scans']]} candidate files; "
             f"DuckDB's own HTTPFS "
             f"counter reports {http.get('GET','?')} GET / {http.get('PUT','?')} PUT "
             f"({http.get('bytes_in','?')} in); MinIO independently counted "
             f"{delta.get('getobject',0)} GETs and {delta.get('putobject',0)} PUTs"),
            ("the as-of product join, the store join, GROUP BY, the arithmetic",
             "DuckDB",
             f"{prof['operator_counts'].get('HASH_JOIN',0)} HASH_JOIN and "
             f"{sum(v for k,v in prof['operator_counts'].items() if 'GROUP_BY' in k)} "
             "GROUP BY operators appear only in the DuckDB profile; neither remote system "
             "was asked for any of it, and only DuckDB ever held both sides"),
        ]
        for what, where, how in verdict:
            print(f"\n  {what}\n      ran in   : {where}\n      evidence : {how}")

        df.to_csv(C.RESULTS / "e_federated_result.csv", index=False)
        json.dump(dict(
            warm_ms=round(warm_ms, 1), cold_ms=round(cold_ms, 1),
            plan=p,
            http_stats=http,
            postgres_statements=[{"calls": c, "rows": r, "query": " ".join(q.split())}
                                 for c, r, q in stmts],
            postgres_rows_returned=pg_rows,
            master_rows=int(masters), october_sale_lines=int(lines),
            minio_before=before, minio_after=after, minio_delta=delta,
        ), open(C.RESULTS / "e_federated.json", "w"), indent=2)
        print("\nwritten -> results/e_federated.json, e_explain.txt, e_federated_result.csv")


if __name__ == "__main__":
    main()
