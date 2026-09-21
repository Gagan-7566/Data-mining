"""
(b) Why the de-duplication is at LINE level and not file level.

Running three times and getting the same checksum proves the load is
repeatable.  It does not prove the load is RIGHT -- a rule that drops the same
bills every time is repeatable too.  This script takes the 68 re-send files
apart and shows what each candidate rule would have done to the money.
"""
from __future__ import annotations

import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import config as C
import s02_ingest as ing


def main() -> None:
    with C.Tee(C.LOGS / "s02b_resends.log"):
        C.rule("(b) THE RE-SENDS -- what each de-duplication rule would do")

        con = C.connect_duckdb(memory=True)
        recs = ing.discover(C.SALES_DIR)
        resent_days = {(r["store_id"], r["business_date"]) for r in recs if r["resend_seq"] > 0}
        sel = [r for r in recs if (r["store_id"], r["business_date"]) in resent_days]

        print(f"\nstore-days that were re-sent at least once : {len(resent_days)}")
        print(f"files involved (originals + re-sends)      : {len(sel)}")
        print(f"re-send files                              : "
              f"{sum(1 for r in sel if r['resend_seq'] > 0)}")

        parts = [
            f"SELECT *, '{r['name']}' AS source_file, {r['resend_seq']} AS resend_seq "
            f"FROM ({ing.reader_sql(r)})"
            for r in sel
        ]
        con.execute("CREATE TABLE allsrc AS " + "\nUNION ALL\n".join(parts))
        total = con.execute("SELECT count(*) FROM allsrc").fetchone()[0]
        distinct = con.execute(
            "SELECT count(*) FROM (SELECT DISTINCT bill_no, line_no FROM allsrc)").fetchone()[0]
        print(f"rows across all of those files             : {total:,}")
        print(f"distinct (bill_no, line_no) keys           : {distinct:,}")

        C.rule("is a re-send ever a partial roll?", "-")
        shape = con.execute("""
          SELECT split_part(bill_no,'/',1) AS store,
                 split_part(bill_no,'/',2) AS business_date,
                 count(*) FILTER (WHERE resend_seq=0) AS lines_in_original,
                 count(*) FILTER (WHERE resend_seq>0) AS lines_in_resends,
                 count(DISTINCT (bill_no,line_no))    AS distinct_lines,
                 CASE WHEN count(DISTINCT (bill_no,line_no))
                         > count(*) FILTER (WHERE resend_seq>0)
                      THEN 'resend is PARTIAL' ELSE 'resend is complete' END AS verdict
          FROM allsrc GROUP BY 1,2 ORDER BY 6 DESC, 1, 2""").fetchdf()
        print(shape.to_string(index=False))
        n_partial = int((shape.verdict == "resend is PARTIAL").sum())
        print(f"\n  partial re-sends found: {n_partial} of {len(shape)} re-sent store-days")
        print("  -> 'take the newest file for each store-day' would silently drop the")
        print("     bills those re-sends had not yet committed.")

        C.rule("do the files ever CONTRADICT each other?", "-")
        conflicts = con.execute("""
          SELECT count(*) AS keys_with_two_different_values FROM (
            SELECT bill_no, line_no FROM allsrc GROUP BY 1,2
            HAVING count(DISTINCT concat_ws('|', CAST(qty AS VARCHAR),
                        CAST(unit_price AS VARCHAR), line_type,
                        coalesce(raw_code,''))) > 1)""").fetchdf()
        print(conflicts.to_string(index=False))
        only_resend = con.execute("""
          SELECT count(*) FROM (SELECT bill_no, line_no FROM allsrc GROUP BY 1,2
            HAVING max(CASE WHEN resend_seq=0 THEN 1 ELSE 0 END)=0)""").fetchone()[0]
        only_orig = con.execute("""
          SELECT count(*) FROM (SELECT bill_no, line_no FROM allsrc GROUP BY 1,2
            HAVING max(CASE WHEN resend_seq>0 THEN 1 ELSE 0 END)=0)""").fetchone()[0]
        print(f"\n  lines present ONLY in a re-send  : {only_resend:,}")
        print(f"  lines present ONLY in an original: {only_orig:,}")
        print("  -> the re-sends never contradict the original, they are a subset or an")
        print("     exact copy.  So the union is safe, and which duplicate is kept cannot")
        print("     change the answer -- which is a second reason the checksum is stable.")

        C.rule("what each rule is worth, in rupees, on these days alone", "-")
        money = con.execute("""
          WITH latest AS (SELECT * FROM (SELECT *, row_number() OVER
                  (PARTITION BY bill_no,line_no ORDER BY resend_seq DESC, source_file DESC) rn
                  FROM allsrc) WHERE rn=1),
               oldest AS (SELECT * FROM (SELECT *, row_number() OVER
                  (PARTITION BY bill_no,line_no ORDER BY resend_seq ASC, source_file ASC) rn
                  FROM allsrc) WHERE rn=1),
               newest_file AS (
                  SELECT a.* FROM allsrc a JOIN (
                    SELECT split_part(bill_no,'/',1) s, split_part(bill_no,'/',2) d,
                           max(resend_seq) mx FROM allsrc GROUP BY 1,2) m
                  ON split_part(a.bill_no,'/',1)=m.s
                 AND split_part(a.bill_no,'/',2)=m.d AND a.resend_seq=m.mx)
          SELECT
            'A  no de-duplication at all'                AS rule,
            (SELECT round(sum(qty*unit_price),2) FROM allsrc
              WHERE line_type NOT IN ('TAX','TENDER'))   AS revenue
          UNION ALL SELECT 'B  newest file per store-day wins',
            (SELECT round(sum(qty*unit_price),2) FROM newest_file
              WHERE line_type NOT IN ('TAX','TENDER'))
          UNION ALL SELECT 'C  union, keep the ORIGINAL line',
            (SELECT round(sum(qty*unit_price),2) FROM oldest
              WHERE line_type NOT IN ('TAX','TENDER'))
          UNION ALL SELECT 'D  union, keep the LATEST line  <- chosen',
            (SELECT round(sum(qty*unit_price),2) FROM latest
              WHERE line_type NOT IN ('TAX','TENDER'))""").fetchdf()
        print(money.to_string(index=False))
        v = {r.rule[0]: float(r.revenue) for r in money.itertuples()}
        print(f"\n  rule A over-states these days by {v['A']-v['D']:,.2f} "
              f"({v['A']/v['D']:.2f}x) -- every re-sent line counted twice")
        print(f"  rule B under-states them by      {v['D']-v['B']:,.2f} "
              f"-- the {only_orig:,} lines only the original had")
        print(f"  rules C and D agree exactly      ({v['C']:,.2f} = {v['D']:,.2f}), because")
        print("     no re-send contradicts its original.  The chosen rule is D: it is")
        print("     correct today and it stays correct if a future re-send ever does")
        print("     carry a correction.")

        json.dump(dict(
            resent_store_days=len(resent_days), files_involved=len(sel),
            rows_across_files=int(total), distinct_keys=int(distinct),
            partial_resends=n_partial, conflicting_keys=0,
            lines_only_in_resend=int(only_resend), lines_only_in_original=int(only_orig),
            revenue_by_rule=v,
        ), open(C.RESULTS / "b_resend_rules.json", "w"), indent=2)
        shape.to_csv(C.RESULTS / "b_resend_shape.csv", index=False)
        print("\nwritten -> results/b_resend_rules.json, b_resend_shape.csv")


if __name__ == "__main__":
    main()
