"""
Run the whole thing, start to finish.

    docker compose up -d
    python run_all.py

Order matters: the stack has to be up and the masters loaded before anything
can be landed, the lake has to exist before the star can be built on it, and
the star has to exist before the dashboard or the reconciliation can read it.

Every step tees its output to logs/, and s10 renders those logs into the
screenshots the report references.  So a full run reproduces every number and
every image in the submission from the raw folder.
"""
from __future__ import annotations

import subprocess
import sys
import time
import pathlib

HERE = pathlib.Path(__file__).parent
PY = sys.executable

STEPS = [
    ("s01_stack_up.py",    [],            "(a) bring the three systems up, load masters"),
    ("s02_ingest.py",      ["--runs", "3"], "(a)(b) land the files, three times over"),
    ("s02b_resends.py",    [],            "(b) what each de-duplication rule is worth"),
    ("s03_layout.py",      [],            "(a) prove the partition pruning"),
    ("s04_model.py",       [],            "(c) build the star"),
    ("s08_dashboard.py",   [],            "(c) the four dashboard slices"),
    ("s05_asof_prices.py", [],            "(d) same query, different period"),
    ("s06_federated.py",   [],            "(e) one query across both systems"),
    ("s07_reconcile.py",   [],            "(f) reconcile against finance"),
    ("s11_minio_console.py", [],          "(a) the bucket, in MinIO's own console"),
    ("s09_charts.py",      [],            "charts for the report"),
    ("s10_screenshots.py", [],            "render every captured run as a PNG"),
]


def main() -> None:
    t0 = time.time()
    failed = []
    for i, (script, args, what) in enumerate(STEPS, 1):
        print(f"\n{'='*78}\n[{i}/{len(STEPS)}] {script}  --  {what}\n{'='*78}", flush=True)
        r = subprocess.run([PY, str(HERE / "src" / script), *args], cwd=HERE)
        if r.returncode != 0:
            print(f"!! {script} exited {r.returncode}")
            failed.append(script)
    mins = (time.time() - t0) / 60
    print(f"\n{'='*78}")
    if failed:
        print(f"FINISHED WITH FAILURES in {mins:.1f} min: {failed}")
        sys.exit(1)
    print(f"ALL {len(STEPS)} STEPS OK in {mins:.1f} min")
    print("  results/      the numbers, as CSV and JSON")
    print("  logs/         every run's console output, verbatim")
    print("  screenshots/  those runs rendered as PNGs, plus the charts")
    print("  REPORT.md     the answers to (a)-(f)")


if __name__ == "__main__":
    main()
