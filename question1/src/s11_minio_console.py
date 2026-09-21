"""
Screenshots of the object store's own console.

Part (a) claims a bucket exists in MinIO, organised by store and month.  The
S3 API agreeing with that is one kind of evidence; MinIO's own web console
showing it is the kind a reader can check at a glance.

This drives a real Chromium at http://localhost:9001, signs in with the
credentials from docker-compose.yml, and photographs four views:

    minio_01_login          the console, before sign-in
    minio_02_buckets        the bucket list, with object count and size
    minio_03_browse_raw     inside raw/sales/ -- the store_id= partitions
    minio_04_browse_month   inside one store -- the business_month= partitions
    minio_05_the_one_file   the single Parquet object a S03/2024-10 query reads

Scripted rather than hand-captured so it can be re-run, and so the shots cannot
drift from what the bucket actually contains.

    python src/s11_minio_console.py            # headless (default)
    python src/s11_minio_console.py --headed   # watch it happen
"""
from __future__ import annotations

import argparse
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import config as C

from playwright.sync_api import sync_playwright

CONSOLE = f"http://localhost:9001"
VIEWPORT = {"width": 1500, "height": 900}


def dismiss(page) -> None:
    """Close the AGPL licence modal the Community Edition console raises on
    first sign-in.  Safe to call when it is not there."""
    for sel in ("button:has-text('Acknowledge')", "button:has-text('Accept')",
                "button:has-text('Close')"):
        try:
            btn = page.locator(sel).first
            if btn.is_visible(timeout=1200):
                btn.click()
                page.wait_for_timeout(900)
                return
        except Exception:
            continue


def open_folder(page, name: str) -> None:
    """Click a folder row in the object browser and wait for the listing."""
    page.wait_for_timeout(700)
    dismiss(page)
    row = page.locator(f"xpath=//*[normalize-space(text())='{name}']").last
    row.wait_for(state="visible", timeout=30_000)
    row.click()
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(1800)


def shot(page, name: str, note: str) -> None:
    path = C.SCREENSHOTS / f"minio_{name}.png"
    page.screenshot(path=str(path))
    print(f"  {path.name:<28} {note}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--headed", action="store_true",
                    help="show the browser window instead of running it headless")
    args = ap.parse_args()

    with C.Tee(C.LOGS / "s11_minio_console.log"):
        C.rule("(a) THE OBJECT STORE, IN ITS OWN CONSOLE")
        print(f"\nconsole : {CONSOLE}")
        print(f"bucket  : {C.BUCKET}")
        print("credentials come from docker-compose.yml, not from anywhere personal\n")

        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=not args.headed)
            page = browser.new_page(viewport=VIEWPORT)

            # ---- 1. the console before sign-in ---------------------------
            page.goto(CONSOLE, wait_until="networkidle", timeout=60_000)
            page.wait_for_timeout(1200)
            shot(page, "01_login", "the console, before sign-in")

            # ---- 2. sign in ----------------------------------------------
            page.fill("#accessKey", C.S3_KEY)
            page.fill("#secretKey", C.S3_SECRET)
            page.click("button[type=submit]")
            page.wait_for_timeout(3500)

            # The Community Edition console opens an AGPL licence modal over the
            # whole page on first sign-in.  Dismiss it, or every shot below is a
            # photograph of a licence notice.
            dismiss(page)

            # The object browser root.  This build of the Community Edition
            # console has no separate /buckets management page -- that route
            # renders an empty panel -- so the browser root IS the bucket view.
            page.goto(f"{CONSOLE}/browser", wait_until="networkidle", timeout=60_000)
            page.wait_for_timeout(2500)
            dismiss(page)
            shot(page, "02_bucket", "the bucket, and its three top-level prefixes")

            # ---- 3, 4, 5. walk down the partitions -----------------------
            # Navigated by clicking, not by building URLs.  This console encodes
            # the prefix into the path in a form that varies between releases;
            # clicking the folder the reader would click is version-agnostic and
            # is also a more honest demonstration -- it is the route a person
            # takes.
            open_folder(page, "raw")
            open_folder(page, "sales")
            shot(page, "03_browse_raw", "raw/sales/ -- the store_id= partitions")

            open_folder(page, "store_id=S03")
            shot(page, "04_browse_month", "one store -- the business_month= partitions")

            open_folder(page, "business_month=2024-10")
            shot(page, "05_the_one_file",
                 "S03 / 2024-10 -- the one file the pruned query opens")

            browser.close()

        print("\nThis is the same bucket the pipeline writes to and the same one")
        print("DuckDB reads over httpfs.  s03_layout.py counted 1 object /")
        print("113,465 bytes for a S03 October query; view 05 is that object.")


if __name__ == "__main__":
    main()
