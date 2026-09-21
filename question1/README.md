# Question 1 — Annapurna Stores

**Read [`REPORT.md`](REPORT.md).** It answers (a)–(f) with the evidence inline.
This file says how to run it and where everything lives.

---

## Run it

Needs Docker, and Python 3.11+ with the packages in `requirements.txt`.

```bash
pip install -r requirements.txt
python -m playwright install chromium    # only for the MinIO console shots
docker compose up -d
python run_all.py
```

Twelve steps, about **5 minutes** end to end on a laptop. It is safe to run
again — that is the point of part (b) — and a second run reproduces every
number, chart and screenshot in the report from the raw folder.

The exam data is expected at `../data` (i.e. `dmw/data/`, containing `sales/`,
`masters.sql`, `finance_monthly.csv`, `billing_notes.md`). Point it elsewhere
with `ANNAPURNA_DATA=/path/to/data`.

### The stack

| role | what | endpoint |
|---|---|---|
| object store | MinIO | `localhost:9000`, console `:9001` (`annapurna` / `annapurna_secret`) |
| relational database | PostgreSQL 16 | `localhost:5433`, db/user `annapurna` |
| analytical engine | DuckDB 1.5.5 | embedded, on the host |

`docker compose down -v` removes the containers and their volumes.

---

## What is in here

```
question1/
├── REPORT.md              the submission — answers to (a)–(f)
├── README.md              this file
├── docker-compose.yml     MinIO + PostgreSQL
├── requirements.txt
├── run_all.py             runs every step in order
│
├── src/
│   ├── config.py            paths, endpoints, the revenue definition, console tee
│   ├── s01_stack_up.py      (a) bring the three systems up, load masters.sql
│   ├── s02_ingest.py        (a)(b) 3 dialects → partitioned Parquet; --runs N
│   ├── s02b_resends.py      (b) what each de-duplication rule is worth
│   ├── s03_layout.py        (a) partition pruning, measured against a flat control
│   ├── s04_model.py         (c) the star, and the two traps in the source
│   ├── s08_dashboard.py     (c) the four slices; the same-number-every-time check
│   ├── s05_asof_prices.py   (d) one query text, two reporting periods
│   ├── s06_federated.py     (e) both systems in one query, with three witnesses
│   ├── s07_reconcile.py     (f) against finance_monthly.csv
│   ├── s11_minio_console.py (a) drives a real browser at MinIO's console
│   ├── s09_charts.py        the charts
│   └── s10_screenshots.py   renders every captured run as a PNG
│
├── sql/
│   ├── d_period_report.sql        parameterised on $period
│   ├── d_what_did_it_sell_for.sql parameterised on $period and $code
│   └── e_federated.sql            the cross-system query
│
├── tools/
│   ├── shoot_all.ps1        runs each step in a real Windows Terminal window
│   ├── shoot_step.ps1       …one step, then photographs the window
│   └── capture_window.ps1   screen-capture of one window's rectangle only
│
├── results/      every number, as CSV and JSON
├── logs/         every run's console output, verbatim
└── screenshots/  terminal_* + minio_* real screenshots, console_* full
                 output, chart_* charts
```

### Screenshots

Four sets, and they are not the same kind of thing:

| prefix | what it is |
|---|---|
| `terminal_*` | **photographs of a real Windows Terminal window** on the desktop, taken while the pipeline actually ran |
| `minio_*` | **a real Chromium driving MinIO's own web console**, showing the bucket the pipeline wrote |
| `console_*` | the **complete stdout** of those same runs, rendered as an image |
| `chart_*` | the charts |

**`minio_*` — the object store's own console.** `src/s11_minio_console.py` drives
a real Chromium at `http://localhost:9001`, signs in with the credentials from
`docker-compose.yml`, and walks down the partitions by clicking the folders a
reader would click. Scripted rather than hand-captured, so the shots cannot
drift from what the bucket actually holds.

| image | what it shows |
|---|---|
| `minio_01_login` | the console before sign-in |
| `minio_02_bucket` | `annapurna-lake`, PRIVATE, **39.6 MiB · 4,682 objects**, and the three prefixes |
| `minio_03_browse_raw` | `raw/sales/` — all twelve `store_id=` partitions |
| `minio_04_browse_month` | inside `store_id=S03` — the twelve `business_month=` partitions |
| `minio_05_the_one_file` | `store_id=S03/business_month=2024-10/` — **one object, `part-0.parquet`, 110.8 KiB** |

That last one is part (a)'s claim made visible: the query that used to scan a
folder of 4,457 files opens this single object.

**`terminal_*` — real screenshots.** `tools/shoot_all.ps1` runs each step in a
dedicated Windows Terminal window (`wt -w annapurna`), waits for it to finish,
and `tools/capture_window.ps1` photographs that window with
`Graphics.CopyFromScreen`. It captures **only the terminal window's own
rectangle** — nothing else on the desktop is in frame — and stops only the
process it started, by PID.

A photograph can only show the last screenful, which for most steps is the
conclusion. For three steps (`04`, `05`, `09`) the headline sits in the middle
of a long output, so those run the real command and then read that section back
out of the log the same run just wrote. The follow-up command is visible in the
shot; nothing is hidden.

```bash
powershell -ExecutionPolicy Bypass -File tools\shoot_all.ps1
```

**`console_*` — the full output.** Each script tees its stdout to
`logs/<script>.log` while it runs (`config.Tee`), and `s10_screenshots.py` draws
those exact bytes into a terminal-styled PNG. These are *renderings*, not
photographs — they exist because a 1536×864 screen cannot hold a 200-line run.
Nothing is retyped, reformatted or abridged: if a number appears, it appeared in
the run, and the matching `.log` is the plain-text original to diff against.

| step | real terminal | full output |
|---|---|---|
| (a) three systems identify themselves; masters loaded | `terminal_01_a_stack_up` | `console_01_s01_stack_up` |
| (b) three loads, one checksum | `terminal_02_b_load_x3` | `console_02_s02_ingest_runs3` |
| (b) partial re-sends, and what each rule costs | `terminal_03_b_resends` | `console_03_s02b_resends` |
| (a) 4,389 objects → 1, with both EXPLAIN plans | `terminal_04_a_layout` | `console_04_s03_layout` |
| (c) the star, the 2.17× trap, the join fan-out | `terminal_05_c_star` | `console_05_s04_model` |
| (c) the four slices; four routes to one October | `terminal_06_c_dashboard` | `console_06_s08_dashboard` |
| (d) the same query answering for March and November | `terminal_07_d_asof` | `console_07_s05_asof_prices` |
| (e) the plan, PostgreSQL's log, MinIO's counters | `terminal_08_e_federated` | `console_08_s06_federated` |
| (f) the twelve months and the three verdicts | `terminal_09_f_reconcile` | `console_09_s07_reconcile` |
| the charts being rendered | `terminal_10_charts` | — |
| both containers healthy | `terminal_11_docker_ps` | — |

---

## The shape of the data, in one place

Everything below is from `billing_notes.md`, verified against the files. It is
repeated here because every design decision in `src/` traces back to one of these
lines.

- **4,457 files**, 68.7 MB, 12 stores × 366 days, plus **68 re-sends**.
- **Three dialects.** S01–S05 comma/ISO; S06–S09 semicolon/`dd-mm-yyyy`;
  S10–S12 comma + UTF-8 BOM/epoch seconds, columns reordered.
- **The file name is the contract.** The business date is the date in the name.
  6.8% of rows carry a timestamp on the next calendar day.
- **Re-sends may be partial.** 8 of 54 re-sent store-days are. De-duplicate on
  `(bill_no, line_no)`, never "newest file wins".
- **`TENDER` is the bill total written as another row**, and `TAX` is GST.
  Neither is revenue. Counting them inflates the year by **2.17×**.
- **`VOID` mirrors the item lines of a cancelled bill with the quantity negated.**
  Keep both or drop the whole bill — half of the rule is worse than neither.
  117 `VOID` rows mirror a bill-level discount and carry the code `DISC`.
- **A product code does not identify a product.** 24 codes were reissued in June
  2024. Resolve `(product_code, business_date)` against
  `products.valid_from/valid_to`.
- **The printed price is not authoritative.** It disagrees with `price_revisions`
  on 1.485% of `SALE` lines. Historical questions are answered from
  `price_revisions` as of the date in question.
- **S07 (Pune) lost three days**, 9–11 July 2024. Those exports do not exist.

The revenue definition is stated once, in `src/config.py`, and used everywhere:

```python
REVENUE_LINE_TYPES     = ("SALE", "RETURN", "DISCOUNT", "VOID")
NON_REVENUE_LINE_TYPES = ("TAX", "TENDER")
```
