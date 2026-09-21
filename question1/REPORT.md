# Annapurna Stores — October is October

**Question 1.** A platform where the CFO's four slices come out the same every time,
and where March 2024 is priced at March 2024.

Everything below was produced by `python run_all.py` against `/exam/data/` —
eleven steps, 4.7 minutes end to end. Every figure quoted here is in `results/`
as CSV or JSON, in `logs/` as the verbatim console output of the run that
produced it, and in `screenshots/` twice over: `terminal_*.png` are photographs
of the real Windows Terminal window the pipeline ran in, `console_*.png` render
that run's complete output, which is longer than a screen can hold. See the
README for how each was made.

One caveat on the numbers, stated once. Counts, bytes, row totals, revenue and
checksums are properties of the data and are identical on every run — they have
been reproduced across several separate invocations hours apart. **Wall-clock
timings are not**: they were measured on a laptop also running Docker, MinIO and
PostgreSQL, and move by a factor of two between runs. Where a millisecond figure
appears below it is the one in `results/` from the run the screenshots come from,
and it is quoted to show a ratio, not a benchmark.

---

## The headline

| | |
|---|---|
| Revenue, FY2024 | **₹52,28,65,735.75** (522,865,735.75) |
| October 2024 | **₹5,63,59,195.92** — the figure finance signed off, to the paisa |
| Months matching finance exactly | **9 of 12** |
| The number the old spreadsheet gives for October | 122,501,668.06 — **2.17× too high** |
| Load run three times | identical row count, identical checksum, identical bytes |
| One store, one month | engine opens **1 file / 113,465 bytes**, not 4,389 / 22,181,867 |

The three different October figures in that meeting were not three people making
mistakes. They were three people making *different* mistakes against a folder that
does not say which rows are revenue. Section (c) is where that gets fixed, and
section (f) is where the remaining differences get named.

---

## (a) The platform, and the layout

### The three systems

| role | what | where |
|---|---|---|
| object store | **MinIO** (S3 API) | container, `:9000` |
| relational database | **PostgreSQL 16** | container, `:5433` |
| analytical query engine | **DuckDB 1.5.5** | embedded on the host |

`docker-compose.yml` brings the first two up. DuckDB is embedded rather than a
server because it is the piece that has to reach *both* — `httpfs` to MinIO,
`postgres_scanner` to PostgreSQL — and section (e) depends on it holding both
sides of a join without either system staging data into the other.

`masters.sql` loads into PostgreSQL unchanged: 12 stores, 14 categories,
1,224 product rows, 4,320 price revisions.
→ `screenshots/terminal_01_a_stack_up.png` (real terminal), `console_01_s01_stack_up.png` (full output)

### The layout

```
s3://annapurna-lake/raw/sales/store_id=<S>/business_month=<YYYY-MM>/part-0.parquet
```

Hive-style, **store first, then month**. 12 × 12 = 144 objects.

The reasoning, in order:

- Every question on the dashboard is bounded by a store, a month, or both. Putting
  both in the *path* means the engine decides what to skip from a directory name,
  before opening anything.
- `business_date` stays a **column**, not a partition. Day-of-week has to work, and
  partitioning by day as well would mean 4,392 tiny objects — more request overhead
  than the scanning it saves. Parquet row-group statistics on that column give the
  within-file pruning instead.
- Category is *not* in the path. It is an attribute of the product, which is in
  PostgreSQL and changes over time; baking it into a file path would freeze a fact
  that the master data owns.

### What the engine can actually have to open

Measured, for **revenue for store S03 in October 2024**. File counts come from an
S3 `LIST`, byte counts from the sizes MinIO reports, and the scan detail from
DuckDB's own profile — no figure here is taken from documentation.

| layout | objects | bytes |
|---|---:|---:|
| the shared folder as it stands today (raw CSV, one folder) | 4,457 | 68,706,877 |
| the same rows as Parquet, still **one flat prefix** | 4,389 | 22,181,867 |
| the partitioned lake, whole year | 144 | 9,733,990 |
| **the partitioned lake, pruned to S03 / 2024-10** | **1** | **113,465** |

**4,389 objects → 1. 22,181,867 bytes → 113,465, a 195× reduction.**
Against the folder as it is today: 4,457 files and 68.7 MB → one file and 113 KB.

Both layouts return the identical answer (`('S03','2024-10', 6525145.39, 2062)`).
The partitioned one takes **19.9 ms**, the flat one **869.8 ms** — 44× — and
DuckDB says why in its own words:

```
partitioned layout            flat layout
Total Files Read: 1           Total Files Read: 4389
```

### The same thing, in MinIO's own console

The S3 API and DuckDB's profile both say the layout is what it claims to be.
MinIO agrees, and says so in a form that can be checked at a glance:

| view | what it shows |
|---|---|
| `minio_02_bucket` | `annapurna-lake`, PRIVATE, **39.6 MiB · 4,682 objects**, prefixes `flat/ mart/ raw/` |
| `minio_03_browse_raw` | `raw/sales/` — all twelve `store_id=` partitions |
| `minio_04_browse_month` | inside `store_id=S03` — the twelve `business_month=` partitions |
| `minio_05_the_one_file` | `store_id=S03/business_month=2024-10/` — **one object, `part-0.parquet`, 110.8 KiB** |

110.8 KiB is the 113,465 bytes counted above, and the 4,682 objects are the
144 partitioned + 4,389 flat-control + 149 mart objects the pipeline wrote.
The claim and the object store's own accounting of itself do not depend on each
other, and they agree.

→ `screenshots/terminal_04_a_layout.png` (real terminal), `minio_02_bucket.png`
through `minio_05_the_one_file.png` (the object store's console),
`chart_08_layout_pruning.png`, `console_04_s03_layout_*.png` (full output),
`results/a_explain_partitioned.txt`, `results/a_explain_flat.txt`

### Landing three dialects

`billing_notes.md` is right about all three, and the loader follows it exactly:

- **S01–S05** comma, ISO-8601 timestamps.
- **S06–S09** semicolon, `dd-mm-yyyy`. The format is given to the parser
  explicitly — left to a sniffer, `05-03-2024` in `SALES_S07_20240305.csv` could be
  read as 3 May and silently move a day's revenue into another month.
- **S10–S12** UTF-8 BOM, epoch seconds UTC, columns in a different order.

> The brief describes the sales folder as "mixed CSV and parquet". In the data
> actually supplied, all 4,457 files are CSV. The loader has a Parquet branch
> anyway, because the vendor's note says that experiment was never rolled back and
> a Parquet file could reappear on any night.

**The business date is the date in the file name**, never the timestamp inside.
76,689 rows — 6.8% — carry a timestamp on the *following* calendar day, because
bills punched between midnight and about 01:30 belong to the trading day that just
ended. Dating those rows by their timestamp would move ~7% of every month's
revenue across the month boundary, and it would do it in both directions, so the
monthly totals would look almost right.

---

## (b) Safe to run twice

### The proof

Three consecutive full loads:

| run | rows | logical checksum (sha256) | objects sha256 |
|---|---:|---|---|
| 1 | 1,120,924 | `4787560c4c8a256123548ee0df83f2a7434e51c27e58eccbc13d0fc031313505` | `2a1c3986c525a819…` |
| 2 | 1,120,924 | `4787560c4c8a256123548ee0df83f2a7434e51c27e58eccbc13d0fc031313505` | `2a1c3986c525a819…` |
| 3 | 1,120,924 | `4787560c4c8a256123548ee0df83f2a7434e51c27e58eccbc13d0fc031313505` | `2a1c3986c525a819…` |

```
identical row count      : True
identical logical digest : True
identical object bytes   : True
VERDICT: IDEMPOTENT
```

Two checksums, deliberately:

- The **logical** one is a Merkle digest — md5 over every row in key order per
  partition, then sha256 over the 144 sorted partition digests. It ignores file
  layout, compression and write time, so it answers "is this the same data".
- The **physical** one is sha256 over the actual bytes of all 144 objects in
  MinIO. It answers the stronger question, "is this the same file", and it holds
  because the load has an explicit `ORDER BY` and no append anywhere.

Worth noting, because it is a stronger claim than "three runs in a row agreed":
the pipeline was later run again, hours apart, in a separate process, for the
terminal screenshots. Both digests came back **identical to the ones above** —
`4787560c…` and `2a1c3986…`. Determinism here is a property of the load, not of
one lucky invocation.

→ `screenshots/terminal_02_b_load_x3.png` (real terminal),
`console_02_s02_ingest_runs3_p2.png` (full output), `results/b_idempotency_runs.json`

### How, and why that rule

Each store-month partition is rebuilt from its full set of source files and
written with overwrite semantics. Nothing appends. Within a partition, rows are
de-duplicated on **`(bill_no, line_no)`** — the line, not the file.

That choice is not cosmetic. Of 54 re-sent store-days, **8 re-sends are partial
rolls**: the till was mid-roll when ops re-triggered it, so the re-send contains
fewer bills than the original. What each candidate rule is worth, on those days
alone:

| rule | revenue on the re-sent days |
|---|---:|
| A — no de-duplication at all | 14,638,905.11 |
| B — newest file per store-day wins | 6,142,450.78 |
| C — union, keep the original line | 6,715,362.99 |
| **D — union, keep the latest line ← chosen** | **6,715,362.99** |

Rule A double-counts every re-sent line — **2.18×**, the same shape of error as the
October one. Rule B, the obvious-looking rule, **loses ₹572,912.21**: 1,282 lines
exist only in the original file. C and D agree exactly, because no re-send ever
contradicts its original (0 conflicting keys, 0 lines present only in a re-send).
D is chosen over C because it stays correct if a future re-send ever *does* carry a
correction, and because "latest wins" is deterministic — which is the second reason
the checksum is stable, not just the first.

→ `screenshots/terminal_03_b_resends.png` (real terminal),
`console_03_s02b_resends_*.png` (full output), `results/b_resend_rules.json`

---

## (c) The tables behind the dashboard

A star. Dimensions published from PostgreSQL, fact partitioned in the object store.

| table | rows | note |
|---|---:|---|
| `dim_store` | 12 | name, address, city, region, floor area |
| `dim_category` | 15 | the 14 real ones + an explicit unknown member |
| `dim_product` | 1,225 | SCD-2 on `product_sk`, + an unknown member |
| `dim_date` | 366 | carries **day of week** and **month** |
| `fact_sale_line` | 789,516 | one revenue-bearing line, **keys only** |
| `agg_revenue_day` | 65,685 | store × category × day — what the dashboard reads |

`agg_revenue_day` is the finest grain any of the four requested slices needs, and
at 65,685 rows all four answer in 12–23 ms.

### Why the fact carries `store_id` and not the store's name

Measured like-for-like — same rows, same order, one file each:

| shape | on disk |
|---|---:|
| keys only (`store_id`) | 6,469,836 |
| store name + address on every row | 6,473,651 |

**1.00×.** Read that honestly: Parquet dictionary-encodes a column with twelve
distinct values down to nothing, so the storage argument people usually lead with
barely exists here, and claiming a 10× saving would be false.

The cost is real in two other places. Materialised as text — a CSV extract, a row
store, strings in memory — those five columns are **62,158,681 bytes** on the fact
against **945 bytes** in `dim_store`, a factor of 65,776. And the argument that
actually decides it is maintenance: the address exists in exactly one row. When
Koramangala moves, one `UPDATE` in PostgreSQL republishes a 12-row dimension;
the alternative is rewriting 789,516 fact rows and hoping every historic extract
gets the same treatment. That is the same class of mistake as three different
October figures — one fact stored in many places, drifting apart.

### Trap 1 — not every line is a sale

**This is the part the warning points at, and it is worth 2.17×.**

| October 2024, added up four ways | |
|---|---:|
| sum every row in the file | 122,501,668.06 |
| drop `TENDER`, keep the GST | 61,250,834.03 |
| **revenue as defined** | **56,359,195.92** |
| finance, signed off 2024-11-09 | 56,359,195.92 |

`TENDER` is the bill total written into the same file as another row, so summing
everything counts each bill roughly twice; `TAX` then adds the GST on top. The
fact table admits only `SALE`, `RETURN`, `DISCOUNT`, `VOID`. Returns, discounts
and voids carry their own sign and subtract.

On `VOID` the vendor's warning is "either keep both, or drop every line of the
bill — do not do half of it." Both are kept, so a cancelled bill nets to zero;
verified, the item lines of every `VOID` bill sum to exactly **0.00**.

A detail that bites: **117 `VOID` rows mirror a bill-level discount** and carry the
literal code `DISC` with `line_type='VOID'`, not `'DISCOUNT'`. Keying the unknown
member on `line_type` leaves those with a null `product_sk`, and they then vanish
from any inner join to the product dimension — ₹1,230.01 of March quietly gone.
They are keyed on the code instead, and an assertion in `s04_model.py` fails the
build if any fact row has an unresolved product.

### Trap 2 — a product code does not identify a product

In June 2024, 24 retired codes were reissued to different products, several in a
different category:

| code | sk | product | category | valid |
|---|---|---|---|---|
| P100621 | 1089 | Catch Coriander Powder 500g | C06 Spices & Masala | …2024-05-31 |
| P100621 | 2213 | Cadbury Chewing Gum 50g | C13 Confectionery | 2024-06-01… |

| | rows |
|---|---:|
| item lines in the lake | 766,913 |
| after joining on `product_code` alone | **782,077** (+15,164 phantom rows) |
| after joining on code **and** business date | 766,796 |

The fact is keyed on `product_sk`, resolved by `(product_code, business_date)`
against `products.valid_from/valid_to`.

### Integrity

```
source revenue-bearing lines : 789,516
fact_sale_line rows          : 789,516      no fan-out: True
rows with unresolved product : 0            must be 0: True
lake revenue                 : 522,865,735.75
fact revenue                 : 522,865,735.75   ties: True
agg  revenue                 : 522,865,735.75   ties: True
```

### The same number every time

Four differently-written October queries — two reading the aggregate, one the fact
table, one going all the way back to the raw landed lines in the object store —
five runs each:

```
agg, filtered on business_month              56,359,195.92   all 5 runs identical
agg, rolled up from store and category       56,359,195.92   all 5 runs identical
fact table, joined to dim_date               56,359,195.92   all 5 runs identical
raw landed lines in the object store         56,359,195.92   all 5 runs identical
finance, signed off 2024-11-09               56,359,195.92
```

→ `screenshots/terminal_05_c_star.png`, `terminal_06_c_dashboard.png` (real terminal),
`console_05_s04_model_*.png`, `console_06_s08_dashboard_*.png` (full output),
`chart_03`–`chart_07`, `results/c_dashboard_*.csv`

---

## (d) March at March's prices

Two parameterised SQL files, neither edited between runs:

- `sql/d_what_did_it_sell_for.sql` — the category manager's question
- `sql/d_period_report.sql` — the period report

Three as-of joins do the work, all driven off `$period` and none hard-coded:

1. **scope** — `business_date BETWEEN period_start AND period_end`
2. **which product the code meant** — `products.valid_from / valid_to`
3. **which price was in force** — `price_revisions.effective_from / effective_to`,
   joined on the **day of the sale**, not the period end and not today. A price
   that moved on the 14th is honoured from the 14th, which is what "March is
   March" has to mean when a price moved mid-month.

### The biscuit pack

`$code = 'P106506'` — *Unibic Marie Gold 60g*. Same query text, one bound value
changed:

| `$period` | what that code meant then | price then | revision window |
|---|---|---:|---|
| `2024-03` | Unibic Marie Gold 60g | **91.95** | 2022-01-01 → 2024-04-07 |
| `2024-11` | Unibic Marie Gold 60g | **125.80** | 2024-08-07 → 9999-12-31 |
| `2024-12` | Unibic Marie Gold 60g | 125.80 | 2024-08-07 → 9999-12-31 |

₹125.80 is the shelf price today, and it is exactly the number the spreadsheet
handed the category manager when they asked about March. March's price was
**₹91.95** — a 27% difference on the answer.

### The harder case

`$code = 'P100621'`, one of the 24 reissued codes:

| `$period` | answer |
|---|---|
| `2024-03` | **Catch Coriander Powder 500g**, Spices & Masala, ₹210.70 |
| `2024-11` | **Cadbury Chewing Gum 50g**, Confectionery, ₹134.28 |

Same code, same query, different period — and not merely a different price. A
different product, in a different department. A report that joins on the code
alone files March's coriander powder under Confectionery.

### The period report, run twice

| `$period` | as billed | at the price then in force | till-sync drift |
|---|---:|---:|---:|
| `2024-03` | 41,971,649.09 | 41,971,885.88 | +236.79 |
| `2024-11` | 51,583,838.47 | 51,574,561.98 | −9,276.49 |

The drift column is the vendor's "one line in seventy" — tills that had not synced
printed the previous price list. On 1.485% of `SALE` lines the printed price
differs from the authoritative one. Both numbers are reported rather than one
silently chosen: **as billed** is what the customer paid and what reconciles to
finance; **at the price then in force** is what the price list says it should have
been. The gap between them is a till-sync report that nobody currently gets.

→ `screenshots/terminal_07_d_asof.png` (real terminal),
`console_07_s05_asof_prices_*.png` (full output), `results/d_period_report_*.csv`

---

## (e) One query across both systems

`sql/e_federated.sql`. October, South and West regions, Food department, by region
and category, per square foot of shop floor. Store region and floor area exist
only in PostgreSQL; the sale lines exist only in the object store. Neither system
can answer it alone, and nothing is staged into either one.

```sql
FROM read_parquet('s3://annapurna-lake/raw/sales/**/*.parquet', hive_partitioning=1) l
JOIN pg.stores              s ON s.store_id = l.store_id
JOIN pg.products            p ON p.product_code = l.raw_code
                             AND l.business_date BETWEEN p.valid_from AND p.valid_to
JOIN pg.product_categories  c ON c.category_id = p.category_id
```

Cold run 134 ms; warm 148 ms. Do not read anything into that ordering — at
this data size the query is dominated by scheduling noise on a laptop that is
also running Docker, and the two are within each other's spread. The cold/warm
distinction matters below for a different reason: DuckDB caches remote Parquet,
so only a cold connection actually makes the object store serve bytes.

### Which part ran where — and how that is known

Three witnesses, two of them *outside* the engine that planned the query. Each
watches exactly one execution on its own cold connection: a warm re-run serves
Parquet from DuckDB's cache and would show zero GETs, making the object store look
idle when it is not, and counters spanning two executions would report double.

**Witness 1 — DuckDB's JSON profile.** (The box-drawn `EXPLAIN ANALYZE` is in
`results/e_explain.txt`; the claims below are read from the structured profile,
because the box art interleaves parallel branches across columns and scraping it
mis-reads operators whose names wrap.)

| `POSTGRES_SCAN` | rows | projection sent down | filter sent down |
|---|---:|---|---|
| `stores` | 8 | `store_id, region` | `region IN ('South','West')` |
| `stores` | 12 | `region, floor_area_sqft` | — |
| `products` | 1,224 | `product_code, valid_from, valid_to, category_id` | — |
| `product_categories` | 6 | `category_id, department, category_name` | `department='Food'` |

`READ_PARQUET`: 56,619 rows, partition filter `(business_month = '2024-10')`,
projection cut to 6 columns, row filter `line_type IN ('SALE','RETURN','VOID')`,
**files considered 12/12, files actually read 8**.

Operators: `HASH_JOIN` ×4, `HASH_GROUP_BY` ×2, `PROJECTION` ×4, `ORDER_BY` ×1,
`POSTGRES_SCAN` ×4, `READ_PARQUET` ×1.

**The most telling line in the whole profile:** `READ_PARQUET` considered twelve
partitions and opened **eight**. The four it skipped are the four stores that are
not in the South or West — and `region` is a column that exists *only in
PostgreSQL*. A predicate that could only be evaluated in the relational database
came back as a dynamic filter and pruned files in the object store. That is the
two systems cooperating on one query, visible in the engine's own profile.

**Witness 2 — PostgreSQL's account of itself**, from `pg_stat_statements` (reset
immediately before, so the counts are one execution) and the server log with
`log_statement=all`:

```
calls=1  rows=1224  COPY (SELECT "product_code","valid_from","valid_to","category_id"
                          FROM "public"."products" …) TO STDOUT (FORMAT "binary")
calls=1  rows=12    COPY (SELECT "region","floor_area_sqft" FROM "public"."stores" …)
calls=1  rows=8     COPY (SELECT "store_id","region" FROM "public"."stores"
                          … AND "region" IN ('South','West')) …
calls=1  rows=6     COPY (SELECT "category_id","department","category_name"
                          FROM "public"."product_categories" … AND "department" = 'Food') …
```

Every statement is a single-table `SELECT` with the projection cut to the columns
the query needs, and two carry a `WHERE` that came from the outer query. There is
no join, no `GROUP BY` and no sale line in anything PostgreSQL was asked to run.
**1,250 rows crossed the wire** — exactly the row count of its three master
tables — against 81,468 October sale lines it never saw.

**Witness 3 — MinIO's own counters** (`/minio/v2/metrics/node`; MinIO refreshes
these on roughly a ten-second cycle, so they are read with a settle loop):

| counter | before | after | delta |
|---|---:|---:|---:|
| `getobject` | 7,021 | 7,062 | **41** |
| `bytes_sent` | 116,665,539 | 117,285,564 | **620,025** |
| `putobject` | 583 | 583 | **0** |

DuckDB's own HTTPFS counter for the same run: **42 GET, 0 PUT, 605.4 KiB in**.
Two systems that do not talk to each other agree to within one request and 0.02%
of the bytes.

**Verdict**

| part of the query | ran in | evidence |
|---|---|---|
| scan + filter of `stores`, `products`, `product_categories` | **PostgreSQL** | `pg_stat_statements` + server log: single-table SELECTs, narrowed projections, outer `WHERE` pushed in, 1,250 rows out |
| reading the October sale lines, partition-pruned | **MinIO**, scanned by DuckDB | profile: 8 of 12 candidate files; DuckDB 42 GET / 0 PUT; MinIO independently 41 GET / 0 PUT / 620,025 bytes |
| the as-of product join, the store join, `GROUP BY`, the arithmetic | **DuckDB** | 4 `HASH_JOIN` + 2 `GROUP BY` appear only in the DuckDB profile; neither remote system was asked for any of it, and only DuckDB ever held both sides |

`putobject` delta of **0** is the direct answer to "without first copying either
side into the other": nothing was written into the object store, and PostgreSQL's
log shows nothing was staged the other way either.

→ `screenshots/terminal_08_e_federated.png` (real terminal),
`console_08_s06_federated_*.png` (full output), `results/e_federated.json`

---

## (f) Reconciliation

### Ruling out a bug in the pipeline — first, not last

Revenue is computed twice from row sets that do not overlap. `billing_notes.md`
gives the identity `TENDER = items − discount + tax`, so `sum(TENDER) − sum(TAX)`
is total revenue computed **without touching a single `SALE`, `RETURN`, `DISCOUNT`
or `VOID` line**.

| month | from item lines | from bill totals | difference |
|---|---:|---:|---:|
| all twelve | — | — | **0.0000** |

Largest disagreement across the year: **0.0000**. Alongside that:

- all **165,704** bills balance against their own lines
- **0** store-days have a gap in the bill sequence — bill numbers run 1..N per
  store-day with no holes, so no whole bill is missing from any day that arrived
- **9 of 12** months land on finance's signed-off figure exactly

Nine exact matches is itself a finding: finance and this platform **define revenue
the same way**. So the three differences are events, not a disagreement about
meaning.

### The comparison

| month | finance | platform | difference | % |
|---|---:|---:|---:|---:|
| 2024-01 | 38,446,071.33 | 38,446,071.33 | 0.00 | — |
| 2024-02 | 34,887,085.55 | 34,887,085.55 | 0.00 | — |
| **2024-03** | 42,457,899.09 | 41,971,649.09 | **−486,250.00** | −1.1453% |
| 2024-04 | 37,958,457.37 | 37,958,457.37 | 0.00 | — |
| 2024-05 | 41,764,716.40 | 41,764,716.40 | 0.00 | — |
| 2024-06 | 38,987,082.82 | 38,987,082.82 | 0.00 | — |
| **2024-07** | 40,527,291.81 | 40,295,160.11 | **−232,131.70** | −0.5728% |
| 2024-08 | 45,252,181.75 | 45,252,181.75 | 0.00 | — |
| 2024-09 | 44,615,037.46 | 44,615,037.46 | 0.00 | — |
| 2024-10 | 56,359,195.92 | 56,359,195.92 | 0.00 | — |
| 2024-11 | 51,583,838.47 | 51,583,838.47 | 0.00 | — |
| **2024-12** | 50,745,209.00 | 50,745,259.48 | **+50.48** | +0.0001% |

→ `screenshots/chart_01_revenue_by_month.png`, `chart_02_variance_vs_finance.png`

### A verdict for each month that differs

**2024-03 — ₹486,250.00 below finance (−1.15%). Cause: DEFINITION / SCOPE.**

What was checked: all 372 store-days present; no gap in any bill sequence; both
reconstructions of March agree to 0.00; and **no bill and no store-day in March is
worth that amount** (0 and 0). The shortfall is exactly ₹486,250.00 — round to the
rupee, in a month whose every other figure carries real paise.

The amount is not in the till data in any form, and the till data for March is
internally complete and self-consistent. Something finance counted in March never
passed through a till: a manual adjustment, an accrual, or a channel that does not
bill through the POS. A perfectly round figure is what a journal entry looks like,
not what 60,000 shopping baskets look like.

**2024-07 — ₹232,131.70 below finance (−0.57%). Cause: SOURCE DATA.**

Three store-days never arrived: **S07 (Pune), 9/10/11 July**, a Tuesday, Wednesday
and Thursday — exactly the outage `billing_notes.md` records. Valuing those three
days on S07's own Tuesday/Wednesday/Thursday averages for July gives
**₹222,193.07** against a gap of ₹232,131.70 — a residual of −₹9,938.63, −4.3%,
well within the day-to-day spread of a single store. (A flat daily average would
have over-estimated: S07's weekdays run well below its weekends, and 9–11 July are
all weekdays.)

The exports do not exist and never will. Finance has the numbers because the store
phoned them in.

**2024-12 — ₹50.48 above finance (+0.0001%). Not worth calling a cause.**

All 372 store-days present, no sequence gaps, both reconstructions agree to 0.00.
And December is the **only** month finance closed on a whole rupee — every other
month's signed-off figure carries real paise. One part in a million, on the one
figure that looks hand-keyed or rounded at close. The platform's number is the
supportable one. Note it; do not escalate it.

### Which of the three goes back to the finance team

**The definition difference — March 2024.**

It is the only one of the three that is about what revenue *means*, and the only
one that will recur silently. ₹486,250.00 is 1.15% of the month. If finance books
adjustments outside the POS, this platform can never derive them; the dashboard
will sit permanently below the closed books and nobody will know which of the two
numbers is wrong — which is exactly the failure that produced three October
figures in one meeting. The ask is narrow: *what is the ₹486,250.00 in March, and
is it a standing practice?* Then it becomes a documented adjustment line in the
model, and the two agree by construction rather than by luck.

**July is not finance's to solve.** It belongs to the billing vendor and the ops
desk — three exports were never produced. What is needed *from* finance is only
the phoned-in figures for those three days, so the gap is booked as a named
adjustment rather than left as an unexplained ₹232,131.70. The real fix is a
process one: an arrival check that notices a missing store-day the next morning
instead of eight months later.

**December buys nothing.** ₹50 on ₹5 crore. Raising it would spend the credibility
the other two need.

**And a bug in this pipeline** is the option that was eliminated first rather than
last — two independent reconstructions agreeing to 0.00 in all twelve months,
165,704 bills each balancing against their own lines, and nine months landing on
the signed-off figure exactly.

→ `screenshots/terminal_09_f_reconcile.png` (real terminal),
`console_09_s07_reconcile_*.png` (full output), `results/f_reconciliation.csv`

---

## What the analyst stops doing

The instruction was that the analyst can stop opening files by hand. Concretely:
the nightly load is one command and is safe to re-run at any point; the four slices
come off a 65,685-row aggregate in milliseconds; a question about March is answered
with March's prices and March's product catalogue from the same query text as a
question about last month; and the number is the same however it is asked for.

## What would need doing before this ran unattended

Stated plainly, because none of it is done here:

1. **An arrival check.** The July gap went unnoticed for eight months. The loader
   knows which store-days it expected and which arrived; that check should fail the
   nightly run, not a reconciliation in September.
2. **The March adjustment.** Until finance answers, the dashboard and the closed
   books differ by 1.15% in at least one month, with no line item explaining it.
3. **Scheduling and alerting.** `run_all.py` is a script, not an orchestrator.
   Full reload takes 4.7 minutes on this dataset; at ten times the volume the
   partition rebuild should be narrowed to the store-months that actually changed —
   the layout already makes that a cheap change, the loader does not yet do it.
4. **A published contract for `line_type`.** Every number here depends on `TENDER`
   and `TAX` being excluded. That rule currently lives in `config.py` and in the
   vendor's handover note. It should be something finance has signed.
