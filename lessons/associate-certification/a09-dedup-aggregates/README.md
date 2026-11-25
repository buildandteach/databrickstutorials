# A09 Deduplication and aggregates with PySpark and SQL

Lesson A09 of the **Databricks Data Engineer Associate exam prep series** for **Databricks Free Edition**.

Exam relevance: *Databricks Certified Data Engineer Associate* exam guide (August 2026), **Section 3, Data Transformation and Modeling (22%)**:

- Perform data deduplication operations and aggregate operations on DataFrames, such as count, approximate count distinct, and mean, summary.

Video: [youtu.be/5ZNyoawWl2k](https://youtu.be/5ZNyoawWl2k)

Previous lessons: [A02 Ingestion patterns](../a02-ingestion-patterns/), [A03 COPY INTO](../a03-copy-into/), [A04 Auto Loader schema evolution](../a04-autoloader-schema-evolution/), [A05 JSON and explode](../a05-json-explode/), [A06 Ingestion methods](../a06-ingestion-methods/), [A07 Bronze to silver cleaning](../a07-bronze-to-silver/), [A08 Joins and unions](../a08-joins-unions/).

## On the job

`orders_cdc` is a change data capture (CDC) feed from the order system. Every change sends the whole row again with a `change_ts` timestamp; rows arrive late and files repeat. Keep the latest change per `order_id` with `row_number()` (5 rows), then report the daily order count and average order value: Nov 2, 2 orders, 32.25; Nov 3, 3 orders, 23.83. The same report on the raw feed gives 4 / 29.63 and 4 / 21.25 with no error (cell 9).

## Sample data only

Fictional data: `orders_cdc`, 8 rows. Order 5003 is an exact copy, order 5001 has a same-day update (24.00 to 28.50), order 5002 has a late update (30.00 to 36.00, arriving Nov 4), orders 5004 and 5005 appear once. No download and no personal data.

## What the notebook writes

Cell 1 creates a new schema `workspace.bt_dedup_` plus 12 hex characters and makes it current with `USE`. Inside it: the managed table `orders_cdc`. Nothing outside the new schema is touched. The solution and cleanup cells are commented out.

## Setup on Databricks Free Edition

1. Home **New**, then **Notebook**, select **Python** and **Serverless**, or import `dedup_aggregates_a09.ipynb`.
2. Run all.

`dedup_aggregates_a09.py` is the Databricks source form. The `.ipynb` is authoritative. `cheatsheet.pdf` is the two-page cheat sheet (spec `cheatsheet-spec.json`), and `script.md` is the lesson narration.

## What a successful run should show

| Check | Expected |
|---|---|
| Cell 2 (feed) | `8` |
| Cell 3 (distinct, dropDuplicates()) | `7`, `7` |
| Cell 4 (dropDuplicates on order_id) | `5` rows; which row survives per order is not guaranteed (one local run kept 5001 24.00 + 5002 36.00, another kept 5002 30.00) |
| Cell 5 (wrong keys) | `4` (customer_id), `7` (order_id + change_ts) |
| Cell 6 (row_number, latest) | `5`; 5001 28.50, 5002 36.00, 5003 13.50, 5004 40.00, 5005 18.00 |
| Cell 7 (SQL) | 2026-11-02, 2, 32.25 and 2026-11-03, 3, 23.83 |
| Cell 8 (daily) | same as cell 7 |
| Cell 9 (raw feed, wrong) | 2026-11-02, 4, 29.63 and 2026-11-03, 4, 21.25 |
| Cell 10 (counts) | `8`, `8`, `5`, `5` |
| Cell 11 (NULL order_id row) | `9`, `8`, `5` |
| Cell 12 (approx on 100,000 ids) | local run: 95546, 102065, 100000 (Databricks may differ slightly for the approximate values) |
| Cell 13 (describe, summary) | count 5, mean 27.200000, min 13.50, 25% 18.0, 50% 28.5, 75% 36.0, max 40.00 |
| Cell 14 (snippet) | no assertion error (5 == 5), same daily report |

## Key points

- `distinct()` and `dropDuplicates()` with no arguments remove only rows that match in every column.
- `dropDuplicates(["key"])` keeps one row per key, but you cannot choose which. Too narrow a key deletes real rows; too wide keeps the duplicates.
- To keep the latest change, use `row_number()` over `Window.partitionBy(key).orderBy(F.col(ts).desc())` and keep row 1. Add a tiebreaker if timestamps can tie.
- Dedupe first, then aggregate. `F.avg` and `F.mean` are the same function and skip NULLs.
- `count("*")` counts rows, `count(col)` skips NULLs, `count_distinct` is exact, `approx_count_distinct` is an estimate (HyperLogLog++, default maximum relative standard deviation 0.05).
- Memory trick: dropDuplicates keeps a row. row_number keeps the right row.

## Dedup checklist

1. Business key: `order_id`, not `customer_id`, not `order_id` + `change_ts`.
2. Sort: `change_ts`, newest first, plus a tiebreaker if two rows can share it.
3. Keep latest: `row_number()`, keep `rn = 1`. Use `dropDuplicates(key)` only when any surviving row is fine (exact copies).
4. Check: `count()` after dedup = `count_distinct(key)` before (5 = 5).
5. Then aggregate.

## Practice

1. `orders_cdc.filter("op = 'insert'").dropDuplicates().count()`: **5** (6 insert rows, one exact copy of 5003).
2. Customer 101's total amount in `orders_latest`: **68.50** (5001 at 28.50 + 5004 at 40.00).
3. `orders_latest.select(F.count_distinct("customer_id"))`: **4**.

## Cheat sheet

[cheatsheet.pdf](cheatsheet.pdf) is the two-page lesson reference. `cheatsheet-spec.json` is its content spec.

## Files

| File | What it is |
|---|---|
| `dedup_aggregates_a09.ipynb` | The notebook (authoritative) |
| `dedup_aggregates_a09.py` | Databricks source form of the same notebook |
| `cheatsheet.pdf` | Two-page cheat sheet |
| `cheatsheet-spec.json` | Cheat sheet content spec |
| `script.md` | Lesson narration script |

## Validation

The notebook and `.py` source compile, and the PDF has 2 pages. Every code cell was run on local Apache Spark 4.0.1 (not Databricks; American National Standards Institute (ANSI) SQL mode on, as on serverless) with the `workspace.` catalog prefix removed; the outputs matched the table above. A Databricks Free Edition run has not been recorded yet.

Independent study material. Not affiliated with or endorsed by Databricks. No exam guarantee.
