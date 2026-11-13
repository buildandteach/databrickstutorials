# A03 COPY INTO incremental loading

Lesson A03 of the **Databricks Data Engineer Associate exam prep series** for **Databricks Free Edition**.

Exam relevance: *Databricks Certified Data Engineer Associate* exam guide (August 2026), **Section 2, Data Ingestion and Loading (21%)**:

- Use the COPY INTO command to incrementally load files from cloud object storage into Unity Catalog governed tables.

Video: coming soon.

Previous lessons: [A01 Platform tour](../a01-platform-tour/), [A02 Ingestion patterns](../a02-ingestion-patterns/). Related: [Lesson 04, Auto Loader ingestion](../../associate-fundamentals/04-autoloader-ingestion/) in the Foundations series.

## Sample data only

Seven fictional orders written by the notebook itself as three small comma-separated values (CSV) files (`orders_day1.csv`, `orders_day2.csv`, `orders_day3.csv`; day 3 adds a `channel` column). No download and no personal data.

## What the notebook writes

The setup cell reads `current_catalog()`, creates a new schema named `bt_copyinto_` plus 12 hex characters, a managed volume `landing` inside it, and makes that schema current with `USE`. The CSV files go to `landing/orders/`. Two managed Delta tables are created in that schema: `orders_copy` (typed columns, loaded with `COPY INTO ... FROM (SELECT ...)`) and `orders_auto` (an empty placeholder table whose columns `COPY INTO` infers and evolves with `mergeSchema`). `CREATE SCHEMA` has no `IF NOT EXISTS`, so it fails loudly instead of reusing an object. Nothing outside the new schema is touched. The practice-challenge and cleanup cells are commented out.

## Setup on Databricks Free Edition

Use a **Python** notebook on **Serverless**. Run the code cells from the top.

1. Home **New**, then **Notebook**, select **Python** and **Serverless**, or import `copy_into_a03.ipynb`.
2. Run all. Optional: open **Catalog**, then **workspace**, then **bt_copyinto_...**, then **Volumes**, then **landing**, then **orders** to see the files.

`copy_into_a03.py` is the Databricks source form. The `.ipynb` is authoritative. No paid-only features are used. In a paid workspace, the source can also be a cloud storage path covered by a Unity Catalog external location.

## What a successful run should show

| Check | Expected |
|---|---|
| Cell 4 (run 1) | `num_affected_rows` 3, `num_inserted_rows` 3, `num_skipped_corrupt_files` 0 |
| Cell 5 (same command again) | `num_inserted_rows` 0, `orders_copy` still 3 rows |
| Cell 6 (day 2) | `num_inserted_rows` 2; `orders_day1.csv` 3, `orders_day2.csv` 2 |
| Cell 7 (schemaless table) | `num_inserted_rows` 5 (a new table has its own load history) |
| Cell 8 (day 3, new column) | `num_inserted_rows` 2, `channel: string` added, 7 rows, `channel` null for the 5 older rows |

## Key points

- `COPY INTO` is idempotent and retryable: files already loaded into that target table are skipped, even if they were edited since.
- `FORMAT_OPTIONS` go to the file reader (`header`, `inferSchema`, `mergeSchema` across source files, `ignoreCorruptFiles`). `COPY_OPTIONS` control the copy: `mergeSchema` evolves the target table, `force = true` turns idempotency off and reloads every file.
- `PATTERN` (glob) or `FILES` (up to 1,000 names), not both. `VALIDATE` checks without writing.
- COPY INTO vs Auto Loader: thousands of files over time, COPY INTO is fine; millions or more, use Auto Loader (fewer file-discovery operations, work split into batches, richer schema evolution). Reloading a chosen subset of files is easier with COPY INTO.

## Practice

After cell 8, rerun the cell 4 `COPY INTO`: 2 inserted (only `orders_day3.csv` is new for `orders_copy`), total 7, no `channel` column because the SELECT lists the columns. With `COPY_OPTIONS ('force' = 'true')`: 3 + 2 + 2 = 7 inserted, total 14 with duplicates. Two million new files a day: Auto Loader.

## Cheat sheet

[cheatsheet.pdf](cheatsheet.pdf) is the two-page lesson reference. `cheatsheet-spec.json` is its content spec.

## Files

| File | Role |
|---|---|
| `copy_into_a03.ipynb` | Authoritative notebook |
| `copy_into_a03.py` | Databricks source |
| [cheatsheet.pdf](cheatsheet.pdf) | Two-page reference |
| `cheatsheet-spec.json` | Content spec for the PDF |
| [script.md](script.md) | Narration script |
| `README.md` | This setup guide |

## Validation

The notebook and `.py` source compile, and the PDF has 2 pages. A Databricks Free Edition run has not been recorded yet; the table above lists the expected results.

Independent educational content; not affiliated with or endorsed by Databricks; no exam guarantee.
