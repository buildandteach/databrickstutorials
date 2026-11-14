# A04 Auto Loader schema enforcement and evolution

Lesson A04 of the **Databricks Data Engineer Associate exam prep series** for **Databricks Free Edition**.

Exam relevance: *Databricks Certified Data Engineer Associate* exam guide (August 2026), **Section 2, Data Ingestion and Loading (21%)**:

- Use Auto Loader with schema enforcement and evolution.

Video: coming soon.

Previous lessons: [A02 Ingestion patterns](../a02-ingestion-patterns/), [A03 COPY INTO](../a03-copy-into/). Related: [Lesson 04, Auto Loader ingestion](../../associate-fundamentals/04-autoloader-ingestion/) in the Foundations series.

## Sample data only

Five fictional orders written by the notebook itself as two small CSV files (`orders_day1.csv`, `orders_day2.csv`). Day 2 adds a `channel` column and has one bad value (`amount = N/A`). The commented practice cell writes an optional `orders_day3.csv` with a `coupon` column. No download and no personal data.

## What the notebook writes

The setup cell reads `current_catalog()`, creates a new schema named `bt_autoloader_` plus 12 hex characters, two managed volumes inside it (`landing` for the source files under `landing/orders/`, `checkpoints` for the stream checkpoint and schema location), and makes that schema current with `USE`. One managed Delta table, `orders_bronze`, is written by an Auto Loader stream with `trigger(availableNow=True)`. `CREATE SCHEMA` has no `IF NOT EXISTS`, so it fails loudly instead of reusing an object. Nothing outside the new schema is touched. The practice-challenge and cleanup cells are commented out.

## Setup on Databricks Free Edition

Use a **Python** notebook on **Serverless**. Run the code cells from the top.

1. Home **New**, then **Notebook**, select **Python** and **Serverless**, or import `autoloader_schema_a04.ipynb`.
2. Run all. Cell 7 is expected to print "stream stopped:" (that's the lesson), and the notebook carries on. Optional: open **Catalog**, then **workspace**, then **bt_autoloader_...**, then **Volumes**, then **checkpoints**, then **orders_bronze**, then **_schemas** to see the stored schema versions.

`autoloader_schema_a04.py` is the Databricks source form. The `.ipynb` is authoritative. No paid-only features are used.

## What a successful run should show

| Check | Expected |
|---|---|
| Cell 3 (default inference) | `order_id`, `customer`, `amount`, `order_date` all `string`, plus `_rescued_data: string` |
| Cell 5 (run 1) | 3 rows; `order_id: integer`, `amount: decimal(10,2)`, `order_date: date`, `_rescued_data: string` |
| Cell 7 (run 2, day 2 with `channel`) | stream stops with an unknown field exception naming `channel`; table still 3 rows |
| Cell 8 (run 3, restart) | 5 rows; `channel` added (null for 1001-1003); 1005 `amount` null and `_rescued_data` = `{"amount":"N/A","_file_path":".../orders_day2.csv"}` |

## Key points

- `cloudFiles.schemaLocation` turns on schema inference and evolution; the schema is stored under `_schemas` there. It can share the checkpoint folder.
- CSV, JSON and XML columns are inferred as strings unless `cloudFiles.inferColumnTypes` is `true`. `cloudFiles.schemaHints` pins known types on top of inference (only when no `.schema(...)` is provided).
- `cloudFiles.schemaEvolutionMode`: `addNewColumns` (default without a provided schema: fail once, schema updated, restart resumes), `addNewColumnsWithTypeWidening` (Public Preview), `rescue` (never fails, new columns go to `_rescued_data`), `failOnNewColumns` (fails until you fix the schema or remove the file), `none` (default with a provided schema: new columns ignored).
- `_rescued_data` keeps type mismatches, columns missing from the schema and case mismatches as JSON with the source `_file_path`.
- `mergeSchema = true` on the Delta write lets the table accept the new column; without it the restart fails with a schema mismatch.
- Databricks recommends running Auto Loader in Lakeflow Jobs with retries so the restart after a new column is automatic.

## Practice

A day 3 file adds a `coupon` column. Default mode: the run fails once (unknown field `coupon`), the schema location is updated, the next run loads day 3 and `mergeSchema` adds `coupon`. `rescue` mode: no failure, no new column, `coupon` values go into `_rescued_data`. Provided schema with no mode: `none`, so `coupon` is ignored and not rescued unless `rescuedDataColumn` is set.

## Cheat sheet

[cheatsheet.pdf](cheatsheet.pdf) is the two-page lesson reference. `cheatsheet-spec.json` is its content spec.

## Files

| File | Role |
|---|---|
| `autoloader_schema_a04.ipynb` | Authoritative notebook |
| `autoloader_schema_a04.py` | Databricks source |
| [cheatsheet.pdf](cheatsheet.pdf) | Two-page reference |
| `cheatsheet-spec.json` | Content spec for the PDF |
| [script.md](script.md) | Narration script |
| `README.md` | This setup guide |

## Validation

The notebook and `.py` source compile, and the PDF has 2 pages. A Databricks Free Edition run has not been recorded yet; the table above lists the expected results.

Independent educational content; not affiliated with or endorsed by Databricks; no exam guarantee.
