# A07 Bronze to silver cleaning with PySpark and SQL

Lesson A07 of the **Databricks Data Engineer Associate exam prep series** for **Databricks Free Edition**.

Exam relevance: *Databricks Certified Data Engineer Associate* exam guide (August 2026), **Section 3, Data Transformation and Modeling (22%)**:

- Implement data cleaning by reading bronze tables with PySpark or SQL, cleaning nulls, standardizing data types, and writing to new silver tables.
- Manipulate columns, rows and table structures by adding, dropping, splitting and renaming columns and applying filters. (Exploding arrays was [A05](../a05-json-explode/).)

Video: coming soon.

Previous lessons: [A02 Ingestion patterns](../a02-ingestion-patterns/), [A03 COPY INTO](../a03-copy-into/), [A04 Auto Loader schema evolution](../a04-autoloader-schema-evolution/), [A05 JSON and explode](../a05-json-explode/), [A06 Ingestion methods](../a06-ingestion-methods/).

## On the job

A nightly bronze orders feed arrives with every column as a string, some bad amounts (`N/A`) and some missing fields. Silver must still load, and you must report how many rows were rejected and why. Cell 10 writes the rejected rows with a reason and reconciles the counts.

## Sample data only

Six fictional orders in `bronze_orders` (`order_id`, `customer_name`, `amount`, `order_date`, `city_state`, `_rescued_data`), all strings. No download and no personal data.

## What the notebook writes

Cell 1 creates a new schema `workspace.bt_silver_` plus 12 hex characters and makes it current with `USE`. Inside it: `bronze_orders`, `silver_orders` (written by the PySpark chain, then replaced with the same rows by the SQL CREATE TABLE AS SELECT (CTAS) cell) and `orders_rejected`. Nothing outside the new schema is touched. The practice-challenge and cleanup cells are commented out.

## Setup on Databricks Free Edition

1. Home **New**, then **Notebook**, select **Python** and **Serverless**, or import `bronze_to_silver_a07.ipynb`.
2. Run all. Cell 3 shows the cast error on purpose inside a `try` block, so Run all keeps going.

`bronze_to_silver_a07.py` is the Databricks source form. The `.ipynb` is authoritative.

## What a successful run should show

| Check | Expected |
|---|---|
| Cell 2 (bronze) | `6` |
| Cell 3 (cast) | `[CAST_INVALID_INPUT] The value 'N/A' of the type "STRING" cannot be cast to "DECIMAL(10,2)" because it is malformed...` |
| Cell 4 (try_cast) | `1` bad value (order 1002) |
| Cell 5 (dropna) | `0`, then `5` |
| Cell 6 (fillna) | `1`, then `0` |
| Cell 7 (silver) | 3 rows: 1001 Ana Lopez 25.50 Austin TX · 1003 unknown 14.00 Denver CO · 1006 Eli Park 42.00 Austin TX; `order_id` int, `amount` decimal(10,2), `order_date` date |
| Cell 8 (rename no-op) | `True`, then the six silver columns |
| Cell 9 (SQL CTAS) | the same 3 rows |
| Cell 10 (rejected) | one row each: `no order_id`, `bad amount`, `no amount`; then `6 3 3` |
| Cell 11 (snippet) | `amount: 1 bad value(s)` |

## Key points

- Bronze keeps raw strings; silver is a NEW table written after cleaning nulls, casting types and filtering.
- Serverless compute runs with American National Standards Institute (ANSI) SQL mode on by default: `cast` fails on a malformed value (`CAST_INVALID_INPUT`), `try_cast` returns NULL. Count the NULLs `try_cast` creates.
- `dropna()` with no `subset` drops a row if any column is null (here: every row, because `_rescued_data` is null). Use `subset`.
- `fillna(0)` ignores string columns. Use a dict.
- `drop` and `withColumnRenamed` do nothing for a missing column. Check `columns` after the chain.
- `split` takes a regex and returns an array; with ANSI on, `getItem` past the end raises an error and `get()` returns NULL.
- Memory trick: cast fails loud. try_cast fails quiet. A quiet NULL still needs a count.

## Bronze to silver checklist

1. Read bronze, write a NEW silver table.
2. Required columns only: `dropna(subset=[...])`.
3. Types: `try_cast`, then count the NULLs it made.
4. Fill with a dict, never a bare `fillna(0)`.
5. Check columns after `drop` and `withColumnRenamed`.
6. Reconcile: bronze rows = silver rows + rejected rows.

## Practice

1. Remove the `.filter(...)` line: **5 rows** (1002 and 1005 land with a NULL amount).
2. `bronze.dropna(how="all").count()`: **6** (no row is null in every column).
3. Clean a `status` column (`"shipped"`, `"SHIPPED"`, `" Shipped"`, null): before the `fillna` step add `.withColumn("status", F.lower(F.trim("status")))` and add `"status": "unknown"` to the `fillna` dict.

## Cheat sheet

[cheatsheet.pdf](cheatsheet.pdf) is the two-page lesson reference. `cheatsheet-spec.json` is its content spec.

## Files

| File | Role |
|---|---|
| `bronze_to_silver_a07.ipynb` | Authoritative notebook |
| `bronze_to_silver_a07.py` | Databricks source |
| [cheatsheet.pdf](cheatsheet.pdf) | Two-page reference |
| `cheatsheet-spec.json` | Content spec for the PDF |
| [script.md](script.md) | Narration script |
| `README.md` | This setup guide |

## Validation

The notebook and `.py` source compile, and the PDF has 2 pages. Every code cell was run on local Apache Spark 4.0.1 (not Databricks; ANSI mode on, as on serverless) with the `workspace.` catalog prefix removed and the CTAS run as DROP + CREATE (no Delta locally); the outputs matched the table above. A Databricks Free Edition run has not been recorded yet.

Independent educational content; not affiliated with or endorsed by Databricks; no exam guarantee.
