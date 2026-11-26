# A10 Gold layer: views, materialized views and streaming tables

Lesson A10 of the **Databricks Data Engineer Associate exam prep series** for **Databricks Free Edition**.

Exam relevance: *Databricks Certified Data Engineer Associate* exam guide (August 2026), **Section 3, Data Transformation and Modeling (22%)**:

- Understand the difference between, and how to build, Gold layer objects such as materialized views, views, streaming tables, and tables for business intelligence (BI) and analytics teams in Unity Catalog.

Video: coming soon.

Previous lessons: [A02 Ingestion patterns](../a02-ingestion-patterns/), [A03 COPY INTO](../a03-copy-into/), [A04 Auto Loader schema evolution](../a04-autoloader-schema-evolution/), [A05 JSON and explode](../a05-json-explode/), [A06 Ingestion methods](../a06-ingestion-methods/), [A07 Bronze to silver cleaning](../a07-bronze-to-silver/), [A08 Joins and unions](../a08-joins-unions/), [A09 Deduplication and aggregates](../a09-dedup-aggregates/).

## On the job

The analysts want `gold.daily_sales_by_region` for their dashboard (order count and total sales per day and region, read many times a day, correct after late updates) and a way to slice orders ad hoc. The notebook builds the first as a **materialized view** (`SCHEDULE EVERY 1 HOUR`) and the second as a **regular view** (`gold.orders_by_region`). Then a late update to order 5002 (a Nov 2 order, 30.00 to 36.00) shows how each object reacts: the view recomputes on read (Nov 2 TX 64.50), a CREATE TABLE AS SELECT (CTAS) snapshot stays at 58.50, and the materialized view shows 64.50 after its next refresh.

## Sample data only

Fictional data. `silver.customers`: 101 Ana Lopez TX, 102 Ben Wu TX, 103 Cara Diaz CO, 104 Dev Shah OK (state = region). `silver.orders`: orders 5001 to 5005, the latest row per order at the end of Nov 3 (the A09 dedupe output). No download and no personal data.

## What the notebook writes

Cell 1 stops if a schema named `silver` or `gold` already exists in the `workspace` catalog, so nothing of yours is touched. Then it creates the two new schemas `workspace.silver` and `workspace.gold`. Inside them: tables `silver.customers`, `silver.orders` (row tracking on), views `gold.orders_by_region` and `gold.daily_sales_v`, the CTAS table `gold.daily_sales_snapshot`, and the materialized view `gold.daily_sales_by_region` (hourly schedule, backed by a serverless pipeline). The streaming table, solution and cleanup cells are commented out. **When you are done, run the cleanup cell**: it drops both schemas, the materialized view and its schedule.

## Setup on Databricks Free Edition

1. Home **New**, then **Notebook**, select **Python** and **Serverless**, or import `gold_layer_views_a10.ipynb`.
2. Run all. Cells 2 to 10 are SQL cells.

Free Edition limit: one active pipeline per pipeline type. Every standalone materialized view or streaming table is backed by a pipeline, so if another one is active, cell 7 may have to wait or fail; drop the other one first. Materialized view refreshes run on serverless compute and use your Free Edition quota (billed serverless usage on paid workspaces).

`gold_layer_views_a10.py` is the Databricks source form. The `.ipynb` is authoritative. `cheatsheet.pdf` is the two-page cheat sheet (spec `cheatsheet-spec.json`), and `script.md` is the lesson narration.

## What a successful run should show

| Check | Expected |
|---|---|
| Cell 1 (schemas) | no assertion error; `silver` and `gold` created |
| Cell 4 (gold query) | 2026-11-02 TX 2 58.50 · 2026-11-03 CO 1 13.50 · 2026-11-03 OK 1 18.00 · 2026-11-03 TX 1 40.00 |
| Cell 5 (regular view, TX) | 5001 28.50 · 5002 30.00 · 5004 40.00 |
| Cell 7 (materialized view) | created; initial load matches cell 4 (Nov 2 TX 58.50) |
| Cell 8 (UPDATE) | 1 row updated (5002 = 36.00) |
| Cell 9 (Nov 2 TX in each object) | view 64.50 · snapshot 58.50 · materialized view 58.50 until its refresh (64.50 if the hourly refresh already ran) |
| Cell 10 (refresh) | Nov 2 TX 2 64.50; the other three rows unchanged |

## Key points

- A **view** stores no data and is recomputed on every read. A **table** changes only when you write to it (a CTAS runs once).
- A **materialized view** stores results and recalculates them on each refresh (manual, `SCHEDULE EVERY` / `SCHEDULE CRON`, or `TRIGGER ON UPDATE`), incrementally when possible, so aggregates and joins stay correct after updates and deletes in the source.
- A **streaming table** processes each input row once and expects an append-only source. A streaming read of a Delta table errors on `UPDATE`, `DELETE`, `MERGE INTO` or an overwrite; `skipChangeCommits` skips the change instead of applying it.
- Memory trick: streaming tables read each row once. Materialized views recompute the answer.

## Object-choice checklist

1. Latency: must every read be fully current?
2. Refresh: who updates it, and when?
3. Late data: can old rows change? If yes and the object aggregates or joins, use a materialized view.
4. Cost: a view pays on every read; a materialized view pays at refresh and reads are cheap.

## Practice

1. After the update, `SELECT SUM(amount) FROM gold.orders_by_region WHERE region = 'TX'`: **104.50** (28.50 + 36.00 + 40.00).
2. November totals frozen at month-end close: **a table** created with CREATE TABLE AS SELECT.
3. Insert order 5006 (customer 104, Nov 4, 25.00), then refresh the materialized view: **5 rows** (new 2026-11-04 OK 1 25.00), Nov 2 TX still **64.50**.

## Cheat sheet

[cheatsheet.pdf](cheatsheet.pdf) is the two-page lesson reference. `cheatsheet-spec.json` is its content spec.

## Files

| File | What it is |
|---|---|
| `gold_layer_views_a10.ipynb` | The notebook (authoritative) |
| `gold_layer_views_a10.py` | Databricks source form of the same notebook |
| `cheatsheet.pdf` | Two-page cheat sheet |
| `cheatsheet-spec.json` | Cheat sheet content spec |
| `script.md` | Lesson narration script |

## Validation

The notebook and `.py` source compile, and the PDF has 2 pages. Every code cell was run on local Apache Spark 4.0.1 (not Databricks; American National Standards Institute (ANSI) SQL mode on) with the `workspace.` catalog prefix removed. Local Spark has no materialized views or Delta `UPDATE`, so the local run used a CTAS stand-in for the materialized view (re-created for the refresh) and rewrote `silver.orders` for the update; the view, snapshot and refreshed values matched the table above. Materialized view and streaming table behavior on Databricks is expected, from the Databricks docs. A Databricks Free Edition run has not been recorded yet.

Independent study material. Not affiliated with or endorsed by Databricks. No exam guarantee.
