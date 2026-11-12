# A02 Ingestion patterns: batch, streaming and incremental

Lesson 2 of the **Databricks Data Engineer Associate series** for **Databricks Free Edition**.

Exam relevance: *Databricks Certified Data Engineer Associate* exam guide (August 2026), **Section 2, Data Ingestion and Loading (21%)**:

- Enable and detail data ingestion patterns, including batch, streaming and incremental loading, and import data from sources such as local files, Lakeflow Connect standard connectors and Lakeflow Connect managed connectors.

Video: [youtu.be/cLrBSH5DXIQ](https://youtu.be/cLrBSH5DXIQ)

Previous lesson: [A01 Platform tour](../a01-platform-tour/). Related: [Lesson 04, Auto Loader ingestion](../../associate-fundamentals/04-autoloader-ingestion/) in the fundamentals series.

## Sample data only

Five fictional orders written by the notebook itself as two small comma-separated values (CSV) files (`orders_day1.csv`, `orders_day2.csv`). No download and no personal data.

## What the notebook writes

The setup cell reads `current_catalog()` and creates a new schema named `bt_ingest_` plus 12 hex characters, and a managed volume `landing` inside it. It writes two small CSV files into `landing/orders/` and creates two managed Delta tables in that schema: `orders_batch` (overwritten by the batch load, which this notebook created) and `orders_bronze` (appended by Auto Loader). Auto Loader's checkpoint lives in `landing/_checkpoints/`. `CREATE SCHEMA` has no `IF NOT EXISTS`, so it fails loudly instead of reusing an object. Nothing outside the new schema is touched, and nothing is dropped. A full rerun creates another schema. The practice-challenge cell is commented out.

## Setup on Databricks Free Edition

Use a **Python** notebook on **Serverless**. Run the code cells from the top.

1. Home **New**, then **Notebook**, select **Python** and **Serverless**, or import `ingestion_patterns_a02.ipynb`.
2. Run all. Optional: open **Catalog**, then **workspace**, then **bt_ingest_...**, then **Volumes**, then **landing**, then **orders** to see the two files.

`ingestion_patterns_a02.py` is the Databricks source form. The `.ipynb` is authoritative.

## What a successful run shows

| Check | Expected |
|---|---|
| Cell 3 (batch) | `orders_batch rows: 3` |
| Cell 4 (Auto Loader) | `orders_bronze rows: 3` |
| Cell 5 (day 2) | `orders_batch rows: 5`, `Auto Loader read: 2`, `orders_bronze rows: 5` |
| Cell 6 | `orders_day1.csv` 3, `orders_day2.csv` 2 |

## Streaming on serverless

Serverless notebooks and jobs support the `availableNow` trigger (and the deprecated `once`). `processingTime` and continuous triggers raise `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`. For always-on streaming, use a pipeline in continuous mode or an `availableNow` query in a continuous job.

## Lakeflow Connect (concepts)

- **Standard connectors**: Spark and Structured Query Language (SQL) application programming interfaces (APIs) such as Auto Loader, `COPY INTO` and Structured Streaming (for example Kafka). More control; you maintain the logic. This notebook uses this pattern.
- **Managed connectors**: a connection plus an ingestion pipeline configured in the workspace or API for sources like Salesforce, Workday, SharePoint or SQL Server. They need a real source system and credentials; database change data capture (CDC) connectors are paid-workspace features. Explained as concepts.
- **Local files**: **New**, then **Add or upload data**, then **Create or modify table** for a one-time load, or upload files to a volume and read them with code.

## Practice

A day 3 file with 4 new orders lands and both loads run again. Answer: `orders_batch` has 9 rows; the Auto Loader run reads 4 and bronze has 9; if all three batch runs had appended, `orders_batch` would hold 3 + 5 + 9 = 17 rows with duplicates.

## Cheat sheet

[cheatsheet.pdf](cheatsheet.pdf) is the two-page lesson reference. `cheatsheet-spec.json` is its content spec.

## Files

| File | Role |
|---|---|
| `ingestion_patterns_a02.ipynb` | Authoritative notebook |
| `ingestion_patterns_a02.py` | Databricks source |
| [cheatsheet.pdf](cheatsheet.pdf) | Two-page reference |
| `cheatsheet-spec.json` | Content spec for the PDF |
| [script.md](script.md) | Narration script |
| `README.md` | This setup guide |

## Validation

The notebook and `.py` source compile, and the PDF has 2 pages. A Databricks Free Edition run was recorded on Nov 12, 2026, in schema `workspace.bt_ingest_49cc33eb5a03`. Observed results matched the table above: the batch load wrote 3 rows, then 5 after day 2; Auto Loader read 2 new rows; bronze held 5 rows; `orders_day1.csv` contributed 3 rows and `orders_day2.csv` contributed 2.

Independent educational content; not affiliated with or endorsed by Databricks; no exam guarantee.
