# A06 Choosing an ingestion method: Lakeflow Connect, Java Database Connectivity (JDBC) and REST

Lesson A06 of the **Databricks Data Engineer Associate exam prep series** for **Databricks Free Edition**.

Exam relevance: *Databricks Certified Data Engineer Associate* exam guide (August 2026), **Section 2, Data Ingestion and Loading (21%)**:

- Configure Lakeflow Connect to reliably ingest data from diverse enterprise sources into Unity Catalog governed tables.
- Use JDBC / Open Database Connectivity (ODBC) or REST clients in notebooks to land data into cloud storage or directly into Unity Catalog governed tables, usually orchestrated and scheduled with Lakeflow Jobs.
- Prioritize between Auto Loader, Lakeflow Connect (standard and managed connectors), partner connectors and other ingestion methods based on data volume, ingestion frequency, data types and governance needs.

Video: coming soon.

Previous lessons: [A02 Ingestion patterns](../a02-ingestion-patterns/), [A03 COPY INTO](../a03-copy-into/), [A04 Auto Loader schema evolution](../a04-autoloader-schema-evolution/), [A05 JSON and explode](../a05-json-explode/).

## What runs in Free Edition (and what doesn't)

| Part | Free Edition |
|---|---|
| Cells 1-5: REST client pattern (simulated paginated API, raw JSON landed in a volume, Auto Loader into a table, direct write) | Runs |
| Cells 6-8: JDBC reads from an external database (secrets, parallel reads, pushdown, JDBC Unity Catalog connection) | **Paid workspace, not runnable in Free Edition** (needs a reachable database). Guarded by `RUN_EXTERNAL = False`, so Run all skips them |
| Lakeflow Connect managed database connector (change data capture (CDC): connection, gateway, staging volume, ingestion pipeline) | **Paid workspace, not runnable in Free Edition** (needs a reachable database; the gateway runs on classic compute). Shown as UI steps and bundle YAML in markdown |

Free Edition restricts outbound internet access to a limited set of trusted domains, so the REST cells call a local `get_page()` function that returns fictional pages. The real `requests.get(...)` call is shown in a comment.

## Sample data only

Five fictional orders (`order_id`, `customer`, `amount`, `status`) returned in two pages by the simulated API. No download and no personal data.

## What the notebook writes

The setup cell reads `current_catalog()`, creates a new schema named `bt_ingest_` plus 12 hex characters, two managed volumes inside it (`landing`, raw folder `landing/api/orders/`, and `checkpoints`), and makes that schema current with `USE`. Two managed Delta tables are written: `api_orders_bronze` (Auto Loader from the landed files) and `api_orders_direct` (direct write). `CREATE SCHEMA` has no `IF NOT EXISTS`, so it fails loudly instead of reusing an object. Nothing outside the new schema is touched. The practice-challenge and cleanup cells are commented out.

## Setup on Databricks Free Edition

1. Home **New**, then **Notebook**, select **Python** and **Serverless**, or import `ingestion_methods_a06.ipynb`.
2. Run all. Leave `RUN_EXTERNAL = False`.

`ingestion_methods_a06.py` is the Databricks source form. The `.ipynb` is authoritative.

## What a successful run should show

| Check | Expected |
|---|---|
| Cell 2 (simulated API) | `2 3` (page 1 points to page 2 and has 3 orders) |
| Cell 3 (land raw) | prints `5`; `p1.json` and `p2.json` in `landing/api/orders/run_YYYYMMDDHHMMSS/` |
| Cell 4 (Auto Loader) | `api_orders_bronze` has 5 rows; columns `amount` (double), `customer`, `order_id` (long), `status`, plus `_rescued_data` (null) |
| Cell 5 (direct write) | `5` on the first run (10 if you run the cell again: duplicates trap) |
| Cells 6-8 (JDBC) | `skipped: needs a paid workspace and a reachable database` |

## Key points

- Databricks recommends starting with the most managed layer that supports your source: managed connectors, then standard connectors (Auto Loader, COPY INTO, Structured Streaming), partner connectors, then do-it-yourself code (JDBC, ODBC, REST). Lakehouse Federation queries the source in place when you don't need a copy.
- A Lakeflow Connect database connector has a connection, an ingestion gateway (classic compute, runs continuously), a staging volume (staged data purged after 30 days), an ingestion pipeline (serverless, run on a schedule by a job) and destination streaming tables. The first run is a snapshot; later runs apply changes, including deletes. SaaS connectors have no gateway or staging volume.
- JDBC: credentials from secrets or a Unity Catalog connection, never in the URL. A read is one single-threaded query unless you set `partitionColumn`, `lowerBound`, `upperBound` and `numPartitions`; `fetchSize` sets rows per round trip; `query` pushes a query to the source.
- REST: follow the pages, land raw responses in a volume (audit trail, replay), load them with Auto Loader, schedule the notebook with Lakeflow Jobs. A direct write is simpler but a rerun duplicates rows.

## Practice

1. Clickstream JSON files in S3, millions per day: **Auto Loader**.
2. Salesforce opportunities daily with minimal maintenance: **Lakeflow Connect managed SaaS connector**.
3. Internal HTTP API without a connector, hourly: **REST client in a notebook**, land raw in a volume, Auto Loader, scheduled with Lakeflow Jobs.

## Cheat sheet

[cheatsheet.pdf](cheatsheet.pdf) is the two-page lesson reference. `cheatsheet-spec.json` is its content spec.

## Files

| File | Role |
|---|---|
| `ingestion_methods_a06.ipynb` | Authoritative notebook |
| `ingestion_methods_a06.py` | Databricks source |
| [cheatsheet.pdf](cheatsheet.pdf) | Two-page reference |
| `cheatsheet-spec.json` | Content spec for the PDF |
| [script.md](script.md) | Narration script |
| `README.md` | This setup guide |

## Validation

The notebook and `.py` source compile, and the PDF has 2 pages. Cells 1-5 were dry-run on local Apache Spark (not Databricks) with stand-ins for the volumes, `dbutils` and `display`, and with `spark.read.json` in place of Auto Loader; they returned the values in the table above. A Databricks Free Edition run has not been recorded yet. The JDBC and Lakeflow Connect parts were checked against the Databricks documentation, not run.

Independent educational content; not affiliated with or endorsed by Databricks; no exam guarantee.
