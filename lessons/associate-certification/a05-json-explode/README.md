# A05 JSON and nested data with explode

Lesson A05 of the **Databricks Data Engineer Associate exam prep series** for **Databricks Free Edition**.

Exam relevance: *Databricks Certified Data Engineer Associate* exam guide (August 2026):

- **Section 2, Data Ingestion and Loading (21%)**: ingest semi-structured data (for example, JSON and nested data) into Unity Catalog governed Delta tables.
- **Section 3, Data Transformation and Modeling (22%)**: manipulate columns, rows and table structures, including exploding arrays.

Video: coming soon.

Previous lessons: [A02 Ingestion patterns](../a02-ingestion-patterns/), [A03 COPY INTO](../a03-copy-into/), [A04 Auto Loader schema evolution](../a04-autoloader-schema-evolution/).

## Sample data only

Four fictional orders written by the notebook itself as one small JSON Lines file (`orders.json`). Each order has a `customer` struct with a nested `address` struct, an `items` array of structs (`sku`, `qty`, `price`) and a `tags` array of strings. Order 1003 has an empty `items` array and order 1004 has no `items` field (null). No download and no personal data.

## What the notebook writes

The setup cell reads `current_catalog()`, creates a new schema named `bt_json_` plus 12 hex characters, one managed volume inside it (`landing`, source folder `landing/orders/`), and makes that schema current with `USE`. Two managed Delta tables are written: `orders_bronze` (nested columns kept as they are) and `order_items` (flat silver table, one row per item). A temporary view `raw_orders` is used for the SQL colon syntax cell. `CREATE SCHEMA` has no `IF NOT EXISTS`, so it fails loudly instead of reusing an object. Nothing outside the new schema is touched. The practice-challenge and cleanup cells are commented out.

## Setup on Databricks Free Edition

Use a **Python** notebook on **Serverless**. Run the code cells from the top.

1. Home **New**, then **Notebook**, select **Python** and **Serverless**, or import `json_explode_a05.ipynb`.
2. Run all.

`json_explode_a05.py` is the Databricks source form. The `.ipynb` is authoritative. No paid-only features are used.

## What a successful run should show

| Check | Expected |
|---|---|
| Cell 3 (inferred schema) | `customer` struct (`address` struct, `name`), `items` array of struct (`price: double`, `qty: long`, `sku: string`), `order_id: long`, `tags` array of string |
| Cell 4 (explicit schema) | `orders_bronze` has 4 rows; `order_id: int`, `price: decimal(10,2)` |
| Cell 5 (dot notation) | 4 rows: 1001 ana Austin, 1002 ben Dallas, 1003 cara Toronto, 1004 dev Austin |
| Cell 6 (`explode`) | 3 rows (1001 twice, 1002 once); 1003 and 1004 dropped |
| Cell 7 (`explode_outer`, `posexplode`) | 5 rows; `pos` 0 and 1 for 1001, 0 for 1002 |
| Cell 8 (silver) | `order_items` 3 rows; line totals 10.00, 12.50, 12.00 |
| Cell 9 (`from_json`) | 4 rows with `order_id` and `city` from the parsed struct |
| Cell 10 (colon syntax) | for 1001: `1001`, `Austin`, `A1`, `["A1","B2"]` (strings, `order_id` cast with `::int`) |
| Cell 11 (SQL `LATERAL explode`) | 3 rows; the `explode_outer` query returns 5 |

## Key points

- The JSON reader expects one record per line by default; a pretty-printed file needs `multiLine` set to `true`.
- Inference turns whole numbers into `long` and decimals into `double`. An explicit schema (a DDL string with `STRUCT` and `ARRAY` types) skips the inference pass and uses your types. Auto Loader (lesson A04) infers JSON columns as strings unless `cloudFiles.inferColumnTypes` is `true`.
- Structs: dot notation (`customer.address.city`) and `customer.*`. The row count never changes.
- Arrays: `explode` gives one row per element and drops null or empty arrays; `explode_outer` keeps them with a null element; `posexplode` returns `pos` (starting at 0) and `col`. On a map, `explode` returns `key` and `value`.
- `from_json(column, schema)` turns a JSON string into a struct; `schema_of_json` derives a schema from a sample.
- Databricks SQL colon syntax (`value:customer.address.city`, `value:items[0].sku`, `value:items[*].sku`) works on JSON strings and returns strings; cast with `::`. In SQL, call `explode` as a table reference with `LATERAL`; the SELECT-list and LATERAL VIEW forms are deprecated.

## Practice

Tags: 1001 web and promo, 1002 store, 1003 empty, 1004 web. `explode("tags")` returns 4 rows (2 + 1 + 0 + 1). `explode_outer("tags")` returns 5 rows (1003 kept with a null tag). With `posexplode("tags")`, `promo` is position 1 (`web` is 0).

## Cheat sheet

[cheatsheet.pdf](cheatsheet.pdf) is the two-page lesson reference. `cheatsheet-spec.json` is its content spec.

## Files

| File | Role |
|---|---|
| `json_explode_a05.ipynb` | Authoritative notebook |
| `json_explode_a05.py` | Databricks source |
| [cheatsheet.pdf](cheatsheet.pdf) | Two-page reference |
| `cheatsheet-spec.json` | Content spec for the PDF |
| [script.md](script.md) | Narration script |
| `README.md` | This setup guide |

## Validation

The notebook and `.py` source compile, and the PDF has 2 pages. The code cells (except the Databricks SQL colon-syntax cell, which is Databricks-only) were dry-run on local Apache Spark 4.0.1, not Databricks, with stand-ins for the volume, `dbutils` and `display`; they returned the counts and values in the table above. A Databricks Free Edition run has not been recorded yet.

Independent educational content; not affiliated with or endorsed by Databricks; no exam guarantee.
