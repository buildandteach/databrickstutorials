# A08 Joins and unions with PySpark and SQL

Lesson A08 of the **Databricks Data Engineer Associate exam prep series** for **Databricks Free Edition**.

Exam relevance: *Databricks Certified Data Engineer Associate* exam guide (August 2026), **Section 3, Data Transformation and Modeling (22%)**:

- Combine DataFrames with operations such as inner join, left join, broadcast join, multiple keys, cross join, union, and union all.

Video: coming soon.

Previous lessons: [A02 Ingestion patterns](../a02-ingestion-patterns/), [A03 COPY INTO](../a03-copy-into/), [A04 Auto Loader schema evolution](../a04-autoloader-schema-evolution/), [A05 JSON and explode](../a05-json-explode/), [A06 Ingestion methods](../a06-ingestion-methods/), [A07 Bronze to silver cleaning](../a07-bronze-to-silver/).

## On the job

Build `silver_order_lines`, one row per order line with the order date, customer name and product price. Orders and order lines are the large tables; customers and products are small dimensions, so they are broadcast. The customer join is a left join so an order whose customer is not loaded yet is kept (5 rows, total 84.00). An inner customer join would give 4 rows and 72.00 with no error (cell 17).

## Sample data only

Fictional retail data: `customers` (4 rows), `orders` (4 rows, order 5004 belongs to customer 105 who is not in `customers`), `products` (3 rows), `order_items` (5 rows). No download and no personal data.

## What the notebook writes

Cell 1 creates a new schema `workspace.bt_joins_` plus 12 hex characters and makes it current with `USE`. Inside it: `customers`, `orders`, `products`, `order_items` and `silver_order_lines`. Nothing outside the new schema is touched. Cell 9 tries to turn automatic broadcasting off for the session (wrapped in `try`, because serverless may not allow it) and the next cell restores the default. The solution and cleanup cells are commented out.

## Setup on Databricks Free Edition

1. Home **New**, then **Notebook**, select **Python** and **Serverless**, or import `joins_unions_a08.ipynb`.
2. Run all. Cell 15 shows two errors on purpose inside a `try` block, so Run all keeps going.

`joins_unions_a08.py` is the Databricks source form. The `.ipynb` is authoritative.

## What a successful run should show

| Check | Expected |
|---|---|
| Cell 3 (tables) | `4 4 3 5` |
| Cell 4 (inner, left, left anti) | `3 4 1`, and the left join shows order 5004 with NULL name and state |
| Cell 5 (key as expression) | `1`, then `2` |
| Cell 6 (multi-key) | `2 1` |
| Cell 7 (duplicate key) | `6` |
| Cell 8 (broadcast plan) | `BroadcastHashJoin ... LeftOuter, BuildRight` (plan text differs on Databricks) |
| Cell 9 (hint on the wrong side) | with automatic broadcasting off: `SortMergeJoin`, hint ignored (optional) |
| Cell 10 (SQL hint) | orders 5001 to 5004, NULL name for 5004 |
| Cell 11 (cross join) | `12` |
| Cell 12 (union) | `4 3` |
| Cell 13 (SQL) | `UNION 3`, `UNION ALL 4` |
| Cell 14 (column order) | `union` puts 103 in `order_id`; `unionByName` gives 5005 / 103 |
| Cell 15 (extra column) | `NUM_COLUMNS_MISMATCH` twice, then 3 rows with `channel` NULL on the first two |
| Cell 16 (silver) | `5 84.00` |
| Cell 17 (inner customer join) | `4 72.00` |
| Cell 18 (snippet) | `4 orders in, 4 out, 1 without a customer`, then `customers has 1 duplicate customer_id value(s)` |

## Key points

- `join` defaults to inner and silently drops unmatched rows. Use `left` for fact tables and `left_anti` to count orphans.
- Join on the full key and make sure the dimension key is unique, or rows multiply.
- Broadcast the small dimension on the right side. A hint on the left side of a left join is ignored. Never broadcast a large fact table.
- A cross join returns left rows times right rows.
- Memory trick: PySpark union is UNION ALL in disguise. It keeps every row and matches columns by position. Use `unionByName` to match by name.

## Join-choice checklist

1. Keys: full key, unique dimension key, same data types.
2. Join type: every fact row, `left`; only matches, `inner`; every combination on purpose, `crossJoin` after checking counts.
3. Broadcast the small dimension on the right side, never the large fact table.
4. `union` keeps duplicates and matches by position; use `unionByName` for different sources.
5. Reconcile: rows in, rows out, unmatched keys.

## Practice

1. `orders.join(customers, "customer_id", "right").count()`: **5**.
2. `customers.crossJoin(products).filter(F.col("state") == "TX").count()`: **6**.
3. `orders_resend.join(orders_mon, "order_id", "left_anti")`: **1 row** (order 5003).

## Cheat sheet

[cheatsheet.pdf](cheatsheet.pdf) is the two-page lesson reference. `cheatsheet-spec.json` is its content spec.

## Files

| File | Role |
|---|---|
| `joins_unions_a08.ipynb` | Authoritative notebook |
| `joins_unions_a08.py` | Databricks source |
| [cheatsheet.pdf](cheatsheet.pdf) | Two-page reference |
| `cheatsheet-spec.json` | Content spec for the PDF |
| [script.md](script.md) | Narration script |
| `README.md` | This setup guide |

## Validation

The notebook and `.py` source compile, and the PDF has 2 pages. Every code cell was run on local Apache Spark 4.0.1 (not Databricks; American National Standards Institute (ANSI) SQL mode on, as on serverless) with the `workspace.` catalog prefix removed; the outputs matched the table above. A Databricks Free Edition run has not been recorded yet.

Independent educational content; not affiliated with or endorsed by Databricks; no exam guarantee.
