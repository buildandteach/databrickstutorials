# A11 Data quality checks: expectations, quarantine and constraints

Lesson A11 of the **Databricks Data Engineer Associate exam prep series** for **Databricks Free Edition**.

Exam relevance: *Databricks Certified Data Engineer Associate* exam guide (August 2026), **Section 3, Data Transformation and Modeling (22%)**:

- Apply data quality checks and validation rules to ensure reliable Silver and Gold datasets.

Video: coming soon.

Previous lessons: [A07 Bronze to silver cleaning](../a07-bronze-to-silver/), [A08 Joins and unions](../a08-joins-unions/), [A09 Deduplication and aggregates](../a09-dedup-aggregates/), [A10 Gold layer views](../a10-gold-layer-views/).

## On the job

On Nov 4 a new batch, `bronze.orders_nov4`, lands with five orders: 5006 ok, 5007 null `customer_id`, 5008 amount -15.00, 5009 for customer 105 (does not exist), 5010 ok. The rule: reject orders with a null `customer_id` or a negative amount, keep orders for unknown customers out of silver too, and keep the broken rows so someone can fix them. Only 5006 and 5010 should reach `silver.orders`.

## Sample data only

Fictional data. `silver.customers`: 101 Ana Lopez TX, 102 Ben Wu TX, 103 Cara Diaz CO, 104 Dev Shah OK. `silver.orders`: orders 5001 to 5005 (Nov 2 and Nov 3, order 5002 at its updated 36.00). No download and no personal data.

## What the notebook writes

Cell 1 stops if a schema named `bronze` or `silver` already exists in the `workspace` catalog, so nothing of yours is touched (if you ran A10, run its cleanup cell first). Then it creates the two new schemas `workspace.bronze` and `workspace.silver`. Inside them: `silver.customers`, `silver.orders`, `bronze.orders_nov4` and `silver.orders_quarantine`, plus a `NOT NULL` and a `CHECK` constraint on `silver.orders`. The pipeline expectation code (SQL and Python), the solution and the cleanup cell are commented out. **When you are done, run the cleanup cell**: it drops both schemas.

## Setup on Databricks Free Edition

1. Home **New**, then **Notebook**, select **Python** and **Serverless**, or import `data_quality_checks_a11.ipynb`.
2. Run all. Cells 2 to 5 and cell 10 are SQL cells.

Expectations (`EXPECT ... ON VIOLATION`) are a Lakeflow pipeline feature and go in a pipeline source file, not a notebook cell. Serverless pipelines run on the Advanced product edition, which includes expectations. Free Edition allows one active pipeline per pipeline type.

`data_quality_checks_a11.py` is the Databricks source form. The `.ipynb` is authoritative. `cheatsheet.pdf` is the two-page cheat sheet (spec `cheatsheet-spec.json`), and `script.md` is the lesson narration.

## What a successful run should show

| Check | Result |
|---|---|
| Cell 5 (profile) | rows_in 5 · null_customer 1 · negative_amount 1 · unknown_customer 1 (observed, local run) |
| Cells 6-8 (quarantine) | clean 5006, 5010 · quarantine 5007 null customer_id, 5008 bad amount, 5009 unknown customer · prints `7 3` (observed) |
| Cell 9 (NULL pitfall) | `0 0`: the NULL amount passes neither filter (observed) |
| Cell 11 (bad inserts) | `[DELTA_VIOLATE_CONSTRAINT_WITH_VALUES]`, `[DELTA_NOT_NULL_CONSTRAINT_VIOLATED]`, `7`: valid order 5012 not written either (observed) |
| Pipeline, warn / drop / fail | 5 rows written / 3 rows (5006, 5009, 5010), 2 dropped / 0 rows, update rolled back (expected, from the docs) |

## Expectations: warn, drop, fail

| Action | SQL | Python | Invalid records |
|---|---|---|---|
| warn (default) | `EXPECT (...)` | `dp.expect` | Written to the target. The violation is counted in metrics. |
| drop | `EXPECT (...) ON VIOLATION DROP ROW` | `dp.expect_or_drop` | Dropped before the write. The dropped count is logged. |
| fail | `EXPECT (...) ON VIOLATION FAIL UPDATE` | `dp.expect_or_fail` | The update fails and is rolled back. |

Memory trick: **Warn writes. Drop drops. Fail stops.**

## Quality checklist

1. Not-null: required keys present.
2. Range: values possible (`amount >= 0`, no future dates).
3. Referential: join, then check the match (a constraint cannot use a subquery on another table).
4. Action: warn, drop, quarantine or fail.
5. Counts: rows in = clean + quarantine.
6. Last guard: `NOT NULL` and `CHECK` constraints on silver and gold (primary and foreign keys are informational only).

## Practice

1. With the `ON VIOLATION DROP ROW` pipeline, which bad order still reaches silver? **5009** (customer 105): no rule checks that the customer exists (expected: 5006, 5009, 5010 land).
2. `CHECK (amount >= 0)`, one `INSERT` of 5008 (-15.00) and 5012 (9.00): **neither row**, the whole insert fails (observed: still 7 rows).
3. NULL amount 5011 split with `filter("amount >= 0")` / `filter("NOT (amount >= 0)")`: **neither side** (observed `0 0`); the snippet quarantines it as `bad amount`.

## Cheat sheet

[cheatsheet.pdf](cheatsheet.pdf) is the two-page lesson reference. `cheatsheet-spec.json` is its content spec.

## Files

| File | What it is |
|---|---|
| `data_quality_checks_a11.ipynb` | The notebook (authoritative) |
| `data_quality_checks_a11.py` | Databricks source form of the same notebook |
| `cheatsheet.pdf` | Two-page cheat sheet |
| `cheatsheet-spec.json` | Cheat sheet content spec |
| `script.md` | Lesson narration script |

## Validation

The notebook and `.py` source compile, and the PDF has 2 pages. Every code cell was run on local Apache Spark 4.0.1 with open-source Delta Lake 4.0.0 (not Databricks; American National Standards Institute (ANSI) SQL mode on) with the `workspace` catalog steps skipped. Open-source Spark rejects `ALTER COLUMN ... SET NOT NULL`, so the local run declared `customer_id NOT NULL` in the table schema instead; all other results matched the table above. Pipeline expectation behavior is expected, from the Databricks docs. A Databricks Free Edition run has not been recorded yet.

Independent study material. Not affiliated with or endorsed by Databricks. No exam guarantee.
