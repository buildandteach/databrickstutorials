# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Checks: Expectations, Quarantine and Constraints
# MAGIC **Databricks Data Engineer Associate exam prep · Lesson A11 · Exam guide (Aug 2026) Section 3, Data Transformation and Modeling (22%)**
# MAGIC
# MAGIC Python notebook for Databricks Free Edition (serverless compute). Cells 2 to 5 and cell 10 are SQL cells. Fictional data only. No downloads or packages.
# MAGIC
# MAGIC Objective covered: apply data quality checks and validation rules to ensure reliable Silver and Gold datasets.
# MAGIC
# MAGIC On the job: the Nov 4 batch `bronze.orders_nov4` has five orders. Reject orders with a null `customer_id` or a negative amount, keep orders for unknown customers out of silver too, and quarantine the broken rows so someone can fix them. Only 5006 and 5010 should reach `silver.orders`.
# MAGIC
# MAGIC Safety: cell 1 stops if a schema named `bronze` or `silver` already exists in the `workspace` catalog, so nothing of yours is touched (if you ran Lesson A10, run its cleanup cell first). It creates those two NEW schemas and everything else lives inside them. Run the cleanup cell at the end when you are done.
# MAGIC
# MAGIC Observed vs expected: the quarantine split (cells 6-9), the profile query (cell 5) and the `NOT NULL` / `CHECK` constraint results (cells 10-11) were observed in a local Apache Spark 4.0.1 run with open-source Delta Lake 4.0.0, not on Databricks. Pipeline expectations (`EXPECT ... ON VIOLATION`) run only in a Lakeflow pipeline, so their results below are expected, based on the Databricks docs. Independent study material; not affiliated with Databricks; no exam guarantee.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1: new bronze and silver schemas
# MAGIC Stops if `workspace.bronze` or `workspace.silver` already exists.

# COMMAND ----------

names = [r[0] for r in spark.sql(
    "SHOW SCHEMAS IN workspace").collect()]
assert "bronze" not in names, "bronze exists"
assert "silver" not in names, "silver exists"

spark.sql("USE CATALOG workspace")
spark.sql("CREATE SCHEMA bronze")
spark.sql("CREATE SCHEMA silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2: silver.customers
# MAGIC Four customers, 101 to 104.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE silver.customers
# MAGIC   (customer_id INT, customer_name STRING, state STRING);
# MAGIC
# MAGIC INSERT INTO silver.customers VALUES
# MAGIC   (101, 'Ana Lopez', 'TX'), (102, 'Ben Wu', 'TX'),
# MAGIC   (103, 'Cara Diaz', 'CO'), (104, 'Dev Shah', 'OK');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3: silver.orders before Nov 4
# MAGIC Five clean orders (order 5002 already at its updated 36.00).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE silver.orders
# MAGIC   (order_id INT, customer_id INT,
# MAGIC    order_date DATE, amount DECIMAL(10,2));
# MAGIC
# MAGIC INSERT INTO silver.orders VALUES
# MAGIC   (5001, 101, DATE'2026-11-02', 28.50),
# MAGIC   (5002, 102, DATE'2026-11-02', 36.00),
# MAGIC   (5003, 103, DATE'2026-11-03', 13.50),
# MAGIC   (5004, 101, DATE'2026-11-03', 40.00),
# MAGIC   (5005, 104, DATE'2026-11-03', 18.00);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4: bronze.orders_nov4, the new batch
# MAGIC 5006 ok · 5007 null customer_id · 5008 amount -15.00 · 5009 customer 105 does not exist · 5010 ok.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE bronze.orders_nov4
# MAGIC   (order_id INT, customer_id INT,
# MAGIC    order_date DATE, amount DECIMAL(10,2));
# MAGIC
# MAGIC INSERT INTO bronze.orders_nov4 VALUES
# MAGIC   (5006, 104,  DATE'2026-11-04',  25.00),
# MAGIC   (5007, NULL, DATE'2026-11-04',  22.00),
# MAGIC   (5008, 103,  DATE'2026-11-04', -15.00),
# MAGIC   (5009, 105,  DATE'2026-11-04',  12.00),
# MAGIC   (5010, 102,  DATE'2026-11-04',  31.50);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5: measure first, the checklist as one query
# MAGIC Observed (local run): rows_in 5, null_customer 1, negative_amount 1, unknown_customer 1.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS rows_in,
# MAGIC   COUNT_IF(o.customer_id IS NULL) AS null_customer,
# MAGIC   COUNT_IF(o.amount < 0) AS negative_amount,
# MAGIC   COUNT_IF(o.customer_id IS NOT NULL
# MAGIC            AND c.customer_id IS NULL) AS unknown_customer
# MAGIC FROM bronze.orders_nov4 o
# MAGIC LEFT JOIN silver.customers c
# MAGIC   ON o.customer_id = c.customer_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline expectations: warn, drop, fail (expected pattern, not run here)
# MAGIC | Action | SQL | Python | Invalid records |
# MAGIC |---|---|---|---|
# MAGIC | warn (default) | `EXPECT (...)` | `dp.expect` | Written to the target. The violation is counted in metrics. |
# MAGIC | drop | `EXPECT (...) ON VIOLATION DROP ROW` | `dp.expect_or_drop` | Dropped before the write. The dropped count is logged. |
# MAGIC | fail | `EXPECT (...) ON VIOLATION FAIL UPDATE` | `dp.expect_or_fail` | The update fails and is rolled back. Manual intervention is needed before reprocessing. |
# MAGIC
# MAGIC Expected for the Nov 4 batch (docs): warn writes all 5 rows (5007 and 5008 too); drop writes 3 rows (5006, 5009, 5010), 2 dropped; fail writes 0 rows (the table update is rolled back). A constraint cannot contain a subquery that references another table. Metrics exist for warn and drop, not fail; for standalone streaming tables and materialized views query the pipeline event log.
# MAGIC
# MAGIC Memory trick: **Warn writes. Drop drops. Fail stops.**
# MAGIC
# MAGIC These definitions go in a pipeline source file, not in this notebook (commented out). Free Edition allows one active pipeline per pipeline type.

# COMMAND ----------

# -- pipeline source file (SQL), expected pattern
# CREATE OR REFRESH STREAMING TABLE silver.orders_validated (
#   CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
#   CONSTRAINT valid_amount      EXPECT (amount >= 0)            ON VIOLATION DROP ROW
# ) AS SELECT * FROM STREAM(bronze.orders_raw);

# COMMAND ----------

# # pipeline source file (Python), expected pattern
# from pyspark import pipelines as dp
#
# @dp.table(name="silver.orders_validated")
# @dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
# @dp.expect_or_drop("valid_amount", "amount >= 0")
# def orders_validated():
#     return spark.readStream.table("bronze.orders_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 6: the reusable filter-or-quarantine snippet
# MAGIC One reason per row (`withColumn` + `when`), the left join is the referential check, then `drop` the helper column. Nulls are checked first.

# COMMAND ----------

from pyspark.sql import functions as F

orders = spark.table("bronze.orders_nov4")
customers = (spark.table("silver.customers")
             .select("customer_id")
             .withColumn("known_customer", F.lit(True)))

checked = (orders
  .join(customers, "customer_id", "left")
  .withColumn("dq_reason",
    F.when(F.col("customer_id").isNull(),
           "null customer_id")
     .when(F.col("amount").isNull()
           | (F.col("amount") < 0), "bad amount")
     .when(F.col("known_customer").isNull(),
           "unknown customer"))
  .drop("known_customer"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7: split, and prove nothing is lost

# COMMAND ----------

clean = (checked.filter(F.col("dq_reason").isNull())
                .drop("dq_reason"))
quarantine = checked.filter(
    F.col("dq_reason").isNotNull())

assert orders.count() == (clean.count()
                          + quarantine.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 8: write both tables
# MAGIC Observed (local run): clean 5006 (104, 25.00), 5010 (102, 31.50); quarantine 5007 null customer_id, 5008 bad amount, 5009 unknown customer; prints `7 3`.

# COMMAND ----------

(clean.select("order_id", "customer_id",
              "order_date", "amount")
      .write.mode("append")
      .saveAsTable("silver.orders"))
(quarantine.write.mode("append")
      .saveAsTable("silver.orders_quarantine"))

print(spark.table("silver.orders").count(),
      spark.table("silver.orders_quarantine").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 9: the NULL pitfall
# MAGIC A filter keeps only rows where the condition is true. Observed (local run): `0 0`, the NULL row passes neither filter.

# COMMAND ----------

row = spark.createDataFrame(
    [(5011, 101, None)],
    "order_id INT, customer_id INT, amount DECIMAL(10,2)")

print(row.filter("amount >= 0").count(),
      row.filter("NOT (amount >= 0)").count())

# observed: 0 0   the NULL row passed neither filter

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 10: table constraints
# MAGIC Both commands first verify that every existing row passes. (Open-source Spark rejects `ALTER COLUMN ... SET NOT NULL`, so the local check declared NOT NULL in the schema; this is the documented Databricks syntax.)

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE silver.orders
# MAGIC   ALTER COLUMN customer_id SET NOT NULL;
# MAGIC
# MAGIC ALTER TABLE silver.orders
# MAGIC   ADD CONSTRAINT non_negative_amount
# MAGIC   CHECK (amount >= 0);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 11: two bad inserts
# MAGIC Observed (local Delta run): `[DELTA_VIOLATE_CONSTRAINT_WITH_VALUES]` then `[DELTA_NOT_NULL_CONSTRAINT_VIOLATED]`, and still 7 rows: valid order 5012 was not written either. A `CHECK` behaves like fail, not drop. Primary key and foreign key constraints on Databricks are informational only (not enforced).

# COMMAND ----------

import re
bad_inserts = [
    "(5008, 103, DATE'2026-11-04', -15.00), "
    "(5012, 101, DATE'2026-11-04', 9.00)",
    "(5007, NULL, DATE'2026-11-04', 22.00)"]
for v in bad_inserts:
    try:
        spark.sql(f"INSERT INTO silver.orders VALUES {v}")
    except Exception as e:
        print(re.search(r"\[[A-Z_]+\]", str(e)).group(0))
print(spark.table("silver.orders").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Practical takeaway: quality checklist
# MAGIC 1. **Not-null.** Required keys present: `customer_id IS NOT NULL`.
# MAGIC 2. **Range.** Values possible: `amount >= 0`, no future dates.
# MAGIC 3. **Referential.** Keys point to something real: join, then check the match (no subquery in a constraint).
# MAGIC 4. **Action.** Warn (soft problem, keep and count), drop (useless row, no review), quarantine (someone must fix it), fail (the whole batch cannot be trusted).
# MAGIC 5. **Counts.** Rows in = clean + quarantine.
# MAGIC 6. **Last guard.** `NOT NULL` and `CHECK` constraints on silver and gold.
# MAGIC
# MAGIC ## Exam-style question (original)
# MAGIC A data engineer wants every order in a pipeline's `silver_orders` table to have a matching customer in `silver.customers`. They write `CONSTRAINT known_customer EXPECT (customer_id IN (SELECT customer_id FROM silver.customers))`. What is the best fix?
# MAGIC - A) Add `ON VIOLATION DROP ROW` to the same constraint
# MAGIC - B) Declare a foreign key constraint on `silver_orders.customer_id`, because Databricks enforces it on every insert
# MAGIC - C) Left join orders to customers in the dataset query, select `c.customer_id AS matched_customer_id`, and add `EXPECT (matched_customer_id IS NOT NULL)`
# MAGIC - D) Switch the expectation to `dp.expect`, because Python expectations allow subqueries
# MAGIC
# MAGIC Answer: **C.** Expectation constraints cannot contain subqueries that reference other tables, so the lookup happens in the query first; an unknown customer becomes a null `matched_customer_id`. A keeps the invalid subquery. B: foreign keys on Databricks are informational, not enforced. D: the same rules apply in Python.
# MAGIC
# MAGIC ## Practice challenge
# MAGIC 1. With the `ON VIOLATION DROP ROW` pipeline, which bad order still reaches silver, and why?
# MAGIC 2. `silver.orders` has `CHECK (amount >= 0)`. One `INSERT` writes 5008 (-15.00) and 5012 (9.00). What is written?
# MAGIC 3. Order 5011 has a NULL amount. You split with `filter("amount >= 0")` and `filter("NOT (amount >= 0)")`. Which side does it land on?
# MAGIC
# MAGIC Solution: 1) 5009 (customer 105): no rule checks that the customer exists (expected: 3 rows land, 5006, 5009, 5010). 2) Neither row: the whole insert fails (cell 11: still 7 rows, no 5012). 3) Neither: `NULL >= 0` is NULL, so both filters skip it (cell 9: `0 0`); check `isNull()` first, like the snippet.

# COMMAND ----------

# MAGIC %md
# MAGIC Earlier lessons: A01 https://youtu.be/PglvHIK-QmU · A02 https://youtu.be/cLrBSH5DXIQ · A03 https://youtu.be/LS0gDlRD_1g · A04 https://youtu.be/AzpWro9XjJY · A05 https://youtu.be/r70MNc5Kla8 · A06 https://youtu.be/-O0mWjGb1gI · A07 https://youtu.be/daDUswtJYTM · A08 https://youtu.be/aaQKkzPOJOw · A09 https://youtu.be/5ZNyoawWl2k
# MAGIC
# MAGIC ## Optional cleanup
# MAGIC Drops only the two schemas cell 1 created, with everything inside them.

# COMMAND ----------

# spark.sql("DROP SCHEMA workspace.silver CASCADE")
# spark.sql("DROP SCHEMA workspace.bronze CASCADE")
