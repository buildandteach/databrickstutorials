# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer: Views, Materialized Views and Streaming Tables
# MAGIC **Databricks Data Engineer Associate exam prep · Lesson A10 · Exam guide (Aug 2026) Section 3, Data Transformation and Modeling (22%)**
# MAGIC
# MAGIC Python notebook for Databricks Free Edition (serverless compute). Cells 2 to 10 are SQL cells. Fictional data only. No downloads or packages.
# MAGIC
# MAGIC Objective covered: understand the difference between, and how to build, Gold layer objects such as materialized views, views, streaming tables, and tables for business intelligence (BI) and analytics teams in Unity Catalog.
# MAGIC
# MAGIC On the job: build `gold.daily_sales_by_region` as a materialized view for the analyst dashboard (cell 7), and a regular view `gold.orders_by_region` for ad-hoc slices of orders (cell 5). Then a late update to order 5002 (Nov 2, 30.00 to 36.00) shows how each object reacts (cells 8 to 10).
# MAGIC
# MAGIC Safety: cell 1 stops if a schema named `silver` or `gold` already exists in the `workspace` catalog, so nothing of yours is touched. It creates those two NEW schemas and everything else lives inside them. Cell 7 creates a materialized view with an hourly schedule, which is backed by a serverless pipeline; Free Edition allows one active pipeline per pipeline type. Run the cleanup cell at the end when you are done (it drops both schemas, the materialized view and its schedule).
# MAGIC
# MAGIC Expected results in the comments: the views and the CREATE TABLE AS SELECT (CTAS) table were checked with a local Apache Spark 4.0 run of the same SQL, not on Databricks. Local Spark cannot create materialized views or streaming tables, so those results are expected, based on the Databricks docs. Independent study material; not affiliated with Databricks; no exam guarantee.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1: new silver and gold schemas
# MAGIC Stops if `workspace.silver` or `workspace.gold` already exists.

# COMMAND ----------

names = [r[0] for r in spark.sql(
    "SHOW SCHEMAS IN workspace").collect()]
assert "silver" not in names and "gold" not in names, \
    "silver or gold exists: use a fresh workspace"

spark.sql("USE CATALOG workspace")
spark.sql("CREATE SCHEMA silver")
spark.sql("CREATE SCHEMA gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2: silver.customers
# MAGIC Four customers. In this small dataset, `state` is the region.

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
# MAGIC ## Cell 3: silver.orders
# MAGIC The latest row per order (the Lesson A09 dedupe), end of Nov 3. Row tracking lets a materialized view refresh incrementally.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE silver.orders
# MAGIC   (order_id INT, customer_id INT,
# MAGIC    order_date DATE, amount DECIMAL(10,2))
# MAGIC TBLPROPERTIES ('delta.enableRowTracking' = 'true');
# MAGIC
# MAGIC INSERT INTO silver.orders VALUES
# MAGIC   (5001, 101, DATE'2026-11-02', 28.50),
# MAGIC   (5002, 102, DATE'2026-11-02', 30.00),
# MAGIC   (5003, 103, DATE'2026-11-03', 13.50),
# MAGIC   (5004, 101, DATE'2026-11-03', 40.00),
# MAGIC   (5005, 104, DATE'2026-11-03', 18.00);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4: the gold query
# MAGIC Expected: 2026-11-02 TX 2 58.50 · 2026-11-03 CO 1 13.50 · 2026-11-03 OK 1 18.00 · 2026-11-03 TX 1 40.00.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT o.order_date,
# MAGIC        c.state       AS region,
# MAGIC        COUNT(*)      AS order_count,
# MAGIC        SUM(o.amount) AS total_sales
# MAGIC FROM silver.orders o
# MAGIC JOIN silver.customers c ON o.customer_id = c.customer_id
# MAGIC GROUP BY o.order_date, c.state
# MAGIC ORDER BY o.order_date, region;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5: a regular view for ad-hoc slices
# MAGIC No stored data; every query re-runs the join. Expected Texas orders: 5001 28.50, 5002 30.00, 5004 40.00.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.orders_by_region AS
# MAGIC SELECT o.order_id, o.order_date,
# MAGIC        c.state AS region, o.amount
# MAGIC FROM silver.orders o
# MAGIC JOIN silver.customers c ON o.customer_id = c.customer_id;
# MAGIC
# MAGIC SELECT * FROM gold.orders_by_region
# MAGIC WHERE region = 'TX' ORDER BY order_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 6: the same aggregate as a view and as a CTAS table
# MAGIC The view recomputes on every read. The CTAS table stores the result once (Nov 3).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.daily_sales_v AS
# MAGIC SELECT o.order_date, c.state AS region,
# MAGIC        COUNT(*) AS order_count,
# MAGIC        SUM(o.amount) AS total_sales
# MAGIC FROM silver.orders o
# MAGIC JOIN silver.customers c ON o.customer_id = c.customer_id
# MAGIC GROUP BY o.order_date, c.state;
# MAGIC
# MAGIC CREATE TABLE gold.daily_sales_snapshot AS
# MAGIC SELECT * FROM gold.daily_sales_v;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7: a materialized view for the dashboard
# MAGIC The initial load runs right away (expected Nov 2 TX 58.50), then it refreshes every hour. Works from a SQL warehouse or a notebook on serverless compute. A serverless pipeline processes the refreshes.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE MATERIALIZED VIEW
# MAGIC   gold.daily_sales_by_region
# MAGIC   SCHEDULE EVERY 1 HOUR
# MAGIC AS SELECT o.order_date,
# MAGIC           c.state       AS region,
# MAGIC           COUNT(*)      AS order_count,
# MAGIC           SUM(o.amount) AS total_sales
# MAGIC FROM silver.orders o
# MAGIC JOIN silver.customers c ON o.customer_id = c.customer_id
# MAGIC GROUP BY o.order_date, c.state;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 8: Nov 4, the late update arrives
# MAGIC An update to an existing row, not a new row. Correct Nov 2 TX total: 28.50 + 36.00 = 64.50.

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE silver.orders
# MAGIC SET amount = 36.00
# MAGIC WHERE order_id = 5002;
# MAGIC
# MAGIC -- order 5002 is still a Nov 2 order
# MAGIC -- correct Nov 2 TX total: 28.50 + 36.00 = 64.50

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 9: Nov 2 TX in each object
# MAGIC Expected: view 64.50 (recomputed on read), snapshot 58.50 (stored once), materialized view 58.50 until its refresh (64.50 if the hourly refresh already ran).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT obj, total_sales FROM (
# MAGIC   SELECT 'view' AS obj, * FROM gold.daily_sales_v
# MAGIC   UNION ALL
# MAGIC   SELECT 'snapshot', * FROM gold.daily_sales_snapshot
# MAGIC   UNION ALL
# MAGIC   SELECT 'materialized view', *
# MAGIC   FROM gold.daily_sales_by_region)
# MAGIC WHERE order_date = '2026-11-02' AND region = 'TX';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 10: refresh the materialized view
# MAGIC Expected: 2026-11-02 TX 2 64.50; the other three rows unchanged. Each refresh recalculates results from the current source tables, including updates and deletes.

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH MATERIALIZED VIEW gold.daily_sales_by_region;
# MAGIC
# MAGIC SELECT * FROM gold.daily_sales_by_region
# MAGIC ORDER BY order_date, region;

# COMMAND ----------

# MAGIC %md
# MAGIC ## The trap: a streaming table for the daily aggregate
# MAGIC A streaming table processes each input row once and expects an append-only source. A streaming read of a Delta table throws an error when the source is modified by `UPDATE`, `DELETE`, `MERGE INTO` or an overwrite; with `skipChangeCommits` the change is skipped, not applied. Either way Nov 2 TX stays wrong until a full refresh. Use a materialized view when the query aggregates or joins, or when the source can change.
# MAGIC
# MAGIC Memory trick: streaming tables read each row once. Materialized views recompute the answer.
# MAGIC
# MAGIC ## Where a streaming table fits (expected pattern, not run)
# MAGIC Bronze ingestion of files that only grow. Commented out: it needs a volume with JSON files, and Free Edition allows one active pipeline per pipeline type.

# COMMAND ----------

# %sql
# CREATE OR REFRESH STREAMING TABLE bronze.orders_raw
# AS SELECT *
# FROM STREAM read_files(
#   '/Volumes/workspace/retail/landing/orders/',
#   format => 'json');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Practical takeaway: object-choice checklist
# MAGIC 1. **Latency.** Must every read be fully current? A view is always current. A materialized view is current as of its last refresh. A streaming table has low latency for new rows.
# MAGIC 2. **Refresh.** Who updates it? A view needs no refresh. A materialized view refreshes manually, on a schedule (`SCHEDULE EVERY 1 HOUR`, `SCHEDULE CRON`) or on source updates (`TRIGGER ON UPDATE`). A streaming table processes new input on each update. A table changes only when you write to it.
# MAGIC 3. **Late data.** Can old rows change? If yes, and the object aggregates or joins, choose a materialized view.
# MAGIC 4. **Cost.** A view pays the full query on every read. A materialized view pays at refresh time, often incrementally, and reads are cheap.
# MAGIC
# MAGIC Rule of thumb: streaming tables for append-only ingestion into bronze, materialized views for gold aggregates that must stay correct, regular views for light ad-hoc slices, tables for frozen snapshots.
# MAGIC
# MAGIC ## Reusable snippet (one of each)
# MAGIC ```sql
# MAGIC -- bronze: append-only files, each row once
# MAGIC CREATE OR REFRESH STREAMING TABLE bronze.orders_raw
# MAGIC AS SELECT * FROM STREAM read_files('/Volumes/workspace/retail/landing/orders/', format => 'json');
# MAGIC
# MAGIC -- gold: aggregate that must reflect late updates
# MAGIC CREATE OR REPLACE MATERIALIZED VIEW gold.daily_sales_by_region
# MAGIC   SCHEDULE EVERY 1 HOUR
# MAGIC AS SELECT o.order_date, c.state AS region,
# MAGIC           COUNT(*) AS order_count, SUM(o.amount) AS total_sales
# MAGIC FROM silver.orders o JOIN silver.customers c ON o.customer_id = c.customer_id
# MAGIC GROUP BY o.order_date, c.state;
# MAGIC
# MAGIC -- gold: light ad-hoc slice, always current, no storage
# MAGIC CREATE OR REPLACE VIEW gold.orders_by_region AS
# MAGIC SELECT o.order_id, o.order_date, c.state AS region, o.amount
# MAGIC FROM silver.orders o JOIN silver.customers c ON o.customer_id = c.customer_id;
# MAGIC ```
# MAGIC
# MAGIC ## Exam-style question (original)
# MAGIC New JSON order files land in a Unity Catalog volume every few minutes. Files never change after they land, and each file must be processed only once, with low latency. Which object fits best for the bronze table?
# MAGIC - A) A regular view over the volume
# MAGIC - B) A materialized view that refreshes every hour
# MAGIC - C) A streaming table that reads the files with `STREAM read_files`
# MAGIC - D) A table created once with CREATE TABLE AS SELECT
# MAGIC
# MAGIC Answer: **C.** The source only grows, each row is handled once, and latency matters. A stores nothing and re-reads every file on each query. B adds latency and is meant for aggregations, joins and changing sources. D is a one-time snapshot that misses later files.
# MAGIC
# MAGIC ## Practice challenge
# MAGIC 1. After the Nov 4 update: `SELECT SUM(amount) FROM gold.orders_by_region WHERE region = 'TX'`
# MAGIC 2. Finance wants November totals frozen at month-end close, never changed by later corrections. Which object?
# MAGIC 3. Order 5006 (customer 104, Nov 4, 25.00) is inserted, then the materialized view refreshes. How many rows does it have, and what is Nov 2 TX?
# MAGIC
# MAGIC Solution (uncomment to check). Q2 has no code: a table created with CREATE TABLE AS SELECT, like `gold.daily_sales_snapshot`.

# COMMAND ----------

# spark.sql("SELECT SUM(amount) FROM gold.orders_by_region WHERE region = 'TX'").show()   # Q1: 104.50 (28.50 + 36.00 + 40.00)
# spark.sql("INSERT INTO silver.orders VALUES (5006, 104, DATE'2026-11-04', 25.00)")
# spark.sql("REFRESH MATERIALIZED VIEW gold.daily_sales_by_region")
# spark.sql("SELECT * FROM gold.daily_sales_by_region ORDER BY order_date, region").show()   # Q3: 5 rows; new 2026-11-04 OK 1 25.00; Nov 2 TX still 64.50

# COMMAND ----------

# MAGIC %md
# MAGIC Earlier lessons: A01 https://youtu.be/PglvHIK-QmU · A02 https://youtu.be/cLrBSH5DXIQ · A03 https://youtu.be/LS0gDlRD_1g · A04 https://youtu.be/AzpWro9XjJY · A05 https://youtu.be/r70MNc5Kla8 · A06 https://youtu.be/-O0mWjGb1gI · A07 https://youtu.be/daDUswtJYTM · A08 https://youtu.be/aaQKkzPOJOw · A09 https://youtu.be/5ZNyoawWl2k
# MAGIC
# MAGIC ## Optional cleanup
# MAGIC Drops only the two schemas cell 1 created, with everything inside them (views, tables, the materialized view and its hourly schedule).

# COMMAND ----------

# spark.sql("DROP SCHEMA workspace.gold CASCADE")
# spark.sql("DROP SCHEMA workspace.silver CASCADE")
