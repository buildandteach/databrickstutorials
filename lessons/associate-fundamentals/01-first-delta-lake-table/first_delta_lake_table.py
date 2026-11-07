# Databricks notebook source
# MAGIC %md
# MAGIC # Build Your First Delta Lake Table — PySpark + SQL
# MAGIC **Build and Teach · Databricks Free Edition**
# MAGIC
# MAGIC This notebook is prepared for Henry's Free Edition workspace. It creates **only** a new tutorial schema and tables under a unique name. It does **not** drop, replace, or overwrite any preexisting tables.
# MAGIC
# MAGIC ### Sample data (fictional)
# MAGIC All rows are **clearly labeled sample / fictional** customer and order data for learning. No real personal data.
# MAGIC
# MAGIC ### Safety rules baked into this notebook
# MAGIC - Unique schema: `byte_delta_demo_20261106`
# MAGIC - Tables: `customers`, `orders` (practice) — created with `IF NOT EXISTS` under that schema only
# MAGIC - **No `DROP`**, **no `CREATE OR REPLACE`**, **no overwrite of preexisting tables**
# MAGIC - **No DBFS `/mnt`**, **no external `LOCATION`**, **no paid / cloud object-storage setup**
# MAGIC - Managed Delta tables only (default storage in Free Edition)
# MAGIC - Re-runs clear **only** rows inside this demo schema's tables via `DELETE` (not `DROP`)
# MAGIC - The pre-update Delta version is kept in the Python variable `pre_update_version` (this notebook does not set a custom Spark config)
# MAGIC
# MAGIC ### Before you run
# MAGIC Use a **Python** notebook on **Serverless**. The lesson README has the current UI steps: home **New > Notebook** (title, Python, Serverless), the import alternative, Code and Text cells, and run shortcuts.
# MAGIC
# MAGIC 1. Confirm the notebook language is **Python** and compute is **Serverless**.
# MAGIC 2. Run cells **top to bottom** in one session so `pre_update_version` exists before time travel.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0) Resolve catalog + demo namespace
# MAGIC Uses `current_catalog()` so the fully qualified names match your Free Edition workspace (often `workspace` or similar). All later cells use these variables.

# COMMAND ----------

# Resolve the Unity Catalog namespace for THIS workspace only.
# The demo schema name is unique so these tables cannot collide with your other data.
# Name parts are quoted, and only simple identifiers are accepted, so a surprising
# catalog name stops the notebook before any write.

import re

DEMO_SCHEMA = "byte_delta_demo_20261106"
CUSTOMERS_TABLE = "customers"
ORDERS_TABLE = "orders"  # practice section only

# Locked names: editing only one of these lines will not retarget the lesson.
if DEMO_SCHEMA != "byte_delta_demo_20261106" or (CUSTOMERS_TABLE, ORDERS_TABLE) != ("customers", "orders"):
    raise RuntimeError(
        "This lesson only writes byte_delta_demo_20261106.customers and byte_delta_demo_20261106.orders. "
        "Restore those names before running."
    )

_SIMPLE_IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def quote_ident(ident: str) -> str:
    """Backtick-quote one Unity Catalog name part."""
    if not isinstance(ident, str) or _SIMPLE_IDENT.fullmatch(ident) is None:
        raise ValueError(
            f"Unexpected identifier {ident!r}. "
            "This notebook only creates the named demo schema."
        )
    return f"`{ident}`"


def require_demo_table(fqn: str) -> str:
    """Return fqn only when it is one of this tutorial's two managed tables."""
    if fqn not in (CUSTOMERS_FQN, ORDERS_FQN):
        raise RuntimeError(
            "Refusing to modify a table outside "
            f"{CATALOG}.{DEMO_SCHEMA}.customers and "
            f"{CATALOG}.{DEMO_SCHEMA}.orders."
        )
    return fqn


catalog_row = spark.sql("SELECT current_catalog() AS catalog").collect()[0]
CATALOG = catalog_row["catalog"]

SCHEMA_FQN = f"{quote_ident(CATALOG)}.{quote_ident(DEMO_SCHEMA)}"
CUSTOMERS_FQN = f"{SCHEMA_FQN}.{quote_ident(CUSTOMERS_TABLE)}"
ORDERS_FQN = f"{SCHEMA_FQN}.{quote_ident(ORDERS_TABLE)}"

print("Catalog:", CATALOG)
print("Demo schema:", f"{CATALOG}.{DEMO_SCHEMA}")
print("Customers table:", f"{CATALOG}.{DEMO_SCHEMA}.{CUSTOMERS_TABLE}")
print("Orders table (practice):", f"{CATALOG}.{DEMO_SCHEMA}.{ORDERS_TABLE}")
print("Sample data: fictional customers/orders for Build and Teach tutorial only")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) Create the demo schema + managed Delta `customers` table
# MAGIC Managed table = no `LOCATION`, no `/mnt`, no external bucket. Safe for Free Edition.
# MAGIC
# MAGIC **Expected when you run:** schema and table appear under the catalog in Catalog Explorer; cell prints Ready.

# COMMAND ----------

# CREATE SCHEMA / TABLE IF NOT EXISTS — does not remove or replace preexisting objects.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_FQN}")

create_customers_sql = f"""
CREATE TABLE IF NOT EXISTS {CUSTOMERS_FQN} (
  customer_id INT,
  name STRING,
  city STRING,
  signup_date DATE,
  data_note STRING COMMENT 'Always sample/fictional for this tutorial'
)
USING DELTA
"""
spark.sql(create_customers_sql)

print(f"Ready: managed Delta table {CATALOG}.{DEMO_SCHEMA}.{CUSTOMERS_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2) Load fictional sample customers
# MAGIC `DELETE` applies **only** to `{catalog}.byte_delta_demo_20261106.customers` so re-runs stay clean without `DROP`.
# MAGIC
# MAGIC **Expected when you run:** `Inserted fictional sample rows: 3`

# COMMAND ----------

# Clear ONLY this demo table's rows (row delete only). Then insert fictional sample data.
customers = require_demo_table(CUSTOMERS_FQN)
spark.sql(f"DELETE FROM {customers}")

insert_customers_sql = f"""
INSERT INTO {customers} VALUES
  (1, 'Ada Lovelace', 'London', DATE '1815-12-10', 'SAMPLE — fictional'),
  (2, 'Alan Turing', 'Manchester', DATE '1912-06-23', 'SAMPLE — fictional'),
  (3, 'Grace Hopper', 'New York', DATE '1906-12-09', 'SAMPLE — fictional')
"""
spark.sql(insert_customers_sql)

n = spark.table(customers).count()
print(f"Inserted fictional sample rows: {n}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) Query with SQL
# MAGIC **Expected when you run:** three rows ordered by `customer_id` (Ada / Alan / Grace). Alan's city is still `Manchester`. Every `data_note` says `SAMPLE — fictional`.

# COMMAND ----------

customers = require_demo_table(CUSTOMERS_FQN)
query_all_sql = f"""
SELECT customer_id, name, city, signup_date, data_note
FROM {customers}
ORDER BY customer_id
"""
display(spark.sql(query_all_sql))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4) Same read with PySpark
# MAGIC **Expected when you run:** same three fictional rows via `spark.table`.

# COMMAND ----------

customers = require_demo_table(CUSTOMERS_FQN)
display(spark.table(customers).orderBy("customer_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5) Filter both ways (PySpark + SQL)
# MAGIC **Expected when you run:** only Ada Lovelace / London, once from PySpark and once from SQL.

# COMMAND ----------

customers = require_demo_table(CUSTOMERS_FQN)
display(spark.table(customers).filter("city = 'London'"))

filter_sql = f"""
SELECT name, city, data_note
FROM {customers}
WHERE city = 'London'
"""
display(spark.sql(filter_sql))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6) Capture the pre-update Delta version (dynamic)
# MAGIC Do **not** hard-code a version. `DELETE` and `INSERT` each add a commit, so the version number changes every time you re-run the load cell. A literal such as `VERSION AS OF 1` matches only one history.
# MAGIC
# MAGIC This cell reads `max(version)` from `DESCRIBE HISTORY` **before** the update and stores it in the Python variable `pre_update_version`. Later cells in this same notebook session read that variable. Free Edition serverless does not use a custom Spark config for this.

# COMMAND ----------

# Capture the version BEFORE the UPDATE commit.
# Python names persist across cells in this notebook session.
# Do not replace this with spark.conf.set — custom Spark configs are not how
# this lesson passes state on Free Edition serverless.
customers = require_demo_table(CUSTOMERS_FQN)
hist = spark.sql(f"DESCRIBE HISTORY {customers}")
raw_version = hist.selectExpr("max(version) AS v").collect()[0]["v"]
if raw_version is None:
    raise RuntimeError(
        "DESCRIBE HISTORY returned no version. "
        "Run the create and load cells, then run this cell again."
    )

pre_update_version = int(raw_version)

print(f"Pre-update Delta version for {CATALOG}.{DEMO_SCHEMA}.{CUSTOMERS_TABLE}: {pre_update_version}")
print("Next cell will UPDATE customer_id = 2 (Alan) city Manchester → Cambridge")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7) UPDATE a demo row (new Delta version)
# MAGIC Updates **only** the fictional Alan Turing row (`customer_id = 2` and a `SAMPLE` data note) in the demo table.
# MAGIC
# MAGIC **Expected when you run:** Alan's `city` becomes `Cambridge`. Ada and Grace stay unchanged.

# COMMAND ----------

customers = require_demo_table(CUSTOMERS_FQN)
update_sql = f"""
UPDATE {customers}
SET city = 'Cambridge'
WHERE customer_id = 2
  AND data_note LIKE 'SAMPLE%'
"""
spark.sql(update_sql)

after_sql = f"""
SELECT customer_id, name, city, data_note
FROM {customers}
ORDER BY customer_id
"""
display(spark.sql(after_sql))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8) `DESCRIBE HISTORY` + time travel to the captured pre-update version
# MAGIC **Expected when you run:** history includes an `UPDATE` operation. SQL `VERSION AS OF` and the PySpark `versionAsOf` read both still show Alan in `Manchester`. The current table (previous cell) shows `Cambridge`.

# COMMAND ----------

customers = require_demo_table(CUSTOMERS_FQN)
display(spark.sql(f"DESCRIBE HISTORY {customers}"))

try:
    captured_version = pre_update_version
except NameError as missing:
    raise RuntimeError(
        "pre_update_version is not defined in this session. "
        "Re-run from the top, or at least the capture cell, then the UPDATE cell, then this cell. "
        "Keep the version in that Python variable. Do not hard-code it and do not store it with spark.conf.set."
    ) from missing

pre_update_version = int(captured_version)
if pre_update_version < 0:
    raise RuntimeError(f"Refusing negative Delta version {pre_update_version}.")

print(f"Time traveling to VERSION AS OF {pre_update_version}")

# SQL time travel using the dynamically captured version (integer literal, not a subquery).
travel_sql = f"""
SELECT customer_id, name, city, data_note
FROM {customers} VERSION AS OF {pre_update_version}
ORDER BY customer_id
"""
display(spark.sql(travel_sql))

# Same prior version with PySpark. format("delta") applies versionAsOf on a catalog table.
df_prior = (
    spark.read
    .format("delta")
    .option("versionAsOf", str(pre_update_version))
    .table(customers)
)
display(df_prior.orderBy("customer_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9) Practice challenge — `orders` in the **same** demo schema
# MAGIC Still fictional sample data. Same safety pattern: `CREATE IF NOT EXISTS` + `DELETE` + `INSERT` on `{catalog}.byte_delta_demo_20261106.orders` only.
# MAGIC
# MAGIC **Your stretch goals after the starter cells:** update one `amount`, capture the pre-update version into a Python variable, and compare with `VERSION AS OF`.
# MAGIC
# MAGIC **Expected from the starter cells:** three order rows joined to sample customers. Ada appears twice (orders 1001 and 1003). Alan appears once (order 1002). Grace has no order, so she is absent from the join.

# COMMAND ----------

orders = require_demo_table(ORDERS_FQN)
customers = require_demo_table(CUSTOMERS_FQN)

create_orders_sql = f"""
CREATE TABLE IF NOT EXISTS {orders} (
  order_id INT,
  customer_id INT,
  amount DOUBLE,
  data_note STRING COMMENT 'Always sample/fictional for this tutorial'
)
USING DELTA
"""
spark.sql(create_orders_sql)

spark.sql(f"DELETE FROM {orders}")

# CAST: serverless notebooks run ANSI SQL. A bare 42.50 is DECIMAL, not DOUBLE.
insert_orders_sql = f"""
INSERT INTO {orders} VALUES
  (1001, 1, CAST(42.50 AS DOUBLE), 'SAMPLE — fictional'),
  (1002, 2, CAST(19.99 AS DOUBLE), 'SAMPLE — fictional'),
  (1003, 1, CAST(7.25 AS DOUBLE), 'SAMPLE — fictional')
"""
spark.sql(insert_orders_sql)

join_sql = f"""
SELECT c.name, o.order_id, o.amount, o.data_note
FROM {orders} o
JOIN {customers} c ON o.customer_id = c.customer_id
ORDER BY o.order_id
"""
display(spark.sql(join_sql))

print(f"Practice table ready: {CATALOG}.{DEMO_SCHEMA}.{ORDERS_TABLE}")
print("Stretch: UPDATE one amount, DESCRIBE HISTORY, time travel with a captured pre-update version.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notes (read-only — no execution required)
# MAGIC
# MAGIC ### Managed vs path-based / external tables
# MAGIC This lesson uses **managed Delta tables** so Free Edition works without configuring cloud storage, DBFS mounts, or `/mnt` paths.
# MAGIC
# MAGIC Path-based / external tables (for example a `LOCATION` under cloud object storage or DBFS) need extra workspace storage configuration and are **intentionally omitted from executable cells** here. Learn the managed create → insert → update → time-travel loop first; add external locations later when storage is configured.
# MAGIC
# MAGIC ### Serverless notebook state
# MAGIC `pre_update_version` is an ordinary Python variable. It remains available to later cells in the same notebook session. This lesson does not call `spark.conf.set`. Re-running the load cell writes new Delta commits, so copy a fresh `max(version)` after each load. If serverless restarts, the variable is gone and you re-run from the top.
# MAGIC
# MAGIC ### What this notebook creates
# MAGIC | Resource | Name |
# MAGIC |---|---|
# MAGIC | Schema | `{current_catalog}.byte_delta_demo_20261106` |
# MAGIC | Table | `{current_catalog}.byte_delta_demo_20261106.customers` |
# MAGIC | Table (practice) | `{current_catalog}.byte_delta_demo_20261106.orders` |
# MAGIC
# MAGIC ### What it never does
# MAGIC - No `DROP` / `DROP SCHEMA`
# MAGIC - No `CREATE OR REPLACE` of tables
# MAGIC - No writes outside `byte_delta_demo_20261106`
# MAGIC - No DBFS `/mnt`, no external `LOCATION`, no paid storage setup
# MAGIC - No custom Spark config for cross-cell state
# MAGIC - No credentials, account identifiers, or workspace URLs in the notebook
# MAGIC
# MAGIC ### Optional cleanup later (manual, not in this notebook)
# MAGIC If you want to remove **only** the demo schema after practicing, do that yourself in Catalog Explorer or with an explicit statement you type — this notebook does not run drops.
