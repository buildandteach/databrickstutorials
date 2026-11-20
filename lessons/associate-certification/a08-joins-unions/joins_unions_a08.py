# Databricks notebook source
# MAGIC %md
# MAGIC # Joins and Unions with PySpark and SQL
# MAGIC **Databricks Data Engineer Associate exam prep · Lesson A08 · Exam guide (Aug 2026) Section 3, Data Transformation and Modeling (22%)**
# MAGIC
# MAGIC Python notebook for Databricks Free Edition (serverless compute). Fictional retail data only. No downloads or packages.
# MAGIC
# MAGIC Objective covered: combine DataFrames with operations such as inner join, left join, broadcast join, multiple keys, cross join, union, and union all.
# MAGIC
# MAGIC On the job: build a silver `silver_order_lines` table, one row per order line with the customer name and product price, without losing orders whose customer is not loaded yet (cell 16).
# MAGIC
# MAGIC Safety: the notebook creates one NEW schema `bt_joins_` + 12 hex characters in the `workspace` catalog, and the tables `customers`, `orders`, `products`, `order_items` and `silver_order_lines` inside that schema only. Cell 9 tries to turn automatic broadcasting off for the session; it is wrapped in `try`, and the cell after it restores the default. Expected results in the comments were checked with a local Apache Spark 4.0 run of the same code, not on Databricks (plan text will differ on Databricks). Independent study material; not affiliated with Databricks; no exam guarantee.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1: a fresh schema

# COMMAND ----------

import uuid
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

schema = "bt_joins_" + uuid.uuid4().hex[:12]
spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS workspace.{schema}")
spark.sql(f"USE workspace.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cells 2 and 3: the retail tables
# MAGIC Four small managed tables. Order 5004 belongs to customer 105, who is not in `customers` yet. Expected: customers 4 rows, orders 4, products 3, order_items 5.

# COMMAND ----------

customers = spark.createDataFrame([
  (101, "Ana Lopez", "TX"), (102, "Ben Wu", "TX"),
  (103, "Cara Diaz", "CO"), (104, "Dev Shah", "OK")],
  "customer_id int, customer_name string, state string")
orders = spark.createDataFrame([
  (5001, 101, "2026-11-02"), (5002, 102, "2026-11-03"),
  (5003, 101, "2026-11-04"), (5004, 105, "2026-11-05")],
  "order_id int, customer_id int, order_date string"
  ).withColumn("order_date", F.to_date("order_date"))

for name, df in [("customers", customers),
                 ("orders", orders)]:
    df.write.mode("overwrite").saveAsTable(name)

# COMMAND ----------

from decimal import Decimal

products = spark.createDataFrame([
  ("P1", "Coffee mug", Decimal("12.00")),
  ("P2", "Notebook", Decimal("4.50")),
  ("P3", "Desk lamp", Decimal("30.00"))],
  "product_id string, product_name string, price decimal(10,2)")

order_items = spark.createDataFrame([
  (5001, 1, "P1", 2), (5001, 2, "P2", 1),
  (5002, 1, "P3", 1), (5003, 1, "P2", 3),
  (5004, 1, "P1", 1)],
  "order_id int, line_no int, product_id string, quantity int")

for name, df in [("products", products),
                 ("order_items", order_items)]:
    df.write.mode("overwrite").saveAsTable(name)

customers   = spark.read.table("customers")
orders      = spark.read.table("orders")
order_items = spark.read.table("order_items")
products    = spark.read.table("products")

print(customers.count(), orders.count(), products.count(), order_items.count())   # 4 4 3 5

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4: inner, left and left anti
# MAGIC `join` defaults to inner and silently drops unmatched rows. Expected: `3 4 1`, and the left join shows order 5004 with NULL `customer_name` and `state`.

# COMMAND ----------

inner = orders.join(customers, "customer_id")
left = orders.join(customers, "customer_id", "left")
anti = orders.join(customers, "customer_id", "left_anti")

print(inner.count(), left.count(), anti.count())
# 3 4 1
left.orderBy("order_id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5: key as a name vs an expression
# MAGIC A string or list of strings keeps one key column. An expression keeps both, so a later `select("customer_id")` is ambiguous. Expected: `1`, then `2`.

# COMMAND ----------

by_name = orders.join(customers, "customer_id")
by_expr = orders.join(
    customers,
    orders.customer_id == customers.customer_id)

print(by_name.columns.count("customer_id"))   # 1
print(by_expr.columns.count("customer_id"))   # 2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 6: join on the full key
# MAGIC `order_items` has a two-part key. Expected: `2 1`.

# COMMAND ----------

returns = spark.createDataFrame(
    [(5001, 2, "damaged")],
    "order_id int, line_no int, reason string")

k1 = order_items.join(returns, "order_id")
k2 = order_items.join(
    returns, ["order_id", "line_no"])
print(k1.count(), k2.count())   # 2 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7: a duplicate dimension key
# MAGIC Customer 101 twice doubles both of its orders. Expected: `6`, not `4`.

# COMMAND ----------

dup = customers.union(spark.createDataFrame(
    [(101, "Ana Lopez", "TX")], customers.schema))

joined = orders.join(dup, "customer_id", "left")
print(joined.count())   # 6, not 4

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 8: broadcast the small side
# MAGIC Expected plan: `BroadcastHashJoin ... LeftOuter, BuildRight` (customers, the right side, is copied). Plan text differs on Databricks.

# COMMAND ----------

bc = orders.join(
    broadcast(customers), "customer_id", "left")
bc.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 9: trap, the hint on the wrong side (optional)
# MAGIC A left outer join cannot broadcast its left side. With automatic broadcasting off, the hint on `orders` is ignored and the plan is a `SortMergeJoin` (local check). Serverless may not allow the setting; the `try` keeps Run all going. If it is not allowed, the plan may still show a broadcast of `customers` that Spark chose by itself.

# COMMAND ----------

try:
    spark.conf.set(
        "spark.sql.autoBroadcastJoinThreshold", "-1")
except Exception as e:
    print("setting not allowed here")

wrong = broadcast(orders).join(
    customers, "customer_id", "left")
wrong.explain()

# COMMAND ----------

# restore the default for the cells below
try:
    spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")
except Exception:
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 10: the same hint in SQL
# MAGIC Expected: orders 5001 to 5004, order 5004 with a NULL name.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT /*+ BROADCAST(c) */
# MAGIC        o.order_id, c.customer_name
# MAGIC FROM orders o
# MAGIC LEFT JOIN customers c
# MAGIC   ON o.customer_id = c.customer_id
# MAGIC ORDER BY o.order_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 11: cross join
# MAGIC Every combination. Expected: `12`. Check both counts first.

# COMMAND ----------

grid = customers.crossJoin(products)
print(grid.count())   # 12 = 4 x 3

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 12: the two extracts, union and distinct
# MAGIC Expected: `4 3`.

# COMMAND ----------

cols = "order_id int, customer_id int, order_date string"
orders_mon = spark.createDataFrame(
  [(5001, 101, "2026-11-02"), (5002, 102, "2026-11-03")], cols)
orders_resend = spark.createDataFrame(
  [(5002, 102, "2026-11-03"), (5003, 101, "2026-11-04")], cols)

u = orders_mon.union(orders_resend)
print(u.count(), u.distinct().count())   # 4 3
orders_mon.createOrReplaceTempView("orders_mon")
orders_resend.createOrReplaceTempView("orders_resend")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 13: SQL UNION vs UNION ALL
# MAGIC Expected: `UNION 3`, `UNION ALL 4`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'UNION' AS how, count(*) AS n FROM (
# MAGIC   SELECT * FROM orders_mon
# MAGIC   UNION SELECT * FROM orders_resend)
# MAGIC UNION ALL
# MAGIC SELECT 'UNION ALL', count(*) FROM (
# MAGIC   SELECT * FROM orders_mon
# MAGIC   UNION ALL SELECT * FROM orders_resend)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 14: the silent column-order bug
# MAGIC `union` matches by position: customer 103 lands in `order_id`. `unionByName` matches by name.

# COMMAND ----------

store = spark.createDataFrame(
    [(103, 5005, "2026-11-05")],
    "customer_id int, order_id int, order_date string")

orders_mon.union(store).show()
orders_mon.unionByName(store).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 15: an extra column
# MAGIC Both `union` and `unionByName` fail with `NUM_COLUMNS_MISMATCH`. `allowMissingColumns=True` fills NULL. Expected: two error lines, then 3 rows with `channel` NULL on the first two.

# COMMAND ----------

extra = spark.createDataFrame(
    [(5006, 104, "2026-11-06", "web")],
    cols + ", channel string")

for name, f in [("union", orders_mon.union),
                ("unionByName", orders_mon.unionByName)]:
    try:
        f(extra).count()
    except Exception as e:
        print(name, str(e).strip().split("]")[0] + "]")

orders_mon.unionByName(
    extra, allowMissingColumns=True).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 16: on the job, silver_order_lines
# MAGIC Orders and order_items are the large tables (shuffle join). The small dimensions are broadcast. The customer join is a left join. Expected: `5 84.00`.

# COMMAND ----------

order_lines = (order_items
  .join(orders, "order_id")            # fact to fact
  .join(broadcast(customers),
        "customer_id", "left")         # small dim
  .join(broadcast(products), "product_id")
  .withColumn("line_total",
              F.col("quantity") * F.col("price"))
  .select("order_id", "line_no", "order_date",
          "customer_id", "customer_name",
          "product_name", "quantity", "price",
          "line_total"))

order_lines.write.mode("overwrite").saveAsTable(
    "silver_order_lines")

total = order_lines.agg(F.sum("line_total")).first()[0]
print(order_lines.count(), total)   # 5 84.00
order_lines.orderBy("order_id", "line_no").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 17: what an inner customer join costs
# MAGIC Expected: `4 72.00`. No error, and order 5004 (12.00) is lost.

# COMMAND ----------

inner_lines = (order_items
  .join(orders, "order_id")
  .join(customers, "customer_id")      # inner now
  .join(products, "product_id")
  .withColumn("line_total",
              F.col("quantity") * F.col("price")))

total = inner_lines.agg(F.sum("line_total")).first()[0]
print(inner_lines.count(), total)   # 4 72.00

# COMMAND ----------

# MAGIC %md
# MAGIC ## Practical takeaway: join-choice checklist
# MAGIC 1. **Keys.** Name the full key (all parts of a composite key). Check the key is unique on the dimension side. Check the data types match.
# MAGIC 2. **Join type.** Need every row from the fact table? Use `left`. Only matches? `inner`. Every combination on purpose? `crossJoin`, after checking both counts.
# MAGIC 3. **Broadcast vs shuffle.** Broadcast the small dimension on the right side. Never broadcast the large fact table. Two large tables: let Spark shuffle.
# MAGIC 4. **Union.** PySpark `union` keeps duplicates and matches by position. Use `unionByName` for files from different sources, and `.distinct()` only when duplicates are truly unwanted.
# MAGIC 5. **Reconcile.** Compare row counts in and out, and count unmatched keys.
# MAGIC
# MAGIC Memory trick: PySpark union is UNION ALL in disguise. It keeps every row and matches columns by position.
# MAGIC
# MAGIC ## Cell 18: reusable snippet
# MAGIC Expected: `4 orders in, 4 out, 1 without a customer`, then `customers has 1 duplicate customer_id value(s)`.

# COMMAND ----------

def join_orders_to_customers(
        orders_df, customers_df, key="customer_id"):
    dupes = (customers_df.groupBy(key).count()
             .filter("count > 1").count())
    if dupes:
        raise ValueError(
            f"customers has {dupes} duplicate "
            f"{key} value(s)")
    joined = orders_df.join(
        broadcast(customers_df), key, "left")
    unmatched = joined.filter(
        F.col("customer_name").isNull()).count()
    print(f"{orders_df.count()} orders in, "
          f"{joined.count()} out, "
          f"{unmatched} without a customer")
    return joined

join_orders_to_customers(orders, customers)
# 4 orders in, 4 out, 1 without a customer

try:
    join_orders_to_customers(orders, dup)
except ValueError as e:
    print(e)
# customers has 1 duplicate customer_id value(s)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exam-style question (original)
# MAGIC 2 billion `order_items` rows, 5,000 `products` rows, join on `product_id`, keep every order line. Which is best?
# MAGIC - A) `broadcast(order_items).join(products, "product_id", "left")`
# MAGIC - B) `order_items.join(broadcast(products), "product_id", "left")`
# MAGIC - C) `order_items.crossJoin(products).filter(order_items.product_id == products.product_id)`
# MAGIC - D) `products.join(order_items, "product_id", "inner")`
# MAGIC
# MAGIC Answer: **B.** Broadcast the small table, keep the big one on the left, and the left join keeps every line. A broadcasts the huge table, and a left outer join cannot broadcast its left side. C builds a 10-trillion-row result first. D is inner, so unknown products are lost.
# MAGIC
# MAGIC ## Practice challenge
# MAGIC 1. `orders.join(customers, "customer_id", "right").count()`
# MAGIC 2. `customers.crossJoin(products).filter(F.col("state") == "TX").count()`
# MAGIC 3. Which rows of `orders_resend` are NOT in `orders_mon`? Write the one-line join and say how many rows it returns.
# MAGIC
# MAGIC Solution (uncomment to check).

# COMMAND ----------

# print(orders.join(customers, "customer_id", "right").count())   # Q1: 5
#   (5001, 5002, 5003 match; customers 103 and 104 stay with NULL order columns; 5004 is dropped)
# print(customers.crossJoin(products).filter(F.col("state") == "TX").count())   # Q2: 6 = 2 x 3
# print(orders_resend.join(orders_mon, "order_id", "left_anti").count())   # Q3: 1 (order 5003)

# COMMAND ----------

# MAGIC %md
# MAGIC Earlier lessons: A01 https://youtu.be/PglvHIK-QmU · A02 https://youtu.be/cLrBSH5DXIQ · A03 https://youtu.be/LS0gDlRD_1g · A04 https://youtu.be/AzpWro9XjJY · A05 https://youtu.be/r70MNc5Kla8 · A06 https://youtu.be/-O0mWjGb1gI · A07 https://youtu.be/daDUswtJYTM
# MAGIC
# MAGIC ## Optional cleanup
# MAGIC Drops only the schema this run created.

# COMMAND ----------

# spark.sql(f"DROP SCHEMA workspace.{schema} CASCADE")
