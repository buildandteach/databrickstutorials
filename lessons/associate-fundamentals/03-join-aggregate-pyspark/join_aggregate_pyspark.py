# Databricks notebook source
# MAGIC %md
# MAGIC # Join and Aggregate Data with PySpark — Build and Teach
# MAGIC Python notebook, Databricks Free Edition, serverless compute. Fictional data only; no downloads or dependency installs. Run from the top.
# MAGIC 
# MAGIC Project: enrich orders with customer regions, keep unmatched orders visible, then summarize revenue and order counts by region. Grain: one row per order before aggregation; one row per region after aggregation.
# MAGIC 
# MAGIC Safety: only one new managed Delta summary table, in a newly named `bt_joins_...` schema in your current writable catalog. No overwrite, append, DROP, external path, custom Spark config, UDF, or paid-only feature. A repeated write refuses replacement. Full reruns create another tiny demo schema; do not rerun setup unnecessarily. Demo objects remain until you deliberately clean them up yourself.

# COMMAND ----------

# 1. Explicit schemas and small fictional datasets
from decimal import Decimal
from uuid import uuid4
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, IntegerType,
                              StringType, DecimalType)
customer_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("customer_name", StringType(), False),
    StructField("region", StringType(), False),
])
order_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), True),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DecimalType(10, 2), False),
])
customers = spark.createDataFrame([
    (1, "Ana", "NORTH"), (2, "Bo", "SOUTH"),
    (3, "Cy", "WEST"), (4, "Dee", "NORTH"),
], customer_schema)
orders = spark.createDataFrame([
    (101, 1, 2, Decimal("10.00")),
    (102, 1, 1, Decimal("5.00")),
    (103, 2, 3, Decimal("7.50")),
    (104, 3, 2, Decimal("4.25")),
    (105, 99, 1, Decimal("9.00")),
    (106, None, 1, Decimal("3.00")),
], order_schema).withColumn(
    "order_total", (F.col("quantity") * F.col("unit_price")).cast("decimal(12,2)")
)
customers.orderBy("customer_id").show(truncate=False)
orders.orderBy("order_id").show(truncate=False)
assert customers.count() == 4 and orders.count() == 6
assert orders.agg(F.sum("order_total")).first()[0] == Decimal("68.00")
print("INPUT CHECK: 4 customers, 6 orders, revenue 68.00")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Protect the join grain
# MAGIC The customer lookup must have one row per non-null customer key. Repeated customer IDs in orders are normal: a customer can place multiple orders. Repeated keys in the lookup can multiply those orders. Stop rather than applying an arbitrary `dropDuplicates` that might discard conflicting customer facts.

# COMMAND ----------

duplicate_customer_keys = (customers.groupBy("customer_id").count()
                           .filter(F.col("count") > 1))
assert customers.filter(F.col("customer_id").isNull()).count() == 0
assert duplicate_customer_keys.count() == 0, "Customer keys are not unique; fix lookup first"
assert orders.groupBy("order_id").count().filter(F.col("count") > 1).count() == 0
print("KEY CHECK: customer lookup unique; order IDs unique")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Inner join: keep only matches
# MAGIC Use aliases and an explicit equality condition; select exactly the columns we want so duplicate key names do not become ambiguous. An ordinary equality join does not match a null key. `inner` keeps order rows with matching customer records. Customer 4 has no orders and does not create an order row.

# COMMAND ----------

o = orders.alias("o")
c = customers.alias("c")
inner_orders = (o.join(c, F.col("o.customer_id") == F.col("c.customer_id"), "inner")
    .select("o.order_id", F.col("o.customer_id").alias("customer_id"),
            "c.customer_name", "c.region", "o.order_total"))
inner_orders.orderBy("order_id").show(truncate=False)
assert inner_orders.count() == 4
assert inner_orders.agg(F.sum("order_total")).first()[0] == Decimal("56.00")
print("INNER CHECK: 4 matched orders, revenue 56.00")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Left join: preserve orders, expose missing matches
# MAGIC The orders DataFrame is on the left. A left join preserves each left row at least once, but can still multiply rows if the lookup key is not unique. Our earlier uniqueness check is what makes the expected row count six.
# MAGIC 
# MAGIC Keep the original customer key and a `customer_found` flag based on the right lookup key. A missing match gives null right-side values. Do not hide unmatched orders with an immediate filter on right-side fields.

# COMMAND ----------

left_orders = (o.join(c, F.col("o.customer_id") == F.col("c.customer_id"), "left")
    .select("o.order_id", F.col("o.customer_id").alias("customer_id"),
            "c.customer_name", "c.region", "o.order_total",
            F.col("c.customer_id").isNotNull().alias("customer_found")))
left_orders.orderBy("order_id").show(truncate=False)
unmatched = left_orders.filter(~F.col("customer_found"))
unmatched.orderBy("order_id").show(truncate=False)
assert left_orders.count() == orders.count() == 6
assert unmatched.count() == 2
assert left_orders.agg(F.sum("order_total")).first()[0] == Decimal("68.00")
print("LEFT CHECK: 6 orders, 2 unmatched, revenue preserved at 68.00")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Controlled duplicate-key demonstration — read-only toy data
# MAGIC This intentionally bad lookup is NOT used downstream and is never saved. Repeating customer 1 gives each of that customer's two orders two matches. A left join is not a guarantee of unchanged row counts. Never fix conflicting business keys by blindly discarding a row.

# COMMAND ----------

bad_customers = customers.unionByName(customers.filter(F.col("customer_id") == 1))
bad_keys = bad_customers.groupBy("customer_id").count().filter(F.col("count") > 1)
bad_keys.show()
# This is an intentionally unsafe, read-only teaching example; do NOT persist it.
bad_join = orders.join(bad_customers, "customer_id", "left")
print("BAD DEMO rows:", bad_join.count())
print("BAD DEMO inflated revenue:", bad_join.agg(F.sum("order_total")).first()[0])
assert bad_join.count() == 8
assert bad_join.agg(F.sum("order_total")).first()[0] == Decimal("93.00")
print("BAD DEMO ONLY: 8 rows and 93.00 instead of 6 rows and 68.00")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Aggregate the SAFE left-join result
# MAGIC Assign unmatched orders an explicit `UNMATCHED` reporting bucket; it is not a real region. `count('*')` counts every order row; `count('customer_name')` skips null names. Here the name field is non-null in matched lookup records, so that count measures matched orders in this controlled example. It is NOT a general substitute for counting orders.
# MAGIC 
# MAGIC `groupBy` changes the grain to one row per reporting region. Use built-in aggregate functions, not a Python UDF. Sorting controls the displayed result, not saved-table order.

# COMMAND ----------

reporting_orders = left_orders.withColumn(
    "report_region", F.when(~F.col("customer_found"), F.lit("UNMATCHED"))
                      .otherwise(F.col("region"))
)
region_summary = (reporting_orders.groupBy("report_region")
    .agg(F.count("*").alias("order_count"),
         F.count("customer_name").alias("matched_order_count"),
         F.sum("order_total").cast("decimal(18,2)").alias("revenue")))
region_summary.orderBy("report_region").show(truncate=False)
assert region_summary.count() == 4
assert region_summary.agg(F.sum("order_count")).first()[0] == 6
assert region_summary.agg(F.sum("matched_order_count")).first()[0] == 4
assert region_summary.agg(F.sum("revenue")).first()[0] == Decimal("68.00")
print("SUMMARY CHECK: 4 buckets, 6 orders, 4 matched orders, revenue 68.00")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Create an isolated managed-table destination
# MAGIC Choose a catalog where you may create a schema. Permission failure means stop and choose a permitted catalog; do not bypass permissions. This cell creates a new schema each time it runs. Do not rerun it just to refresh verification. Table name is fully qualified and quoted; no changes to the current schema are required.

# COMMAND ----------

def qi(name):
    return "`" + name.replace("`", "``") + "`"

catalog_name = spark.sql("SELECT current_catalog()").first()[0]
demo_schema = "bt_joins_" + uuid4().hex[:12]
assert demo_schema.startswith("bt_joins_")
schema_fqn = qi(catalog_name) + "." + qi(demo_schema)
table_fqn = schema_fqn + "." + qi("region_revenue")
spark.sql(f"CREATE SCHEMA {schema_fqn} COMMENT 'Build and Teach fictional joins demo'")
print("Demo schema:", schema_fqn)
print("New managed Delta summary:", table_fqn)


# COMMAND ----------

# 8. Exact mode spelling "error" works with the Free Edition Spark Connect client.
# Refuse replacement. If already written successfully, skip to verification.
assert demo_schema.startswith("bt_joins_")
region_summary.write.format("delta").mode("error").saveAsTable(table_fqn)
print("CREATED managed Delta summary:", table_fqn)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Verify persisted totals and every bucket
# MAGIC SQL uses the same fully qualified table name and explicit ORDER BY. This checks stored results, not merely a DataFrame variable. Expected bucket values are assertions, not pre-populated outputs.

# COMMAND ----------

persisted = spark.sql(f"""
SELECT report_region, order_count, matched_order_count, revenue
FROM {table_fqn}
ORDER BY report_region
""")
persisted.show(truncate=False)
expected = [
    ("NORTH", 2, 2, Decimal("25.00")),
    ("SOUTH", 1, 1, Decimal("22.50")),
    ("UNMATCHED", 2, 0, Decimal("12.00")),
    ("WEST", 1, 1, Decimal("8.50")),
]
assert [tuple(r) for r in persisted.collect()] == expected
checks = spark.sql(f"""
SELECT COUNT(*) AS bucket_count, SUM(order_count) AS total_orders,
       SUM(matched_order_count) AS matched_orders, SUM(revenue) AS total_revenue
FROM {table_fqn}
""")
checks.show()
a = checks.first()
assert (a.bucket_count, a.total_orders, a.matched_orders, a.total_revenue) == (4, 6, 4, Decimal("68.00"))
print("PASS: stored summary = 4 buckets / 6 orders / 4 matched / revenue 68.00")
print("Copy for a SQL cell:")
print(f"SELECT * FROM {table_fqn} ORDER BY report_region;")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Practice challenge — no writes
# MAGIC Starting from `left_orders`, keep only matched orders, group by region, and compute order count and revenue. Predict three rows: NORTH 2/25.00, SOUTH 1/22.50, WEST 1/8.50. Total matched revenue is 56.00, not all-order revenue 68.00. Do not overwrite the saved summary.
# MAGIC 
# MAGIC ## Recap
# MAGIC - Define the grain and validate lookup keys before joining.
# MAGIC - Inner keeps matches; left retains unmatched left rows but can multiply rows with duplicate right keys.
# MAGIC - Inspect unmatched records; null keys do not match via ordinary equality.
# MAGIC - groupBy changes the grain; count(*) and count(nullable_column) differ.
# MAGIC - Use decimal prices and built-in aggregations, reconcile row counts and revenue.
# MAGIC - Save one new managed Delta table with mode("error"), then verify the persisted values.
# MAGIC 
# MAGIC Official Associate August 2026 guide Section 3 includes inner/left joins and DataFrame aggregations such as count. This lesson covers a subset of foundation skills, not full Associate or Professional exam preparation or an exam guarantee.
# MAGIC https://www.databricks.com/sites/default/files/2026-10/Databricks-Certified-Data-Engineer-Associate-Exam-Guide-August-2026.pdf
# MAGIC 
# MAGIC Build and Teach — https://buildandteach.com/
