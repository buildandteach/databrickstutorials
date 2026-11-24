# PySpark dropDuplicates Trap: The Latest Row Is Not Guaranteed

Video: Short coming soon (pending review) on [youtube.com/@buildandteach](https://youtube.com/@buildandteach)

Companion to [Lesson A09, Deduplication and Aggregates](../../lessons/associate-certification/a09-dedup-aggregates/) in the Databricks Data Engineer Associate Series. Exam guide Section 3, Data Transformation and Modeling.

`df.dropDuplicates(["order_id"])` keeps one row per `order_id`, but it has no sort argument, so you cannot choose which row survives. `distinct()` and `dropDuplicates()` with no arguments remove only rows that match in every column. To keep the latest change per key, use `row_number()` over a window partitioned by the key and ordered by the change timestamp descending, then keep row 1.

A09 exam-style question: order 5002 has two rows in `orders_cdc`, 30.00 and a late update to 36.00. Which row does `orders_cdc.dropDuplicates(["order_id"])` keep? Answer: not guaranteed. In a local Spark run, order 5001 kept its old 24.00 row and order 5002 kept its new 36.00 row.

```python
w = Window.partitionBy("order_id").orderBy(F.col("change_ts").desc())
latest = (orders_cdc.withColumn("rn", F.row_number().over(w))
          .filter("rn = 1").drop("rn"))   # 5 rows, latest change each
```

Dedup checklist: business key (`order_id`, not `customer_id`), sort (`change_ts`, newest first), keep latest (`row_number() = 1`), then aggregate. On the raw feed, Nov 2 shows 4 orders (average 29.63). Deduplicated, it is 2 orders (average 32.25).

Outputs checked on local Apache Spark 4.0, not on Databricks.

## Memory trick

dropDuplicates keeps a row. row_number keeps the right row.

One-page reference: [cheatsheet.pdf](cheatsheet.pdf)
