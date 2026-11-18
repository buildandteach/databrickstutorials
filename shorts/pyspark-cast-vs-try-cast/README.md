# PySpark cast vs try_cast: One N/A Fails the Job on Serverless

Video: Short coming soon (pending review) on [youtube.com/@buildandteach](https://youtube.com/@buildandteach)

Companion to [Lesson A07, Bronze to Silver Cleaning](../../lessons/associate-certification/a07-bronze-to-silver/) in the Databricks Data Engineer Associate Series. Exam guide Section 3, Data Transformation and Modeling.

Serverless compute runs with American National Standards Institute (ANSI) SQL mode on by default. In ANSI mode, an invalid cast raises an error instead of quietly returning NULL. So casting a string `amount` column that holds `N/A` to `DECIMAL(10,2)` fails the whole query with `CAST_INVALID_INPUT`. `try_cast` returns NULL for malformed values instead, so the job keeps going. Count the NULLs it creates, then filter or quarantine those rows before you write the silver table.

A07 exam-style question: `bronze_orders.amount` is a string with "25.50", "N/A" and "14.00". On serverless, what does `cast("decimal(10,2)")` do? Answer: the query fails.

```python
t = "try_cast(amount AS DECIMAL(10,2))"
silver = (bronze.withColumn("amount", F.expr(t))
          .filter(F.col("amount").isNotNull()))   # 1002 "N/A" -> NULL, filtered
```

ANSI mode is on by default on serverless compute (including Databricks Free Edition), in Apache Spark 4.0, and in Databricks Runtime 17.0 and above. On classic compute with older runtimes it was off, and the same cast returned NULL.

## Memory trick

Cast fails loud. try_cast fails quiet. A quiet NULL still needs a count.

One-page reference: [cheatsheet.pdf](cheatsheet.pdf)
