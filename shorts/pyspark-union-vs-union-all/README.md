# PySpark union vs UNION ALL: Duplicates Are Kept

Video: [youtube.com/shorts/H_MMKRj7CYk](https://youtube.com/shorts/H_MMKRj7CYk)

Companion to [Lesson A08, Joins and Unions](../../lessons/associate-certification/a08-joins-unions/) in the Databricks Data Engineer Associate Series. Exam guide Section 3, Data Transformation and Modeling.

In PySpark, `df1.union(df2)` keeps duplicate rows, the same as `UNION ALL` in SQL. `unionAll` is an alias of `union`. Both match columns by position, not by name. In SQL, a plain `UNION` removes duplicates and `UNION ALL` keeps them. To get SQL `UNION` behavior from DataFrames, add `.distinct()`.

A08 exam-style question: `orders_mon` has orders 5001 and 5002, `orders_resend` has 5002 and 5003, same columns in the same order. What does `orders_mon.union(orders_resend).count()` return? Answer: 4.

```python
orders_mon.union(orders_resend).count()              # 4
orders_mon.union(orders_resend).distinct().count()   # 3
orders_mon.unionByName(orders_store)                 # match by name
```

If a file lists `customer_id` before `order_id`, `union` puts customer 103 in the `order_id` column with no error. `unionByName` matches by name. Different column counts fail with `NUM_COLUMNS_MISMATCH`; `unionByName(other, allowMissingColumns=True)` fills missing columns with NULL.

## Memory trick

PySpark union is UNION ALL in disguise. It keeps every row and matches columns by position.

One-page reference: [cheatsheet.pdf](cheatsheet.pdf)
