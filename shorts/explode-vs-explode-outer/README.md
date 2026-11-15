# explode vs explode_outer: 3 Rows or 5

Video: Short coming soon (pending review) on [youtube.com/@buildandteach](https://youtube.com/@buildandteach)

Companion to [Lesson A05, JSON and Nested Data with explode](../../lessons/associate-certification/a05-json-explode/) in the Databricks Data Engineer Associate Series. Exam guide Section 3, Data Transformation and Modeling (exploding arrays).

explode turns an array column into rows: one row per element, and the other selected columns repeat on every row. When the array is null or empty, explode returns no row, so that order disappears. explode_outer works the same way, but a null or empty array still gives one row, with a null element.

In the A05 data there are 4 orders: 1001 items [A1, B2], 1002 [C3], 1003 [] (empty array), and 1004 null. explode("items") returns 3 rows (1001 twice, 1002 once). explode_outer("items") returns 5 rows: the same 3, plus 1003 and 1004 with a null item. posexplode also skips null or empty arrays, and posexplode_outer keeps them.

```python
from pyspark.sql.functions import explode, explode_outer
orders.select("order_id", explode("items").alias("item")).count()        # 3
orders.select("order_id", explode_outer("items").alias("item")).count()  # 5
```

## Memory trick

OUTER keeps the outsiders. The empty array and the null stay in.

One-page reference: [cheatsheet.pdf](cheatsheet.pdf)
