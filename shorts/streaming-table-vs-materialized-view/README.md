# Streaming Table vs Materialized View: Late Updates Need a Materialized View

Video: Short coming soon (pending review) on [youtube.com/@buildandteach](https://youtube.com/@buildandteach)

Companion to [Lesson A10, Gold Layer: Views, Materialized Views and Streaming Tables](../../lessons/associate-certification/a10-gold-layer-views/) in the Databricks Data Engineer Associate Series. Exam guide Section 3, Data Transformation and Modeling.

A streaming table processes each input row once and expects an append-only source, so a row it already processed is not re-queried when the source changes. A materialized view stores results and recalculates them on each refresh to reflect the current state of its sources, including updates and deletes. A regular view stores no data and runs its query on every read.

A10 exam-style question: `gold.daily_sales_by_region` shows Nov 2, TX at 58.50. On Nov 4 a late update changes order 5002 (a Nov 2 order) from 30.00 to 36.00. Which gold object keeps Nov 2 correct? Answer: a materialized view. After its next refresh, Nov 2 TX shows 64.50 (expected, per the Databricks docs).

```sql
CREATE OR REPLACE MATERIALIZED VIEW gold.daily_sales_by_region
  SCHEDULE EVERY 1 HOUR
AS SELECT o.order_date, c.state AS region,
  COUNT(*) AS order_count, SUM(o.amount) AS total_sales
FROM silver.orders o JOIN silver.customers c
  ON o.customer_id = c.customer_id
GROUP BY o.order_date, c.state;
```

The trap: a streaming read of a Delta table fails when the source gets an `UPDATE`, `DELETE` or `MERGE`. With `skipChangeCommits`, the change is skipped. Either way the old day stays wrong until a full refresh.

Object checklist: latency (a view is always current, a materialized view is current as of its last refresh), refresh (manual, scheduled, or on update), late data (old rows change: materialized view), cost (a view runs the full query on every read). Free Edition allows one active pipeline per pipeline type.

View and snapshot-table results were checked on local Apache Spark 4.0, not on Databricks. Materialized view and streaming table results are expected behavior from the Databricks docs.

## Memory trick

Streaming tables read each row once. Materialized views recompute the answer.

One-page reference: [cheatsheet.pdf](cheatsheet.pdf)
