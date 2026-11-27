# Pipeline Expectations: Warn, Drop or Fail? A Warn-Only Check Still Writes Bad Rows

Video: Short coming soon (pending review) on [youtube.com/@buildandteach](https://youtube.com/@buildandteach)

Companion to [Lesson A11, Data Quality Checks](../../lessons/associate-certification/a11-data-quality-checks/) in the Databricks Data Engineer Associate Series. Exam guide Section 3, Data Transformation and Modeling.

An expectation is a named data quality check on a pipeline streaming table, materialized view or view. The action decides what happens to a record that fails. With no `ON VIOLATION` clause the action is warn: the record is written to the target and the violation is counted in the data quality metrics.

A11 exam-style question: `CONSTRAINT valid_amount EXPECT (amount >= 0)` with no `ON VIOLATION` clause. On Nov 4, order 5008 arrives with -15.00. What happens to it? Answer: it is written to the table, and the violation is counted. It flows on into the gold totals.

```sql
CREATE OR REFRESH STREAMING TABLE silver.orders_validated (
  CONSTRAINT valid_customer_id
    EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_amount
    EXPECT (amount >= 0) ON VIOLATION DROP ROW
) AS SELECT * FROM STREAM(bronze.orders_raw);
```

| Action | SQL | Python | Invalid records |
|---|---|---|---|
| warn (default) | `EXPECT (...)` | `dp.expect` | Written to the target, violation counted |
| drop | `EXPECT (...) ON VIOLATION DROP ROW` | `dp.expect_or_drop` | Dropped before the write, count logged |
| fail | `EXPECT (...) ON VIOLATION FAIL UPDATE` | `dp.expect_or_fail` | Update fails and is rolled back, good rows not written either |

The other trap is failing the whole load over one bad row. When someone needs to fix the bad rows, quarantine them instead: one reason per row, clean rows to `silver.orders`, bad rows to `silver.orders_quarantine`. In the local run, 5 orders came in, 2 were clean and 3 were quarantined. Free Edition: serverless pipelines include expectations, and Free Edition allows one active pipeline per pipeline type.

The quarantine split was checked on local Apache Spark 4.0, not on Databricks. Expectation results are expected behavior from the Databricks docs.

## Memory trick

Warn writes. Drop drops. Fail stops.

One-page reference: [cheatsheet.pdf](cheatsheet.pdf)
