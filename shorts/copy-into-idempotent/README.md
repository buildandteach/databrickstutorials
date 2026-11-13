# COPY INTO: Rerun Loads 0 Rows

Video: Short coming soon (pending review) on [youtube.com/@buildandteach](https://youtube.com/@buildandteach)

Companion to [Lesson A03, COPY INTO Incremental Loading](../../lessons/associate-certification/a03-copy-into/) in the Databricks Data Engineer Associate Series.

COPY INTO is idempotent. Databricks keeps a load history for each target table, so files already loaded are skipped. In the A03 example, run 1 loads orders_day1.csv (3 rows), a rerun with no new files inserts 0 rows (total still 3), and orders_day2.csv adds only its 2 rows (total 5). An edited file with the same name is still skipped. A new target table has its own load history, so every file loads into it. COPY_OPTIONS ('force' = 'true') turns idempotency off: every file loads again, so after day 2 a forced rerun inserts 5 rows and the table has 10, with duplicates.

## Memory trick

COPY INTO remembers. force forgets.

One-page reference: [cheatsheet.pdf](cheatsheet.pdf)
