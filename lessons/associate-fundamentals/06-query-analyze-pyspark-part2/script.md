# Query and Analyze Data with PySpark, Part 2 — final narration

## title
Welcome to Build and Teach. Today we'll learn how to aggregate and window-analyze order data with PySpark. This is Part 2 of a three-part series. Part 1 covered select, filter, derived columns, and nulls. Part 3 will cover Spark SQL and a safe managed Delta summary.

## early
Here is the actual Part 2 notebook in Databricks Free Edition, with Python and serverless compute. These are still screenshots from the successful run, not a screen recording. Import the companion as Build and Teach - Query and Analyze Data with PySpark - Part 2, attach serverless, and run from the top. The notebook rebuilds the same eight fictional orders so Part 2 stands alone on Free Edition serverless with Unity Catalog available. You do not need Part 1's notebook session. Run the four code cells in order. This part is read-only and creates no schemas or tables.

## grain
Aggregation changes the grain. Before grouping, one row means one order. After grouping by region and category, one row means one reporting bucket. Window functions are different: they keep the detail rows and add columns such as a rank or a running total. Ask which grain your question needs before you write the code. The wrong grain is how successful-looking jobs quietly overstate or hide revenue. A useful habit is to say the sentence out loud: one row means one order, or one row means one region and category. If you cannot finish that sentence, do not aggregate yet.

## objective
Associate August twenty twenty-six Section 3 lists aggregations such as count and summary operations as foundation skills. Window ranking is a common pattern for ordering within groups. This part practices that foundation without claiming a full exam course, and there is no exam guarantee.

## input_live
The actual input check confirmed eight orders and six valid revenue rows totaling one hundred forty-two fifty. Incomplete quantity or price rows stay out of revenue math, just as in Part 1. Reconcile every later total back to that number. If your total is not one hundred forty-two fifty, stop and inspect null quantity or price rows before grouping.

## agg_code
Group by region and category, then aggregate count star for order count and sum of order total for revenue. Cast the sum to a decimal size that fits this tiny sample, and order the report for a stable display. Count star counts every row in the group. If you counted a nullable column instead, nulls would be skipped, which is a different question. Order by after aggregation only changes how the report is shown. It does not change the bucket math. Still, a stable order makes screenshots and reviews easier to compare.

## agg_live
The actual aggregation produced five buckets: EAST Books one order and twelve dollars, NORTH Books one and twenty, NORTH Toys two and sixty, SOUTH Toys one and twenty-five, and WEST Books one and twenty-five fifty. Across buckets: six orders and one hundred forty-two fifty. That matches the valid input total. NORTH Toys is the only multi-order bucket in this sample, which is why its revenue is larger than the single-order buckets.

## region_code
Roll up by region alone when you want a coarser report. Same valid rows, fewer groups. After the rollup, add the region revenues and confirm you still land on one hundred forty-two fifty. Reconciliation is part of the analysis, not an optional extra. Region rollups are where unmatched or incomplete rows often vanish if you filtered them earlier without documenting why. Because we already restricted to valid revenue rows, the rollup should be complete for that definition of valid.

## region_live
The actual region rollup was EAST twelve, NORTH eighty across three orders, SOUTH twenty-five, and WEST twenty-five fifty. NORTH's eighty is the twenty plus sixty from its two category buckets.

## window_code
Window functions keep detail while adding analysis columns. Row number partitions by category and orders by total descending, then order ID descending, so ties inside a category get a stable rank. A running revenue window orders by date and order ID with rows between unbounded preceding and current row. That running window is intentionally unpartitioned so the cumulative total spans the whole valid set. Partitioning a window is not the same as grouping. Partition by category restarts the rank for each category. Leaving the running total unpartitioned keeps one cumulative series across all valid orders by date.

## window_live
In the actual Free Edition run, Toys ranks were one oh eight first, one oh four second, and one oh two third. The running revenue ended at one hundred forty-two fifty on order one oh eight. Spark also printed a non-fatal warning: no partition defined for the window operation, so data moves to a single partition, and that can cause serious performance degradation at scale. For this tiny teaching sample, a global running total is intentional. On large data, partition the window when the cumulative total should restart per key, or accept the shuffle cost deliberately and measure it. Do not ignore the warning just because the answer looked right. Capture the warning in your notes the first time you see it. Right answers with expensive plans are still a production risk. The ranked and running-total columns appear beside the original order fields, which is how windows differ from group by.

## practice
For practice, from the valid rows keep only Toys, compute row number by total descending, and predict the ranks before you run. You should see one oh eight, one oh four, then one oh two. Do not write any tables in this part. This practice answer is a prediction, not a separately captured live cell. Part 2 ends without writes so you can experiment safely before Part 3 persists a summary.

## recap
Part 2 changed the grain with group by, rolled up by region, and ranked rows with windows while keeping detail. Treat unpartitioned windows as an explicit choice, not an accident. Next, Part 3 answers the same questions in Spark SQL and persists one new managed summary safely. The companion notebook and cheat sheet have the full code. This is Build and Teach. When you continue to Part 3, you will see the same five buckets appear again from SQL, which is a useful cross-check of today's DataFrame work.