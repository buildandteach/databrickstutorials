# Join and Aggregate Data with PySpark — final narration

## title
Welcome to Build and Teach. Today we'll learn how to join and aggregate data with PySpark.

## early
Here is the actual lesson notebook in Databricks Free Edition, with Python and serverless compute. The Run all button runs the notebook, while the triangle beside a code cell runs that cell. These are still screenshots from the successful run, not a screen recording. Import the companion notebook as a new Python notebook and run from the top. We are continuing our small data engineering project. Previously, we cleaned rows and calculated order totals. Today, we add customer information and turn individual orders into a regional revenue report. The example is self-contained, so you do not need yesterday's tables.

## grain
A grain means what one row represents. Our orders have one row per order. Our customer lookup has one row per customer. At the end, the summary will have one row per reporting region. Keeping that meaning clear helps us catch errors that look like successful code but produce wrong totals.

## objective
The official August twenty twenty-six Data Engineer Associate guide lists inner and left joins, DataFrame aggregations such as count, and data quality checks in Data Transformation and Modeling. This lesson practices part of that foundation. It is not a complete Associate or Professional exam course, and there is no exam guarantee.

## input_code
Start with four fictional customers and six orders. Prices use decimal values, and each order total is quantity times unit price. The missing customer key is explicitly allowed in the order schema. All other order fields in this small example are present and valid. We are focusing on matching records, not repeating yesterday's numeric cleaning rules.

## inputs_live
Ana has two orders. Bo and Cy each have one. Dee has no orders. One order refers to customer ninety-nine, who is not in our lookup. Another has no customer key. The starting total is sixty-eight. Write that down: it will help us reconcile the join and the summary.

## key_code
We first check that the customer lookup has no repeated customer keys and no missing keys. Order identifiers must also be unique. It is perfectly normal for a customer identifier to appear in several order rows. It is not safe for that same key to unexpectedly appear several times in the lookup.

## key_explain
The group by and count check finds lookup keys whose count is greater than one. Our assertion stops if it finds any. Do not blindly drop duplicates to make a key check pass. If two customer records disagree about region, you need a business rule for which record is valid. Arbitrarily keeping one record hides the problem instead of solving it.

## inner_code
The inner join keeps only orders with a matching customer. We give the DataFrames short aliases, o and c, so our equality condition clearly compares the order's customer key with the lookup's customer key. Then select exactly the output columns we need. This avoids ambiguous duplicate column names.

## inner_live
Ana's two orders remain, along with Bo's and Cy's. The missing customer ninety-nine and the null key do not match. Ordinary equality does not match null to null. Dee does not create an order just because she is in the customer lookup. Inner join revenue is fifty-six. That is matched-order revenue, not the original total for all orders.

## left_code
Now make orders the left side and use a left join. This keeps each order, adding customer columns when a match exists. Missing lookup matches produce null customer-side fields. We keep the original customer key, so the unmatched ninety-nine is still visible. A customer found flag checks whether a right-side lookup key was present.

## left_live
The two missing matches have false for that flag. We did not remove them just because their region is missing. Be careful: filtering right-side values immediately after a left join can throw away the unmatched rows you intended to preserve.

## left_caution
A left join does not always keep exactly the original number of rows. It retains each left row at least once, but repeated matching lookup keys can produce more rows. The earlier uniqueness check is what makes this example preserve six orders and total revenue of sixty-eight.

## bad_code
The next cell deliberately creates a bad customer lookup by repeating Ana's record. It is a read-only demonstration and is never used for the report or saved table. Now each of Ana's two orders can match two customer rows.

## bad_live
In the actual demonstration, the bad join produced eight rows and ninety-three revenue. Two extra rows add another twenty-five to the total. The join can run without an error while silently overstating revenue. This is why checking keys and reconciling totals are more important than seeing a green check mark. We now return to the safe left join built from the unique lookup.

## bucket_code
We give unmatched orders an explicit reporting bucket called UNMATCHED. That is not a real region. It is a visible reporting decision that keeps missing lookup matches from disappearing in the summary.

## agg_code
Group by reporting region changes the grain. Instead of individual orders, each result row now represents one region bucket. Aggregate applies several calculations to each group. Count star counts every order row. Count customer name skips null names. Our customer schema requires names for matched customers, so that second count measures matched orders in this controlled example. In real data, a nullable name is not a reliable universal order counter.

## sum_explain
Sum adds the decimal order totals. We cast revenue to a decimal size that fits this tiny dataset. Production systems need deliberate precision and overflow policies. Order by controls the displayed report, not the physical ordering of a saved table.

## summary_live
NORTH has Ana's two orders and revenue twenty-five. SOUTH has one order and twenty-two fifty. WEST has one order and eight fifty. UNMATCHED has two orders and twelve. It has zero matched-order count because those customer-side names are null.

## reconcile
Adding the buckets gives six orders and sixty-eight revenue, matching the original orders. The matched count is four. These checks ensure we did not accidentally aggregate the intentionally bad duplicate-key join or lose the unmatched orders.

## destination_code
The destination cell creates a uniquely named schema beginning with b t joins inside the currently selected catalog. Keep the printed fully qualified table name. Your random suffix will differ from the one in this run. If you lack permission to create a schema, stop and select a permitted catalog rather than bypassing access controls.

## destination_live
Here is the actual destination from our run. The catalog is workspace, the schema begins with b t joins, and the table is region revenue. Your suffix will be different. This is only our small fictional-data demo, not an existing user table.

## write_code
We write only the safe regional summary, not the raw data or the bad demonstration. The managed Delta table needs no external path or storage credentials. The exact write mode is error, which refuses replacement if the table already exists. It is not overwrite or append. If a write already succeeded, skip it and run verification rather than recreating the schema or running the entire notebook again.

## write_live
The actual managed Delta write succeeded with error mode. No overwrite or drop was used. We saved only the safe summary, not the deliberately bad duplicate-key result. The demo schema and table remain available for inspection.

## verify_code
Read the stored summary using the full name. The query explicitly orders by reporting region. The notebook checks every expected bucket, not only the grand total. Then a second SQL query reconciles bucket count, total orders, matched orders, and revenue.

## verify_live
The actual persisted SQL checks show four buckets, six orders, four matched orders, and sixty-eight revenue. All nine code cells passed without errors. That message appears only after the assertions pass. This is a check of persisted data, not merely a DataFrame variable that has not been written. The tiny demo objects remain available for inspection; we do not delete them automatically.

## practice
For practice, start from the safe left join and keep only matched orders. Group by region and compute order count and revenue. Predict the result before running. You should get three real regions and total revenue fifty-six, not all-order revenue sixty-eight. Do not write over the saved summary. This practice answer is a prediction, not a separately captured live result.

## recap
Remember the sequence. Define the grain, check lookup keys, choose inner or left based on the reporting question, and inspect missing matches. Aggregate from the safe joined result. Count all rows when you need order count, and understand that counting a nullable column skips nulls. Reconcile counts and revenue before persisting a new table, then read it back and verify it again.

## end
The companion notebook, Python source, and quick-reference cheat sheet contain the project and practice challenge. This is Build and Teach. Keep building, and keep checking your results.