# Query and Analyze Data with PySpark, Part 3 — final narration

## title
Welcome to Build and Teach. Today we'll learn how to query the same orders with Spark SQL and save a safe managed Delta summary. This is Part 3 of a three-part series. Parts 1 and 2 covered the DataFrame API for filtering, nulls, aggregations, and windows.

## early
Here is the actual Part 3 notebook in Databricks Free Edition, with Python and serverless compute. These are still screenshots from the successful run, not a screen recording. Import as Build and Teach - Query and Analyze Data with PySpark - Part 3, attach serverless, and run from the top. Free Edition includes serverless compute and Unity Catalog, which we use for the unique demo schema and managed table. The notebook rebuilds the sample so Part 3 stands alone. Run the six code cells in order. The destination write cell should run once; if it already succeeded, skip ahead to verification.

## why
Spark SQL and the DataFrame API are two doors into the same engine. Temporary views let you write SQL against DataFrames you already shaped. Persisting a small Gold-style summary is useful when analysts need a stable table, but only after you reconcile the numbers. Writes in this lesson create a new managed table with mode error. No overwrite, no drop, and no append onto an existing object. Mode error is the write-side version of fail loud. If the table already exists, the notebook stops instead of quietly replacing yesterday's summary.

## objective
Associate August twenty twenty-six Section 3 includes Gold-layer tables for analytics, and Section 7 covers managed versus external tables. This part creates one managed summary and verifies it with SQL. It is foundation practice, not a complete exam course, and there is no exam guarantee.

## input_live
The actual input check showed six valid rows, five region and category buckets, and revenue one hundred forty-two fifty. Keep that total in mind; every SQL check and the persisted table must land on the same number. Creating or replacing the temp view is session-scoped and does not persist data by itself.

## sql_high
Register valid orders as a temporary view, then filter high-value orders in SQL where the total is at least thirty. Order the result so the display is clear. The same validity rule applies: quantity and unit price must both be present before revenue is trusted. Temporary views last for the session. They are ideal for SQL exploration on DataFrames you already trust, without creating permanent objects yet.

## sql_high_live
The actual SQL high-value result returned orders one oh eight and one oh four, each thirty dollars, totaling sixty. That is a stricter threshold than Part 1's twenty-five dollar high-value cut, so fewer rows remain.

## sql_agg
Aggregate in SQL by region and category with count and sum, and confirm every bucket matches the DataFrame aggregation from Part 2. Duality is the point: same engine, two APIs, same answer. If SQL and DataFrame disagree, stop and find the filter difference before you write anything. Matching SQL to DataFrame results is also a debugging skill. When they diverge, the bug is usually a filter, a null rule, or a join duplication, not the SQL engine itself.

## sql_agg_live
The actual SQL aggregation matched bucket for bucket: EAST Books twelve, NORTH Books twenty, NORTH Toys sixty, SOUTH Toys twenty-five, and WEST Books twenty-five fifty, totaling one hundred forty-two fifty. Those five buckets are exactly what we will persist next, so the write is a snapshot of a checked answer, not a new calculation.

## dest_code
Create a uniquely named schema beginning with b t query inside the current catalog, quote identifiers safely, and print the fully qualified destination table name. Your random suffix will differ from this run. If you lack permission to create a schema, stop and choose a writable catalog rather than bypassing access controls. Unique schema names beginning with b t query keep teaching objects isolated. Do not reuse someone else's suffix, and do not drop a schema that already has data you care about.

## dest_live
In the actual Free Edition run, the catalog was workspace and the schema was b t query c f nine one one one four eight f c three d. The table name was region category revenue. Keep the printed fully qualified name for verification. Screenshot or copy the FQN into your notes. Verification queries need the real suffix from your run.

## write_code
Write only the safe region and category summary with format delta and mode error. Error refuses replacement if the table already exists. It is not overwrite, not append, and there is no drop. If a write already succeeded, skip the destination cell and run verification instead of recreating objects. A full notebook rerun would create another tiny unique schema. Managed Delta tables store data in the catalog's managed location. We do not set an external LOCATION, and we do not convert table types in this lesson.

## write_live
The actual managed Delta write succeeded with mode error. No overwrite or drop was used. Only the safe summary was saved into the unique demo schema. Because mode error was used, a second write to the same FQN would fail instead of replacing rows.

## verify_live
Reading the persisted table matched all five expected buckets and the aggregate gate: five buckets, six orders, revenue one hundred forty-two fifty. The cell printed PASS and a copyable SELECT so you can re-check the stored rows yourself. That check reads the table, not a leftover DataFrame variable. If PASS does not print, read the assertion failure before rerunning writes. Fix the logic, or choose a new suffix, rather than forcing a replace.

## recap
The series path: Part 1 shaped rows and nulls, Part 2 aggregated and ranked, Part 3 queried in SQL and persisted safely. Reconcile before you write, use mode error for new managed tables, and verify the stored result. The companion notebook and cheat sheet contain the project. This is Build and Teach. Keep building, and keep checking your results. After this series you can shape rows, aggregate and rank them, query in SQL, and persist a checked summary on Free Edition serverless. If you are following the series in order, you now have DataFrame skills from Parts 1 and 2 plus SQL and a verified managed table from Part 3.