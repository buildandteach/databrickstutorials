# Query and Analyze Data with PySpark, Part 1 — final narration

## title
Welcome to Build and Teach. Today we'll learn how to query and analyze data with PySpark. This is Part 1 of a three-part series: select, filter, derive columns, and inspect nulls. Part 2 covers aggregations and window functions. Part 3 covers Spark SQL and a safe managed Delta summary.

## early
Here is the actual Part 1 notebook in Databricks Free Edition, with Python and serverless compute. The Run all button runs the notebook, while the triangle beside a code cell runs that cell. These are still screenshots from the successful run, not a screen recording. Import the companion notebook as a new Python notebook named Build and Teach - Query and Analyze Data with PySpark - Part 1, attach serverless, and run from the top. Free Edition includes serverless compute and Unity Catalog, which is what we use here. The sample is self-contained, so you do not need tables from earlier lessons. Run the four code cells in order the first time. This part is read-only: it creates no schemas and writes no tables.

## why
Before you aggregate or join, shape the rows you care about and see what is missing. Select chooses columns. Filter or where keeps rows that match a condition. With column returns a new DataFrame with a derived field; it does not mutate the previous DataFrame in place. That immutability matters when you want to keep both the raw view and a cleaned view in memory. Nulls are not a crash by themselves. They are a signal that something upstream was incomplete. Count them before you decide how to clean, or you can silently overstate revenue. Think of Part 1 as the hygiene step that every later report depends on. If you skip it, group by and SQL will still run, and they will happily summarize bad inputs.

## objective
The official August twenty twenty-six Data Engineer Associate guide covers column and row manipulation, filters, and data quality checks in Section 3, Data Transformation and Modeling. This part practices that foundation. It is not a complete Associate or Professional exam course, and there is no exam guarantee.

## input_code
We start with eight fictional orders and an explicit schema: order ID, customer, region, category, quantity, unit price, and order date. Customer, quantity, and unit price are nullable on purpose so null practice is realistic. Order total is quantity times unit price, cast to a decimal with two places. When either input is missing, the total is null. Explicit schemas make those nullability choices visible instead of hiding them behind inference. Prices use decimal types rather than floating point so currency math stays exact on this small sample. Production systems still need deliberate precision and overflow policies; the cast here is sized for teaching, not for every enterprise ledger.

## input_live
The actual Free Edition run displayed all eight rows. Order totals were twenty, twenty-five, twenty-five fifty, thirty, twelve, then null, null, and thirty for IDs one oh one through one oh eight. The input check confirmed eight orders with two incomplete quantity or price rows for null practice. Write those numbers down; later filters and totals should reconcile to them.

## select_code
Valid revenue rows require both quantity and unit price. Build that filter first, then derive an is high value flag for totals at or above twenty-five. From the valid DataFrame, select the columns you need, keep Books in NORTH or WEST with a compound filter, and order by order ID so the display is predictable. Filtering does not invent values; it only keeps rows that already match. Compound filters combine category equality with region membership. Keep the validity filter separate from the business filter so you can explain each rule. When a review asks why a row disappeared, you want one clear predicate to point at.

## select_live
The actual select and filter returned orders one oh one and one oh three, with totals twenty and twenty-five fifty, summing to forty-five fifty. Six rows were valid overall. The select and filter check passed. Notice order one oh five stayed valid for revenue even though its customer is null; missing customer is a different quality problem than missing price.

## high_code
Next keep high-value orders where the total is at least twenty-five, and order by total descending, then order ID descending. Sorting controls presentation only. A stable secondary sort makes ties readable when two orders share the same total.

## high_live
The actual high-value list was one oh eight, one oh four, one oh three, and one oh two, totaling one hundred ten fifty. Two orders tied at thirty dollars, and the secondary sort by order ID descending broke the tie cleanly. The check passed. If your local prediction listed a different order for the thirty-dollar tie, check the secondary sort direction.

## null_code
Before any cleaning, print the schema, run a numeric summary on quantity, unit price, and order total, and count nulls in customer, quantity, and unit price with explicit when expressions. Cleaning is a business decision: fill, drop, quarantine, or reject with a reason. This lesson only makes the gaps visible so later aggregates are honest. Schema tells you which columns allow nulls. Summary shows how many values were present and their range. Explicit null counts answer the quality question without guessing from a sparse display. Together they are a lightweight data quality checklist before silver cleaning.

## null_live
The actual null check found one missing customer, one missing quantity, and one missing unit price. The summary showed seven non-null quantities, seven non-null unit prices, and six non-null order totals. Those counts match the two incomplete source rows and the one null customer on an otherwise complete order.

## practice
For practice, from the valid rows select Toys in SOUTH or NORTH, keep order ID, customer, and order total, and order by total descending. Predict before you run: one oh eight and one oh four at thirty, then one oh two at twenty-five. This practice answer is a prediction aligned with the validated sample, not a separately captured live cell. If your prediction disagrees with the notebook, check whether you accidentally included invalid quantity or price rows. The validity rule is the whole point of the exercise.

## recap
Part 1 shaped rows with select and filter, derived columns with with column, ordered the display, and made nulls visible before any write. Keep the validity rule explicit whenever revenue matters. Next, Part 2 aggregates revenue with group by and ranks rows with window functions. The companion notebook and cheat sheet contain the full code. This is Build and Teach. Save the Part 1 query or notebook under the Build and Teach title so you can reopen the exact checks later.