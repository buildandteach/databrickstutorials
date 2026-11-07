# Clean and Transform Data with PySpark — final narration

## title
Welcome to Build and Teach. Today we'll learn how to clean and transform data with PySpark.

## early
Here is the actual lesson notebook in Databricks Free Edition. We have Python selected and serverless compute attached. Import the companion notebook as a new notebook, then run its cells from the top. The Run all button runs the notebook; the triangle beside a code cell runs just that cell. These are still screenshots from the actual run, not a screen recording.

## plan
We will choose columns, filter invalid orders, handle missing labels, calculate order totals, and save a new Delta table. Everything uses eight fictional orders. We will pause at each result so you can understand what changed. We will not replace an existing table or delete anyone's data.

## objective
These are foundation skills for data engineering. The official August twenty twenty-six Databricks Data Engineer Associate guide has a section called Data Transformation and Modeling. It includes cleaning nulls, manipulating columns, applying filters, and data quality checks. Today practices part of that foundation, not a complete exam course. These basics also prepare you for more advanced work later. There is no certification guarantee.

## schema
Start with the imports and our small orders list. Every row is invented. The explicit schema names each column and its type, and allows missing values. Order identifier and quantity are integers. Customer and region are strings. Unit price is decimal ten comma two: ten digits in total, with two after the decimal point. This avoids using binary floating-point numbers for our sample prices.

## raw
Here are the eight raw orders from our actual run. Ana's name has spaces around it. Order one thousand three has no customer label. The last order has a blank region. One quantity and one price are missing, and another quantity is negative. These are different problems. We should not fix all of them by filling everything with zero. The internal note explains the sample problem, but is not needed in our clean result.

## select_code
Select chooses the five columns we need. We assign its result to a new variable called selected. A transformation returns a new DataFrame. It does not reach into the original and remove its columns. The assertions check that selected has no internal note, while raw orders still has it. This lets us preserve the original as we build a new result.

## selected
The actual selected result still has all eight rows, but the internal note column is gone. Choosing columns is not the same as filtering rows. Show displays a result and count computes a row count. These are actions that ask Spark to work. Our repeated counts are small teaching checks, not a recommendation to count large datasets unnecessarily in production.

## policy
Before filtering, decide what a valid order means. Here an order needs an identifier, quantity, and price. Quantity must be greater than zero. Price must not be negative. A zero price is allowed, but a missing price is not a free sale. A negative quantity could represent a return in another system. Returns need their own business rule; our simple example rejects them.

## filter_code
Read the condition one part at a time. Is not null checks that a value exists. We combine Spark column conditions with the ampersand, and put each comparison in parentheses. Do not compare a column to Python None with equals. Use is null or is not null. Comparing a missing value to a number does not produce true. Explicit null checks make our required fields clear.

## reject_code
We filter by valid to create accepted, and by the opposite condition to create rejected. The required-field null guards keep this decision explicit. Neither operation changes selected or the original raw orders. Keeping rejected rows separately lets us inspect the problems instead of silently losing the evidence.

## rejected
The actual result has five accepted rows and three rejected rows. Order one thousand two is missing its quantity. Order one thousand four has a negative quantity. Order one thousand five is missing its price. Filling the missing price with zero would invent a free sale. Filling missing quantity with zero would invent a quantity. Our rule rejects these rows so they can be investigated.

## label_code
Customer and region are descriptive labels, so we can use a different, explicit policy. Trim removes spaces around the string. When the trimmed value is null or empty, we use Unknown. Otherwise we keep the trimmed value. Finally, uppercase the region so north and NORTH do not become different labels.

## label_explain
Our small helper returns a Spark column expression. It is not a Python user-defined function running row by row. Spark's built-in trim, when, lit, and upper expressions do the work. With column returns a new DataFrame with an added or replaced column. It does not mutate accepted. We assign this new result to labeled.

## labels
In the actual cleaned labels, Ana no longer has surrounding spaces. The missing customer is now Unknown. The blank region is UNKNOWN after uppercasing. Notice that we are not claiming Unknown is a real customer's name or a real region. It is simply our explicit label for missing information.

## total_code
Now multiply quantity by unit price and name the derived column order total. We cast it to decimal twelve comma two. That size fits these tiny sample values. Production currency pipelines need precision, rounding, and overflow rules based on real values. Order by order identifier makes the displayed rows easy to follow. It does not guarantee the physical order of a saved table.

## derived
The actual clean result has five orders. Two units at ten gives twenty for Ana. The next totals are twenty-two fifty, twelve, eight fifty, and three. Together they add up to sixty-six. The checks confirm eight original rows, five clean rows, and three rejected rows. The original raw DataFrame still has no order total column. Each step built a new result rather than changing the original in place.

## destination_code
Before writing, look carefully at the destination. The notebook uses the currently selected catalog and creates a new schema beginning with b t clean orders, followed by a random suffix. The table name is clean orders inside that unique schema. Fully qualifying and quoting the name avoids accidentally using another schema. The catalog must allow schema creation. If permission is denied, stop and choose a permitted catalog; do not bypass permissions.

## destination_live
Here is the destination from the actual run. The catalog is workspace, and the schema has a unique suffix. Keep this printed full name if you want to inspect this exact demo later. Your suffix will be different. We use a managed table, so there is no external location, mount path, or storage credential to configure.

## write_code
The write specifies Delta format and error mode. Error means fail if the table already exists, not overwrite and not append. We use the exact spelling error because it works with this serverless Spark Connect client. The longer alias was rejected by the client during testing. This correction keeps the same fail-if-existing safety. There is no need to change to overwrite.

## write_live
This actual run successfully created the new managed Delta table. If you rerun only this write cell after creation, it is intended to refuse to replace the existing table. Skip the write and run verification instead. We did not overwrite or drop anything. Running the whole notebook again creates another tiny demo schema, so do not rerun setup just to refresh a result.

## verify_code
An in-memory result is not enough. Read the stored table with SQL using its full name. The first query selects the clean columns and orders by order identifier. The second counts rows and sums order total. Use order by whenever the presentation order matters, even if an earlier DataFrame was ordered.

## verified_rows
Here are the persisted rows from the actual Free Edition run. The identifiers are one thousand one, one thousand three, one thousand six, one thousand seven, and one thousand eight. Their labels, quantities, prices, and totals match the cleaned DataFrame. This checks the stored result, not just a variable still in the notebook.

## verified_summary
The SQL summary reports five rows and total sales of sixty-six. The assertions passed, and the notebook printed its pass message. All eight code cells completed after the write-mode correction. The notebook also prints a SQL statement with the real table name, ready to copy into a SQL cell. The tiny demo objects remain available for inspection; this notebook does not delete them for you.

## practice
Now try a small challenge without writing another table. Start from clean orders. Keep NORTH orders whose total is at least fifteen. Select only order identifier, customer, and total. Show the result ordered by the identifier. Pause and predict the answer before running. You should keep Ana's order for twenty, not Fay's smaller order. This is a practice prediction, not another screenshot from our run.

## cheatsheet
Here is the pattern. Select chooses columns. Filter chooses rows. Is null and is not null help you reason about missing values. With column derives or replaces a column in a new DataFrame. Order by controls result presentation. A Delta write persists the data, and a separate read checks what was saved.

## end
Keep raw input separate from clean results. Make null handling a business decision, not a blind fill. Prefer built-in functions when they fit. Write to a clearly scoped demo table and refuse accidental replacement. Finally, verify the stored result before calling the work complete. The companion notebook and Python source contain the full lesson and practice challenge. This is Build and Teach.