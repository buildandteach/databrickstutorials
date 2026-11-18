# A07 Bronze to silver cleaning — narration script

Databricks Data Engineer Associate exam prep, Lesson A07. Exam guide Aug 2026, Section 3 Data Transformation and Modeling (22%).

## 01 hook
Exam question. On serverless compute, one bronze amount says N slash A. You cast the column to decimal ten comma two. What happens?

## 02 welcome
Welcome to Build and Teach. Today we'll learn how to clean a bronze table into a new silver table, with PySpark and SQL.

## 03 follow
To follow along, grab the notebook and the cheat sheet from the Build and Teach databricks tutorials repo on GitHub. The link is in the description.

## 04 objectives
This is lesson seven of our Databricks Data Engineer Associate series. It covers two objectives in section three, Data Transformation and Modeling, which is twenty-two percent of the exam. First, implement data cleaning. Read bronze tables with PySpark or SQL, clean nulls, standardize data types, and write new silver tables. Second, manipulate columns, rows, and table structures. Add, drop, split, and rename columns, and apply filters. Everything today runs in Databricks Free Edition, in a Python notebook on serverless compute. The results on screen are the expected results. The code was checked with a local Apache Spark run, not on Databricks.

## 05 recap
A quick recap. Lesson A one toured the platform. A two through A four loaded data into bronze, with batch and streaming patterns, copy into, and Auto Loader. A five flattened nested JSON with explode, and A six picked an ingestion method. All of that ends in bronze. Today we take it one layer further. The earlier lessons are linked in the description.

## 06 scenario
Here's the on the job version. Every night, an orders feed lands in a bronze table, with every column stored as a string. Some nights, a few amounts say N slash A, and some fields are missing. Your job is to make sure the silver table still loads by morning, and that you can say exactly how many rows were rejected, and why. That's the same skill the exam tests, so let's build it.

## 07 layers
First, the layers. In the medallion architecture, bronze is raw, silver is validated, and gold is enriched for business use. Databricks recommends storing most bronze fields as strings, so an unexpected value never drops a record. Silver is where you clean. You handle nulls, cast types, enforce the schema, and run quality checks. And you build silver by reading from bronze and writing a new table, not by cleaning bronze in place.

## 08 setup
Create a Python notebook on serverless. Cell one imports the functions module as F, and creates a new schema in the workspace catalog. Its name is b t underscore silver, plus twelve random hex characters, so nothing else in your workspace is touched.

## 09 bronze
Cell two builds the bronze table from six fictional orders. Each row has one problem. Ana's name has extra spaces. Ben's amount says N slash A. Order ten oh three has no customer. One row has no order i d. Order ten oh five has no amount. And city and state share one column. Every column is a string, like a real bronze table. We also add an empty rescued data column, the kind of metadata column Auto Loader adds, as we saw in lesson A four. Then we write bronze orders, and read it back with spark dot read dot table. Here's the expected bronze table. Six rows, all strings, and rescued data is null on every row. Keep that last detail in mind. It matters in a minute.

## 10 cast
Now trap one. The obvious way to type the amount is cast. Cell three casts amount to decimal ten comma two, inside a try block, so the notebook keeps running. Expect an error. Cast invalid input. The value N slash A cannot be cast, because it is malformed. One bad value, and the whole query fails.

## 11 hook_answer
And that's the answer to our opening question. C, the query fails. Serverless compute runs with American National Standards Institute SQL mode, or ANSI mode, on by default. In ANSI mode, an invalid cast raises an error, instead of quietly returning null. ANSI mode is also on by default in Apache Spark four, and in Databricks Runtime seventeen and above. On classic compute with older runtimes, the same cast quietly returned a null value, with no error. That's answer A, the old behavior, and it's why code that worked on an old cluster can fail when it moves to serverless. B and D never happen.

## 12 try_cast
Try cast is the tolerant version. If a value is malformed, it returns null instead of an error. Cell four uses it in an expression, so now order ten oh two has a null amount, and the job keeps going. But a null from try cast hides the original value. So before you filter anything out, count the bad rows. Keep rows where the raw amount is not null, but try cast is null. Expect one, order ten oh two. One more detail. Try cast only forgives bad values. If the two types can't be cast at all, you still get an error.

## 13 dropna
Trap two. Drop N A with no arguments uses how equals any. It drops a row if any column is null. Every row here has a null rescued data, so this returns zero rows. Your silver table would be empty. Name the columns that must not be null, with subset. With subset equals order i d, expect five rows. How equals all drops a row only when every column is null. And thresh keeps rows with at least that many non-null values.

## 14 fillna
Trap three. Fill N A with zero fills nulls in numeric columns only. Customer name is a string, so it's simply ignored. Order ten oh three still has a null customer, and there's no error to warn you. Use a dictionary instead, to say exactly which column gets which value. With customer name mapped to unknown, expect zero nulls left.

## 15 pipeline
Now the full pipeline, in one chain, where each line maps to a verb in the objective. Drop rows with no order i d. Cast order i d to an integer, and amount with try cast. Parse the order date as a date. Trim the spaces from the customer name, then fill the missing one with unknown. Then the column work. With column adds city and state, by splitting city state on comma space, and picking item zero and item one. Drop removes city state and rescued data. With column renamed turns customer name into customer. And filter keeps only rows with an amount. Finally, write it as a new managed table, silver orders.

## 16 silver_out
Here's the expected silver table. Three rows. Ten oh one, with Ana's name trimmed. Ten oh three, with customer unknown. And ten oh six. The schema is typed now. Order i d is an integer, amount is a decimal, and order date is a date. Six bronze rows became three. The missing order i d was dropped first. Then ten oh two, with N slash A, and ten oh five, with no amount, failed the filter.

## 17 details
A few details the exam likes. With column adds a column, or replaces it if the name already exists. That's how the casts overwrite order i d, amount, and order date in place. Split takes a regular expression pattern, and returns an array. Get item zero is the first element. With ANSI mode on, an index past the end of the array raises an error. The get function returns null instead. Drop and with column renamed do nothing if the column doesn't exist. A typo won't fail. It just leaves the column alone. And filter and where are the same thing.

## 18 noop
Cell eight shows that trap. Renaming a column called cust name, which doesn't exist, raises no error. The columns are unchanged, so this prints true. The fix is cheap. Print silver dot columns after the chain, and check you got exactly these six.

## 19 sql
The objective says PySpark or SQL, so here's the SQL version. It's a create table as select statement, or C T A S. Cast and try cast type the columns. Coalesce plays the role of fill N A. Split with square brackets picks the city and the state. The where clause removes the missing i d and the bad amounts. And leaving a column out of the select plays the role of drop. Expect the same three rows.

## 20 rejected
Back to the nightly job. Silver loaded, but your team will ask what was rejected, and why. Cell ten gives every rejected bronze row a reason. No order i d, no amount, or a bad amount that try cast couldn't read. Then it counts them by reason, and writes them to their own table, orders rejected. Databricks' medallion example quarantines invalid records like this. Expect three rejected rows, one for each reason. And the numbers reconcile. Six bronze rows equals three silver rows, plus three rejected rows. That's the line you put in the morning report.

## 21 traps
Five exam traps. Number one. With ANSI mode on, cast fails on a bad value, and try cast returns null. Number two. Drop N A with no subset drops a row if any column is null. Number three. Fill N A with zero ignores string columns. So use a dictionary. Number four. Drop and with column renamed quietly ignore a column that doesn't exist. Number five. Get item past the end of an array fails under ANSI mode, and the get function returns null.

## 22 checklist
Here's the checklist I'd keep next to any bronze to silver job. Read bronze, and write a new silver table. Drop rows only on the columns that are truly required, with subset. Type columns with try cast, then count the nulls it made. Fill with a dictionary, never a bare fill N A with zero. Check the columns after drop and rename. And reconcile. Bronze rows should equal silver rows plus rejected rows. The checklist is in the cheat sheet.

## 23 snippet
And a reusable snippet. Try cast count takes a DataFrame, a column, and a target type. It try casts the column, counts the values that were there but couldn't be read, prints the count, and returns the typed DataFrame plus the bad rows. On our bronze table, expect amount, one bad value. Copy it into any job that types a string column.

## 24 challenge
Your practice challenge. Question one. Remove the filter line from the pipeline. How many rows land in silver orders? Question two. What does drop N A, with how equals all, return on our six rows? Question three. Bronze gains a status column, with the values shipped in lowercase, shipped in capitals, shipped with a leading space, and a null value. In silver, make every value lowercase with no spaces, and fill the null values with unknown. What do you add to the chain? Pause the video and write down your three answers.

## 25 answer
Here's the explanation. Question one. Five rows. Only the missing order i d is dropped, so ten oh two and ten oh five land in silver with a null amount. That's why the filter, and the rejected count, matter. Question two. Six. How equals all drops a row only when every column is null, and no row here is empty. Question three. Before the fill step, add a with column call for status, using F dot lower, around F dot trim of status. Then map status to unknown in the fill N A dictionary. Trim and lower return null for a null input, so the fill still works.

## 26 close
Let's recap. Silver is a new table you write from bronze. Use subset with drop N A, and a dictionary with fill N A, and check your columns after drop and rename. And the memory trick. Cast fails loud. Try cast fails quiet. A quiet null still needs a count. Next time, in lesson eight, we join and union DataFrames, including broadcast and cross joins. For a slower walk through select, filter, and nulls, the Foundations video on cleaning data with PySpark is linked in the description. The notebook and cheat sheet are in the Build and Teach databricks tutorials repo on GitHub. Thanks for watching Build and Teach.
