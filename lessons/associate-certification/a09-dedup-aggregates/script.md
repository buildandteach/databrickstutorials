# A09 Deduplication and aggregates — narration script

Databricks Data Engineer Associate exam prep, Lesson A09. Exam guide Aug 2026, Section 3 Data Transformation and Modeling (22%).

## 01 hook
Exam question. Order fifty oh two has two rows. Thirty dollars, then a late update to thirty-six. You drop duplicates on order i d. Which amount does Spark keep?

## 02 welcome
Welcome to Build and Teach. Today we'll learn how to deduplicate a DataFrame in PySpark, keep the latest change for each key, and aggregate it with the right kind of count.

## 03 follow
To follow along, grab the notebook and the cheat sheet from the Build and Teach databricks tutorials repo on GitHub. The link is in the description.

## 04 objectives
This is lesson nine of our Databricks Data Engineer Associate series. It covers one objective in section three, Data Transformation and Modeling, which is twenty-two percent of the exam. Perform data deduplication operations and aggregate operations on DataFrames, such as count, approximate count distinct, and mean, summary. Everything today runs in Databricks Free Edition, in a Python notebook on serverless compute. The results on screen are the expected results. The code was checked with a local Apache Spark run, not on Databricks.

## 05 recap
A quick recap. Lessons A one through A six covered the platform and ingestion into bronze. Lesson A seven cleaned bronze into silver. And last time, in lesson A eight, we combined tables with joins and unions. A union of two files is one of the easiest ways to get duplicates, so today we remove them, and then aggregate. The earlier lessons are linked in the description.

## 06 scenario
Here's the on the job version. The order system sends a change data capture feed, or C D C feed, called orders C D C. Every time an order is created or changed, the whole row is sent again, with a change timestamp. Rows can arrive late, and the same file is sometimes sent twice. Your job is to keep one row per order, the latest change, and then report the daily order count and the average order value. If you get the dedup wrong, the dashboard is wrong, and nothing errors.

## 07 data
Here's the feed. Eight rows, in arrival order. Each row has an order i d, a customer i d, the order date, the amount, the operation, insert or update, and the change T S column, which is the change timestamp. All of the data is fictional.

## 08 kinds
Three kinds of duplicates are mixed in. Order fifty oh three is an exact copy. Every column matches. Order fifty oh one has an update on the same day. The amount went from twenty-four dollars to twenty-eight fifty. And order fifty oh two has a late update. It belongs to November second, but the change to thirty-six dollars arrived on November fourth. The goal is five orders, each with its latest change.

## 09 setup
Create a Python notebook on serverless. Cell one imports the functions module as F, and the Window class. Then it creates a new schema in the workspace catalog. Its name is b t underscore dedup, plus twelve random hex characters, so nothing else in your workspace is touched.

## 10 table
Cell two holds the eight rows as plain values. Then it sets the types. Order date becomes a date, amount becomes decimal ten comma two, and change T S becomes a timestamp. It saves the managed table orders C D C, and reads it back. Expect eight rows.

## 11 distinct
Let's start with distinct. Distinct removes rows that are identical in every column. Drop duplicates with no arguments does exactly the same thing. Both return seven. Only the exact copy of order fifty oh three is gone. Orders fifty oh one and fifty oh two still have two rows each, because their amounts and timestamps differ.

## 12 dd_key
Drop duplicates also takes a list of columns. Then it keeps one row per value of those columns. On order i d, that's five rows, one per order. Looks right. But look at which rows survived. In the local Spark run, order fifty oh one kept the old twenty-four dollar row, and order fifty oh two kept the new thirty-six dollar row. One old, one new. Your run may keep different rows.

## 13 why
That's the trap. Drop duplicates has no argument for sort order, so you can't ask it for the latest row. In the query plan, Spark runs it as an aggregation that takes the first value of each column, per key. The PySpark docs say first is non-deterministic, because row order can change after a shuffle. And sorting the DataFrame before drop duplicates doesn't turn that into a guarantee. If you need a specific row, you have to choose it yourself.

## 14 wrongkeys
The key matters just as much. Here are two easy mistakes. Deduplicate on customer i d, and you get four rows. Customer one oh one placed two orders, fifty oh one and fifty oh four, so one real order is silently deleted. Now make the key too wide, order i d plus change T S. Every update looks unique, so you get seven, and only the exact copy is removed.

## 15 rn_why
To keep the latest row per key, number the rows inside each key, newest first, and keep number one. That's the row number window function. Drop duplicates keeps a row. Row number lets you say which row.

## 16 rn_code
Cell six builds the window. Partition by order i d, and order by change T S, descending. Row number gives one, two, three, and so on, within each order, in that sort order. So the latest change gets number one. We keep row one, and drop the helper column. Expect five rows. Here's the expected result. Both updates won. Order fifty oh one is twenty-eight fifty, and order fifty oh two is thirty-six. And the late update for fifty oh two still lands on its real order date, November second.

## 17 answer_hook
So the answer to the opening question is D, not guaranteed. Drop duplicates keeps one row for order fifty oh two, but it could be thirty or thirty-six, and you can't tell it which. To keep the latest change every time, use row number over a window, ordered by change T S descending, and keep row one.

## 18 memory
Here's the memory trick. Drop duplicates keeps a row. Row number keeps the right row.

## 19 details
Two details worth knowing. First, ties. If two rows for one key can share the same change T S, add a second column to the order by, as a tiebreaker. In our data, the only tie is the exact copy of fifty oh three, so either row gives the same result. Second, streaming. On a streaming DataFrame, drop duplicates keeps every key it has seen as state, across triggers. The docs recommend with watermark, to limit how late duplicates can be, so that state doesn't grow forever.

## 20 sql
Here's the same thing in SQL, in cell seven. The inner query adds row number, partitioned by order i d and ordered by change T S descending. The outer query keeps row one, then groups by order date. This also gives us the daily report, so let's look at that next.

## 21 daily
Cell eight is the PySpark version. Aggregate after you dedupe, never before. Group the latest rows by order date, count the rows, and take the average amount, rounded to two decimals. Here's the expected result. November second has two orders, with an average of thirty-two point two five. That's twenty-eight fifty plus thirty-six, divided by two. November third has three orders, with an average of twenty-three point eight three.

## 22 raw
Now run the same aggregation on the raw feed, in cell nine. Both days are wrong. November second shows four orders, because two orders are counted twice, and the average of twenty-nine point six three mixes in the stale twenty-four and thirty dollar amounts. November third counts order fifty oh three twice. Nothing errors. The dashboard is just wrong. One more detail. In PySpark, A V G and mean are the same function, and both skip null values.

## 23 counts_table
Next, three counts the exam likes to mix up. Count star counts rows. Count of a column counts the non null values in that column. Count distinct is exact, and it also skips nulls. It also has a camel case alias, and in SQL it's written as count of distinct. And approximate count distinct is an estimate.

## 24 counts_code
Cell ten runs all of them on the raw feed. Expect eight rows, eight order i d values, five distinct orders, and an approximate count of five. On five keys, the estimate happens to be exact.

## 25 nulls
Cell eleven adds one row with a null order i d. Now count star is nine, but count of order i d is still eight, and count distinct is still five. Count of a column, and count distinct, both skip nulls.

## 26 approx
On larger data, the estimate usually isn't exact. Cell twelve counts one hundred thousand distinct I Ds. In the local run, approximate count distinct returned ninety-five thousand, five hundred forty-six. With the second argument set to zero point one, it returned one hundred two thousand and sixty-five. Count distinct returned exactly one hundred thousand. Databricks documents approximate count distinct as using the HyperLogLog plus plus algorithm, accurate within about five percent by default. The second argument, r s d, is the maximum relative standard deviation. The default is zero point zero five. Use the estimate when a close answer on very large data is fine, like unique visitors on a dashboard. Use count distinct when the number must be exact, like the customer count on an invoice. And if you need an r s d below zero point zero one, the PySpark docs say count distinct is the more efficient choice.

## 27 summary
For a quick profile of a column, describe returns count, mean, standard deviation, min and max. Summary returns the same, plus the twenty-five, fifty and seventy-five percent percentiles. You can also pass just the statistics you want. Here's the expected summary of the amount column. Five rows, a mean of twenty-seven point two, a min of thirteen fifty, and a max of forty. The percentiles are approximate. The docs describe summary as meant for exploratory analysis, so use group by and agg for numbers that feed a report.

## 28 checklist
Here's the practical takeaway, a deduplication checklist. One, the business key. What makes a row unique for the business? Here it's order i d. Not customer i d, and not order i d plus change T S. Two, the sort. Which column says which row is newest? Here it's change T S. Add a tiebreaker if two rows can share it. Three, keep the latest. Use row number over a window partitioned by the key and ordered by the sort column descending, and keep row one. Use drop duplicates on a key only when any surviving row is acceptable, like exact copies. Four, check the counts. The row count after deduplication should equal the count distinct of the key before it. Here, both are five. Five, then aggregate. Count, average and sum on the deduplicated DataFrame.

## 29 snippet
And here's the reusable snippet. Keep latest takes a DataFrame, a list of key columns, and a timestamp column. It builds the window, keeps row one, and drops the helper column. Then we check the counts. The rows after deduplication must equal the distinct keys before it. Five equals five, so the assert passes. And then we aggregate. Copy this into any job that reads a change feed.

## 30 examq
Now an exam style question, one I wrote myself. A data engineer must report the exact number of distinct customers who placed an order last month, for an invoice to a partner. The orders table has two billion rows. Which expression should they use? A. Approximate count distinct of customer i d. B. Count of customer i d. C. Count distinct of customer i d. D. Drop duplicates on order i d, then count. Pause the video if you'd like to think.

## 31 examans
The answer is C. The number must be exact, and distinct, so count distinct is the right function. A is an estimate, accurate within about five percent by default. That's fine for a dashboard, but not for an invoice. B counts every non null customer i d, so a customer with ten orders counts ten times. And D counts unique orders, not customers.

## 32 challenge
Your practice challenge. Question one. How many rows does orders C D C return, if you filter to insert rows, and then drop duplicates with no arguments? Question two. Using orders latest, what is customer one oh one's total amount? Question three. What does count distinct of customer i d return on orders latest? Pause the video and write down your three answers.

## 33 answer
Here's the explanation. Question one. Five. There are six insert rows, and one of them is the exact copy of fifty oh three, so five remain. Question two. Sixty-eight fifty. Customer one oh one has order fifty oh one, at its latest amount of twenty-eight fifty, plus order fifty oh four, at forty dollars. Question three. Four. Customers one oh one, one oh two, one oh three, and one oh four. Customer one oh one has two orders, but it's counted once.

## 34 close
Let's recap. Distinct, and drop duplicates with no arguments, remove only exact copies. Drop duplicates on a key keeps one row per key, but you can't choose which. Pick the key carefully. Too narrow deletes real rows, and too wide keeps the duplicates. To keep the latest change, use row number over a window, and keep row one. Dedupe first, then aggregate. Count star counts rows, count distinct is exact, and approximate count distinct is an estimate. And the memory trick. Drop duplicates keeps a row. Row number keeps the right row. Next time, in lesson ten, we build the gold layer with views, materialized views and streaming tables. For a slower walk through group by and window functions, the Foundations video is linked in the description. The notebook and cheat sheet are in the Build and Teach databricks tutorials repo on GitHub. Thanks for watching Build and Teach.
