# A10 Gold layer: views, materialized views and streaming tables — narration script

Databricks Data Engineer Associate exam prep, Lesson A10. Exam guide Aug 2026, Section 3 Data Transformation and Modeling (22%).

## 01 hook
Exam question. A dashboard shows daily sales by region. Then, on November fourth, a late update changes order fifty oh two. It's a November second order, and its amount goes from thirty dollars to thirty-six. Which gold object keeps November second correct, without writing your own merge logic?

## 02 welcome
Welcome to Build and Teach. Today we'll learn how to choose between a view, a materialized view, a streaming table and a table for the gold layer, and why a late update decides the answer.

## 03 follow
To follow along, grab the notebook and the cheat sheet from the Build and Teach databricks tutorials repo on GitHub. The link is in the description.

## 04 objectives
This is lesson ten of our Databricks Data Engineer Associate series. It covers one objective in section three, Data Transformation and Modeling, which is twenty-two percent of the exam. Understand the difference between, and how to build, gold layer objects such as materialized views, views, streaming tables, and tables, for business intelligence and analytics teams in Unity Catalog. Everything today fits in Databricks Free Edition, on serverless compute. The results on screen are expected results. The views and tables were checked with a local Apache Spark run, not on Databricks. Materialized views and streaming tables can't run locally, so those results are expected, from the Databricks docs.

## 05 recap
A quick recap. Lessons A one through A six covered the platform and ingestion into bronze. Lesson A seven cleaned bronze into silver. Lesson A eight joined tables, and last time, in lesson A nine, we kept the latest change for each order. That gave us a clean silver orders table. Today we serve it to analysts as gold. The earlier lessons are linked in the description.

## 06 scenario
Here's the on the job version. The analysts ask for two things. First, gold dot daily sales by region, for their dashboard. Order count and total sales per day and region. It's read many times a day, so it should be fast. And it must be right after late updates. Second, a way to slice orders ad hoc, by region, date or amount. Always current, without storing another copy of the data.

## 07 objects
There are four gold objects to choose from. A view stores no data. It's recomputed every time someone queries it. A materialized view stores its results, and recomputes them on refresh. A streaming table also stores data, but it processes each new input row once, from an append-only source. And a table changes only when you write to it. For example, a create table as select statement, or C T A S, runs once. Materialized views and streaming tables are both Unity Catalog managed tables. You can define them in a Lakeflow Spark Declarative Pipeline, or create them on their own with SQL. Then Databricks creates and manages a serverless pipeline for you.

## 08 data
Here's the data, continued from lessons eight and nine. Silver customers has four customers. In this small dataset, a customer's state is the region. Ana Lopez and Ben Wu are in Texas, Cara Diaz is in Colorado, and Dev Shah is in Oklahoma. Silver orders holds the latest version of each order, the output of the lesson nine dedupe. This is the end of November third. Order fifty oh two is still at thirty dollars. Its late update hasn't arrived yet. All of the data is fictional.

## 09 setup
Create a Python notebook on serverless. Cell one checks the schemas in the workspace catalog. If a schema called silver or gold already exists, it stops, so nothing of yours is touched. Then it creates the two new schemas, silver and gold.

## 10 tables
Cells two and three are SQL cells. Cell two creates silver customers and inserts the four customers. Cell three creates silver orders, with amount as decimal ten comma two. It also turns on row tracking with a table property. That matters later. Incremental refresh of a materialized view needs Delta source tables with row tracking enabled. Then it inserts the five orders.

## 11 gold_query
Both requests start from the same query, in cell four. Join orders to customers on customer i d. Then group by order date and state, which we call region, and compute the order count and the sum of the amount. Here's the expected result. Four rows. November second, Texas, has two orders, for fifty-eight fifty. That's twenty-eight fifty plus thirty. November third has one order each in Colorado, Oklahoma and Texas.

## 12 view
The second request is the easy one. Cell five creates a regular view, gold dot orders by region. It joins orders to customers, and keeps the order i d, the order date, the region and the amount. Then any analyst can slice it. Here we filter to Texas. Expect three Texas orders. Fifty oh one and fifty oh two from November second, and fifty oh four from November third. A view stores no data, so there's nothing to refresh. Every query re-runs the join on the current silver orders.

## 13 snapshot
To compare the objects, cell six builds the same aggregate two more ways. First as a regular view, gold dot daily sales v. Then as a table, gold dot daily sales snapshot, with create table as select. That stores the result once, as of November third.

## 14 mv
And now the dashboard object. Cell seven creates gold dot daily sales by region as a materialized view, with the same query, and a schedule of every one hour. Create materialized view runs the initial load right away, so right now it's expected to show the same four rows, with fifty-eight fifty for November second in Texas. You can create it from a SQL warehouse, or from a notebook on serverless compute. Behind the scenes, Databricks creates a serverless pipeline that processes the refreshes.

## 15 refresh
Here's what to know about refresh. After the initial load, a materialized view is only as current as its last refresh. You choose how it refreshes. Run refresh materialized view yourself, add a schedule, like every one hour, or a cron schedule, or use trigger on update, to refresh when the source tables change. A refresh is incremental when Databricks can do it that way, and a full recompute when it can't, or when that's cheaper. The refresh runs on a serverless pipeline, not on your SQL warehouse, and its cost scales with the data processed. Dashboard reads hit the stored result, so they're fast and cheap.

## 16 update
Now it's November fourth, and the late update arrives. In notebook cell eight, order fifty oh two goes from thirty dollars to thirty-six. Its order date is still November second. So the correct total for November second in Texas becomes twenty-eight fifty plus thirty-six, which is sixty-four fifty. Notice what kind of change this is. It's an update to a row that already exists, not a new row.

## 17 compare3
Cell nine asks all three objects for November second in Texas. Here's the expected result. The view shows sixty-four fifty, with no refresh step, because it re-ran the query on the current silver orders. The local Spark run confirmed that. The C T A S snapshot still shows fifty-eight fifty. Nothing rewrote it, and nothing ever will. The local run confirmed that too. And the materialized view is expected to still show fifty-eight fifty, because it's current as of its last refresh. If the hourly refresh already ran, you'll see sixty-four fifty.

## 18 mv_refresh
Cell ten refreshes the materialized view now, instead of waiting for the schedule, and reads it. The expected result is sixty-four fifty for November second in Texas. The docs say that each refresh recalculates the results to reflect changes in the source tables, including updates and deletes, not only new rows. The other three rows stay the same.

## 19 trap
Now the trap. It's tempting to build daily sales by region as a streaming table, because streaming sounds fresher. Here's why that breaks. A streaming table processes each input row only once, and it's designed for append-only sources. A row it already processed is not re-read with later updates. The change to order fifty oh two is an update to an existing row in silver orders, not a new row. The Databricks docs say a streaming read of a Delta table throws an error when the source is modified by an update, delete, merge or overwrite. And if you set skip change commits to avoid that error, the change is skipped, not applied. Either way, the dashboard doesn't show sixty-four fifty for November second, until someone runs a full refresh, which reprocesses everything.

## 20 st_vs_mv
The docs say it directly. Use a materialized view when your query performs aggregations or joins, or when the source data can change, with updates and deletes, rather than only growing. They also note that streaming table joins don't recompute when dimensions change, while materialized views do.

## 21 answer_hook
So the answer to the opening question is B, a materialized view. After its next refresh, November second in Texas moves from fifty-eight fifty to sixty-four fifty. A is wrong, because a streaming table reads each row once, and the update stops the stream. C is a snapshot that stays at fifty-eight fifty. And D skips the update, so November second stays wrong.

## 22 memory
Here's the memory trick. Streaming tables read each row once. Materialized views recompute the answer.

## 23 st_fit
So where does a streaming table fit? Upstream, where data only grows. New order files land in a volume, and each file should be read exactly once. This is the expected pattern from the create streaming table reference. Stream read files uses Auto Loader to pick up only new files, and each row is appended once. For sources that send updates and deletes, the docs point to change data capture, or C D C, with auto C D C flows into a streaming table, instead of a plain append.

## 24 free
One Free Edition detail. Every standalone materialized view or streaming table is backed by a pipeline. And Free Edition documents a limit of one active pipeline per pipeline type. So in the notebook, the streaming table cell is commented out, and it's shown as an expected pattern. When you're done, run the cleanup cell. Dropping the schemas also drops the materialized view and its hourly schedule.

## 25 checklist
Here's the practical takeaway, an object choice checklist. Four questions before you build a gold object. One, latency. Must every read be fully current? A view is always current. A materialized view is current as of its last refresh. A streaming table has low latency for new rows. Two, refresh. Who updates it? A view needs no refresh. A materialized view refreshes manually, on a schedule, or on source updates. A streaming table processes new input on each update. And a table changes only when you write to it. Three, late data. Can old rows change, through updates, deletes or corrections? If yes, and the object aggregates or joins, choose a materialized view. Four, cost. How often is it read? A view pays for the full query on every read. A materialized view pays at refresh time, often incrementally, and reads are cheap.

## 26 rule
Rule of thumb for the medallion layers. Streaming tables for append-only ingestion into bronze. Materialized views for gold aggregates that must stay correct. Regular views for light, ad hoc slices. And a table when you want a snapshot that never changes.

## 27 snippet
And here's the reusable snippet, one of each. A streaming table for append-only files into bronze, and a regular view for light ad hoc slices in gold. And the materialized view for the gold aggregate that must reflect late updates, refreshed every hour. All three are in the cheat sheet.

## 28 examq
Now an exam style question, one I wrote myself. A data engineer must load new JSON order files, that land in a Unity Catalog volume every few minutes, into a bronze table. Files are never changed after they land, and each file must be processed only once, with low latency. Which object fits best? A. A regular view over the volume. B. A materialized view that refreshes every hour. C. A streaming table that reads the files with stream read files. D. A table created once with create table as select. Pause the video if you'd like to think.

## 29 examans
The answer is C, a streaming table. The source only grows, every row should be handled once, and latency matters. That's exactly what streaming tables are designed for. A stores nothing, and would re-read every file on each query. B recomputes on a schedule, which adds latency, and it's meant for aggregations, joins, and sources that change. And D is a one-time snapshot that misses every file that lands later.

## 30 challenge
Your practice challenge. Question one. After the November fourth update, what is the sum of the amount in gold dot orders by region, for Texas? Question two. Finance wants the November totals frozen at month end close, never changed by later corrections. Which object should you use? Question three. Order fifty oh six, for customer one oh four, on November fourth, for twenty-five dollars, is inserted. Then the materialized view refreshes. How many rows does it have, and what is the total for November second in Texas? Pause the video and write down your three answers.

## 31 answer
Here's the explanation. Question one. One hundred four fifty. That's twenty-eight fifty, plus thirty-six, plus forty. The view re-runs on the updated row, so it already uses thirty-six. Question two. A table, created with create table as select. It's a snapshot, so later updates never change it. That's the behavior we saw in cell nine. Question three. Five rows. The refresh adds one new line to the result, for November fourth in Oklahoma, with one order and twenty-five dollars. And November second in Texas is still sixty-four fifty.

## 32 close
Let's recap. A view stores no data, and is recomputed on every read. A materialized view stores its results, and recalculates them on each refresh, so aggregates and joins stay correct after updates and deletes. A streaming table processes each input row once, and expects an append-only source. Use it for ingestion, not for aggregates that must reflect late updates. And a table changes only when you write to it. Choose with the checklist. Latency, refresh, late data, and cost. And the memory trick. Streaming tables read each row once. Materialized views recompute the answer. Next time, in lesson eleven, we add data quality checks, so silver and gold stay reliable. The notebook and cheat sheet are in the Build and Teach databricks tutorials repo on GitHub. Thanks for watching Build and Teach.
