# A11 Data quality checks: expectations, quarantine and constraints — narration script

Databricks Data Engineer Associate exam prep, Lesson A11. Exam guide Aug 2026, Section 3 Data Transformation and Modeling (22%).

## 01 hook
Exam question. A pipeline loads the November fourth orders into silver. Its expectation says the amount must be zero or more. There is no on violation clause. Order fifty oh eight arrives with an amount of minus fifteen. Is it kept out of the table?

## 02 welcome
Welcome to Build and Teach. Today we'll learn how data quality checks keep bad rows out of silver and gold, and what warn, drop and fail really do with them.

## 03 follow
To follow along, grab the notebook and the cheat sheet from the Build and Teach databricks tutorials repo on GitHub. The link is in the description.

## 04 objectives
This is lesson eleven of our Databricks Data Engineer Associate series. It covers one objective in section three, Data Transformation and Modeling, which is twenty-two percent of the exam. Apply data quality checks and validation rules to ensure reliable silver and gold datasets. Everything today fits in Databricks Free Edition. The quarantine code and the table constraints were checked with a local Apache Spark run, with open source Delta Lake, not on Databricks. On screen, that's marked observed. Pipeline expectations can't run locally, so those results are marked expected, from the Databricks docs.

## 05 recap
A quick recap. Lesson A seven cleaned bronze into silver, with with column, drop and filter. We'll reuse those today. Lessons A eight and A nine joined and deduplicated the orders. And last time, lesson A ten built gold objects on top of silver orders. If a bad row reaches silver, every gold object built on it is wrong too. So today we guard silver. The earlier lessons are linked in the description.

## 06 scenario
Here's the on the job version. On November fourth, a new batch of five orders lands in bronze. The rule from the business. Reject orders with a null customer i d or a negative amount, and don't let an order for an unknown customer reach silver either. But the broken rows must not disappear, because someone has to fix them. Only two orders should reach silver orders. Fifty oh six and fifty ten.

## 07 data
Here's the data, continued from lessons eight to ten. Silver customers has four customers, one oh one to one oh four. Silver orders holds orders fifty oh one to fifty oh five, from November second and third, with order fifty oh two already at its updated thirty-six dollars. The new batch is bronze dot orders nov four. Five orders, all dated November fourth. Fifty oh six and fifty ten are fine. Fifty oh seven has a null customer i d. Fifty oh eight has an amount of minus fifteen. And fifty oh nine is for customer one oh five, who doesn't exist. All of the data is fictional.

## 08 checklist_intro
Most validation rules fall into three groups. One, not null. Required keys are present, like customer i d is not null. Two, range. Values are possible, like an amount of zero or more, or a date that isn't in the future. Three, referential. Keys point to something real. Every customer i d in orders must exist in customers. For each rule, decide the action before you write any code. Warn, when the problem is soft, and the row is still useful, but you want to count how often it happens. Drop, when the row is useless downstream, and nobody needs to review it. Quarantine, when the row is wrong, but someone has to fix it. And fail, when the whole batch can't be trusted, like a broken file or a missing key column.

## 09 setup
Create a Python notebook on serverless. Cell one checks the schemas in the workspace catalog. If bronze or silver already exists, it stops, so nothing of yours is touched. If you ran lesson ten, run its cleanup cell first. Then cell one creates the two new schemas.

## 10 tables
Cells two, three and four are SQL cells. Cell two creates silver customers, with the four customers. Cell three creates silver orders, with amount as decimal ten comma two, and inserts the five earlier orders. Cell four creates the new batch. That's the bronze table, orders nov four. It holds the five orders from November fourth.

## 11 profile
Before fixing anything, measure. Cell five runs the checklist as one query. It left joins the batch to customers, and counts each problem. Count if customer i d is null. Count if the amount is below zero. And count the rows that have a customer i d, but no match in customers. In the local Spark run, we observed five rows in, and one of each problem. Three bad orders out of five. Now, what would a pipeline expectation do with them?

## 12 expect
In a Lakeflow Spark Declarative Pipeline, a data quality check is called an expectation. It's a named constraint on a streaming table, a materialized view, or a view. The constraint is a SQL Boolean expression, checked on every record. The action decides what happens to a record that fails. Warn is the default. Invalid records are written to the target, and the violation is counted in the data quality metrics. Drop row drops invalid records before the write, and logs the dropped count. And fail update fails the update, so someone has to step in before it's reprocessed. In Python, the same three are d p dot expect, expect or drop, and expect or fail.

## 13 pipe_sql
Here's the drop version, as a pipeline source file. It's a streaming table on the bronze orders raw table from lesson ten, with two constraints. Valid customer i d expects that customer i d is not null. Valid amount expects the amount to be zero or more. Both drop the row on violation. This is the expected pattern from the docs. It runs in a pipeline, not in our notebook. The same thing in Python. Import pipelines as d p, from pyspark. Then the table decorator, and one expect or drop decorator per rule, each with a name and the same SQL expression.

## 14 expected
Here are the expected results for the November fourth batch, based on the docs. Warn, if you remove the on violation clauses, writes all five rows, including fifty oh seven and fifty oh eight. The metrics show one failure for each expectation. Drop writes three rows. Fifty oh six, fifty oh nine and fifty ten. Two records are dropped. And notice order fifty oh nine. It still gets through, because no rule checks that customer one oh five exists. Fail stops at the first invalid record, and the table update is rolled back. So none of the five rows is written, not even the good ones.

## 15 traps
So there are two traps. The first is a check that only warns, while bad rows still reach gold. The dashboard looks fine, and a negative order flows into the totals. The second is failing the whole load over two bad orders. Two more details for the exam. A constraint can't contain a subquery that references another table, so a referential check needs a join before the expectation. And metrics are recorded for warn and drop. A fail stops the update, so there are no metrics for it. For a standalone streaming table or materialized view, created in Databricks SQL, query the pipeline event log for these metrics.

## 16 answer_hook
So, the answer to the opening question is C. Order fifty oh eight is not kept out. It's written to the target table. An expectation with no action uses the default, warn, which writes invalid records and only counts the violation. To keep it out, add on violation drop row. Or use on violation fail update, to stop the update. And there's no automatic quarantine table. Quarantine is a pattern you build yourself.

## 17 memory
Here's the memory trick. Warn writes. Drop drops. Fail stops.

## 18 quarantine
Dropping loses the rows. Failing loses the whole batch. When the bad rows need follow up, split them instead. This is the reusable filter or quarantine snippet. It's plain PySpark, so it runs in the notebook, and we ran it locally. Cell six starts with the two inputs. The new orders, and a small lookup of known customers. Select customer i d, and add a column called known customer, set to true. Then one reason column. Orders get a left join to the lookup, on customer i d. With column adds a column called d q reason, using when. A null customer i d comes first. Then a bad amount, which is null or below zero. Then unknown customer, when known customer is null, because that customer had no match. Rows with no problem keep a null reason. Finally, drop the helper column. That's the with column, drop and filter from lesson seven, used as a quality gate.

## 19 split
Cell seven splits on the reason. Clean is every row with a null reason, without the reason column. Quarantine is every row with a reason. And the assert proves nothing was lost. Rows in must equal clean plus quarantine. Cell eight appends the clean rows to silver orders, with the same four columns. And it appends the quarantined rows, with their reason, to a new table, silver orders quarantine.

## 20 observed
Here's what we observed in the local Spark run. Clean has fifty oh six and fifty ten. Quarantine has three rows. Fifty oh seven, null customer i d. Fifty oh eight, bad amount. And fifty oh nine, unknown customer. Five in, two clean, three quarantined. Silver orders now has seven rows, and silver orders quarantine has three.

## 21 why
Why is the snippet written this way? One reason column, then two filters. Each row gets exactly one label, so every row lands in exactly one table. And the left join is the referential check. Customer one oh five has no match, so known customer is null. And nulls are handled on purpose. Cell nine shows why. Order fifty eleven has a null amount. A filter keeps only rows where the condition is true. For a null amount, amount is zero or more is null, and so is its negation. In the local run, the row passed neither filter. We observed zero and zero. So a naive good or bad split would silently lose it. That's why the snippet checks is null first. The docs describe the same quarantine idea for pipelines. One table with an is quarantined flag, plus a view for the valid rows and a view for the invalid rows.

## 22 constraints
Expectations live in pipelines. A Delta table can also guard itself, with enforced constraints, whoever writes to it. In notebook cell ten, we set customer i d to not null, and add a check constraint called non negative amount. Both commands first verify that every existing row passes. Silver orders only has clean rows, so both should succeed. One local detail. Open source Spark rejects alter column set not null, so the local run declared not null in the table schema instead. The alter form is the documented Databricks syntax.

## 23 bad_insert
Notebook cell eleven tries two inserts. The first writes two orders in one statement. Fifty oh eight, with minus fifteen, and fifty twelve, a valid order for nine dollars. The second writes fifty oh seven, with a null customer i d. In the local Delta run, we observed both inserts fail. The first with delta violate constraint with values, and the second with delta not null constraint violated. The table still had seven rows, and fifty twelve was not written either. So a check constraint behaves like fail, not like drop. Use constraints as the last guard on silver and gold, and quarantine bad rows before the write, so one bad order doesn't block the rest. And primary key and foreign key constraints on Databricks are informational only. They're not enforced, so they don't replace a referential check.

## 24 free
A Free Edition note. Expectations are a Lakeflow pipeline feature. Serverless pipelines run on the Advanced product edition, which includes expectations. And Free Edition allows one active pipeline per pipeline type. The quarantine code and the constraints run right in the notebook, on serverless compute. When you're done, run the cleanup cell. It drops the two schemas it created.

## 25 checklist
Here's the practical takeaway, the quality checklist. Before you load silver, ask these six questions. One, not null. Are the required keys present? Two, range. Are the values possible? Three, referential. Does every key exist? Check it with a join, not a subquery. Four, the action. Warn, drop, quarantine or fail. Five, do the counts add up? Rows in must equal clean plus quarantine. And six, add not null and check constraints on silver and gold, as the last guard.

## 26 snippet
And here's the reusable snippet, in three steps. Label every row with one reason. Split on the reason, into clean and quarantine. Then assert that the counts add up, before you write anything. Swap in your own rules, and it works for any table. The full version is in the notebook and the cheat sheet.

## 27 examq
Now an exam style question, one I wrote myself. A data engineer wants every order in a pipeline's silver orders table to have a matching customer in silver customers. They write an expectation called known customer, where customer i d is in a subquery on silver customers. What's the best fix? A. Add on violation drop row to the same constraint. B. Declare a foreign key on customer i d, because Databricks enforces it on every insert. C. Left join orders to customers in the dataset query, select the customer's i d as matched customer i d, and expect matched customer i d is not null. D. Switch to d p dot expect, because Python expectations allow subqueries. Pause the video if you'd like to think.

## 28 examans
The answer is C. Expectation constraints can't contain subqueries that reference other tables, so the lookup has to happen in the query first. After a left join, an unknown customer shows up as a null matched customer i d, and a plain expectation on that column catches it. The docs' missing record pattern works the same way. A keeps the invalid subquery. B is wrong, because foreign keys on Databricks are informational and not enforced. And D is wrong, because the same constraint rules apply in Python.

## 29 challenge
Your practice challenge. Question one. With the drop row version of the pipeline, which bad order still reaches silver, and why? Question two. Silver orders has a check constraint, amount zero or more. One insert writes order fifty oh eight with minus fifteen, and order fifty twelve with nine dollars. What's written? Question three. Order fifty eleven has a null amount. You split the batch with two filters, amount zero or more, and not amount zero or more. Which side does it land on? Pause the video and write down your three answers.

## 30 answer
Here's the explanation. Question one. Order fifty oh nine, for customer one oh five. The drop version only checks for a null customer i d and a negative amount. No rule checks that the customer exists, so it's expected to land, with fifty oh six and fifty ten. Question two. Neither row. A check violation fails the whole transaction. In the local run, the table stayed at seven rows, and fifty twelve was not written. Question three. Neither side. For a null amount, amount is zero or more is null, and so is its negation, so both filters skip it. We observed zero and zero. That's why the snippet checks is null first, and puts the row in quarantine as a bad amount.

## 31 close
Let's recap. Pick your checks from the checklist. Not null, range, and referential. Then pick the action. An expectation with no on violation clause only warns, so bad rows still land. Drop row drops them, and logs the count. Fail update stops the update and rolls it back. When someone needs to fix the bad rows, quarantine them, with one reason column and counts that add up. And not null and check constraints fail the whole write, so check rows before you write. Here's the memory trick. Warn writes. Drop drops. Fail stops. Next time, in lesson twelve, we cover Spark tuning basics. The notebook and cheat sheet are in the Build and Teach databricks tutorials repo on GitHub. Thanks for watching Build and Teach.
