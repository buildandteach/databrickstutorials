# A08 Joins and unions — narration script

Databricks Data Engineer Associate exam prep, Lesson A08. Exam guide Aug 2026, Section 3 Data Transformation and Modeling (22%).

## 01 hook
Exam question. Two orders files, two rows each, and both contain order fifty oh two. You union them, then count. How many rows?

## 02 welcome
Welcome to Build and Teach. Today we'll learn how to combine DataFrames in PySpark, with inner and left joins, broadcast and cross joins, and union.

## 03 follow
To follow along, grab the notebook and the cheat sheet from the Build and Teach databricks tutorials repo on GitHub. The link is in the description.

## 04 objectives
This is lesson eight of our Databricks Data Engineer Associate series. It covers one objective in section three, Data Transformation and Modeling, which is twenty-two percent of the exam. Combine DataFrames with operations such as inner join, left join, broadcast join, multiple keys, cross join, union, and union all. Everything today runs in Databricks Free Edition, in a Python notebook on serverless compute. The results on screen are the expected results. The code was checked with a local Apache Spark run, not on Databricks.

## 05 recap
A quick recap. Lessons A one through A six covered the platform and ingestion into bronze. Last time, in lesson A seven, we cleaned a bronze table into a new silver table. Silver tables usually combine several sources, so joins and unions are the next step. The earlier lessons are linked in the description.

## 06 scenario
Here's the on the job version. You're a data engineer at a small retailer. The reporting team wants one row per order line, with the order date, the customer name, and the product price. The facts live in orders and order items. The names live in two small tables, customers and products. Some orders arrive before their customer is loaded. If your join drops those orders, revenue is quietly understated, and nobody sees an error. We'll build that table, and learn to choose every join on purpose.

## 07 data
We'll use one small retail dataset for the rest of the series. Customers has four rows. Orders has four rows, and order fifty oh four belongs to customer one oh five, who isn't in customers yet. Products has three rows. And order items has five rows, one for each order line. All names are fictional.

## 08 setup
Create a Python notebook on serverless. Cell one imports the functions module as F, and the broadcast function. Then it creates a new schema in the workspace catalog. Its name is b t underscore joins, plus twelve random hex characters, so nothing else in your workspace is touched.

## 09 tables
Cell two creates customers and orders as DataFrames. Order fifty oh four is the orphan, with customer one oh five. Then it saves each one as a managed table. Cell three creates products. The price is decimal ten comma two. Then order items, one row for each order line. Then we save those two as well, and read all four tables back with spark dot read dot table.

## 10 innerleft
Now the joins. By default, join is an inner join. It keeps only the rows with a match on both sides. Our orders joined to customers, on customer i d, returns three rows. Order fifty oh four disappears, and there's no error. A left join keeps every order, so it returns four. And left anti returns only the left rows with no match. That's one row.

## 11 innerleft_x
Here's the expected output. Three, four, one. In the left join, order fifty oh four comes back with a null customer name and a null state. When the left table is your fact table, a left join is usually the safe default, because it never loses facts. And left anti is a quick way to count orphan keys, before they become a reporting gap.

## 12 exprjoin
Two details the exam likes. Pass the key as a string, or a list of strings, and Spark does an equi join and keeps one customer i d column. Pass an expression instead, and you keep both columns, so a later select of customer i d is ambiguous. Expect one, then two. Also, the how argument accepts inner, left or left outer, right, full, cross, left semi, and left anti.

## 13 multikey
Next, multiple keys. Order items has a two part key, order i d plus line number. Suppose a returns table has one row. Order fifty oh one, line two, damaged. Join on order i d alone, and both lines of order fifty oh one match. That's two rows, which is wrong. Join on the list of order i d and line number, and you get one. Join on the full key. If the column names differ, combine the conditions with the and operator.

## 14 dupkey
The same rule applies to dimension tables. If customers had customer one oh one twice, every order for one oh one would match twice. Our left join would return six rows instead of four. Check that the dimension key is unique before you join.

## 15 bcast_why
Now broadcast joins. In a normal join between two large tables, Spark shuffles both sides across the cluster by the join key, and uses a sort merge join. A broadcast join sends a full copy of the small table to every executor instead, so the large table doesn't have to be shuffled. That's called a broadcast hash join.

## 16 bcast_code
You ask for a broadcast join by wrapping the small DataFrame in broadcast. Then explain shows the plan. Here's the expected plan, simplified. A broadcast hash join, left outer, build right. Build right means customers, the right side, is the side that was copied. The exact plan text differs on Databricks.

## 17 bcast_docs
What do the docs say about choosing? Spark broadcasts a table by itself when its estimated size is under the auto broadcast join threshold. That's ten megabytes by default in Apache Spark. The broadcast hint overrides that threshold. On Databricks, adaptive query execution, or A Q E, is on by default. It can switch a sort merge join to a broadcast join at run time, when one side turns out to be small. Databricks still recommends the hint when you know the table is small. And the hint is only a suggestion. If the join type doesn't support it, Spark ignores it.

## 18 wrongside
Here's the trap. A left outer join can't broadcast its left side. In the local check, with automatic broadcasting turned off, the hint on orders was ignored, and Spark fell back to a sort merge join. On serverless that setting may be locked, so cell nine is wrapped in a try block. The rule is simple. Keep the big table on the left, and broadcast the small dimension on the right. Never broadcast orders or order items. Every executor has to hold a full copy of the broadcast side in memory, so broadcasting a large fact table can be slower than a shuffle, or even fail.

## 19 sqlhint
In SQL, the same hint goes in a comment right after select. Broadcast, then the table alias. Broadcast join and map join are accepted aliases. Expect the same four orders, with a null name for order fifty oh four.

## 20 cross
Cross joins multiply rows. A cross join pairs every row on the left with every row on the right, with no join key. Customers cross join products returns twelve rows, four times three. Twelve is fine. But a million customers and ten thousand products produce ten billion rows. Use cross join only when you really want every combination, like a calendar by store grid, and check both counts first.

## 21 union_table
Now unions, where PySpark and SQL use the same word differently. SQL union removes duplicates. SQL union all keeps them. PySpark union keeps duplicates, so it behaves like SQL union all. Union all in PySpark is just an alias for union. All of these match columns by position. Only union by name matches by name.

## 22 union_code
Cell twelve builds the two extracts from the opening question. Union, then count, returns four. Add distinct, and you get three.

## 23 union_sql
The SQL version in cell thirteen counts both. Union returns three rows, and union all returns four. So the answer to the opening question is C, four. Order fifty oh two appears twice, because PySpark union is union all in disguise. Add distinct if you want three.

## 24 memory
Here's the memory trick. PySpark union is union all in disguise. It keeps every row, and it matches columns by position.

## 25 colorder
Position matching has a second trap. A store extract arrives with the columns in a different order. Customer i d, then order i d, then order date. Both are integers, so union doesn't fail. It quietly puts customer one oh three in the order i d column. Here's the expected result. The last row of the union has order one oh three, and customer fifty fifty-five. That's wrong. Union by name matches by column name, and returns order fifty oh five for customer one oh three.

## 26 mismatch
If one side has an extra column, both union and union by name fail, with num columns mismatch. Add allow missing columns equals true to union by name, and the missing column is filled with null instead. Here's the expected result. Two error lines, then three rows, with a null channel on the first two.

## 27 silver
Now the on the job build. Order items and orders are the large tables, so that join is a normal shuffle join. Customers and products are small dimensions, so we broadcast both. The customer join is a left join, so unmatched orders survive. Then we add the line total, and select the columns. Cell sixteen writes silver order lines, and checks the count and the total. Here's the expected result. Five rows, one for each order line. Order fifty oh four stays in the table, with a null customer name. The line totals add up to eighty-four. In the plan, orders and order items use a sort merge join, and both dimension joins show as broadcast hash join, build right.

## 28 inner_cost
Now see what an inner customer join costs you. Make that one join inner, and the table has four lines. The total drops from eighty-four to seventy-two. There's no error, and twelve dollars of revenue is gone.

## 29 checklist
Here's the practical takeaway, a join choice checklist. One, keys. Name the full key, check that the dimension key is unique, and check that the data types match. Two, join type. Need every fact row? Use left. Only the matches? Inner. Every combination, on purpose? Cross join, after checking both counts. Three, broadcast or shuffle. Broadcast the small dimension on the right side, and never the large fact table. Two large tables? Let Spark shuffle. Four, union. It keeps duplicates and matches by position, so use union by name for files from different sources. Five, reconcile. Compare the row counts in and out, and count the unmatched keys.

## 30 snippet
And a reusable snippet. Join orders to customers first checks that the customer key is unique. Then it does a broadcast left join, and prints the counts in, out, and unmatched. On our data, expect four orders in, four out, and one without a customer. And with the duplicate one oh one from before, the function stops with an error, instead of writing six rows. Copy it into any job that joins to a dimension.

## 31 examq
Now an exam style question, one I wrote myself. A data engineer joins a two billion row order items table to a five thousand row products table on product i d, keeping every order line. Which code is the best choice? A. Broadcast order items, then left join products. B. Order items left join broadcast products. C. Cross join, then filter on product i d. D. Products inner join order items. Pause the video if you'd like to think.

## 32 examans
The answer is B. Products is the small table, so it's the one to broadcast, and the left join keeps every order line. A broadcasts the huge table, and a left outer join can't broadcast its left side anyway. C builds a ten trillion row intermediate result before it filters. And D is an inner join, so lines with an unknown product would be lost.

## 33 challenge
Your practice challenge. Question one. How many rows does orders join customers, on customer i d, with a right join, return? Question two. How many rows does customers cross join products return, if you filter to state equals T X? Question three. Which rows of orders resend are not in orders mon? Write the one line join, and say how many rows it returns. Pause the video and write down your three answers.

## 34 answer
Here's the explanation. Question one. Five. A right join keeps every customer. Orders fifty oh one, fifty oh two and fifty oh three match, and customers one oh three and one oh four have no orders, so they stay with null order columns. Order fifty oh four has no customer, so it's dropped. Question two. Six. Two customers live in T X, and each one pairs with all three products. Question three. Left anti. Orders resend, left anti join orders mon, on order i d. It returns one row, order fifty oh three.

## 35 close
Let's recap. Join defaults to inner, which silently drops unmatched rows. Use left for fact tables, and left anti to count the orphans. Join on the full key, and make sure the dimension key is unique. Broadcast the small dimension on the right side. A cross join returns left rows times right rows. And the memory trick. PySpark union is union all in disguise. It keeps every row and matches columns by position. Next time, in lesson nine, we deduplicate and aggregate. For a slower walk through inner and left joins, the Foundations video on PySpark joins is linked in the description. The notebook and cheat sheet are in the Build and Teach databricks tutorials repo on GitHub. Thanks for watching Build and Teach.
