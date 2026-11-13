# A03 COPY INTO incremental loading — narration script

Databricks Data Engineer Associate exam prep, Lesson A03. Exam guide Aug 2026, Section 2 Data Ingestion and Loading (21%).

## 01 hook
Exam question. The same copy into command runs every hour, and no new files arrive between runs. How many rows does the next run load? By the end of this lesson, you'll answer it in five seconds.

## 02 welcome
Welcome to Build and Teach. Today we'll learn copy into, the SQL command for incremental file loading.

## 03 follow
To follow along, grab the notebook and the cheat sheet from the Build and Teach databricks tutorials repo on GitHub. The link is in the description.

## 04 objectives
This is lesson three of our Databricks Data Engineer Associate series, in Section two of the exam guide, Data Ingestion and Loading. That section is twenty-one percent of the exam. The objective is to use the copy into command to incrementally load files from cloud object storage, such as Amazon S three, Azure Data Lake Storage, or Google Cloud Storage, into Unity Catalog governed tables. In Free Edition, a Unity Catalog volume plays the part of cloud storage. Auto Loader was covered in lesson two and in the Auto Loader Foundations video. Both are linked in the description, so today we'll only contrast the two.

## 05 what
Copy into is a SQL command that loads files from a folder into an existing Delta table. The important word is incremental. Databricks remembers which files have already been loaded into that table, and the next run skips them. That makes copy into idempotent and retryable. You can rerun it after a failure, or on a schedule, without loading the same file twice. It runs in notebooks, on SQL warehouses, and in Lakeflow Jobs, and it reads C S V, JSON, Parquet, Avro, and other file formats.

## 06 plan
Here's the plan. We create a schema and a Unity Catalog volume to hold the source files. A day one file lands, and copy into loads it into a typed table. Then we rerun the exact same command and watch it load nothing. A day two file arrives, and only that file loads. Finally, we let copy into create the columns of an empty table, and evolve them when a new column shows up.

## 07 setup
Create a Python notebook on Serverless. Cell one reads the current catalog, creates a new schema that starts with b t underscore copy into plus twelve random hex characters, and creates a volume called landing. Then it switches to that schema with use, so we can write short table names. The source folder is landing slash orders, under slash Volumes.

## 08 day1
Cell two plays the source system. It writes a C S V file called orders day one into the source folder. The file has a header row and three fictional orders. Listing the folder shows one file.

## 09 table
Cell three creates the target, a managed Delta table called orders copy, with typed columns plus a source file column. Copy into needs the target table to exist before it runs. Because the table lives in a Unity Catalog schema, it's governed like any other table.

## 10 copy
Cell four is the copy into command. It loads into orders copy, from a select over the source folder. The select casts each C S V column to the right type, and adds the file name from the hidden underscore metadata column. File format is C S V, and format options pass settings to the file reader, here header equals true. We keep the command in a variable called copy sql so we can run it again.

## 11 run1
Here's the result you should expect from the first run. Copy into returns a small table. Three affected rows, three inserted rows, and zero skipped corrupt files.

## 12 rerun
Now cell five runs the exact same command again. No new files arrived. The expected result is zero inserted rows, and the table still has three rows. That's the answer to our opening question. Zero rows, because copy into skips every file that this table has already loaded. It doesn't append duplicates.

## 13 rerun_ui
Here's the expected output of the second run. Zero affected rows and zero inserted rows. Even if someone edits a file that was already loaded and keeps the same name, copy into still skips it.

## 14 day2
Cell six drops a day two file with two more orders, and runs the same copy into command. Only the new file loads, so we expect two inserted rows and five in total. Grouping by source file shows three rows from day one and two from day two. Each file loaded exactly once.

## 15 options
Two option lists matter on the exam. Format options are passed to the file reader. Common ones are header, infer schema, merge schema across the source files, and ignore corrupt files. Copy options control the copy itself, and there are two. Merge schema lets the target table's schema evolve. And force equals true turns idempotency off, so every file loads again, duplicates included.

## 16 filters
Here are a few more clauses. The pattern clause takes a glob pattern, so only matching files load. The files clause takes an explicit list, up to one thousand file names. You can use one of them, but not both. Validate checks that the data parses and fits the table's schema, without writing anything. And the source can be a volume path, like ours, or a cloud storage path that Unity Catalog lets you read through an external location.

## 17 schemaless
Cell seven creates an empty placeholder table called orders auto, with no columns at all. Copy into fills in the schema. In format options, we set infer schema and merge schema. In copy options, we set merge schema. So the table takes its columns from the files. Notice it inserts five rows. A new table has its own load history, so both existing files load. Until that first copy into, the empty table can't be used by insert or merge.

## 18 evolve
Cell eight lands a day three file with a new column, channel. We run the same auto command. Two rows load, and because merge schema is on in copy options, the table gains a channel column. The five older rows show null for channel, and the table has seven rows.

## 19 evolve_ui
This is the schema you should expect. Order i d, customer, amount, order date, and the new channel column at the end.

## 20 vs
So when do you choose copy into, and when Auto Loader? Databricks gives clear guidance. For files in the order of thousands over time, copy into is fine. For millions of files or more, use Auto Loader. It needs fewer operations to discover files and can split work into batches, so it's cheaper and more efficient at scale. If the schema changes often, Auto Loader has richer schema inference and evolution, which is lesson four. And reloading a chosen subset of files is easier with copy into.

## 21 traps
Five exam traps. Number one. A rerun with no new files loads zero rows, not duplicates. Number two. An edited file with the same name is still skipped. Number three. Setting force to true loads every file again, with duplicates. Number four. A new target table has its own load history, so it loads all the files. Number five. To evolve the target table's schema, set merge schema in copy options.

## 22 challenge
Your practice challenge. After cell eight, orders copy has five rows, and the day three file, with its extra channel column, is in the source folder. Question one. You run copy sql from cell four again. How many rows are inserted, what's the total, and does channel appear? Question two. Then you add copy options with force equals true and run it. How many rows are inserted, and what's the total? Question three. Two million new files land every day. Copy into, or Auto Loader? Pause the video and write down your three answers.

## 23 answer
Here's the explanation. Question one. Two rows are inserted, because only the day three file is new for orders copy. The total is seven, and there's no channel column, because the select lists exactly which columns to load. Question two. Force reloads all three files. That's three plus two plus two, so seven more rows, for fourteen in total, with duplicates. Question three. Auto Loader. Millions of files is its job, with fewer discovery operations and work split into batches. The solution cell is in the notebook, commented out.

## 24 recap
Let's recap. Copy into loads files into an existing Delta table. It's idempotent, so files already loaded into that table are skipped. Format options go to the reader, and copy options hold merge schema and force. Thousands of files, copy into. Millions, Auto Loader. Next time, in lesson four, we go deeper on Auto Loader schema enforcement and evolution. The notebook and cheat sheet are in the Build and Teach databricks tutorials repo on GitHub, linked in the description. Thanks for watching Build and Teach.
