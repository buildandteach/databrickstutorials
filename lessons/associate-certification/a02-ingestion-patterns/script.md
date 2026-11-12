# A02 Ingestion patterns: batch, streaming and incremental — narration script

Databricks Data Engineer Associate series, Lesson 2. Exam guide Aug 2026, Section 2 (21%).

## 01 title
Welcome to Build and Teach. Today we'll learn the three ways data gets into Databricks: batch, streaming, and incremental loading. This is lesson two of our Databricks Data Engineer Associate certification series. It starts Section two of the August twenty twenty-six exam guide, Data Ingestion and Loading, which carries twenty-one percent of the exam weight. That's the second biggest section, so this one matters.

## 02 follow
To follow along, grab the lesson files and the cheat sheet from the Build and Teach databricks tutorials repo on GitHub. The link is in the description.

## 03 objectives
Section two opens with one broad objective. Enable and detail data ingestion patterns, including batch, streaming, and incremental loading, and import data from sources such as local files, Lakeflow Connect standard connectors, and Lakeflow Connect managed connectors. Today is the map for the whole section. Copy into, Auto Loader schema evolution, and nested data each get their own lesson right after this one. This is independent study material, not official Databricks training, and there's no guarantee about the exam.

## 04 plan
Here's the plan. In Databricks Free Edition, we'll create a schema and a Unity Catalog volume to act as a landing zone for raw files. A day one file of orders lands there, and we load it two ways: a batch load and an incremental load with Auto Loader. Then a day two file arrives, we run both again, and compare what each one actually read. Everything uses five made-up orders, and the notebook is in the Build and Teach GitHub repo.

## 05 patterns
First, the three patterns. A batch load reads the whole source every time it runs. It's simple, and it's fine for small sources or ones that get fully replaced, but the cost grows with all the data you've ever received. An incremental load only processes data that's new since the last run. It remembers what it already loaded, so each run stays small. Streaming is the same idea running all the time. Data is picked up within seconds or minutes of arriving. On Databricks, incremental and streaming use the same Structured Streaming engine. The difference is the trigger, which decides how often it runs.

## 06 setup_code
Create a Python notebook on Serverless. The first cell reads the current catalog, creates a new schema that starts with b t underscore ingest plus twelve random hex characters, and creates a volume called landing inside it. A volume is a Unity Catalog object that stores files instead of tables, and its path starts with slash Volumes. Our raw order files go in an orders folder, and a separate checkpoints folder will hold Auto Loader's progress.

## 07 day1_code
Cell two plays the part of a source system. It writes a small comma-separated values file, or C S V file, called orders day one, with a header row and three fictional orders, into the orders folder. Listing the folder shows one file. In real life this file would come from an upstream system, a cloud storage drop, or an upload.

## 08 volume_ui
You can see the same thing in Catalog Explorer. Open the schema, then Volumes, then landing, and the orders folder holds the day one file. Volumes are also where you'd put files you upload from your own computer.

## 09 batch_code
Pattern one, batch. We define the schema once: order i d, customer, amount, and order date. The run batch function uses spark dot read to read every file in the folder, then overwrites a managed table called orders batch. Overwrite matters here. Because a batch read sees all the files every time, appending would load the same orders again on every run. After the first run, the table has three rows.

## 10 al_code
Pattern two, incremental, using Auto Loader. Auto Loader is the cloud files source in spark dot read stream. We give it the format, the header option, and the same schema, and add a source file column from the hidden file metadata, so we can see where each row came from. The write side does two important things. The checkpoint location is where Auto Loader records which files it has already processed. And the trigger is available now, which means process everything new, then stop. That's a streaming query running as an incremental batch. The first run also finds three rows.

## 11 trigger
Why available now? On serverless notebooks and jobs, which is everything in Free Edition, Structured Streaming supports the available now trigger, and the older trigger once, which is deprecated. Processing time and continuous triggers aren't supported there, and they raise an error. For truly always-on streaming, Databricks points you to a pipeline in continuous mode, or an available now query inside a continuous job. In practice, most incremental loads are available now runs on a schedule, which Lakeflow Jobs handles later in this series.

## 12 day2_code
Now a day two file arrives with two more orders. We run both loads again. The batch load reads both files, all five rows, and overwrites the table with five rows. Auto Loader checks its checkpoint, sees that the day one file is already done, and reads only the new file, so it adds just two rows, for five in total. Same final count, but very different work. With five rows, nobody cares. With five years of files, the batch job re-reads five years every night, and the incremental job reads one day.

## 13 al_ui
Here's what cell five prints. The batch table has five rows, and the Auto Loader run read only the two new ones.

## 14 byfile_code
Cell six groups the bronze table by source file. Three rows came from day one and two from day two, and each file was loaded exactly once. Keeping the source file name in a bronze table is a cheap habit that makes reruns and audits easy to check.

## 15 upload
The exam also names local files as a source. There are two common routes in the workspace. From New, choose add or upload data, then create or modify a table. You drop in a file, preview it, and Databricks creates a managed Delta table. That's a one-time batch load. Or upload the file into a volume, like our landing volume, and read it with code, which is exactly the pattern we just ran. Uploads are great for small reference files, not for data that keeps arriving.

## 16 lakeflow
Now Lakeflow Connect, the umbrella name for Databricks ingestion. It has two families. Standard connectors use Spark and Structured Query Language, or SQL, application programming interfaces, like Auto Loader and copy into for cloud object storage, or Structured Streaming for message buses such as Kafka. You get more control, and you maintain the logic. What we built today is a standard connector pattern. Managed connectors are configured, not coded. You create a connection and an ingestion pipeline for a source like Salesforce, Workday, SharePoint, or a SQL Server database, and Databricks handles authentication, incremental reads, retries, and schema changes. They run on serverless Lakeflow pipelines and land in Unity Catalog tables.

## 17 lakeflow_fe
A quick Free Edition note. We demoed the standard pattern because it runs fully inside Free Edition. Managed connectors need a real source system and its credentials, and database connectors that use change data capture, or C D C, are paid workspace features. Free Edition also restricts outbound internet access and allows one active pipeline per type. So treat managed connectors here as concepts. Lesson six goes deeper on choosing between Lakeflow Connect, partner tools, and custom code.

## 18 choose
How do you choose on the exam? Read the scenario words. Small source, fully replaced, a whole snapshot once a day, or a one-time upload, points to batch, and batch should overwrite, not append. New files keep arriving, only process new data, or don't reprocess files, points to incremental loading with Auto Loader and a checkpoint. Latency of seconds or minutes points to streaming. And a popular software as a service app or database with built-in support points to a managed connector.

## 19 challenge
Your practice challenge. A day three file lands with four new orders, and you run both loads again. How many rows are in orders batch? How many rows does this Auto Loader run read, and what's the total in bronze? And here's the trap. If all three batch runs had used append instead of overwrite, how many rows would orders batch hold? Pause the video and write down your three answers.

## 20 answer
Here's the explanation. Orders batch holds nine rows, because the batch load re-reads all three files and overwrites the table. Auto Loader reads only the four new rows, and bronze also ends up with nine. With append, the first run adds three, the second adds five, and the third adds nine, for seventeen rows, full of duplicates. Batch plus append means duplicates on every run. Incremental loading, with a checkpoint, loads each file once. The solution cell is in the notebook, commented out.

## 21 recap
Let's recap. Batch re-reads the whole source, so overwrite to stay correct. Incremental loading with Auto Loader and a checkpoint loads each file exactly once. Streaming is the same engine running continuously, and on serverless you run it with the available now trigger. Lakeflow Connect gives you standard connectors you code, and managed connectors you configure. Next time, in lesson three, we load files incrementally with copy into. The notebook and cheat sheet are linked in the description. Thanks for watching Build and Teach.
