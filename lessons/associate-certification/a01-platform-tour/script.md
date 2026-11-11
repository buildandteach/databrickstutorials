# A01 Platform tour: architecture, Delta Lake, Unity Catalog and compute — narration script

Databricks Data Engineer Associate series, Lesson 1. Exam guide Aug 2026, Section 1 (6%).

## 01 title
Welcome to Build and Teach. Today we'll learn the Databricks platform from the top down: the architecture, Delta Lake, Unity Catalog, and the compute choices you pay for. This is lesson one of our Databricks Data Engineer Associate certification series. It covers Section 1 of the August twenty twenty-six exam guide, the Databricks Intelligence Platform, which carries six percent of the exam weight. Let's get started.

## 02 follow
To follow along, grab the lesson files and the cheat sheet from the Build and Teach databricks tutorials repo on GitHub. The link is in the description.

## 03 objectives
Section 1 has two objectives. First, understand the core components of the platform, such as its architecture, Delta Lake, and Unity Catalog. Second, understand the compute services, including their characteristics, limitations, and cost models, and select the most suitable option for each workload. That weight sounds small, but every other section builds on these ideas. This is independent study material, not official Databricks training, and there's no guarantee about the exam.

## 04 plan
Here's the hands-on plan. In Databricks Free Edition, we'll walk the catalog, schema, and table hierarchy, create a new schema and a managed Delta table, inspect it with describe detail and describe history, and then query the same table from a notebook and from the Structured Query Language, or SQL, warehouse. Everything uses three rows of made-up study plan data, and the notebook is in the Build and Teach GitHub repo.

## 05 planes
Start with architecture. Databricks has a control plane and a compute plane. The control plane holds the backend services Databricks manages, including the web application where you write notebooks and queries. The compute plane is where your data is actually processed. With serverless compute, it runs in a serverless compute plane in your Databricks account. With classic compute, the machines run in your own cloud account. Free Edition is serverless only, so everything we run today lands in the serverless compute plane.

## 06 objects
Above the workspace sits the Databricks account. The account manages identity, workspaces, usage, and the Unity Catalog metastore. A workspace is where people run work: notebooks, jobs, queries, and dashboards. A metastore is the top of Unity Catalog governance, and one metastore can be attached to several workspaces in the same region. Free Edition gives you one workspace and one metastore, with no account console, so today we stay inside the workspace.

## 07 namespace
Unity Catalog organizes data in a three-level namespace: catalog, then schema, then the object. At that third level you'll find tables, views, volumes, functions, and models. So a full table name is catalog dot schema dot table. Unity Catalog is also where access control, lineage, and auditing live, which is why the exam keeps saying Unity Catalog governed tables. We'll go deeper on permissions in the governance lessons later in this series.

## 08 catalog_ui
In Free Edition, open Catalog in the left sidebar. The default catalog is usually named workspace, and it contains a schema named default. Expand a catalog to see its schemas, and expand a schema to see its tables and volumes. That tree is the same three-level namespace, just drawn as folders.

## 09 explore_code
Now the notebook. Create a Python notebook and attach it to Serverless. The first cell asks Spark which catalog and schema we're in, using current catalog and current schema, and then lists the schemas in that catalog. We read the catalog name instead of hard coding it, so the notebook works in any workspace.

## 10 create_code
Next, we create our own space. The setup cell builds a schema name that starts with b t underscore platform, plus twelve random hex characters, so a rerun never collides with an existing schema. Then we create a table called study plan with four columns: lesson i d, section, topic, and weight percent. We don't give it a location, which makes it a managed table: Unity Catalog decides where the files live and manages their lifecycle. And because Delta is the default table format on Databricks, this is a Delta table without us saying so.

## 11 insert_code
Insert three made-up study plan rows: A zero one for the platform section at six percent, A zero two for ingestion at twenty-one percent, and A zero seven for transformation at twenty-two percent. Those weights come from the exam guide. Then read the table back by its full three-level name, and the notebook checks that exactly three rows came back.

## 12 delta
So what makes this Delta Lake? Delta Lake is the storage layer under Databricks tables. The rows are stored in Parquet files, and next to them is a transaction log. That log gives you atomicity, consistency, isolation, and durability, or ACID, transactions, so readers never see a half-finished write. It enforces the schema, supports update, delete, and merge, and keeps every version so you can time travel. Our earlier lesson, Build Your First Delta Lake Table, already walked through time travel, so I'll link it in the description instead of repeating it.

## 13 detail_code
Describe detail shows the table's physical facts. The format column says delta, and you'll also see the number of files, the size in bytes, and when the table was created. Then describe table extended shows the table type, and for our table it says managed, not external.

## 14 detail_ui
Notice we never configured storage. Free Edition uses default storage, and custom workspace storage locations aren't supported there. Managed versus external tables get their own lesson later in the series, in the governance section.

## 15 history_code
Now change the data. An update sets the A zero seven topic to transformation and modeling, which matches the section's full name. Then describe history lists each version with its operation. Expect version zero for create table, then a write for the insert, then the update. Every commit to the transaction log becomes a new version.

## 16 history_ui
In the history you can read the story of the table, newest first: the update, the insert, and the create. If background maintenance like predictive optimization runs later, you may see extra versions too, and that's normal.

## 17 compute_fe
Now compute, the second objective. In Free Edition you get two kinds. Serverless compute for notebooks is what ran our Python cells. There's no cluster to configure, and Databricks manages the size and scaling. A SQL warehouse is compute built for SQL queries, business intelligence tools, or B I tools, and dashboards. Free Edition gives you one SQL warehouse, limited to the two X small size.

## 18 warehouse_ui
Open Compute in the sidebar, then the SQL warehouses tab. In a new Free Edition workspace, the one warehouse is called the Serverless Starter Warehouse, and its size is two X small.

## 19 warehouse_sql
Open the SQL editor, pick that warehouse, and select from your study plan table using its full name, with the schema name your notebook printed. Same three rows, same Unity Catalog governance, different compute. The data doesn't move; only the engine running the query changes.

## 20 classic
In a paid workspace you'll meet more options, and the exam expects you to know them. Classic all-purpose compute is for interactive work. You choose the size and the runtime, it can be standard, which is shared, or dedicated to one user or group, and auto termination stops it when it sits idle. Jobs compute is created for a job run and terminates when the run ends. Instance pools keep idle instances ready, which cuts startup time. Free Edition doesn't offer these, because custom compute configurations aren't supported there, so treat this part as concepts, not a demo.

## 21 cost
How is compute billed? Databricks usage is measured in Databricks Units, or D B U for short, a unit of processing capability billed per second of use. With classic compute, you pay Databricks for D B U usage, and your cloud provider bills you separately for the virtual machines. With serverless, the compute runs in the Databricks account, so the serverless D B U price covers it. Jobs compute has a lower D B U rate than all-purpose compute, which is why scheduled production work belongs on jobs compute, not on an interactive cluster. And pools don't charge for D B U usage while instances sit idle, though the cloud provider still bills for those instances.

## 22 choose
A quick way to choose. For interactive exploration in notebooks, use serverless notebook compute, or all-purpose compute when you need a custom configuration. For scheduled production work, use serverless jobs, or classic jobs compute. For SQL analytics, B I tools, and dashboards, use a SQL warehouse, ideally serverless for fast startup. And if classic jobs must start fast, back them with a pool. When you read a scenario, look for the workload words first: interactive, scheduled, SQL, startup time, and cost.

## 23 limits
Keep Free Edition's limits in mind as we work through the series. Serverless compute only, one two X small SQL warehouse, a maximum of five concurrent job tasks, one active pipeline per pipeline type, restricted outbound internet access, and no account console. There's also a fair usage quota. If you exceed it, compute shuts down for the rest of the day, but your data and settings are kept.

## 24 challenge
Your practice challenge. Without running anything, predict what happens if you delete every row in study plan where weight percent is below ten, and then run describe history again. How many rows remain? What's the newest operation in the history? And can you still read the row for lesson A zero one? Pause the video and write down your three answers.

## 25 answer
Here's the explanation. Two rows remain, A zero two and A zero seven, because only A zero one is below ten percent. The delete commits a new version, so the newest history entry is a delete. And yes, you can still read A zero one with time travel, by querying the version before the delete. Delta keeps the older data files until vacuum removes them after the retention period. The solution cell is in the notebook, commented out, so you can check your prediction.

## 26 recap
Let's recap. The control plane runs the Databricks services, and the compute plane processes your data, serverless or classic. Unity Catalog names everything catalog dot schema dot table and governs it in one place. Managed tables default to Delta, and the transaction log gives you versions and history. Pick compute by workload, and remember that D B U usage drives the cost. Next time, in lesson two, we move into Section 2 with ingestion patterns: batch, streaming, and incremental. The notebook and cheat sheet are linked in the description. Thanks for watching Build and Teach.
