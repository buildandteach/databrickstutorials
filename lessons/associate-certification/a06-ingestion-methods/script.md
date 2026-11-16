# A06 Choosing an ingestion method: Lakeflow Connect, JDBC and REST — narration script

Databricks Data Engineer Associate exam prep, Lesson A06. Exam guide Aug 2026, Section 2 Data Ingestion and Loading (21%).

## 01 hook
Exam question. A SQL Server orders table has to land in Unity Catalog every thirty minutes, with inserts, updates, and deletes, and with the least code to maintain. Do you pick a scheduled notebook with Java Database Connectivity, or J D B C, a REST client, Auto Loader, or Lakeflow Connect? By the end of this lesson, you'll answer it in five seconds.

## 02 welcome
Welcome to Build and Teach. Today we'll learn how to choose an ingestion method, and how Lakeflow Connect, J D B C, and REST clients each land data in Unity Catalog.

## 03 follow
To follow along, grab the notebook and the cheat sheet from the Build and Teach databricks tutorials repo on GitHub. The link is in the description.

## 04 objectives
This is lesson six of our Databricks Data Engineer Associate series. It covers three objectives in section two, Data Ingestion and Loading, which is twenty-one percent of the exam. Configure Lakeflow Connect. Use J D B C, Open Database Connectivity, or O D B C, and REST clients in notebooks. And prioritize between ingestion methods. A quick Free Edition note. Free Edition restricts outbound internet access, so the REST pattern runs today against a simulated A P I. Lakeflow Connect database connectors with change data capture, or C D C, and J D B C reads from an external database need a paid workspace and a reachable database. Those parts are shown as code, and they're not runnable in Free Edition.

## 05 recap
A quick recap. Lesson A one toured the platform. A two mapped the ingestion patterns, batch, streaming, and incremental. A three loaded files with copy into, A four covered Auto Loader schema evolution, and A five flattened nested JSON. Those lessons all started from files. Today we look at sources that aren't files, and decide which tool fits. The earlier lessons are linked in the description.

## 06 menu
Here's the menu. Databricks recommends starting with the most managed layer, and only dropping down when it doesn't support your source. At the top are Lakeflow Connect managed connectors, for software as a service apps like Salesforce and Workday, and for databases like SQL Server, MySQL, and PostgreSQL. Next are standard connectors, the Spark and SQL tools you code yourself, like Auto Loader, copy into, and Kafka streams. Then partner connectors, third party tools you can reach through Partner Connect. At the bottom is do it yourself code, J D B C, O D B C, or REST in a notebook. And if you don't need a copy of the data at all, Lakehouse Federation queries the source in place.

## 07 lfc_arch
Let's configure Lakeflow Connect. A managed database connector has five parts. A connection, which is a Unity Catalog securable that stores the host and the credentials. An ingestion gateway, which runs continuously on classic compute and reads the database change logs before they're truncated. A staging volume in Unity Catalog, where the gateway writes snapshots and changes. Staged data is purged after thirty days. Then the ingestion pipeline, which runs on serverless compute on a schedule, and applies those changes to the destination streaming tables. Software as a service connectors are simpler. They skip the gateway and the staging volume.

## 08 lfc_ui
In a paid workspace, you configure it in the U I. Open Data Ingestion in the sidebar, and pick SQL Server under Databricks connectors. Select a connection, or create one if you have the create connection privilege. Name the ingestion pipeline, the gateway, and the staging location. Then select the source tables, choose the destination catalog and schema, validate the source, and add a schedule. The source database needs change tracking or change data capture turned on first. You can't run this in Free Edition, because it needs a reachable database, and the gateway runs on classic compute.

## 09 lfc_yaml
The same setup can live in code, in a Declarative Automation Bundle. The gateway pipeline has a gateway definition, with the connection name and the catalog and schema for staging. It's marked continuous, because it must keep up with the change logs. The ingestion pipeline points at the gateway, and lists the objects to ingest. Here that's a whole source schema, so new tables in that schema are picked up too. The ingestion pipeline doesn't run continuously. A scheduled job triggers it, every thirty minutes for example. The first run takes a full snapshot, and later runs apply only the changes, including deletes.

## 10 hook_answer
And that's the answer to our opening question. D, the Lakeflow Connect managed database connector. It uses change data capture, so inserts, updates, and deletes all arrive, and it handles retries and schema changes for you. You configure it, you don't code it. A J D B C notebook could read the table, but a query only returns the rows that exist now. A deleted row just disappears, and you'd have to write and maintain the merge logic yourself. REST doesn't fit a database, and Auto Loader reads files, not tables.

## 11 jdbc
Now the do it yourself options, starting with J D B C. Spark has a built-in J D B C reader. You pass the J D B C U R L, the table, and the credentials, and Spark reads the schema from the database and maps the types for you. Then you write the result to a Unity Catalog table. Keep the user and password in Databricks secrets, never in the U R L, or in the notebook. This cell is shown as code. It needs an external database the workspace can reach, so it's not runnable in Free Edition.

## 12 jdbc_tune
Two J D B C details the exam likes. By default, the read is a single query on one thread. To read in parallel, set a partition column, with a lower bound, an upper bound, and the number of partitions. Pick an indexed, evenly spread column, and don't set too many partitions, or you'll overload the source database. Fetch size controls how many rows come back per round trip. And with the query option, the filter runs inside the source database, so only the rows you need cross the network.

## 13 jdbc_uc
For governed access, Databricks now offers a J D B C Unity Catalog connection. An admin creates the connection once, with the driver, the U R L, and the credentials. In the notebook, you pass the connection name instead of a U R L and a password, and users need the use connection privilege. The query runs in the source database's own SQL dialect. What about O D B C? From a notebook you can use a Python O D B C client like py o d b c, but you install and manage the driver yourself. J D B C is built into Spark, so it's the usual choice.

## 14 rest_intro
Now REST. When an H T T P A P I has no managed connector, you call it from a notebook, usually with the requests library. The exam objective names two landing spots. You can land the raw responses in cloud storage, like a Unity Catalog volume, and then load them with Auto Loader or copy into. That keeps an audit trail, and you can replay without calling the A P I again. Or you can write the records straight into a Unity Catalog table. That's fewer moving parts, and fine for small responses. Either way, you schedule the notebook with Lakeflow Jobs. Let's build the first one, because it runs in Free Edition.

## 15 setup
Create a Python notebook on Serverless. Cell one reads the current catalog, and creates a new schema that starts with b t underscore ingest, plus twelve random hex characters. Inside it we create two volumes, landing and checkpoints. The raw A P I responses go under landing slash A P I slash orders.

## 16 api
Cell two plays the A P I. Free Edition only allows outbound calls to a limited set of trusted domains, so instead of a real endpoint, a small function returns pages of fictional orders. Page one has three orders and points to page two. Page two has two orders, and next page is none. In a real workspace, you'd swap in the commented line, requests dot get, with the page as a parameter, an authorization header from secrets, and a timeout.

## 17 land
Cell three follows the pages. It asks for page one, writes the records as JSON lines into a file in the volume, and then moves to the next page until next page is none. Each run gets its own folder, stamped with the time, so nothing is overwritten. You should expect it to print five, with two files landed.

## 18 load
Cell four loads the landed files into a Unity Catalog table with Auto Loader, the same pattern as lesson A four. Infer column types is on, so the amounts come back as numbers. The trigger is available now, so it processes the new files and stops. Next time the job runs, only the newly landed pages are read.

## 19 load_out
Here's what you should expect. Five rows, one per order, from the two landed files. The columns come back in alphabetical order, amount, customer, order i d, and status, and Auto Loader adds the rescued data column, which stays empty here.

## 20 direct
Cell five is the direct option. It collects every page into a Python list, turns it into a DataFrame, and appends it to a table. Expect five rows. It's simple, but notice the trap. Append twice and you'll have ten rows, because the A P I returns the same orders again. With a direct write, plan for duplicates, with a merge on the order i d for example.

## 21 prioritize
Here's how to prioritize on the exam. Read the scenario words. Files keep arriving in cloud storage, at any volume, points to Auto Loader. A simple SQL reload of files that should be idempotent points to copy into. A supported software as a service app or database points to a managed connector, and so does a database where you need changes, including deletes. A custom query, or a database with no connector, points to J D B C. An H T T P A P I with no connector points to REST code in a notebook, scheduled with Lakeflow Jobs. A partner tool your team already runs points to a partner connector. And if you don't want a copy at all, use Lakehouse Federation. Behind each choice are four requirements, data volume, ingestion frequency, data types, and governance with Unity Catalog.

## 22 traps
Five exam traps. Number one. Databricks recommends the most managed layer that supports your source. Number two. A database C D C connector has a gateway that runs continuously on classic compute, and an ingestion pipeline that runs on a schedule. Number three. A J D B C read is a single query on one thread, unless you set the partition column, the bounds, and the number of partitions. Number four. Credentials belong in secrets or a Unity Catalog connection, never in the U R L. Number five. Auto Loader reads files. A database or an A P I first needs a connector, or code that lands files.

## 23 challenge
Your practice challenge. Pick the method for each scenario. Question one. Clickstream JSON files land in an S three bucket every few minutes, millions of them per day. Question two. Salesforce opportunities need to land in Unity Catalog every day, with minimal maintenance. Question three. An internal H T T P A P I with no connector has to be pulled every hour into a table. Pause the video and write down your three answers.

## 24 answer
Here's the explanation. Question one is Auto Loader. The source is files in cloud storage, arriving all the time, and Auto Loader loads them incrementally at that scale. Question two is a Lakeflow Connect managed connector. Salesforce is supported, so you configure it instead of coding it. Question three is a REST client in a notebook. Land the raw responses in a volume, load them with Auto Loader, and schedule the notebook hourly with Lakeflow Jobs. The decision table is in the cheat sheet.

## 25 close
Let's recap. Start with the most managed layer. A managed database connector has a connection, a gateway, a staging volume, an ingestion pipeline, and streaming tables. J D B C needs secrets or a Unity Catalog connection, and a partition column for parallel reads. And REST code lands raw pages in a volume, loads them with Auto Loader, and runs on a schedule with Lakeflow Jobs. Next time, in lesson seven, we clean bronze data into silver tables. The notebook and cheat sheet are in the Build and Teach databricks tutorials repo on GitHub, linked in the description. Thanks for watching Build and Teach.
