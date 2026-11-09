# Ingest Files Incrementally with Auto Loader into a Bronze Delta Table — final narration

## title
Welcome to Build and Teach. Today we'll learn how to ingest files incrementally with Auto Loader into a bronze Delta table.

## early
Here is the actual lesson notebook in Databricks Free Edition, with Python and serverless compute. The Run all button runs the notebook, while the triangle beside a code cell runs that cell. These are still screenshots from the successful run, not a screen recording. Import the companion notebook as a new Python notebook named Build and Teach - Auto Loader Ingestion, attach serverless, and run from the top. Everything uses small fictional CSV files, so you do not need tables from earlier lessons.

## why
In real projects, files keep landing over time. Reloading every file on every run wastes compute and can duplicate rows. Incremental ingestion means each run picks up only what is new. Auto Loader watches a folder, discovers new files, and loads them into a Delta table. A checkpoint records which files are already processed, so each file is ingested exactly once even if you rerun the job. The destination is a bronze table: the raw landing layer in the medallion architecture, kept close to how the data arrived, plus lineage columns. Cleaning happens later, in silver.

## objective
The official August twenty twenty-six Data Engineer Associate guide lists ingestion patterns, COPY INTO, and Auto Loader landing data into Unity Catalog-governed tables in Section 2, Data Ingestion and Loading. The Professional guide builds on this with append-only pipelines and quarantining bad data. This lesson is a hands-on foundation for those topics, not a complete exam course, and there is no exam guarantee.

## schema_code
The first cell creates a new schema whose name starts with b t underscore ingest, plus a random suffix, so it cannot collide with anything you already have. Inside it we create two managed Unity Catalog volumes. A volume is governed file storage under slash Volumes. One volume, raw files, is the landing zone. The other, pipeline state, holds the Auto Loader checkpoint. Keep the checkpoint out of the folder Auto Loader scans.

## schema_live
In the actual Free Edition run, the catalog was workspace and the schema was b t ingest three b five d five d seven three six c six six. SHOW VOLUMES listed pipeline state and raw files. Your suffix will be different. Keep the printed fully qualified names.

## batch1_code
Next we simulate the first delivery: two small CSV files with a header for order ID, store, quantity, and unit price. File A has three orders and file B has two. We write them with dbutils dot fs dot put and overwrite equals false, so an existing file is not silently replaced.

## batch1_live
The actual listing showed orders underscore twenty twenty-six underscore eleven underscore zero nine underscore a dot csv and the matching b file. Two files, ready for the first load.

## stream_code
Auto Loader is a Structured Streaming source. Call spark dot read stream, use the format cloud files, set the cloud files format to C S V, and say the files have a header. Give an explicit schema: order ID and quantity as integers, store as a string, and unit price as a decimal with two places. Set the rescued data column option to underscore rescued data. When a value cannot be parsed, the typed column becomes null and the original text is kept as JSON in the rescued column. Bronze keeps the evidence instead of dropping the row. Add lineage from underscore metadata for the source file path and file name, a current timestamp named ingested at U T C, and a run label. On the write side, set the checkpoint in the pipeline state volume, use append mode, write to a managed table with to table, and set trigger available now equals true so the run processes every available file and then stops. Available now is the serverless-friendly trigger. Always call await termination so the cell finishes only after the load completes.

## run1_live
The first actual run created the bronze table and recorded both files in the checkpoint. We got five rows from two files, every row tagged run one, and nothing in the rescued column. The check confirmed five rows, two source files, and zero rescued rows.

## batch2_live
A third file then arrived. Order one oh seven used the word two instead of the number two for quantity. The landing folder now listed three CSV files.

## run2_live
We ran exactly the same ingestion function again: same source, same checkpoint, same table. Run one still owned files A and B. Run two loaded only file C, three new rows. The table reached eight rows with no duplicate order IDs. Auto Loader read its checkpoint, skipped A and B, and picked up only the new file. That is incremental ingestion.

## rescue_live
Order one oh seven stayed in bronze. Quantity is null, unit price is four dollars, and the rescued data column holds the original quantity text two plus the source file path under the raw files volume. Bronze does not throw the row away. Silver is where you decide to fix, quarantine, or reject it with a reason.

## idempotent_live
What if a scheduled job runs and no new files have arrived? We ran the stream a third time. In the actual Free Edition output, the progress helper printed eight. That figure comes from summing recent progress and is not the count of newly appended rows. The bronze table still had eight rows, and no row carried the run-three label. The unchanged table count is the idempotency proof. The checkpoint remembered the three files already processed, so the rerun added nothing.

## sql_live
Finally we verified the stored table with SQL. The actual summary showed eight rows, three files, one rescued row, eight distinct orders, and valid revenue of sixty-three dollars and fifty cents. Revenue skips order one oh seven because its quantity is null. Delta history showed version zero as create table, then two streaming updates for the loads that appended data. When every assertion passed, the cell printed PASS.

## copy_into
You will also hear about COPY INTO. It is a SQL command that loads files and skips ones already loaded, tracking them in the target table metadata. Auto Loader tracks progress in its checkpoint and is built for growing file counts, schema options, and the rescued data column. A simple rule of thumb: COPY INTO fits straightforward scheduled loads; Auto Loader fits larger or evolving folders and the same code path from batch available-now runs toward streaming. The notebook shows a COPY INTO example as text only; we do not run it here.

## practice
Pause and try this. A fourth file lands with two rows: order one oh nine, North, quantity three, price two fifty; and order one ten, South, quantity one, price free. You run the ingestion with a new run label. How many rows are in bronze? How many came from the new run? How many rescued rows are there now? What is the valid revenue? And what happens if you run it again? Answers: ten rows total; two from the new run; two rescued rows, one oh seven and one ten; valid revenue seventy-one dollars; running again adds nothing. This practice answer is a prediction, not a separately captured live result.

## recap
Land files in a Unity Catalog volume and keep the checkpoint separate. Use format cloud files with an explicit schema and a rescued data column. Add file metadata and ingestion time for lineage. Use trigger available now with a checkpoint for incremental, exactly-once, serverless-friendly runs. Prove idempotency with the table count, not a progress print alone. Always verify the stored table with SQL.

## end
The companion notebook, Python source, and quick-reference cheat sheet contain the project and practice challenge. This is Build and Teach. Keep building, and keep checking your results.