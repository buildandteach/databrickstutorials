# Auto Loader: A New Column Fails Once

Video: Short coming soon (pending review) on [youtube.com/@buildandteach](https://youtube.com/@buildandteach)

Companion to [Lesson A04, Auto Loader Schema Evolution](../../lessons/associate-certification/a04-autoloader-schema-evolution/) in the Databricks Data Engineer Associate Series. Exam guide Section 2, Data Ingestion and Loading.

Auto Loader stores the schema it infers at cloudFiles.schemaLocation, and cloudFiles.schemaEvolutionMode decides what happens when a file brings a new column. The default is addNewColumns: the stream fails once with an UnknownFieldException, after Auto Loader adds the new column to the schema location. Restart the same code and the stream picks up the column. In the A04 example, day 2 adds a channel column: run 2 stops and the table still has 3 rows, then the restart loads 5 rows, with channel null for the day 1 rows. mergeSchema = true on the write lets the Delta table accept the new column, and order 1005 (amount N/A) keeps that value in _rescued_data.

Compare: rescue never fails and never adds the column (new columns go to _rescued_data). failOnNewColumns fails and stays failed until you update the schema or remove the file. none ignores new columns and never fails, and it is the default when you provide a schema.

## Memory trick

Default fails once, then adds. Rescue never fails, never adds.

One-page reference: [cheatsheet.pdf](cheatsheet.pdf)
