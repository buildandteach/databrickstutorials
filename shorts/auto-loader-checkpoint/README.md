# Auto Loader: Load Only New Files

Video: [youtube.com/shorts/Bv2EzzZoSnI](https://youtube.com/shorts/Bv2EzzZoSnI)

Companion to [Lesson 2, Ingestion Patterns](../../lessons/associate-certification/a02-ingestion-patterns/) in the Databricks Data Engineer Associate Series.

A batch load with spark.read re-reads every file each run, so batch plus append means duplicates. Auto Loader with a checkpoint loads each file once, and trigger(availableNow=True) processes all new data then stops, which works on serverless compute. The example uses five fictional orders. The day 1 file has 3 rows and the day 2 file has 2 rows. A batch re-read returns 5 rows. Auto Loader reads 2.

## Memory trick

Batch re-reads, Auto Loader remembers. The checkpoint is its bookmark.

One-page reference: [cheatsheet.pdf](cheatsheet.pdf)
