# A04 Auto Loader schema enforcement and evolution — narration script

Databricks Data Engineer Associate exam prep, Lesson A04. Exam guide Aug 2026, Section 2 Data Ingestion and Loading (21%).

## 01 hook
Exam question. An Auto Loader stream runs with the default settings. A new file arrives with an extra column. What happens to the stream on its next run? By the end of this lesson, you'll answer it in five seconds.

## 02 welcome
Welcome to Build and Teach. Today we'll learn how Auto Loader enforces a schema, and how it evolves that schema when the files change.

## 03 follow
To follow along, grab the notebook and the cheat sheet from the Build and Teach databricks tutorials repo on GitHub. The link is in the description.

## 04 objectives
This is lesson four of our Databricks Data Engineer Associate series, in Section two of the exam guide, Data Ingestion and Loading. That section is twenty-one percent of the exam. The objective is to use Auto Loader with schema enforcement and evolution. We'll cover schema inference, schema hints, the rescued data column, the schema evolution modes, and merge schema on the write. Everything runs in Free Edition, with a Unity Catalog volume as the source and a serverless notebook.

## 05 recap
A quick recap. Auto Loader is the cloud files streaming source. It picks up only new files, and its checkpoint remembers which files were already loaded. On serverless we run it with trigger available now, so it processes what's there and stops. The basics are in the Auto Loader Foundations video and lesson A two, and copy into is lesson A three. All three are linked in the description, so today we go straight to schemas.

## 06 plan
Here's the plan. We create a schema with two volumes, one for the source files and one for checkpoints. A day one file lands, and we look at the schema Auto Loader infers by default. Then we add typed inference and schema hints, and load a bronze table. A day two file arrives with a new column and one bad value. We watch the stream fail, restart it, and find the bad value in the rescued data column.

## 07 setup
Create a Python notebook on Serverless. Cell one reads the current catalog, and creates a new schema that starts with b t underscore autoloader, plus twelve random hex characters. Inside it, we create two volumes. One is called landing, and one is called checkpoints. The source folder is landing slash orders. The checkpoints volume will hold the stream's checkpoint and its schema location.

## 08 day1
Cell two plays the source system. It writes a C S V file called orders day one, with a header row and three fictional orders. The columns are order i d, customer, amount, and order date.

## 09 peek
Cell three only looks at the schema. We read the folder with format cloud files, and set cloud files dot schema location. That option is what turns on schema inference and evolution. Auto Loader samples the files, and stores the schema it infers in an underscore schemas folder at that location, so it can track changes over time. We don't start a stream here. Print schema is enough to see what was inferred.

## 10 peek_out
Here's the schema you should expect. Every column is a string, even order i d and amount. For C S V, JSON, and X M L files, Auto Loader infers strings by default, so a changing value can't break the type later. Notice the extra column at the end, underscore rescued data. Auto Loader adds it automatically when it infers the schema.

## 11 stream
Cell four is the real stream, wrapped in a function so we can run it again. Infer column types set to true gives typed columns, the way the regular Spark reader infers them. Schema hints pin the types we already know. Amount is a decimal, and order date is a date. Hints apply on top of the inferred schema, and they only work when you don't provide a full schema. The schema location and the checkpoint can share one folder. On the write side, merge schema set to true lets the Delta table accept new columns. Trigger available now processes the files that are there, then stops.

## 12 run1
Cell five calls run bronze. Here's what you should expect. Three rows, and this time the types are real. Order i d is an integer from inference, amount is a decimal and order date is a date from our hints, and underscore rescued data is still there at the end.

## 13 day2
Now the source system changes. Cell six writes a day two file with a new column, channel. And order ten oh five has amount N slash A, which is not a decimal. That's two different schema problems in one file.

## 14 fail
Cell seven runs the stream again. With the default schema evolution mode, add new columns, the stream stops with an unknown field exception. Before it fails, Auto Loader merges the new column into the schema stored at the schema location. The existing column types don't change. And that's the answer to our opening question. The stream fails once, after it records the new column. Restart it, and it picks up the new column. No rows from day two were written yet, so the table still has three rows.

## 15 fail_out
Here's the kind of output to expect. The error names the unknown field, channel, and the file it came from. In production, Databricks recommends running Auto Loader in Lakeflow Jobs, set to retry, so the restart happens automatically.

## 16 restart
Cell eight runs the exact same code again. This time Auto Loader reads with the updated schema, so day two loads, and the table now has five rows. This is where merge schema on the write matters. Now the stream carries a channel column the Delta table doesn't have. Without merge schema set to true, the write fails with a schema mismatch. With it, the table gains the new column.

## 17 restart_out
Here's the table you should expect. The three day one rows have null for channel. Order ten oh four loaded normally. And order ten oh five has a null amount, but the original value isn't lost. Underscore rescued data holds a small JSON blob, with amount N slash A and the path of the source file.

## 18 rescued
So what goes into the rescued data column? Three things. Type mismatches, like our N slash A. Columns that are missing from the schema, which you'll see in rescue mode. And case mismatches, like a capital A amount column. It's stored as JSON, along with the source file path, so you can fix the data later instead of losing it. With a rescued data column, a type mismatch doesn't fail the read. Only truly malformed records do.

## 19 modes
The option cloud files dot schema evolution mode picks the behavior when a new column shows up. Add new columns is the default. The stream fails once, adds the column, and resumes on restart. Add new columns with type widening does the same, and also widens types, like integer to long. It's in public preview. Rescue never evolves the schema and never fails. New columns go into the rescued data column. Fail on new columns stops the stream, and it stays stopped until you update the schema or remove the file. And none ignores new columns, never fails, and doesn't rescue them unless you set a rescued data column.

## 20 provided
One more exam detail is schema enforcement. If you provide the full schema with dot schema, the default mode becomes none, and add new columns isn't allowed. New columns are ignored. That's strict enforcement, and you can set the rescued data column option to keep values that don't fit. If you infer the schema and add schema hints, the default stays add new columns. Hints pin the types you know, they can even add a column that hasn't appeared yet, and evolution still works.

## 21 traps
Five exam traps. Number one. C S V and JSON columns are inferred as strings, unless you set infer column types. Number two. In the default mode, a new column makes the stream fail once, and a restart picks it up. Number three. Rescue mode never fails and never adds the column. Number four. With a provided schema, the default mode is none. Number five. To add the new column to the Delta table, set merge schema on the write.

## 22 challenge
Your practice challenge. After cell eight, a day three file lands with another new column, coupon. Question one. You call the same run bronze function, in the default mode. What happens on the next run, and what happens after that? Question two. A different stream, set to rescue mode, reads day three. Does it add a column, and does it fail? Question three. A stream with a provided schema, and no mode set, reads the file. Which mode applies, and where does coupon go? Pause the video and write down your three answers.

## 23 answer
Here's the explanation. Question one. The run fails once with an unknown field exception for coupon, and the schema location is updated. Run it again, day three loads, and merge schema adds coupon to the table. Question two. Rescue mode doesn't fail and doesn't add a column. The coupon values go into the rescued data column. Question three. With a provided schema, the default mode is none. Coupon is ignored, the stream doesn't fail, and the values aren't rescued unless you set the rescued data column option. The solution cell is in the notebook, commented out.

## 24 close
Let's recap. Schema location turns on inference and evolution. Infer column types and schema hints give you real types. The default mode fails once on a new column, and a restart with merge schema adds it. And the rescued data column keeps values that don't fit. Next time, in lesson five, we load JSON and nested data, and flatten arrays with explode. The notebook and cheat sheet are in the Build and Teach databricks tutorials repo on GitHub, linked in the description. Thanks for watching Build and Teach.
