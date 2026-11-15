# A05 JSON and nested data with explode — narration script

Databricks Data Engineer Associate exam prep, Lesson A05. Exam guide Aug 2026, Section 2 Data Ingestion and Loading (21%) and Section 3 Data Transformation and Modeling (22%).

## 01 hook
Exam question. A table has four orders. One has two items, one has one item, one has an empty array, and one has no items at all. You explode the items column. How many rows come back? By the end of this lesson, you'll answer it in five seconds.

## 02 welcome
Welcome to Build and Teach. Today we'll learn how to load nested JSON, reach into structs, and flatten arrays with explode.

## 03 follow
To follow along, grab the notebook and the cheat sheet from the Build and Teach databricks tutorials repo on GitHub. The link is in the description.

## 04 objectives
This is lesson five of our Databricks Data Engineer Associate series, and it covers two sections of the exam guide. Section two, Data Ingestion and Loading, is twenty-one percent of the exam. One of its objectives is ingesting semi-structured data, like JSON and nested data, into Unity Catalog governed Delta tables. Section three, Data Transformation and Modeling, is twenty-two percent. It asks you to manipulate columns and rows, and that includes exploding arrays. Everything today runs in Free Edition, with a Unity Catalog volume as the source and a serverless notebook.

## 05 recap
A quick recap. In the A two lesson, we covered ingestion patterns. In A three, we covered the copy into command. And in A four, we covered Auto Loader schema evolution. The objective also names Lakeflow Connect and managed connectors. Whichever way the JSON comes in, you get the same nested shape. So today we land the files in a volume, and focus on that shape. The earlier lessons are linked in the description.

## 06 plan
Here's the plan. We create a schema and a volume, and write a small file of nested JSON orders. We read it twice, once with an inferred schema and once with an explicit schema. Then we reach into structs with dot notation, and flatten the items array with explode, explode outer, and pos explode. Finally, we write a flat silver table, use from JSON to parse a JSON string, and query the raw text with the SQL colon syntax.

## 07 setup
Create a Python notebook on Serverless. Cell one reads the current catalog, and creates a new schema that starts with b t underscore JSON, plus twelve random hex characters. Inside it we create one volume, called landing, and the source folder is landing slash orders.

## 08 write
Cell two plays the source system. It writes a file called orders dot JSON, with one order per line. Each order has an order i d, and a customer struct with a name and an address. The address is another struct, with a city and a country. Then there's an items array, where every item is a struct with a sku, a quantity, and a price, and a tags array of plain strings. Order ten oh one has two items, ten oh two has one, ten oh three has an empty items array, and ten oh four has no items field at all.

## 09 infer
Cell three reads the folder with spark dot read dot JSON, and prints the schema. By default, the JSON reader expects one complete record per line. If your file is a pretty-printed document that spans many lines, set the multi line option to true. Otherwise the reader can't parse the records.

## 10 infer_out
Here's the schema you should expect. Spark found the nesting on its own. Customer is a struct with another struct inside. Items is an array of structs, and tags is an array of strings. Whole numbers come back as long, decimals as double, and the field names are sorted alphabetically. Notice this is different from Auto Loader in lesson A four. Auto Loader infers JSON columns as strings unless you set infer column types. The plain JSON reader infers real types.

## 11 explicit
Cell four reads the same files with an explicit schema, written as a D D L string. Struct and array types nest inside angle brackets. With an explicit schema, Spark skips the inference pass, and you get exactly the types you want. Here that's an integer order i d and a decimal price. We save this as a bronze Delta table called orders bronze. The nested columns are stored as they are.

## 12 dot
Cell five reaches into the structs. Customer dot name gives the name, and customer dot address dot city goes two levels down. The new column takes the last field name, so we alias it to keep things clear. And customer dot star expands every field of the struct into its own column. Dot notation works on structs. It doesn't turn an array into rows. For that, we need explode.

## 13 dot_out
Here's what you should expect. Still four rows, one per order, with the name and the city as plain columns. Reading struct fields never changes the number of rows.

## 14 explode
Cell six is the core of the lesson. Explode takes an array column, and returns one row for every element. The other columns you select, like order i d, are repeated on each row. And that's the answer to our opening question. Order ten oh one gives two rows, ten oh two gives one row. The empty array in ten oh three gives no rows, and the null in ten oh four gives no rows. So explode returns three rows, and two of the four orders disappear.

## 15 explode_out
Here's what you should expect. Three rows. Order ten oh one appears twice, once per item, and each item is still a struct. Orders ten oh three and ten oh four are not in the result at all.

## 16 outer
Cell seven shows the two variations the exam likes. Explode outer works like explode, but when the array is null or empty, it still returns one row, with null for the item. So now we get five rows, and every order is kept. Pos explode adds the position of each element. It returns two columns, pos and col, and the position starts at zero. Like plain explode, it skips empty and null arrays. If you need the position and every order, there's pos explode outer.

## 17 outer_out
Here's what you should expect. Explode outer returns five rows, and orders ten oh three and ten oh four show a null item. On the right, pos explode numbers the items zero and one for order ten oh one, and zero for ten oh two.

## 18 silver
Cell eight puts it together. First we explode the items into one row per item. Then we use dot notation on the exploded struct to pull out the sku, the quantity, and the price, and we compute a line total. We write that as a flat silver table called order items. Expect three rows, one per item. Ten oh one gets a line total of ten and twelve fifty, and ten oh two gets twelve. If the business needs orders with no items too, use explode outer instead.

## 19 fromjson
Sometimes the JSON arrives as a string column, not as a file the reader parses for you. A message payload, or an A P I response saved as text, is a common example. Cell nine simulates that. It reads the file as text, so each line is one string in a column called value. From JSON takes the string column and a schema, and returns a struct. We reuse our D D L schema, and then dot notation works again, like o dot customer dot address dot city. If you don't know the schema, schema of JSON can derive one from a sample string.

## 20 sql
Databricks SQL can also query JSON strings directly, with the colon syntax. Cell ten registers the text as a temporary view. Value colon order i d extracts a field, and dot notation goes deeper, like value colon customer dot address dot city. Square brackets index an array from zero, and bracket star pulls a field from every element. The results are strings, so cast with double colon when you need a number. For order ten oh one, expect Austin, A one, and both skus. One detail. Bracket star works only on JSON strings, not on a native array column. Databricks also has a VARIANT type for semi-structured data, which is covered in the Professional series. Cell eleven explodes in SQL. Databricks now recommends calling explode as a table reference, with LATERAL. The older forms, explode in the select list, or lateral view, still work, but they're deprecated. Expect the same three rows.

## 21 compare
Here's the mental model for the exam. A struct is a set of named fields. You read it with dot notation, or expand it with star, and the row count never changes. An array is a list of elements. You turn it into rows with explode, keep null and empty arrays with explode outer, and get the position with pos explode. The row count changes. And on a map column, explode returns two columns, key and value.

## 22 traps
Five exam traps. Number one. Explode drops rows where the array is null or empty. Number two. Explode outer keeps those rows, with a null element. Number three. Pos explode returns pos and col, and the position starts at zero. Number four. A pretty-printed JSON file needs multi line set to true. Number five. The plain JSON reader infers real types, but Auto Loader infers JSON columns as strings unless you turn on infer column types.

## 23 challenge
Your practice challenge uses the tags array. Order ten oh one has web and promo. Ten oh two has store. Ten oh three has an empty array, and ten oh four has web. Question one. How many rows does explode on tags return? Question two. How many with explode outer? Question three. With pos explode, what position does promo get? Pause the video and write down your three answers.

## 24 answer
Here's the explanation. Question one. Explode returns four rows. Two for ten oh one, one for ten oh two, none for the empty array in ten oh three, and one for ten oh four. Question two. Explode outer returns five rows, because ten oh three is kept with a null tag. Question three. Promo gets position one, because positions start at zero, and web is zero. The solution cell is in the notebook, commented out.

## 25 close
Let's recap. The JSON reader expects one record per line, unless you set multi line. An explicit schema skips inference and gives you the types you choose. Use dot notation for structs, and explode for arrays. Explode outer keeps null and empty arrays. And pos explode gives you the position. Next time, in lesson six, we compare ingestion methods, Lakeflow Connect, J D B C, and REST. The notebook and cheat sheet are in the Build and Teach databricks tutorials repo on GitHub, linked in the description. Thanks for watching Build and Teach.
