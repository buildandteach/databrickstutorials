-- =============================================================================
-- READABLE COPY of part3.sql (reformatted Nov 10, 2026). part3.sql is the
-- original text from the saved query "Build and Teach - SQL Editor Part 3". It is unchanged.
-- Part 3 of 3: summary table, visualization, saved queries, query history
-- Only whitespace/line breaks differ from the original. The STATEMENT/EXPECT comments are added here:
-- the original has no EXPECT comments for Part 3. The EXPECT values were computed from this exact
-- SQL (DuckDB, Nov 10) and agree with Part 1's observed 6 orders / 142.50.
-- Confirm them on the Free Edition captures (shotlist.md).
-- =============================================================================

-- STATEMENT 01 — Use the workspace catalog
USE CATALOG workspace;
-- EXPECT: OK

-- STATEMENT 02 — Create a schema for this lesson (IF NOT EXISTS)
CREATE SCHEMA IF NOT EXISTS bt_sql_6aab57cf80df;
-- EXPECT: OK

-- STATEMENT 03 — Managed Delta table: orders
CREATE TABLE workspace.bt_sql_6aab57cf80df.orders AS
SELECT * FROM VALUES
  (101, 'Ana', 'NORTH', 'Books', 2, CAST(10.00 AS DECIMAL(10,2)), DATE '2026-10-01'),
  (102, 'Bo', 'SOUTH', 'Toys', 1, CAST(25.00 AS DECIMAL(10,2)), DATE '2026-10-02'),
  (103, 'Cy', 'WEST', 'Books', 3, CAST(8.50 AS DECIMAL(10,2)), DATE '2026-10-02'),
  (104, 'Dee', 'NORTH', 'Toys', 2, CAST(15.00 AS DECIMAL(10,2)), DATE '2026-10-03'),
  (105, CAST(NULL AS STRING), 'EAST', 'Books', 1, CAST(12.00 AS DECIMAL(10,2)), DATE '2026-10-03'),
  (106, 'Fay', 'SOUTH', 'Toys', CAST(NULL AS INT), CAST(20.00 AS DECIMAL(10,2)), DATE '2026-10-04'),
  (107, 'Gus', 'WEST', 'Books', 4, CAST(NULL AS DECIMAL(10,2)), DATE '2026-10-04'),
  (108, 'Hana', 'NORTH', 'Toys', 1, CAST(30.00 AS DECIMAL(10,2)), DATE '2026-10-05')
AS t(order_id, customer, region, category, quantity, unit_price, order_date);
-- EXPECT: 8 rows inserted

-- STATEMENT 04 — Managed Delta table: customers
CREATE TABLE workspace.bt_sql_6aab57cf80df.customers AS
SELECT * FROM VALUES
  ('Ana','gold'),
  ('Bo','silver'),
  ('Cy','gold'),
  ('Dee','bronze'),
  ('Fay','silver'),
  ('Gus','bronze'),
  ('Hana','gold')
AS c(customer,tier);
-- EXPECT: 7 rows inserted

-- STATEMENT 05 — Row counts
SELECT 'orders' AS table_name, COUNT(*) AS n
FROM workspace.bt_sql_6aab57cf80df.orders
UNION ALL
SELECT 'customers', COUNT(*)
FROM workspace.bt_sql_6aab57cf80df.customers;
-- EXPECT: orders 8 · customers 7

-- STATEMENT 06 — Baseline: valid orders and revenue
SELECT
  COUNT(*) AS valid_orders,
  SUM(CAST(quantity * unit_price AS DECIMAL(12,2))) AS valid_revenue
FROM workspace.bt_sql_6aab57cf80df.orders
WHERE quantity IS NOT NULL AND unit_price IS NOT NULL;
-- EXPECT: 6 · 142.50

-- STATEMENT 07 — Summary (gold) table: region_category_revenue
CREATE TABLE workspace.bt_sql_6aab57cf80df.region_category_revenue AS
SELECT
  region,
  category,
  COUNT(*) AS order_count,
  SUM(CAST(quantity * unit_price AS DECIMAL(12,2))) AS revenue
FROM workspace.bt_sql_6aab57cf80df.orders
WHERE quantity IS NOT NULL AND unit_price IS NOT NULL
GROUP BY region, category;
-- EXPECT: 5 rows inserted

-- STATEMENT 08 — Read the summary table
SELECT region, category, order_count, revenue
FROM workspace.bt_sql_6aab57cf80df.region_category_revenue
ORDER BY region, category;
-- EXPECT: same 5 buckets as Part 2 statement 02

-- STATEMENT 09 — Validation check
SELECT
  COUNT(*) AS bucket_count,
  SUM(order_count) AS total_orders,
  SUM(revenue) AS total_revenue,
  CASE
    WHEN COUNT(*)=5 AND SUM(order_count)=6 AND SUM(revenue)=CAST(142.50 AS DECIMAL(12,2))
      THEN 'PASS'
    ELSE 'FAIL'
  END AS status
FROM workspace.bt_sql_6aab57cf80df.region_category_revenue;
-- EXPECT: bucket_count 5 · total_orders 6 · total_revenue 142.50 · status PASS

-- STATEMENT 10 — Revenue by region (source for the bar chart)
SELECT region, SUM(revenue) AS revenue
FROM workspace.bt_sql_6aab57cf80df.region_category_revenue
GROUP BY region
ORDER BY region;
-- EXPECT: EAST 12.00 · NORTH 80.00 · SOUTH 25.00 · WEST 25.50

-- STATEMENT 11 — Revenue by customer tier
SELECT
  c.tier,
  COUNT(DISTINCT o.order_id) AS matched_orders,
  SUM(CAST(o.quantity * o.unit_price AS DECIMAL(12,2))) AS revenue
FROM workspace.bt_sql_6aab57cf80df.orders o
INNER JOIN workspace.bt_sql_6aab57cf80df.customers c ON o.customer=c.customer
WHERE o.quantity IS NOT NULL AND o.unit_price IS NOT NULL
GROUP BY c.tier
ORDER BY c.tier;
-- EXPECT: bronze 1/30.00 · gold 3/75.50 · silver 1/25.00
