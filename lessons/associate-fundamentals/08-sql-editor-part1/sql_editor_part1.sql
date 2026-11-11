-- READABLE COPY of part1.sql (Nov 10, 2026): only the stacked indentation from pasting is undone.
-- Every character apart from leading spaces is identical. part1.sql is the original.
-- =============================================================================
-- Build and Teach — Query and Analyze Data with the Databricks SQL Editor
-- Part 1 of 3: Open the SQL Editor; SELECT, WHERE, ORDER BY, CASE, and nulls
-- Free Edition: serverless SQL warehouse (one warehouse, 2X-Small).
-- Saved-query name: Build and Teach - SQL Editor Part 1
-- Standalone: VALUES CTEs only — no CREATE/DROP/REPLACE/overwrite.
-- Replace nothing for Part 1. Run each numbered statement separately (or
-- highlight and Run). Hand-computed expected results are in VALIDATION_HANDOFF.md.
-- =============================================================================

-- STATEMENT 01 — Warehouse / connectivity smoke check
-- UI first: SQL Editor → choose Serverless Starter Warehouse (or the single
-- Free Edition warehouse) → set catalog (usually workspace) → schema default OK.
SELECT 1 AS sql_editor_ready;


-- STATEMENT 02 — Inline sample: eight fictional orders (two incomplete rows)
-- Same sample as the PySpark query/analyze series for cross-lesson consistency.
WITH orders AS (
  SELECT * FROM VALUES
    (101, 'Ana',  'NORTH', 'Books', 2,    CAST(10.00 AS DECIMAL(10,2)), DATE '2026-10-01'),
    (102, 'Bo',   'SOUTH', 'Toys',  1,    CAST(25.00 AS DECIMAL(10,2)), DATE '2026-10-02'),
    (103, 'Cy',   'WEST',  'Books', 3,    CAST(8.50  AS DECIMAL(10,2)), DATE '2026-10-02'),
    (104, 'Dee',  'NORTH', 'Toys',  2,    CAST(15.00 AS DECIMAL(10,2)), DATE '2026-10-03'),
    (105, NULL,   'EAST',  'Books', 1,    CAST(12.00 AS DECIMAL(10,2)), DATE '2026-10-03'),
    (106, 'Fay',  'SOUTH', 'Toys',  NULL, CAST(20.00 AS DECIMAL(10,2)), DATE '2026-10-04'),
    (107, 'Gus',  'WEST',  'Books', 4,    CAST(NULL  AS DECIMAL(10,2)), DATE '2026-10-04'),
    (108, 'Hana', 'NORTH', 'Toys',  1,    CAST(30.00 AS DECIMAL(10,2)), DATE '2026-10-05')
  AS t(order_id, customer, region, category, quantity, unit_price, order_date)
)
SELECT
  order_id,
  customer,
  region,
  category,
  quantity,
  unit_price,
  order_date,
  CAST(quantity * unit_price AS DECIMAL(12,2)) AS order_total
FROM orders
ORDER BY order_id;
-- EXPECT: 8 rows. order_total NULL for 106 and 107.


-- STATEMENT 03 — Valid revenue rows (quantity and unit_price both present)
WITH orders AS (
  SELECT * FROM VALUES
    (101, 'Ana',  'NORTH', 'Books', 2,    CAST(10.00 AS DECIMAL(10,2)), DATE '2026-10-01'),
    (102, 'Bo',   'SOUTH', 'Toys',  1,    CAST(25.00 AS DECIMAL(10,2)), DATE '2026-10-02'),
    (103, 'Cy',   'WEST',  'Books', 3,    CAST(8.50  AS DECIMAL(10,2)), DATE '2026-10-02'),
    (104, 'Dee',  'NORTH', 'Toys',  2,    CAST(15.00 AS DECIMAL(10,2)), DATE '2026-10-03'),
    (105, NULL,   'EAST',  'Books', 1,    CAST(12.00 AS DECIMAL(10,2)), DATE '2026-10-03'),
    (106, 'Fay',  'SOUTH', 'Toys',  NULL, CAST(20.00 AS DECIMAL(10,2)), DATE '2026-10-04'),
    (107, 'Gus',  'WEST',  'Books', 4,    CAST(NULL  AS DECIMAL(10,2)), DATE '2026-10-04'),
    (108, 'Hana', 'NORTH', 'Toys',  1,    CAST(30.00 AS DECIMAL(10,2)), DATE '2026-10-05')
  AS t(order_id, customer, region, category, quantity, unit_price, order_date)
),
valid AS (
  SELECT
    order_id, customer, region, category, quantity, unit_price, order_date,
    CAST(quantity * unit_price AS DECIMAL(12,2)) AS order_total
  FROM orders
  WHERE quantity IS NOT NULL AND unit_price IS NOT NULL
)
SELECT COUNT(*) AS valid_orders, SUM(order_total) AS valid_revenue
FROM valid;
-- EXPECT: valid_orders = 6, valid_revenue = 142.50


-- STATEMENT 04 — SELECT / WHERE / ORDER BY: Books in NORTH or WEST
WITH orders AS (
  SELECT * FROM VALUES
    (101, 'Ana',  'NORTH', 'Books', 2,    CAST(10.00 AS DECIMAL(10,2)), DATE '2026-10-01'),
    (102, 'Bo',   'SOUTH', 'Toys',  1,    CAST(25.00 AS DECIMAL(10,2)), DATE '2026-10-02'),
    (103, 'Cy',   'WEST',  'Books', 3,    CAST(8.50  AS DECIMAL(10,2)), DATE '2026-10-02'),
    (104, 'Dee',  'NORTH', 'Toys',  2,    CAST(15.00 AS DECIMAL(10,2)), DATE '2026-10-03'),
    (105, NULL,   'EAST',  'Books', 1,    CAST(12.00 AS DECIMAL(10,2)), DATE '2026-10-03'),
    (106, 'Fay',  'SOUTH', 'Toys',  NULL, CAST(20.00 AS DECIMAL(10,2)), DATE '2026-10-04'),
    (107, 'Gus',  'WEST',  'Books', 4,    CAST(NULL  AS DECIMAL(10,2)), DATE '2026-10-04'),
    (108, 'Hana', 'NORTH', 'Toys',  1,    CAST(30.00 AS DECIMAL(10,2)), DATE '2026-10-05')
  AS t(order_id, customer, region, category, quantity, unit_price, order_date)
),
valid AS (
  SELECT
    order_id, customer, region, category,
    CAST(quantity * unit_price AS DECIMAL(12,2)) AS order_total
  FROM orders
  WHERE quantity IS NOT NULL AND unit_price IS NOT NULL
)
SELECT order_id, customer, region, category, order_total
FROM valid
WHERE category = 'Books' AND region IN ('NORTH', 'WEST')
ORDER BY order_id;
-- EXPECT: 101 (20.00), 103 (25.50); sum 45.50


-- STATEMENT 05 — CASE expression + high-value filter + ORDER BY
WITH orders AS (
  SELECT * FROM VALUES
    (101, 'Ana',  'NORTH', 'Books', 2,    CAST(10.00 AS DECIMAL(10,2)), DATE '2026-10-01'),
    (102, 'Bo',   'SOUTH', 'Toys',  1,    CAST(25.00 AS DECIMAL(10,2)), DATE '2026-10-02'),
    (103, 'Cy',   'WEST',  'Books', 3,    CAST(8.50  AS DECIMAL(10,2)), DATE '2026-10-02'),
    (104, 'Dee',  'NORTH', 'Toys',  2,    CAST(15.00 AS DECIMAL(10,2)), DATE '2026-10-03'),
    (105, NULL,   'EAST',  'Books', 1,    CAST(12.00 AS DECIMAL(10,2)), DATE '2026-10-03'),
    (106, 'Fay',  'SOUTH', 'Toys',  NULL, CAST(20.00 AS DECIMAL(10,2)), DATE '2026-10-04'),
    (107, 'Gus',  'WEST',  'Books', 4,    CAST(NULL  AS DECIMAL(10,2)), DATE '2026-10-04'),
    (108, 'Hana', 'NORTH', 'Toys',  1,    CAST(30.00 AS DECIMAL(10,2)), DATE '2026-10-05')
  AS t(order_id, customer, region, category, quantity, unit_price, order_date)
),
valid AS (
  SELECT
    order_id, customer, region, category,
    CAST(quantity * unit_price AS DECIMAL(12,2)) AS order_total
  FROM orders
  WHERE quantity IS NOT NULL AND unit_price IS NOT NULL
)
SELECT
  order_id,
  customer,
  region,
  category,
  order_total,
  CASE
    WHEN order_total >= 30 THEN 'premium'
    WHEN order_total >= 25 THEN 'high'
    ELSE 'standard'
  END AS value_band
FROM valid
WHERE order_total >= 25
ORDER BY order_total DESC, order_id DESC;
-- EXPECT: 108 premium 30.00, 104 premium 30.00, 103 high 25.50, 102 high 25.00; sum 110.50


-- STATEMENT 06 — Null handling: counts before cleaning
WITH orders AS (
  SELECT * FROM VALUES
    (101, 'Ana',  'NORTH', 'Books', 2,    CAST(10.00 AS DECIMAL(10,2)), DATE '2026-10-01'),
    (102, 'Bo',   'SOUTH', 'Toys',  1,    CAST(25.00 AS DECIMAL(10,2)), DATE '2026-10-02'),
    (103, 'Cy',   'WEST',  'Books', 3,    CAST(8.50  AS DECIMAL(10,2)), DATE '2026-10-02'),
    (104, 'Dee',  'NORTH', 'Toys',  2,    CAST(15.00 AS DECIMAL(10,2)), DATE '2026-10-03'),
    (105, NULL,   'EAST',  'Books', 1,    CAST(12.00 AS DECIMAL(10,2)), DATE '2026-10-03'),
    (106, 'Fay',  'SOUTH', 'Toys',  NULL, CAST(20.00 AS DECIMAL(10,2)), DATE '2026-10-04'),
    (107, 'Gus',  'WEST',  'Books', 4,    CAST(NULL  AS DECIMAL(10,2)), DATE '2026-10-04'),
    (108, 'Hana', 'NORTH', 'Toys',  1,    CAST(30.00 AS DECIMAL(10,2)), DATE '2026-10-05')
  AS t(order_id, customer, region, category, quantity, unit_price, order_date)
)
SELECT
  COUNT(*) AS total_orders,
  SUM(CASE WHEN customer IS NULL THEN 1 ELSE 0 END) AS null_customer,
  SUM(CASE WHEN quantity IS NULL THEN 1 ELSE 0 END) AS null_quantity,
  SUM(CASE WHEN unit_price IS NULL THEN 1 ELSE 0 END) AS null_unit_price
FROM orders;
-- EXPECT: 8 / 1 / 1 / 1


-- STATEMENT 07 — COALESCE for display (does not invent business values)
WITH orders AS (
  SELECT * FROM VALUES
    (101, 'Ana',  'NORTH', 'Books', 2,    CAST(10.00 AS DECIMAL(10,2)), DATE '2026-10-01'),
    (102, 'Bo',   'SOUTH', 'Toys',  1,    CAST(25.00 AS DECIMAL(10,2)), DATE '2026-10-02'),
    (103, 'Cy',   'WEST',  'Books', 3,    CAST(8.50  AS DECIMAL(10,2)), DATE '2026-10-02'),
    (104, 'Dee',  'NORTH', 'Toys',  2,    CAST(15.00 AS DECIMAL(10,2)), DATE '2026-10-03'),
    (105, NULL,   'EAST',  'Books', 1,    CAST(12.00 AS DECIMAL(10,2)), DATE '2026-10-03'),
    (106, 'Fay',  'SOUTH', 'Toys',  NULL, CAST(20.00 AS DECIMAL(10,2)), DATE '2026-10-04'),
    (107, 'Gus',  'WEST',  'Books', 4,    CAST(NULL  AS DECIMAL(10,2)), DATE '2026-10-04'),
    (108, 'Hana', 'NORTH', 'Toys',  1,    CAST(30.00 AS DECIMAL(10,2)), DATE '2026-10-05')
  AS t(order_id, customer, region, category, quantity, unit_price, order_date)
)
SELECT
  order_id,
  COALESCE(customer, '(missing)') AS customer_display,
  region,
  category
FROM orders
WHERE customer IS NULL OR quantity IS NULL OR unit_price IS NULL
ORDER BY order_id;
-- EXPECT: 105 (missing), 106 Fay, 107 Gus


-- STATEMENT 08 — Practice: Toys in SOUTH or NORTH from valid rows, high to low
WITH orders AS (
  SELECT * FROM VALUES
    (101, 'Ana',  'NORTH', 'Books', 2,    CAST(10.00 AS DECIMAL(10,2)), DATE '2026-10-01'),
    (102, 'Bo',   'SOUTH', 'Toys',  1,    CAST(25.00 AS DECIMAL(10,2)), DATE '2026-10-02'),
    (103, 'Cy',   'WEST',  'Books', 3,    CAST(8.50  AS DECIMAL(10,2)), DATE '2026-10-02'),
    (104, 'Dee',  'NORTH', 'Toys',  2,    CAST(15.00 AS DECIMAL(10,2)), DATE '2026-10-03'),
    (105, NULL,   'EAST',  'Books', 1,    CAST(12.00 AS DECIMAL(10,2)), DATE '2026-10-03'),
    (106, 'Fay',  'SOUTH', 'Toys',  NULL, CAST(20.00 AS DECIMAL(10,2)), DATE '2026-10-04'),
    (107, 'Gus',  'WEST',  'Books', 4,    CAST(NULL  AS DECIMAL(10,2)), DATE '2026-10-04'),
    (108, 'Hana', 'NORTH', 'Toys',  1,    CAST(30.00 AS DECIMAL(10,2)), DATE '2026-10-05')
  AS t(order_id, customer, region, category, quantity, unit_price, order_date)
),
valid AS (
  SELECT
    order_id, customer, region, category,
    CAST(quantity * unit_price AS DECIMAL(12,2)) AS order_total
  FROM orders
  WHERE quantity IS NOT NULL AND unit_price IS NOT NULL
)
SELECT order_id, customer, order_total
FROM valid
WHERE category = 'Toys' AND region IN ('SOUTH', 'NORTH')
ORDER BY order_total DESC, order_id DESC;
-- EXPECT: 108 (30.00), 104 (30.00), 102 (25.00)
