-- =============================================================================
-- READABLE COPY of part2.sql (reformatted Nov 10, 2026). part2.sql is the
-- original text from the saved query "Build and Teach - SQL Editor Part 2". It is unchanged.
-- Part 2 of 3: GROUP BY, HAVING, joins, CTEs and window functions
-- Only whitespace/line breaks differ from the original. The STATEMENT/EXPECT comments are added here:
-- the original has no EXPECT comments for Part 2. The EXPECT values were computed from this exact
-- SQL (DuckDB, Nov 10) and agree with Part 1's observed 6 orders / 142.50.
-- Confirm them on the Free Edition captures (shotlist.md).
-- =============================================================================

-- STATEMENT 01 — Baseline: valid orders and revenue
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
    order_id,
    customer,
    region,
    category,
    order_date,
    CAST(quantity * unit_price AS DECIMAL(12,2)) AS order_total
  FROM orders
  WHERE quantity IS NOT NULL AND unit_price IS NOT NULL
)
SELECT COUNT(*) AS valid_orders, SUM(order_total) AS valid_revenue
FROM valid;
-- EXPECT: valid_orders = 6, valid_revenue = 142.50

-- STATEMENT 02 — GROUP BY region and category
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
    order_id,
    region,
    category,
    CAST(quantity * unit_price AS DECIMAL(12,2)) AS order_total
  FROM orders
  WHERE quantity IS NOT NULL AND unit_price IS NOT NULL
)
SELECT region, category, COUNT(*) AS order_count, SUM(order_total) AS revenue
FROM valid
GROUP BY region, category
ORDER BY region, category;
-- EXPECT: 5 rows: EAST/Books 1/12.00 · NORTH/Books 1/20.00 · NORTH/Toys 2/60.00 · SOUTH/Toys 1/25.00 · WEST/Books 1/25.50

-- STATEMENT 03 — HAVING keeps groups with revenue of at least 25
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
  SELECT region, category, CAST(quantity * unit_price AS DECIMAL(12,2)) AS order_total
  FROM orders
  WHERE quantity IS NOT NULL AND unit_price IS NOT NULL
)
SELECT region, category, COUNT(*) AS order_count, SUM(order_total) AS revenue
FROM valid
GROUP BY region, category
HAVING SUM(order_total) >= 25
ORDER BY revenue DESC, region, category;
-- EXPECT: 3 rows: NORTH/Toys 60.00 · WEST/Books 25.50 · SOUTH/Toys 25.00

-- STATEMENT 04 — INNER JOIN to customer tiers
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
customers AS (
  SELECT * FROM VALUES
    ('Ana', 'gold'),
    ('Bo', 'silver'),
    ('Cy', 'gold'),
    ('Dee', 'bronze'),
    ('Fay', 'silver'),
    ('Gus', 'bronze'),
    ('Hana', 'gold')
  AS c(customer, tier)
),
valid AS (
  SELECT
    order_id,
    customer,
    region,
    category,
    CAST(quantity * unit_price AS DECIMAL(12,2)) AS order_total
  FROM orders
  WHERE quantity IS NOT NULL AND unit_price IS NOT NULL
)
SELECT v.order_id, v.customer, c.tier, v.order_total
FROM valid v
INNER JOIN customers c ON v.customer = c.customer
ORDER BY v.order_id;
-- EXPECT: 5 rows: 101 gold · 102 silver · 103 gold · 104 bronze · 108 gold (105 drops: customer is NULL)

-- STATEMENT 05 — LEFT JOIN keeps every valid order
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
customers AS (
  SELECT * FROM VALUES
    ('Ana', 'gold'),
    ('Bo', 'silver'),
    ('Cy', 'gold'),
    ('Dee', 'bronze'),
    ('Fay', 'silver'),
    ('Gus', 'bronze'),
    ('Hana', 'gold')
  AS c(customer, tier)
),
valid AS (
  SELECT
    order_id,
    customer,
    region,
    category,
    CAST(quantity * unit_price AS DECIMAL(12,2)) AS order_total
  FROM orders
  WHERE quantity IS NOT NULL AND unit_price IS NOT NULL
)
SELECT v.order_id, v.customer, c.tier, v.order_total
FROM valid v
LEFT JOIN customers c ON v.customer = c.customer
ORDER BY v.order_id;
-- EXPECT: 6 rows: same 5 + 105 with customer NULL and tier NULL

-- STATEMENT 06 — CTE: revenue by region
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
  SELECT region, CAST(quantity * unit_price AS DECIMAL(12,2)) AS order_total
  FROM orders
  WHERE quantity IS NOT NULL AND unit_price IS NOT NULL
),
by_region AS (
  SELECT region, COUNT(*) AS order_count, SUM(order_total) AS revenue
  FROM valid
  GROUP BY region
)
SELECT region, order_count, revenue
FROM by_region
ORDER BY region;
-- EXPECT: EAST 1/12.00 · NORTH 3/80.00 · SOUTH 1/25.00 · WEST 1/25.50

-- STATEMENT 07 — Window: ROW_NUMBER per category
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
    order_id,
    customer,
    category,
    CAST(quantity * unit_price AS DECIMAL(12,2)) AS order_total
  FROM orders
  WHERE quantity IS NOT NULL AND unit_price IS NOT NULL
)
SELECT
  order_id,
  customer,
  category,
  order_total,
  ROW_NUMBER() OVER (
    PARTITION BY category
    ORDER BY order_total DESC, order_id DESC
  ) AS category_rank
FROM valid
ORDER BY category, category_rank;
-- EXPECT: Books: 103=1, 101=2, 105=3 · Toys: 108=1, 104=2, 102=3

-- STATEMENT 08 — Window: running revenue
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
    order_id,
    order_date,
    CAST(quantity * unit_price AS DECIMAL(12,2)) AS order_total
  FROM orders
  WHERE quantity IS NOT NULL AND unit_price IS NOT NULL
)
SELECT
  order_id,
  order_date,
  order_total,
  SUM(order_total) OVER (
    ORDER BY order_date, order_id
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS running_revenue
FROM valid
ORDER BY order_date, order_id;
-- EXPECT: 20.00, 45.00, 70.50, 100.50, 112.50, 142.50 (ends at 142.50)
