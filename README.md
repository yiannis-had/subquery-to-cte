# Subquery-to-CTE

Automatically rewrites SQL `SELECT` statements by extracting nested subqueries into Common Table Expressions (CTEs).

## Supported subquery locations

- **FROM** clause (including nested subqueries within subqueries)
- **JOIN** clause (LEFT, RIGHT, INNER, CROSS, etc.)
- **WHERE** clause (`IN`, `EXISTS`, scalar comparisons)
- **SELECT** clause (scalar subqueries in the projection list)

## Features

- **Alias-aware naming** — CTE names are derived from existing aliases when available (e.g. `recent_orders`, `tier`), falling back to `cte_1`, `cte_2`, etc.
- **Comment propagation** — SQL comments (`--` and `/* */`) preceding a subquery are preserved and attached above the corresponding CTE in the output.
- **Name collision avoidance** — existing table names in the query are collected upfront so generated CTE names never shadow them.

## Usage

Edit the `query` variable in `subq-to-cte.py` and run:

```bash
python3 subq-to-cte.py
```

The rewritten SQL is printed to stdout.

### As a library

```python
from subq_to_cte import rewrite_query  # rename file or adjust import as needed

sql = "SELECT * FROM (SELECT id FROM users) u WHERE ..."
print(rewrite_query(sql))
```

## Example

**Input:**
```sql
SELECT c.name, spending.total
FROM customers c
JOIN (
    SELECT customer_id, SUM(amount) AS total
    FROM orders
    WHERE order_id IN (
        SELECT order_id FROM order_items WHERE quantity > 1
    )
    GROUP BY customer_id
) spending ON spending.customer_id = c.id
WHERE c.id IN (
    SELECT customer_id FROM vip_list
)
```

**Output:**
```sql
WITH cte_1 AS (
 SELECT order_id
 FROM order_items
 WHERE quantity > 1
),
spending AS (
 SELECT customer_id, SUM(amount) AS total
 FROM orders
 WHERE order_id IN (SELECT * FROM cte_1)
 GROUP BY customer_id
),
cte_2 AS (
 SELECT customer_id
 FROM vip_list
)
SELECT c.name, spending.total
FROM customers c
JOIN spending
ON spending.customer_id = c.id
WHERE c.id IN (SELECT * FROM cte_2)
```

## Limitations

- The bundled `sqlvalidator` parser targets standard SQL. Specific syntax (BigQuery backtick paths, Postgres `::` casts, Snowflake `FLATTEN`, etc.) may not parse correctly.
- Correlated subqueries are extracted as CTEs but may produce semantically incorrect SQL since CTEs cannot reference columns from the outer query scope.
- The parser does not handle `EXISTS` as a first-class keyword — it is treated as a function call, which works for rewriting but may produce validation warnings.
