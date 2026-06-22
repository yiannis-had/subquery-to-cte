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
