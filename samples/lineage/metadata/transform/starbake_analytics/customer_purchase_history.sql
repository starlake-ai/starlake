WITH customer_orders AS (
    SELECT
        o.customer_id,
        COUNT(DISTINCT o.order_id) AS total_orders,
        SUM(o.quantity * p.price) AS total_spent,
        MIN(o.order_date) AS first_order_date,
        MAX(o.order_date) AS last_order_date,
        LIST(DISTINCT p.category) AS purchased_categories
    FROM
        starbake.orders o
            JOIN
        starbake.products p ON o.product_id = p.product_id
    GROUP BY
        o.customer_id
)
SELECT
    co.customer_id,
    concat(c.first_name,' ', c.last_name) AS customer_name,
    c.email,
    co.total_orders,
    co.total_spent,
    co.first_order_date,
    co.last_order_date,
    co.purchased_categories,
    DATEDIFF('day', co.first_order_date, co.last_order_date) AS days_since_first_order
FROM
    starbake.customers c
        LEFT JOIN
    customer_orders co ON c.id = co.customer_id
ORDER BY
    co.total_spent DESC NULLS LAST;
