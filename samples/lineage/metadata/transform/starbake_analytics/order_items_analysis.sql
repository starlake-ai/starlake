--SQL
with order_details AS (
  SELECT
    o.order_id,
    o.order_date,
    o.customer_id,
    p.price,
    LIST(p.name || ' (' || o.quantity || ')') AS purchased_items,
    SUM(o.quantity * p.price) AS total_order_value
  FROM
    starbake.orders o,    starbake.products p
  WHERE o.product_id = p.product_id
  GROUP BY
    o.order_id, o.order_date, o.customer_id
)
SELECT
  order_id,
  order_date,
  customer_id,
  price,
  purchased_items,
  total_order_value
FROM
  order_details
ORDER BY
  order_id;

