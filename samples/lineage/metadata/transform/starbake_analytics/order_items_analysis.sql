WITH order_details AS (
    SELECT  o.order_id
         , o.order_date
         , o.customer_id
         , List( p.name || ' (' || o.quantity || ')' ) AS purchased_items
         , Sum( o.quantity * p.price ) AS total_order_value
    FROM starbake.orders o
             JOIN starbake.products p
                  ON o.product_id = p.product_id
    GROUP BY    o.order_id
           , o.order_date
           , o.customer_id )
SELECT  order_id
     , order_date
     , customer_id
     , purchased_items
     , total_order_value
FROM order_details
ORDER BY order_id
