WITH order_details AS (
    SELECT  o.order_id
         , o.customer_id
         , List( p.name || ' (' || o.quantity || ')' ) AS purchased_items
         , Sum( o.quantity * p.price ) AS total_order_value
    FROM starbake.order_line o
             JOIN starbake.product p
                  ON o.product_id = p.product_id
    GROUP BY    o.order_id
           , o.customer_id )
SELECT  order_id
     , customer_id
     , purchased_items
     , total_order_value
FROM order_details
ORDER BY order_id



