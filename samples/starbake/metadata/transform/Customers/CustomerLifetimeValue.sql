With CustomerOrderSummary AS (
    SELECT
        c.customer_id,
        c.first_name,
        c.last_name,
        COUNT(o.order_id) AS order_count,
        SUM(o.products_total) AS total_spend,
        AVG(o.products_total) AS average_spend_per_order
    FROM
        starbake.Customers c
            LEFT JOIN (
            SELECT
                customer_id,
                order_id,
                SUM(price * quantity) AS products_total
            FROM
                (select customer_id, order_id, product.* from (select o.customer_id, o.order_id, explode(o.products) as product from starbake.Orders o))
            GROUP BY
                customer_id,
                order_id
        ) o
                      ON
                              c.customer_id = o.customer_id
    GROUP BY
        c.customer_id,
        c.first_name,
        c.last_name,
        c.join_date
)

SELECT
    COS.customer_id,
    COS.first_name,
    COS.last_name,
    COS.order_count,
    COS.total_spend,
    COS.average_spend_per_order,
    CASE
        WHEN COS.order_count > 0 THEN COS.total_spend / COS.order_count
        ELSE 0
        END AS lifetime_value
FROM
    CustomerOrderSummary COS