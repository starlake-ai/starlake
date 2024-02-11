WITH ProductSalesSummary AS (
    SELECT
        p.product_id,
        p.name AS product_name,
        SUM(op.quantity) AS total_units_sold,
        SUM(op.quantity * op.price) AS total_revenue,
        CASE
            WHEN SUM(op.quantity) > 0 THEN SUM(op.quantity * op.price) / SUM(op.quantity)
            ELSE 0 
            END AS average_revenue_per_unit
    FROM
        (select product.* from (select explode(o.products) as product from starbake.Orders o)) AS op
            JOIN
        starbake.Products p
        ON
                op.product_id = p.product_id
    GROUP BY
        p.product_id,
        p.name
)
SELECT
    PSS.product_id,
    PSS.product_name,
    PSS.total_units_sold,
    PSS.total_revenue,
    PSS.average_revenue_per_unit
FROM
    ProductSalesSummary PSS