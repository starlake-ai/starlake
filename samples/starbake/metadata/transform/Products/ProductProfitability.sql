WITH ProductCostSummary AS (
    SELECT
        p.product_id,
        p.name AS product_name,
        SUM(i.price * pi.quantity) AS total_cost,
        SUM(op.quantity * op.price) AS total_revenue,
        SUM(op.quantity * op.price) - SUM(i.price * pi.quantity) AS total_profit,
        CASE
            WHEN SUM(op.quantity) > 0 THEN (SUM(op.quantity * op.price) - SUM(i.price * pi.quantity)) / SUM(op.quantity)
            ELSE 0
            END AS profit_per_unit
        FROM (select product.* from (select explode(o.products) as product from starbake.Orders o)) AS op
            JOIN
        starbake.Products p
        ON
                op.product_id = p.product_id
            JOIN (
            SELECT
                p.product_id,
                i.ingredient_id,
                i.quantity
            FROM
                starbake.Products p
                    JOIN
                (SELECT ingedient.* FROM (SELECT explode(p.ingredients) as ingedient from starbake.Products p)) AS i
        ) AS pi
                 ON
                         p.product_id = pi.product_id
            JOIN
        starbake.Ingredients i
        ON
                pi.ingredient_id = i.ingredient_id
    GROUP BY
        p.product_id,
        p.name
)
SELECT
    PCS.product_id,
    PCS.product_name,
    PCS.total_cost,
    PCS.total_revenue,
    PCS.total_profit,
    PCS.profit_per_unit
FROM
    ProductCostSummary PCS