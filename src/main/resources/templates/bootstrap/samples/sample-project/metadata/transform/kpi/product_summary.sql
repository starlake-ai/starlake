SELECT
    p.product_id,
    p.name AS product_name,
    SUM(ol.quantity) AS total_units_sold,
    (SUM(ol.sale_price) - Sum(ol.quantity * p.cost)) AS profit,
    o.order_id,
    o.timestamp AS order_date
FROM
    starbake.products p
        JOIN starbake.order_lines ol ON p.product_id = ol.product_id
        JOIN starbake.orders o ON ol.order_id = o.order_id
GROUP BY
    p.product_id,
    o.order_id, p.name, o.timestamp
