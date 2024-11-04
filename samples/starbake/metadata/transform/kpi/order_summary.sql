    ps.order_id,
    ps.order_date,
    rs.total_revenue,
    ps.profit,
    ps.total_units_sold
FROM
    kpi.product_summary ps
        JOIN kpi.revenue_summary rs ON ps.order_id = rs.order_id
