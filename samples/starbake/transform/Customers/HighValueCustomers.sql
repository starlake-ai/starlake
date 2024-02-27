SELECT
    customer_id,
    first_name,
    last_name,
    order_count,
    total_spend,
    average_spend_per_order,
    lifetime_value
FROM
    CustomerLifetimeValue
ORDER BY
    lifetime_value DESC
LIMIT 3
