--SQL
-- Starbake Overall KPIs
WITH customer_metrics AS (
  SELECT
    COUNT(DISTINCT customer_id) AS total_customers,
    AVG(total_orders) AS avg_orders_per_customer,
    AVG(total_spent) AS avg_spent_per_customer,
    MIN(first_order_date) AS earliest_order_date,
    MAX(last_order_date) AS latest_order_date,
    AVG(days_since_first_order) AS avg_customer_lifetime_days,
    AVG(array_length(purchased_categories)) AS avg_categories_per_customer
  FROM
    starbake_analytics.customer_purchase_history
),
order_metrics AS (
  SELECT
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(total_order_value) AS total_revenue,
    AVG(total_order_value) AS avg_order_value,
    COUNT(DISTINCT customer_id) AS customers_with_orders
  FROM
    starbake_analytics.order_items_analysis
)
SELECT
  cm.total_customers,
  om.total_orders,
  om.total_revenue,
  om.avg_order_value,
  cm.avg_orders_per_customer,
  cm.avg_spent_per_customer,
  cm.earliest_order_date,
  cm.latest_order_date,
  cm.avg_customer_lifetime_days,
  cm.avg_categories_per_customer,
  om.customers_with_orders::FLOAT / cm.total_customers AS customer_order_rate,
  om.total_revenue / NULLIF(CAST(cm.latest_order_date as DATE) - CAST(cm.earliest_order_date as DATE), 0) AS daily_revenue,
  om.total_orders::FLOAT / NULLIF(CAST(cm.latest_order_date as DATE) - CAST(cm.earliest_order_date as DATE), 0) AS daily_order_rate
--  om.total_revenue / NULLIF(DATEDIFF('day', cm.earliest_order_date, cm.latest_order_date), 0) AS daily_revenue,
--  om.total_orders::FLOAT / NULLIF(DATEDIFF('day', cm.earliest_order_date, cm.latest_order_date), 0) AS daily_order_rate
FROM
  customer_metrics cm
CROSS JOIN
  order_metrics om;


