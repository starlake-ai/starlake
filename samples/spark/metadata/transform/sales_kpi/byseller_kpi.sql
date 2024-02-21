with mycte as (
    select o.amount, c.id, CURRENT_DATETIME() as timestamp
    from sales.orders o, sales.customers c
    where o.customer_id = c.id
)
select id, sum(amount) as sum, timestamp,
       ARRAY<STRUCT<city STRING, state STRING>>[("Seattle", "Washington"),("Phoenix", "Arizona")] AS location1,
       STRUCT("Seattle" AS city, "Washington" AS state) AS location2
from mycte
group by mycte.id, mycte.timestamp



