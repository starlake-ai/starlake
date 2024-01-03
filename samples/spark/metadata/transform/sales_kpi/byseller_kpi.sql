with mycte as (
    select o.amount, c.id
    from sales.orders o, sales.customers c
    where o.customer_id = c.id
)
select id, sum(amount) as sum from mycte
group by mycte.id



