with mycte as (
    select "seller_email", "amount"
    from HR.SELLERS hrs, SALES.ORDERS sos where hrs."id" = sos."seller_id"
)
select "seller_email", sum("amount") as sum from mycte
group by mycte."seller_email"



