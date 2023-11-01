with mycte as (
    select "SELLER_EMAIL", "AMOUNT"
    from hr.sellers hrs, sales.orders sos where hrs."ID" = sos."SELLER_ID"
    )
select "SELLER_EMAIL", sum("AMOUNT") as sum from mycte
group by mycte."SELLER_EMAIL"
