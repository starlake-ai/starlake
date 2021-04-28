select seller_email, sum(amount) as sum from sellers, orders where sellers.id = orders.seller_id
group by sellers.seller_email

