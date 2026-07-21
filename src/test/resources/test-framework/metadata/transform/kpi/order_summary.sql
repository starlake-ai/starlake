select customer_id, sum(amount) as total_amount
from sales.orders
group by customer_id