explain analyze SELECT o_orderpriority,
COUNT(*) as order_count
FROM tpch.orders
WHERE o_orderdate >= date '1993-07-01'
AND o_orderdate < date '1993-07-01' + interval '3' month
AND exists (select *
from tpch.lineitem
WHERE l_orderkey = o_orderkey
AND l_commitdate < l_receiptdate)
GROUP BY o_orderpriority
ORDER BY o_orderpriority