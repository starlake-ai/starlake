SELECT l_orderkey,
SUM(l_extendedprice*(1-l_discount)) AS revenue,
o_orderdate,
o_shippriority
FROM tpch.customer
INNER JOIN tpch.orders ON c_custkey = o_custkey
INNER JOIN tpch.lineitem ON l_orderkey = o_orderkey
WHERE c_mktsegment = 'BUILDING'
AND o_orderdate < date '1995-03-15'
AND l_shipdate > date '1995-03-15'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue DESC, o_orderdate