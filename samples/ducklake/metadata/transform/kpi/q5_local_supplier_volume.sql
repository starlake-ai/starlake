explain analyze SELECT n_name,
SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM tpch.customer
INNER JOIN tpch.orders ON c_custkey = o_custkey
INNER JOIN tpch.lineitem ON l_orderkey = o_orderkey
INNER JOIN tpch.supplier ON l_suppkey = s_suppkey AND c_nationkey = s_nationkey
INNER JOIN tpch.nation ON s_nationkey = n_nationkey
INNER JOIN tpch.region ON n_regionkey = r_regionkey
WHERE r_name = 'ASIA'
AND o_orderdate >= date '1994-01-01'
AND o_orderdate < date '1994-01-01' + INTERVAL '1' year
GROUP BY n_name
ORDER BY revenue DESC