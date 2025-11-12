SELECT c_name,
c_custkey,
o_orderkey,
o_orderdate,
o_totalprice,
SUM(l_quantity) AS quantity
FROM tpch.customer
INNER JOIN tpch.orders ON c_custkey = o_custkey
INNER JOIN tpch.lineitem ON o_orderkey = l_orderkey
WHERE o_orderkey IN (SELECT l_orderkey FROM tpch.lineitem GROUP BY l_orderkey HAVING sum(l_quantity) > 300)
GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
ORDER BY o_totalprice DESC, o_orderdate