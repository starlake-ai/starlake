SELECT nation,
o_year,
SUM(amount) AS sum_profit
FROM (
SELECT n_name AS nation,
extract(year FROM o_orderdate) AS o_year,
l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
FROM tpch.part
INNER JOIN tpch.lineitem ON p_partkey = l_partkey
INNER JOIN tpch.supplier ON s_suppkey = l_suppkey
INNER JOIN tpch.partsupp ON ps_suppkey = l_suppkey and ps_partkey = l_partkey
INNER JOIN tpch.orders ON o_orderkey = l_orderkey
INNER JOIN tpch.nation ON s_nationkey = n_nationkey
WHERE p_name like '%green%') AS profit
GROUP BY nation,  o_year
ORDER BY nation,  o_year DESC