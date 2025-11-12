SELECT supp_nation,
cust_nation,
l_year,
SUM(volume) AS revenue
FROM (
SELECT n1.n_name AS supp_nation,
n2.n_name AS cust_nation,
extract(year FROM l_shipdate) AS l_year,
l_extendedprice * (1 - l_discount) as volume
FROM tpch.supplier
INNER JOIN tpch.lineitem ON s_suppkey = l_suppkey
INNER JOIN tpch.orders ON o_orderkey = l_orderkey
INNER JOIN tpch.customer ON c_custkey = o_custkey
INNER JOIN tpch.nation n1 ON  s_nationkey = n1.n_nationkey
INNER JOIN tpch.nation n2 ON  c_nationkey = n2.n_nationkey
WHERE ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))
AND l_shipdate between date '1995-01-01' AND date '1996-12-31') AS shipping
GROUP BY supp_nation, cust_nation, l_year
ORDER BY supp_nation, cust_nation, l_year