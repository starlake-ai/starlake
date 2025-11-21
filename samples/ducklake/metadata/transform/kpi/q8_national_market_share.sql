explain analyze SELECT o_year,
SUM(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END) / SUM(volume) AS mkt_share
FROM (
  SELECT extract(year FROM o_orderdate) AS o_year,
  l_extendedprice * (1-l_discount) AS volume,
  n2.n_name AS nation
  FROM tpch.part
  INNER JOIN tpch.lineitem ON p_partkey = l_partkey
  INNER JOIN tpch.supplier ON s_suppkey = l_suppkey
  INNER JOIN tpch.orders ON l_orderkey = o_orderkey
  INNER JOIN tpch.customer ON o_custkey = c_custkey
  INNER JOIN tpch.nation n1 ON c_nationkey = n1.n_nationkey
  INNER JOIN tpch.nation n2 ON s_nationkey = n2.n_nationkey
  INNER JOIN tpch.region ON n1.n_regionkey = r_regionkey
  WHERE r_name = 'AMERICA'
  AND o_orderdate between date '1995-01-01' AND date '1996-12-31'
  AND p_type = 'ECONOMY ANODIZED STEEL') AS all_nations
GROUP BY o_year
ORDER BY o_year