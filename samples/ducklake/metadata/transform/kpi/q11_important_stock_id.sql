SELECT ps_partkey,
SUM(ps_supplycost*ps_availqty) AS value
FROM tpch.partsupp
INNER JOIN tpch.supplier ON ps_suppkey = s_suppkey
INNER JOIN tpch.nation ON s_nationkey = n_nationkey
WHERE n_name  = 'SAUDI ARABIA'
GROUP BY ps_partkey
HAVING SUM(ps_supplycost*ps_availqty) > (
  SELECT SUM(ps_supplycost*ps_availqty) * 0.0000000333
  FROM tpch.partsupp,
  tpch.supplier,
  tpch.nation
  WHERE ps_suppkey = s_suppkey
  AND s_nationkey = n_nationkey
  AND n_name  = 'SAUDI ARABIA'
)
ORDER BY value DESC