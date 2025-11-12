SELECT s_name,
s_address
FROM tpch.supplier
INNER JOIN tpch.nation ON s_nationkey = n_nationkey
WHERE s_suppkey in (
  SELECT ps_suppkey
  FROM tpch.partsupp
  WHERE ps_partkey in (
    SELECT p_partkey
    FROM tpch.part
    WHERE p_name like 'forest%')
    AND ps_availqty > (
      SELECT 0.5 * sum(l_quantity)
      FROM tpch.lineitem
      WHERE l_partkey = ps_partkey
      AND l_suppkey = ps_suppkey
      AND l_shipdate >= date('1994-01-01')
      AND l_shipdate < date('1994-01-01') + INTERVAL '1' year))
AND n_name = 'CANADA'
ORDER BY s_name