SELECT c_count,
COUNT(*) as custdist
FROM (
  SELECT c_custkey,
  COUNT(o_orderkey)
  FROM tpch.customer
  LEFT OUTER JOIN tpch.orders ON c_custkey = o_custkey AND o_comment NOT LIKE '%special%requests%'
  GROUP BY c_custkey
) AS c_orders (c_custkey, c_count)
GROUP BY c_count
ORDER BY custdist DESC, c_count DESC