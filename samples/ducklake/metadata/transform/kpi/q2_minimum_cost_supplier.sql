SELECT s_acctbal,
s_name,
n_name,
p_partkey,
p_mfgr,
s_address,
s_phone,
s_comment
FROM tpch.part
INNER JOIN tpch.partsupp ON p_partkey = ps_partkey
INNER JOIN tpch.supplier ON s_suppkey = ps_suppkey
INNER JOIN tpch.nation ON s_nationkey = n_nationkey
INNER JOIN tpch.region ON n_regionkey = r_regionkey
WHERE p_size = 15
AND p_type like '%BRASS'
AND r_name = 'EUROPE'
AND ps_supplycost = (
  SELECT  min(ps_supplycost)
  FROM tpch.partsupp
  INNER JOIN tpch.supplier ON s_suppkey = ps_suppkey
  INNER JOIN tpch.nation ON s_nationkey = n_nationkey
  INNER JOIN tpch.region ON n_regionkey = r_regionkey
  WHERE p_partkey = ps_partkey
  AND r_name = 'EUROPE'
)
ORDER BY s_acctbal DESC, n_name, s_name, p_partkey