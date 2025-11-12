SELECT c_custkey,
c_name,
SUM(l_extendedprice * (1 - l_discount)) AS revenue,
c_acctbal,
n_name,
c_address,
c_phone,
c_comment
FROM tpch.customer
INNER JOIN tpch.orders ON c_custkey = o_custkey
INNER JOIN tpch.lineitem ON l_orderkey = o_orderkey
INNER JOIN tpch.nation ON c_nationkey = n_nationkey
WHERE o_orderdate >= date '1993-10-01'
AND o_orderdate < date '1993-10-01' + INTERVAL '3' month
AND l_returnflag = 'R'
GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
ORDER BY revenue DESC