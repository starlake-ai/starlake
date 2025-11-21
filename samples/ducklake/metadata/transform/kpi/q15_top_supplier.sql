SELECT s_suppkey,
s_name,
s_address,
s_phone,
total_revenue
FROM tpch.supplier
INNER JOIN (
  SELECT l_suppkey as supplier_no,  sum(l_extendedprice * (1 - l_discount)) AS total_revenue
  FROM tpch.lineitem
  WHERE l_shipdate >= date '1996-01-01'
  AND l_shipdate < date '1996-01-01' + INTERVAL '3' month
  GROUP BY l_suppkey
) AS revenue ON s_suppkey = supplier_no
WHERE total_revenue IN (
  SELECT  MAX(total_revenue)
  FROM (
    SELECT l_suppkey as supplier_no,
    SUM(l_extendedprice * (1 - l_discount)) AS total_revenue
    FROM tpch.lineitem
    WHERE l_shipdate >= date '1996-01-01'
    AND l_shipdate < date '1996-01-01' + INTERVAL '3' month
    GROUP BY l_suppkey
  ) AS revenue
)
order by s_suppkey