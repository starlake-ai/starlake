SELECT p_brand,
p_type,
p_size,
COUNT(distinct ps_suppkey) AS supplier_cnt
FROM tpch.partsupp
INNER JOIN tpch.part ON p_partkey = ps_partkey
WHERE p_brand <> 'Brand#45' AND p_type NOT LIKE 'MEDIUM POLISHED%'
AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
AND ps_suppkey NOT IN (SELECT s_suppkey FROM tpch.supplier WHERE s_comment like '%Customer%Complaints%')
GROUP BY p_brand, p_type, p_size
ORDER BY supplier_cnt desc, p_brand, p_type, p_size