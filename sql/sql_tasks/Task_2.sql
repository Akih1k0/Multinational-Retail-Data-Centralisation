SELECT 
    locality,
    COUNT(*) AS total_no_stores
FROM 
    dim_store_details 
GROUP BY
    locality
HAVING
    COUNT(*) >= 10
ORDER BY
    total_no_stores DESC;