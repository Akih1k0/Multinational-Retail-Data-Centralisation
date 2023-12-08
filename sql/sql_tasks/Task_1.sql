SELECT 
    country_code AS country,
    COUNT(*) AS total_no_stores
FROM 
    dim_store_details
WHERE
    store_code NOT LIKE 'WEB%'
GROUP BY
    country_code;