SELECT
    COUNT(*) AS number_of_sales,
    SUM(product_quantity) AS product_quantity_count,
    'Web' AS location
FROM
    orders_table
WHERE
    store_code LIKE 'WEB%'

UNION ALL

SELECT
    COUNT(*) AS number_of_sales,
    SUM(product_quantity) AS product_quantity_count,
    'Offline' AS location
FROM
    orders_table
WHERE
    store_code NOT LIKE 'WEB%';