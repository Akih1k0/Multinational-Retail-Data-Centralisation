WITH sales AS (
    SELECT
        dim_store_details.store_type,
        SUM(dim_products.product_price_gbp * orders_table.product_quantity) AS total_sales
    FROM
        orders_table
    JOIN
        dim_products ON orders_table.product_code = dim_products.product_code
    JOIN
        dim_store_details ON orders_table.store_code = dim_store_details.store_code
    GROUP BY
        dim_store_details.store_type
)

SELECT 
    store_type,
    ROUND(SUM(total_sales)::numeric, 2) AS total_sales,
    ROUND((SUM(total_sales)::numeric * 100) / (SELECT SUM(total_sales) FROM sales)::numeric, 2) AS percentage_total
FROM
    sales
GROUP BY
    store_type
ORDER BY
    total_sales DESC;