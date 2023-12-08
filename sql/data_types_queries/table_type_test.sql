SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'orders_table';

SELECT *
FROM orders_table;

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'dim_users';

SELECT *
FROM dim_users;

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'dim_store_details';

SELECT *
FROM dim_store_details;

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'dim_products';

SELECT *
FROM dim_products;

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'dim_date_times';

SELECT *
FROM dim_date_times;

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'dim_card_details';

SELECT *
FROM dim_card_details;