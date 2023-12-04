ALTER TABLE orders_table
ADD FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid);

ALTER TABLE orders_table
ADD FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid);

ALTER TABLE orders_table
ADD FOREIGN KEY (product_code) REFERENCES dim_products(product_code);

ALTER TABLE orders_table
ADD CONSTRAINT fk_product_code
FOREIGN KEY (product_code) 
REFERENCES dim_products(product_code);

ALTER TABLE orders_table
ADD CONSTRAINT fk_store_code
FOREIGN KEY (store_code) 
REFERENCES dim_store_details(store_code);