-- Create a new column to store invalid store codes
ALTER TABLE orders_table
ADD COLUMN invalid_store_codes VARCHAR(255);

-- Update the new column with invalid store codes
UPDATE orders_table
SET invalid_store_codes = store_code
WHERE store_code NOT IN (SELECT store_code FROM dim_store_details);

-- Update the original column to set invalid store codes as NULL
UPDATE orders_table
SET store_code = NULL
WHERE store_code NOT IN (SELECT store_code FROM dim_store_details);
