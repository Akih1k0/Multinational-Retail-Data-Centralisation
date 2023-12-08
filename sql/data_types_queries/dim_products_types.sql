ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(15);

UPDATE dim_products
SET weight_class = CASE
    WHEN weight_kg >=0 AND weight_kg <2 THEN 'Light'
    WHEN weight_kg >=2 AND weight_kg <40 THEN 'Mid_Sized'
    WHEN weight_kg >=40 AND weight_kg <140 THEN 'Heavy'
    WHEN weight_kg >=140 THEN 'Truck_Required'
END;

ALTER TABLE dim_products
ALTER COLUMN product_price_gbp TYPE FLOAT USING product_price_gbp::FLOAT;

ALTER TABLE dim_products
ALTER COLUMN weight_kg TYPE FLOAT USING weight_kg::FLOAT;

ALTER TABLE dim_products
ALTER COLUMN ean TYPE VARCHAR(20) USING ean::VARCHAR(20);

ALTER TABLE dim_products
ALTER COLUMN product_code TYPE VARCHAR(20) USING product_code::VARCHAR(20);

ALTER TABLE dim_products
ALTER COLUMN date_added TYPE DATE USING date_added::DATE;

ALTER TABLE dim_products
ALTER COLUMN uuid TYPE UUID USING uuid::UUID;

ALTER TABLE dim_products
ALTER COLUMN still_available TYPE BOOL USING still_available::BOOL;