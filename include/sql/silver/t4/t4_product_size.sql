CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.transform_dataset }}.{{ params.table_name}}` AS
WITH split_size AS (
SELECT 
    product_id AS product_id,
    TRIM(size) AS size  
FROM `{{ params.project_id }}.{{ params.transform_dataset }}.t1_products`,
UNNEST(SPLIT(sizes, '|')) AS size
)
SELECT 
    DISTINCT
    product.id AS product_id,
    size.id AS size_id
FROM split_size  
JOIN `{{ params.project_id }}.{{ params.transform_dataset }}.t2_products` product
ON split_size.product_id = product.product_id
JOIN `{{ params.project_id }}.{{ params.transform_dataset }}.t2_sizes` size 
ON size.size = split_size.size 