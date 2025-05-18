CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.transform_dataset }}.{{ params.table_name}}` AS
WITH split_size AS (
SELECT 
    product_id AS product_id,
    TRIM(size) AS size 
FROM `{{ params.project_id }}.{{ params.transform_dataset }}.t1_products`,
UNNEST(SPLIT(sizes, '|')) AS size
), distinct_size AS (
SELECT 
    DISTINCT
    size
FROM split_size
)
SELECT 
    GENERATE_UUID() AS id,
    size 
FROM distinct_size
