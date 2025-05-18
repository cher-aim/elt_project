CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.transform_dataset }}.{{ params.table_name}}` AS
WITH distinct_product AS (
SELECT 
    DISTINCT
    product_id,
    category,
    sub_category,
    description_pt,
    description_de,
    description_fr,
    description_es,
    description_en,
    description_zh,
    color,
    production_cost
FROM `{{ params.project_id }}.{{ params.transform_dataset }}.t1_products`
)
SELECT 
    GENERATE_UUID() AS id,
    product_id,
    category,
    sub_category,
    description_pt,
    description_de,
    description_fr,
    description_es,
    description_en,
    description_zh,
    color,
    production_cost
FROM distinct_product