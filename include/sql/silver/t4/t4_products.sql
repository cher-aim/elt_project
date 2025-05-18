CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.transform_dataset }}.{{ params.table_name}}` AS
SELECT 
    id,
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
FROM `{{ params.project_id }}.{{ params.transform_dataset }}.t2_products`