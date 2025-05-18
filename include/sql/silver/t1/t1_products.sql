CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.transform_dataset }}.{{ params.table_name}}` AS
SELECT 
    `Product ID` AS product_id,
    category,
    `Sub Category` AS sub_category,
    `Description PT` AS description_pt,
    `Description DE` AS description_de,
    `Description FR` AS description_fr,
    `Description ES` AS description_es,
    `Description EN` AS description_en,
    `Description ZH` AS description_zh,
    color,
    sizes,
    CAST(`Production Cost` AS FLOAT64) AS production_cost
FROM `bronze.products`