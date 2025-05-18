CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.transform_dataset }}.{{ params.table_name}}` AS
SELECT  
    id,
    CAST(discount AS FLOAT64) AS discount,
    description,
    start_date,
    end_date,
    is_current
FROM `{{ params.project_id }}.{{ params.transform_dataset }}.t2_discounts`