CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.transform_dataset }}.{{ params.table_name}}` AS
SELECT 
    id,
    store_id,
    name,
    position
FROM `{{ params.project_id }}.{{ params.transform_dataset }}.t3_employees`