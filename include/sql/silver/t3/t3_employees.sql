CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.transform_dataset }}.{{ params.table_name}}` AS
SELECT 
    employee_id,
    GENERATE_UUID() AS id,
    s.id AS store_id,
    name,
    position
FROM `{{ params.project_id }}.{{ params.transform_dataset }}.t1_employees` e
JOIN `{{ params.project_id }}.{{ params.transform_dataset }}.t2_stores` s 
ON e.store_id = s.store_id