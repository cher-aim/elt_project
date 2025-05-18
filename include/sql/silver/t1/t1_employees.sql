CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.transform_dataset }}.{{ params.table_name}}` AS
SELECT 
    `Employee ID` AS employee_id,
    `Store ID` AS store_id,
    name,
    position
FROM `bronze.employees`