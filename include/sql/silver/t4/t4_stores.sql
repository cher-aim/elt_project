CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.transform_dataset }}.{{ params.table_name}}` AS
SELECT 
    id,
    country,
    city,
    store_name,
    number_of_employees,
    zip_code,
    latitude,
    longitude
FROM `{{ params.project_id }}.{{ params.transform_dataset }}.t2_stores`