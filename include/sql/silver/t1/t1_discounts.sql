CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.transform_dataset }}.{{ params.table_name}}` AS
SELECT 
    DISTINCT
    discont AS discount,
    description,
    PARSE_DATE('%F', `start`) AS start_date,
    PARSE_DATE('%F', `end`) AS end_date
FROM `bronze.discounts`