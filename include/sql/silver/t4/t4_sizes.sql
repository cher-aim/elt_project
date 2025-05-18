CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.transform_dataset }}.{{ params.table_name}}` AS
SELECT 
    id,
    size
FROM `{{ params.project_id }}.{{ params.transform_dataset }}.t2_sizes`