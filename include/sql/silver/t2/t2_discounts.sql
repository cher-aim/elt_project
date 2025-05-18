CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.transform_dataset }}.{{ params.table_name}}` AS
WITH find_current AS (
SELECT 
    DISTINCT
    discount,
    description,
    start_date,
    end_date,
    DENSE_RANK() OVER(PARTITION BY CAST(discount AS STRING) ORDER BY start_date DESC, end_date DESC) AS ranking
FROM `{{ params.project_id}}.{{ params.transform_dataset }}.t1_discounts` 
)
SELECT  
    DISTINCT
    GENERATE_UUID() AS id,
    discount,
    description,
    start_date,
    end_date,
    CASE WHEN ranking = 1 THEN 'Y' 
    ELSE 'N' END AS is_current
FROM find_current 