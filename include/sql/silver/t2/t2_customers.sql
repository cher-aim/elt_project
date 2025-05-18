CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.transform_dataset }}.{{ params.table_name}}` AS
SELECT
    customer_id,
    GENERATE_UUID() AS id,
    name,
    email,
    telephone,
    city,
    country,
    gender,
    date_of_birth,
    job_title
FROM `{{ params.project_id }}.{{ params.transform_dataset }}.t1_customers`