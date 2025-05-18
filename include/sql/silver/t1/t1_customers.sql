CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.transform_dataset }}.{{ params.table_name}}` AS
SELECT
    `Customer ID` AS customer_id,
    name,
    email,
    telephone,
    city,
    country,
    gender,
    `Date of Birth` AS date_of_birth,
    `Job Title` AS job_title
FROM `bronze.customers`