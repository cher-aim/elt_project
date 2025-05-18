CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.transform_dataset }}.{{ params.table_name }}` AS
SELECT 
    `Store ID` AS store_id,
    Country AS country,
    City AS city,
    `Store Name` AS store_name,
    CAST(`Number of Employees` AS INT) AS number_of_employees,
    `ZIP Code` AS zip_code,
    Latitude AS latitude,
    Longitude AS longitude
FROM `bronze.stores`