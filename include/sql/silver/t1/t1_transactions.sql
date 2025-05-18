CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.transform_dataset }}.{{ params.table_name }}` AS
SELECT 
    `Invoice ID` AS invoice_id,
    CAST(line AS INT) AS line,
    `Customer ID` customer_id,
    `Product ID` product_id,
    size,
    discount,
    `Store ID` store_id,
    `Employee ID` employee_id,
    color,
    PARSE_DATETIME('%F %T', date) AS date,
    CAST(quantity AS INT) AS quantity,
    CAST(`Unit Price` AS FLOAT64) AS unit_price,
    `Payment Method` payment_method,
    `Transaction Type` transaction_type,
    sku,
    currency,
    `Currency Symbol` AS currency_symbol,
    CAST(`Invoice Total` AS FLOAT64) AS invoice_total,
    CAST(`Line Total` AS FLOAT64) AS line_total
FROM `bronze.transactions`