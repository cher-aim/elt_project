CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.transform_dataset }}.{{ params.table_name}}` AS (
SELECT 
    invoice_id,
    line,
    c.id AS customer_id,
    p.id AS product_id,
    s.id AS size_id,
    d.id AS discount_id,
    store.id AS store_id,
    e.id AS employee_id,
    t.color,
    date,
    quantity,
    unit_price,
    payment_method,
    transaction_type,
    sku,
    currency,
    currency_symbol,
    invoice_total,
    line_total
FROM `{{ params.project_id }}.{{ params.transform_dataset }}.t1_transactions` t 
LEFT JOIN `{{ params.project_id }}.{{ params.transform_dataset }}.t3_employees` e 
ON e.employee_id = t.employee_id 
LEFT JOIN `{{ params.project_id }}.{{ params.transform_dataset }}.t2_discounts` d 
ON CAST(t.discount AS STRING) = CAST(d.discount AS STRING) 
AND DATE(t.date) BETWEEN d.start_date AND d.end_date
LEFT JOIN `{{ params.project_id }}.{{ params.transform_dataset }}.t2_sizes` s 
ON s.size = t.size 
LEFT JOIN `{{ params.project_id }}.{{ params.transform_dataset }}.t2_stores` store 
ON store.store_id = t.store_id
LEFT JOIN `{{ params.project_id }}.{{ params.transform_dataset }}.t2_customers` c 
ON t.customer_id = c.customer_id
LEFT JOIN `{{ params.project_id }}.{{ params.transform_dataset }}.t2_products` p 
ON p.product_id = t.product_id
);