CREATE OR REPLACE TABLE workspace.bigquery_db_cohort_db.silver_cohort_analysis AS

-- Step 1: Calculate the First Purchase Date
WITH first_purchases AS (
  SELECT 
    customer_id, 
    MIN(order_date) AS first_purchase_date, 
    COUNT(*) AS total_orders
  FROM workspace.bigquery_db_cohort_db.bronze_ecom_orders
  GROUP BY customer_id
),

-- Step 2: Calculate the Second Purchase Date
second_purchases AS (
  SELECT 
    eo.customer_id, 
    MIN(eo.order_date) AS second_purchase_date
  FROM workspace.bigquery_db_cohort_db.bronze_ecom_orders AS eo
  JOIN first_purchases fp ON eo.customer_id = fp.customer_id
  WHERE eo.order_date > fp.first_purchase_date
  GROUP BY eo.customer_id
)

-- Step 3: Combine the Results and Calculate the Days Between Purchases
SELECT 
  fp.customer_id, 
  first_purchase_date,
  total_orders, 
  second_purchase_date,
  DATEDIFF(second_purchase_date, first_purchase_date) AS days_between_first_and_second
FROM first_purchases AS fp
LEFT JOIN second_purchases AS sp ON fp.customer_id = sp.customer_id
