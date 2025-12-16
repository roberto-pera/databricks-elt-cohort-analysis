--- Create a Query to Compute Retention Rates ---
-- Write a query that calculates, for each customer (using the raw ecom_orders table), the first purchase date and then (if available) the date of their second order. 
-- Then, group customers into cohorts by truncating the first purchase date by month and compute the percentage of customers who placed a second order within 1, 2, and 3 months.*/

CREATE OR REPLACE TABLE workspace.bigquery_db_cohort_db.gold_retention_rate_by_cohort AS

-- 1) Compute the month difference and cohort month
WITH customer_differences AS (
    SELECT 
        customer_id,
        DATE_TRUNC('month', first_purchase_date) AS cohort_month,
        first_purchase_date,
        second_purchase_date,
        FLOOR(
            MONTHS_BETWEEN(second_purchase_date, first_purchase_date)
        ) AS month_diff
    FROM workspace.bigquery_db_cohort_db.silver_cohort_analysis
)

-- 2) Aggregate cumulative retention rates
SELECT
    DATE_FORMAT(cohort_month, 'yyyy-MM') AS cohort_month,
    COUNT(*) AS num_customers,
    ROUND(100.0 * SUM(CASE WHEN month_diff <= 1 THEN 1 END) / COUNT(*)) AS retention_rate_1m,
    ROUND(100.0 * SUM(CASE WHEN month_diff <= 2 THEN 1 END) / COUNT(*)) AS retention_rate_2m,
    ROUND(100.0 * SUM(CASE WHEN month_diff <= 3 THEN 1 END) / COUNT(*)) AS retention_rate_3m
FROM customer_differences
GROUP BY cohort_month
ORDER BY cohort_month;
